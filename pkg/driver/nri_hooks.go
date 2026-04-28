/*
Copyright The Kubernetes Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/containerd/nri/pkg/api"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	metav1apply "k8s.io/client-go/applyconfigurations/meta/v1"
	resourceapply "k8s.io/client-go/applyconfigurations/resource/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/set"
)

// NRI hooks into the container runtime, the lifecycle of the Pod seen here is local to the runtime
// and is not the same as the Pod lifecycle for kubernetes, per example, a Pod that can fail to start
// is retried locally multiple times, so the hooks need to be idempotent to all operations on the Pod.
// The NRI hooks are time sensitive, any slow operation needs to be added on the DRA hooks and only
// the information necessary should passed to the NRI hooks via the np.podConfigStore so it can be executed
// quickly.

func (np *NetworkDriver) Synchronize(_ context.Context, pods []*api.PodSandbox, containers []*api.Container) ([]*api.ContainerUpdate, error) {
	klog.Infof("Synchronized state with the runtime (%d pods, %d containers)...",
		len(pods), len(containers))

	for _, pod := range pods {
		klog.Infof("Synchronize Pod %s/%s UID %s", pod.Namespace, pod.Name, pod.Uid)
		klog.V(2).Infof("pod %s/%s: namespace=%s ips=%v", pod.GetNamespace(), pod.GetName(), getNetworkNamespace(pod), pod.GetIps())
		// get the pod network namespace
		ns := getNetworkNamespace(pod)
		// host network pods are skipped
		if ns != "" {
			// store the Pod metadata in the db
			np.netdb.AddPodNetNs(podKey(pod), ns)
		}
	}

	return nil, nil
}

// CreateContainer handles container creation requests.
func (np *NetworkDriver) CreateContainer(ctx context.Context, pod *api.PodSandbox, ctr *api.Container) (*api.ContainerAdjustment, []*api.ContainerUpdate, error) {
	klog.V(2).Infof("CreateContainer Pod %s/%s UID %s Container %s", pod.Namespace, pod.Name, pod.Uid, ctr.Name)
	start := time.Now()
	status := statusNoop
	defer func() {
		nriPluginRequestsTotal.WithLabelValues(methodCreateContainer, status).Inc()
		nriPluginRequestsLatencySeconds.WithLabelValues(methodCreateContainer, status).Observe(time.Since(start).Seconds())
	}()
	podConfig, ok := np.podConfigStore.GetPodConfigs(types.UID(pod.GetUid()))
	if !ok {
		return nil, nil, nil
	}
	adjust, update, err := np.createContainer(ctx, pod, ctr, podConfig)
	if err != nil {
		status = statusFailed
	} else {
		status = statusSuccess
	}
	return adjust, update, err
}

func (np *NetworkDriver) createContainer(_ context.Context, pod *api.PodSandbox, _ *api.Container, podConfig map[string]PodConfig) (*api.ContainerAdjustment, []*api.ContainerUpdate, error) {
	// Containers only cares about the RDMA char devices
	devPaths := set.Set[string]{}
	adjust := &api.ContainerAdjustment{}
	for _, config := range podConfig {
		for _, dev := range config.RDMADevice.DevChars {
			// do not insert the same path multiple times
			if devPaths.Has(dev.Path) {
				continue
			}
			devPaths.Insert(dev.Path)
			// TODO check the file permissions and uid and gid fields
			adjust.AddDevice(&api.LinuxDevice{
				Path:  dev.Path,
				Type:  dev.Type,
				Major: dev.Major,
				Minor: dev.Minor,
			})
		}
	}
	return adjust, nil, nil
}

func (np *NetworkDriver) RunPodSandbox(ctx context.Context, pod *api.PodSandbox) error {
	klog.Infof("DEBUG RunPodSandbox ENTER Pod %s/%s UID %s", pod.Namespace, pod.Name, pod.Uid)
	start := time.Now()
	status := statusNoop
	defer func() {
		nriPluginRequestsTotal.WithLabelValues(methodRunPodSandbox, status).Inc()
		klog.Infof("DEBUG RunPodSandbox EXIT Pod %s/%s UID %s status=%s took %v", pod.Namespace, pod.Name, pod.Uid, status, time.Since(start))
		nriPluginRequestsLatencySeconds.WithLabelValues(methodRunPodSandbox, status).Observe(time.Since(start).Seconds())
	}()

	podUID := types.UID(pod.GetUid())
	klog.Infof("DEBUG RunPodSandbox Pod %s/%s: podUID=%s", pod.Namespace, pod.Name, podUID)

	// Check upstream DRA config store first.
	podConfig, hasDRAConfig := np.podConfigStore.GetPodConfigs(podUID)
	klog.Infof("DEBUG RunPodSandbox Pod %s/%s: podConfigStore.GetPodConfigs returned hasDRAConfig=%v, len(podConfig)=%d", pod.Namespace, pod.Name, hasDRAConfig, len(podConfig))
	for devName, cfg := range podConfig {
		klog.Infof("DEBUG RunPodSandbox Pod %s/%s: DRA device=%q claim=%s/%s hostIface=%q podIface=%q rdmaLinkDev=%q",
			pod.Namespace, pod.Name, devName, cfg.Claim.Namespace, cfg.Claim.Name,
			cfg.NetworkInterfaceConfigInHost.Interface.Name, cfg.NetworkInterfaceConfigInPod.Interface.Name, cfg.RDMADevice.LinkDev)
	}

	// Check SwiftV2 config store.
	swiftV2Configs := np.swiftV2Store.Get(podUID)
	klog.Infof("DEBUG RunPodSandbox Pod %s/%s: swiftV2Store.Get returned %d configs (nil=%v)",
		pod.Namespace, pod.Name, len(swiftV2Configs), swiftV2Configs == nil)

	if !hasDRAConfig && swiftV2Configs == nil {
		klog.Infof("DEBUG RunPodSandbox Pod %s/%s UID %s: NO config from either store, returning no-op",
			pod.Namespace, pod.Name, pod.Uid)
		return nil
	}

	// Process upstream DRA devices if present.
	if hasDRAConfig {
		klog.Infof("DEBUG RunPodSandbox Pod %s/%s: entering DRA path (runPodSandbox) with %d devices", pod.Namespace, pod.Name, len(podConfig))
		err := np.runPodSandbox(ctx, pod, podConfig)
		if err != nil {
			klog.Infof("DEBUG RunPodSandbox Pod %s/%s: DRA path FAILED: %v", pod.Namespace, pod.Name, err)
			status = statusFailed
			return err
		}
		klog.Infof("DEBUG RunPodSandbox Pod %s/%s: DRA path succeeded", pod.Namespace, pod.Name)
		status = statusSuccess
	}

	// Process SwiftV2 devices if present.
	if swiftV2Configs != nil {
		klog.Infof("DEBUG RunPodSandbox Pod %s/%s: entering SwiftV2 path (runPodSandboxSwiftV2) with %d configs", pod.Namespace, pod.Name, len(swiftV2Configs))
		err := np.runPodSandboxSwiftV2(ctx, pod, swiftV2Configs)
		if err != nil {
			klog.Infof("DEBUG RunPodSandbox Pod %s/%s: SwiftV2 path FAILED: %v", pod.Namespace, pod.Name, err)
			status = statusFailed
			return err
		}
		klog.Infof("DEBUG RunPodSandbox Pod %s/%s: SwiftV2 path succeeded", pod.Namespace, pod.Name)
		status = statusSuccess
	}

	return nil
}

func (np *NetworkDriver) runPodSandbox(_ context.Context, pod *api.PodSandbox, podConfig map[string]PodConfig) error {
	klog.Infof("DEBUG runPodSandbox ENTER Pod %s/%s UID %s with %d device configs", pod.Namespace, pod.Name, pod.Uid, len(podConfig))
	// get the pod network namespace
	ns := getNetworkNamespace(pod)
	klog.Infof("DEBUG runPodSandbox Pod %s/%s: networkNamespace=%q", pod.Namespace, pod.Name, ns)
	// host network pods can not allocate network devices because it impact the host
	if ns == "" {
		klog.Infof("DEBUG runPodSandbox Pod %s/%s: host network detected, returning error", pod.Namespace, pod.Name)
		return fmt.Errorf("RunPodSandbox pod %s/%s using host network can not claim host devices", pod.Namespace, pod.Name)
	}
	// store the Pod metadata in the db
	klog.Infof("DEBUG runPodSandbox Pod %s/%s: storing netns mapping podKey=%q ns=%q", pod.Namespace, pod.Name, podKey(pod), ns)
	np.netdb.AddPodNetNs(podKey(pod), ns)

	// Track all the status updates needed for the resource claims of the pod.
	statusUpdates := map[types.NamespacedName]*resourceapply.ResourceClaimStatusApplyConfiguration{}
	// Process the configurations of the ResourceClaim
	for deviceName, config := range podConfig {
		klog.Infof("DEBUG runPodSandbox Pod %s/%s: processing device=%q claim=%s/%s", pod.Namespace, pod.Name, deviceName, config.Claim.Namespace, config.Claim.Name)
		klog.Infof("DEBUG runPodSandbox Pod %s/%s device=%q: hostIface=%q podIface=%q rdmaLinkDev=%q rdmaDevChars=%d",
			pod.Namespace, pod.Name, deviceName,
			config.NetworkInterfaceConfigInHost.Interface.Name, config.NetworkInterfaceConfigInPod.Interface.Name,
			config.RDMADevice.LinkDev, len(config.RDMADevice.DevChars))
		resourceClaim := types.NamespacedName{Name: config.Claim.Name, Namespace: config.Claim.Namespace}
		resourceClaimStatus := statusUpdates[resourceClaim]
		if statusUpdates[resourceClaim] == nil {
			resourceClaimStatus = resourceapply.ResourceClaimStatus()
			statusUpdates[resourceClaim] = resourceClaimStatus
		}
		// resourceClaim status for this specific device
		resourceClaimStatusDevice := resourceapply.
			AllocatedDeviceStatus().
			WithDevice(deviceName).
			WithDriver(np.driverName).
			WithPool(np.nodeName)

		ifName := config.NetworkInterfaceConfigInHost.Interface.Name

		klog.Infof("DEBUG runPodSandbox Pod %s/%s device=%q: moving hostIface=%q to ns=%q", pod.Namespace, pod.Name, deviceName, ifName, ns)
		// TODO config options to rename the device and pass parameters
		// use https://github.com/opencontainers/runtime-spec/pull/1271
		networkData, err := nsAttachNetdev(ifName, ns, config.NetworkInterfaceConfigInPod.Interface)
		if err != nil {
			klog.Infof("RunPodSandbox error moving device %s to namespace %s: %v", deviceName, ns, err)
			return fmt.Errorf("error moving network device %s to namespace %s: %v", deviceName, ns, err)
		}

		resourceClaimStatusDevice.WithConditions(
			metav1apply.Condition().
				WithType("Ready").
				WithReason("NetworkDeviceReady").
				WithStatus(metav1.ConditionTrue).
				WithLastTransitionTime(metav1.Now()),
		).WithNetworkData(resourceapply.NetworkDeviceData().
			WithInterfaceName(networkData.InterfaceName).
			WithHardwareAddress(networkData.HardwareAddress).
			WithIPs(networkData.IPs...),
		) // End of WithNetworkData

		// The interface name inside the container's namespace.
		ifNameInNs := networkData.InterfaceName

		// Apply Ethtool configurations
		if config.NetworkInterfaceConfigInPod.Ethtool != nil {
			klog.Infof("DEBUG runPodSandbox Pod %s/%s device=%q: applying ethtool config for ifNameInNs=%q", pod.Namespace, pod.Name, deviceName, ifNameInNs)
			err = applyEthtoolConfig(ns, ifNameInNs, config.NetworkInterfaceConfigInPod.Ethtool)
			if err != nil {
				klog.Infof("RunPodSandbox error applying ethtool config for %s in ns %s: %v", ifNameInNs, ns, err)
				return fmt.Errorf("error applying ethtool config for %s in ns %s: %v", ifNameInNs, ns, err)
			}
		} else {
			klog.Infof("DEBUG runPodSandbox Pod %s/%s device=%q: no ethtool config, skipping", pod.Namespace, pod.Name, deviceName)
		}

		// Check if the ebpf programs should be disabled
		if config.NetworkInterfaceConfigInPod.Interface.DisableEBPFPrograms != nil &&
			*config.NetworkInterfaceConfigInPod.Interface.DisableEBPFPrograms {
			klog.Infof("DEBUG runPodSandbox Pod %s/%s device=%q: disabling eBPF programs for ifNameInNs=%q", pod.Namespace, pod.Name, deviceName, ifNameInNs)
			err := detachEBPFPrograms(ns, ifNameInNs)
			if err != nil {
				klog.Infof("error disabling ebpf programs for %s in ns %s: %v", ifNameInNs, ns, err)
				return fmt.Errorf("error disabling ebpf programs for %s in ns %s: %v", ifNameInNs, ns, err)
			}
		} else {
			klog.Infof("DEBUG runPodSandbox Pod %s/%s device=%q: DisableEBPFPrograms not set or false, skipping eBPF detach", pod.Namespace, pod.Name, deviceName)
		}

		// Configure routes
		klog.Infof("DEBUG runPodSandbox Pod %s/%s device=%q: applying %d routes for ifNameInNs=%q", pod.Namespace, pod.Name, deviceName, len(config.NetworkInterfaceConfigInPod.Routes), ifNameInNs)
		err = applyRoutingConfig(ns, ifNameInNs, config.NetworkInterfaceConfigInPod.Routes)
		if err != nil {
			klog.Infof("RunPodSandbox error configuring device %s namespace %s routing: %v", deviceName, ns, err)
			return fmt.Errorf("error configuring device %s routes on namespace %s: %v", deviceName, ns, err)
		}

		// Configure rules
		klog.Infof("DEBUG runPodSandbox Pod %s/%s device=%q: applying %d rules", pod.Namespace, pod.Name, deviceName, len(config.NetworkInterfaceConfigInPod.Rules))
		err = applyRulesConfig(ns, config.NetworkInterfaceConfigInPod.Rules)
		if err != nil {
			klog.Infof("RunPodSandbox error configuring device %s namespace %s rules: %v", deviceName, ns, err)
			return fmt.Errorf("error configuring device %s rules on namespace %s: %v", deviceName, ns, err)
		}

		// Configure neighbors
		klog.Infof("DEBUG runPodSandbox Pod %s/%s device=%q: applying %d neighbors for ifNameInNs=%q", pod.Namespace, pod.Name, deviceName, len(config.NetworkInterfaceConfigInPod.Neighbors), ifNameInNs)
		err = applyNeighborConfig(ns, ifNameInNs, config.NetworkInterfaceConfigInPod.Neighbors)
		if err != nil {
			klog.Infof("RunPodSandbox for pod %s/%s (UID %s) failed to apply neighbor configuration for interface %s in namespace %s: %v", pod.Namespace, pod.Name, pod.Uid, ifNameInNs, ns, err)
			return fmt.Errorf("failed to apply neighbor configuration for interface %s in namespace %s: %w", ifNameInNs, ns, err)
		}

		resourceClaimStatusDevice.WithConditions(
			metav1apply.Condition().
				WithType("NetworkReady").
				WithStatus(metav1.ConditionTrue).
				WithReason("NetworkReady").
				WithLastTransitionTime(metav1.Now()),
		)

		// Move the RDMA device to the namespace if the host is in exclusive mode
		klog.Infof("DEBUG runPodSandbox Pod %s/%s device=%q: rdmaSharedMode=%v rdmaLinkDev=%q", pod.Namespace, pod.Name, deviceName, np.rdmaSharedMode, config.RDMADevice.LinkDev)
		if !np.rdmaSharedMode && config.RDMADevice.LinkDev != "" {
			klog.Infof("DEBUG runPodSandbox Pod %s/%s device=%q: moving RDMA device %q to ns=%q (exclusive mode)", pod.Namespace, pod.Name, deviceName, config.RDMADevice.LinkDev, ns)
			err := nsAttachRdmadev(config.RDMADevice.LinkDev, ns)
			if err != nil {
				klog.Infof("RunPodSandbox error getting RDMA device %s to namespace %s: %v", config.RDMADevice.LinkDev, ns, err)
				return fmt.Errorf("error moving RDMA device %s to namespace %s: %v", config.RDMADevice.LinkDev, ns, err)
			}
			resourceClaimStatusDevice.WithConditions(
				metav1apply.Condition().
					WithType("RDMALinkReady").
					WithStatus(metav1.ConditionTrue).
					WithReason("RDMALinkReady").
					WithLastTransitionTime(metav1.Now()),
			)
		}
		// Ok
		resourceClaimStatus.WithDevices(resourceClaimStatusDevice)
	}
	// do not block the handler to update the status
	for claim, status := range statusUpdates {
		resourceClaimApply := resourceapply.ResourceClaim(claim.Name, claim.Namespace).WithStatus(status)
		go func() {
			ctxStatus, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err := np.kubeClient.ResourceV1().ResourceClaims(claim.Namespace).ApplyStatus(ctxStatus,
				resourceClaimApply,
				metav1.ApplyOptions{FieldManager: np.driverName, Force: true},
			)
			if err != nil {
				klog.Infof("failed to update status for claim %s/%s : %v", claim.Namespace, claim.Name, err)
			} else {
				klog.V(4).Infof("updated status for claim %s/%s", claim.Namespace, claim.Name)
			}
		}()
	}

	return nil
}

// StopPodSandbox tries to move back the devices to the rootnamespace but does not fail
// to avoid disrupting the pod shutdown. The kernel will do the cleanup once the namespace
// is deleted.
func (np *NetworkDriver) StopPodSandbox(ctx context.Context, pod *api.PodSandbox) error {
	klog.Infof("DEBUG StopPodSandbox ENTER Pod %s/%s UID %s", pod.Namespace, pod.Name, pod.Uid)
	start := time.Now()
	status := statusNoop
	defer func() {
		nriPluginRequestsTotal.WithLabelValues(methodStopPodSandbox, status).Inc()
		klog.Infof("DEBUG StopPodSandbox EXIT Pod %s/%s UID %s status=%s took %v", pod.Namespace, pod.Name, pod.Uid, status, time.Since(start))
		nriPluginRequestsLatencySeconds.WithLabelValues(methodStopPodSandbox, status).Observe(time.Since(start).Seconds())
	}()

	podUID := types.UID(pod.GetUid())
	klog.Infof("DEBUG StopPodSandbox Pod %s/%s: podUID=%s", pod.Namespace, pod.Name, podUID)

	// Check upstream DRA config store.
	podConfig, hasDRAConfig := np.podConfigStore.GetPodConfigs(podUID)
	klog.Infof("DEBUG StopPodSandbox Pod %s/%s: podConfigStore.GetPodConfigs returned hasDRAConfig=%v, len(podConfig)=%d", pod.Namespace, pod.Name, hasDRAConfig, len(podConfig))
	for devName, cfg := range podConfig {
		klog.Infof("DEBUG StopPodSandbox Pod %s/%s: DRA device=%q claim=%s/%s hostIface=%q podIface=%q rdmaLinkDev=%q",
			pod.Namespace, pod.Name, devName, cfg.Claim.Namespace, cfg.Claim.Name,
			cfg.NetworkInterfaceConfigInHost.Interface.Name, cfg.NetworkInterfaceConfigInPod.Interface.Name, cfg.RDMADevice.LinkDev)
	}

	// Check SwiftV2 config store.
	swiftV2Configs := np.swiftV2Store.Get(podUID)
	klog.Infof("DEBUG StopPodSandbox Pod %s/%s: swiftV2Store.Get returned %d configs (nil=%v)",
		pod.Namespace, pod.Name, len(swiftV2Configs), swiftV2Configs == nil)

	if !hasDRAConfig && swiftV2Configs == nil {
		klog.Infof("DEBUG StopPodSandbox Pod %s/%s UID %s: NO config from either store, returning no-op",
			pod.Namespace, pod.Name, pod.Uid)
		return nil
	}

	// Process upstream DRA cleanup if present.
	if hasDRAConfig {
		klog.Infof("DEBUG StopPodSandbox Pod %s/%s: entering DRA path (stopPodSandbox) with %d devices", pod.Namespace, pod.Name, len(podConfig))
		err := np.stopPodSandbox(ctx, pod, podConfig)
		if err != nil {
			klog.Infof("DEBUG StopPodSandbox Pod %s/%s: DRA path FAILED: %v", pod.Namespace, pod.Name, err)
			status = statusFailed
		} else {
			klog.Infof("DEBUG StopPodSandbox Pod %s/%s: DRA path succeeded", pod.Namespace, pod.Name)
			status = statusSuccess
		}
	}

	// Process SwiftV2 cleanup if present.
	// Note: we do NOT delete the SwiftV2 store entry here. Containerd may
	// tear down and retry the sandbox (StopPodSandbox → RunPodSandbox) without
	// re-calling PrepareResourceClaims. The store entry is cleaned up in
	// UnprepareResourceClaims, which is called when the pod is actually deleted.
	if swiftV2Configs != nil {
		klog.Infof("DEBUG StopPodSandbox Pod %s/%s: entering SwiftV2 path (stopPodSandboxSwiftV2) with %d configs", pod.Namespace, pod.Name, len(swiftV2Configs))
		np.stopPodSandboxSwiftV2(pod, swiftV2Configs)
		klog.Infof("DEBUG StopPodSandbox Pod %s/%s: SwiftV2 path completed", pod.Namespace, pod.Name)
		status = statusSuccess
	}

	return nil
}

func (np *NetworkDriver) stopPodSandbox(_ context.Context, pod *api.PodSandbox, podConfig map[string]PodConfig) error {
	klog.Infof("DEBUG stopPodSandbox ENTER Pod %s/%s UID %s with %d device configs", pod.Namespace, pod.Name, pod.Uid, len(podConfig))
	defer func() {
		klog.Infof("DEBUG stopPodSandbox Pod %s/%s: removing netns mapping for podKey=%q", pod.Namespace, pod.Name, podKey(pod))
		np.netdb.RemovePodNetNs(podKey(pod))
	}()
	// get the pod network namespace
	ns := getNetworkNamespace(pod)
	klog.Infof("DEBUG stopPodSandbox Pod %s/%s: getNetworkNamespace returned %q", pod.Namespace, pod.Name, ns)
	if ns == "" {
		// some version of containerd does not send the network namespace information on this hook so
		// we workaround it using the local copy we have in the db to associate interfaces with Pods via
		// the network namespace id.
		ns = np.netdb.GetPodNetNs(podKey(pod))
		klog.Infof("DEBUG stopPodSandbox Pod %s/%s: fallback netdb.GetPodNetNs(%q) returned %q", pod.Namespace, pod.Name, podKey(pod), ns)
		if ns == "" {
			klog.Infof("DEBUG stopPodSandbox Pod %s/%s: no netns found from pod or db, assuming host network, skipping", pod.Namespace, pod.Name)
			return nil
		}
	}
	for deviceName, config := range podConfig {
		klog.Infof("DEBUG stopPodSandbox Pod %s/%s: detaching device=%q podIface=%q hostIface=%q from ns=%q",
			pod.Namespace, pod.Name, deviceName, config.NetworkInterfaceConfigInPod.Interface.Name, config.NetworkInterfaceConfigInHost.Interface.Name, ns)
		if err := nsDetachNetdev(ns, config.NetworkInterfaceConfigInPod.Interface.Name, config.NetworkInterfaceConfigInHost.Interface.Name); err != nil {
			klog.Infof("fail to return network device %s : %v", deviceName, err)
		}

		klog.Infof("DEBUG stopPodSandbox Pod %s/%s device=%q: rdmaSharedMode=%v rdmaLinkDev=%q", pod.Namespace, pod.Name, deviceName, np.rdmaSharedMode, config.RDMADevice.LinkDev)
		if !np.rdmaSharedMode && config.RDMADevice.LinkDev != "" {
			klog.Infof("DEBUG stopPodSandbox Pod %s/%s device=%q: detaching RDMA device %q from ns=%q", pod.Namespace, pod.Name, deviceName, config.RDMADevice.LinkDev, ns)
			if err := nsDetachRdmadev(ns, config.RDMADevice.LinkDev); err != nil {
				klog.Infof("fail to return rdma device %s : %v", deviceName, err)
			}
		}
	}
	klog.Infof("DEBUG stopPodSandbox EXIT Pod %s/%s: all devices processed", pod.Namespace, pod.Name)
	return nil
}

func (np *NetworkDriver) RemovePodSandbox(ctx context.Context, pod *api.PodSandbox) error {
	klog.V(2).Infof("RemovePodSandbox Pod %s/%s UID %s", pod.Namespace, pod.Name, pod.Uid)
	start := time.Now()
	status := statusNoop
	defer func() {
		nriPluginRequestsTotal.WithLabelValues(methodRemovePodSandbox, status).Inc()
		nriPluginRequestsLatencySeconds.WithLabelValues(methodRemovePodSandbox, status).Observe(time.Since(start).Seconds())
	}()
	if _, ok := np.podConfigStore.GetPodConfigs(types.UID(pod.GetUid())); !ok {
		return nil
	}
	err := np.removePodSandbox(ctx, pod)
	if err != nil {
		status = statusFailed
	} else {
		status = statusSuccess
	}
	return err
}

func (np *NetworkDriver) removePodSandbox(_ context.Context, pod *api.PodSandbox) error {
	np.netdb.RemovePodNetNs(podKey(pod))
	return nil
}

func (np *NetworkDriver) Shutdown(_ context.Context) {
	klog.Info("Runtime shutting down...")
}

func getNetworkNamespace(pod *api.PodSandbox) string {
	// get the pod network namespace
	for _, namespace := range pod.Linux.GetNamespaces() {
		if namespace.Type == "network" {
			return namespace.Path
		}
	}
	return ""
}

func podKey(pod *api.PodSandbox) string {
	return fmt.Sprintf("%s/%s", pod.GetNamespace(), pod.GetName())
}
