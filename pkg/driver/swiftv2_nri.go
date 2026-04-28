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
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	metav1apply "k8s.io/client-go/applyconfigurations/meta/v1"
	resourceapply "k8s.io/client-go/applyconfigurations/resource/v1"
	"k8s.io/klog/v2"
)

var (
	nsAttachSwiftV2NICHook = nsAttachSwiftV2NIC
	nicExistsInNetnsHook   = nicExistsInNetns
)

// runPodSandboxSwiftV2 processes SwiftV2-managed devices for a pod during
// the NRI RunPodSandbox hook. It handles both shared NIC (ipvlan L3S) and
// dedicated NIC (physical NIC move) configurations.
//
// For shared NIC: creates an ipvlan L3S sub-interface, adds host /32 route,
// moves interface into pod namespace, configures IP and routes.
//
// For dedicated NIC: checks if the NIC already exists in the pod namespace
// (CNI may have already plumbed it). If not, moves the NIC into the pod
// namespace, brings it up, assigns IP addresses, and configures routes.
// Issues a background DHCP discover for wireserver DNS mapping.
//
// After successful attachment, it patches the corresponding ResourceClaim's
// status.devices[*] with the runtime-observed network data (interface name,
// MAC, IPs) and Ready/NetworkReady conditions, mirroring the upstream
// inventory path so consumers can discover pod NIC state via the standard
// DRA API surface.
func (np *NetworkDriver) runPodSandboxSwiftV2(ctx context.Context, pod *api.PodSandbox, configs map[string]SwiftV2PodConfig) error {
	ns := getNetworkNamespace(pod)
	if ns == "" {
		return fmt.Errorf("SwiftV2 RunPodSandbox: pod %s/%s using host network cannot claim network devices", pod.Namespace, pod.Name)
	}

	// Track ResourceClaim status updates keyed by claim. Multiple devices on the
	// same pod may belong to the same claim; merge their AllocatedDeviceStatus
	// entries into a single apply config per claim.
	statusUpdates := map[types.NamespacedName]*resourceapply.ResourceClaimStatusApplyConfiguration{}

	for deviceName, cfg := range configs {
		switch cfg.Mode {
		case NICModeShared:
			if cfg.NIC.MAC == "" {
				klog.Warningf("SwiftV2 RunPodSandbox: device %s has shared mode but empty MAC, skipping", deviceName)
				continue
			}
			klog.V(2).Infof("SwiftV2 RunPodSandbox: attaching shared ipvlan L3S for device %s (MAC %s, pod IP %s) on pod %s/%s",
				deviceName, cfg.NIC.MAC, cfg.NIC.PodIP, pod.Namespace, pod.Name)

			networkData, err := nsAttachSwiftV2NICHook(cfg.Mode, &cfg.NIC, ns)
			if err != nil {
				return fmt.Errorf("SwiftV2 RunPodSandbox: failed to attach ipvlan L3S for device %s: %w", deviceName, err)
			}
			klog.V(2).Infof("SwiftV2 RunPodSandbox: attached shared NIC %s as %s with IPs %v on pod %s/%s",
				deviceName, networkData.InterfaceName, networkData.IPs, pod.Namespace, pod.Name)

			np.recordSwiftV2DeviceStatus(statusUpdates, cfg.Claim, deviceName, networkData)

		case NICModeDedicated:
			if cfg.NIC.MAC == "" {
				klog.Warningf("SwiftV2 RunPodSandbox: device %s has dedicated mode but empty MAC, skipping", deviceName)
				continue
			}

			// Idempotent gate for dedicated NIC migration:
			// - If NIC is already in the pod namespace, do nothing.
			// - If NIC is not in the pod namespace, proceed to move and configure it.
			// This ensures NRI never re-plumbs a NIC that has already been moved.
			if nicExistsInNetnsHook(ns, cfg.NIC.MAC) {
				klog.V(2).Infof("SwiftV2 RunPodSandbox: dedicated NIC %s (MAC %s) already in pod netns, skipping (CNI plumbed)",
					deviceName, cfg.NIC.MAC)
				continue
			}

			klog.V(2).Infof("SwiftV2 RunPodSandbox: attaching dedicated NIC %s (MAC %s) to pod %s/%s",
				deviceName, cfg.NIC.MAC, pod.Namespace, pod.Name)

			networkData, err := nsAttachSwiftV2NICHook(cfg.Mode, &cfg.NIC, ns)
			if err != nil {
				return fmt.Errorf("SwiftV2 RunPodSandbox: failed to attach dedicated NIC for device %s: %w", deviceName, err)
			}
			klog.V(2).Infof("SwiftV2 RunPodSandbox: attached dedicated NIC %s as %s with IPs %v on pod %s/%s",
				deviceName, networkData.InterfaceName, networkData.IPs, pod.Namespace, pod.Name)

			np.recordSwiftV2DeviceStatus(statusUpdates, cfg.Claim, deviceName, networkData)

		default:
			klog.Warningf("SwiftV2 RunPodSandbox: unknown NIC mode %q for device %s, skipping", cfg.Mode, deviceName)
		}
	}

	np.applySwiftV2ClaimStatusUpdates(statusUpdates)
	return nil
}

// recordSwiftV2DeviceStatus appends an AllocatedDeviceStatus entry for a
// SwiftV2-attached device to the claim's status apply config, creating the
// claim entry on first device. networkData carries the runtime-observed
// interface name, MAC, and IPs returned by nsAttachSwiftV2NIC.
func (np *NetworkDriver) recordSwiftV2DeviceStatus(
	statusUpdates map[types.NamespacedName]*resourceapply.ResourceClaimStatusApplyConfiguration,
	claimKey types.NamespacedName,
	deviceName string,
	networkData *resourceapi.NetworkDeviceData,
) {
	if claimKey.Name == "" || np.kubeClient == nil || np.cnsDriverName == "" {
		return
	}

	claimStatus, ok := statusUpdates[claimKey]
	if !ok {
		claimStatus = resourceapply.ResourceClaimStatus()
		statusUpdates[claimKey] = claimStatus
	}

	now := metav1.Now()
	deviceStatus := resourceapply.
		AllocatedDeviceStatus().
		WithDevice(deviceName).
		WithDriver(np.cnsDriverName).
		WithPool(delegatedNICsPoolName).
		WithConditions(
			metav1apply.Condition().
				WithType("Ready").
				WithReason("NetworkDeviceReady").
				WithStatus(metav1.ConditionTrue).
				WithLastTransitionTime(now),
			metav1apply.Condition().
				WithType("NetworkReady").
				WithReason("NetworkReady").
				WithStatus(metav1.ConditionTrue).
				WithLastTransitionTime(now),
		).
		WithNetworkData(resourceapply.NetworkDeviceData().
			WithInterfaceName(networkData.InterfaceName).
			WithHardwareAddress(networkData.HardwareAddress).
			WithIPs(networkData.IPs...),
		)

	claimStatus.WithDevices(deviceStatus)
}

// applySwiftV2ClaimStatusUpdates patches each accumulated ResourceClaim
// status in a non-blocking goroutine with a short timeout, mirroring the
// upstream inventory NRI flow. Failures are logged but never block pod
// startup.
func (np *NetworkDriver) applySwiftV2ClaimStatusUpdates(
	statusUpdates map[types.NamespacedName]*resourceapply.ResourceClaimStatusApplyConfiguration,
) {
	if np.kubeClient == nil {
		return
	}
	for claim, status := range statusUpdates {
		claimApply := resourceapply.ResourceClaim(claim.Name, claim.Namespace).WithStatus(status)
		claim := claim
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err := np.kubeClient.ResourceV1().ResourceClaims(claim.Namespace).ApplyStatus(ctx,
				claimApply,
				metav1.ApplyOptions{FieldManager: np.cnsDriverName, Force: true},
			)
			if err != nil {
				klog.Infof("SwiftV2: failed to update status for claim %s/%s: %v", claim.Namespace, claim.Name, err)
				return
			}
			klog.V(4).Infof("SwiftV2: updated status for claim %s/%s", claim.Namespace, claim.Name)
		}()
	}
}

// stopPodSandboxSwiftV2 cleans up SwiftV2-managed devices for a pod during
// the NRI StopPodSandbox hook. This is best-effort — errors are logged but
// not returned to avoid disrupting pod shutdown.
//
// For shared NIC: removes the host-side /32 route. The ipvlan sub-interface
// is automatically destroyed with the pod's network namespace.
//
// For dedicated NIC: moves the physical NIC back to the host namespace.
// If the namespace is already gone, the kernel has returned the NIC automatically.
func (np *NetworkDriver) stopPodSandboxSwiftV2(pod *api.PodSandbox, configs map[string]SwiftV2PodConfig) {
	ns := getNetworkNamespace(pod)
	if ns == "" {
		ns = np.netdb.GetPodNetNs(podKey(pod))
	}

	for deviceName, cfg := range configs {
		switch cfg.Mode {
		case NICModeShared:
			if cfg.NIC.MAC == "" {
				continue
			}
			klog.V(2).Infof("SwiftV2 StopPodSandbox: cleaning up shared NIC for device %s on pod %s/%s",
				deviceName, pod.Namespace, pod.Name)
			cleanupIPVlanL3(&cfg.NIC)

		case NICModeDedicated:
			if cfg.NIC.MAC == "" {
				continue
			}
			if ns == "" {
				klog.V(2).Infof("SwiftV2 StopPodSandbox: no netns for dedicated NIC %s (MAC %s), kernel will return it",
					deviceName, cfg.NIC.MAC)
				continue
			}
			klog.V(2).Infof("SwiftV2 StopPodSandbox: returning dedicated NIC %s (MAC %s) to host for pod %s/%s",
				deviceName, cfg.NIC.MAC, pod.Namespace, pod.Name)
			cleanupDedicatedNIC(ns, cfg.NIC.MAC)
		}
	}

}
