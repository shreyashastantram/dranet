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
	"fmt"

	"github.com/containerd/nri/pkg/api"
	"k8s.io/klog/v2"
)

var (
	nsAttachSwiftV2NICHook = nsAttachSwiftV2NIC
	nicExistsInNetnsHook   = nicExistsInNetns
)

// runPodSandboxSwiftV2 processes SwiftV2-managed devices for a pod during
// the NRI RunPodSandbox hook. It handles both shared NIC (ipvlan L3) and
// dedicated NIC (physical NIC move) configurations.
//
// For shared NIC: creates an ipvlan L3 sub-interface, adds host /32 route,
// moves interface into pod namespace, configures IP and routes.
//
// For dedicated NIC: checks if the NIC already exists in the pod namespace
// (CNI may have already plumbed it). If not, moves the NIC into the pod
// namespace, brings it up, assigns IP addresses, and configures routes.
// Issues a background DHCP discover for wireserver DNS mapping.
func (np *NetworkDriver) runPodSandboxSwiftV2(pod *api.PodSandbox, configs map[string]SwiftV2PodConfig) error {
	ns := getNetworkNamespace(pod)
	if ns == "" {
		return fmt.Errorf("SwiftV2 RunPodSandbox: pod %s/%s using host network cannot claim network devices", pod.Namespace, pod.Name)
	}

	for deviceName, cfg := range configs {
		switch cfg.Mode {
		case NICModeShared:
			if cfg.NIC.MAC == "" {
				klog.Warningf("SwiftV2 RunPodSandbox: device %s has shared mode but empty MAC, skipping", deviceName)
				continue
			}
			klog.V(2).Infof("SwiftV2 RunPodSandbox: attaching shared ipvlan L3 for device %s (MAC %s, pod IP %s) on pod %s/%s",
				deviceName, cfg.NIC.MAC, cfg.NIC.PodIP, pod.Namespace, pod.Name)

			networkData, err := nsAttachSwiftV2NICHook(cfg.Mode, &cfg.NIC, ns)
			if err != nil {
				return fmt.Errorf("SwiftV2 RunPodSandbox: failed to attach ipvlan L3 for device %s: %w", deviceName, err)
			}
			klog.V(2).Infof("SwiftV2 RunPodSandbox: attached shared NIC %s as %s with IPs %v on pod %s/%s",
				deviceName, networkData.InterfaceName, networkData.IPs, pod.Namespace, pod.Name)

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

		default:
			klog.Warningf("SwiftV2 RunPodSandbox: unknown NIC mode %q for device %s, skipping", cfg.Mode, deviceName)
		}
	}
	return nil
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
