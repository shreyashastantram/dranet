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

	"github.com/containerd/nri/pkg/api"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

var nsAttachSecondaryNICHook = nsAttachSecondaryNIC
var cleanupExclusiveNICHook = cleanupExclusiveNIC

// runPodSandboxSecondaryNICs processes secondary NIC devices for a pod during
// the NRI RunPodSandbox hook. It handles both shared NIC (IPVLAN L3) and
// exclusive NIC (physical NIC move) configurations.
//
// For shared NIC: ensures the parent NIC is enslaved to a per-MAC host VRF,
// creates an IPVLAN L3 child, moves it into the pod namespace, and configures
// pod IP/routes/neighbors.
//
// For exclusive NIC: moves the NIC into the pod namespace, brings it up,
// assigns IP addresses, and configures routes. If the NIC is currently a
// shared parent (enslaved to a host VRF), it is first detached from the VRF
// and the shared routing state is torn down. It then issues DHCP discover
// synchronously for wireserver DNS mapping; failure aborts the attach so the
// NIC is returned to the host and plumbing can be retried.
//
// Claim status reporting (patching ResourceClaim status.devices) is handled by
// the DRA wiring, not here.
func (np *NetworkDriver) runPodSandboxSecondaryNICs(_ context.Context, pod *api.PodSandbox, configs map[string]SecondaryNICPodConfig) error {
	ns := getNetworkNamespace(pod)
	if ns == "" {
		return fmt.Errorf("secondary NIC RunPodSandbox: pod %s/%s using host network cannot claim network devices", pod.Namespace, pod.Name)
	}
	if len(configs) > 1 {
		return fmt.Errorf("secondary NIC RunPodSandbox: pod %s/%s has %d secondary NIC devices; only one is supported",
			pod.Namespace, pod.Name, len(configs))
	}

	for deviceName, cfg := range configs {
		switch cfg.Mode {
		case NICModeShared:
			if cfg.NIC.MAC == "" {
				return fmt.Errorf("secondary NIC RunPodSandbox: device %s has shared mode but empty MAC", deviceName)
			}
			klog.V(2).Infof("secondary NIC RunPodSandbox: attaching shared IPVLAN L3 via host VRF for device %s (MAC %s, pod IP %s) on pod %s/%s",
				deviceName, cfg.NIC.MAC, cfg.NIC.PodIP, pod.Namespace, pod.Name)

			networkData, err := nsAttachSecondaryNICHook(cfg.Mode, &cfg.NIC, ns)
			if err != nil {
				return fmt.Errorf("secondary NIC RunPodSandbox: failed to attach shared IPVLAN L3 for device %s: %w", deviceName, err)
			}
			klog.V(2).Infof("secondary NIC RunPodSandbox: attached shared NIC %s as %s with IPs %v on pod %s/%s",
				deviceName, networkData.InterfaceName, networkData.IPs, pod.Namespace, pod.Name)

		case NICModeExclusive:
			if cfg.NIC.MAC == "" {
				return fmt.Errorf("secondary NIC RunPodSandbox: device %s has exclusive mode but empty MAC", deviceName)
			}
			if np.secondaryNICStore != nil && np.secondaryNICStore.HasSharedPodForMAC(cfg.NIC.MAC) {
				return fmt.Errorf("secondary NIC RunPodSandbox: cannot attach exclusive device %s (MAC %s): a shared pod still uses this NIC",
					deviceName, cfg.NIC.MAC)
			}

			// NRI owns the exclusive NIC move: always plumb. nsAttachExclusiveNIC
			// detaches the NIC from any shared-parent VRF and tears down the shared
			// routing state before moving it into the pod, so a NIC previously used
			// by shared pods can be reclaimed for exclusive use.
			klog.V(2).Infof("secondary NIC RunPodSandbox: attaching exclusive NIC %s (MAC %s) to pod %s/%s",
				deviceName, cfg.NIC.MAC, pod.Namespace, pod.Name)

			networkData, err := nsAttachSecondaryNICHook(cfg.Mode, &cfg.NIC, ns)
			if err != nil {
				return fmt.Errorf("secondary NIC RunPodSandbox: failed to attach exclusive NIC for device %s: %w", deviceName, err)
			}
			klog.V(2).Infof("secondary NIC RunPodSandbox: attached exclusive NIC %s as %s with IPs %v on pod %s/%s",
				deviceName, networkData.InterfaceName, networkData.IPs, pod.Namespace, pod.Name)

		default:
			return fmt.Errorf("secondary NIC RunPodSandbox: device %s has unsupported NIC mode %q", deviceName, cfg.Mode)
		}
	}

	return nil
}

// stopPodSandboxSecondaryNICs cleans up secondary NIC devices for a pod during
// the NRI StopPodSandbox hook. This is best-effort — errors are logged but
// not returned to avoid disrupting pod shutdown.
//
// For shared NIC: no-op — the datapath keeps no per-pod host state. The IPVLAN
// sub-interface is destroyed with the pod's network namespace, and parent VRF
// state remains shared by later pods on the same NIC.
//
// For exclusive NIC: moves the physical NIC back to the host namespace.
// If the namespace is already gone, the kernel has returned the NIC automatically.
func (np *NetworkDriver) stopPodSandboxSecondaryNICs(pod *api.PodSandbox, configs map[string]SecondaryNICPodConfig) {
	ns := getNetworkNamespace(pod)
	if ns == "" && np.podConfigStore != nil {
		if podConfig, ok := np.podConfigStore.GetPodConfig(types.UID(pod.GetUid())); ok {
			ns = podConfig.NetNS
		}
	}

	for deviceName, cfg := range configs {
		switch cfg.Mode {
		case NICModeShared:
			// The IPVLAN child is destroyed with the pod network namespace. Keep
			// the shared parent VRF state in place for other pods using this NIC.

		case NICModeExclusive:
			if cfg.NIC.MAC == "" {
				klog.Warningf("secondary NIC StopPodSandbox: exclusive device %s has empty MAC; skipping explicit NIC cleanup", deviceName)
				continue
			}
			if ns == "" {
				klog.V(2).Infof("secondary NIC StopPodSandbox: no netns for exclusive NIC %s (MAC %s), kernel will return it",
					deviceName, cfg.NIC.MAC)
				continue
			}
			klog.V(2).Infof("secondary NIC StopPodSandbox: returning exclusive NIC %s (MAC %s) to host for pod %s/%s",
				deviceName, cfg.NIC.MAC, pod.Namespace, pod.Name)
			cleanupExclusiveNICHook(ns, cfg.NIC.MAC)

		default:
			klog.Warningf("secondary NIC StopPodSandbox: device %s has unsupported NIC mode %q; skipping cleanup", deviceName, cfg.Mode)
		}
	}

}
