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

	"github.com/vishvananda/netlink"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/dranet/pkg/apis"
	"sigs.k8s.io/dranet/pkg/cnsclient"
)

type podConsumer struct {
	UID       types.UID
	Name      string
	Namespace string
}

func getPodConsumers(claim *resourceapi.ResourceClaim) []podConsumer {
	klog.Infof("getPodConsumers: claim %s/%s UID=%s has %d ReservedFor entries",
		claim.Namespace, claim.Name, claim.UID, len(claim.Status.ReservedFor))
	consumers := make([]podConsumer, 0, len(claim.Status.ReservedFor))
	for i, reserved := range claim.Status.ReservedFor {
		klog.Infof("getPodConsumers: ReservedFor[%d]: Resource=%q APIGroup=%q Name=%q UID=%s",
			i, reserved.Resource, reserved.APIGroup, reserved.Name, reserved.UID)
		if reserved.Resource != "pods" || reserved.APIGroup != "" {
			klog.Infof("getPodConsumers: skipping ReservedFor[%d] (not a pod)", i)
			continue
		}
		consumers = append(consumers, podConsumer{
			UID:       reserved.UID,
			Name:      reserved.Name,
			Namespace: claim.Namespace,
		})
	}
	klog.Infof("getPodConsumers: returning %d pod consumers for claim %s/%s", len(consumers), claim.Namespace, claim.Name)
	return consumers
}

// claimGoalState is the CNS RequestClaimResourceInfo result for a claim: the
// owning pod's IP configs plus the resource-slice properties of its NICs.
type claimGoalState struct {
	podIPInfo    []cnsclient.PodIPInfo
	nicResources []cnsclient.NICResource
}

func (np *NetworkDriver) getClaimGoalState(ctx context.Context, claimUID types.UID, cache map[types.UID]claimGoalState) (claimGoalState, error) {
	if gs, ok := cache[claimUID]; ok {
		klog.Infof("getClaimGoalState: cache hit for claim UID=%s (%d infos, %d nics)", claimUID, len(gs.podIPInfo), len(gs.nicResources))
		return gs, nil
	}

	klog.Infof("getClaimGoalState: calling CNS GetClaimResourceInfo for claim UID=%s", claimUID)
	resp, err := np.cnsClient.GetClaimResourceInfo(ctx, string(claimUID))
	if err != nil {
		return claimGoalState{}, fmt.Errorf("failed to get CNS claim resource info for claim %s: %w", claimUID, err)
	}
	gs := claimGoalState{podIPInfo: resp.PodIPInfo, nicResources: resp.NICResources}
	klog.Infof("getClaimGoalState: CNS returned %d PodIPInfo entries and %d NIC resources for claim %s", len(gs.podIPInfo), len(gs.nicResources), claimUID)
	for i, info := range gs.podIPInfo {
		klog.Infof("getClaimGoalState: PodIPInfo[%d]: NICType=%q MAC=%q InterfaceName=%q PodIP=%s/%d PrimaryIP=%s GW=%s SharedNIC=%v",
			i, info.NICType, info.MacAddress, info.InterfaceName,
			info.PodIPConfig.IPAddress, info.PodIPConfig.PrefixLength,
			info.NetworkContainerPrimaryIPConfig.PrimaryIP,
			info.NetworkContainerPrimaryIPConfig.GatewayIPAddress,
			info.SharedNIC)
	}
	cache[claimUID] = gs
	return gs, nil
}

func (np *NetworkDriver) populateSecondaryNICStoreForDevice(ctx context.Context, claimUID types.UID, pod podConsumer, deviceName string, claimKey types.NamespacedName, shareID string, cache map[types.UID]claimGoalState) error {
	if np.cnsClient == nil {
		klog.Infof("populateSecondaryNICStoreForDevice: cnsClient is nil, skipping for pod %s/%s device %s", pod.Namespace, pod.Name, deviceName)
		return nil
	}

	klog.Infof("populateSecondaryNICStoreForDevice: fetching goal state for pod %s/%s device=%q", pod.Namespace, pod.Name, deviceName)
	gs, err := np.getClaimGoalState(ctx, claimUID, cache)
	if err != nil {
		return err
	}
	infos := gs.podIPInfo

	klog.Infof("populateSecondaryNICStoreForDevice: matching device %q against %d CNS PodIPInfo entries", deviceName, len(infos))
	for i, info := range infos {
		klog.Infof("populateSecondaryNICStoreForDevice: candidate[%d] MAC=%q NICType=%q InterfaceName=%q",
			i, info.MacAddress, info.NICType, info.InterfaceName)
	}

	info, found := findPodIPInfoByDeviceName(infos, deviceName)
	if !found {
		return fmt.Errorf("no CNS PodIPInfo matched pod %s/%s device %s — kubelet will retry",
			pod.Namespace, pod.Name, deviceName)
	}
	klog.Infof("populateSecondaryNICStoreForDevice: matched device %q -> MAC=%q NICType=%q PodIP=%s/%d GW=%s",
		deviceName, info.MacAddress, info.NICType, info.PodIPConfig.IPAddress, info.PodIPConfig.PrefixLength,
		info.NetworkContainerPrimaryIPConfig.GatewayIPAddress)

	// Validate required fields from CNS PodIPConfig response.
	// If any are missing, fail so kubelet retries the prepare call.
	if err := validatePodIPInfo(info); err != nil {
		return fmt.Errorf("CNS PodIPConfig validation failed for pod %s/%s device %s: %w — kubelet will retry",
			pod.Namespace, pod.Name, deviceName, err)
	}

	cfg, err := buildSecondaryNICPodConfig(pod.UID, info)
	if err != nil {
		return fmt.Errorf("failed to build secondary NIC config for pod %s/%s device %s: %w", pod.Namespace, pod.Name, deviceName, err)
	}
	cfg.Claim = claimKey
	cfg.ShareID = shareID

	if np.secondaryNICStore == nil {
		return fmt.Errorf("secondary NIC config store is not initialized")
	}
	if err := np.secondaryNICStore.Set(pod.UID, deviceName, cfg, claimKey); err != nil {
		return fmt.Errorf("persist secondary NIC config for pod %s/%s device %s: %w", pod.Namespace, pod.Name, deviceName, err)
	}
	klog.Infof("populateSecondaryNICStoreForDevice: stored secondary NIC config for pod %s/%s UID=%s device=%q", pod.Namespace, pod.Name, pod.UID, deviceName)
	return nil
}

// findPodIPInfoByDeviceName returns the PodIPInfo whose NIC maps to the given
// ResourceSlice device name. CNS device names are sanitizeMACForK8s(mac), so the
// match compares the sanitized MAC of each PodIPInfo entry to the device name.
func findPodIPInfoByDeviceName(infos []cnsclient.PodIPInfo, deviceName string) (cnsclient.PodIPInfo, bool) {
	for _, info := range infos {
		if info.PodIPConfig.IPAddress == "" {
			continue
		}
		if sanitizeMACForK8s(info.MacAddress) == deviceName {
			return info, true
		}
	}
	return cnsclient.PodIPInfo{}, false
}

func buildSecondaryNICPodConfig(podUID types.UID, info cnsclient.PodIPInfo) (SecondaryNICPodConfig, error) {
	if info.MacAddress == "" {
		return SecondaryNICPodConfig{}, fmt.Errorf("missing MAC address")
	}
	if info.PodIPConfig.IPAddress == "" {
		return SecondaryNICPodConfig{}, fmt.Errorf("missing pod IP address")
	}

	gatewayIP := info.NetworkContainerPrimaryIPConfig.GatewayIPAddress
	if gatewayIP == "" {
		gatewayIP = secondaryNICVirtualGateway
	}

	prefixLength := int(info.PodIPConfig.PrefixLength)
	if prefixLength == 0 {
		prefixLength = 32
	}
	addressCIDR := fmt.Sprintf("%s/%d", info.PodIPConfig.IPAddress, prefixLength)

	cfg := SecondaryNICPodConfig{
		NIC: NICConfig{
			MAC:       info.MacAddress,
			GatewayIP: gatewayIP,
		},
		InterfaceConfig: apis.NetworkConfig{
			Interface: apis.InterfaceConfig{
				Addresses: []string{addressCIDR},
			},
			Routes: []apis.RouteConfig{
				{Destination: gatewayIP + "/32", Scope: uint8(netlink.SCOPE_LINK)},
				{Destination: "0.0.0.0/0", Gateway: gatewayIP},
			},
		},
	}

	if info.SharedNIC {
		cfg.Mode = NICModeShared
		cfg.NIC.PodIP = info.PodIPConfig.IPAddress
		cfg.NIC.PodUID = string(podUID)
		cfg.InterfaceConfig.Interface.Addresses = []string{fmt.Sprintf("%s/32", info.PodIPConfig.IPAddress)}
		return cfg, nil
	}

	cfg.Mode = NICModeExclusive
	cfg.NIC.Addresses = []string{addressCIDR}
	return cfg, nil
}

// validatePodIPInfo checks that a CNS PodIPInfo has the required fields
// (MAC address and IP address) needed to configure pod networking. If any
// required field is missing, it returns an error so the prepare call fails
// and kubelet retries. Gateway is not strictly required because
// buildSecondaryNICPodConfig falls back to the secondary NIC virtual gateway.
func validatePodIPInfo(info cnsclient.PodIPInfo) error {
	if info.MacAddress == "" {
		return fmt.Errorf("missing MAC address in CNS PodIPConfig response")
	}
	if info.PodIPConfig.IPAddress == "" {
		return fmt.Errorf("missing IP address in CNS PodIPConfig response")
	}
	return nil
}
