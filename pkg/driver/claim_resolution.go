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
	"net"
	"strings"

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

func (np *NetworkDriver) getPodGoalState(ctx context.Context, pod podConsumer, cache map[types.UID][]cnsclient.PodIPInfo) ([]cnsclient.PodIPInfo, error) {
	if infos, ok := cache[pod.UID]; ok {
		klog.Infof("getPodGoalState: cache hit for pod %s/%s UID=%s (%d infos)", pod.Namespace, pod.Name, pod.UID, len(infos))
		return infos, nil
	}

	klog.Infof("getPodGoalState: calling CNS GetPodIPConfig for pod %s/%s UID=%s", pod.Namespace, pod.Name, pod.UID)
	infos, err := np.cnsClient.GetPodIPConfig(ctx, pod.Name, pod.Namespace, string(pod.UID))
	if err != nil {
		return nil, fmt.Errorf("failed to get CNS pod IP config for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	klog.Infof("getPodGoalState: CNS returned %d PodIPInfo entries for pod %s/%s", len(infos), pod.Namespace, pod.Name)
	for i, info := range infos {
		klog.Infof("getPodGoalState: PodIPInfo[%d]: NICType=%q MAC=%q InterfaceName=%q PodIP=%s/%d PrimaryIP=%s GW=%s SharedNIC=%v",
			i, info.NICType, info.MacAddress, info.InterfaceName,
			info.PodIPConfig.IPAddress, info.PodIPConfig.PrefixLength,
			info.NetworkContainerPrimaryIPConfig.PrimaryIP,
			info.NetworkContainerPrimaryIPConfig.GatewayIPAddress,
			info.SharedNIC)
	}
	cache[pod.UID] = infos
	return infos, nil
}

func (np *NetworkDriver) populateSwiftV2StoreForDevice(ctx context.Context, pod podConsumer, deviceName, deviceMAC string, claimKey types.NamespacedName, shareID string, cache map[types.UID][]cnsclient.PodIPInfo) error {
	if np.cnsClient == nil {
		klog.Infof("populateSwiftV2StoreForDevice: cnsClient is nil, skipping for pod %s/%s device %s", pod.Namespace, pod.Name, deviceName)
		return nil
	}

	klog.Infof("populateSwiftV2StoreForDevice: fetching goal state for pod %s/%s device=%q MAC=%q", pod.Namespace, pod.Name, deviceName, deviceMAC)
	infos, err := np.getPodGoalState(ctx, pod, cache)
	if err != nil {
		return err
	}

	klog.Infof("populateSwiftV2StoreForDevice: matching MAC=%q against %d CNS PodIPInfo entries", deviceMAC, len(infos))
	for i, info := range infos {
		klog.Infof("populateSwiftV2StoreForDevice: candidate[%d] MAC=%q NICType=%q InterfaceName=%q",
			i, info.MacAddress, info.NICType, info.InterfaceName)
	}

	info, found := findPodIPInfoByMAC(infos, deviceMAC)
	if !found {
		return fmt.Errorf("no CNS PodIPInfo matched pod %s/%s device %s MAC %s — kubelet will retry",
			pod.Namespace, pod.Name, deviceName, deviceMAC)
	}
	klog.Infof("populateSwiftV2StoreForDevice: matched MAC=%q -> NICType=%q PodIP=%s/%d GW=%s",
		deviceMAC, info.NICType, info.PodIPConfig.IPAddress, info.PodIPConfig.PrefixLength,
		info.NetworkContainerPrimaryIPConfig.GatewayIPAddress)

	// Validate required fields from CNS PodIPConfig response.
	// If any are missing, fail so kubelet retries the prepare call.
	if err := validatePodIPInfo(info); err != nil {
		return fmt.Errorf("CNS PodIPConfig validation failed for pod %s/%s device %s: %w — kubelet will retry",
			pod.Namespace, pod.Name, deviceName, err)
	}

	cfg, err := buildSwiftV2PodConfig(pod.UID, info)
	if err != nil {
		return fmt.Errorf("failed to build SwiftV2 config for pod %s/%s device %s: %w", pod.Namespace, pod.Name, deviceName, err)
	}
	cfg.Claim = claimKey
	cfg.ShareID = shareID

	if np.swiftV2Store == nil {
		np.swiftV2Store = NewSwiftV2PodConfigStore()
	}
	np.swiftV2Store.Set(pod.UID, deviceName, cfg, claimKey)
	klog.Infof("populateSwiftV2StoreForDevice: stored SwiftV2 config for pod %s/%s UID=%s device=%q", pod.Namespace, pod.Name, pod.UID, deviceName)
	return nil
}

func findPodIPInfoByMAC(infos []cnsclient.PodIPInfo, mac string) (cnsclient.PodIPInfo, bool) {
	normalizedMAC, ok := normalizeMAC(mac)
	if !ok {
		return cnsclient.PodIPInfo{}, false
	}

	for _, info := range infos {
		candidateMAC, valid := normalizeMAC(info.MacAddress)
		if !valid || candidateMAC != normalizedMAC {
			continue
		}
		if info.PodIPConfig.IPAddress == "" {
			continue
		}
		return info, true
	}

	return cnsclient.PodIPInfo{}, false
}

func normalizeMAC(mac string) (string, bool) {
	parsed, err := net.ParseMAC(mac)
	if err != nil {
		return "", false
	}
	return strings.ToLower(parsed.String()), true
}

func buildSwiftV2PodConfig(podUID types.UID, info cnsclient.PodIPInfo) (SwiftV2PodConfig, error) {
	if info.MacAddress == "" {
		return SwiftV2PodConfig{}, fmt.Errorf("missing MAC address")
	}
	if info.PodIPConfig.IPAddress == "" {
		return SwiftV2PodConfig{}, fmt.Errorf("missing pod IP address")
	}

	gatewayIP := info.NetworkContainerPrimaryIPConfig.GatewayIPAddress
	if gatewayIP == "" {
		gatewayIP = swiftV2VirtualGW
	}

	prefixLength := int(info.PodIPConfig.PrefixLength)
	if prefixLength == 0 {
		prefixLength = 32
	}
	addressCIDR := fmt.Sprintf("%s/%d", info.PodIPConfig.IPAddress, prefixLength)

	cfg := SwiftV2PodConfig{
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

	cfg.Mode = NICModeDedicated
	cfg.NIC.Addresses = []string{addressCIDR}
	return cfg, nil
}

// validatePodIPInfo checks that a CNS PodIPInfo has the required fields
// (MAC address and IP address) needed to configure pod networking. If any
// required field is missing, it returns an error so the prepare call fails
// and kubelet retries. Gateway is not strictly required because
// buildSwiftV2PodConfig falls back to the SwiftV2 virtual gateway.
func validatePodIPInfo(info cnsclient.PodIPInfo) error {
	if info.MacAddress == "" {
		return fmt.Errorf("missing MAC address in CNS PodIPConfig response")
	}
	if info.PodIPConfig.IPAddress == "" {
		return fmt.Errorf("missing IP address in CNS PodIPConfig response")
	}
	return nil
}
