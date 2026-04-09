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
	consumers := make([]podConsumer, 0, len(claim.Status.ReservedFor))
	for _, reserved := range claim.Status.ReservedFor {
		if reserved.Resource != "pods" || reserved.APIGroup != "" {
			continue
		}
		consumers = append(consumers, podConsumer{
			UID:       reserved.UID,
			Name:      reserved.Name,
			Namespace: claim.Namespace,
		})
	}
	return consumers
}

func (np *NetworkDriver) getPodGoalState(ctx context.Context, pod podConsumer, cache map[types.UID][]cnsclient.PodIPInfo) ([]cnsclient.PodIPInfo, error) {
	if infos, ok := cache[pod.UID]; ok {
		return infos, nil
	}

	infos, err := np.cnsClient.GetPodGoalState(ctx, pod.Name, pod.Namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to get CNS goal state for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	cache[pod.UID] = infos
	return infos, nil
}

func (np *NetworkDriver) populateSwiftV2StoreForDevice(ctx context.Context, pod podConsumer, deviceName, deviceMAC string, cache map[types.UID][]cnsclient.PodIPInfo) error {
	if np.cnsClient == nil {
		return nil
	}

	infos, err := np.getPodGoalState(ctx, pod, cache)
	if err != nil {
		return err
	}

	info, found := findPodIPInfoByMAC(infos, deviceMAC)
	if !found {
		klog.V(4).Infof("SwiftV2 PrepareResourceClaim: no CNS goal state matched pod %s/%s device %s MAC %s",
			pod.Namespace, pod.Name, deviceName, deviceMAC)
		return nil
	}

	cfg, err := buildSwiftV2PodConfig(pod.UID, info)
	if err != nil {
		return fmt.Errorf("failed to build SwiftV2 config for pod %s/%s device %s: %w", pod.Namespace, pod.Name, deviceName, err)
	}

	if np.swiftV2Store == nil {
		np.swiftV2Store = NewSwiftV2PodConfigStore()
	}
	np.swiftV2Store.Set(pod.UID, deviceName, cfg)
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
		cfg.NIC.SubnetPrefix = prefixLength
		cfg.NIC.PodUID = string(podUID)
		cfg.InterfaceConfig.Interface.Addresses = []string{fmt.Sprintf("%s/32", info.PodIPConfig.IPAddress)}
		return cfg, nil
	}

	cfg.Mode = NICModeDedicated
	cfg.NIC.Addresses = []string{addressCIDR}
	return cfg, nil
}
