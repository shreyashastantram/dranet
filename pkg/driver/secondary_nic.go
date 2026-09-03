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
	"errors"
	"fmt"
	"net"

	"github.com/vishvananda/netlink"
	resourceapi "k8s.io/api/resource/v1"
	"sigs.k8s.io/dranet/internal/nlwrap"
)

const (
	secondaryNICVirtualGateway = "169.254.2.1"

	// Shared secondary NICs use eth1 because eth0 is installed by the cluster
	// CNI before the NRI hook runs. One-pod exclusive NICs retain their host name.
	sharedNICInterfacePrefix = "eth"

	// Linux interface names are capped at 15 bytes: sv2 plus normalized MAC.
	sharedNICVRFPrefix = "sv2"

	secondaryNICSDNGatewayMAC = "12:34:56:78:9a:bc"
)

// findLinkByMAC returns the host-network-namespace link whose hardware address
// matches mac. The lookup is name-independent because secondary NIC names are
// not stable across all ownership states.
func findLinkByMAC(mac string) (netlink.Link, error) {
	targetMAC, err := net.ParseMAC(mac)
	if err != nil {
		return nil, fmt.Errorf("invalid MAC address %s: %w", mac, err)
	}

	links, err := nlwrap.LinkList()
	if err != nil {
		return nil, fmt.Errorf("failed to list host links: %w", err)
	}
	for _, link := range links {
		// IPVLAN children inherit the parent's MAC, so never return one when the
		// caller is looking for the host-visible physical NIC.
		if link.Attrs() != nil && link.Type() != "ipvlan" && link.Attrs().HardwareAddr.String() == targetMAC.String() {
			return link, nil
		}
	}
	return nil, fmt.Errorf("physical NIC with MAC %s not found on host", targetMAC)
}

// parseSecondaryNICGateway validates that gatewayIP is the IPv4 virtual gateway
// supported by secondary NICs and returns its four-byte representation.
func parseSecondaryNICGateway(gatewayIP, mode string) (net.IP, error) {
	parsed := net.ParseIP(gatewayIP)
	if parsed == nil {
		return nil, fmt.Errorf("invalid gateway IP: %s", gatewayIP)
	}
	parsedIPv4 := parsed.To4()
	if parsedIPv4 == nil {
		return nil, fmt.Errorf("secondary NIC %s currently requires an IPv4 gateway IP, got %s", mode, gatewayIP)
	}
	expected := net.ParseIP(secondaryNICVirtualGateway).To4()
	if !parsedIPv4.Equal(expected) {
		return nil, fmt.Errorf("unsupported secondary NIC gateway IP %s, expected %s", gatewayIP, secondaryNICVirtualGateway)
	}
	return parsedIPv4, nil
}

// isLinkNotFound reports whether err is netlink's typed link-not-found error.
func isLinkNotFound(err error) bool {
	var notFound netlink.LinkNotFoundError
	return errors.As(err, &notFound)
}

// nsAttachSecondaryNIC dispatches attach behavior by NIC ownership mode.
func nsAttachSecondaryNIC(mode NICMode, cfg *NICConfig, containerNsPath string) (*resourceapi.NetworkDeviceData, error) {
	switch mode {
	case NICModeShared:
		return nsAttachIPVlanL3(cfg, containerNsPath)
	case NICModeExclusive:
		return nsAttachExclusiveNIC(cfg, containerNsPath)
	default:
		return nil, fmt.Errorf("unsupported secondary NIC mode %q", mode)
	}
}
