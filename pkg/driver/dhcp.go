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
	"runtime"
	"time"

	"sigs.k8s.io/dranet/pkg/apis"

	"github.com/insomniacslk/dhcp/dhcpv4/nclient4"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"k8s.io/klog/v2"
	"sigs.k8s.io/dranet/internal/nlwrap"
)

// dhcpDiscoverTimeout bounds a single DHCP DISCOVER/OFFER exchange on the
// exclusive attach path.
const dhcpDiscoverTimeout = 3 * time.Second

func getDHCP(ctx context.Context, ifName string) (ip string, routes []apis.RouteConfig, err error) {
	link, err := nlwrap.LinkByName(ifName)
	if err != nil {
		return "", nil, err
	}
	if link.Attrs().OperState != netlink.OperUp {
		if err := netlink.LinkSetUp(link); err != nil {
			return "", nil, fmt.Errorf("failed to set interface %s up: %v", ifName, err)
		}
	}
	dhclient, err := nclient4.New(ifName)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create DHCP client on interface %s  up: %v", ifName, err)
	}
	defer dhclient.Close()

	lease, err := dhclient.Request(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("failed to obtain DHCP lease on interface %s  up: %v", ifName, err)
	}
	if lease.ACK == nil {
		return "", nil, fmt.Errorf("failed to obtain DHCP lease on interface %s  up: %v", ifName, err)
	}
	ip = (&net.IPNet{
		IP:   lease.ACK.YourIPAddr,
		Mask: lease.ACK.SubnetMask(),
	}).String()

	// only support opt 121 (ignore 33)
	for _, route := range lease.ACK.ClasslessStaticRoute() {
		routeCfg := apis.RouteConfig{
			Destination: route.Dest.String(),
			Gateway:     route.Router.String(),
		}
		routes = append(routes, routeCfg)
	}
	return
}

// issueDHCPDiscover broadcasts a single DHCP DISCOVER on the exclusive NIC (now
// in the pod network namespace) and waits for an OFFER, matching the Azure CNI
// SecondaryEndpointClient behavior. The OFFER is intentionally discarded: the
// NIC's IP and routes are already programmed from CNS, so the discover exists
// only to make the host/wireserver create the DNS mapping for the NIC. This is
// called synchronously on the exclusive attach path and its error is fatal so
// the plumbing is retried. nclient4 uses DiscoverOffer (DISCOVER + OFFER, no
// lease), the same scope as CNI's DiscoverRequest.
//
// Netns handling is consistent with CNI: in azure-container-networking's
// addEndpointImpl the NIC is moved into the container netns, then the caller
// thread enters it via ns.Enter() (runtime.LockOSThread + setns) and runs
// ConfigureContainerInterfacesAndRoutes -> DiscoverRequest inside it, restoring
// the host netns with ns.Exit() (setns back + UnlockOSThread). We do the same
// here because nclient4.New binds an AF_PACKET socket to the interface in the
// current thread's netns, and the NIC now lives in the pod netns. (getDHCP, by
// contrast, runs in the host netns because there the NIC is still host-visible.)
func issueDHCPDiscover(containerNsPath string, ifName string, mac net.HardwareAddr) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	hostNS, err := netns.Get()
	if err != nil {
		return fmt.Errorf("dhcp: failed to capture host netns: %w", err)
	}
	defer hostNS.Close()
	// Always restore the calling thread to the host netns before unlocking it.
	defer func() {
		if serr := netns.Set(hostNS); serr != nil {
			klog.Errorf("secondary NIC DHCP: failed to restore host netns: %v", serr)
		}
	}()

	containerNS, err := netns.GetFromPath(containerNsPath)
	if err != nil {
		return fmt.Errorf("dhcp: failed to open netns %s: %w", containerNsPath, err)
	}
	defer containerNS.Close()

	if err := netns.Set(containerNS); err != nil {
		return fmt.Errorf("dhcp: failed to enter netns %s: %w", containerNsPath, err)
	}

	// nclient4 reads the hardware address from the interface itself; mac is used
	// for logging so the bound identity is visible in the logs.
	dhclient, err := nclient4.New(ifName)
	if err != nil {
		return fmt.Errorf("dhcp: failed to create client on %s (MAC %s): %w", ifName, mac, err)
	}
	defer dhclient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), dhcpDiscoverTimeout)
	defer cancel()

	if _, err := dhclient.DiscoverOffer(ctx); err != nil {
		return fmt.Errorf("dhcp: discover/offer failed on %s (MAC %s): %w", ifName, mac, err)
	}
	klog.V(2).Infof("secondary NIC DHCP: received DHCP offer on %s (MAC %s); wireserver DNS mapping created", ifName, mac)
	return nil
}
