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
	"os"
	"syscall"

	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"sigs.k8s.io/dranet/internal/nlwrap"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/klog/v2"
)

const (
	// swiftV2VirtualGW is the Azure virtual gateway IP used by SwiftV2 for
	// delegated NIC routing. This matches the CNS middleware constant.
	swiftV2VirtualGW = "169.254.2.1"

	// swiftV2DelegatedIfName is the interface name assigned to delegated NICs
	// inside the pod namespace (both shared ipvlan children and dedicated NICs
	// when NRI plumbs them).
	//
	// The CNI computes this dynamically as "eth" + strconv.Itoa(endpointIndex),
	// where endpointIndex starts at 1 (eth0 is the infra NIC from CNI_IFNAME).
	// Since each pod currently gets exactly one delegated NIC, the index is
	// always 1. If multi-delegated-NIC pods are ever supported, this should be
	// replaced with a computed name.
	swiftV2DelegatedIfName = "eth1"
)

// findLinkByMAC finds a network interface on the host by its MAC address.
// Returns the link and nil error on success, or nil and an error if not found.
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
		if link.Attrs().HardwareAddr.String() == targetMAC.String() {
			return link, nil
		}
	}
	return nil, fmt.Errorf("NIC with MAC %s not found on host", mac)
}

// nsAttachIPVlanL3 creates an ipvlan L3 sub-interface off the shared parent NIC,
// adds a host-side /32 route for host-to-pod traffic, moves the sub-interface
// into the pod network namespace, and configures IP + routes inside the pod.
//
// The host-side /32 route is needed because host processes (CNS, kubelet health
// probes) need the kernel routing table to reach pod IPs. Wire-to-pod traffic
// is handled internally by the ipvlan L3 driver and does not require host routes.
func nsAttachIPVlanL3(cfg *NICConfig, containerNsPath string) (*resourceapi.NetworkDeviceData, error) {
	if cfg == nil {
		return nil, fmt.Errorf("NICConfig is nil")
	}

	// --- Host namespace operations ---

	// Find parent NIC by MAC address, matching CNI behavior.
	parent, err := findLinkByMAC(cfg.MAC)
	if err != nil {
		return nil, fmt.Errorf("parent NIC (MAC %s) not found: %w", cfg.MAC, err)
	}

	// Ensure the parent NIC is UP. Azure delegated NICs (e.g. eth1) are
	// attached to the VM by the platform but start with "state DOWN, qdisc noop".
	// Nothing else on the node brings them up — the CNI's network_linux.go does
	// this via SetLinkState(hostIf.Name, true) for its parent interfaces.
	// ipvlan children and host routes require the parent to be operationally up.
	if parent.Attrs().OperState != netlink.OperUp {
		klog.V(2).Infof("SwiftV2: parent NIC %s (MAC %s) is %s, bringing it UP",
			parent.Attrs().Name, cfg.MAC, parent.Attrs().OperState)
		if err := netlink.LinkSetUp(parent); err != nil {
			return nil, fmt.Errorf("failed to bring parent NIC %s up: %w", parent.Attrs().Name, err)
		}
	}

	// Compute the deterministic ipvlan child name (first 8 chars of PodUID).
	ipvlName := fmt.Sprintf("ipvl-%s", truncateUID(cfg.PodUID))

	// Open the container namespace and netlink handle early so we can clean up
	// stale state from any previous failed attempt before creating new resources.
	containerNs, err := netns.GetFromPath(containerNsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get netns %s: %w", containerNsPath, err)
	}
	defer containerNs.Close()

	nhNs, err := nlwrap.NewHandleAt(containerNs)
	if err != nil {
		return nil, fmt.Errorf("failed to get netlink handle in netns %s: %w", containerNsPath, err)
	}
	defer nhNs.Close()

	// --- Idempotent cleanup of stale state from previous failed attempts ---
	// A previous call may have partially completed, leaving behind:
	//   1. ipvl-<uid> on the host  (created but never moved to pod ns — reuse it)
	//   2. ipvl-<uid> in the pod ns (moved but never renamed to eth1)
	//   3. eth1 in the pod ns       (renamed but IP/route config failed)
	// Clean up pod-ns artifacts and reuse the host-side child if it exists.

	// Remove stale ipvlan child from pod ns (moved but not renamed).
	if stale, err := nhNs.LinkByName(ipvlName); err == nil {
		klog.V(2).Infof("SwiftV2: removing stale ipvlan child %s from pod ns (previous failed attempt)", ipvlName)
		_ = nhNs.LinkDel(stale)
	}

	// Remove stale eth1 from pod ns (renamed but later steps failed).
	if stale, err := nhNs.LinkByName(swiftV2DelegatedIfName); err == nil {
		klog.V(2).Infof("SwiftV2: removing stale %s from pod ns (previous failed attempt)", swiftV2DelegatedIfName)
		_ = nhNs.LinkDel(stale)
	}

	// --- Reuse or create ipvlan child on host ---

	// If ipvl-<uid> already exists on the host (from a previous attempt that
	// created it but failed before moving it to the pod ns), reuse it rather
	// than deleting and recreating — the interface is already correctly
	// parented to the physical NIC.
	ipvl, err := nlwrap.LinkByName(ipvlName)
	if err != nil {
		var notFound netlink.LinkNotFoundError
		if !errors.As(err, &notFound) {
			return nil, fmt.Errorf("failed to look up ipvlan %s on host: %w", ipvlName, err)
		}

		// No existing child — create a fresh ipvlan L3 sub-interface.
		//
		// Mode rationale (L3, not L3S):
		//   * L3 places the FIB lookup, RPF and netfilter hooks for slave
		//     traffic in the *parent's* (host) network namespace. The host's
		//     rp_filter=2 (loose) setting therefore actually applies to pod
		//     egress, and there is no ambiguity about which netns runs RPF.
		//   * L3S would run those hooks in the slave (pod) netns where rp_filter
		//     defaults to 1 (strict) and would reject the asymmetric reverse
		//     path that SwiftV2 prefix-on-NIC inherently has (host-FIB has
		//     no per-pod /32 in the slave's view).
		//   * L3 with bridge sub-mode (default for ipvlan) still delivers
		//     intra-master cross-slave traffic correctly: the ipvlan driver
		//     demuxes by destination IP. The receiving slave's L2 admittance
		//     check passes because every slave shares the parent's MAC, and
		//     the static neighbor entry installed below uses the parent's
		//     real MAC (not a synthetic placeholder), so frames carry a dst
		//     MAC that matches every slave's own address.
		newIPVL := &netlink.IPVlan{
			LinkAttrs: netlink.LinkAttrs{
				Name:        ipvlName,
				ParentIndex: parent.Attrs().Index,
			},
			Mode: netlink.IPVLAN_MODE_L3,
		}
		if err := netlink.LinkAdd(newIPVL); err != nil {
			return nil, fmt.Errorf("failed to create ipvlan L3 interface %s on parent (MAC %s): %w", ipvlName, cfg.MAC, err)
		}
		ipvl, err = nlwrap.LinkByName(ipvlName)
		if err != nil {
			return nil, fmt.Errorf("ipvlan %s not found after creation: %w", ipvlName, err)
		}
	} else {
		klog.V(2).Infof("SwiftV2: reusing existing ipvlan child %s on host (previous failed attempt)", ipvlName)
	}

	// Add host-side /32 route for host-to-pod traffic.
	// Uses RouteReplace on EEXIST for retry idempotency (stale route from failed attempt).
	hostRoute := &netlink.Route{
		Dst:       parseIP32(cfg.PodIP),
		LinkIndex: parent.Attrs().Index,
		Scope:     netlink.SCOPE_LINK,
	}
	if err := netlink.RouteAdd(hostRoute); err != nil {
		if errors.Is(err, syscall.EEXIST) {
			if err := netlink.RouteReplace(hostRoute); err != nil {
				return nil, fmt.Errorf("failed to replace host route for %s: %w", cfg.PodIP, err)
			}
		} else {
			return nil, fmt.Errorf("failed to add host route for %s: %w", cfg.PodIP, err)
		}
	}

	// Move sub-interface into pod network namespace.
	if err := netlink.LinkSetNsFd(ipvl, int(containerNs)); err != nil {
		return nil, fmt.Errorf("failed to move ipvlan %s to netns %s: %w", ipvlName, containerNsPath, err)
	}

	// --- Pod namespace operations ---

	nsLink, err := nhNs.LinkByName(ipvlName)
	if err != nil {
		return nil, fmt.Errorf("ipvlan interface %s not found in netns: %w", ipvlName, err)
	}

	// Rename to eth1 inside the pod namespace.
	// Must use the container namespace handle (nhNs) because the link lives
	// there — the default package handle operates in the host namespace.
	if err := nhNs.LinkSetName(nsLink, swiftV2DelegatedIfName); err != nil {
		return nil, fmt.Errorf("failed to rename %s to %s: %w", ipvlName, swiftV2DelegatedIfName, err)
	}

	nsLink, err = nhNs.LinkByName(swiftV2DelegatedIfName)
	if err != nil {
		return nil, fmt.Errorf("renamed interface %s not found in netns: %w", swiftV2DelegatedIfName, err)
	}

	// Assign IP with /32 mask (point-to-point).
	podIP := net.ParseIP(cfg.PodIP)
	if podIP == nil {
		return nil, fmt.Errorf("invalid pod IP: %s", cfg.PodIP)
	}
	if err := nhNs.AddrAdd(nsLink, &netlink.Addr{
		IPNet: &net.IPNet{IP: podIP, Mask: net.CIDRMask(32, 32)},
	}); err != nil && !errors.Is(err, syscall.EEXIST) {
		return nil, fmt.Errorf("failed to add IP %s/32: %w", cfg.PodIP, err)
	}

	// Bring interface up.
	if err := nhNs.LinkSetUp(nsLink); err != nil {
		return nil, fmt.Errorf("failed to bring up %s: %w", swiftV2DelegatedIfName, err)
	}

	// Add static neighbor entry for the Azure virtual gateway.
	//
	// ipvlan L3 interfaces have NOARP set, so the kernel cannot resolve
	// 169.254.2.1 via ARP. Without this entry, the pod's outbound frames
	// would be dropped before egress.
	//
	// We use the *parent NIC's real MAC* (not a synthetic placeholder).
	// Reason: ipvlan slaves share the parent's MAC. For same-host cross-slave
	// traffic, the ipvlan bridge sub-mode delivers the frame directly from
	// one slave to another without going to wire. The receiving slave then
	// runs the standard "is dst MAC mine?" L2 admittance check; with the
	// parent's real MAC this check passes (every slave shares it). With a
	// fake/placeholder MAC the check fails and the frame is dropped silently
	// before the IP stack ever sees it. For off-host traffic the SmartNIC
	// ignores L2 entirely, so either MAC value works there.
	gwIP := net.ParseIP(cfg.GatewayIP)
	if gwIP == nil {
		return nil, fmt.Errorf("invalid gateway IP: %s", cfg.GatewayIP)
	}
	gwMAC := parent.Attrs().HardwareAddr
	if len(gwMAC) == 0 {
		return nil, fmt.Errorf("parent NIC %s has empty MAC address", parent.Attrs().Name)
	}
	neighEntry := &netlink.Neigh{
		LinkIndex:    nsLink.Attrs().Index,
		IP:           gwIP,
		HardwareAddr: gwMAC,
		State:        netlink.NUD_PERMANENT,
	}
	if err := nhNs.NeighSet(neighEntry); err != nil {
		return nil, fmt.Errorf("failed to set static neighbor %s -> %s on pod eth1: %w", cfg.GatewayIP, gwMAC, err)
	}

	// Add gateway /32 scope link route (gateway reachable directly on link).
	gwRoute := netlink.Route{
		Dst:       &net.IPNet{IP: gwIP, Mask: net.CIDRMask(32, 32)},
		LinkIndex: nsLink.Attrs().Index,
		Scope:     netlink.SCOPE_LINK,
	}
	if err := nhNs.RouteAdd(&gwRoute); err != nil && !errors.Is(err, syscall.EEXIST) {
		return nil, fmt.Errorf("failed to add gateway route %s/32: %w", cfg.GatewayIP, err)
	}

	// Add default route via gateway.
	//
	// Use RouteReplace (not RouteAdd): the cluster CNI plugin runs *before*
	// the dranet NRI hook and will already have installed a default route
	// via the pod's cluster-network interface (eth0). RouteAdd would return
	// EEXIST and the cluster default would win, leaving SwiftV2 pod egress
	// to non-prefix destinations going out eth0 instead of the delegated
	// NIC. We deliberately override that default so all pod egress flows
	// through eth1 → SmartNIC → VFP/SDN policy.
	_, defaultDst, _ := net.ParseCIDR("0.0.0.0/0")
	defaultRoute := netlink.Route{
		Dst:       defaultDst,
		Gw:        gwIP,
		LinkIndex: nsLink.Attrs().Index,
	}
	if err := nhNs.RouteReplace(&defaultRoute); err != nil {
		return nil, fmt.Errorf("failed to replace default route via %s: %w", cfg.GatewayIP, err)
	}

	// --- Host sysctl: loose RPF on parent and "all" ---
	//
	// Pod replies arrive on the parent eth1 from the SmartNIC. The host's
	// FIB has only a /32 to the pod IP via the parent and no per-pod source
	// route, so a strict RPF check on the parent (`from src=<podIP>` resolves
	// via parent? yes via the /32) is symmetric for the *destination* path
	// but the *source* check on packets the parent receives that are sourced
	// from another customer-VNet endpoint may not have a return-path entry,
	// causing strict RPF to drop them silently. Loose RPF (mode 2) only
	// requires that the source be reachable via *any* interface, which is
	// always true for valid VNet traffic.
	//
	// The kernel takes MAX(all/rp_filter, <iface>/rp_filter) for ingress, so
	// both must be set. Best-effort; sysctls may not exist on minimal kernels.
	setRPFilterLoose(parent.Attrs().Name)

	return &resourceapi.NetworkDeviceData{
		InterfaceName:   swiftV2DelegatedIfName,
		HardwareAddress: parent.Attrs().HardwareAddr.String(),
		IPs:             []string{fmt.Sprintf("%s/32", cfg.PodIP)},
	}, nil
}

// cleanupIPVlanL3 removes the host-side /32 route for a shared NIC pod.
// The ipvlan sub-interface is automatically destroyed when the pod's network
// namespace is deleted by the kernel.
// This is best-effort — errors are logged but not returned.
func cleanupIPVlanL3(cfg *NICConfig) {
	if cfg == nil {
		return
	}

	parent, err := findLinkByMAC(cfg.MAC)
	if err != nil {
		klog.Warningf("SwiftV2 cleanup: parent NIC (MAC %s) not found: %v", cfg.MAC, err)
		return
	}

	route := &netlink.Route{
		Dst:       parseIP32(cfg.PodIP),
		LinkIndex: parent.Attrs().Index,
		Scope:     netlink.SCOPE_LINK,
	}
	if err := netlink.RouteDel(route); err != nil {
		klog.Warningf("SwiftV2 cleanup: failed to remove host route for %s: %v", cfg.PodIP, err)
	}
}

// nsAttachSwiftV2NIC is the common attach entrypoint for SwiftV2 networking.
// Behavior is driven by mode:
//   - shared: attach ipvlan L3 child off a delegated parent NIC
//   - dedicated: move a delegated physical NIC into pod netns
func nsAttachSwiftV2NIC(mode NICMode, cfg *NICConfig, containerNsPath string) (*resourceapi.NetworkDeviceData, error) {
	switch mode {
	case NICModeShared:
		return nsAttachIPVlanL3(cfg, containerNsPath)
	case NICModeDedicated:
		return nsAttachDedicatedNIC(cfg, containerNsPath)
	default:
		return nil, fmt.Errorf("unsupported SwiftV2 NIC mode %q", mode)
	}
}

// nicExistsInNetns checks whether a NIC with the given MAC address already
// exists in the specified network namespace. Used for idempotent dedicated NIC
// plumbing — if CNI has already moved the NIC in, NRI skips entirely.
func nicExistsInNetns(containerNsPath string, mac string) bool {
	containerNs, err := netns.GetFromPath(containerNsPath)
	if err != nil {
		return false
	}
	defer containerNs.Close()

	nhNs, err := nlwrap.NewHandleAt(containerNs)
	if err != nil {
		return false
	}
	defer nhNs.Close()

	links, err := nhNs.LinkList()
	if err != nil {
		return false
	}

	targetMAC, err := net.ParseMAC(mac)
	if err != nil {
		return false
	}

	for _, link := range links {
		if link.Attrs().HardwareAddr.String() == targetMAC.String() {
			return true
		}
	}
	return false
}

// nsAttachDedicatedNIC moves a dedicated physical NIC (identified by MAC address)
// into the pod network namespace, assigns IP addresses and routes.
// This matches the CNI SecondaryEndpointClient behavior:
//   - Lookup NIC by MAC (not name)
//   - Move NIC into pod netns (no rename — keeps original name)
//   - Bring link up
//   - Assign IP addresses
//   - Add routes: virtual GW /32 scope link + default via virtual GW
//   - Issue DHCP discover for DNS wireserver mapping (background goroutine)
func nsAttachDedicatedNIC(cfg *NICConfig, containerNsPath string) (*resourceapi.NetworkDeviceData, error) {
	if cfg == nil {
		return nil, fmt.Errorf("NICConfig is nil")
	}

	// Find NIC by MAC address on the host, matching CNI's GetNetworkInterfaceByMac.
	hostLink, err := findLinkByMAC(cfg.MAC)
	if err != nil {
		// NIC not on host: verify whether it is already in the pod namespace.
		// If yes, treat as idempotent success and perform no additional operations.
		// If no, return an error because the NIC cannot be found in either location.
		if nicExistsInNetns(containerNsPath, cfg.MAC) {
			klog.V(2).Infof("SwiftV2 dedicated NIC: MAC %s not on host but already in pod netns (CNI race), skipping", cfg.MAC)
			return &resourceapi.NetworkDeviceData{
				HardwareAddress: cfg.MAC,
			}, nil
		}
		return nil, fmt.Errorf("dedicated NIC with MAC %s not found on host or in pod netns", cfg.MAC)
	}

	targetMAC, _ := net.ParseMAC(cfg.MAC) // already validated by findLinkByMAC

	ifName := hostLink.Attrs().Name

	// Move NIC into pod namespace.
	containerNs, err := netns.GetFromPath(containerNsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get netns %s: %w", containerNsPath, err)
	}
	defer containerNs.Close()

	if err := netlink.LinkSetNsFd(hostLink, int(containerNs)); err != nil {
		return nil, fmt.Errorf("failed to move NIC %s to netns %s: %w", ifName, containerNsPath, err)
	}

	// --- Pod namespace operations ---

	nhNs, err := nlwrap.NewHandleAt(containerNs)
	if err != nil {
		return nil, fmt.Errorf("failed to get netlink handle in netns %s: %w", containerNsPath, err)
	}
	defer nhNs.Close()

	nsLink, err := nhNs.LinkByName(ifName)
	if err != nil {
		return nil, fmt.Errorf("NIC %s not found in netns after move: %w", ifName, err)
	}

	// Bring link up (matches CNI SetupContainerInterfaces).
	if err := nhNs.LinkSetUp(nsLink); err != nil {
		return nil, fmt.Errorf("failed to bring up %s: %w", ifName, err)
	}

	// Assign IP addresses.
	networkData := &resourceapi.NetworkDeviceData{
		InterfaceName:   ifName,
		HardwareAddress: targetMAC.String(),
	}

	for _, addr := range cfg.Addresses {
		ip, ipNet, err := net.ParseCIDR(addr)
		if err != nil {
			klog.Warningf("SwiftV2 dedicated NIC: invalid address %s: %v", addr, err)
			continue
		}
		if err := nhNs.AddrAdd(nsLink, &netlink.Addr{
			IPNet: &net.IPNet{IP: ip, Mask: ipNet.Mask},
		}); err != nil && !errors.Is(err, syscall.EEXIST) {
			return nil, fmt.Errorf("failed to add address %s to %s: %w", addr, ifName, err)
		}
		networkData.IPs = append(networkData.IPs, addr)
	}

	// Delete kernel-added subnet routes (matches CNI ConfigureContainerInterfacesAndRoutes).
	// When assigning an IP, the kernel auto-adds a subnet route that we need to remove.
	for _, addr := range cfg.Addresses {
		_, ipNet, err := net.ParseCIDR(addr)
		if err != nil {
			continue
		}
		subnetRoute := netlink.Route{
			Dst:       ipNet,
			LinkIndex: nsLink.Attrs().Index,
			Scope:     netlink.SCOPE_LINK,
			Protocol:  syscall.RTPROT_KERNEL,
		}
		// Best-effort removal — may not exist if prefix is /32.
		_ = nhNs.RouteDel(&subnetRoute)
	}

	// Add virtual gateway /32 scope link route.
	gwIP := net.ParseIP(cfg.GatewayIP)
	if gwIP == nil {
		return nil, fmt.Errorf("invalid gateway IP: %s", cfg.GatewayIP)
	}
	gwRoute := netlink.Route{
		Dst:       &net.IPNet{IP: gwIP, Mask: net.CIDRMask(32, 32)},
		LinkIndex: nsLink.Attrs().Index,
		Scope:     netlink.SCOPE_LINK,
	}
	if err := nhNs.RouteAdd(&gwRoute); err != nil && !errors.Is(err, syscall.EEXIST) {
		return nil, fmt.Errorf("failed to add gateway route %s/32: %w", cfg.GatewayIP, err)
	}

	// Add default route via virtual gateway.
	//
	// Use RouteReplace (not RouteAdd): see the matching block in
	// nsAttachIPVlanL3 — the cluster CNI plugin installs an eth0 default
	// before our NRI hook runs, so we must override it to ensure all pod
	// egress leaves through the delegated NIC (and thus the SmartNIC).
	_, defaultDst, _ := net.ParseCIDR("0.0.0.0/0")
	defaultRoute := netlink.Route{
		Dst:       defaultDst,
		Gw:        gwIP,
		LinkIndex: nsLink.Attrs().Index,
	}
	if err := nhNs.RouteReplace(&defaultRoute); err != nil {
		return nil, fmt.Errorf("failed to replace default route via %s: %w", cfg.GatewayIP, err)
	}

	// Issue DHCP discover in background to create DNS mapping in host via wireserver.
	// This matches the CNI SecondaryEndpointClient behavior. Run as goroutine because
	// the NRI plugin has a 2-second default timeout and DHCP can take ~3 seconds.
	go func() {
		if err := issueDHCPDiscover(containerNsPath, ifName, targetMAC); err != nil {
			klog.Warningf("SwiftV2 dedicated NIC: DHCP discover failed for %s (MAC %s): %v — DNS via wireserver may not work", ifName, cfg.MAC, err)
		}
	}()

	return networkData, nil
}

// cleanupDedicatedNIC moves a dedicated NIC back from the pod namespace to the
// host namespace. This matches the CNI SecondaryEndpointClient.DeleteEndpoints
// behavior. Best-effort — if the netns is already gone, the kernel has already
// returned the NIC to the host.
func cleanupDedicatedNIC(containerNsPath string, mac string) {
	containerNs, err := netns.GetFromPath(containerNsPath)
	if err != nil {
		// Namespace likely already deleted — NIC returned to host by kernel.
		klog.V(2).Infof("SwiftV2 cleanup: netns %s not found (NIC returned by kernel): %v", containerNsPath, err)
		return
	}
	defer containerNs.Close()

	nhNs, err := nlwrap.NewHandleAt(containerNs)
	if err != nil {
		klog.Warningf("SwiftV2 cleanup: failed to get netlink handle in netns: %v", err)
		return
	}
	defer nhNs.Close()

	// Find the NIC by MAC in the pod namespace.
	links, err := nhNs.LinkList()
	if err != nil {
		klog.Warningf("SwiftV2 cleanup: failed to list links in netns: %v", err)
		return
	}

	targetMAC, err := net.ParseMAC(mac)
	if err != nil {
		klog.Warningf("SwiftV2 cleanup: invalid MAC %s: %v", mac, err)
		return
	}

	var nicLink netlink.Link
	for _, link := range links {
		if link.Attrs().HardwareAddr.String() == targetMAC.String() {
			nicLink = link
			break
		}
	}
	if nicLink == nil {
		klog.V(2).Infof("SwiftV2 cleanup: NIC with MAC %s not found in netns (already returned)", mac)
		return
	}

	// Move NIC back to host namespace.
	// Must use the container namespace handle (nhNs) because the link lives
	// there — the default package handle operates in the host namespace and
	// would look up the wrong link index.
	rootNs, err := netns.Get()
	if err != nil {
		klog.Warningf("SwiftV2 cleanup: failed to get root netns: %v", err)
		return
	}
	defer rootNs.Close()

	if err := nhNs.LinkSetNsFd(nicLink, int(rootNs)); err != nil {
		klog.Warningf("SwiftV2 cleanup: failed to move NIC (MAC %s) back to host: %v", mac, err)
		return
	}

	klog.V(2).Infof("SwiftV2 cleanup: returned dedicated NIC (MAC %s) to host namespace", mac)
}

// issueDHCPDiscover sends a DHCP discover packet on the specified interface
// inside the given network namespace. This creates a mapping in the host for
// DNS resolution via wireserver, matching the CNI SecondaryEndpointClient behavior.
//
// TODO: implement DHCP discover. For now this is a stub that logs the intent.
// The real implementation needs a DHCP client library (e.g., github.com/insomniacslk/dhcp).
func issueDHCPDiscover(containerNsPath string, ifName string, mac net.HardwareAddr) error {
	klog.V(2).Infof("SwiftV2: would issue DHCP discover on %s (MAC %s) in netns %s for DNS wireserver mapping",
		ifName, mac.String(), containerNsPath)
	// TODO: implement actual DHCP discover using a DHCP client library.
	// The CNI uses a 3-second timeout for this operation.
	return nil
}

// parseIP32 parses an IP string and returns a /32 IPNet.
func parseIP32(ipStr string) *net.IPNet {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return nil
	}
	return &net.IPNet{IP: ip, Mask: net.CIDRMask(32, 32)}
}

// truncateUID returns the first 8 characters of a UID string for use in
// interface naming. If the UID is shorter than 8 characters, returns the whole string.
func truncateUID(uid string) string {
	if len(uid) > 8 {
		return uid[:8]
	}
	return uid
}

// setRPFilterLoose sets reverse path filtering to "loose" (mode 2) on the
// specified interface and on the kernel's "all" pseudo-interface.
//
// The kernel takes MAX(all/rp_filter, <iface>/rp_filter) for ingress, so
// both must be set to ensure loose semantics on the parent. Loose RPF is
// required for SwiftV2 prefix-on-NIC because the host FIB does not have a
// per-source return-path entry for arbitrary customer-VNet endpoints — the
// SmartNIC handles all that — and strict RPF would silently drop their
// frames as "martians" from the host's perspective.
//
// Best-effort: errors are logged but do not fail Prepare.
func setRPFilterLoose(ifName string) {
	for _, p := range []string{
		"/proc/sys/net/ipv4/conf/" + ifName + "/rp_filter",
		"/proc/sys/net/ipv4/conf/all/rp_filter",
	} {
		if err := os.WriteFile(p, []byte("2\n"), 0644); err != nil {
			klog.Warningf("SwiftV2: failed to set %s=2 (loose RPF): %v", p, err)
		}
	}
}
