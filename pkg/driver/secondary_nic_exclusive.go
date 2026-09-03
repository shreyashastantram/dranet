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
	"syscall"

	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/dranet/internal/nlwrap"
)

// exclusiveNICAddress retains both the configured CIDR and its parsed
// address/subnet forms so attach performs no input parsing after NIC movement.
type exclusiveNICAddress struct {
	cidr    string
	address *net.IPNet
	subnet  *net.IPNet
}

// validateExclusiveNICConfig validates and parses all exclusive-NIC input
// before shared state is reclaimed or the NIC leaves the host namespace.
func validateExclusiveNICConfig(cfg *NICConfig) (net.HardwareAddr, net.IP, []exclusiveNICAddress, error) {
	if cfg == nil {
		return nil, nil, nil, fmt.Errorf("NICConfig is nil")
	}

	targetMAC, err := net.ParseMAC(cfg.MAC)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("invalid MAC %q: %w", cfg.MAC, err)
	}

	gwIPv4, err := parseSecondaryNICGateway(cfg.GatewayIP, "exclusive mode")
	if err != nil {
		return nil, nil, nil, err
	}

	// Exclusive-mode NICs are single-stack IPv4 with exactly one address;
	// multiple IPs and dual-stack are not supported.
	if len(cfg.Addresses) != 1 {
		return nil, nil, nil, fmt.Errorf("exclusive NIC (MAC %s) requires exactly one IPv4 address, got %d", cfg.MAC, len(cfg.Addresses))
	}
	cidr := cfg.Addresses[0]
	ip, subnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("invalid exclusive NIC address %q: %w", cidr, err)
	}
	ipIPv4 := ip.To4()
	if ipIPv4 == nil {
		return nil, nil, nil, fmt.Errorf("exclusive NIC requires IPv4 addresses, got %s", cidr)
	}
	addresses := []exclusiveNICAddress{{
		cidr:    cidr,
		address: &net.IPNet{IP: ipIPv4, Mask: subnet.Mask},
		subnet:  subnet,
	}}

	return targetMAC, gwIPv4, addresses, nil
}

// detachNICFromSharedVRF tears down any shared-mode routing state on a NIC
// before it is moved into a pod as an exclusive NIC. It validates ownership,
// clears the parent's routes, NAT exemption, and gateway neighbor, detaches the
// NIC from its VRF master, and deletes the now-unused VRF. It is a no-op when
// the NIC has no master and its expected shared NIC VRF does not exist. Returns the
// refreshed host link.
func detachNICFromSharedVRF(hostLink netlink.Link, mac string) (netlink.Link, error) {
	if hostLink == nil || hostLink.Attrs() == nil {
		return nil, fmt.Errorf("host link for MAC %s is nil", mac)
	}

	// Resolve the exact VRF identity that this MAC would have used in shared
	// mode, including its deterministic routing table.
	vrfName := sharedNICVRFName(mac)
	vrfTable, err := sharedNICVRFTable(mac)
	if err != nil {
		return nil, err
	}
	if err := validateSharedNICVRFTableOwner(vrfName, vrfTable); err != nil {
		return nil, err
	}

	// Validate the expected VRF and master relationship before deleting any
	// shared datapath state.
	vrf, vrfErr := nlwrap.LinkByName(vrfName)
	masterIndex := hostLink.Attrs().MasterIndex
	if vrfErr != nil {
		if isLinkNotFound(vrfErr) {
			if masterIndex != 0 {
				return nil, fmt.Errorf("exclusive NIC %s (MAC %s) is enslaved to master index %d but expected shared NIC VRF %s is missing",
					hostLink.Attrs().Name, mac, masterIndex, vrfName)
			}
			return hostLink, nil
		}
		return nil, fmt.Errorf("failed to look up shared NIC VRF %s before exclusive attach: %w", vrfName, vrfErr)
	}

	vrfLink, ok := vrf.(*netlink.Vrf)
	if !ok || vrf.Type() != "vrf" {
		return nil, fmt.Errorf("link %s exists but is %q, expected vrf", vrfName, vrf.Type())
	}
	if int(vrfLink.Table) != vrfTable {
		return nil, fmt.Errorf("VRF %s exists with table %d, expected %d", vrfName, vrfLink.Table, vrfTable)
	}
	if masterIndex != 0 && masterIndex != vrf.Attrs().Index {
		return nil, fmt.Errorf("exclusive NIC %s (MAC %s) is enslaved to unexpected master index %d, expected shared NIC VRF %s index %d",
			hostLink.Attrs().Name, mac, masterIndex, vrfName, vrf.Attrs().Index)
	}

	// Remove ancillary host state that applied while this NIC served shared
	// IPVLAN children. Deleting the validated per-MAC VRF below removes its routes.
	cleanupSharedVRFParentState(hostLink)

	// Return the physical NIC to the host's main routing domain before moving it
	// into the exclusive pod namespace.
	if masterIndex != 0 {
		klog.V(2).Infof("exclusive NIC: detaching %s (MAC %s) from shared VRF %s before pod move",
			hostLink.Attrs().Name, mac, vrfName)
		if err := netlink.LinkSetNoMaster(hostLink); err != nil {
			return nil, fmt.Errorf("failed to detach exclusive NIC %s (MAC %s) from shared NIC VRF %s: %w",
				hostLink.Attrs().Name, mac, vrfName, err)
		}
		hostLink, err = findLinkByMAC(mac)
		if err != nil {
			return nil, fmt.Errorf("exclusive NIC (MAC %s) not found after detaching from shared NIC VRF %s: %w", mac, vrfName, err)
		}
	}

	// No shared parent remains, so remove the now-unused per-MAC VRF device.
	klog.V(2).Infof("exclusive NIC: deleting shared VRF %s before moving NIC %s (MAC %s) into pod netns",
		vrfName, hostLink.Attrs().Name, mac)
	if err := netlink.LinkDel(vrf); err != nil && !isLinkNotFound(err) {
		return nil, fmt.Errorf("failed to delete shared NIC VRF %s before exclusive attach for NIC %s (MAC %s): %w",
			vrfName, hostLink.Attrs().Name, mac, err)
	}
	return hostLink, nil
}

// cleanupSharedVRFParentState removes the NAT exemption and gateway neighbor
// programmed while parent served shared IPVLAN children. VRF deletion removes
// the routes in its per-MAC routing table.
func cleanupSharedVRFParentState(parent netlink.Link) {
	// The parent will leave the host namespace, so its host NAT exemption is no
	// longer needed.
	cleanupSharedNICNATExemption(parent.Attrs().Name)

	// Remove the permanent Azure gateway neighbor programmed for shared egress.
	gwIP := net.ParseIP(secondaryNICVirtualGateway).To4()
	neigh := &netlink.Neigh{LinkIndex: parent.Attrs().Index, IP: gwIP}
	if err := netlink.NeighDel(neigh); err != nil && !errors.Is(err, syscall.ENOENT) && !errors.Is(err, syscall.ESRCH) {
		klog.V(2).Infof("exclusive NIC: best-effort gateway neighbor cleanup on %s failed: %v", parent.Attrs().Name, err)
	}
}

// nsAttachExclusiveNIC moves an exclusive physical NIC (identified by MAC address)
// into the pod network namespace, assigns IP addresses and routes.
//
// No static gateway neighbor is programmed for an exclusive NIC, unlike the shared
// IPVLAN path. The shared parent is enslaved to a VRF and carries no address of
// its own, so it has no source IP to ARP with and must pin 169.254.2.1 to the SDN
// gateway MAC statically. An exclusive NIC instead holds the customer IP directly,
// so the kernel resolves the virtual gateway through normal ARP, which Azure SDN
// answers — dynamic neighbor discovery makes a static entry unnecessary.
//
// DRANET NRI owns this datapath; Azure CNI's FrontendNIC path is a no-op. The
// operation mirrors the former CNI SecondaryEndpointClient plumbing contract:
//   - Lookup NIC by MAC (not name)
//   - Move NIC into pod netns (no rename — keeps original name)
//   - Bring link up
//   - Assign IP addresses
//   - Add routes: virtual GW /32 scope link + default via virtual GW
//   - Issue DHCP discover for DNS wireserver mapping (synchronous; failure is fatal)
func nsAttachExclusiveNIC(cfg *NICConfig, containerNsPath string) (networkData *resourceapi.NetworkDeviceData, retErr error) {
	return nsAttachExclusiveNICWithPostAttach(cfg, containerNsPath, issueDHCPDiscover)
}

// nsAttachExclusiveNICWithPostAttach performs the exclusive attach and then runs
// an optional post-attach operation. Production currently uses issueDHCPDiscover;
// dependency injection keeps post-move rollback testable.
func nsAttachExclusiveNICWithPostAttach(
	cfg *NICConfig,
	containerNsPath string,
	postAttach func(string, string, net.HardwareAddr) error,
) (networkData *resourceapi.NetworkDeviceData, retErr error) {
	// Validate and parse every input before reclaiming shared state or moving the
	// physical NIC out of the host namespace.
	targetMAC, gwIP, addresses, err := validateExclusiveNICConfig(cfg)
	if err != nil {
		return nil, err
	}

	// Open the destination pod namespace while the NIC is still host-visible.
	containerNs, err := netns.GetFromPath(containerNsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get netns %s: %w", containerNsPath, err)
	}
	defer containerNs.Close()

	// Reclaim the NIC from any shared-parent role and move it into the pod. This
	// runs under the per-MAC lock so it cannot race a concurrent shared attach or
	// reclaim on the same NIC; the lock is released once the NIC is in the pod.
	ifName, err := reclaimAndMoveExclusiveNIC(cfg, targetMAC, containerNs, containerNsPath)
	if err != nil {
		return nil, err
	}

	// Any failure after the move returns the NIC to the host so a later sandbox
	// retry can find it again by MAC. cleanupExclusiveNIC re-takes the per-MAC
	// lock, which is safe now that reclaimAndMoveExclusiveNIC has released it.
	defer func() {
		if retErr != nil {
			klog.V(2).Infof("exclusive NIC: attach failed after move, returning NIC (MAC %s) to host: %v", cfg.MAC, retErr)
			cleanupExclusiveNIC(containerNsPath, cfg.MAC)
		}
	}()

	// Configure the moved NIC through a netlink handle scoped to the pod.
	nhNs, err := nlwrap.NewHandleAt(containerNs)
	if err != nil {
		return nil, fmt.Errorf("failed to get netlink handle in netns %s: %w", containerNsPath, err)
	}
	defer nhNs.Close()

	nsLink, err := nhNs.LinkByName(ifName)
	if err != nil {
		return nil, fmt.Errorf("NIC %s not found in netns after move: %w", ifName, err)
	}

	// Bring the physical interface up inside its new pod namespace.
	if err := nhNs.LinkSetUp(nsLink); err != nil {
		return nil, fmt.Errorf("failed to bring up %s: %w", ifName, err)
	}

	// Build the runtime-visible result while assigning every validated customer IP.
	networkData = &resourceapi.NetworkDeviceData{
		InterfaceName:   ifName,
		HardwareAddress: targetMAC.String(),
	}
	for _, addr := range addresses {
		if err := nhNs.AddrAdd(nsLink, &netlink.Addr{IPNet: addr.address}); err != nil && !errors.Is(err, syscall.EEXIST) {
			return nil, fmt.Errorf("failed to add address %s to %s: %w", addr.cidr, ifName, err)
		}
		networkData.IPs = append(networkData.IPs, addr.cidr)
	}

	// Make the virtual gateway directly reachable from the exclusive interface.
	gwRoute := netlink.Route{
		Dst:       &net.IPNet{IP: gwIP, Mask: net.CIDRMask(32, 32)},
		LinkIndex: nsLink.Attrs().Index,
		Scope:     netlink.SCOPE_LINK,
	}
	if err := nhNs.RouteAdd(&gwRoute); err != nil && !errors.Is(err, syscall.EEXIST) {
		return nil, fmt.Errorf("failed to add gateway route %s/32: %w", cfg.GatewayIP, err)
	}

	// Override the primary-CNI default so pod traffic exits through this NIC.
	_, defaultDst, _ := net.ParseCIDR("0.0.0.0/0")
	defaultRoute := netlink.Route{
		Dst:       defaultDst,
		Gw:        gwIP,
		LinkIndex: nsLink.Attrs().Index,
	}
	if err := nhNs.RouteReplace(&defaultRoute); err != nil {
		return nil, fmt.Errorf("failed to replace default route via %s: %w", cfg.GatewayIP, err)
	}

	// Run any required post-attach operation synchronously. Failure triggers the
	// rollback above instead of leaving a partially configured exclusive NIC.
	if postAttach != nil {
		if err := postAttach(containerNsPath, ifName, targetMAC); err != nil {
			return nil, fmt.Errorf("exclusive NIC: post-attach operation failed for %s (MAC %s): %w", ifName, cfg.MAC, err)
		}
	}

	return networkData, nil
}

// reclaimAndMoveExclusiveNIC finds the physical NIC by MAC, reclaims it from any
// shared-parent VRF, and moves it into containerNs — all under the per-MAC lock
// so it cannot race a concurrent shared attach/reclaim on the same NIC. The lock
// is released on return: once the NIC is in the pod a concurrent shared attach
// can no longer find it on the host, so the caller's remaining pod-local setup
// (and its rollback) run without it. Returns the moved interface name.
func reclaimAndMoveExclusiveNIC(cfg *NICConfig, targetMAC net.HardwareAddr, containerNs netns.NsHandle, containerNsPath string) (string, error) {
	lock := secondaryNICLock(cfg.MAC)
	lock.Lock()
	defer lock.Unlock()

	// Locate the exclusive physical NIC by MAC while it is still host-visible.
	hostLink, err := findLinkByMAC(targetMAC.String())
	if err != nil {
		return "", fmt.Errorf("exclusive NIC with MAC %s not found on host: %w", cfg.MAC, err)
	}

	// Reclaim the NIC from any previous shared-parent role before exclusive use.
	hostLink, err = detachNICFromSharedVRF(hostLink, targetMAC.String())
	if err != nil {
		return "", err
	}

	// Move the whole physical NIC into the pod without renaming it.
	ifName := hostLink.Attrs().Name
	if err := netlink.LinkSetNsFd(hostLink, int(containerNs)); err != nil {
		return "", fmt.Errorf("failed to move NIC %s to netns %s: %w", ifName, containerNsPath, err)
	}
	return ifName, nil
}

// cleanupExclusiveNIC best-effort moves the physical NIC identified by mac from
// the pod namespace back to the host namespace, under the per-MAC lock so it
// cannot race a concurrent shared attach/reclaim on the same NIC. A missing
// namespace or NIC means the kernel or an earlier cleanup already returned it.
func cleanupExclusiveNIC(containerNsPath string, mac string) {
	lock := secondaryNICLock(mac)
	lock.Lock()
	defer lock.Unlock()

	// Open the old pod namespace while it still exists. If it is already gone,
	// the kernel has returned the physical NIC to the host automatically.
	containerNs, err := netns.GetFromPath(containerNsPath)
	if err != nil {
		klog.V(2).Infof("exclusive NIC cleanup: netns %s not found (NIC returned by kernel): %v", containerNsPath, err)
		return
	}
	defer containerNs.Close()

	nhNs, err := nlwrap.NewHandleAt(containerNs)
	if err != nil {
		klog.Warningf("exclusive NIC cleanup: failed to get netlink handle in netns: %v", err)
		return
	}
	defer nhNs.Close()

	// Find the moved physical NIC by its stable MAC rather than pod interface name.
	links, err := nhNs.LinkList()
	if err != nil {
		klog.Warningf("exclusive NIC cleanup: failed to list links in netns: %v", err)
		return
	}

	targetMAC, err := net.ParseMAC(mac)
	if err != nil {
		klog.Warningf("exclusive NIC cleanup: invalid MAC %s: %v", mac, err)
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
		klog.V(2).Infof("exclusive NIC cleanup: NIC with MAC %s not found in netns (already returned)", mac)
		return
	}

	// Move the physical NIC back to the host network namespace for reuse.
	rootNs, err := netns.Get()
	if err != nil {
		klog.Warningf("exclusive NIC cleanup: failed to get root netns: %v", err)
		return
	}
	defer rootNs.Close()

	if err := nhNs.LinkSetNsFd(nicLink, int(rootNs)); err != nil {
		klog.Warningf("exclusive NIC cleanup: failed to move NIC (MAC %s) back to host: %v", mac, err)
		return
	}

	klog.V(2).Infof("exclusive NIC cleanup: returned exclusive NIC (MAC %s) to host namespace", mac)
}
