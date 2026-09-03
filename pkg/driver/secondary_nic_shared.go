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
	"hash/crc32"
	"net"
	"strings"
	"sync"
	"syscall"

	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/dranet/internal/nlwrap"
)

// Concurrent shared attaches on the same physical NIC must serialize host VRF
// programming and stale-child recovery.
var secondaryNICLocks sync.Map // map[normalizedMAC]*sync.Mutex

// secondaryNICLock returns the process-local mutex shared by all operations on
// the physical NIC identified by mac.
func secondaryNICLock(mac string) *sync.Mutex {
	key := normalizedMACKey(mac)
	if value, ok := secondaryNICLocks.Load(key); ok {
		return value.(*sync.Mutex)
	}
	mutex := &sync.Mutex{}
	actual, _ := secondaryNICLocks.LoadOrStore(key, mutex)
	return actual.(*sync.Mutex)
}

// normalizedMACKey normalizes a MAC address into a lowercase, separator-free key
// suitable for lock-map keys and Linux interface-name suffixes.
func normalizedMACKey(mac string) string {
	parsed, err := net.ParseMAC(mac)
	if err != nil {
		return strings.ToLower(strings.NewReplacer(":", "", "-", "").Replace(mac))
	}
	return strings.ReplaceAll(strings.ToLower(parsed.String()), ":", "")
}

// sharedNICVRFName returns the deterministic per-parent VRF interface name.
func sharedNICVRFName(mac string) string {
	return sharedNICVRFPrefix + normalizedMACKey(mac)
}

// sharedNICVRFTable derives a stable routing-table ID from the parent MAC.
// Although collisions between different MACs are extremely unlikely in practice,
// ensureSharedNICVRFLink detects an ID already owned by another VRF and fails
// safely rather than allowing unrelated NICs to share the same routing table.
func sharedNICVRFTable(mac string) (int, error) {
	parsed, err := net.ParseMAC(mac)
	if err != nil {
		return 0, fmt.Errorf("invalid MAC address %s: %w", mac, err)
	}
	return int(0x100000 + (crc32.ChecksumIEEE([]byte(parsed.String())) & 0x7fffffff)), nil
}

// sharedNICParentVRF contains the host routing state resolved for one shared
// parent NIC.
type sharedNICParentVRF struct {
	name   string
	table  int
	parent netlink.Link
}

// ensureSharedNICParentVRF ensures the per-MAC VRF for mac exists, enslaves the
// physical parent NIC to that VRF, and brings the parent up. Callers must hold
// secondaryNICLock(mac) across this call and the parent setup that follows.
func ensureSharedNICParentVRF(mac string) (*sharedNICParentVRF, error) {
	if _, err := net.ParseMAC(mac); err != nil {
		return nil, fmt.Errorf("invalid MAC address %s: %w", mac, err)
	}

	// Resolve the physical secondary NIC by its stable MAC address rather than
	// relying on a host interface name.
	parent, err := findLinkByMAC(mac)
	if err != nil {
		return nil, err
	}

	// Create or recover the deterministic VRF and routing table exclusive to
	// this parent NIC.
	vrfName := sharedNICVRFName(mac)
	table, err := sharedNICVRFTable(mac)
	if err != nil {
		return nil, err
	}
	vrf, err := ensureSharedNICVRFLink(vrfName, table)
	if err != nil {
		return nil, err
	}

	// Make the physical NIC a member of its VRF. This selects the per-parent
	// routing table for traffic emitted by the NIC's IPVLAN children.
	masterIndex := parent.Attrs().MasterIndex
	if masterIndex != 0 && masterIndex != vrf.Attrs().Index {
		return nil, fmt.Errorf("parent NIC %s (MAC %s) is already enslaved to unexpected master index %d, expected shared NIC VRF %s index %d",
			parent.Attrs().Name, mac, masterIndex, vrfName, vrf.Attrs().Index)
	}
	if masterIndex != vrf.Attrs().Index {
		klog.V(2).Infof("shared NIC: enslaving parent NIC %s (MAC %s) to VRF %s table %d",
			parent.Attrs().Name, mac, vrfName, table)
		if err := netlink.LinkSetMaster(parent, vrf); err != nil {
			return nil, fmt.Errorf("failed to enslave parent NIC %s (MAC %s) to VRF %s: %w", parent.Attrs().Name, mac, vrfName, err)
		}
		parent, err = findLinkByMAC(mac)
		if err != nil {
			return nil, fmt.Errorf("parent NIC (MAC %s) not found after VRF enslave: %w", mac, err)
		}
	}

	// Ensure the parent can carry both internally delivered IPVLAN traffic and
	// frames sent toward Azure SDN.
	if parent.Attrs().OperState != netlink.OperUp {
		klog.V(2).Infof("shared NIC: parent NIC %s (MAC %s) is %s, bringing it UP",
			parent.Attrs().Name, mac, parent.Attrs().OperState)
		if err := netlink.LinkSetUp(parent); err != nil {
			return nil, fmt.Errorf("failed to bring parent NIC %s up: %w", parent.Attrs().Name, err)
		}
		parent, err = findLinkByMAC(mac)
		if err != nil {
			return nil, fmt.Errorf("parent NIC (MAC %s) not found after LinkSetUp: %w", mac, err)
		}
	}

	return &sharedNICParentVRF{name: vrfName, table: table, parent: parent}, nil
}

// ensureSharedNICVRFLink returns an operational Linux VRF link bound to table. It
// creates the link when absent and rejects an existing link with the wrong type
// or routing-table association.
func ensureSharedNICVRFLink(name string, table int) (netlink.Link, error) {
	// Refuse a CRC-derived table collision before creating or reusing the VRF.
	if err := validateSharedNICVRFTableOwner(name, table); err != nil {
		return nil, err
	}

	// Reuse the deterministic VRF after a retry, or create it on first attach.
	link, err := nlwrap.LinkByName(name)
	if err != nil {
		if !isLinkNotFound(err) {
			return nil, fmt.Errorf("failed to look up VRF %s: %w", name, err)
		}
		newVRF := &netlink.Vrf{LinkAttrs: netlink.LinkAttrs{Name: name}, Table: uint32(table)}
		if err := netlink.LinkAdd(newVRF); err != nil {
			return nil, fmt.Errorf("failed to create VRF %s table %d: %w", name, table, err)
		}
		link, err = nlwrap.LinkByName(name)
		if err != nil {
			return nil, fmt.Errorf("VRF %s not found after creation: %w", name, err)
		}
	}

	// Never adopt a same-named link with different semantics or routing state.
	vrf, ok := link.(*netlink.Vrf)
	if !ok || link.Type() != "vrf" {
		return nil, fmt.Errorf("link %s exists but is %q, expected vrf", name, link.Type())
	}
	if int(vrf.Table) != table {
		return nil, fmt.Errorf("VRF %s exists with table %d, expected %d", name, vrf.Table, table)
	}
	// The VRF master must be administratively up before the parent is enslaved.
	if link.Attrs().OperState != netlink.OperUp {
		if err := netlink.LinkSetUp(link); err != nil {
			return nil, fmt.Errorf("failed to bring VRF %s up: %w", name, err)
		}
		link, err = nlwrap.LinkByName(name)
		if err != nil {
			return nil, fmt.Errorf("VRF %s not found after LinkSetUp: %w", name, err)
		}
	}
	return link, nil
}

// validateSharedNICVRFTableOwner rejects a routing table already assigned to a
// differently named VRF, preventing two parent NICs from sharing one FIB.
func validateSharedNICVRFTableOwner(name string, table int) error {
	links, err := nlwrap.LinkList()
	if err != nil {
		return fmt.Errorf("failed to list links while validating shared NIC VRF table %d: %w", table, err)
	}
	for _, link := range links {
		vrf, ok := link.(*netlink.Vrf)
		if !ok || int(vrf.Table) != table || link.Attrs().Name == name {
			continue
		}
		return fmt.Errorf("shared NIC VRF table %d is already owned by VRF %s, cannot assign it to %s", table, link.Attrs().Name, name)
	}
	return nil
}

// cleanupStaleIPVlanLinks removes retry leftovers only at the two names
// owned by this attach. It preserves unrelated links and rejects a name
// collision with a non-IPVLAN interface.
func cleanupStaleIPVlanLinks(handle nlwrap.Handle, temporaryName, podInterfaceName string) error {
	podLinks, err := handle.LinkList()
	if err != nil {
		return fmt.Errorf("failed to list pod interfaces: %w", err)
	}
	for _, link := range podLinks {
		name := link.Attrs().Name
		if name != temporaryName && name != podInterfaceName {
			continue
		}
		if link.Type() != "ipvlan" {
			return fmt.Errorf("pod interface %s exists but is %q, expected stale ipvlan", name, link.Type())
		}
		klog.V(2).Infof("shared NIC: removing stale ipvlan child %s from pod ns (previous failed attempt)", name)
		if err := handle.LinkDel(link); err != nil {
			return fmt.Errorf("failed to remove stale ipvlan child %s from pod ns: %w", name, err)
		}
	}
	return nil
}

// validateIPVlanChild verifies that a host-side retry leftover is an L3
// IPVLAN child of the expected physical parent before it can be reused.
func validateIPVlanChild(link netlink.Link, parent netlink.Link) error {
	if link == nil || link.Attrs() == nil {
		return fmt.Errorf("stale ipvlan link is nil")
	}
	ipvl, ok := link.(*netlink.IPVlan)
	if !ok || link.Type() != "ipvlan" {
		return fmt.Errorf("link %s exists but is %q, expected ipvlan", link.Attrs().Name, link.Type())
	}
	if ipvl.Mode != netlink.IPVLAN_MODE_L3 {
		return fmt.Errorf("ipvlan %s has mode %d, expected L3", link.Attrs().Name, ipvl.Mode)
	}
	if link.Attrs().ParentIndex != parent.Attrs().Index {
		return fmt.Errorf("ipvlan %s has parent index %d, expected %s index %d",
			link.Attrs().Name, link.Attrs().ParentIndex, parent.Attrs().Name, parent.Attrs().Index)
	}
	return nil
}

// nsAttachIPVlanL3 creates a pod IPVLAN L3 child while retaining its physical
// parent in a per-MAC host VRF. The VRF table contains the virtual-gateway and
// default routes; the parent link carries the permanent SDN gateway neighbor.
//
// Pod netns, eth1:
//
//	addr   <podIP>/32 dev eth1
//	route  169.254.2.1/32 dev eth1 scope link
//	route  default via 169.254.2.1 dev eth1
//	neigh  169.254.2.1 dev eth1 -> <parent MAC>
//
// Host VRF routing state:
//
//	route  169.254.2.1/32 dev <parent> scope link
//	route  default via 169.254.2.1 dev <parent> onlink
//	neigh  169.254.2.1 dev <parent> -> 12:34:56:78:9a:bc
func nsAttachIPVlanL3(cfg *NICConfig, containerNsPath string) (*resourceapi.NetworkDeviceData, error) {
	if cfg == nil {
		return nil, fmt.Errorf("NICConfig is nil")
	}
	if _, err := net.ParseMAC(cfg.MAC); err != nil {
		return nil, fmt.Errorf("invalid MAC %q: %w", cfg.MAC, err)
	}

	gwIPv4, err := parseSecondaryNICGateway(cfg.GatewayIP, "shared IPVLAN mode")
	if err != nil {
		return nil, err
	}
	podIP := net.ParseIP(cfg.PodIP)
	if podIP == nil {
		return nil, fmt.Errorf("invalid pod IP: %s", cfg.PodIP)
	}
	podIPv4 := podIP.To4()
	if podIPv4 == nil {
		return nil, fmt.Errorf("shared IPVLAN currently requires an IPv4 pod IP, got %s", cfg.PodIP)
	}

	// Serialize all shared-pod setup on this physical NIC so concurrent pods do
	// not race while creating the VRF or updating common parent state. Held for
	// the full parent + child setup below.
	lock := secondaryNICLock(cfg.MAC)
	lock.Lock()
	defer lock.Unlock()

	// Find the exclusive physical NIC, ensure its per-parent shared VRF exists,
	// and enslave the NIC to that VRF.
	parentRouting, err := ensureSharedNICParentVRF(cfg.MAC)
	if err != nil {
		return nil, err
	}

	// Prepare the host side so traffic from this parent's IPVLAN children can
	// reach Azure's virtual gateway without being source-NATed by the host.
	if err := configureSharedNICParentRouting(parentRouting, gwIPv4); err != nil {
		return nil, err
	}

	// Create the pod-facing IPVLAN child, move it into the pod namespace, and
	// configure its IP, gateway neighbor, and routes.
	return attachIPVlanL3ToPod(cfg, containerNsPath, parentRouting.parent, gwIPv4, podIPv4)
}

// configureSharedNICParentRouting programs the parent VRF table, permanent SDN
// gateway neighbor, and host masquerade exemption required by shared pods.
func configureSharedNICParentRouting(parentRouting *sharedNICParentVRF, gwIPv4 net.IP) error {
	if parentRouting == nil {
		return fmt.Errorf("shared NIC parent routing is nil")
	}
	parent := parentRouting.parent

	// Parent has no subnet address, so pin a link route to make the gateway reachable.
	gwLinkRoute := &netlink.Route{
		Dst:       &net.IPNet{IP: gwIPv4, Mask: net.CIDRMask(32, 32)},
		LinkIndex: parent.Attrs().Index,
		Scope:     netlink.SCOPE_LINK,
		Table:     parentRouting.table,
	}
	if err := netlink.RouteReplace(gwLinkRoute); err != nil {
		return fmt.Errorf("failed to add gateway link route for parent %s table %d: %w", parent.Attrs().Name, parentRouting.table, err)
	}

	// Send off-subnet pod egress through the gateway; ONLINK trusts the link route above.
	_, defaultDst, _ := net.ParseCIDR("0.0.0.0/0")
	parentDefault := &netlink.Route{
		Dst:       defaultDst,
		Gw:        gwIPv4,
		LinkIndex: parent.Attrs().Index,
		Table:     parentRouting.table,
		Flags:     int(netlink.FLAG_ONLINK),
	}
	if err := netlink.RouteReplace(parentDefault); err != nil {
		addrs, _ := nlwrap.AddrList(parent, netlink.FAMILY_V4)
		routes, _ := netlink.RouteListFiltered(netlink.FAMILY_V4, &netlink.Route{Table: parentRouting.table}, netlink.RT_FILTER_TABLE)
		klog.Errorf("shared NIC: RouteReplace(default) FAILED for parent %s table %d: err=%v "+
			"route={Dst:%s Gw:%s GwLen:%d LinkIndex:%d Flags:%d Scope:%d Family:%d} "+
			"parent={Name:%s Index:%d OperState:%s Flags:%s MTU:%d HW:%s} "+
			"existing_addrs=%v existing_routes=%v",
			parent.Attrs().Name, parentRouting.table, err,
			parentDefault.Dst, parentDefault.Gw, len(parentDefault.Gw),
			parentDefault.LinkIndex, parentDefault.Flags, parentDefault.Scope, parentDefault.Family,
			parent.Attrs().Name, parent.Attrs().Index, parent.Attrs().OperState,
			parent.Attrs().Flags, parent.Attrs().MTU, parent.Attrs().HardwareAddr,
			addrs, routes)
		return fmt.Errorf("failed to add default route for parent %s table %d: %w", parent.Attrs().Name, parentRouting.table, err)
	}

	// Pin the virtual gateway to Azure's SDN gateway MAC. There is no ARP
	// responder for this address; SmartNIC/VFP intercepts frames sent to it.
	sdnGWMAC, _ := net.ParseMAC(secondaryNICSDNGatewayMAC)
	parentNeigh := &netlink.Neigh{
		LinkIndex:    parent.Attrs().Index,
		IP:           gwIPv4,
		HardwareAddr: sdnGWMAC,
		State:        netlink.NUD_PERMANENT,
	}
	if err := netlink.NeighSet(parentNeigh); err != nil {
		return fmt.Errorf("failed to set gateway neigh on parent %s: %w", parent.Attrs().Name, err)
	}

	// Exempt this parent from host POSTROUTING masquerade so customer traffic
	// keeps the pod source IP expected by Azure SDN policy.
	if err := ensureSharedNICNATExemption(parent.Attrs().Name); err != nil {
		return err
	}
	return nil
}

// attachIPVlanL3ToPod creates or recovers the pod's L3 IPVLAN child,
// moves it to the pod namespace, and configures its address, neighbor, and
// gateway/default routes.
func attachIPVlanL3ToPod(cfg *NICConfig, containerNsPath string, parent netlink.Link, gwIPv4, podIPv4 net.IP) (*resourceapi.NetworkDeviceData, error) {
	// Use a short host-side name while constructing the child; inside the pod it
	// becomes eth1 because eth0 is already owned by the primary CNI.
	ipvlName := fmt.Sprintf("ipvl-%s", truncateUID(cfg.PodUID))
	podInterfaceName := sharedNICInterfacePrefix + "1"

	// Open handles for configuring links directly in the target pod namespace.
	containerNs, err := netns.GetFromPath(containerNsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get netns %s: %w", containerNsPath, err)
	}
	defer containerNs.Close()

	nhPod, err := nlwrap.NewHandleAt(containerNs)
	if err != nil {
		return nil, fmt.Errorf("failed to get netlink handle in netns %s: %w", containerNsPath, err)
	}
	defer nhPod.Close()

	// A child left in the pod namespace may be only partially configured, so
	// remove and recreate it from the known config instead of reusing stale state.
	if err := cleanupStaleIPVlanLinks(nhPod, ipvlName, podInterfaceName); err != nil {
		return nil, fmt.Errorf("failed to prepare pod netns %s for shared secondary NIC attach: %w", containerNsPath, err)
	}

	// Reuse a validated host-side child left by a retry, or create a new L3
	// IPVLAN child connected to the exclusive physical parent.
	ipvl, err := nlwrap.LinkByName(ipvlName)
	if err != nil {
		if !isLinkNotFound(err) {
			return nil, fmt.Errorf("failed to look up ipvlan %s in host netns: %w", ipvlName, err)
		}
		newIPVL := &netlink.IPVlan{
			LinkAttrs: netlink.LinkAttrs{Name: ipvlName, ParentIndex: parent.Attrs().Index},
			Mode:      netlink.IPVLAN_MODE_L3,
		}
		if err := netlink.LinkAdd(newIPVL); err != nil {
			return nil, fmt.Errorf("failed to create ipvlan L3 interface %s on parent %s: %w", ipvlName, parent.Attrs().Name, err)
		}
		ipvl, err = nlwrap.LinkByName(ipvlName)
		if err != nil {
			return nil, fmt.Errorf("ipvlan %s not found after creation in host netns: %w", ipvlName, err)
		}
	} else {
		if err := validateIPVlanChild(ipvl, parent); err != nil {
			return nil, err
		}
		// A previous attach can leave the child on the host if LinkAdd succeeds
		// but the operation fails or is interrupted before LinkSetNsFd.
		klog.V(2).Infof("shared NIC: reusing existing ipvlan child %s in host netns (previous failed attempt)", ipvlName)
	}

	// Transfer ownership of the child from the host to the pod namespace.
	if err := netlink.LinkSetNsFd(ipvl, int(containerNs)); err != nil {
		return nil, fmt.Errorf("failed to move ipvlan %s to pod netns: %w", ipvlName, err)
	}

	nsLink, err := nhPod.LinkByName(ipvlName)
	if err != nil {
		return nil, fmt.Errorf("ipvlan interface %s not found in pod netns: %w", ipvlName, err)
	}
	// Expose the secondary NIC child to the workload using the expected pod name.
	if err := nhPod.LinkSetName(nsLink, podInterfaceName); err != nil {
		return nil, fmt.Errorf("failed to rename %s to %s: %w", ipvlName, podInterfaceName, err)
	}
	nsLink, err = nhPod.LinkByName(podInterfaceName)
	if err != nil {
		return nil, fmt.Errorf("renamed interface %s not found in pod netns: %w", podInterfaceName, err)
	}

	// Assign the customer pod IP as a /32; all reachability is explicit through
	// the virtual gateway rather than an inferred connected subnet.
	if err := nhPod.AddrAdd(nsLink, &netlink.Addr{IPNet: &net.IPNet{IP: podIPv4, Mask: net.CIDRMask(32, 32)}}); err != nil && !errors.Is(err, syscall.EEXIST) {
		return nil, fmt.Errorf("failed to add IP %s/32 to pod %s: %w", cfg.PodIP, podInterfaceName, err)
	}
	if err := nhPod.LinkSetUp(nsLink); err != nil {
		return nil, fmt.Errorf("failed to bring up %s in pod ns: %w", podInterfaceName, err)
	}

	// Inside the pod, resolve the virtual gateway to the real parent MAC. IPVLAN
	// siblings share this MAC, which also preserves same-host pod delivery.
	gwMAC := parent.Attrs().HardwareAddr
	if len(gwMAC) == 0 {
		return nil, fmt.Errorf("parent NIC %s has empty MAC address", parent.Attrs().Name)
	}
	podNeigh := &netlink.Neigh{
		LinkIndex:    nsLink.Attrs().Index,
		IP:           gwIPv4,
		HardwareAddr: gwMAC,
		State:        netlink.NUD_PERMANENT,
	}
	if err := nhPod.NeighSet(podNeigh); err != nil {
		return nil, fmt.Errorf("failed to set static neighbor %s -> %s on pod %s: %w", cfg.GatewayIP, gwMAC, podInterfaceName, err)
	}

	// Make the virtual gateway reachable from the pod's /32 address.
	gwRoute := &netlink.Route{
		Dst:       &net.IPNet{IP: gwIPv4, Mask: net.CIDRMask(32, 32)},
		LinkIndex: nsLink.Attrs().Index,
		Scope:     netlink.SCOPE_LINK,
	}
	if err := nhPod.RouteAdd(gwRoute); err != nil && !errors.Is(err, syscall.EEXIST) {
		return nil, fmt.Errorf("failed to add gateway route in pod ns: %w", err)
	}
	// Override the primary-CNI default so customer traffic exits through eth1.
	_, defaultDst, _ := net.ParseCIDR("0.0.0.0/0")
	podDefault := &netlink.Route{Dst: defaultDst, Gw: gwIPv4, LinkIndex: nsLink.Attrs().Index}
	if err := nhPod.RouteReplace(podDefault); err != nil {
		return nil, fmt.Errorf("failed to replace default route via %s in pod ns: %w", cfg.GatewayIP, err)
	}

	return &resourceapi.NetworkDeviceData{
		InterfaceName:   podInterfaceName,
		HardwareAddress: parent.Attrs().HardwareAddr.String(),
		IPs:             []string{fmt.Sprintf("%s/32", cfg.PodIP)},
	}, nil
}

// truncateUID returns at most the first eight UID characters for temporary
// interface naming within Linux's interface-name length limit.
func truncateUID(uid string) string {
	if len(uid) > 8 {
		return uid[:8]
	}
	return uid
}
