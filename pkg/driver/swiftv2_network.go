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
	"os/exec"
	"strings"
	"sync"
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

	// swiftV2DelegatedIfPrefix is the interface-name prefix for a shared-mode
	// delegated NIC (ipvlan child) inside the pod namespace. The full name is
	// built at attach time (see nsAttachIPVlanL3) as prefix+index; the single
	// SwiftV2 delegated NIC takes index 1 ("eth1"), since eth0 is the infra NIC
	// the cluster CNI installs before the NRI hook runs. This mirrors the CNI
	// "eth"+index scheme. Dedicated NICs are not renamed; they keep their host
	// interface name, matching CNI's FrontendNIC behavior.
	swiftV2DelegatedIfPrefix = "eth"

	// swiftV2VRFPrefix is the prefix for the per-shared-parent-NIC host VRF.
	// Linux interface names are capped at 15 bytes, so the full name is the
	// short prefix plus the 12-character normalized MAC: sv2<mac-no-colons>.
	swiftV2VRFPrefix = "sv2"

	// swiftV2SDNGatewayMAC is the well-known Azure SDN gateway MAC.
	// Used as the static neigh entry for swiftV2VirtualGW on the host parent
	// with the per-MAC routing table. SmartNIC/VFP intercepts SwiftV2 egress on
	// the delegated NIC and routes packets according to SDN policy.
	swiftV2SDNGatewayMAC = "12:34:56:78:9a:bc"
)

// swiftV2ParentLocks serializes attach/cleanup operations per shared parent
// NIC. Concurrent attaches for two pods sharing the same parent NIC must not
// race on VRF creation, parent enslave, route/neighbor programming, or stale
// ipvlan cleanup.
var swiftV2ParentLocks sync.Map // map[normalizedMAC]*sync.Mutex

func swiftV2ParentLock(mac string) *sync.Mutex {
	key := swiftV2MACKey(mac)
	if v, ok := swiftV2ParentLocks.Load(key); ok {
		return v.(*sync.Mutex)
	}
	mu := &sync.Mutex{}
	actual, _ := swiftV2ParentLocks.LoadOrStore(key, mu)
	return actual.(*sync.Mutex)
}

// swiftV2MACKey returns a stable, lowercased, colon-stripped key for the MAC,
// suitable for use as a netns name suffix and lock map key. The input must
// already have been validated by the caller via net.ParseMAC; this returns
// the input unchanged when parsing fails (callers gate on validation first).
func swiftV2MACKey(mac string) string {
	parsed, err := net.ParseMAC(mac)
	if err != nil {
		return strings.ToLower(strings.NewReplacer(":", "", "-", "").Replace(mac))
	}
	return strings.ReplaceAll(strings.ToLower(parsed.String()), ":", "")
}

// swiftV2VRFName returns the per-shared-parent-NIC host VRF name for the MAC.
func swiftV2VRFName(mac string) string {
	return swiftV2VRFPrefix + swiftV2MACKey(mac)
}

func swiftV2VRFTable(mac string) (int, error) {
	parsed, err := net.ParseMAC(mac)
	if err != nil {
		return 0, fmt.Errorf("invalid MAC address %s: %w", mac, err)
	}
	// Keep table IDs outside the reserved low range while making them stable per
	// parent MAC. A collision is possible but very unlikely for node-local NICs.
	return int(0x100000 + (crc32.ChecksumIEEE([]byte(parsed.String())) & 0x7fffffff)), nil
}

type swiftV2ParentVRF struct {
	name   string
	table  int
	parent netlink.Link
	unlock func()
}

func (v *swiftV2ParentVRF) Close() {
	if v != nil && v.unlock != nil {
		v.unlock()
	}
}

// findLinkByMAC finds a network interface in the host namespace by MAC.
// Returns the link and nil on success, or nil and an error if not found.
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

func ensureSwiftV2ParentVRF(mac string) (*swiftV2ParentVRF, error) {
	if _, err := net.ParseMAC(mac); err != nil {
		return nil, fmt.Errorf("invalid MAC address %s: %w", mac, err)
	}

	lock := swiftV2ParentLock(mac)
	lock.Lock()
	success := false
	defer func() {
		if !success {
			lock.Unlock()
		}
	}()

	parent, err := findLinkByMAC(mac)
	if err != nil {
		return nil, err
	}

	vrfName := swiftV2VRFName(mac)
	table, err := swiftV2VRFTable(mac)
	if err != nil {
		return nil, err
	}

	// Host VRF datapath: ensure the per-parent VRF exists and enslave the parent
	// NIC to it. Table selection for ipvlan L3 child egress then follows the
	// kernel l3mdev rule (priority 1000) via the VRF master, with no oif policy
	// rule and no VRF-local source anchors required.
	vrf, err := ensureSwiftV2VRFLink(vrfName, table)
	if err != nil {
		return nil, err
	}

	masterIndex := parent.Attrs().MasterIndex
	if masterIndex != 0 && masterIndex != vrf.Attrs().Index {
		return nil, fmt.Errorf("parent NIC %s (MAC %s) is already enslaved to unexpected master index %d, expected SwiftV2 VRF %s index %d",
			parent.Attrs().Name, mac, masterIndex, vrfName, vrf.Attrs().Index)
	}
	if masterIndex != vrf.Attrs().Index {
		klog.V(2).Infof("SwiftV2: enslaving parent NIC %s (MAC %s) to VRF %s table %d",
			parent.Attrs().Name, mac, vrfName, table)
		if err := netlink.LinkSetMaster(parent, vrf); err != nil {
			return nil, fmt.Errorf("failed to enslave parent NIC %s (MAC %s) to VRF %s: %w", parent.Attrs().Name, mac, vrfName, err)
		}
		parent, err = findLinkByMAC(mac)
		if err != nil {
			return nil, fmt.Errorf("parent NIC (MAC %s) not found after VRF enslave: %w", mac, err)
		}
	}

	if parent.Attrs().OperState != netlink.OperUp {
		klog.V(2).Infof("SwiftV2: parent NIC %s (MAC %s) is %s, bringing it UP",
			parent.Attrs().Name, mac, parent.Attrs().OperState)
		if err := netlink.LinkSetUp(parent); err != nil {
			return nil, fmt.Errorf("failed to bring parent NIC %s up: %w", parent.Attrs().Name, err)
		}
		parent, err = findLinkByMAC(mac)
		if err != nil {
			return nil, fmt.Errorf("parent NIC (MAC %s) not found after LinkSetUp: %w", mac, err)
		}
	}

	success = true
	return &swiftV2ParentVRF{
		name:   vrfName,
		table:  table,
		parent: parent,
		unlock: lock.Unlock,
	}, nil
}

func ensureSwiftV2VRFLink(name string, table int) (netlink.Link, error) {
	link, err := nlwrap.LinkByName(name)
	if err != nil {
		if !isLinkNotFound(err) {
			return nil, fmt.Errorf("failed to look up VRF %s: %w", name, err)
		}
		newVRF := &netlink.Vrf{
			LinkAttrs: netlink.LinkAttrs{Name: name},
			Table:     uint32(table),
		}
		if err := netlink.LinkAdd(newVRF); err != nil {
			return nil, fmt.Errorf("failed to create VRF %s table %d: %w", name, table, err)
		}
		link, err = nlwrap.LinkByName(name)
		if err != nil {
			return nil, fmt.Errorf("VRF %s not found after creation: %w", name, err)
		}
	}

	vrf, ok := link.(*netlink.Vrf)
	if !ok || link.Type() != "vrf" {
		return nil, fmt.Errorf("link %s exists but is %q, expected vrf", name, link.Type())
	}
	if int(vrf.Table) != table {
		return nil, fmt.Errorf("VRF %s exists with table %d, expected %d", name, vrf.Table, table)
	}
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

func isLinkNotFound(err error) bool {
	var notFound netlink.LinkNotFoundError
	return errors.As(err, &notFound)
}

func releaseSwiftV2VRFForDedicated(hostLink netlink.Link, mac string) (netlink.Link, error) {
	if hostLink == nil || hostLink.Attrs() == nil {
		return nil, fmt.Errorf("host link for MAC %s is nil", mac)
	}

	// TODO: The common dedicated-only path has no master. If we decide orphaned
	// per-MAC VRF cleanup is not needed for no-master links, return early before
	// computing the VRF table and doing the VRF LinkByName lookup.
	vrfName := swiftV2VRFName(mac)
	vrfTable, err := swiftV2VRFTable(mac)
	if err != nil {
		return nil, err
	}
	if err := cleanupSwiftV2VRFParentState(hostLink, vrfTable); err != nil {
		return nil, err
	}

	vrf, vrfErr := nlwrap.LinkByName(vrfName)
	masterIndex := hostLink.Attrs().MasterIndex
	if masterIndex != 0 {
		if vrfErr != nil {
			return nil, fmt.Errorf("dedicated NIC %s (MAC %s) is enslaved to master index %d, but expected SwiftV2 VRF %s was not found: %w",
				hostLink.Attrs().Name, mac, masterIndex, vrfName, vrfErr)
		}
		if masterIndex != vrf.Attrs().Index {
			return nil, fmt.Errorf("dedicated NIC %s (MAC %s) is enslaved to unexpected master index %d, expected SwiftV2 VRF %s index %d",
				hostLink.Attrs().Name, mac, masterIndex, vrfName, vrf.Attrs().Index)
		}

		klog.V(2).Infof("SwiftV2 dedicated NIC: detaching %s (MAC %s) from shared VRF %s before pod move",
			hostLink.Attrs().Name, mac, vrfName)
		if err := netlink.LinkSetNoMaster(hostLink); err != nil {
			return nil, fmt.Errorf("failed to detach dedicated NIC %s (MAC %s) from SwiftV2 VRF %s: %w",
				hostLink.Attrs().Name, mac, vrfName, err)
		}
		hostLink, err = findLinkByMAC(mac)
		if err != nil {
			return nil, fmt.Errorf("dedicated NIC (MAC %s) not found after detaching from SwiftV2 VRF %s: %w", mac, vrfName, err)
		}
	} else if !isLinkNotFound(vrfErr) {
		return nil, fmt.Errorf("failed to look up SwiftV2 VRF %s before dedicated attach: %w", vrfName, vrfErr)
	}

	if vrfErr == nil {
		klog.V(2).Infof("SwiftV2 dedicated NIC: deleting shared VRF %s before moving NIC %s (MAC %s) into pod netns",
			vrfName, hostLink.Attrs().Name, mac)
		if err := netlink.LinkDel(vrf); err != nil && !isLinkNotFound(err) {
			return nil, fmt.Errorf("failed to delete SwiftV2 VRF %s before dedicated attach for NIC %s (MAC %s): %w",
				vrfName, hostLink.Attrs().Name, mac, err)
		}
	}
	return hostLink, nil
}

func cleanupSwiftV2VRFTableRoutes(table int, linkIndex int) error {
	routes, err := netlink.RouteListFiltered(netlink.FAMILY_V4, &netlink.Route{Table: table}, netlink.RT_FILTER_TABLE)
	if err != nil {
		return fmt.Errorf("failed to list SwiftV2 VRF table %d routes before dedicated attach: %w", table, err)
	}
	for _, route := range routes {
		if linkIndex != 0 && route.LinkIndex != linkIndex {
			continue
		}
		route := route
		if err := netlink.RouteDel(&route); err != nil && !errors.Is(err, syscall.ESRCH) && !errors.Is(err, syscall.ENOENT) {
			return fmt.Errorf("failed to delete SwiftV2 VRF table %d route %v before dedicated attach: %w", table, route, err)
		}
	}
	return nil
}

func cleanupSwiftV2VRFParentState(parent netlink.Link, table int) error {
	if err := cleanupSwiftV2VRFTableRoutes(table, parent.Attrs().Index); err != nil {
		return err
	}
	cleanupSwiftV2NATExemption(parent.Attrs().Name)

	gwIP := net.ParseIP(swiftV2VirtualGW).To4()
	if gwIP == nil {
		return fmt.Errorf("invalid SwiftV2 virtual gateway IP %s", swiftV2VirtualGW)
	}
	neigh := &netlink.Neigh{LinkIndex: parent.Attrs().Index, IP: gwIP}
	if err := netlink.NeighDel(neigh); err != nil && !errors.Is(err, syscall.ENOENT) && !errors.Is(err, syscall.ESRCH) {
		klog.V(2).Infof("SwiftV2 dedicated NIC: best-effort gateway neighbor cleanup on %s failed: %v", parent.Attrs().Name, err)
	}
	return nil
}

// runIPTables runs an iptables command, passing -w so it waits (up to 5s) for
// the xtables lock instead of failing when another component (kube-proxy,
// ip-masq-agent, CNS) is mutating the table concurrently.
func runIPTables(args ...string) error {
	full := append([]string{"-w", "5"}, args...)
	output, err := exec.Command("iptables", full...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("iptables %s failed: %w: %s", strings.Join(full, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

// swiftV2NATExemptionExists reports whether the masquerade-exemption rule for
// parentName is already present in nat POSTROUTING. `iptables -C` exits non-zero
// when the rule is absent, which is a normal "not present" answer rather than an
// execution error, so only a zero exit is treated as present.
func swiftV2NATExemptionExists(parentName string) bool {
	return exec.Command("iptables", "-w", "5", "-t", "nat", "-C", "POSTROUTING", "-o", parentName, "-j", "ACCEPT").Run() == nil
}

// ensureSwiftV2NATExemption makes sure the masquerade-exemption rule for the
// given delegated parent NIC is present in nat POSTROUTING, so delegated
// customer traffic leaving that NIC keeps the real pod source IP instead of
// being SNATed by host masquerade (ip-masq-agent / kube-proxy).
//
// It is check-first and idempotent: when the rule already exists it does
// nothing, so it is safe to call both synchronously on every shared-pod attach
// and repeatedly from the periodic CNS reconcile without churning the host nat
// table or stacking duplicates. When absent it is inserted at the top of
// POSTROUTING so it runs ahead of any later MASQUERADE rule.
func ensureSwiftV2NATExemption(parentName string) error {
	if swiftV2NATExemptionExists(parentName) {
		return nil
	}
	if err := runIPTables("-t", "nat", "-I", "POSTROUTING", "1", "-o", parentName, "-j", "ACCEPT"); err != nil {
		return fmt.Errorf("failed to add SwiftV2 NAT exemption for %s: %w", parentName, err)
	}
	return nil
}

func cleanupSwiftV2NATExemption(parentName string) {
	for {
		if err := runIPTables("-t", "nat", "-D", "POSTROUTING", "-o", parentName, "-j", "ACCEPT"); err != nil {
			return
		}
	}
}

// nsAttachIPVlanL3 attaches a SwiftV2 shared-mode pod to its delegated parent
// NIC by creating an ipvlan L3 child off the parent and moving the child into
// the pod network namespace.
//
// Design:
//
//	One host VRF per shared parent NIC. The parent NIC stays visible in the
//	host namespace and is enslaved to a per-MAC VRF bound to a per-parent
//	routing table. That table owns the Azure virtual gateway route, default
//	route, and static gateway neighbor for the parent.
//
// Why host VRF and not host-ns source policy routing:
//
//	The parent owns the off-subnet routing decision for its ipvlan L3 children.
//	Enslaving the parent to a VRF selects the per-parent table through the
//	kernel l3mdev rule, so overlapping customer prefixes on different parent
//	NICs never collide and no source-prefix rules are required.
//
// Expected end state programmed by this function (example: pod IP 10.0.0.5,
// gateway 169.254.2.1, Azure SDN gateway MAC 12:34:56:78:9a:bc):
//
//	Pod netns, delegated NIC eth1:
//	  addr   10.0.0.5/32 dev eth1
//	  route  169.254.2.1 dev eth1 scope link              # gateway on-link
//	  route  default via 169.254.2.1 dev eth1             # all egress via eth1 (overrides CNI eth0 default)
//	  neigh  169.254.2.1 dev eth1 -> <parent MAC>         # gateway = parent's REAL MAC (same-host ipvlan delivery)
//
//	Host, per-parent VRF table T (parent enslaved to VRF sv2<mac>, T chosen by l3mdev):
//	  route  169.254.2.1 dev <parent> scope link          # gateway on-link
//	  route  default via 169.254.2.1 dev <parent> onlink  # off-subnet egress to the Azure gateway
//	  neigh  169.254.2.1 dev <parent> -> 12:34:56:78:9a:bc # gateway = SDN gateway MAC (SmartNIC/VFP intercept)
//
// The same gateway IP resolves to two different MACs on purpose: the parent's
// real MAC inside the pod (ipvlan slaves share it; needed for same-host
// cross-pod delivery) and the SDN gateway MAC on the host (SmartNIC/VFP
// intercepts egress framed to it).
func nsAttachIPVlanL3(cfg *NICConfig, containerNsPath string) (*resourceapi.NetworkDeviceData, error) {
	if cfg == nil {
		return nil, fmt.Errorf("NICConfig is nil")
	}
	if _, err := net.ParseMAC(cfg.MAC); err != nil {
		return nil, fmt.Errorf("invalid MAC %q: %w", cfg.MAC, err)
	}

	gwIP := net.ParseIP(cfg.GatewayIP)
	if gwIP == nil {
		return nil, fmt.Errorf("invalid gateway IP: %s", cfg.GatewayIP)
	}
	gwIPv4 := gwIP.To4()
	if gwIPv4 == nil {
		return nil, fmt.Errorf("SwiftV2 shared ipvlan currently requires an IPv4 gateway IP, got %s", cfg.GatewayIP)
	}
	podIP := net.ParseIP(cfg.PodIP)
	if podIP == nil {
		return nil, fmt.Errorf("invalid pod IP: %s", cfg.PodIP)
	}
	podIPv4 := podIP.To4()
	if podIPv4 == nil {
		return nil, fmt.Errorf("SwiftV2 shared ipvlan currently requires an IPv4 pod IP, got %s", cfg.PodIP)
	}

	parentRouting, err := ensureSwiftV2ParentVRF(cfg.MAC)
	if err != nil {
		return nil, err
	}
	defer parentRouting.Close()
	parent := parentRouting.parent

	// VRF table, route 1 of 2: gateway /32 scope-link route.
	// Makes the gateway reachable out the parent so the default below can use it
	// as an on-link next hop.
	gwLinkRoute := &netlink.Route{
		Dst:       &net.IPNet{IP: gwIPv4, Mask: net.CIDRMask(32, 32)},
		LinkIndex: parent.Attrs().Index,
		Scope:     netlink.SCOPE_LINK,
		Table:     parentRouting.table,
	}
	if err := netlink.RouteReplace(gwLinkRoute); err != nil {
		return nil, fmt.Errorf("failed to add gateway link route for parent %s table %d: %w", parent.Attrs().Name, parentRouting.table, err)
	}

	// VRF table, route 2 of 2: default via the gateway (off-subnet egress).
	// ONLINK because the gateway is only reachable via the scope-link route above.
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
		klog.Errorf("SwiftV2: RouteReplace(default) FAILED for parent %s table %d: err=%v "+
			"route={Dst:%s Gw:%s GwLen:%d LinkIndex:%d Flags:%d Scope:%d Family:%d} "+
			"parent={Name:%s Index:%d OperState:%s Flags:%s MTU:%d HW:%s} "+
			"existing_addrs=%v existing_routes=%v",
			parent.Attrs().Name, parentRouting.table, err,
			parentDefault.Dst, parentDefault.Gw, len(parentDefault.Gw),
			parentDefault.LinkIndex, parentDefault.Flags, parentDefault.Scope, parentDefault.Family,
			parent.Attrs().Name, parent.Attrs().Index, parent.Attrs().OperState,
			parent.Attrs().Flags, parent.Attrs().MTU, parent.Attrs().HardwareAddr,
			addrs, routes)
		return nil, fmt.Errorf("failed to add default route for parent %s table %d: %w", parent.Attrs().Name, parentRouting.table, err)
	}

	// Parent neigh: pin the gateway to the Azure SDN gateway MAC (no real ARP
	// responder; SmartNIC/VFP intercepts egress framed to it).
	sdnGWMAC, _ := net.ParseMAC(swiftV2SDNGatewayMAC)
	parentNeigh := &netlink.Neigh{
		LinkIndex:    parent.Attrs().Index,
		IP:           gwIPv4,
		HardwareAddr: sdnGWMAC,
		State:        netlink.NUD_PERMANENT,
	}
	if err := netlink.NeighSet(parentNeigh); err != nil {
		return nil, fmt.Errorf("failed to set gateway neigh on parent %s: %w", parent.Attrs().Name, err)
	}
	if err := ensureSwiftV2NATExemption(parent.Attrs().Name); err != nil {
		return nil, err
	}

	// --- Pod namespace setup ---

	ipvlName := fmt.Sprintf("ipvl-%s", truncateUID(cfg.PodUID))

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

	// Idempotent cleanup of stale pod-ns state from a previous failed attempt.
	// SwiftV2 plumbs a single delegated ipvlan child per pod, so any ipvlan-type
	// link already in the pod ns is a leftover of ours (whether still named
	// ipvl-<uid> or already renamed to eth<N>) and is removed.
	podLinks, err := nhPod.LinkList()
	if err != nil {
		return nil, fmt.Errorf("failed to list interfaces in pod netns %s: %w", containerNsPath, err)
	}
	for _, l := range podLinks {
		if l.Type() == "ipvlan" {
			klog.V(2).Infof("SwiftV2: removing stale ipvlan child %s from pod ns (previous failed attempt)", l.Attrs().Name)
			_ = nhPod.LinkDel(l)
		}
	}

	// The single delegated NIC takes index 1 ("eth1"): eth0 is the infra NIC the
	// cluster CNI installs before the NRI hook runs, mirroring the CNI "eth"+index
	// scheme. A multi-delegated-NIC design would compute the index in the NRI hook
	// loop instead of fixing it to 1 here.
	delegatedIfName := swiftV2DelegatedIfPrefix + "1"

	// --- ipvlan child: create from the host-visible parent, then move to pod ---

	// Reuse a leftover ipvl-<uid> from a prior failed attempt if present in the
	// host namespace. Otherwise create a fresh ipvlan L3 child.
	ipvl, err := nlwrap.LinkByName(ipvlName)
	if err != nil {
		var notFound netlink.LinkNotFoundError
		if !errors.As(err, &notFound) {
			return nil, fmt.Errorf("failed to look up ipvlan %s in host netns: %w", ipvlName, err)
		}

		// L3 places FIB/RPF/netfilter hooks for slave traffic in the parent's
		// namespace. The parent is host-visible and enslaved to the per-MAC VRF
		// programmed above.
		newIPVL := &netlink.IPVlan{
			LinkAttrs: netlink.LinkAttrs{
				Name:        ipvlName,
				ParentIndex: parent.Attrs().Index,
			},
			Mode: netlink.IPVLAN_MODE_L3,
		}
		if err := netlink.LinkAdd(newIPVL); err != nil {
			return nil, fmt.Errorf("failed to create ipvlan L3 interface %s on parent %s: %w", ipvlName, parent.Attrs().Name, err)
		}
		ipvl, err = nlwrap.LinkByName(ipvlName)
		if err != nil {
			return nil, fmt.Errorf("ipvlan %s not found after creation in host netns: %w", ipvlName, err)
		}
	} else {
		klog.V(2).Infof("SwiftV2: reusing existing ipvlan child %s in host netns (previous failed attempt)", ipvlName)
	}

	// Move ipvlan child from host to the pod ns.
	if err := netlink.LinkSetNsFd(ipvl, int(containerNs)); err != nil {
		return nil, fmt.Errorf("failed to move ipvlan %s to pod netns: %w", ipvlName, err)
	}

	// --- Pod namespace operations on the moved ipvlan child ---

	nsLink, err := nhPod.LinkByName(ipvlName)
	if err != nil {
		return nil, fmt.Errorf("ipvlan interface %s not found in pod netns: %w", ipvlName, err)
	}

	if err := nhPod.LinkSetName(nsLink, delegatedIfName); err != nil {
		return nil, fmt.Errorf("failed to rename %s to %s: %w", ipvlName, delegatedIfName, err)
	}

	nsLink, err = nhPod.LinkByName(delegatedIfName)
	if err != nil {
		return nil, fmt.Errorf("renamed interface %s not found in pod netns: %w", delegatedIfName, err)
	}

	if err := nhPod.AddrAdd(nsLink, &netlink.Addr{
		IPNet: &net.IPNet{IP: podIPv4, Mask: net.CIDRMask(32, 32)},
	}); err != nil && !errors.Is(err, syscall.EEXIST) {
		return nil, fmt.Errorf("failed to add IP %s/32 to pod %s: %w", cfg.PodIP, delegatedIfName, err)
	}

	if err := nhPod.LinkSetUp(nsLink); err != nil {
		return nil, fmt.Errorf("failed to bring up %s in pod ns: %w", delegatedIfName, err)
	}

	// Pod-side static neigh for the virtual gateway.
	//
	// We use the parent NIC's real MAC (not the SDN gateway MAC) for the
	// pod-side neigh entry. Reason: ipvlan slaves share the parent's MAC,
	// and for same-host cross-slave traffic the ipvlan bridge sub-mode
	// delivers the frame directly between slaves. The receiving slave's
	// L2 admittance check passes only if dst MAC matches its own (== parent's)
	// MAC. A synthetic placeholder MAC would silently drop intra-host
	// cross-pod traffic before the IP stack ever sees it.
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
		return nil, fmt.Errorf("failed to set static neighbor %s -> %s on pod %s: %w", cfg.GatewayIP, gwMAC, delegatedIfName, err)
	}

	// Pod route 1 of 2: gateway /32 scope-link route on eth1.
	// The pod IP is a /32, so this makes the gateway reachable for the default
	// route below.
	gwRoute := &netlink.Route{
		Dst:       &net.IPNet{IP: gwIPv4, Mask: net.CIDRMask(32, 32)},
		LinkIndex: nsLink.Attrs().Index,
		Scope:     netlink.SCOPE_LINK,
	}
	if err := nhPod.RouteAdd(gwRoute); err != nil && !errors.Is(err, syscall.EEXIST) {
		return nil, fmt.Errorf("failed to add gateway route in pod ns: %w", err)
	}

	// Pod route 2 of 2: default via the gateway on eth1.
	// Overrides the CNI-installed eth0 default so all pod egress leaves via eth1.
	// RouteReplace (not Add) because the CNI default already exists.
	podDefault := &netlink.Route{
		Dst:       defaultDst,
		Gw:        gwIPv4,
		LinkIndex: nsLink.Attrs().Index,
	}
	if err := nhPod.RouteReplace(podDefault); err != nil {
		return nil, fmt.Errorf("failed to replace default route via %s in pod ns: %w", cfg.GatewayIP, err)
	}

	return &resourceapi.NetworkDeviceData{
		InterfaceName:   delegatedIfName,
		HardwareAddress: parent.Attrs().HardwareAddr.String(),
		IPs:             []string{fmt.Sprintf("%s/32", cfg.PodIP)},
	}, nil
}

// cleanupIPVlanL3 is intentionally per-pod best-effort cleanup only. The ipvlan
// child is destroyed with the pod netns. The parent NIC, VRF, per-parent table,
// gateway neighbor, and NAT exemption are per-parent shared state and stay in
// place for subsequent pods on the same NIC.
func cleanupIPVlanL3(cfg *NICConfig) {
	if cfg == nil {
		return
	}
	if _, err := net.ParseMAC(cfg.MAC); err != nil {
		klog.Warningf("SwiftV2 cleanup: invalid MAC %q: %v", cfg.MAC, err)
		return
	}
	klog.V(4).Infof("SwiftV2 cleanup: shared pod IP %s removed; parent routing state for MAC %s remains shared", cfg.PodIP, cfg.MAC)
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

// nsAttachDedicatedNIC moves a dedicated physical NIC (identified by MAC address)
// into the pod network namespace, assigns IP addresses and routes.
// This matches the CNI SecondaryEndpointClient behavior:
//   - Lookup NIC by MAC (not name)
//   - Move NIC into pod netns (no rename — keeps original name)
//   - Bring link up
//   - Assign IP addresses
//   - Add routes: virtual GW /32 scope link + default via virtual GW
//   - Issue DHCP discover for DNS wireserver mapping (synchronous; failure is fatal)
func nsAttachDedicatedNIC(cfg *NICConfig, containerNsPath string) (networkData *resourceapi.NetworkDeviceData, retErr error) {
	if cfg == nil {
		return nil, fmt.Errorf("NICConfig is nil")
	}

	// Find NIC by MAC address on the host, matching CNI's GetNetworkInterfaceByMac.
	// NRI owns the dedicated NIC move, so the NIC must be present on the host; if
	// it is not found there, that is an error.
	hostLink, err := findLinkByMAC(cfg.MAC)
	if err != nil {
		return nil, fmt.Errorf("dedicated NIC with MAC %s not found on host: %w", cfg.MAC, err)
	}

	// If this NIC is currently a shared parent enslaved to a per-MAC host VRF,
	// detach it and tear down the shared routing state before moving it into the
	// dedicated pod. For a NIC that was never shared this is a no-op.
	targetMAC, _ := net.ParseMAC(cfg.MAC) // already validated by findLinkByMAC
	hostLink, err = releaseSwiftV2VRFForDedicated(hostLink, targetMAC.String())
	if err != nil {
		return nil, err
	}

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

	// The NIC has now left the host and lives in the pod netns. Mirror the CNI
	// SecondaryEndpointClient contract (newEndpointImpl defers cleanup that calls
	// DeleteEndpoints, returning a FrontendNIC to the host): if any step below
	// fails — including the fatal DHCP discover — roll the move back by returning
	// the NIC to the host, so a retry finds it via findLinkByMAC instead of
	// stranding it in a failed pod netns. cleanupDedicatedNIC is a no-op if the
	// NIC is already gone.
	defer func() {
		if retErr != nil {
			klog.V(2).Infof("SwiftV2 dedicated NIC: attach failed after move, returning NIC (MAC %s) to host: %v", cfg.MAC, retErr)
			cleanupDedicatedNIC(containerNsPath, cfg.MAC)
		}
	}()

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
	networkData = &resourceapi.NetworkDeviceData{
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

	// A dedicated NIC with no usable address cannot carry pod traffic. Fail the
	// attach (the deferred rollback above returns the NIC to the host) rather
	// than leaving the pod with a dead delegated interface.
	if len(networkData.IPs) == 0 {
		return nil, fmt.Errorf("dedicated NIC %s (MAC %s): no valid address assigned from %v", ifName, cfg.MAC, cfg.Addresses)
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

	// Issue DHCP discover synchronously to create the DNS mapping in the host via
	// wireserver. This matches the CNI SecondaryEndpointClient behavior: a failed
	// discover is fatal and fails the attach with "network not ready" so the NIC
	// plumbing is retried rather than leaving the pod without working DNS. The CNI
	// uses a 3-second timeout for this operation.
	if err := issueDHCPDiscover(containerNsPath, ifName, targetMAC); err != nil {
		return nil, fmt.Errorf("SwiftV2 dedicated NIC: network not ready - failed to issue DHCP discover for %s (MAC %s): %w", ifName, cfg.MAC, err)
	}

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

// truncateUID returns the first 8 characters of a UID string for use in
// interface naming. If the UID is shorter than 8 characters, returns the whole string.
func truncateUID(uid string) string {
	if len(uid) > 8 {
		return uid[:8]
	}
	return uid
}
