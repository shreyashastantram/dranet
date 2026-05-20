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
	"runtime"
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

	// swiftV2NSPrefix is the prefix for the per-shared-parent-NIC network
	// namespace name. The full name is swiftv2-<mac-no-colons>, e.g.
	// "swiftv2-6045bd70e489". One namespace is created per shared parent NIC
	// and persists for the lifetime of the NIC on the node — it holds the
	// parent NIC, the Azure SDN gateway route/neigh, and per-pod /32 routes,
	// so cross-subnet routing is isolated per tenant (overlapping customer
	// prefixes on different parent NICs do not collide because each lives
	// in its own routing domain).
	swiftV2NSPrefix = "swiftv2-"

	// swiftV2SDNGatewayMAC is the well-known Azure SDN gateway MAC.
	// Used as the static neigh entry for swiftV2VirtualGW inside the per-MAC
	// interface namespace. SmartNIC/VFP intercepts SwiftV2 egress regardless
	// of L2 dst, so the exact MAC value only matters as a kernel ARP-cache
	// placeholder that prevents the kernel from dropping frames before egress.
	swiftV2SDNGatewayMAC = "12:34:56:78:9a:bc"
)

// swiftV2NSLocks serializes attach/cleanup operations per shared parent NIC.
// Concurrent attaches for two pods sharing the same parent NIC must not race
// on namespace creation, parent-NIC move, gateway route programming, or
// per-pod /32 route add — all of which are mutating operations on the same
// per-MAC interface namespace.
var swiftV2NSLocks sync.Map // map[normalizedMAC]*sync.Mutex

func swiftV2NSLock(mac string) *sync.Mutex {
	key := swiftV2MACKey(mac)
	if v, ok := swiftV2NSLocks.Load(key); ok {
		return v.(*sync.Mutex)
	}
	mu := &sync.Mutex{}
	actual, _ := swiftV2NSLocks.LoadOrStore(key, mu)
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

// swiftV2NSName returns the per-shared-parent-NIC netns name for the given MAC.
func swiftV2NSName(mac string) string {
	return swiftV2NSPrefix + swiftV2MACKey(mac)
}

// ensureSwiftV2NS returns a handle to the named netns swiftv2-<mac>, creating
// it (and bind-mounting it under /run/netns/) if it does not already exist.
//
// netns.NewNamed switches the *current OS thread* into the new namespace, so
// we must LockOSThread around the call and explicitly Set back to the host ns
// before unlocking. Otherwise any goroutine spawned on the dirty thread (e.g.
// by the Go runtime) will silently inherit the wrong namespace.
func ensureSwiftV2NS(name string) (netns.NsHandle, error) {
	if h, err := netns.GetFromName(name); err == nil {
		return h, nil
	}

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	hostNS, err := netns.Get()
	if err != nil {
		return -1, fmt.Errorf("failed to capture host netns: %w", err)
	}
	defer hostNS.Close()

	ns, err := netns.NewNamed(name)
	if err != nil {
		// Race: another caller created it between our GetFromName and NewNamed.
		if existing, gerr := netns.GetFromName(name); gerr == nil {
			// Best-effort restore of the calling thread to host ns.
			_ = netns.Set(hostNS)
			return existing, nil
		}
		_ = netns.Set(hostNS)
		return -1, fmt.Errorf("failed to create netns %s: %w", name, err)
	}

	// NewNamed switched the current thread into the new ns — switch back to host.
	if setErr := netns.Set(hostNS); setErr != nil {
		ns.Close()
		return -1, fmt.Errorf("failed to restore host netns after creating %s: %w", name, setErr)
	}

	klog.V(2).Infof("SwiftV2: created interface namespace %s", name)
	return ns, nil
}

// findLinkByMACInHandle finds a link by MAC using the supplied netlink handle.
// The handle determines which network namespace is searched.
func findLinkByMACInHandle(h nlwrap.Handle, mac string) (netlink.Link, error) {
	targetMAC, err := net.ParseMAC(mac)
	if err != nil {
		return nil, fmt.Errorf("invalid MAC address %s: %w", mac, err)
	}
	links, err := h.LinkList()
	if err != nil {
		return nil, fmt.Errorf("failed to list links: %w", err)
	}
	for _, link := range links {
		if link.Attrs().HardwareAddr.String() == targetMAC.String() {
			return link, nil
		}
	}
	return nil, fmt.Errorf("NIC with MAC %s not found", mac)
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

// nsAttachIPVlanL3 attaches a SwiftV2 shared-mode pod to its delegated parent
// NIC by creating an ipvlan L3 child off the parent and moving the child into
// the pod network namespace.
//
// Design (per the SwiftV2 Shared NIC Cross-Subnet Routing one-pager):
//
//	One persistent netns per shared parent NIC, named swiftv2-<mac>. The
//	parent NIC, the Azure SDN gateway route + neigh, and all per-pod /32
//	routes live inside this namespace. The pod namespace receives only the
//	ipvlan child (renamed eth1) plus its pod-IP and gateway state.
//
// Why per-MAC namespace and not host-ns policy routing:
//
//	The parent-side namespace owns the off-subnet routing decision for ipvlan
//	L3 children. Keeping the parent in the host ns either breaks cross-subnet
//	flows (no gateway route) or requires source-scoped rules that cannot
//	disambiguate overlapping customer prefixes across tenants. A dedicated
//	netns per NIC gives strict routing isolation: two tenants can use the
//	same prefix without collision because each parent lives in a separate
//	routing domain.
func nsAttachIPVlanL3(cfg *NICConfig, containerNsPath string) (*resourceapi.NetworkDeviceData, error) {
	if cfg == nil {
		return nil, fmt.Errorf("NICConfig is nil")
	}
	if _, err := net.ParseMAC(cfg.MAC); err != nil {
		return nil, fmt.Errorf("invalid MAC %q: %w", cfg.MAC, err)
	}

	// Serialize per-parent-NIC: namespace ensure, parent move, gateway
	// programming, and per-pod /32 add are all mutating operations that must
	// not race across concurrent pods sharing the same parent.
	lock := swiftV2NSLock(cfg.MAC)
	lock.Lock()
	defer lock.Unlock()

	gwIP := net.ParseIP(cfg.GatewayIP)
	if gwIP == nil {
		return nil, fmt.Errorf("invalid gateway IP: %s", cfg.GatewayIP)
	}
	podIP := net.ParseIP(cfg.PodIP)
	if podIP == nil {
		return nil, fmt.Errorf("invalid pod IP: %s", cfg.PodIP)
	}

	// --- Per-MAC interface namespace ---

	nsName := swiftV2NSName(cfg.MAC)
	parentNs, err := ensureSwiftV2NS(nsName)
	if err != nil {
		return nil, err
	}
	defer parentNs.Close()

	nhParent, err := nlwrap.NewHandleAt(parentNs)
	if err != nil {
		return nil, fmt.Errorf("failed to get netlink handle in %s: %w", nsName, err)
	}
	defer nhParent.Close()

	// Locate the parent NIC: prefer the interface namespace, fall back to the
	// host namespace (first-time attach moves it in). Any other location (e.g.
	// a stale anonymous namespace from manual debugging) is a hard error —
	// fail closed rather than silently attach to the wrong routing domain.
	parent, err := findLinkByMACInHandle(nhParent, cfg.MAC)
	if err != nil {
		hostParent, herr := findLinkByMAC(cfg.MAC)
		if herr != nil {
			return nil, fmt.Errorf("parent NIC (MAC %s) not found in %s or host netns: %w", cfg.MAC, nsName, herr)
		}
		klog.V(2).Infof("SwiftV2: moving parent NIC %s (MAC %s) from host into %s",
			hostParent.Attrs().Name, cfg.MAC, nsName)
		if err := netlink.LinkSetNsFd(hostParent, int(parentNs)); err != nil {
			return nil, fmt.Errorf("failed to move parent NIC %s into %s: %w", hostParent.Attrs().Name, nsName, err)
		}
		parent, err = findLinkByMACInHandle(nhParent, cfg.MAC)
		if err != nil {
			return nil, fmt.Errorf("parent NIC (MAC %s) not visible in %s after move: %w", cfg.MAC, nsName, err)
		}
	}

	// Bring the parent UP inside the interface namespace. Azure delegated NICs
	// start with "state DOWN" and require an explicit LinkSetUp.
	if parent.Attrs().OperState != netlink.OperUp {
		klog.V(2).Infof("SwiftV2: parent NIC %s (MAC %s) is %s in %s, bringing it UP",
			parent.Attrs().Name, cfg.MAC, parent.Attrs().OperState, nsName)
		if err := nhParent.LinkSetUp(parent); err != nil {
			return nil, fmt.Errorf("failed to bring parent NIC %s up in %s: %w", parent.Attrs().Name, nsName, err)
		}
	}

	// Gateway route + neigh in the interface namespace. These are the missing
	// pieces vs. the old host-ns design — the parent-side routing decision
	// for off-subnet ipvlan traffic now resolves through the Azure SDN gateway.
	// RouteReplace is idempotent across repeated attaches for the same parent.
	gwLinkRoute := &netlink.Route{
		Dst:       &net.IPNet{IP: gwIP.To4(), Mask: net.CIDRMask(32, 32)},
		LinkIndex: parent.Attrs().Index,
		Scope:     netlink.SCOPE_LINK,
	}
	if err := nhParent.RouteReplace(gwLinkRoute); err != nil {
		return nil, fmt.Errorf("failed to add gateway link route in %s: %w", nsName, err)
	}

	_, defaultDst, _ := net.ParseCIDR("0.0.0.0/0")
	parentDefault := &netlink.Route{
		Dst:       defaultDst,
		Gw:        gwIP,
		LinkIndex: parent.Attrs().Index,
		Flags:     int(netlink.FLAG_ONLINK),
	}
	if err := nhParent.RouteReplace(parentDefault); err != nil {
		return nil, fmt.Errorf("failed to add default route in %s: %w", nsName, err)
	}

	sdnGWMAC, _ := net.ParseMAC(swiftV2SDNGatewayMAC)
	parentNeigh := &netlink.Neigh{
		LinkIndex:    parent.Attrs().Index,
		IP:           gwIP,
		HardwareAddr: sdnGWMAC,
		State:        netlink.NUD_PERMANENT,
	}
	if err := nhParent.NeighSet(parentNeigh); err != nil {
		return nil, fmt.Errorf("failed to set gateway neigh in %s: %w", nsName, err)
	}

	// Per-pod /32 route inside the interface namespace. This is what makes
	// the parent's FIB route same-host cross-pod traffic into the right
	// ipvlan slave (the kernel demuxes by destination /32).
	podRoute := &netlink.Route{
		Dst:       &net.IPNet{IP: podIP.To4(), Mask: net.CIDRMask(32, 32)},
		LinkIndex: parent.Attrs().Index,
		Scope:     netlink.SCOPE_LINK,
	}
	if err := nhParent.RouteReplace(podRoute); err != nil {
		return nil, fmt.Errorf("failed to add pod /32 route in %s: %w", nsName, err)
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

	// Idempotent cleanup of stale state in the pod ns from previous failed attempts.
	if stale, err := nhPod.LinkByName(ipvlName); err == nil {
		klog.V(2).Infof("SwiftV2: removing stale ipvlan child %s from pod ns (previous failed attempt)", ipvlName)
		_ = nhPod.LinkDel(stale)
	}
	if stale, err := nhPod.LinkByName(swiftV2DelegatedIfName); err == nil {
		klog.V(2).Infof("SwiftV2: removing stale %s from pod ns (previous failed attempt)", swiftV2DelegatedIfName)
		_ = nhPod.LinkDel(stale)
	}

	// --- ipvlan child: create inside the interface namespace, then move to pod ---

	// Reuse a leftover ipvl-<uid> from a prior failed attempt if present in the
	// interface ns. Otherwise create a fresh ipvlan L3 child.
	ipvl, err := nhParent.LinkByName(ipvlName)
	if err != nil {
		var notFound netlink.LinkNotFoundError
		if !errors.As(err, &notFound) {
			return nil, fmt.Errorf("failed to look up ipvlan %s in %s: %w", ipvlName, nsName, err)
		}

		// Mode rationale (L3, not L3S): see prior version of this comment block.
		// L3 places the FIB/RPF/netfilter hooks for slave traffic in the
		// *parent's* namespace (now the interface ns, where we just programmed
		// the gateway/default routes). L3S would run them in the slave (pod)
		// ns where strict RPF would drop the asymmetric prefix-on-NIC return
		// path.
		newIPVL := &netlink.IPVlan{
			LinkAttrs: netlink.LinkAttrs{
				Name:        ipvlName,
				ParentIndex: parent.Attrs().Index,
			},
			Mode: netlink.IPVLAN_MODE_L3,
		}
		if err := nhParent.LinkAdd(newIPVL); err != nil {
			return nil, fmt.Errorf("failed to create ipvlan L3 interface %s in %s: %w", ipvlName, nsName, err)
		}
		ipvl, err = nhParent.LinkByName(ipvlName)
		if err != nil {
			return nil, fmt.Errorf("ipvlan %s not found after creation in %s: %w", ipvlName, nsName, err)
		}
	} else {
		klog.V(2).Infof("SwiftV2: reusing existing ipvlan child %s in %s (previous failed attempt)", ipvlName, nsName)
	}

	// Move ipvlan child from the interface ns to the pod ns.
	if err := nhParent.LinkSetNsFd(ipvl, int(containerNs)); err != nil {
		return nil, fmt.Errorf("failed to move ipvlan %s from %s to pod netns: %w", ipvlName, nsName, err)
	}

	// --- Pod namespace operations on the moved ipvlan child ---

	nsLink, err := nhPod.LinkByName(ipvlName)
	if err != nil {
		return nil, fmt.Errorf("ipvlan interface %s not found in pod netns: %w", ipvlName, err)
	}

	if err := nhPod.LinkSetName(nsLink, swiftV2DelegatedIfName); err != nil {
		return nil, fmt.Errorf("failed to rename %s to %s: %w", ipvlName, swiftV2DelegatedIfName, err)
	}

	nsLink, err = nhPod.LinkByName(swiftV2DelegatedIfName)
	if err != nil {
		return nil, fmt.Errorf("renamed interface %s not found in pod netns: %w", swiftV2DelegatedIfName, err)
	}

	if err := nhPod.AddrAdd(nsLink, &netlink.Addr{
		IPNet: &net.IPNet{IP: podIP, Mask: net.CIDRMask(32, 32)},
	}); err != nil && !errors.Is(err, syscall.EEXIST) {
		return nil, fmt.Errorf("failed to add IP %s/32 to pod eth1: %w", cfg.PodIP, err)
	}

	if err := nhPod.LinkSetUp(nsLink); err != nil {
		return nil, fmt.Errorf("failed to bring up %s in pod ns: %w", swiftV2DelegatedIfName, err)
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
		IP:           gwIP,
		HardwareAddr: gwMAC,
		State:        netlink.NUD_PERMANENT,
	}
	if err := nhPod.NeighSet(podNeigh); err != nil {
		return nil, fmt.Errorf("failed to set static neighbor %s -> %s on pod eth1: %w", cfg.GatewayIP, gwMAC, err)
	}

	gwRoute := &netlink.Route{
		Dst:       &net.IPNet{IP: gwIP, Mask: net.CIDRMask(32, 32)},
		LinkIndex: nsLink.Attrs().Index,
		Scope:     netlink.SCOPE_LINK,
	}
	if err := nhPod.RouteAdd(gwRoute); err != nil && !errors.Is(err, syscall.EEXIST) {
		return nil, fmt.Errorf("failed to add gateway route in pod ns: %w", err)
	}

	// Override the CNI-installed default route so all pod egress flows through
	// eth1 → SmartNIC → VFP/SDN policy rather than out the cluster-network eth0.
	podDefault := &netlink.Route{
		Dst:       defaultDst,
		Gw:        gwIP,
		LinkIndex: nsLink.Attrs().Index,
	}
	if err := nhPod.RouteReplace(podDefault); err != nil {
		return nil, fmt.Errorf("failed to replace default route via %s in pod ns: %w", cfg.GatewayIP, err)
	}

	// Loose RPF on the parent: pod replies arrive on the parent (now inside
	// the interface ns) from the SmartNIC, and strict RPF could drop valid
	// VNet traffic that lacks a perfectly symmetric return path. The /proc
	// sysctl is host-ns-scoped, so setting it from this code path no longer
	// targets the parent NIC (which now lives in the interface ns). The
	// per-pod /32 route inside the interface ns provides a symmetric reverse
	// path for the common case, so loose RPF is a hardening, not a hard
	// requirement; it is intentionally not applied here.

	return &resourceapi.NetworkDeviceData{
		InterfaceName:   swiftV2DelegatedIfName,
		HardwareAddress: parent.Attrs().HardwareAddr.String(),
		IPs:             []string{fmt.Sprintf("%s/32", cfg.PodIP)},
	}, nil
}

// cleanupIPVlanL3 removes only the per-pod /32 route from the parent's
// interface namespace. The parent NIC itself remains in the interface ns
// (it represents the routing domain for the shared NIC and is shared by all
// pods on that NIC). GC of an empty interface ns and moving the parent back
// to host is intentionally not done here — that belongs to an explicit
// reconcile/teardown when the NIC is removed from the node, not to per-pod
// cleanup, which would otherwise destroy and recreate the namespace on every
// last-pod transition.
//
// The ipvlan child itself is destroyed automatically by the kernel when the
// pod's netns is removed; we do not need to delete it.
//
// Best-effort: errors are logged but not returned.
func cleanupIPVlanL3(cfg *NICConfig) {
	if cfg == nil {
		return
	}
	if _, err := net.ParseMAC(cfg.MAC); err != nil {
		klog.Warningf("SwiftV2 cleanup: invalid MAC %q: %v", cfg.MAC, err)
		return
	}

	lock := swiftV2NSLock(cfg.MAC)
	lock.Lock()
	defer lock.Unlock()

	nsName := swiftV2NSName(cfg.MAC)
	parentNs, err := netns.GetFromName(nsName)
	if err != nil {
		// No interface ns means no per-pod state for us to clean up. This is
		// the normal case if the NIC was never attached on this node or if
		// the ns has already been GC'd.
		klog.V(2).Infof("SwiftV2 cleanup: interface ns %s not present, nothing to remove for pod IP %s", nsName, cfg.PodIP)
		return
	}
	defer parentNs.Close()

	nhParent, err := nlwrap.NewHandleAt(parentNs)
	if err != nil {
		klog.Warningf("SwiftV2 cleanup: failed to get netlink handle in %s: %v", nsName, err)
		return
	}
	defer nhParent.Close()

	parent, err := findLinkByMACInHandle(nhParent, cfg.MAC)
	if err != nil {
		klog.Warningf("SwiftV2 cleanup: parent NIC (MAC %s) not found in %s: %v", cfg.MAC, nsName, err)
		return
	}

	route := &netlink.Route{
		Dst:       parseIP32(cfg.PodIP),
		LinkIndex: parent.Attrs().Index,
		Scope:     netlink.SCOPE_LINK,
	}
	if err := nhParent.RouteDel(route); err != nil {
		klog.Warningf("SwiftV2 cleanup: failed to remove pod /32 route for %s in %s: %v", cfg.PodIP, nsName, err)
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
