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
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"sigs.k8s.io/dranet/internal/nlwrap"
)

// sharedNICInterfaceName is the pod-side interface name the shared-NIC
// integration tests expect. Production supports one secondary NIC and assigns
// it eth1 because the cluster CNI has already installed eth0.
const sharedNICInterfaceName = "eth1"

// hostNS holds the host network namespace fd, saved once before any test runs.
// This is necessary because the Go runtime may clone new OS threads from a
// thread that is temporarily in a non-host namespace (during netns.NewNamed →
// Unshare). Those cloned threads inherit the wrong namespace permanently.
// When a new goroutine (e.g., from t.Run) lands on such a dirty thread,
// netns.Get() returns the wrong namespace. By saving the host ns here and
// using netns.Set(hostNS) at the start of each test, we guarantee threads
// are restored to the host namespace regardless of which thread they start on.
var hostNS netns.NsHandle

func TestMain(m *testing.M) {
	runtime.LockOSThread()
	var err error
	hostNS, err = netns.Get()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get host namespace: %v\n", err)
		os.Exit(1)
	}
	runtime.UnlockOSThread()

	code := m.Run()
	hostNS.Close()
	os.Exit(code)
}

// Integration test run helper (from the repository root):
//   sudo go test ./pkg/driver -run '^TestIntegration_' -timeout 30s -v
// Keep the -run regex quoted; otherwise shell metacharacters can break command parsing.

// skipIfNotRoot skips the test if not running as root. Integration tests require
// CAP_NET_ADMIN to create network namespaces, interfaces and routes.
func skipIfNotRoot(t *testing.T) {
	t.Helper()
	if os.Getuid() != 0 {
		t.Skip("Test requires root privileges.")
	}
}

// testNetns creates a named network namespace for testing and registers cleanup.
// Follows the pattern established in hostdevice_test.go and ethtool_test.go.
// Returns the namespace handle and its filesystem path under /run/netns/.
//
// IMPORTANT: the test goroutine stays locked to a host-namespace OS thread for
// the test lifetime. NewNamed runs on a separate locked goroutine that exits
// without unlocking, forcing Go to retire the OS thread after it entered the new
// namespace instead of returning that thread to the runtime pool.
//
// The goroutine's thread is explicitly restored to the host namespace (via
// the package-level hostNS handle saved in TestMain) before any namespace
// operations, because t.Run subtests spawn new goroutines that may land on
// dirty threads where netns.Get() would return the wrong namespace.
func testNetns(t *testing.T) (netns.NsHandle, string) {
	t.Helper()

	// Lock the current goroutine to its OS thread for the entire test duration.
	// Network namespace operations are thread-local in Linux — without this
	// lock, Go's scheduler could migrate the goroutine to a thread that was
	// cloned while another thread was in a non-host namespace.
	runtime.LockOSThread()

	// Force this thread into the host namespace. The goroutine may have
	// been scheduled on a dirty thread that was cloned while another thread
	// was temporarily in a non-host namespace (during a prior NewNamed call).
	// Without this, everything below would operate in the wrong namespace.
	if err := netns.Set(hostNS); err != nil {
		runtime.UnlockOSThread()
		t.Fatalf("failed to restore host namespace: %v", err)
	}

	// Generate a random namespace name to avoid collisions between parallel tests.
	rndString := make([]byte, 4)
	if _, err := rand.Read(rndString); err != nil {
		runtime.UnlockOSThread()
		t.Fatalf("failed to generate random name: %v", err)
	}
	nsName := fmt.Sprintf("ns%x", rndString)

	// Create the named namespace on a disposable OS thread. NewNamed enters the
	// new namespace; exiting while still locked prevents that thread from being
	// reused by another test after it has observed the foreign namespace.
	type result struct {
		ns  netns.NsHandle
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		runtime.LockOSThread()
		if err := netns.Set(hostNS); err != nil {
			resultCh <- result{err: fmt.Errorf("restore host namespace before creating %s: %w", nsName, err)}
			return
		}
		ns, err := netns.NewNamed(nsName)
		resultCh <- result{ns: ns, err: err}
		// Do not unlock: the goroutine entered the new namespace, so its OS thread
		// must be retired instead of returned to the runtime thread pool.
	}()
	created := <-resultCh
	if created.err != nil {
		runtime.UnlockOSThread()
		t.Fatalf("failed to create namespace %s: %v", nsName, created.err)
	}
	testNS := created.ns

	// Register cleanup to delete the named namespace, close handles, and
	// release the OS thread lock. This runs AFTER all later-registered
	// cleanups (t.Cleanup is LIFO), so testDummyNIC cleanup etc. still
	// execute on the locked (host-ns) thread.
	t.Cleanup(func() {
		netns.DeleteNamed(nsName)
		testNS.Close()
		runtime.UnlockOSThread()
	})

	return testNS, path.Join("/run/netns", nsName)
}

// testDummyNIC creates a dummy network interface in the current (host) namespace,
// brings it UP, and returns its MAC address. Cleanup is registered via t.Cleanup.
// The dummy interface simulates a physical NIC for testing purposes (it supports
// MAC addresses, can be moved between namespaces, and supports ipvlan).
//
// A locally-administered MAC is explicitly set on the dummy NIC because the
// kernel's auto-assigned MAC on dummy interfaces is regenerated when an ipvlan
// child is moved to another network namespace. Real physical NICs (e.g., Azure
// VM NICs) have hardware-burned MACs that never change, so this behavior is a
// dummy-device-specific quirk. Setting the MAC explicitly makes it persistent
// across ipvlan child moves, matching real NIC behavior.
func testDummyNIC(t *testing.T, name string) string {
	t.Helper()

	// This helper is also used by tests that do not call testNetns. Pin it to the
	// known host namespace so a runtime thread contaminated by another netns test
	// cannot create or clean up the dummy NIC in the wrong namespace.
	runtime.LockOSThread()
	if err := netns.Set(hostNS); err != nil {
		runtime.UnlockOSThread()
		t.Fatalf("failed to restore host namespace before creating %s: %v", name, err)
	}
	t.Cleanup(runtime.UnlockOSThread)

	// Create a dummy link.
	la := netlink.NewLinkAttrs()
	la.Name = name
	link := &netlink.Dummy{LinkAttrs: la}
	if err := netlink.LinkAdd(link); err != nil {
		t.Fatalf("failed to add dummy link %s: %v", name, err)
	}

	// Register cleanup to delete the dummy interface when the test finishes.
	// Uses LinkByName first because the NIC may have been moved to another
	// namespace during the test and returned — we only delete if it's found.
	t.Cleanup(func() {
		if l, err := nlwrap.LinkByName(name); err == nil {
			_ = netlink.LinkDel(l)
		}
	})

	// Generate a locally-administered MAC (bit 1 of first octet = 1) using
	// random bytes. This simulates a hardware-assigned MAC that persists
	// through ipvlan child creation and cross-namespace moves.
	macBytes := make([]byte, 6)
	if _, err := rand.Read(macBytes); err != nil {
		t.Fatalf("failed to generate random MAC: %v", err)
	}
	macBytes[0] = (macBytes[0] | 0x02) & 0xFE // locally administered, unicast
	mac := net.HardwareAddr(macBytes)

	// Re-fetch the link object (LinkAdd's return doesn't carry kernel state).
	actual, err := nlwrap.LinkByName(name)
	if err != nil {
		t.Fatalf("failed to find dummy link %s after creation: %v", name, err)
	}

	// Set the deterministic MAC on the dummy NIC.
	if err := netlink.LinkSetHardwareAddr(actual, mac); err != nil {
		t.Fatalf("failed to set MAC on dummy link %s: %v", name, err)
	}

	// Bring the interface UP — required for ipvlan child creation and
	// for the interface to appear in routing table operations.
	if err := netlink.LinkSetUp(actual); err != nil {
		t.Fatalf("failed to bring up dummy link %s: %v", name, err)
	}

	return mac.String()
}

// ensureLinkMAC verifies that the named host link has the expected MAC and
// resets it if the kernel changed it. This guards integration tests against a
// dummy-device-specific quirk where moving an ipvlan child across namespaces
// can regenerate the parent dummy's MAC. Real physical NICs do not exhibit
// this behavior (their MACs are hardware-burned and stable).
func ensureLinkMAC(t *testing.T, name string, wantMAC string) {
	t.Helper()

	link, err := nlwrap.LinkByName(name)
	if err != nil {
		t.Fatalf("failed to find host link %s: %v", name, err)
	}

	current := link.Attrs().HardwareAddr.String()
	if current == wantMAC {
		return
	}

	target, err := net.ParseMAC(wantMAC)
	if err != nil {
		t.Fatalf("invalid target MAC %s: %v", wantMAC, err)
	}

	if err := netlink.LinkSetHardwareAddr(link, target); err != nil {
		t.Fatalf("failed to reset MAC on %s from %s to %s: %v", name, current, wantMAC, err)
	}
}

// successfulTestPostAttach isolates exclusive NIC netlink tests from external
// post-attach dependencies. Failure and rollback are covered separately.
func successfulTestPostAttach(string, string, net.HardwareAddr) error {
	return nil
}

// --- Integration tests ---

// TestIntegration_findLinkByMAC verifies MAC-based lookup of a host interface.
// This is the foundational lookup used by all secondary NIC attach functions; both
// shared (IPVLAN) and exclusive modes find their target NIC by MAC address.
// In plain terms: find the physical NIC, ignore its child, and report a missing NIC.
func TestIntegration_findLinkByMAC(t *testing.T) {
	skipIfNotRoot(t)

	// Create a dummy NIC on the host — this gives us a known MAC to search for.
	mac := testDummyNIC(t, "test-dnic-mac")

	// Positive case: findLinkByMAC should locate the dummy by its MAC address
	// and return a link whose name matches the one we created.
	link, err := findLinkByMAC(mac)
	if err != nil {
		t.Fatalf("findLinkByMAC(%s) failed: %v", mac, err)
	}
	if link.Attrs().Name != "test-dnic-mac" {
		t.Errorf("findLinkByMAC returned %q, want %q", link.Attrs().Name, "test-dnic-mac")
	}

	// IPVLAN children inherit the parent's MAC. Keep a child host-visible and
	// verify MAC lookup still returns the physical parent rather than the child.
	parent, err := nlwrap.LinkByName("test-dnic-mac")
	if err != nil {
		t.Fatalf("failed to refresh test parent: %v", err)
	}
	child := &netlink.IPVlan{
		LinkAttrs: netlink.LinkAttrs{Name: "test-sw-child", ParentIndex: parent.Attrs().Index},
		Mode:      netlink.IPVLAN_MODE_L3,
	}
	if err := netlink.LinkAdd(child); err != nil {
		t.Fatalf("failed to create test IPVLAN child: %v", err)
	}
	link, err = findLinkByMAC(mac)
	if err != nil {
		t.Fatalf("findLinkByMAC with same-MAC IPVLAN child failed: %v", err)
	}
	if link.Type() == "ipvlan" || link.Attrs().Name != "test-dnic-mac" {
		t.Fatalf("findLinkByMAC returned %s link %q, want physical parent", link.Type(), link.Attrs().Name)
	}

	// Negative case: a fabricated MAC that doesn't exist on the host should
	// return an error — this is the path taken when a NIC is missing or
	// has already been moved into a pod namespace.
	_, err = findLinkByMAC("02:00:00:00:ff:ff")
	if err == nil {
		t.Error("expected error for nonexistent MAC, got nil")
	}
}

// TestIntegration_nsAttachIPVlanL3_FullCycle tests the complete shared NIC lifecycle:
//  1. Create a parent dummy NIC in the host namespace.
//  2. Call nsAttachIPVlanL3 to create a host VRF, enslave the parent, create an
//     ipvlan L3 child, move it into the pod namespace, and configure IP/routes.
//  3. Verify: eth1 exists in pod ns with correct IP/32, GW route, default route.
//  4. Verify: parent remains host-visible, enslaved to the per-MAC VRF, and the
//     VRF table contains the Azure gateway/default routes and static neighbor.
//
// In plain terms: one shared pod receives complete host and pod network setup.
func TestIntegration_nsAttachIPVlanL3_FullCycle(t *testing.T) {
	skipIfNotRoot(t)

	// Create an isolated network namespace to simulate the pod's netns.
	testNS, nsPath := testNetns(t)
	// Create a dummy NIC on the host to act as the shared parent (physical NIC).
	parentMAC := testDummyNIC(t, "test-dnic-prt")

	// Build a NICConfig that mirrors what CNS would provide for a shared NIC pod.
	// MAC identifies the parent NIC; PodIP/GatewayIP configure the
	// ipvlan child; PodUID is used for the temporary ipvlan interface name.
	cfg := &NICConfig{
		MAC:       parentMAC,
		PodIP:     "10.244.1.42",
		GatewayIP: secondaryNICVirtualGateway,
		PodUID:    "abcdef12-3456-7890-abcd-ef1234567890",
	}

	// Dummy NIC quirk: MAC can occasionally regenerate; force it back to the
	// expected value so this test models real hardware NIC behavior.
	ensureLinkMAC(t, "test-dnic-prt", parentMAC)

	// --- Attach: exercise the full nsAttachIPVlanL3 code path ---
	// This creates/enslaves the parent to a host VRF, creates an ipvlan L3 child
	// off the parent, moves the child into the pod ns, renames it to eth1, and
	// assigns IP + routes.
	deviceData, err := nsAttachIPVlanL3(cfg, nsPath)
	if err != nil {
		t.Fatalf("nsAttachIPVlanL3 failed: %v", err)
	}

	// Verify the returned NetworkDeviceData that DRA publishes to the ResourceSlice.
	// InterfaceName should be "eth1" (the constant sharedNICInterfaceName).
	if deviceData.InterfaceName != sharedNICInterfaceName {
		t.Errorf("InterfaceName = %s, want %s", deviceData.InterfaceName, sharedNICInterfaceName)
	}
	// HardwareAddress should be the parent NIC's MAC (pod inherits parent identity).
	if deviceData.HardwareAddress != parentMAC {
		t.Errorf("HardwareAddress = %s, want %s", deviceData.HardwareAddress, parentMAC)
	}
	// IPs should contain exactly the pod IP with a /32 mask.
	if len(deviceData.IPs) != 1 || deviceData.IPs[0] != "10.244.1.42/32" {
		t.Errorf("IPs = %v, want [10.244.1.42/32]", deviceData.IPs)
	}

	// --- Verify pod namespace state ---
	// Open a netlink handle scoped to the pod namespace to inspect its interfaces,
	// addresses, and routes — this is the same pattern the production code uses.
	nhNs, err := nlwrap.NewHandleAt(testNS)
	if err != nil {
		t.Fatalf("failed to get handle in test netns: %v", err)
	}
	defer nhNs.Close()

	// Verify that the ipvlan child was renamed to eth1 inside the pod namespace.
	nsLink, err := nhNs.LinkByName(sharedNICInterfaceName)
	if err != nil {
		t.Fatalf("interface %s not found in container ns: %v", sharedNICInterfaceName, err)
	}

	// Verify the interface is administratively UP (required for traffic to flow).
	if nsLink.Attrs().Flags&net.FlagUp == 0 {
		t.Error("interface eth1 is not UP in container ns")
	}

	// List IPv4 addresses on eth1 and verify the pod IP was assigned with /32 mask.
	// Shared secondary NICs use point-to-point /32 addressing; all routing goes through the
	// virtual gateway rather than relying on subnet-based forwarding.
	addrs, err := nhNs.AddrList(nsLink, netlink.FAMILY_V4)
	if err != nil {
		t.Fatalf("failed to list addrs in container ns: %v", err)
	}
	foundPodIP := false
	for _, a := range addrs {
		if a.IPNet.IP.String() == "10.244.1.42" {
			ones, _ := a.IPNet.Mask.Size()
			if ones != 32 {
				t.Errorf("pod IP mask = /%d, want /32", ones)
			}
			foundPodIP = true
		}
	}
	if !foundPodIP {
		t.Error("pod IP 10.244.1.42/32 not found on eth1 in container ns")
	}

	// List routes on eth1 inside the pod namespace and verify both required routes:
	// 1. Gateway /32 scope-link route: makes the virtual GW (169.254.2.1) "reachable"
	//    on the link so the kernel accepts it as a next-hop.
	// 2. Default route (0.0.0.0/0) via the virtual GW: sends all outbound traffic
	//    through the Azure virtual gateway.
	routes, err := nhNs.RouteList(nsLink, netlink.FAMILY_V4)
	if err != nil {
		t.Fatalf("failed to list routes in container ns: %v", err)
	}
	foundGWRoute := false
	foundDefaultRoute := false
	for _, r := range routes {
		// Check for the gateway /32 scope-link route (169.254.2.1/32 dev eth1 scope link).
		if r.Dst != nil && r.Dst.IP.String() == secondaryNICVirtualGateway {
			ones, _ := r.Dst.Mask.Size()
			if ones == 32 && r.Scope == netlink.SCOPE_LINK {
				foundGWRoute = true
			}
		}
		// Check for the default route via virtual gateway (default via 169.254.2.1 dev eth1).
		if r.Dst != nil && r.Dst.IP.String() == "0.0.0.0" && r.Gw != nil && r.Gw.String() == secondaryNICVirtualGateway {
			foundDefaultRoute = true
		}
	}
	if !foundGWRoute {
		t.Error("gateway /32 scope link route not found in container ns")
	}
	if !foundDefaultRoute {
		t.Error("default route via gateway not found in container ns")
	}

	// --- Verify host parent-side VRF routing state ---
	// Look up parent by name (not MAC) because creating an ipvlan child on some
	// kernel versions may alter the parent's reported HardwareAddr.
	parent, err := nlwrap.LinkByName("test-dnic-prt")
	if err != nil {
		t.Fatalf("parent NIC not found after attach: %v", err)
	}
	vrfName := sharedNICVRFName(parentMAC)
	vrfLink, err := nlwrap.LinkByName(vrfName)
	if err != nil {
		t.Fatalf("shared NIC VRF %s not found after attach: %v", vrfName, err)
	}
	t.Cleanup(func() {
		if vrf, err := nlwrap.LinkByName(vrfName); err == nil {
			_ = netlink.LinkDel(vrf)
		}
	})
	if vrfLink.Type() != "vrf" {
		t.Fatalf("link %s is %q, want vrf", vrfName, vrfLink.Type())
	}
	if parent.Attrs().MasterIndex != vrfLink.Attrs().Index {
		t.Fatalf("parent master index = %d, want VRF %s index %d", parent.Attrs().MasterIndex, vrfName, vrfLink.Attrs().Index)
	}
	parentTable, err := sharedNICVRFTable(parentMAC)
	if err != nil {
		t.Fatalf("failed to compute parent table: %v", err)
	}
	parentRoutes, err := netlink.RouteListFiltered(netlink.FAMILY_V4, &netlink.Route{Table: parentTable}, netlink.RT_FILTER_TABLE)
	if err != nil {
		t.Fatalf("failed to list parent table %d routes: %v", parentTable, err)
	}
	foundGatewayRoute := false
	foundParentDefaultRoute := false
	for _, r := range parentRoutes {
		if r.LinkIndex != parent.Attrs().Index {
			continue
		}
		if r.Dst != nil && r.Dst.IP.String() == secondaryNICVirtualGateway {
			ones, _ := r.Dst.Mask.Size()
			foundGatewayRoute = ones == 32 && r.Scope == netlink.SCOPE_LINK
		}
		if (r.Dst == nil || r.Dst.String() == "0.0.0.0/0") && r.Gw != nil && r.Gw.String() == secondaryNICVirtualGateway {
			foundParentDefaultRoute = true
		}
	}
	if !foundGatewayRoute {
		t.Errorf("gateway /32 route for %s not found in parent table %d", secondaryNICVirtualGateway, parentTable)
	}
	if !foundParentDefaultRoute {
		t.Errorf("default route via %s not found in parent table %d", secondaryNICVirtualGateway, parentTable)
	}
	neighs, err := netlink.NeighList(parent.Attrs().Index, netlink.FAMILY_V4)
	if err != nil {
		t.Fatalf("failed to list parent neighbors: %v", err)
	}
	foundGatewayNeigh := false
	for _, n := range neighs {
		if n.IP.String() == secondaryNICVirtualGateway && n.HardwareAddr.String() == secondaryNICSDNGatewayMAC && n.State == netlink.NUD_PERMANENT {
			foundGatewayNeigh = true
			break
		}
	}
	if !foundGatewayNeigh {
		t.Errorf("permanent gateway neighbor %s -> %s not found on parent", secondaryNICVirtualGateway, secondaryNICSDNGatewayMAC)
	}

}

// TestIntegration_nsAttachIPVlanL3_TwoPodsShareParent verifies that two pods can
// receive different IPs while safely sharing the same physical NIC and host VRF.
func TestIntegration_nsAttachIPVlanL3_TwoPodsShareParent(t *testing.T) {
	skipIfNotRoot(t)

	_, firstNSPath := testNetns(t)
	_, secondNSPath := testNetns(t)
	parentName := "test-two-shared"
	parentMAC := testDummyNIC(t, parentName)
	vrfName := sharedNICVRFName(parentMAC)
	t.Cleanup(func() {
		if vrf, err := nlwrap.LinkByName(vrfName); err == nil {
			_ = netlink.LinkDel(vrf)
		}
	})

	tests := []struct {
		name   string
		nsPath string
		podUID string
		podIP  string
	}{
		{name: "first pod", nsPath: firstNSPath, podUID: "pod-one-12345678", podIP: "10.244.10.10"},
		{name: "second pod", nsPath: secondNSPath, podUID: "pod-two-12345678", podIP: "10.244.10.11"},
	}

	for _, test := range tests {
		ensureLinkMAC(t, parentName, parentMAC)
		data, err := nsAttachIPVlanL3(&NICConfig{
			MAC:       parentMAC,
			PodIP:     test.podIP,
			GatewayIP: secondaryNICVirtualGateway,
			PodUID:    test.podUID,
		}, test.nsPath)
		if err != nil {
			t.Fatalf("%s attach failed: %v", test.name, err)
		}
		if data.InterfaceName != sharedNICInterfaceName {
			t.Fatalf("%s interface = %s, want %s", test.name, data.InterfaceName, sharedNICInterfaceName)
		}

		podNS, err := netns.GetFromPath(test.nsPath)
		if err != nil {
			t.Fatalf("failed to open %s namespace: %v", test.name, err)
		}
		handle, err := nlwrap.NewHandleAt(podNS)
		podNS.Close()
		if err != nil {
			t.Fatalf("failed to create %s netlink handle: %v", test.name, err)
		}
		link, err := handle.LinkByName(sharedNICInterfaceName)
		if err != nil {
			handle.Close()
			t.Fatalf("%s %s not found: %v", test.name, sharedNICInterfaceName, err)
		}
		addrs, err := handle.AddrList(link, netlink.FAMILY_V4)
		handle.Close()
		if err != nil {
			t.Fatalf("failed to list %s addresses: %v", test.name, err)
		}
		found := false
		for _, addr := range addrs {
			if addr.IP.String() == test.podIP {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("%s IP %s not found on %s", test.name, test.podIP, sharedNICInterfaceName)
		}
	}

	parent, err := nlwrap.LinkByName(parentName)
	if err != nil {
		t.Fatalf("shared parent not found after two attaches: %v", err)
	}
	vrf, err := nlwrap.LinkByName(vrfName)
	if err != nil {
		t.Fatalf("shared VRF not found after two attaches: %v", err)
	}
	if parent.Attrs().MasterIndex != vrf.Attrs().Index {
		t.Fatalf("parent master index = %d, want shared VRF index %d", parent.Attrs().MasterIndex, vrf.Attrs().Index)
	}
}

// testSharedRetryWithRetainedParentState verifies that retry reuses completed
// host setup when the first pod stopped before creating its IPVLAN child.
func testSharedRetryWithRetainedParentState(t *testing.T) {
	skipIfNotRoot(t)

	testNS, nsPath := testNetns(t)
	parentName := "test-ret-prt"
	parentMAC := testDummyNIC(t, parentName)
	ensureLinkMAC(t, parentName, parentMAC)
	vrfName := sharedNICVRFName(parentMAC)
	t.Cleanup(func() {
		if vrf, err := nlwrap.LinkByName(vrfName); err == nil {
			_ = netlink.LinkDel(vrf)
		}
	})

	// The first two calls create the NAT exemption during the simulated failed
	// attempt. The third reports it already present during retry.
	script := useScriptedIPTables(t, []error{errors.New("rule absent"), nil, nil})
	parentRouting, err := ensureSharedNICParentVRF(parentMAC)
	if err != nil {
		t.Fatalf("failed to create retained parent state: %v", err)
	}
	gwIPv4 := net.ParseIP(secondaryNICVirtualGateway).To4()
	if err := configureSharedNICParentRouting(parentRouting, gwIPv4); err != nil {
		t.Fatalf("failed to configure retained parent state: %v", err)
	}
	originalVRFIndex := parentRouting.parent.Attrs().MasterIndex

	data, err := nsAttachIPVlanL3(&NICConfig{
		MAC:       parentMAC,
		PodIP:     "10.244.10.20",
		GatewayIP: secondaryNICVirtualGateway,
		PodUID:    "retry-parent-state-12345678",
	}, nsPath)
	if err != nil {
		t.Fatalf("retry with retained parent state failed: %v", err)
	}
	if data.InterfaceName != sharedNICInterfaceName {
		t.Fatalf("retry interface = %s, want %s", data.InterfaceName, sharedNICInterfaceName)
	}

	parent, err := nlwrap.LinkByName(parentName)
	if err != nil {
		t.Fatalf("shared parent missing after retry: %v", err)
	}
	if parent.Attrs().MasterIndex != originalVRFIndex {
		t.Fatalf("retry replaced VRF: parent master index = %d, want %d", parent.Attrs().MasterIndex, originalVRFIndex)
	}
	handle, err := nlwrap.NewHandleAt(testNS)
	if err != nil {
		t.Fatalf("failed to inspect pod namespace: %v", err)
	}
	defer handle.Close()
	if _, err := handle.LinkByName(sharedNICInterfaceName); err != nil {
		t.Fatalf("shared child missing after retry: %v", err)
	}
	if len(script.calls) != 3 {
		t.Fatalf("iptables calls = %d, want 3 (create then check-only retry)", len(script.calls))
	}
	if got := strings.Join(script.calls[2], " "); got != "-t nat -C POSTROUTING -o "+parentName+" -j ACCEPT" {
		t.Fatalf("retry iptables call = %q, want check-only existing rule", got)
	}
}

// TestIntegration_nsAttachIPVlanL3_IdempotentRetry verifies that retry completes
// a shared-pod attach from every partial state left by an interrupted attempt:
//
//   - Retained parent state: VRF/routes/neighbor/NAT setup completed, but the
//     IPVLAN child was not created. Retry must reuse the shared host state.
//   - Stale child on host: LinkAdd completed, but the child was not moved into
//     the pod namespace. Retry must validate and reuse the host child.
//   - Stale child in pod ns: the child was moved but not renamed to eth1. Retry
//     must remove the temporary child and recreate it.
//   - Stale eth1 in pod ns: the child was renamed, but IP/route setup was
//     incomplete. Retry must remove eth1 and recreate the pod network setup.
//
// In plain terms: retry succeeds no matter which attach stage was interrupted.
func TestIntegration_nsAttachIPVlanL3_IdempotentRetry(t *testing.T) {
	skipIfNotRoot(t)

	// All subtests share the same NICConfig (same PodUID → same ipvl-<uid> name).
	podUID := "idempotent-test-uid-1234567890"
	ipvlName := fmt.Sprintf("ipvl-%s", truncateUID(podUID))

	// The first pod stops before creating its child; retry reuses the shared host setup.
	t.Run("HostSetupFinishedBeforeChildCreation", testSharedRetryWithRetainedParentState)

	// The child is created but never reaches the pod; retry validates and reuses it.
	t.Run("ChildCreatedButStillOnHost", func(t *testing.T) {
		skipIfNotRoot(t)

		// Setup: parent NIC + pod namespace.
		_, nsPath := testNetns(t)
		parentMAC := testDummyNIC(t, "test-idem-prt1")

		cfg := &NICConfig{
			MAC:       parentMAC,
			PodIP:     "10.244.9.10",
			GatewayIP: secondaryNICVirtualGateway,
			PodUID:    podUID,
		}

		// Dummy NIC quirk: parent MAC can occasionally change; reset to expected
		// value so retry behavior matches real hardware NICs.
		ensureLinkMAC(t, "test-idem-prt1", parentMAC)

		// Simulate a failed first attempt: create the ipvlan child on the host
		// but do NOT move it into the pod namespace. This is the state left
		// behind when LinkAdd succeeds but LinkSetNsFd fails before the child
		// leaves the host namespace.
		//
		// Use LinkByName (not findLinkByMAC) because we know the NIC name.
		// findLinkByMAC does a full LinkList dump which can return partial
		// results under NLM_F_DUMP_INTR (netlink dump interrupted by
		// concurrent namespace changes from other subtests' cleanups).
		parent, err := nlwrap.LinkByName("test-idem-prt1")
		if err != nil {
			t.Fatalf("parent NIC not found: %v", err)
		}
		staleIPVL := &netlink.IPVlan{
			LinkAttrs: netlink.LinkAttrs{
				Name:        ipvlName,
				ParentIndex: parent.Attrs().Index,
			},
			Mode: netlink.IPVLAN_MODE_L3,
		}
		if err := netlink.LinkAdd(staleIPVL); err != nil {
			t.Fatalf("failed to create stale ipvlan: %v", err)
		}
		// Confirm stale child exists on host.
		if _, err := nlwrap.LinkByName(ipvlName); err != nil {
			t.Fatalf("stale ipvlan %s not found on host after creation: %v", ipvlName, err)
		}

		// Retry: nsAttachIPVlanL3 should reuse the stale child and succeed.
		deviceData, err := nsAttachIPVlanL3(cfg, nsPath)
		if err != nil {
			t.Fatalf("nsAttachIPVlanL3 retry failed with stale child on host: %v", err)
		}
		if deviceData.InterfaceName != sharedNICInterfaceName {
			t.Errorf("InterfaceName = %s, want %s", deviceData.InterfaceName, sharedNICInterfaceName)
		}
		if len(deviceData.IPs) != 1 || deviceData.IPs[0] != "10.244.9.10/32" {
			t.Errorf("IPs = %v, want [10.244.9.10/32]", deviceData.IPs)
		}

	})

	// The child reaches the pod but keeps its temporary name; retry recreates it cleanly.
	t.Run("ChildMovedButNotRenamed", func(t *testing.T) {
		skipIfNotRoot(t)

		// Setup: parent NIC + pod namespace.
		testNS, nsPath := testNetns(t)
		parentMAC := testDummyNIC(t, "test-idem-prt2")

		cfg := &NICConfig{
			MAC:       parentMAC,
			PodIP:     "10.244.9.11",
			GatewayIP: secondaryNICVirtualGateway,
			PodUID:    podUID,
		}

		// Simulate a failed first attempt: create ipvlan child on host, then
		// move it into the pod namespace — but leave it with its original name
		// (ipvl-<uid>), simulating a failure before the rename to eth1.
		//
		// Use LinkByName (not findLinkByMAC) — see StaleChildOnHost comment.
		parent, err := nlwrap.LinkByName("test-idem-prt2")
		if err != nil {
			t.Fatalf("parent NIC not found: %v", err)
		}
		staleIPVL := &netlink.IPVlan{
			LinkAttrs: netlink.LinkAttrs{
				Name:        ipvlName,
				ParentIndex: parent.Attrs().Index,
			},
			Mode: netlink.IPVLAN_MODE_L3,
		}
		if err := netlink.LinkAdd(staleIPVL); err != nil {
			t.Fatalf("failed to create stale ipvlan: %v", err)
		}
		// Move into pod namespace (simulating successful LinkSetNsFd).
		containerNs, err := netns.GetFromPath(nsPath)
		if err != nil {
			t.Fatalf("failed to get container ns: %v", err)
		}
		defer containerNs.Close()
		if err := netlink.LinkSetNsFd(staleIPVL, int(containerNs)); err != nil {
			t.Fatalf("failed to move stale ipvlan to pod ns: %v", err)
		}
		// Dummy NIC quirk: parent MAC may change after child ns move; restore the
		// expected MAC so findLinkByMAC behavior matches real hardware NICs.
		ensureLinkMAC(t, "test-idem-prt2", parentMAC)
		// Confirm it's in the pod namespace (unrenamed).
		nhNs, err := nlwrap.NewHandleAt(testNS)
		if err != nil {
			t.Fatalf("failed to get ns handle: %v", err)
		}
		if _, err := nhNs.LinkByName(ipvlName); err != nil {
			t.Fatalf("stale ipvlan %s not found in pod ns: %v", ipvlName, err)
		}
		nhNs.Close()

		// Retry: nsAttachIPVlanL3 should remove the stale child and succeed.
		deviceData, err := nsAttachIPVlanL3(cfg, nsPath)
		if err != nil {
			t.Fatalf("nsAttachIPVlanL3 retry failed with stale child in pod ns: %v", err)
		}
		if deviceData.InterfaceName != sharedNICInterfaceName {
			t.Errorf("InterfaceName = %s, want %s", deviceData.InterfaceName, sharedNICInterfaceName)
		}

	})

	// The child is named eth1 but only partly configured; retry recreates its pod setup.
	t.Run("ChildRenamedButNotConfigured", func(t *testing.T) {
		skipIfNotRoot(t)

		// Setup: parent NIC + pod namespace.
		testNS, nsPath := testNetns(t)
		parentMAC := testDummyNIC(t, "test-idem-prt3")

		cfg := &NICConfig{
			MAC:       parentMAC,
			PodIP:     "10.244.9.12",
			GatewayIP: secondaryNICVirtualGateway,
			PodUID:    podUID,
		}

		// Simulate a failed first attempt: create ipvlan child, move it into
		// the pod namespace, and rename it to eth1 — but leave IP/route
		// configuration incomplete, simulating a failure during AddrAdd or
		// RouteAdd.
		//
		// Use LinkByName (not findLinkByMAC) — see StaleChildOnHost comment.
		parent, err := nlwrap.LinkByName("test-idem-prt3")
		if err != nil {
			t.Fatalf("parent NIC not found: %v", err)
		}
		staleIPVL := &netlink.IPVlan{
			LinkAttrs: netlink.LinkAttrs{
				Name:        ipvlName,
				ParentIndex: parent.Attrs().Index,
			},
			Mode: netlink.IPVLAN_MODE_L3,
		}
		if err := netlink.LinkAdd(staleIPVL); err != nil {
			t.Fatalf("failed to create stale ipvlan: %v", err)
		}
		containerNs, err := netns.GetFromPath(nsPath)
		if err != nil {
			t.Fatalf("failed to get container ns: %v", err)
		}
		defer containerNs.Close()
		// Move into pod namespace.
		if err := netlink.LinkSetNsFd(staleIPVL, int(containerNs)); err != nil {
			t.Fatalf("failed to move stale ipvlan to pod ns: %v", err)
		}
		// Dummy NIC quirk: parent MAC may change after child ns move; restore the
		// expected MAC so findLinkByMAC behavior matches real hardware NICs.
		ensureLinkMAC(t, "test-idem-prt3", parentMAC)
		// Rename to eth1 inside the pod namespace.
		nhNs, err := nlwrap.NewHandleAt(testNS)
		if err != nil {
			t.Fatalf("failed to get ns handle: %v", err)
		}
		nsLink, err := nhNs.LinkByName(ipvlName)
		if err != nil {
			t.Fatalf("stale ipvlan %s not found in pod ns: %v", ipvlName, err)
		}
		if err := nhNs.LinkSetName(nsLink, sharedNICInterfaceName); err != nil {
			t.Fatalf("failed to rename stale ipvlan to %s: %v", sharedNICInterfaceName, err)
		}
		// Confirm eth1 exists in pod ns.
		if _, err := nhNs.LinkByName(sharedNICInterfaceName); err != nil {
			t.Fatalf("stale %s not found in pod ns after rename: %v", sharedNICInterfaceName, err)
		}
		nhNs.Close()

		// Retry: nsAttachIPVlanL3 should remove the stale eth1 and succeed.
		deviceData, err := nsAttachIPVlanL3(cfg, nsPath)
		if err != nil {
			t.Fatalf("nsAttachIPVlanL3 retry failed with stale eth1 in pod ns: %v", err)
		}
		if deviceData.InterfaceName != sharedNICInterfaceName {
			t.Errorf("InterfaceName = %s, want %s", deviceData.InterfaceName, sharedNICInterfaceName)
		}
		if len(deviceData.IPs) != 1 || deviceData.IPs[0] != "10.244.9.12/32" {
			t.Errorf("IPs = %v, want [10.244.9.12/32]", deviceData.IPs)
		}

	})
}

// TestIntegration_cleanupStaleIPVlanLinks_PreservesUnrelatedIPVlan verifies that
// retry removes only this pod's stale child and leaves another IPVLAN untouched.
func TestIntegration_cleanupStaleIPVlanLinks_PreservesUnrelatedIPVlan(t *testing.T) {
	skipIfNotRoot(t)

	testNS, nsPath := testNetns(t)
	testDummyNIC(t, "test-clean-prt")
	parent, err := nlwrap.LinkByName("test-clean-prt")
	if err != nil {
		t.Fatalf("failed to find parent: %v", err)
	}
	destinationNS, err := netns.GetFromPath(nsPath)
	if err != nil {
		t.Fatalf("failed to open pod namespace: %v", err)
	}
	defer destinationNS.Close()

	for _, name := range []string{"ipvl-owned", "ipvl-unrelated"} {
		link := &netlink.IPVlan{
			LinkAttrs: netlink.LinkAttrs{Name: name, ParentIndex: parent.Attrs().Index},
			Mode:      netlink.IPVLAN_MODE_L3,
		}
		if err := netlink.LinkAdd(link); err != nil {
			t.Fatalf("failed to create %s: %v", name, err)
		}
		if err := netlink.LinkSetNsFd(link, int(destinationNS)); err != nil {
			t.Fatalf("failed to move %s into pod namespace: %v", name, err)
		}
	}

	handle, err := nlwrap.NewHandleAt(testNS)
	if err != nil {
		t.Fatalf("failed to create pod netlink handle: %v", err)
	}
	defer handle.Close()
	if err := cleanupStaleIPVlanLinks(handle, "ipvl-owned", sharedNICInterfaceName); err != nil {
		t.Fatalf("cleanupStaleIPVlanLinks failed: %v", err)
	}
	if _, err := handle.LinkByName("ipvl-owned"); err == nil {
		t.Fatal("owned stale IPVLAN still exists after cleanup")
	}
	if _, err := handle.LinkByName("ipvl-unrelated"); err != nil {
		t.Fatalf("unrelated IPVLAN was removed: %v", err)
	}
}

// TestIntegration_cleanupStaleIPVlanLinks_RejectsNonIPVlanOwnedName verifies that
// retry refuses to delete an unrelated interface that happens to use our name.
func TestIntegration_cleanupStaleIPVlanLinks_RejectsNonIPVlanOwnedName(t *testing.T) {
	skipIfNotRoot(t)

	testNS, nsPath := testNetns(t)
	destinationNS, err := netns.GetFromPath(nsPath)
	if err != nil {
		t.Fatalf("failed to open pod namespace: %v", err)
	}
	defer destinationNS.Close()

	const ownedName = "ipvl-owned"
	collision := &netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Name: ownedName}}
	if err := netlink.LinkAdd(collision); err != nil {
		t.Fatalf("failed to create same-name dummy link: %v", err)
	}
	if err := netlink.LinkSetNsFd(collision, int(destinationNS)); err != nil {
		t.Fatalf("failed to move same-name dummy link into pod namespace: %v", err)
	}

	handle, err := nlwrap.NewHandleAt(testNS)
	if err != nil {
		t.Fatalf("failed to create pod netlink handle: %v", err)
	}
	defer handle.Close()

	err = cleanupStaleIPVlanLinks(handle, ownedName, sharedNICInterfaceName)
	if err == nil || !strings.Contains(err.Error(), "expected stale ipvlan") {
		t.Fatalf("cleanup error = %v, want non-IPVLAN collision error", err)
	}
	remaining, err := handle.LinkByName(ownedName)
	if err != nil {
		t.Fatalf("same-name non-IPVLAN link was removed: %v", err)
	}
	if remaining.Type() != "dummy" {
		t.Fatalf("same-name link type = %q, want dummy", remaining.Type())
	}
	if err := handle.LinkDel(remaining); err != nil {
		t.Fatalf("failed to remove test collision link: %v", err)
	}
}

// TestIntegration_nsAttachExclusiveNIC_FullCycle tests the complete exclusive NIC lifecycle:
//  1. Create a dummy NIC in the host namespace (simulating an exclusive physical NIC).
//  2. Attach it with DHCP stubbed successful, isolating NIC/IP/route behavior.
//  3. Verify: NIC is in pod ns, not on host, has IP, routes, link UP.
//  4. Call cleanupExclusiveNIC and verify the NIC is returned to the host.
//
// In plain terms: the whole NIC enters the pod and returns to the host on cleanup.
func TestIntegration_nsAttachExclusiveNIC_FullCycle(t *testing.T) {
	skipIfNotRoot(t)

	// Create an isolated pod namespace and a dummy NIC simulating an exclusive
	// physical NIC (1:1 NIC-to-pod mapping, as opposed to shared ipvlan).
	_, nsPath := testNetns(t)
	nicMAC := testDummyNIC(t, "test-dnic-ded")

	// Build NICConfig for exclusive NIC. Addresses are carried directly in
	// NICConfig for exclusive mode.
	cfg := &NICConfig{
		MAC:       nicMAC,
		GatewayIP: secondaryNICVirtualGateway,
		Addresses: []string{"10.244.2.50/24"},
	}

	// Dummy NIC quirk: ensure the host-side MAC is still the expected value
	// before invoking MAC-based exclusive attach logic.
	ensureLinkMAC(t, "test-dnic-ded", nicMAC)

	// --- Attach: move NIC into pod ns with full IP + route configuration ---
	deviceData, err := nsAttachExclusiveNICWithPostAttach(cfg, nsPath, nil)
	if err != nil {
		t.Fatalf("exclusive NIC attach failed: %v", err)
	}

	// Verify returned NetworkDeviceData fields.
	// HardwareAddress should match the NIC's MAC.
	if deviceData.HardwareAddress != nicMAC {
		t.Errorf("HardwareAddress = %s, want %s", deviceData.HardwareAddress, nicMAC)
	}
	// IPs should contain the addresses we assigned.
	if len(deviceData.IPs) != 1 || deviceData.IPs[0] != "10.244.2.50/24" {
		t.Errorf("IPs = %v, want [10.244.2.50/24]", deviceData.IPs)
	}
	// InterfaceName should be the original host name (exclusive NICs are NOT renamed).
	if deviceData.InterfaceName != "test-dnic-ded" {
		t.Errorf("InterfaceName = %s, want test-dnic-ded", deviceData.InterfaceName)
	}

	// After attach, the NIC should no longer be visible in the host namespace —
	// it was moved into the pod namespace by LinkSetNsFd.
	_, err = findLinkByMAC(nicMAC)
	if err == nil {
		t.Error("exclusive NIC still visible on host after nsAttachExclusiveNIC")
	}

	// Confirm the NIC is now inside the pod namespace by MAC-based lookup.
	if !nicExistsInNetns(nsPath, nicMAC) {
		t.Error("exclusive NIC not found in container netns after attach")
	}

	// --- Cleanup: move NIC back from pod ns to host ns ---
	// This simulates pod teardown. cleanupExclusiveNIC finds the NIC by MAC
	// inside the pod ns and moves it back to the host namespace.
	cleanupExclusiveNIC(nsPath, nicMAC)

	// Verify the NIC is back on the host and has the correct MAC.
	link, err := findLinkByMAC(nicMAC)
	if err != nil {
		t.Fatalf("exclusive NIC not found on host after cleanupExclusiveNIC: %v", err)
	}
	if link.Attrs().HardwareAddr.String() != nicMAC {
		t.Errorf("returned NIC MAC = %s, want %s", link.Attrs().HardwareAddr.String(), nicMAC)
	}
}

// TestIntegration_nsAttachExclusiveNIC_PreflightFailureDoesNotMoveNIC verifies
// malformed configuration is rejected before the exclusive NIC leaves the host.
// In plain terms: invalid input must not move the NIC into the pod.
func TestIntegration_nsAttachExclusiveNIC_PreflightFailureDoesNotMoveNIC(t *testing.T) {
	skipIfNotRoot(t)

	_, nsPath := testNetns(t)
	nicMAC := testDummyNIC(t, "test-dnic-rb")

	cfg := &NICConfig{
		MAC:       nicMAC,
		GatewayIP: "not-an-ip",
		Addresses: []string{"10.244.3.60/24"},
	}

	ensureLinkMAC(t, "test-dnic-rb", nicMAC)

	// The attach must fail before LinkSetNsFd moves the NIC.
	if _, err := nsAttachExclusiveNIC(cfg, nsPath); err == nil {
		t.Fatal("expected nsAttachExclusiveNIC to fail with an invalid gateway IP, got nil")
	}

	// Preflight assertion: the NIC remains in the host namespace.
	link, err := findLinkByMAC(nicMAC)
	if err != nil {
		t.Fatalf("exclusive NIC left the host during preflight validation: %v", err)
	}
	if link.Attrs().HardwareAddr.String() != nicMAC {
		t.Errorf("returned NIC MAC = %s, want %s", link.Attrs().HardwareAddr.String(), nicMAC)
	}

	// The NIC must never appear inside the pod namespace.
	if nicExistsInNetns(nsPath, nicMAC) {
		t.Error("exclusive NIC moved into pod netns before preflight validation completed")
	}
}

// TestIntegration_nsAttachExclusiveNIC_RollbackOnPostMoveFailure verifies that
// a failure after the NIC enters the pod returns it to the host for retry.
func TestIntegration_nsAttachExclusiveNIC_RollbackOnPostMoveFailure(t *testing.T) {
	skipIfNotRoot(t)

	_, nsPath := testNetns(t)
	nicMAC := testDummyNIC(t, "test-dnic-rb2")
	ensureLinkMAC(t, "test-dnic-rb2", nicMAC)

	cfg := &NICConfig{
		MAC:       nicMAC,
		GatewayIP: secondaryNICVirtualGateway,
		Addresses: []string{"10.244.3.61/24"},
	}
	discoverErr := errors.New("forced DHCP discover failure")
	discoverCalled := false
	discover := func(containerNsPath, ifName string, mac net.HardwareAddr) error {
		discoverCalled = true
		if _, err := findLinkByMAC(nicMAC); err == nil {
			t.Error("exclusive NIC is still visible on host at the post-move failure point")
		}
		if !nicExistsInNetns(containerNsPath, mac.String()) {
			t.Errorf("exclusive NIC %s is not in the pod netns at the post-move failure point", ifName)
		}
		return discoverErr
	}

	_, err := nsAttachExclusiveNICWithPostAttach(cfg, nsPath, discover)
	if !errors.Is(err, discoverErr) {
		t.Fatalf("attach error = %v, want forced DHCP failure", err)
	}
	if !discoverCalled {
		t.Fatal("DHCP discover hook was not reached after moving the NIC")
	}

	link, err := findLinkByMAC(nicMAC)
	if err != nil {
		t.Fatalf("exclusive NIC was not returned to host after post-move failure: %v", err)
	}
	if link.Attrs().HardwareAddr.String() != nicMAC {
		t.Errorf("returned NIC MAC = %s, want %s", link.Attrs().HardwareAddr, nicMAC)
	}
	if nicExistsInNetns(nsPath, nicMAC) {
		t.Error("exclusive NIC remains in pod netns after rollback")
	}
}

// TestIntegration_nsAttachExclusiveNIC_ReclaimsSharedVRF verifies that a NIC
// previously used as a shared ipvlan parent can be reused by an exclusive pod.
// Exclusive attach must detach the parent from its per-MAC shared NIC VRF,
// delete the VRF and its route state, and then move the NIC into the pod
// namespace.
// In plain terms: remove all shared host state before the exclusive pod takes the NIC.
func TestIntegration_nsAttachExclusiveNIC_ReclaimsSharedVRF(t *testing.T) {
	skipIfNotRoot(t)

	_, nsPath := testNetns(t)
	nicMAC := testDummyNIC(t, "test-ded-vrf")
	ensureLinkMAC(t, "test-ded-vrf", nicMAC)

	parent, err := nlwrap.LinkByName("test-ded-vrf")
	if err != nil {
		t.Fatalf("failed to find dummy parent: %v", err)
	}
	vrfName := sharedNICVRFName(nicMAC)
	vrfTable, err := sharedNICVRFTable(nicMAC)
	if err != nil {
		t.Fatalf("failed to compute VRF table: %v", err)
	}
	vrf := &netlink.Vrf{LinkAttrs: netlink.LinkAttrs{Name: vrfName}, Table: uint32(vrfTable)}
	if err := netlink.LinkAdd(vrf); err != nil {
		t.Fatalf("failed to create VRF %s: %v", vrfName, err)
	}
	t.Cleanup(func() {
		if staleVRF, err := nlwrap.LinkByName(vrfName); err == nil {
			_ = netlink.LinkDel(staleVRF)
		}
	})
	vrfLink, err := nlwrap.LinkByName(vrfName)
	if err != nil {
		t.Fatalf("VRF %s not found after creation: %v", vrfName, err)
	}
	if err := netlink.LinkSetUp(vrfLink); err != nil {
		t.Fatalf("failed to bring VRF up: %v", err)
	}
	if err := netlink.LinkSetMaster(parent, vrfLink); err != nil {
		t.Fatalf("failed to enslave parent to VRF: %v", err)
	}
	parent, err = nlwrap.LinkByName("test-ded-vrf")
	if err != nil {
		t.Fatalf("failed to refresh parent after VRF enslave: %v", err)
	}
	gwIP := net.ParseIP(secondaryNICVirtualGateway).To4()
	if err := netlink.RouteReplace(&netlink.Route{
		Dst:       &net.IPNet{IP: gwIP, Mask: net.CIDRMask(32, 32)},
		LinkIndex: parent.Attrs().Index,
		Scope:     netlink.SCOPE_LINK,
		Table:     vrfTable,
	}); err != nil {
		t.Fatalf("failed to add VRF gateway route: %v", err)
	}
	_, defaultDst, _ := net.ParseCIDR("0.0.0.0/0")
	if err := netlink.RouteReplace(&netlink.Route{
		Dst:       defaultDst,
		Gw:        gwIP,
		LinkIndex: parent.Attrs().Index,
		Table:     vrfTable,
		Flags:     int(netlink.FLAG_ONLINK),
	}); err != nil {
		t.Fatalf("failed to add VRF default route: %v", err)
	}
	sdnGWMAC, _ := net.ParseMAC(secondaryNICSDNGatewayMAC)
	if err := netlink.NeighSet(&netlink.Neigh{
		LinkIndex:    parent.Attrs().Index,
		IP:           gwIP,
		HardwareAddr: sdnGWMAC,
		State:        netlink.NUD_PERMANENT,
	}); err != nil {
		t.Fatalf("failed to add VRF gateway neighbor: %v", err)
	}
	script := useScriptedIPTables(t, []error{nil, commandExitError(t, 1)})

	cfg := &NICConfig{
		MAC:       nicMAC,
		GatewayIP: secondaryNICVirtualGateway,
		Addresses: []string{"10.244.8.50/24"},
	}
	postAttach := func(containerNsPath, ifName string, _ net.HardwareAddr) error {
		containerNS, err := netns.GetFromPath(containerNsPath)
		if err != nil {
			return fmt.Errorf("open pod namespace: %w", err)
		}
		defer containerNS.Close()
		handle, err := nlwrap.NewHandleAt(containerNS)
		if err != nil {
			return fmt.Errorf("create pod netlink handle: %w", err)
		}
		defer handle.Close()
		link, err := handle.LinkByName(ifName)
		if err != nil {
			return fmt.Errorf("find exclusive NIC in pod: %w", err)
		}
		neighbors, err := handle.NeighList(link.Attrs().Index, netlink.FAMILY_V4)
		if err != nil {
			return fmt.Errorf("list exclusive NIC neighbors: %w", err)
		}
		for _, neighbor := range neighbors {
			if neighbor.IP.Equal(gwIP) {
				return fmt.Errorf("stale shared gateway neighbor %s remains after reclaim", gwIP)
			}
		}
		return nil
	}
	if _, err := nsAttachExclusiveNICWithPostAttach(cfg, nsPath, postAttach); err != nil {
		t.Fatalf("exclusive NIC attach failed while reclaiming VRF parent: %v", err)
	}
	if _, err := nlwrap.LinkByName(vrfName); err == nil {
		t.Fatalf("VRF %s still exists after exclusive reclaim", vrfName)
	}
	if !nicExistsInNetns(nsPath, nicMAC) {
		t.Fatal("exclusive NIC not found in pod namespace after reclaim")
	}
	routes, err := netlink.RouteListFiltered(netlink.FAMILY_V4, &netlink.Route{Table: vrfTable}, netlink.RT_FILTER_TABLE)
	if err != nil {
		t.Fatalf("failed to inspect reclaimed VRF table %d: %v", vrfTable, err)
	}
	if len(routes) != 0 {
		t.Fatalf("VRF table %d still has routes after reclaim: %v", vrfTable, routes)
	}
	if len(script.calls) != 2 {
		t.Fatalf("NAT cleanup calls = %d, want delete followed by not-found", len(script.calls))
	}
	for i, call := range script.calls {
		if got := strings.Join(call, " "); got != "-t nat -D POSTROUTING -o test-ded-vrf -j ACCEPT" {
			t.Fatalf("NAT cleanup call %d = %q, want shared exemption delete", i+1, got)
		}
	}

	cleanupExclusiveNIC(nsPath, nicMAC)
	if _, err := findLinkByMAC(nicMAC); err != nil {
		t.Fatalf("exclusive NIC not returned to host after cleanup: %v", err)
	}
}

// TestIntegration_SecondaryNICConcurrentSharedAndExclusiveSameMAC runs a shared
// attach and an exclusive attach/reclaim on the same physical NIC concurrently.
// The per-MAC secondaryNICLock must serialize them so host VRF programming and
// the namespace move cannot interleave, and the cleanupExclusiveNIC locked/
// unlocked split must not self-deadlock on the in-attach rollback. Whichever
// wins the lock first, the outcome is one of two clean states with no
// corruption; run under -race to catch interleaved netlink mutation.
// In plain terms: simultaneous requests leave one valid owner and no orphaned state.
func TestIntegration_SecondaryNICConcurrentSharedAndExclusiveSameMAC(t *testing.T) {
	skipIfNotRoot(t)

	_, sharedNsPath := testNetns(t)
	_, exclusiveNsPath := testNetns(t)
	nicMAC := testDummyNIC(t, "test-dnic-cc")
	ensureLinkMAC(t, "test-dnic-cc", nicMAC)

	// The shared attach may leave a host VRF if it wins but exclusive fails to
	// reclaim; delete it on teardown regardless of which path won.
	t.Cleanup(func() {
		if v, err := nlwrap.LinkByName(sharedNICVRFName(nicMAC)); err == nil {
			_ = netlink.LinkDel(v)
		}
	})

	sharedCfg := &NICConfig{
		MAC:       nicMAC,
		PodIP:     "10.244.1.42",
		GatewayIP: secondaryNICVirtualGateway,
		PodUID:    "abcdef12-3456-7890-abcd-ef1234567890",
	}
	exclusiveCfg := &NICConfig{
		MAC:       nicMAC,
		GatewayIP: secondaryNICVirtualGateway,
		Addresses: []string{"10.244.8.50/24"},
	}

	var wg sync.WaitGroup
	var sharedErr, exclusiveErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		// Pin to a host-namespace thread; the attach switches into the pod ns
		// through explicit handles and must start from the host ns.
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		if err := netns.Set(hostNS); err != nil {
			sharedErr = fmt.Errorf("enter host ns: %w", err)
			return
		}
		_, sharedErr = nsAttachIPVlanL3(sharedCfg, sharedNsPath)
	}()
	go func() {
		defer wg.Done()
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		if err := netns.Set(hostNS); err != nil {
			exclusiveErr = fmt.Errorf("enter host ns: %w", err)
			return
		}
		_, exclusiveErr = nsAttachExclusiveNICWithPostAttach(exclusiveCfg, exclusiveNsPath, successfulTestPostAttach)
	}()

	// A lock-ordering deadlock (e.g., the rollback re-locking) would hang both
	// goroutines; fail fast instead of relying on the outer test timeout.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent shared+exclusive attach deadlocked")
	}

	// Exactly one of two serialized outcomes is valid. The ordering is
	// nondeterministic, so assert the outcome that occurred rather than a winner.
	switch {
	case exclusiveErr == nil:
		// Exclusive ran after shared (reclaiming the VRF) or before it: either
		// way it owns the NIC, which must live in the exclusive pod ns with no
		// orphaned host VRF left behind.
		if !nicExistsInNetns(exclusiveNsPath, nicMAC) {
			t.Fatalf("physical NIC not in exclusive pod ns after concurrent attach (sharedErr=%v)", sharedErr)
		}
		if _, err := nlwrap.LinkByName(sharedNICVRFName(nicMAC)); err == nil {
			t.Fatalf("shared VRF %s still present after exclusive reclaim", sharedNICVRFName(nicMAC))
		}
		cleanupExclusiveNIC(exclusiveNsPath, nicMAC)
	case sharedErr == nil:
		// Exclusive ran first and moved the NIC into its pod, so the later shared
		// attach failed cleanly (parent NIC no longer on host). No corruption to
		// assert beyond that; namespaces and VRF are reaped by t.Cleanup.
	default:
		t.Fatalf("both shared and exclusive attach failed under concurrency: sharedErr=%v exclusiveErr=%v", sharedErr, exclusiveErr)
	}
}

// TestIntegration_detachNICFromSharedVRF_RejectsWrongTableBeforeMutation verifies
// that unexpected VRF ownership fails without changing the NIC or deleting state.
func TestIntegration_detachNICFromSharedVRF_RejectsWrongTableBeforeMutation(t *testing.T) {
	skipIfNotRoot(t)

	nicMAC := testDummyNIC(t, "test-wrong-vrf")
	parent, err := nlwrap.LinkByName("test-wrong-vrf")
	if err != nil {
		t.Fatalf("failed to find parent: %v", err)
	}
	expectedTable, err := sharedNICVRFTable(nicMAC)
	if err != nil {
		t.Fatalf("failed to compute expected VRF table: %v", err)
	}
	vrfName := sharedNICVRFName(nicMAC)
	wrongVRF := &netlink.Vrf{
		LinkAttrs: netlink.LinkAttrs{Name: vrfName},
		Table:     uint32(expectedTable + 1),
	}
	if err := netlink.LinkAdd(wrongVRF); err != nil {
		t.Fatalf("failed to create wrong-table VRF: %v", err)
	}
	t.Cleanup(func() {
		if link, err := nlwrap.LinkByName(vrfName); err == nil {
			_ = netlink.LinkDel(link)
		}
	})
	vrfLink, err := nlwrap.LinkByName(vrfName)
	if err != nil {
		t.Fatalf("failed to refresh wrong-table VRF: %v", err)
	}
	if err := netlink.LinkSetMaster(parent, vrfLink); err != nil {
		t.Fatalf("failed to enslave parent to wrong-table VRF: %v", err)
	}

	if _, err := detachNICFromSharedVRF(parent, nicMAC); err == nil || !strings.Contains(err.Error(), "expected") {
		t.Fatalf("detach error = %v, want VRF table mismatch", err)
	}
	parent, err = nlwrap.LinkByName("test-wrong-vrf")
	if err != nil {
		t.Fatalf("parent disappeared after rejected detach: %v", err)
	}
	if parent.Attrs().MasterIndex != vrfLink.Attrs().Index {
		t.Fatalf("parent master index = %d, want unchanged VRF index %d", parent.Attrs().MasterIndex, vrfLink.Attrs().Index)
	}
	if _, err := nlwrap.LinkByName(vrfName); err != nil {
		t.Fatalf("wrong-table VRF was deleted before ownership validation: %v", err)
	}
}

// TestIntegration_ensureSharedNICVRFLink_RejectsTableCollision verifies that two
// different VRFs cannot use the same routing table or alter the existing owner.
func TestIntegration_ensureSharedNICVRFLink_RejectsTableCollision(t *testing.T) {
	skipIfNotRoot(t)

	const table = 424242
	ownerName := "test-vrf-owner"
	requestedName := "test-vrf-new"
	owner := &netlink.Vrf{
		LinkAttrs: netlink.LinkAttrs{Name: ownerName},
		Table:     table,
	}
	if err := netlink.LinkAdd(owner); err != nil {
		t.Fatalf("failed to create table owner VRF: %v", err)
	}
	t.Cleanup(func() {
		if link, err := nlwrap.LinkByName(ownerName); err == nil {
			_ = netlink.LinkDel(link)
		}
		if link, err := nlwrap.LinkByName(requestedName); err == nil {
			_ = netlink.LinkDel(link)
		}
	})

	if _, err := ensureSharedNICVRFLink(requestedName, table); err == nil || !strings.Contains(err.Error(), "already owned") {
		t.Fatalf("ensure error = %v, want table ownership collision", err)
	}
	if _, err := nlwrap.LinkByName(requestedName); err == nil {
		t.Fatalf("VRF %s was created despite table collision", requestedName)
	}
	if _, err := nlwrap.LinkByName(ownerName); err != nil {
		t.Fatalf("existing table owner VRF was mutated: %v", err)
	}
}

// TestIntegration_ensureSharedNICVRFLink_RejectsNameCollisions verifies that an
// existing wrong-type link or wrong-table VRF is rejected and preserved.
func TestIntegration_ensureSharedNICVRFLink_RejectsNameCollisions(t *testing.T) {
	skipIfNotRoot(t)

	tests := []struct {
		name      string
		link      netlink.Link
		table     int
		wantError string
	}{
		{
			name:      "non-VRF link",
			link:      &netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Name: "test-vrf-name"}},
			table:     424243,
			wantError: "expected vrf",
		},
		{
			name: "VRF with wrong table",
			link: &netlink.Vrf{
				LinkAttrs: netlink.LinkAttrs{Name: "test-vrf-table"},
				Table:     424244,
			},
			table:     424245,
			wantError: "expected 424245",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := netlink.LinkAdd(test.link); err != nil {
				t.Fatalf("failed to create collision link: %v", err)
			}
			name := test.link.Attrs().Name
			t.Cleanup(func() {
				if link, err := nlwrap.LinkByName(name); err == nil {
					_ = netlink.LinkDel(link)
				}
			})

			_, err := ensureSharedNICVRFLink(name, test.table)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("ensure error = %v, want substring %q", err, test.wantError)
			}
			remaining, err := nlwrap.LinkByName(name)
			if err != nil {
				t.Fatalf("collision link was removed: %v", err)
			}
			if remaining.Type() != test.link.Type() {
				t.Fatalf("collision link type = %q, want %q", remaining.Type(), test.link.Type())
			}
		})
	}
}

// TestIntegration_ensureSharedNICParentVRF_RejectsUnexpectedMaster verifies that
// a NIC already owned by another VRF is rejected without changing its owner.
func TestIntegration_ensureSharedNICParentVRF_RejectsUnexpectedMaster(t *testing.T) {
	skipIfNotRoot(t)

	parentName := "test-wrong-mst"
	parentMAC := testDummyNIC(t, parentName)
	parent, err := nlwrap.LinkByName(parentName)
	if err != nil {
		t.Fatalf("failed to find parent: %v", err)
	}
	wrongVRF := &netlink.Vrf{
		LinkAttrs: netlink.LinkAttrs{Name: "test-other-vrf"},
		Table:     424246,
	}
	if err := netlink.LinkAdd(wrongVRF); err != nil {
		t.Fatalf("failed to create unexpected master VRF: %v", err)
	}
	t.Cleanup(func() {
		if vrf, err := nlwrap.LinkByName(sharedNICVRFName(parentMAC)); err == nil {
			_ = netlink.LinkDel(vrf)
		}
		if vrf, err := nlwrap.LinkByName(wrongVRF.Attrs().Name); err == nil {
			_ = netlink.LinkDel(vrf)
		}
	})
	wrongVRFLink, err := nlwrap.LinkByName(wrongVRF.Attrs().Name)
	if err != nil {
		t.Fatalf("failed to refresh unexpected master VRF: %v", err)
	}
	if err := netlink.LinkSetMaster(parent, wrongVRFLink); err != nil {
		t.Fatalf("failed to enslave parent to unexpected master: %v", err)
	}

	_, err = ensureSharedNICParentVRF(parentMAC)
	if err == nil || !strings.Contains(err.Error(), "unexpected master index") {
		t.Fatalf("ensure error = %v, want unexpected master error", err)
	}
	parent, err = nlwrap.LinkByName(parentName)
	if err != nil {
		t.Fatalf("parent disappeared after rejected ensure: %v", err)
	}
	if parent.Attrs().MasterIndex != wrongVRFLink.Attrs().Index {
		t.Fatalf("parent master index = %d, want unchanged index %d", parent.Attrs().MasterIndex, wrongVRFLink.Attrs().Index)
	}
}

// nicExistsInNetns reports whether a NIC with the given MAC address exists in
// the specified network namespace. Test-only helper used by the exclusive-NIC
// integration tests to verify placement across a netns move.
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

// TestIntegration_cleanupExclusiveNIC_NonexistentNetns verifies that cleanup
// handles a deleted namespace gracefully. When a pod's namespace is already gone
// (e.g., kubelet deleted the sandbox), the kernel automatically returns the NIC
// to the host namespace. cleanupExclusiveNIC should detect this and return
// without error or panic.
// In plain terms: cleanup is a safe no-op after the pod namespace is gone.
func TestIntegration_cleanupExclusiveNIC_NonexistentNetns(t *testing.T) {
	skipIfNotRoot(t)

	// Call cleanup with a netns path that doesn't exist — this should be a no-op.
	// The function logs a klog.V(2) message and returns immediately.
	cleanupExclusiveNIC("/run/netns/nonexistent-test-ns", "00:11:22:33:44:55")
}
