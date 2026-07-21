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
	"fmt"
	"net"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"sigs.k8s.io/dranet/internal/nlwrap"
)

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

// Integration test run helper:
//   cd /home/<username>/projects/dranet && sudo /home/<usrname>/go-install/go/bin/go test ./pkg/driver -run '^TestIntegration_' -timeout 30s -v
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
// IMPORTANT: the goroutine is locked to the current OS thread for the entire
// lifetime of the test (unlocked via t.Cleanup). This is necessary because:
//  1. Network namespace operations (Unshare, Set) are per-thread in Linux.
//  2. The Go runtime may clone new threads from a thread that is temporarily
//     in a non-host namespace (during NewNamed → Unshare). Those cloned threads
//     inherit the wrong namespace and are never restored.
//  3. After UnlockOSThread, the goroutine can migrate to one of those "dirty"
//     threads, causing subsequent netlink calls to operate in the wrong namespace.
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

	// Create a named network namespace. This bind-mounts it under /run/netns/<name>
	// so it persists until explicitly deleted (netns.DeleteNamed).
	testNS, err := netns.NewNamed(nsName)
	if err != nil {
		runtime.UnlockOSThread()
		t.Fatalf("failed to create namespace %s: %v", nsName, err)
	}

	// NewNamed switches the current thread into the new namespace — switch back
	// to the host namespace so subsequent operations run in the host context.
	if err := netns.Set(hostNS); err != nil {
		runtime.UnlockOSThread()
		t.Fatalf("failed to switch back to host namespace: %v", err)
	}

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

// --- Integration tests ---

// TestIntegration_findLinkByMAC verifies MAC-based lookup of a host interface.
// This is the foundational lookup used by all SwiftV2 attach functions — both
// shared (ipvlan) and dedicated NIC modes find their target NIC by MAC address.
func TestIntegration_findLinkByMAC(t *testing.T) {
	skipIfNotRoot(t)

	// Create a dummy NIC on the host — this gives us a known MAC to search for.
	mac := testDummyNIC(t, "test-swift-mac")

	// Positive case: findLinkByMAC should locate the dummy by its MAC address
	// and return a link whose name matches the one we created.
	link, err := findLinkByMAC(mac)
	if err != nil {
		t.Fatalf("findLinkByMAC(%s) failed: %v", mac, err)
	}
	if link.Attrs().Name != "test-swift-mac" {
		t.Errorf("findLinkByMAC returned %q, want %q", link.Attrs().Name, "test-swift-mac")
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
func TestIntegration_nsAttachIPVlanL3_FullCycle(t *testing.T) {
	skipIfNotRoot(t)

	// Create an isolated network namespace to simulate the pod's netns.
	testNS, nsPath := testNetns(t)
	// Create a dummy NIC on the host to act as the shared parent (physical NIC).
	parentMAC := testDummyNIC(t, "test-swift-prt")

	// Build a NICConfig that mirrors what CNS would provide for a shared NIC pod.
	// MAC identifies the parent NIC; PodIP/GatewayIP configure the
	// ipvlan child; PodUID is used for the temporary ipvlan interface name.
	cfg := &NICConfig{
		MAC:       parentMAC,
		PodIP:     "10.244.1.42",
		GatewayIP: swiftV2VirtualGW,
		PodUID:    "abcdef12-3456-7890-abcd-ef1234567890",
	}

	// Dummy NIC quirk: MAC can occasionally regenerate; force it back to the
	// expected value so this test models real hardware NIC behavior.
	ensureLinkMAC(t, "test-swift-prt", parentMAC)

	// --- Attach: exercise the full nsAttachIPVlanL3 code path ---
	// This creates/enslaves the parent to a host VRF, creates an ipvlan L3 child
	// off the parent, moves the child into the pod ns, renames it to eth1, and
	// assigns IP + routes.
	deviceData, err := nsAttachIPVlanL3(cfg, nsPath)
	if err != nil {
		t.Fatalf("nsAttachIPVlanL3 failed: %v", err)
	}

	// Verify the returned NetworkDeviceData that DRA publishes to the ResourceSlice.
	// InterfaceName should be "eth1" (the constant swiftV2DelegatedIfName).
	if deviceData.InterfaceName != swiftV2DelegatedIfName {
		t.Errorf("InterfaceName = %s, want %s", deviceData.InterfaceName, swiftV2DelegatedIfName)
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
	nsLink, err := nhNs.LinkByName(swiftV2DelegatedIfName)
	if err != nil {
		t.Fatalf("interface %s not found in container ns: %v", swiftV2DelegatedIfName, err)
	}

	// Verify the interface is administratively UP (required for traffic to flow).
	if nsLink.Attrs().Flags&net.FlagUp == 0 {
		t.Error("interface eth1 is not UP in container ns")
	}

	// List IPv4 addresses on eth1 and verify the pod IP was assigned with /32 mask.
	// SwiftV2 uses point-to-point /32 addressing — all routing goes through the
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
		if r.Dst != nil && r.Dst.IP.String() == swiftV2VirtualGW {
			ones, _ := r.Dst.Mask.Size()
			if ones == 32 && r.Scope == netlink.SCOPE_LINK {
				foundGWRoute = true
			}
		}
		// Check for the default route via virtual gateway (default via 169.254.2.1 dev eth1).
		if r.Dst != nil && r.Dst.IP.String() == "0.0.0.0" && r.Gw != nil && r.Gw.String() == swiftV2VirtualGW {
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
	parent, err := nlwrap.LinkByName("test-swift-prt")
	if err != nil {
		t.Fatalf("parent NIC not found after attach: %v", err)
	}
	vrfName := swiftV2VRFName(parentMAC)
	vrfLink, err := nlwrap.LinkByName(vrfName)
	if err != nil {
		t.Fatalf("SwiftV2 VRF %s not found after attach: %v", vrfName, err)
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
	parentTable, err := swiftV2VRFTable(parentMAC)
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
		if r.Dst != nil && r.Dst.IP.String() == swiftV2VirtualGW {
			ones, _ := r.Dst.Mask.Size()
			foundGatewayRoute = ones == 32 && r.Scope == netlink.SCOPE_LINK
		}
		if (r.Dst == nil || r.Dst.String() == "0.0.0.0/0") && r.Gw != nil && r.Gw.String() == swiftV2VirtualGW {
			foundParentDefaultRoute = true
		}
	}
	if !foundGatewayRoute {
		t.Errorf("gateway /32 route for %s not found in parent table %d", swiftV2VirtualGW, parentTable)
	}
	if !foundParentDefaultRoute {
		t.Errorf("default route via %s not found in parent table %d", swiftV2VirtualGW, parentTable)
	}
	neighs, err := netlink.NeighList(parent.Attrs().Index, netlink.FAMILY_V4)
	if err != nil {
		t.Fatalf("failed to list parent neighbors: %v", err)
	}
	foundGatewayNeigh := false
	for _, n := range neighs {
		if n.IP.String() == swiftV2VirtualGW && n.HardwareAddr.String() == swiftV2SDNGatewayMAC && n.State == netlink.NUD_PERMANENT {
			foundGatewayNeigh = true
			break
		}
	}
	if !foundGatewayNeigh {
		t.Errorf("permanent gateway neighbor %s -> %s not found on parent", swiftV2VirtualGW, swiftV2SDNGatewayMAC)
	}

	cleanupIPVlanL3(cfg)
}

// TestIntegration_nsAttachIPVlanL3_IdempotentRetry tests the idempotent cleanup
// logic in nsAttachIPVlanL3. It covers all three stale-state scenarios that can
// result from a partially-failed previous attempt:
//
//	Subtest 1 – Stale child on host: a previous call created the ipvlan child
//	  (LinkAdd) but failed before moving it into the pod namespace (LinkSetNsFd).
//	  The stale child sits on the host. A retry must reuse it rather than failing
//	  with EEXIST on LinkAdd.
//
//	Subtest 2 – Stale child in pod ns (not renamed): the ipvlan child was moved
//	  into the pod namespace but the rename to eth1 failed. The stale child
//	  (ipvl-<uid>) is inside the pod ns. A retry must clean it up and succeed.
//
//	Subtest 3 – Stale eth1 in pod ns: the child was moved and renamed to eth1
//	  but IP or route configuration failed. A retry must delete the stale eth1
//	  and recreate everything from scratch.
func TestIntegration_nsAttachIPVlanL3_IdempotentRetry(t *testing.T) {
	skipIfNotRoot(t)

	// All subtests share the same NICConfig (same PodUID → same ipvl-<uid> name).
	podUID := "idempotent-test-uid-1234567890"
	ipvlName := fmt.Sprintf("ipvl-%s", truncateUID(podUID))

	t.Run("StaleChildOnHost", func(t *testing.T) {
		skipIfNotRoot(t)

		// Setup: parent NIC + pod namespace.
		_, nsPath := testNetns(t)
		parentMAC := testDummyNIC(t, "test-idem-prt1")

		cfg := &NICConfig{
			MAC:       parentMAC,
			PodIP:     "10.244.9.10",
			GatewayIP: swiftV2VirtualGW,
			PodUID:    podUID,
		}

		// Dummy NIC quirk: parent MAC can occasionally change; reset to expected
		// value so retry behavior matches real hardware NICs.
		ensureLinkMAC(t, "test-idem-prt1", parentMAC)

		// Simulate a failed first attempt: create the ipvlan child on the host
		// but do NOT move it into the pod namespace. This is the state left
		// behind when LinkAdd succeeds but a later step (e.g., RouteAdd or
		// LinkSetNsFd) fails.
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
		if deviceData.InterfaceName != swiftV2DelegatedIfName {
			t.Errorf("InterfaceName = %s, want %s", deviceData.InterfaceName, swiftV2DelegatedIfName)
		}
		if len(deviceData.IPs) != 1 || deviceData.IPs[0] != "10.244.9.10/32" {
			t.Errorf("IPs = %v, want [10.244.9.10/32]", deviceData.IPs)
		}

		// Clean up host route.
		cleanupIPVlanL3(cfg)
	})

	t.Run("StaleChildInPodNs_NotRenamed", func(t *testing.T) {
		skipIfNotRoot(t)

		// Setup: parent NIC + pod namespace.
		testNS, nsPath := testNetns(t)
		parentMAC := testDummyNIC(t, "test-idem-prt2")

		cfg := &NICConfig{
			MAC:       parentMAC,
			PodIP:     "10.244.9.11",
			GatewayIP: swiftV2VirtualGW,
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
		if deviceData.InterfaceName != swiftV2DelegatedIfName {
			t.Errorf("InterfaceName = %s, want %s", deviceData.InterfaceName, swiftV2DelegatedIfName)
		}

		// Clean up host route.
		cleanupIPVlanL3(cfg)
	})

	t.Run("StaleEth1InPodNs", func(t *testing.T) {
		skipIfNotRoot(t)

		// Setup: parent NIC + pod namespace.
		testNS, nsPath := testNetns(t)
		parentMAC := testDummyNIC(t, "test-idem-prt3")

		cfg := &NICConfig{
			MAC:       parentMAC,
			PodIP:     "10.244.9.12",
			GatewayIP: swiftV2VirtualGW,
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
		if err := nhNs.LinkSetName(nsLink, swiftV2DelegatedIfName); err != nil {
			t.Fatalf("failed to rename stale ipvlan to %s: %v", swiftV2DelegatedIfName, err)
		}
		// Confirm eth1 exists in pod ns.
		if _, err := nhNs.LinkByName(swiftV2DelegatedIfName); err != nil {
			t.Fatalf("stale %s not found in pod ns after rename: %v", swiftV2DelegatedIfName, err)
		}
		nhNs.Close()

		// Retry: nsAttachIPVlanL3 should remove the stale eth1 and succeed.
		deviceData, err := nsAttachIPVlanL3(cfg, nsPath)
		if err != nil {
			t.Fatalf("nsAttachIPVlanL3 retry failed with stale eth1 in pod ns: %v", err)
		}
		if deviceData.InterfaceName != swiftV2DelegatedIfName {
			t.Errorf("InterfaceName = %s, want %s", deviceData.InterfaceName, swiftV2DelegatedIfName)
		}
		if len(deviceData.IPs) != 1 || deviceData.IPs[0] != "10.244.9.12/32" {
			t.Errorf("IPs = %v, want [10.244.9.12/32]", deviceData.IPs)
		}

		// Clean up host route.
		cleanupIPVlanL3(cfg)
	})
}

// TestIntegration_nsAttachDedicatedNIC_FullCycle tests the complete dedicated NIC lifecycle:
//  1. Create a dummy NIC in the host namespace (simulating a dedicated physical NIC).
//  2. Call nsAttachDedicatedNIC to move it into the pod namespace with IP and routes.
//  3. Verify: NIC is in pod ns, not on host, has IP, routes, link UP.
//  4. Call cleanupDedicatedNIC and verify the NIC is returned to the host.
func TestIntegration_nsAttachDedicatedNIC_FullCycle(t *testing.T) {
	skipIfNotRoot(t)

	// Create an isolated pod namespace and a dummy NIC simulating a dedicated
	// physical NIC (1:1 NIC-to-pod mapping, as opposed to shared ipvlan).
	_, nsPath := testNetns(t)
	nicMAC := testDummyNIC(t, "test-swift-ded")

	// Build NICConfig for dedicated NIC. Addresses are carried directly in
	// NICConfig for dedicated mode.
	cfg := &NICConfig{
		MAC:       nicMAC,
		GatewayIP: swiftV2VirtualGW,
		Addresses: []string{"10.244.2.50/24"},
	}

	// Dummy NIC quirk: ensure the host-side MAC is still the expected value
	// before invoking MAC-based dedicated attach logic.
	ensureLinkMAC(t, "test-swift-ded", nicMAC)

	// --- Attach: move NIC into pod ns with full IP + route configuration ---
	deviceData, err := nsAttachDedicatedNIC(cfg, nsPath)
	if err != nil {
		t.Fatalf("nsAttachDedicatedNIC failed: %v", err)
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
	// InterfaceName should be the original host name (dedicated NICs are NOT renamed).
	if deviceData.InterfaceName != "test-swift-ded" {
		t.Errorf("InterfaceName = %s, want test-swift-ded", deviceData.InterfaceName)
	}

	// After attach, the NIC should no longer be visible in the host namespace —
	// it was moved into the pod namespace by LinkSetNsFd.
	_, err = findLinkByMAC(nicMAC)
	if err == nil {
		t.Error("dedicated NIC still visible on host after nsAttachDedicatedNIC")
	}

	// Confirm the NIC is now inside the pod namespace by MAC-based lookup.
	if !nicExistsInNetns(nsPath, nicMAC) {
		t.Error("dedicated NIC not found in container netns after attach")
	}

	// --- Cleanup: move NIC back from pod ns to host ns ---
	// This simulates pod teardown. cleanupDedicatedNIC finds the NIC by MAC
	// inside the pod ns and moves it back to the host namespace.
	cleanupDedicatedNIC(nsPath, nicMAC)

	// Verify the NIC is back on the host and has the correct MAC.
	link, err := findLinkByMAC(nicMAC)
	if err != nil {
		t.Fatalf("dedicated NIC not found on host after cleanupDedicatedNIC: %v", err)
	}
	if link.Attrs().HardwareAddr.String() != nicMAC {
		t.Errorf("returned NIC MAC = %s, want %s", link.Attrs().HardwareAddr.String(), nicMAC)
	}
}

// TestIntegration_nsAttachDedicatedNIC_ReclaimsSharedVRF verifies that a NIC
// previously used as a shared ipvlan parent can be reused by a dedicated pod.
// Dedicated attach must detach the parent from its per-MAC SwiftV2 VRF, clean
// the VRF route/neigh state, delete the VRF device, and then move the NIC into
// the pod namespace.
func TestIntegration_nsAttachDedicatedNIC_ReclaimsSharedVRF(t *testing.T) {
	skipIfNotRoot(t)

	_, nsPath := testNetns(t)
	nicMAC := testDummyNIC(t, "test-ded-vrf")
	ensureLinkMAC(t, "test-ded-vrf", nicMAC)

	parent, err := nlwrap.LinkByName("test-ded-vrf")
	if err != nil {
		t.Fatalf("failed to find dummy parent: %v", err)
	}
	vrfName := swiftV2VRFName(nicMAC)
	vrfTable, err := swiftV2VRFTable(nicMAC)
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
	gwIP := net.ParseIP(swiftV2VirtualGW).To4()
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
	sdnGWMAC, _ := net.ParseMAC(swiftV2SDNGatewayMAC)
	if err := netlink.NeighSet(&netlink.Neigh{
		LinkIndex:    parent.Attrs().Index,
		IP:           gwIP,
		HardwareAddr: sdnGWMAC,
		State:        netlink.NUD_PERMANENT,
	}); err != nil {
		t.Fatalf("failed to add VRF gateway neighbor: %v", err)
	}

	cfg := &NICConfig{
		MAC:       nicMAC,
		GatewayIP: swiftV2VirtualGW,
		Addresses: []string{"10.244.8.50/24"},
	}
	if _, err := nsAttachDedicatedNIC(cfg, nsPath); err != nil {
		t.Fatalf("nsAttachDedicatedNIC failed while reclaiming VRF parent: %v", err)
	}
	if _, err := nlwrap.LinkByName(vrfName); err == nil {
		t.Fatalf("VRF %s still exists after dedicated reclaim", vrfName)
	}
	if !nicExistsInNetns(nsPath, nicMAC) {
		t.Fatal("dedicated NIC not found in pod namespace after reclaim")
	}

	cleanupDedicatedNIC(nsPath, nicMAC)
	if _, err := findLinkByMAC(nicMAC); err != nil {
		t.Fatalf("dedicated NIC not returned to host after cleanup: %v", err)
	}
}

// TestIntegration_nsAttachDedicatedNIC_MultipleAddresses verifies that multiple
// IP addresses can be assigned to a dedicated NIC. This scenario occurs when CNS
// assigns multiple IPs to the same NIC (e.g., dual-stack or multi-IP pods).
func TestIntegration_nsAttachDedicatedNIC_MultipleAddresses(t *testing.T) {
	skipIfNotRoot(t)

	// Set up a pod namespace and a dummy NIC for the test.
	testNS, nsPath := testNetns(t)
	nicMAC := testDummyNIC(t, "test-swift-mip")

	cfg := &NICConfig{
		MAC:       nicMAC,
		GatewayIP: swiftV2VirtualGW,
		Addresses: []string{"10.244.3.10/24", "10.244.3.11/24"},
	}

	// Dummy NIC quirk: ensure the host-side MAC is still the expected value
	// before invoking MAC-based dedicated attach logic.
	ensureLinkMAC(t, "test-swift-mip", nicMAC)

	// Attach the dedicated NIC with both addresses.
	deviceData, err := nsAttachDedicatedNIC(cfg, nsPath)
	if err != nil {
		t.Fatalf("nsAttachDedicatedNIC failed: %v", err)
	}

	// The returned NetworkDeviceData should list both IPs.
	if len(deviceData.IPs) != 2 {
		t.Fatalf("expected 2 IPs, got %d: %v", len(deviceData.IPs), deviceData.IPs)
	}

	// Open a netlink handle into the pod namespace to inspect the NIC's addresses.
	nhNs, err := nlwrap.NewHandleAt(testNS)
	if err != nil {
		t.Fatalf("failed to get handle in test netns: %v", err)
	}
	defer nhNs.Close()

	// Find the NIC by its original name inside the pod namespace.
	nsLink, err := nhNs.LinkByName("test-swift-mip")
	if err != nil {
		t.Fatalf("NIC not found in container ns: %v", err)
	}

	// List all IPv4 addresses on the NIC and verify both expected IPs are present.
	addrs, err := nhNs.AddrList(nsLink, netlink.FAMILY_V4)
	if err != nil {
		t.Fatalf("failed to list addrs: %v", err)
	}

	// Track which expected IPs were found on the interface.
	expectedIPs := map[string]bool{"10.244.3.10": false, "10.244.3.11": false}
	for _, a := range addrs {
		if _, ok := expectedIPs[a.IPNet.IP.String()]; ok {
			expectedIPs[a.IPNet.IP.String()] = true
		}
	}
	for ip, found := range expectedIPs {
		if !found {
			t.Errorf("IP %s not found on dedicated NIC in container ns", ip)
		}
	}

	// Move the NIC back to the host namespace for cleanup.
	cleanupDedicatedNIC(nsPath, nicMAC)
}

// TestIntegration_nicExistsInNetns verifies NIC detection by MAC in a namespace.
// This function is used by nsAttachDedicatedNIC for idempotency — if CNI has
// already moved the NIC into the pod ns, NRI skips the attach and returns early.
func TestIntegration_nicExistsInNetns(t *testing.T) {
	skipIfNotRoot(t)

	testNS, nsPath := testNetns(t)

	// Create a dummy NIC on the host (not yet moved into the pod ns).
	name := "test-swift-ne"
	mac := testDummyNIC(t, name)
	actual, err := nlwrap.LinkByName(name)
	if err != nil {
		t.Fatalf("failed to find dummy: %v", err)
	}

	// The NIC is still on the host — nicExistsInNetns should return false
	// because the NIC is not inside the pod namespace.
	if nicExistsInNetns(nsPath, mac) {
		t.Error("nicExistsInNetns should return false when NIC is on host")
	}

	// Move the NIC into the pod namespace. Use GetFromPath (matching the
	// production code pattern) rather than the raw testNS handle, which
	// avoids potential fd-validity issues after goroutine rescheduling.
	destNs, err := netns.GetFromPath(nsPath)
	if err != nil {
		t.Fatalf("failed to open namespace from path: %v", err)
	}
	defer destNs.Close()

	// LinkSetNsFd moves the NIC from the host ns into the pod ns.
	if err := netlink.LinkSetNsFd(actual, int(destNs)); err != nil {
		t.Fatalf("failed to move NIC to test netns: %v", err)
	}

	// Dummy NIC quirk: moving across namespaces may regenerate MAC. Restore
	// the expected MAC inside the pod namespace so MAC-based detection is stable.
	nhFix, err := nlwrap.NewHandleAt(testNS)
	if err != nil {
		t.Fatalf("failed to get handle in test netns: %v", err)
	}
	nsMoved, err := nhFix.LinkByName(name)
	if err != nil {
		nhFix.Close()
		t.Fatalf("NIC not found in test ns after move: %v", err)
	}
	targetMAC, err := net.ParseMAC(mac)
	if err != nil {
		nhFix.Close()
		t.Fatalf("invalid MAC %s: %v", mac, err)
	}
	if err := nhFix.LinkSetHardwareAddr(nsMoved, targetMAC); err != nil {
		nhFix.Close()
		t.Fatalf("failed to restore MAC in test ns: %v", err)
	}
	nhFix.Close()

	// Sanity check: the NIC should no longer be visible on the host.
	if _, lookupErr := nlwrap.LinkByName(name); lookupErr == nil {
		t.Fatal("NIC still on host after LinkSetNsFd — move did not take effect")
	}

	// Now the NIC is in the pod namespace — nicExistsInNetns should find it
	// by iterating all links in the namespace and matching MAC addresses.
	if !nicExistsInNetns(nsPath, mac) {
		t.Error("nicExistsInNetns should return true after NIC moved into pod ns")
	}

	// A fabricated MAC that doesn't match any NIC should return false.
	if nicExistsInNetns(nsPath, "02:00:00:00:ff:ff") {
		t.Error("nicExistsInNetns returned true for wrong MAC")
	}

	// --- Manual cleanup: move NIC back to host namespace ---
	// Open a netlink handle scoped to the pod namespace to find the NIC there.
	nhNs, err := nlwrap.NewHandleAt(testNS)
	if err != nil {
		t.Fatalf("failed to get handle: %v", err)
	}
	defer nhNs.Close()

	// Look up the NIC by name inside the pod namespace.
	nsLink, err := nhNs.LinkByName(name)
	if err != nil {
		t.Fatalf("NIC not found in test ns: %v", err)
	}

	// Get a handle to the host (root) namespace to move the NIC back.
	rootNs, err := netns.Get()
	if err != nil {
		t.Fatalf("failed to get root ns: %v", err)
	}
	defer rootNs.Close()

	// Move the NIC back to the host so t.Cleanup can delete it.
	if err := nhNs.LinkSetNsFd(nsLink, int(rootNs)); err != nil {
		t.Logf("warning: failed to move NIC back to host ns: %v", err)
	}

	// Register cleanup to delete the dummy NIC after the test.
}

// TestIntegration_cleanupDedicatedNIC_NonexistentNetns verifies that cleanup
// handles a deleted namespace gracefully. When a pod's namespace is already gone
// (e.g., kubelet deleted the sandbox), the kernel automatically returns the NIC
// to the host namespace. cleanupDedicatedNIC should detect this and return
// without error or panic.
func TestIntegration_cleanupDedicatedNIC_NonexistentNetns(t *testing.T) {
	skipIfNotRoot(t)

	// Call cleanup with a netns path that doesn't exist — this should be a no-op.
	// The function logs a klog.V(2) message and returns immediately.
	cleanupDedicatedNIC("/run/netns/nonexistent-test-ns", "00:11:22:33:44:55")
}

// TestIntegration_cleanupIPVlanL3_NonexistentParent verifies that cleanup handles
// a missing parent NIC gracefully. This can happen if the parent NIC is detached
// from the VM (Azure host maintenance) before the pod is torn down. The function
// should log a warning and return without panic.
func TestIntegration_cleanupIPVlanL3_NonexistentParent(t *testing.T) {
	skipIfNotRoot(t)

	// Create a config with a MAC that doesn't match any NIC on the host.
	cfg := &NICConfig{
		MAC:   "02:00:00:00:ff:ff",
		PodIP: "10.244.1.99",
	}
	// cleanupIPVlanL3 should handle the missing parent gracefully — it logs
	// a klog.Warning and returns without attempting to delete a route.
	cleanupIPVlanL3(cfg)
}
