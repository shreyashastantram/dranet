package driver

import (
	"context"
	"errors"
	"strings"
	"testing"

	nriapi "github.com/containerd/nri/pkg/api"
	"github.com/prometheus/client_golang/prometheus/testutil"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
)

func testSecondaryNICPod(ns string) *nriapi.PodSandbox {
	return &nriapi.PodSandbox{
		Name:      "pod-a",
		Namespace: "ns-a",
		Linux: &nriapi.LinuxPodSandbox{
			Namespaces: []*nriapi.LinuxNamespace{{
				Type: "network",
				Path: ns,
			}},
		},
	}
}

func TestRunPodSandboxSecondaryNIC_SharedHappyPath(t *testing.T) {
	oldAttach := nsAttachSecondaryNICHook
	defer func() { nsAttachSecondaryNICHook = oldAttach }()

	called := 0
	nsAttachSecondaryNICHook = func(mode NICMode, cfg *NICConfig, containerNsPath string) (*resourceapi.NetworkDeviceData, error) {
		called++
		if mode != NICModeShared {
			t.Fatalf("unexpected mode: %s", mode)
		}
		if cfg.MAC != "aa:bb:cc:dd:ee:01" {
			t.Fatalf("unexpected MAC: %s", cfg.MAC)
		}
		if containerNsPath != "/run/netns/pod-a" {
			t.Fatalf("unexpected ns path: %s", containerNsPath)
		}
		return &resourceapi.NetworkDeviceData{InterfaceName: sharedNICInterfaceName, IPs: []string{"10.1.0.10/32"}}, nil
	}

	np := &NetworkDriver{}
	cfgs := map[string]SecondaryNICPodConfig{
		"eth1": {
			Mode: NICModeShared,
			NIC:  NICConfig{MAC: "aa:bb:cc:dd:ee:01", PodIP: "10.1.0.10", GatewayIP: secondaryNICVirtualGateway, PodUID: "pod-uid"},
		},
	}

	if err := np.runPodSandboxSecondaryNICs(context.Background(), testSecondaryNICPod("/run/netns/pod-a"), cfgs); err != nil {
		t.Fatalf("runPodSandboxSecondaryNICs failed: %v", err)
	}
	if called != 1 {
		t.Fatalf("shared attach called %d times, want 1", called)
	}
}

func TestRunPodSandboxSecondaryNIC_ExclusiveHappyPath(t *testing.T) {
	oldAttach := nsAttachSecondaryNICHook
	defer func() {
		nsAttachSecondaryNICHook = oldAttach
	}()

	called := 0
	nsAttachSecondaryNICHook = func(mode NICMode, cfg *NICConfig, containerNsPath string) (*resourceapi.NetworkDeviceData, error) {
		called++
		if mode != NICModeExclusive {
			t.Fatalf("unexpected mode: %s", mode)
		}
		if containerNsPath != "/run/netns/pod-b" {
			t.Fatalf("unexpected ns path: %s", containerNsPath)
		}
		if len(cfg.Addresses) != 1 {
			t.Fatalf("unexpected addresses: %v", cfg.Addresses)
		}
		return &resourceapi.NetworkDeviceData{InterfaceName: "ethX", IPs: cfg.Addresses}, nil
	}

	np := &NetworkDriver{}
	cfgs := map[string]SecondaryNICPodConfig{
		"eth2": {
			Mode: NICModeExclusive,
			NIC: NICConfig{
				MAC:       "aa:bb:cc:dd:ee:02",
				GatewayIP: secondaryNICVirtualGateway,
				Addresses: []string{"10.2.0.10/24"},
			},
		},
	}

	if err := np.runPodSandboxSecondaryNICs(context.Background(), testSecondaryNICPod("/run/netns/pod-b"), cfgs); err != nil {
		t.Fatalf("runPodSandboxSecondaryNICs failed: %v", err)
	}
	if called != 1 {
		t.Fatalf("exclusive attach called %d times, want 1", called)
	}
}

func TestRunPodSandboxSecondaryNIC_ExclusiveRejectsSharedPodOnSameNIC(t *testing.T) {
	oldAttach := nsAttachSecondaryNICHook
	defer func() { nsAttachSecondaryNICHook = oldAttach }()

	attachCalls := 0
	nsAttachSecondaryNICHook = func(NICMode, *NICConfig, string) (*resourceapi.NetworkDeviceData, error) {
		attachCalls++
		return &resourceapi.NetworkDeviceData{}, nil
	}

	store := NewSecondaryNICPodConfigStore()
	store.Set(types.UID("shared-pod"), "shared-device", SecondaryNICPodConfig{
		Mode: NICModeShared,
		NIC:  NICConfig{MAC: "AA:BB:CC:DD:EE:01"},
	}, types.NamespacedName{Namespace: "ns-a", Name: "shared-claim"})

	np := &NetworkDriver{secondaryNICStore: store}
	configs := map[string]SecondaryNICPodConfig{
		"exclusive-device": {
			Mode: NICModeExclusive,
			NIC:  NICConfig{MAC: "aa-bb-cc-dd-ee-01"},
		},
	}

	err := np.runPodSandboxSecondaryNICs(context.Background(), testSecondaryNICPod("/run/netns/exclusive-pod"), configs)
	if err == nil || !strings.Contains(err.Error(), "a shared pod still uses this NIC") {
		t.Fatalf("error = %v, want shared-pod guard rejection", err)
	}
	if attachCalls != 0 {
		t.Fatalf("attach called %d times, want 0", attachCalls)
	}
}

func TestRunPodSandboxSecondaryNIC_SharedRejectsExclusivePodOnSameNIC(t *testing.T) {
	store := NewSecondaryNICPodConfigStore()
	claim := types.NamespacedName{Namespace: "ns", Name: "claim"}
	if err := store.Set("exclusive-pod", "device-a", SecondaryNICPodConfig{
		Mode: NICModeExclusive,
		NIC:  NICConfig{MAC: "aa:bb:cc:dd:ee:01"},
	}, claim); err != nil {
		t.Fatal(err)
	}

	np := &NetworkDriver{secondaryNICStore: store}
	pod := testSecondaryNICPod("/run/netns/shared-pod")
	err := np.runPodSandboxSecondaryNICs(context.Background(), pod, map[string]SecondaryNICPodConfig{
		"device-b": {
			Mode: NICModeShared,
			NIC:  NICConfig{MAC: "AA-BB-CC-DD-EE-01"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "an exclusive pod still uses this NIC") {
		t.Fatalf("runPodSandboxSecondaryNICs() error = %v, want exclusive-use rejection", err)
	}
}

func TestRunPodSandboxSecondaryNIC_RejectsEmptyMAC(t *testing.T) {
	tests := []struct {
		name string
		mode NICMode
	}{
		{name: "shared", mode: NICModeShared},
		{name: "exclusive", mode: NICModeExclusive},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configs := map[string]SecondaryNICPodConfig{
				"device-a": {Mode: test.mode},
			}
			err := (&NetworkDriver{}).runPodSandboxSecondaryNICs(context.Background(), testSecondaryNICPod("/run/netns/pod-a"), configs)
			if err == nil || !strings.Contains(err.Error(), "empty MAC") {
				t.Fatalf("error = %v, want empty MAC error", err)
			}
		})
	}
}

func TestRunPodSandboxSecondaryNIC_RejectsHostNetwork(t *testing.T) {
	err := (&NetworkDriver{}).runPodSandboxSecondaryNICs(context.Background(), testSecondaryNICPod(""), map[string]SecondaryNICPodConfig{})
	if err == nil || !strings.Contains(err.Error(), "host network") {
		t.Fatalf("error = %v, want host network error", err)
	}
}

func TestRunPodSandboxSecondaryNIC_RejectsMultipleDevicesBeforeAttach(t *testing.T) {
	oldAttach := nsAttachSecondaryNICHook
	defer func() { nsAttachSecondaryNICHook = oldAttach }()

	attachCalls := 0
	nsAttachSecondaryNICHook = func(NICMode, *NICConfig, string) (*resourceapi.NetworkDeviceData, error) {
		attachCalls++
		return &resourceapi.NetworkDeviceData{}, nil
	}
	configs := map[string]SecondaryNICPodConfig{
		"device-a": {Mode: NICModeShared, NIC: NICConfig{MAC: "aa:bb:cc:dd:ee:01"}},
		"device-b": {Mode: NICModeExclusive, NIC: NICConfig{MAC: "aa:bb:cc:dd:ee:02"}},
	}

	err := (&NetworkDriver{}).runPodSandboxSecondaryNICs(context.Background(), testSecondaryNICPod("/run/netns/pod-a"), configs)
	if err == nil || !strings.Contains(err.Error(), "has 2 secondary NIC devices; only one is supported") {
		t.Fatalf("error = %v, want multiple-device rejection", err)
	}
	if attachCalls != 0 {
		t.Fatalf("attach called %d times, want 0", attachCalls)
	}
}

func TestRunPodSandboxSecondaryNIC_PropagatesAttachError(t *testing.T) {
	oldAttach := nsAttachSecondaryNICHook
	defer func() { nsAttachSecondaryNICHook = oldAttach }()

	attachErr := errors.New("attach failed")
	nsAttachSecondaryNICHook = func(NICMode, *NICConfig, string) (*resourceapi.NetworkDeviceData, error) {
		return nil, attachErr
	}

	for _, mode := range []NICMode{NICModeShared, NICModeExclusive} {
		t.Run(string(mode), func(t *testing.T) {
			configs := map[string]SecondaryNICPodConfig{
				"device-a": {Mode: mode, NIC: NICConfig{MAC: "aa:bb:cc:dd:ee:01"}},
			}
			err := (&NetworkDriver{}).runPodSandboxSecondaryNICs(context.Background(), testSecondaryNICPod("/run/netns/pod-a"), configs)
			if !errors.Is(err, attachErr) {
				t.Fatalf("error = %v, want wrapped attach error", err)
			}
		})
	}
}

func TestRunPodSandboxSecondaryNIC_RejectsUnsupportedMode(t *testing.T) {
	configs := map[string]SecondaryNICPodConfig{
		"device-a": {Mode: NICMode("unexpected")},
	}
	err := (&NetworkDriver{}).runPodSandboxSecondaryNICs(context.Background(), testSecondaryNICPod("/run/netns/pod-a"), configs)
	if err == nil || !strings.Contains(err.Error(), "unsupported NIC mode") {
		t.Fatalf("error = %v, want unsupported mode error", err)
	}
}

func TestStopPodSandboxSecondaryNIC_ExclusiveCleanup(t *testing.T) {
	oldCleanup := cleanupExclusiveNICHook
	defer func() { cleanupExclusiveNICHook = oldCleanup }()

	var gotNamespace, gotMAC string
	cleanupExclusiveNICHook = func(namespace, mac string) {
		gotNamespace = namespace
		gotMAC = mac
	}

	podUID := types.UID("pod-uid-a")
	store := mustNewPodConfigStore()
	if err := store.SetDeviceConfig(podUID, "dra-device", DeviceConfig{}); err != nil {
		t.Fatalf("SetDeviceConfig failed: %v", err)
	}
	store.SetPodNetNs(podUID, "/run/netns/from-cache")
	np := &NetworkDriver{podConfigStore: store}
	configs := map[string]SecondaryNICPodConfig{
		"device-a": {Mode: NICModeExclusive, NIC: NICConfig{MAC: "aa:bb:cc:dd:ee:01"}},
	}
	pod := testSecondaryNICPod("")
	pod.Uid = string(podUID)
	np.stopPodSandboxSecondaryNICs(pod, configs)

	if gotNamespace != "/run/netns/from-cache" || gotMAC != "aa:bb:cc:dd:ee:01" {
		t.Fatalf("cleanup called with namespace=%q MAC=%q", gotNamespace, gotMAC)
	}
}

func TestStopPodSandboxSecondaryNIC_SkipsUnusableExclusiveConfig(t *testing.T) {
	oldCleanup := cleanupExclusiveNICHook
	defer func() { cleanupExclusiveNICHook = oldCleanup }()

	cleanupCalls := 0
	cleanupExclusiveNICHook = func(string, string) { cleanupCalls++ }
	np := &NetworkDriver{netdb: newFakeInventoryDB()}

	tests := []struct {
		name string
		pod  *nriapi.PodSandbox
		mac  string
		mode NICMode
	}{
		{name: "empty MAC", pod: testSecondaryNICPod("/run/netns/pod-a"), mode: NICModeExclusive},
		{name: "missing namespace", pod: testSecondaryNICPod(""), mac: "aa:bb:cc:dd:ee:01", mode: NICModeExclusive},
		{name: "unsupported mode", pod: testSecondaryNICPod("/run/netns/pod-a"), mode: NICMode("unexpected")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configs := map[string]SecondaryNICPodConfig{
				"device-a": {Mode: test.mode, NIC: NICConfig{MAC: test.mac}},
			}
			np.stopPodSandboxSecondaryNICs(test.pod, configs)
		})
	}
	if cleanupCalls != 0 {
		t.Fatalf("cleanup called %d times, want 0", cleanupCalls)
	}
}

func TestRunPodSandbox_DispatchesSecondaryNIC(t *testing.T) {
	nriPluginRequestsTotal.Reset()
	nriPluginRequestsLatencySeconds.Reset()

	oldAttach := nsAttachSecondaryNICHook
	defer func() { nsAttachSecondaryNICHook = oldAttach }()
	called := 0
	nsAttachSecondaryNICHook = func(mode NICMode, cfg *NICConfig, containerNsPath string) (*resourceapi.NetworkDeviceData, error) {
		called++
		return &resourceapi.NetworkDeviceData{InterfaceName: sharedNICInterfaceName, IPs: []string{"10.1.0.10/32"}}, nil
	}

	np := &NetworkDriver{
		podConfigStore:    mustNewPodConfigStore(),
		secondaryNICStore: NewSecondaryNICPodConfigStore(),
	}
	podUID := types.UID("pod-uid-sv2")
	np.secondaryNICStore.Set(podUID, "eth1", SecondaryNICPodConfig{
		Mode: NICModeShared,
		NIC:  NICConfig{MAC: "aa:bb:cc:dd:ee:01", PodIP: "10.1.0.10", GatewayIP: secondaryNICVirtualGateway, PodUID: string(podUID)},
	}, types.NamespacedName{Namespace: "ns-a", Name: "claim-a"})

	pod := testSecondaryNICPod("/run/netns/pod-a")
	pod.Uid = string(podUID)

	if err := np.RunPodSandbox(context.Background(), pod); err != nil {
		t.Fatalf("RunPodSandbox failed: %v", err)
	}
	if called != 1 {
		t.Fatalf("exclusive NIC attach dispatched %d times, want 1", called)
	}
}

func TestRunPodSandbox_SecondaryNICAttachFailure(t *testing.T) {
	nriPluginRequestsTotal.Reset()
	nriPluginRequestsLatencySeconds.Reset()

	oldAttach := nsAttachSecondaryNICHook
	defer func() { nsAttachSecondaryNICHook = oldAttach }()
	attachErr := errors.New("attach failed")
	nsAttachSecondaryNICHook = func(NICMode, *NICConfig, string) (*resourceapi.NetworkDeviceData, error) {
		return nil, attachErr
	}

	podUID := types.UID("pod-uid-attach-failure")
	np := &NetworkDriver{
		podConfigStore:    mustNewPodConfigStore(),
		secondaryNICStore: NewSecondaryNICPodConfigStore(),
	}
	np.secondaryNICStore.Set(podUID, "eth1", SecondaryNICPodConfig{
		Mode: NICModeShared,
		NIC:  NICConfig{MAC: "aa:bb:cc:dd:ee:01"},
	}, types.NamespacedName{Namespace: "ns-a", Name: "claim-a"})
	pod := testSecondaryNICPod("/run/netns/pod-a")
	pod.Uid = string(podUID)

	err := np.RunPodSandbox(context.Background(), pod)
	if !errors.Is(err, attachErr) {
		t.Fatalf("RunPodSandbox error = %v, want wrapped attach error", err)
	}
	if got := testutil.ToFloat64(nriPluginRequestsTotal.WithLabelValues(methodRunPodSandbox, statusFailed)); got != 1 {
		t.Fatalf("failed metric = %v, want 1", got)
	}
	if got := testutil.ToFloat64(nriPluginRequestsTotal.WithLabelValues(methodRunPodSandbox, statusSuccess)); got != 0 {
		t.Fatalf("success metric = %v, want 0", got)
	}
}

func TestRunPodSandbox_DispatchesDRAAndSecondaryNIC(t *testing.T) {
	oldAttach := nsAttachSecondaryNICHook
	defer func() { nsAttachSecondaryNICHook = oldAttach }()
	attachCalls := 0
	nsAttachSecondaryNICHook = func(NICMode, *NICConfig, string) (*resourceapi.NetworkDeviceData, error) {
		attachCalls++
		return &resourceapi.NetworkDeviceData{InterfaceName: sharedNICInterfaceName}, nil
	}

	podUID := types.UID("pod-uid-both")
	netdb := newFakeInventoryDB()
	np := &NetworkDriver{
		podConfigStore:    mustNewPodConfigStore(),
		secondaryNICStore: NewSecondaryNICPodConfigStore(),
		netdb:             netdb,
	}
	// An empty-but-present map exercises the DRA dispatch without requiring a
	// real host device; runPodSandbox still records the pod namespace.
	np.podConfigStore.configs[podUID] = PodConfig{DeviceConfigs: map[string]DeviceConfig{}}
	np.secondaryNICStore.Set(podUID, "eth1", SecondaryNICPodConfig{
		Mode: NICModeShared,
		NIC:  NICConfig{MAC: "aa:bb:cc:dd:ee:01"},
	}, types.NamespacedName{Namespace: "ns-a", Name: "claim-a"})
	pod := testSecondaryNICPod("/run/netns/pod-both")
	pod.Uid = string(podUID)

	if err := np.RunPodSandbox(context.Background(), pod); err != nil {
		t.Fatalf("RunPodSandbox failed: %v", err)
	}
	podConfig, ok := np.podConfigStore.GetPodConfig(podUID)
	if !ok || podConfig.NetNS != "/run/netns/pod-both" {
		t.Fatalf("DRA path stored namespace %q, want /run/netns/pod-both", podConfig.NetNS)
	}
	if attachCalls != 1 {
		t.Fatalf("exclusive NIC attach called %d times, want 1", attachCalls)
	}
}

func TestStopPodSandbox_DispatchesSecondaryNICCleanup(t *testing.T) {
	nriPluginRequestsTotal.Reset()
	nriPluginRequestsLatencySeconds.Reset()

	np := &NetworkDriver{
		podConfigStore:    mustNewPodConfigStore(),
		secondaryNICStore: NewSecondaryNICPodConfigStore(),
	}
	podUID := types.UID("pod-uid-sv2-stop")
	np.secondaryNICStore.Set(podUID, "eth1", SecondaryNICPodConfig{
		Mode: NICModeShared,
		NIC:  NICConfig{MAC: "aa:bb:cc:dd:ee:02", PodIP: "10.1.0.11"},
	}, types.NamespacedName{Namespace: "ns-a", Name: "claim-b"})

	pod := testSecondaryNICPod("/run/netns/pod-b")
	pod.Uid = string(podUID)

	if err := np.StopPodSandbox(context.Background(), pod); err != nil {
		t.Fatalf("StopPodSandbox returned error: %v", err)
	}
	// Entry is intentionally retained here; removed on claim unprepare.
	if _, found := np.secondaryNICStore.Get(podUID); !found {
		t.Fatal("exclusive NIC store entry should be retained after StopPodSandbox")
	}
}

func TestStopPodSandboxSecondaryNIC_PrefersCallbackNamespace(t *testing.T) {
	oldCleanup := cleanupExclusiveNICHook
	defer func() { cleanupExclusiveNICHook = oldCleanup }()

	var gotNamespace string
	cleanupExclusiveNICHook = func(namespace, _ string) {
		gotNamespace = namespace
	}
	np := &NetworkDriver{podConfigStore: mustNewPodConfigStore()}
	configs := map[string]SecondaryNICPodConfig{
		"device-a": {Mode: NICModeExclusive, NIC: NICConfig{MAC: "aa:bb:cc:dd:ee:01"}},
	}

	np.stopPodSandboxSecondaryNICs(testSecondaryNICPod("/run/netns/from-callback"), configs)
	if gotNamespace != "/run/netns/from-callback" {
		t.Fatalf("cleanup namespace = %q, want callback namespace", gotNamespace)
	}
}
