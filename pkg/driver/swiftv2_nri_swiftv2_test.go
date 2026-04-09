package driver

import (
	"fmt"
	"testing"

	nriapi "github.com/containerd/nri/pkg/api"
	resourceapi "k8s.io/api/resource/v1"
)

func testSwiftV2Pod(ns string) *nriapi.PodSandbox {
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

func TestRunPodSandboxSwiftV2_SharedHappyPath(t *testing.T) {
	oldAttach := nsAttachSwiftV2NICHook
	defer func() { nsAttachSwiftV2NICHook = oldAttach }()

	called := 0
	nsAttachSwiftV2NICHook = func(mode NICMode, cfg *NICConfig, containerNsPath string) (*resourceapi.NetworkDeviceData, error) {
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
		return &resourceapi.NetworkDeviceData{InterfaceName: swiftV2DelegatedIfName, IPs: []string{"10.1.0.10/32"}}, nil
	}

	np := &NetworkDriver{}
	cfgs := map[string]SwiftV2PodConfig{
		"eth1": {
			Mode: NICModeShared,
			NIC:  NICConfig{MAC: "aa:bb:cc:dd:ee:01", PodIP: "10.1.0.10", GatewayIP: swiftV2VirtualGW, PodUID: "pod-uid"},
		},
	}

	if err := np.runPodSandboxSwiftV2(testSwiftV2Pod("/run/netns/pod-a"), cfgs); err != nil {
		t.Fatalf("runPodSandboxSwiftV2 failed: %v", err)
	}
	if called != 1 {
		t.Fatalf("shared attach called %d times, want 1", called)
	}
}

func TestRunPodSandboxSwiftV2_DedicatedHappyPath(t *testing.T) {
	oldAttach := nsAttachSwiftV2NICHook
	oldExists := nicExistsInNetnsHook
	defer func() {
		nsAttachSwiftV2NICHook = oldAttach
		nicExistsInNetnsHook = oldExists
	}()

	nicExistsInNetnsHook = func(string, string) bool { return false }
	called := 0
	nsAttachSwiftV2NICHook = func(mode NICMode, cfg *NICConfig, containerNsPath string) (*resourceapi.NetworkDeviceData, error) {
		called++
		if mode != NICModeDedicated {
			t.Fatalf("unexpected mode: %s", mode)
		}
		if containerNsPath != "/run/netns/pod-b" {
			t.Fatalf("unexpected ns path: %s", containerNsPath)
		}
		if len(cfg.Addresses) != 2 {
			t.Fatalf("unexpected addresses: %v", cfg.Addresses)
		}
		return &resourceapi.NetworkDeviceData{InterfaceName: "ethX", IPs: cfg.Addresses}, nil
	}

	np := &NetworkDriver{}
	cfgs := map[string]SwiftV2PodConfig{
		"eth2": {
			Mode: NICModeDedicated,
			NIC: NICConfig{
				MAC:       "aa:bb:cc:dd:ee:02",
				GatewayIP: swiftV2VirtualGW,
				Addresses: []string{"10.2.0.10/24", "10.2.0.11/24"},
			},
		},
	}

	if err := np.runPodSandboxSwiftV2(testSwiftV2Pod("/run/netns/pod-b"), cfgs); err != nil {
		t.Fatalf("runPodSandboxSwiftV2 failed: %v", err)
	}
	if called != 1 {
		t.Fatalf("dedicated attach called %d times, want 1", called)
	}
}

func TestRunPodSandboxSwiftV2_DedicatedAlreadyMovedSkipsAttach(t *testing.T) {
	oldAttach := nsAttachSwiftV2NICHook
	oldExists := nicExistsInNetnsHook
	defer func() {
		nsAttachSwiftV2NICHook = oldAttach
		nicExistsInNetnsHook = oldExists
	}()

	nicExistsInNetnsHook = func(containerNsPath string, mac string) bool {
		return containerNsPath == "/run/netns/pod-c" && mac == "aa:bb:cc:dd:ee:03"
	}
	nsAttachSwiftV2NICHook = func(mode NICMode, cfg *NICConfig, containerNsPath string) (*resourceapi.NetworkDeviceData, error) {
		return nil, fmt.Errorf("should not be called")
	}

	np := &NetworkDriver{}
	cfgs := map[string]SwiftV2PodConfig{
		"eth2": {
			Mode: NICModeDedicated,
			NIC:  NICConfig{MAC: "aa:bb:cc:dd:ee:03", GatewayIP: swiftV2VirtualGW, Addresses: []string{"10.3.0.10/24"}},
		},
	}

	if err := np.runPodSandboxSwiftV2(testSwiftV2Pod("/run/netns/pod-c"), cfgs); err != nil {
		t.Fatalf("runPodSandboxSwiftV2 failed: %v", err)
	}
}
