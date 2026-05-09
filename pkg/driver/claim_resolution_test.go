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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/dranet/pkg/cnsclient"
)

func TestBuildSwiftV2PodConfigShared(t *testing.T) {
	cfg, err := buildSwiftV2PodConfig(types.UID("pod-uid-1"), cnsclient.PodIPInfo{
		PodIPConfig:                     cnsclient.IPSubnet{IPAddress: "10.0.0.10", PrefixLength: 24},
		NetworkContainerPrimaryIPConfig: cnsclient.IPConfiguration{GatewayIPAddress: "169.254.2.1"},
		MacAddress:                      "aa:bb:cc:dd:ee:01",
		SharedNIC:                       true,
	}, "10.0.0.1")
	if err != nil {
		t.Fatalf("buildSwiftV2PodConfig() failed: %v", err)
	}
	if cfg.Mode != NICModeShared {
		t.Fatalf("expected shared mode, got %s", cfg.Mode)
	}
	if cfg.NIC.PodIP != "10.0.0.10" {
		t.Fatalf("unexpected pod IP %s", cfg.NIC.PodIP)
	}
	if cfg.NIC.SubnetPrefix != 24 {
		t.Fatalf("unexpected prefix %d", cfg.NIC.SubnetPrefix)
	}
	if len(cfg.InterfaceConfig.Interface.Addresses) != 1 || cfg.InterfaceConfig.Interface.Addresses[0] != "10.0.0.10/32" {
		t.Fatalf("unexpected shared addresses %v", cfg.InterfaceConfig.Interface.Addresses)
	}
}

// TestBuildSwiftV2PodConfigSharedHostPrimaryIPCIDR verifies that when CNS
// returns the host primary IP as a CIDR using the subnet address-space
// width, dranet uses the IP as HostPrimaryIP and overrides SubnetPrefix
// with the subnet width (not the narrower NC prefix from PodIPConfig).
func TestBuildSwiftV2PodConfigSharedHostPrimaryIPCIDR(t *testing.T) {
	cfg, err := buildSwiftV2PodConfig(types.UID("pod-uid-1"), cnsclient.PodIPInfo{
		PodIPConfig:                     cnsclient.IPSubnet{IPAddress: "165.0.0.17", PrefixLength: 28},
		NetworkContainerPrimaryIPConfig: cnsclient.IPConfiguration{GatewayIPAddress: "169.254.2.1"},
		MacAddress:                      "7c:1e:52:07:01:ba",
		SharedNIC:                       true,
	}, "165.0.0.16/20")
	if err != nil {
		t.Fatalf("buildSwiftV2PodConfig() failed: %v", err)
	}
	if cfg.NIC.HostPrimaryIP != "165.0.0.16" {
		t.Fatalf("HostPrimaryIP: want 165.0.0.16, got %q", cfg.NIC.HostPrimaryIP)
	}
	if cfg.NIC.SubnetPrefix != 20 {
		t.Fatalf("SubnetPrefix: want 20 (subnet width), got %d", cfg.NIC.SubnetPrefix)
	}
}

func TestBuildSwiftV2PodConfigDedicated(t *testing.T) {
	cfg, err := buildSwiftV2PodConfig(types.UID("pod-uid-1"), cnsclient.PodIPInfo{
		PodIPConfig: cnsclient.IPSubnet{IPAddress: "10.0.0.20", PrefixLength: 24},
		MacAddress:  "aa:bb:cc:dd:ee:02",
	}, "")
	if err != nil {
		t.Fatalf("buildSwiftV2PodConfig() failed: %v", err)
	}
	if cfg.Mode != NICModeDedicated {
		t.Fatalf("expected dedicated mode, got %s", cfg.Mode)
	}
	if len(cfg.NIC.Addresses) != 1 || cfg.NIC.Addresses[0] != "10.0.0.20/24" {
		t.Fatalf("unexpected dedicated addresses %v", cfg.NIC.Addresses)
	}
	if cfg.NIC.GatewayIP != swiftV2VirtualGW {
		t.Fatalf("unexpected gateway IP %s", cfg.NIC.GatewayIP)
	}
}

func TestPopulateSwiftV2StoreForDevice(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req cnsclient.IPConfigsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("failed to decode request: %v", err)
		}

		_ = json.NewEncoder(w).Encode(cnsclient.IPConfigsResponse{
			Response: cnsclient.Response{ReturnCode: 0},
			PodIPInfo: []cnsclient.PodIPInfo{
				{
					PodIPConfig:                     cnsclient.IPSubnet{IPAddress: "10.0.0.10", PrefixLength: 24},
					NetworkContainerPrimaryIPConfig: cnsclient.IPConfiguration{GatewayIPAddress: "169.254.2.1"},
					MacAddress:                      "aa:bb:cc:dd:ee:01",
					SharedNIC:                       true,
				},
			},
		})
	}))
	defer server.Close()

	client, err := cnsclient.New(server.URL, 0)
	if err != nil {
		t.Fatalf("failed to create CNS client: %v", err)
	}

	np := &NetworkDriver{
		cnsClient:    client,
		swiftV2Store: NewSwiftV2PodConfigStore(),
	}

	pod := podConsumer{UID: types.UID("pod-uid-1"), Name: "pod-a", Namespace: "ns-a"}
	cache := map[types.UID][]cnsclient.PodIPInfo{}
	if err := np.populateSwiftV2StoreForDevice(context.Background(), pod, "eth1", "aa:bb:cc:dd:ee:01", "", types.NamespacedName{Namespace: "ns-a", Name: "claim-a"}, "", cache); err != nil {
		t.Fatalf("populateSwiftV2StoreForDevice() failed: %v", err)
	}

	got := np.swiftV2Store.Get(pod.UID)
	if got == nil {
		t.Fatal("expected SwiftV2 store entry")
	}
	if got["eth1"].Mode != NICModeShared {
		t.Fatalf("unexpected mode %s", got["eth1"].Mode)
	}
	if got["eth1"].NIC.MAC != "aa:bb:cc:dd:ee:01" {
		t.Fatalf("unexpected MAC %s", got["eth1"].NIC.MAC)
	}
}
