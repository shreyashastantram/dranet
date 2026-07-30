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

	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/dranet/pkg/cnsclient"
)

func TestIntegration_PrepareResourceClaims_PopulatesSwiftV2Store(t *testing.T) {
	skipIfNotRoot(t)

	testCases := []struct {
		name          string
		podIPInfo     cnsclient.PodIPInfo
		expectedMode  NICMode
		expectedAddrs []string
	}{
		{
			name: "shared",
			podIPInfo: cnsclient.PodIPInfo{
				PodIPConfig:                     cnsclient.IPSubnet{IPAddress: "10.0.0.10", PrefixLength: 24},
				NetworkContainerPrimaryIPConfig: cnsclient.IPConfiguration{GatewayIPAddress: "169.254.2.1"},
				SharedNIC:                       true,
			},
			expectedMode:  NICModeShared,
			expectedAddrs: []string{"10.0.0.10/32"},
		},
		{
			name: "dedicated",
			podIPInfo: cnsclient.PodIPInfo{
				PodIPConfig: cnsclient.IPSubnet{IPAddress: "10.0.0.20", PrefixLength: 24},
			},
			expectedMode:  NICModeDedicated,
			expectedAddrs: []string{"10.0.0.20/24"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ifaceName := "tp" + tc.name + "swift0"
			deviceMAC := testDummyNIC(t, ifaceName)

			requestCount := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requestCount++
				if r.Method != http.MethodPost {
					t.Fatalf("unexpected method %s", r.Method)
				}
				if r.URL.Path != "/network/requestipconfigs" {
					t.Fatalf("unexpected path %s", r.URL.Path)
				}

				var req cnsclient.IPConfigsRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Fatalf("failed to decode request: %v", err)
				}

				var podInfo cnsclient.KubernetesPodInfo
				if err := json.Unmarshal(req.OrchestratorContext, &podInfo); err != nil {
					t.Fatalf("failed to decode orchestrator context: %v", err)
				}
				if podInfo.PodName != "pod-a" {
					t.Fatalf("unexpected pod name %s", podInfo.PodName)
				}
				if podInfo.PodNamespace != "ns-a" {
					t.Fatalf("unexpected pod namespace %s", podInfo.PodNamespace)
				}

				resp := tc.podIPInfo
				resp.MacAddress = deviceMAC
				if err := json.NewEncoder(w).Encode(cnsclient.IPConfigsResponse{
					Response:  cnsclient.Response{ReturnCode: 0},
					PodIPInfo: []cnsclient.PodIPInfo{resp},
				}); err != nil {
					t.Fatalf("failed to encode response: %v", err)
				}
			}))
			defer server.Close()

			client, err := cnsclient.New(server.URL, 0)
			if err != nil {
				t.Fatalf("failed to create CNS client: %v", err)
			}

			fakeNetDB := newFakeInventoryDB()
			fakeNetDB.SetNetInterfaceName("device-1", ifaceName)

			np := &NetworkDriver{
				driverName:     "test.driver",
				netdb:          fakeNetDB,
				cnsClient:      client,
				podConfigStore: NewPodConfigStore(),
				swiftV2Store:   NewSwiftV2PodConfigStore(),
			}

			claim := &resourcev1.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "claim-a",
					Namespace: "ns-a",
					UID:       types.UID("claim-uid-1"),
				},
				Status: resourcev1.ResourceClaimStatus{
					ReservedFor: []resourcev1.ResourceClaimConsumerReference{{
						APIGroup: "",
						Resource: "pods",
						Name:     "pod-a",
						UID:      types.UID("pod-uid-1"),
					}},
					Allocation: &resourcev1.AllocationResult{
						Devices: resourcev1.DeviceAllocationResult{
							Results: []resourcev1.DeviceRequestAllocationResult{{
								Driver:  "test.driver",
								Device:  "device-1",
								Request: "req-1",
							}},
						},
					},
				},
			}

			result, err := np.PrepareResourceClaims(context.Background(), []*resourcev1.ResourceClaim{claim})
			if err != nil {
				t.Fatalf("PrepareResourceClaims() failed: %v", err)
			}
			if result[claim.UID].Err != nil {
				t.Fatalf("PrepareResourceClaims() returned claim error: %v", result[claim.UID].Err)
			}
			if requestCount != 1 {
				t.Fatalf("expected 1 CNS request, got %d", requestCount)
			}

			podCfg, ok := np.podConfigStore.Get(types.UID("pod-uid-1"), "device-1")
			if !ok {
				t.Fatal("expected pod config store entry")
			}
			if podCfg.NetworkInterfaceConfigInHost.Interface.Name != ifaceName {
				t.Fatalf("unexpected host interface name %s", podCfg.NetworkInterfaceConfigInHost.Interface.Name)
			}

			swiftCfgs := np.swiftV2Store.Get(types.UID("pod-uid-1"))
			if swiftCfgs == nil {
				t.Fatal("expected SwiftV2 store entry")
			}
			swiftCfg, ok := swiftCfgs["device-1"]
			if !ok {
				t.Fatal("expected SwiftV2 device config")
			}
			if swiftCfg.Mode != tc.expectedMode {
				t.Fatalf("unexpected mode %s", swiftCfg.Mode)
			}
			if swiftCfg.NIC.MAC != deviceMAC {
				t.Fatalf("unexpected MAC %s", swiftCfg.NIC.MAC)
			}
			if len(swiftCfg.InterfaceConfig.Interface.Addresses) != len(tc.expectedAddrs) {
				t.Fatalf("unexpected addresses %v", swiftCfg.InterfaceConfig.Interface.Addresses)
			}
			for i := range tc.expectedAddrs {
				if swiftCfg.InterfaceConfig.Interface.Addresses[i] != tc.expectedAddrs[i] {
					t.Fatalf("unexpected address at %d: got %s want %s", i, swiftCfg.InterfaceConfig.Interface.Addresses[i], tc.expectedAddrs[i])
				}
			}
		})
	}
}

func TestIntegration_PrepareCNSResourceClaim_FastPath(t *testing.T) {
	testCases := []struct {
		name          string
		podIPInfo     cnsclient.PodIPInfo
		expectedMode  NICMode
		expectedAddrs []string
	}{
		{
			name: "shared fast path",
			podIPInfo: cnsclient.PodIPInfo{
				PodIPConfig:                     cnsclient.IPSubnet{IPAddress: "10.0.0.30", PrefixLength: 24},
				NetworkContainerPrimaryIPConfig: cnsclient.IPConfiguration{GatewayIPAddress: "169.254.2.1"},
				SharedNIC:                       true,
			},
			expectedMode:  NICModeShared,
			expectedAddrs: []string{"10.0.0.30/32"},
		},
		{
			name: "dedicated fast path",
			podIPInfo: cnsclient.PodIPInfo{
				PodIPConfig: cnsclient.IPSubnet{IPAddress: "10.0.0.40", PrefixLength: 24},
			},
			expectedMode:  NICModeDedicated,
			expectedAddrs: []string{"10.0.0.40/24"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// NIC state comes from CNS, not netlink — no root required.
			deviceMAC := "aa:bb:cc:dd:ee:42"
			deviceName := sanitizeMACForK8s(deviceMAC)
			cnsName := "cns-nic-" + tc.name[:3]

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/network/nicresources":
					_ = json.NewEncoder(w).Encode(cnsclient.GetNICResourcesResponse{
						Response: cnsclient.Response{ReturnCode: 0},
						NICResources: []cnsclient.NICResource{{
							Name:       cnsName,
							MacAddress: deviceMAC,
							SubnetID:   "/subscriptions/sub1/subnets/sn1",
						}},
					})
				case "/network/requestipconfigs":
					resp := tc.podIPInfo
					resp.MacAddress = deviceMAC
					_ = json.NewEncoder(w).Encode(cnsclient.IPConfigsResponse{
						Response:  cnsclient.Response{ReturnCode: 0},
						PodIPInfo: []cnsclient.PodIPInfo{resp},
					})
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			client, err := cnsclient.New(server.URL, 0)
			if err != nil {
				t.Fatalf("failed to create CNS client: %v", err)
			}

			np := &NetworkDriver{
				driverName:     "dra.net",
				cnsDriverName:  "networking.azure.com",
				netdb:          newFakeInventoryDB(),
				cnsClient:      client,
				podConfigStore: NewPodConfigStore(),
				swiftV2Store:   NewSwiftV2PodConfigStore(),
			}

			claim := &resourcev1.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cns-claim",
					Namespace: "ns-a",
					UID:       types.UID("cns-claim-uid"),
				},
				Status: resourcev1.ResourceClaimStatus{
					ReservedFor: []resourcev1.ResourceClaimConsumerReference{{
						APIGroup: "",
						Resource: "pods",
						Name:     "pod-a",
						UID:      types.UID("pod-uid-1"),
					}},
					Allocation: &resourcev1.AllocationResult{
						Devices: resourcev1.DeviceAllocationResult{
							Results: []resourcev1.DeviceRequestAllocationResult{{
								Driver:  "networking.azure.com",
								Device:  deviceName,
								Request: "nic",
							}},
						},
					},
				},
			}

			result, err := np.PrepareResourceClaims(context.Background(), []*resourcev1.ResourceClaim{claim})
			if err != nil {
				t.Fatalf("PrepareResourceClaims() failed: %v", err)
			}
			if result[claim.UID].Err != nil {
				t.Fatalf("PrepareResourceClaims() returned claim error: %v", result[claim.UID].Err)
			}

			// Fast path should NOT populate podConfigStore
			if _, ok := np.podConfigStore.GetPodConfigs(types.UID("pod-uid-1")); ok {
				t.Fatal("podConfigStore should be empty for CNS fast path")
			}

			// Fast path SHOULD populate swiftV2Store
			swiftCfgs := np.swiftV2Store.Get(types.UID("pod-uid-1"))
			if swiftCfgs == nil {
				t.Fatal("expected SwiftV2 store entry")
			}
			swiftCfg, ok := swiftCfgs[deviceName]
			if !ok {
				t.Fatalf("expected SwiftV2 device config for %s", deviceName)
			}
			if swiftCfg.Mode != tc.expectedMode {
				t.Fatalf("expected mode %s, got %s", tc.expectedMode, swiftCfg.Mode)
			}
			if swiftCfg.NIC.MAC != deviceMAC {
				t.Fatalf("expected MAC %s, got %s", deviceMAC, swiftCfg.NIC.MAC)
			}
			if len(swiftCfg.InterfaceConfig.Interface.Addresses) != len(tc.expectedAddrs) {
				t.Fatalf("unexpected addresses %v", swiftCfg.InterfaceConfig.Interface.Addresses)
			}
			for i := range tc.expectedAddrs {
				if swiftCfg.InterfaceConfig.Interface.Addresses[i] != tc.expectedAddrs[i] {
					t.Fatalf("address[%d]: got %s, want %s", i, swiftCfg.InterfaceConfig.Interface.Addresses[i], tc.expectedAddrs[i])
				}
			}
		})
	}
}
