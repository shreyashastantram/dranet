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
			name: "exclusive fast path",
			podIPInfo: cnsclient.PodIPInfo{
				PodIPConfig: cnsclient.IPSubnet{IPAddress: "10.0.0.40", PrefixLength: 24},
			},
			expectedMode:  NICModeExclusive,
			expectedAddrs: []string{"10.0.0.40/24"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// NIC state comes from CNS, not netlink — no root required.
			deviceMAC := "aa:bb:cc:dd:ee:42"
			deviceName := sanitizeMACForK8s(deviceMAC)

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/network/nicresources":
					_ = json.NewEncoder(w).Encode(cnsclient.GetNICResourcesResponse{
						Response: cnsclient.Response{ReturnCode: 0},
						NICResources: []cnsclient.NICResource{{
							MacAddress: deviceMAC,
							SubnetName: "sn1",
						}},
					})
				case "/network/requestclaimresourceinfo":
					resp := tc.podIPInfo
					resp.MacAddress = deviceMAC
					_ = json.NewEncoder(w).Encode(cnsclient.ClaimResourceInfoResponse{
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
				driverName:        "dra.net",
				cnsDriverName:     "networking.azure.com",
				netdb:             newFakeInventoryDB(),
				cnsClient:         client,
				podConfigStore:    mustNewPodConfigStore(),
				secondaryNICStore: NewSecondaryNICPodConfigStore(),
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
			if _, ok := np.podConfigStore.GetPodConfig(types.UID("pod-uid-1")); ok {
				t.Fatal("podConfigStore should be empty for CNS fast path")
			}

			// Fast path SHOULD populate secondaryNICStore
			secondaryNICConfigs := np.secondaryNICStore.Get(types.UID("pod-uid-1"))
			if secondaryNICConfigs == nil {
				t.Fatal("expected secondary NIC store entry")
			}
			secondaryNICConfig, ok := secondaryNICConfigs[deviceName]
			if !ok {
				t.Fatalf("expected secondary NIC device config for %s", deviceName)
			}
			if secondaryNICConfig.Mode != tc.expectedMode {
				t.Fatalf("expected mode %s, got %s", tc.expectedMode, secondaryNICConfig.Mode)
			}
			if secondaryNICConfig.NIC.MAC != deviceMAC {
				t.Fatalf("expected MAC %s, got %s", deviceMAC, secondaryNICConfig.NIC.MAC)
			}
			if len(secondaryNICConfig.InterfaceConfig.Interface.Addresses) != len(tc.expectedAddrs) {
				t.Fatalf("unexpected addresses %v", secondaryNICConfig.InterfaceConfig.Interface.Addresses)
			}
			for i := range tc.expectedAddrs {
				if secondaryNICConfig.InterfaceConfig.Interface.Addresses[i] != tc.expectedAddrs[i] {
					t.Fatalf("address[%d]: got %s, want %s", i, secondaryNICConfig.InterfaceConfig.Interface.Addresses[i], tc.expectedAddrs[i])
				}
			}
		})
	}
}
