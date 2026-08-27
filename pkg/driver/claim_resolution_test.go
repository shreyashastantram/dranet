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
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/dranet/pkg/cnsclient"
)

func TestBuildSecondaryNICPodConfigShared(t *testing.T) {
	cfg, err := buildSecondaryNICPodConfig(types.UID("pod-uid-1"), cnsclient.PodIPInfo{
		PodIPConfig:                     cnsclient.IPSubnet{IPAddress: "10.0.0.10", PrefixLength: 24},
		NetworkContainerPrimaryIPConfig: cnsclient.IPConfiguration{GatewayIPAddress: "169.254.2.1"},
		MacAddress:                      "aa:bb:cc:dd:ee:01",
		SharedNIC:                       true,
	})
	if err != nil {
		t.Fatalf("buildSecondaryNICPodConfig() failed: %v", err)
	}
	if cfg.Mode != NICModeShared {
		t.Fatalf("expected shared mode, got %s", cfg.Mode)
	}
	if cfg.NIC.PodIP != "10.0.0.10" {
		t.Fatalf("unexpected pod IP %s", cfg.NIC.PodIP)
	}
	if len(cfg.InterfaceConfig.Interface.Addresses) != 1 || cfg.InterfaceConfig.Interface.Addresses[0] != "10.0.0.10/32" {
		t.Fatalf("unexpected shared addresses %v", cfg.InterfaceConfig.Interface.Addresses)
	}
}

func TestBuildSecondaryNICPodConfigExclusive(t *testing.T) {
	cfg, err := buildSecondaryNICPodConfig(types.UID("pod-uid-1"), cnsclient.PodIPInfo{
		PodIPConfig: cnsclient.IPSubnet{IPAddress: "10.0.0.20", PrefixLength: 24},
		MacAddress:  "aa:bb:cc:dd:ee:02",
	})
	if err != nil {
		t.Fatalf("buildSecondaryNICPodConfig() failed: %v", err)
	}
	if cfg.Mode != NICModeExclusive {
		t.Fatalf("expected exclusive mode, got %s", cfg.Mode)
	}
	if len(cfg.NIC.Addresses) != 1 || cfg.NIC.Addresses[0] != "10.0.0.20/24" {
		t.Fatalf("unexpected exclusive addresses %v", cfg.NIC.Addresses)
	}
	if cfg.NIC.GatewayIP != secondaryNICVirtualGateway {
		t.Fatalf("unexpected gateway IP %s", cfg.NIC.GatewayIP)
	}
}

func TestPopulateSecondaryNICStoreForDevice(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req cnsclient.ClaimResourceInfoRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("failed to decode request: %v", err)
		}

		_ = json.NewEncoder(w).Encode(cnsclient.ClaimResourceInfoResponse{
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
		cnsClient:         client,
		secondaryNICStore: NewSecondaryNICPodConfigStore(),
	}

	pod := podConsumer{UID: types.UID("pod-uid-1"), Name: "pod-a", Namespace: "ns-a"}
	deviceName := sanitizeMACForK8s("aa:bb:cc:dd:ee:01")
	cache := map[types.UID]claimGoalState{}
	if err := np.populateSecondaryNICStoreForDevice(context.Background(), types.UID("claim-uid-a"), pod, deviceName, types.NamespacedName{Namespace: "ns-a", Name: "claim-a"}, "", cache); err != nil {
		t.Fatalf("populateSecondaryNICStoreForDevice() failed: %v", err)
	}

	got := np.secondaryNICStore.Get(pod.UID)
	if got == nil {
		t.Fatal("expected secondary NIC store entry")
	}
	if got[deviceName].Mode != NICModeShared {
		t.Fatalf("unexpected mode %s", got[deviceName].Mode)
	}
	if got[deviceName].NIC.MAC != "aa:bb:cc:dd:ee:01" {
		t.Fatalf("unexpected MAC %s", got[deviceName].NIC.MAC)
	}

	writeErr := errors.New("checkpoint write failed")
	failingStore, err := newSecondaryNICPodConfigStore(&failingSecondaryNICCheckpointer{storeErr: writeErr})
	if err != nil {
		t.Fatalf("newSecondaryNICPodConfigStore() failed: %v", err)
	}
	np.secondaryNICStore = failingStore
	err = np.populateSecondaryNICStoreForDevice(context.Background(), types.UID("claim-uid-a"), pod, deviceName, types.NamespacedName{Namespace: "ns-a", Name: "claim-a"}, "", cache)
	if !errors.Is(err, writeErr) {
		t.Fatalf("populate error = %v, want checkpoint error %v", err, writeErr)
	}
	if np.secondaryNICStore.Get(pod.UID) != nil {
		t.Fatal("failed persistent write populated secondary NIC memory store")
	}
}
