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
	"testing"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/dranet/pkg/apis"
)

func TestSecondaryNICPodConfigStore_SetAndGet(t *testing.T) {
	store := NewSecondaryNICPodConfigStore()

	podUID := types.UID("test-pod-uid-1234")
	deviceName := "eth1"
	cfg := SecondaryNICPodConfig{
		Mode: NICModeShared,
		NIC: NICConfig{
			MAC:       "00:0d:3a:12:34:56",
			GatewayIP: "169.254.2.1",
			PodIP:     "10.0.1.10",
			PodUID:    "test-pod",
		},
		InterfaceConfig: apis.NetworkConfig{
			Interface: apis.InterfaceConfig{
				Addresses: []string{"10.0.1.10/32"},
			},
			Routes: []apis.RouteConfig{
				{Destination: "169.254.2.1/32", Scope: 253},
				{Destination: "0.0.0.0/0", Gateway: "169.254.2.1"},
			},
		},
	}

	store.Set(podUID, deviceName, cfg, types.NamespacedName{Namespace: "ns", Name: "claim"})

	got := store.Get(podUID)
	if got == nil {
		t.Fatal("expected non-nil config, got nil")
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 device config, got %d", len(got))
	}
	gotCfg, ok := got[deviceName]
	if !ok {
		t.Fatalf("expected device %s in config map", deviceName)
	}
	if gotCfg.Mode != NICModeShared {
		t.Errorf("expected mode %s, got %s", NICModeShared, gotCfg.Mode)
	}
	if gotCfg.NIC.MAC != "00:0d:3a:12:34:56" {
		t.Errorf("expected MAC 00:0d:3a:12:34:56, got %s", gotCfg.NIC.MAC)
	}
	if gotCfg.NIC.PodIP != "10.0.1.10" {
		t.Errorf("expected PodIP 10.0.1.10, got %s", gotCfg.NIC.PodIP)
	}
}

func TestSecondaryNICPodConfigStore_GetReturnsNilForUnknownPod(t *testing.T) {
	store := NewSecondaryNICPodConfigStore()
	got := store.Get(types.UID("nonexistent"))
	if got != nil {
		t.Errorf("expected nil for unknown pod, got %v", got)
	}
}

func TestSecondaryNICPodConfigStore_GetReturnsCopy(t *testing.T) {
	store := NewSecondaryNICPodConfigStore()

	podUID := types.UID("test-pod-uid")
	store.Set(podUID, "eth1", SecondaryNICPodConfig{Mode: NICModeShared}, types.NamespacedName{Namespace: "ns", Name: "claim"})

	got := store.Get(podUID)
	// Mutate the returned map
	got["mutated-device"] = SecondaryNICPodConfig{Mode: NICModeExclusive}

	// Original store should not be affected
	original := store.Get(podUID)
	if len(original) != 1 {
		t.Errorf("expected 1 device in original store, got %d", len(original))
	}
	if _, exists := original["mutated-device"]; exists {
		t.Error("mutating returned map should not affect the store")
	}
}

func TestSecondaryNICPodConfigStore_HasSharedPodForMAC(t *testing.T) {
	store := NewSecondaryNICPodConfigStore()
	claim := types.NamespacedName{Namespace: "ns", Name: "claim"}
	sharedPod := types.UID("shared-pod")
	exclusivePod := types.UID("exclusive-pod")

	store.Set(sharedPod, "shared-device", SecondaryNICPodConfig{
		Mode: NICModeShared,
		NIC:  NICConfig{MAC: "AA:BB:CC:DD:EE:01"},
	}, claim)
	store.Set(exclusivePod, "exclusive-device", SecondaryNICPodConfig{
		Mode: NICModeExclusive,
		NIC:  NICConfig{MAC: "aa:bb:cc:dd:ee:02"},
	}, claim)

	if !store.HasSharedPodForMAC("aa-bb-cc-dd-ee-01") {
		t.Fatal("expected shared pod match for normalized MAC")
	}
	if store.HasSharedPodForMAC("aa:bb:cc:dd:ee:02") {
		t.Fatal("exclusive pod must not count as a shared user")
	}
	if store.HasSharedPodForMAC("aa:bb:cc:dd:ee:03") {
		t.Fatal("unexpected shared pod match for unknown MAC")
	}

	store.Delete(sharedPod)
	if store.HasSharedPodForMAC("aa:bb:cc:dd:ee:01") {
		t.Fatal("deleted shared pod still reported for MAC")
	}
}

func TestSecondaryNICPodConfigStore_Delete(t *testing.T) {
	store := NewSecondaryNICPodConfigStore()

	podUID := types.UID("test-pod-uid")
	store.Set(podUID, "eth1", SecondaryNICPodConfig{Mode: NICModeShared}, types.NamespacedName{Namespace: "ns", Name: "claim"})

	store.Delete(podUID)

	got := store.Get(podUID)
	if got != nil {
		t.Errorf("expected nil after delete, got %v", got)
	}
}

func TestSecondaryNICPodConfigStore_MultipleDevices(t *testing.T) {
	store := NewSecondaryNICPodConfigStore()

	podUID := types.UID("test-pod-uid")
	store.Set(podUID, "eth1", SecondaryNICPodConfig{Mode: NICModeShared}, types.NamespacedName{Namespace: "ns", Name: "claim"})
	store.Set(podUID, "eth2", SecondaryNICPodConfig{Mode: NICModeExclusive}, types.NamespacedName{Namespace: "ns", Name: "claim"})

	got := store.Get(podUID)
	if len(got) != 2 {
		t.Fatalf("expected 2 device configs, got %d", len(got))
	}
	if got["eth1"].Mode != NICModeShared {
		t.Errorf("expected eth1 mode shared, got %s", got["eth1"].Mode)
	}
	if got["eth2"].Mode != NICModeExclusive {
		t.Errorf("expected eth2 mode exclusive, got %s", got["eth2"].Mode)
	}
}

func TestSecondaryNICPodConfigStore_OverwriteDevice(t *testing.T) {
	store := NewSecondaryNICPodConfigStore()

	podUID := types.UID("test-pod-uid")
	store.Set(podUID, "eth1", SecondaryNICPodConfig{Mode: NICModeShared}, types.NamespacedName{Namespace: "ns", Name: "claim"})
	store.Set(podUID, "eth1", SecondaryNICPodConfig{Mode: NICModeExclusive}, types.NamespacedName{Namespace: "ns", Name: "claim"})

	got := store.Get(podUID)
	if len(got) != 1 {
		t.Fatalf("expected 1 device config after overwrite, got %d", len(got))
	}
	if got["eth1"].Mode != NICModeExclusive {
		t.Errorf("expected mode exclusive after overwrite, got %s", got["eth1"].Mode)
	}
}

func TestSecondaryNICPodConfigStore_ExclusiveNICConfig(t *testing.T) {
	store := NewSecondaryNICPodConfigStore()

	podUID := types.UID("test-pod-uid")
	cfg := SecondaryNICPodConfig{
		Mode: NICModeExclusive,
		NIC: NICConfig{
			MAC:       "00:0d:3a:ab:cd:ef",
			GatewayIP: "169.254.2.1",
		},
		InterfaceConfig: apis.NetworkConfig{
			Interface: apis.InterfaceConfig{
				Addresses: []string{"10.0.1.20/32"},
			},
			Routes: []apis.RouteConfig{
				{Destination: "169.254.2.1/32", Scope: 253},
				{Destination: "0.0.0.0/0", Gateway: "169.254.2.1"},
			},
		},
	}

	store.Set(podUID, "eth1", cfg, types.NamespacedName{Namespace: "ns", Name: "claim"})

	got := store.Get(podUID)
	gotCfg := got["eth1"]
	if gotCfg.Mode != NICModeExclusive {
		t.Errorf("expected mode exclusive, got %s", gotCfg.Mode)
	}
	if gotCfg.NIC.MAC != "00:0d:3a:ab:cd:ef" {
		t.Errorf("expected MAC 00:0d:3a:ab:cd:ef, got %s", gotCfg.NIC.MAC)
	}
	if gotCfg.NIC.PodIP != "" {
		t.Error("expected empty PodIP for exclusive mode")
	}
}

func TestSecondaryNICPodConfigStore_DeletePreservesOtherPods(t *testing.T) {
	store := NewSecondaryNICPodConfigStore()
	claim := types.NamespacedName{Namespace: "ns", Name: "claim"}
	podA := types.UID("pod-a")
	podB := types.UID("pod-b")
	store.Set(podA, "eth1", SecondaryNICPodConfig{Mode: NICModeShared}, claim)
	store.Set(podB, "eth1", SecondaryNICPodConfig{Mode: NICModeShared}, claim)

	if err := store.Delete(podA); err != nil {
		t.Fatalf("Delete() error: %v", err)
	}
	if store.Get(podA) != nil {
		t.Fatal("deleted pod still exists")
	}
	if store.Get(podB) == nil {
		t.Fatal("deleting pod A removed pod B")
	}
}

func TestSecondaryNICPodConfigStore_DeleteByClaim(t *testing.T) {
	store := NewSecondaryNICPodConfigStore()
	claim := types.NamespacedName{Namespace: "ns", Name: "claim-a"}
	otherClaim := types.NamespacedName{Namespace: "ns", Name: "claim-b"}
	podA := types.UID("pod-a")
	podB := types.UID("pod-b")
	podC := types.UID("pod-c")
	store.Set(podA, "eth1", SecondaryNICPodConfig{Mode: NICModeShared}, claim)
	store.Set(podB, "eth1", SecondaryNICPodConfig{Mode: NICModeShared}, claim)
	store.Set(podC, "eth1", SecondaryNICPodConfig{Mode: NICModeShared}, otherClaim)

	if err := store.DeleteByClaim(claim); err != nil {
		t.Fatalf("DeleteByClaim() error: %v", err)
	}

	if store.Get(podA) != nil || store.Get(podB) != nil {
		t.Error("expected both pods removed from store after DeleteByClaim")
	}
	if store.Get(podC) == nil {
		t.Error("DeleteByClaim removed a pod belonging to another claim")
	}
	if err := store.DeleteByClaim(types.NamespacedName{Namespace: "ns", Name: "unknown"}); err != nil {
		t.Fatalf("DeleteByClaim(unknown) error: %v", err)
	}
}

func TestSecondaryNICPodConfigStore_OverwriteUpdatesClaim(t *testing.T) {
	store := NewSecondaryNICPodConfigStore()
	podUID := types.UID("pod-a")
	claimA := types.NamespacedName{Namespace: "ns", Name: "claim-a"}
	claimB := types.NamespacedName{Namespace: "ns", Name: "claim-b"}

	if err := store.Set(podUID, "eth1", SecondaryNICPodConfig{Mode: NICModeShared}, claimA); err != nil {
		t.Fatalf("Set(claimA) error: %v", err)
	}
	if err := store.Set(podUID, "eth1", SecondaryNICPodConfig{Mode: NICModeExclusive}, claimB); err != nil {
		t.Fatalf("Set(claimB) error: %v", err)
	}
	if err := store.DeleteByClaim(claimA); err != nil {
		t.Fatalf("DeleteByClaim(claimA) error: %v", err)
	}

	configs := store.Get(podUID)
	if configs == nil {
		t.Fatal("old claim deletion removed overwritten config")
	}
	if got := configs["eth1"].Claim; got != claimB {
		t.Fatalf("stored claim = %v, want %v", got, claimB)
	}
}
