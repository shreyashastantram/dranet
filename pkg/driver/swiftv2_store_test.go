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

func TestSwiftV2PodConfigStore_SetAndGet(t *testing.T) {
	store := NewSwiftV2PodConfigStore()

	podUID := types.UID("test-pod-uid-1234")
	deviceName := "eth1"
	cfg := SwiftV2PodConfig{
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

func TestSwiftV2PodConfigStore_GetReturnsNilForUnknownPod(t *testing.T) {
	store := NewSwiftV2PodConfigStore()
	got := store.Get(types.UID("nonexistent"))
	if got != nil {
		t.Errorf("expected nil for unknown pod, got %v", got)
	}
}

func TestSwiftV2PodConfigStore_GetReturnsCopy(t *testing.T) {
	store := NewSwiftV2PodConfigStore()

	podUID := types.UID("test-pod-uid")
	store.Set(podUID, "eth1", SwiftV2PodConfig{Mode: NICModeShared}, types.NamespacedName{Namespace: "ns", Name: "claim"})

	got := store.Get(podUID)
	// Mutate the returned map
	got["mutated-device"] = SwiftV2PodConfig{Mode: NICModeDedicated}

	// Original store should not be affected
	original := store.Get(podUID)
	if len(original) != 1 {
		t.Errorf("expected 1 device in original store, got %d", len(original))
	}
	if _, exists := original["mutated-device"]; exists {
		t.Error("mutating returned map should not affect the store")
	}
}

func TestSwiftV2PodConfigStore_Delete(t *testing.T) {
	store := NewSwiftV2PodConfigStore()

	podUID := types.UID("test-pod-uid")
	store.Set(podUID, "eth1", SwiftV2PodConfig{Mode: NICModeShared}, types.NamespacedName{Namespace: "ns", Name: "claim"})

	store.Delete(podUID)

	got := store.Get(podUID)
	if got != nil {
		t.Errorf("expected nil after delete, got %v", got)
	}
}

func TestSwiftV2PodConfigStore_MultipleDevices(t *testing.T) {
	store := NewSwiftV2PodConfigStore()

	podUID := types.UID("test-pod-uid")
	store.Set(podUID, "eth1", SwiftV2PodConfig{Mode: NICModeShared}, types.NamespacedName{Namespace: "ns", Name: "claim"})
	store.Set(podUID, "eth2", SwiftV2PodConfig{Mode: NICModeDedicated}, types.NamespacedName{Namespace: "ns", Name: "claim"})

	got := store.Get(podUID)
	if len(got) != 2 {
		t.Fatalf("expected 2 device configs, got %d", len(got))
	}
	if got["eth1"].Mode != NICModeShared {
		t.Errorf("expected eth1 mode shared, got %s", got["eth1"].Mode)
	}
	if got["eth2"].Mode != NICModeDedicated {
		t.Errorf("expected eth2 mode dedicated, got %s", got["eth2"].Mode)
	}
}

func TestSwiftV2PodConfigStore_OverwriteDevice(t *testing.T) {
	store := NewSwiftV2PodConfigStore()

	podUID := types.UID("test-pod-uid")
	store.Set(podUID, "eth1", SwiftV2PodConfig{Mode: NICModeShared}, types.NamespacedName{Namespace: "ns", Name: "claim"})
	store.Set(podUID, "eth1", SwiftV2PodConfig{Mode: NICModeDedicated}, types.NamespacedName{Namespace: "ns", Name: "claim"})

	got := store.Get(podUID)
	if len(got) != 1 {
		t.Fatalf("expected 1 device config after overwrite, got %d", len(got))
	}
	if got["eth1"].Mode != NICModeDedicated {
		t.Errorf("expected mode dedicated after overwrite, got %s", got["eth1"].Mode)
	}
}

func TestSwiftV2PodConfigStore_DedicatedNICConfig(t *testing.T) {
	store := NewSwiftV2PodConfigStore()

	podUID := types.UID("test-pod-uid")
	cfg := SwiftV2PodConfig{
		Mode: NICModeDedicated,
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
	if gotCfg.Mode != NICModeDedicated {
		t.Errorf("expected mode dedicated, got %s", gotCfg.Mode)
	}
	if gotCfg.NIC.MAC != "00:0d:3a:ab:cd:ef" {
		t.Errorf("expected MAC 00:0d:3a:ab:cd:ef, got %s", gotCfg.NIC.MAC)
	}
	if gotCfg.NIC.PodIP != "" {
		t.Error("expected empty PodIP for dedicated mode")
	}
}

func TestSwiftV2PodConfigStore_DeleteRemovesFromClaimToPods(t *testing.T) {
	store := NewSwiftV2PodConfigStore()
	claim := types.NamespacedName{Namespace: "ns", Name: "claim"}
	podA := types.UID("pod-a")
	podB := types.UID("pod-b")
	store.Set(podA, "eth1", SwiftV2PodConfig{Mode: NICModeShared}, claim)
	store.Set(podB, "eth1", SwiftV2PodConfig{Mode: NICModeShared}, claim)

	store.Delete(podA)

	if uids := store.claimToPods[claim]; len(uids) != 1 || uids[0] != podB {
		t.Fatalf("expected claimToPods[%v] == [%s] after deleting %s, got %v", claim, podB, podA, uids)
	}

	store.Delete(podB)

	if _, exists := store.claimToPods[claim]; exists {
		t.Errorf("expected claim entry removed from claimToPods once its last pod is deleted")
	}
}

func TestSwiftV2PodConfigStore_DeleteByClaim(t *testing.T) {
	store := NewSwiftV2PodConfigStore()
	claim := types.NamespacedName{Namespace: "ns", Name: "claim"}
	podA := types.UID("pod-a")
	podB := types.UID("pod-b")
	store.Set(podA, "eth1", SwiftV2PodConfig{Mode: NICModeShared}, claim)
	store.Set(podB, "eth1", SwiftV2PodConfig{Mode: NICModeShared}, claim)

	store.DeleteByClaim(claim)

	if store.Get(podA) != nil || store.Get(podB) != nil {
		t.Error("expected both pods removed from store after DeleteByClaim")
	}
	if _, exists := store.claimToPods[claim]; exists {
		t.Error("expected claim removed from claimToPods after DeleteByClaim")
	}
}
