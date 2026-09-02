/*
Copyright The Kubernetes Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	bolt "go.etcd.io/bbolt"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/dranet/pkg/apis"
)

func TestSecondaryNICPodConfigStore_Persistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "secondary-nic.db")
	claim := types.NamespacedName{Namespace: "ns", Name: "claim"}
	config := SecondaryNICPodConfig{
		Mode: NICModeShared,
		NIC: NICConfig{
			MAC:       "00:0d:3a:12:34:56",
			GatewayIP: "169.254.2.1",
			PodIP:     "10.0.1.10",
			PodUID:    "pod-1",
		},
		InterfaceConfig: apis.NetworkConfig{
			Interface: apis.InterfaceConfig{Addresses: []string{"10.0.1.10/32"}},
			Routes:    []apis.RouteConfig{{Destination: "0.0.0.0/0", Gateway: "169.254.2.1"}},
		},
		ShareID: "share-1",
	}

	cp1, err := newSecondaryNICBoltCheckpointer(dbPath)
	if err != nil {
		t.Fatalf("newSecondaryNICBoltCheckpointer() error: %v", err)
	}
	store1, err := newSecondaryNICPodConfigStore(cp1)
	if err != nil {
		t.Fatalf("newSecondaryNICPodConfigStore() error: %v", err)
	}
	if err := store1.Set("pod-1", "device-1", config, claim); err != nil {
		t.Fatalf("Set() error: %v", err)
	}
	secondDeviceConfig := SecondaryNICPodConfig{Mode: NICModeShared, NIC: NICConfig{MAC: "00:0d:3a:12:34:57"}}
	if err := store1.Set("pod-1", "device-2", secondDeviceConfig, claim); err != nil {
		t.Fatalf("Set(second device) error: %v", err)
	}
	exclusiveConfig := SecondaryNICPodConfig{
		Mode: NICModeExclusive,
		NIC: NICConfig{
			MAC:       "00:0d:3a:65:43:21",
			GatewayIP: "169.254.2.1",
			Addresses: []string{"10.0.2.10/24"},
		},
		InterfaceConfig: apis.NetworkConfig{
			Interface: apis.InterfaceConfig{Addresses: []string{"10.0.2.10/24"}},
		},
	}
	if err := store1.Set("pod-2", "device-2", exclusiveConfig, claim); err != nil {
		t.Fatalf("Set(exclusive) error: %v", err)
	}
	if err := store1.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	cp2, err := newSecondaryNICBoltCheckpointer(dbPath)
	if err != nil {
		t.Fatalf("reopen checkpointer error: %v", err)
	}
	store2, err := newSecondaryNICPodConfigStore(cp2)
	if err != nil {
		t.Fatalf("restore store error: %v", err)
	}
	defer store2.Close()

	pod1Configs, found := store2.Get("pod-1")
	if !found {
		t.Fatal("restored pod-1 config not found")
	}
	got := pod1Configs["device-1"]
	config.Claim = claim
	if diff := cmp.Diff(config, got); diff != "" {
		t.Fatalf("restored config mismatch (-want +got):\n%s", diff)
	}
	secondDeviceConfig.Claim = claim
	if diff := cmp.Diff(secondDeviceConfig, pod1Configs["device-2"]); diff != "" {
		t.Fatalf("restored second device mismatch (-want +got):\n%s", diff)
	}
	exclusiveConfig.Claim = claim
	pod2Configs, found := store2.Get("pod-2")
	if !found {
		t.Fatal("restored pod-2 config not found")
	}
	if diff := cmp.Diff(exclusiveConfig, pod2Configs["device-2"]); diff != "" {
		t.Fatalf("restored exclusive config mismatch (-want +got):\n%s", diff)
	}
}

func TestSecondaryNICPodConfigStore_NoCheckpointer(t *testing.T) {
	store := NewSecondaryNICPodConfigStore()
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	if err := store.Set("pod-a", "device", SecondaryNICPodConfig{}, types.NamespacedName{Name: "claim"}); err != nil {
		t.Fatalf("Set() error: %v", err)
	}
	if err := store.Delete("unknown-pod"); err != nil {
		t.Fatalf("Delete(unknown) error: %v", err)
	}
}

func TestSecondaryNICBoltCheckpointer_EmptyDatabase(t *testing.T) {
	cp, err := newSecondaryNICBoltCheckpointer(filepath.Join(t.TempDir(), "secondary-nic.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()
	configs, err := cp.GetOrCreate()
	if err != nil {
		t.Fatal(err)
	}
	if len(configs) != 0 {
		t.Fatalf("empty database returned %d pods", len(configs))
	}
	if err := cp.DeletePods([]types.UID{"unknown-pod"}); err != nil {
		t.Fatalf("DeletePods(unknown) error: %v", err)
	}
}

func TestSecondaryNICBoltCheckpointer_MissingRootBucket(t *testing.T) {
	cp, err := newSecondaryNICBoltCheckpointer(filepath.Join(t.TempDir(), "secondary-nic.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()
	if err := cp.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(secondaryNICPodConfigsBucket)
	}); err != nil {
		t.Fatal(err)
	}
	if configs, err := cp.GetOrCreate(); err != nil || len(configs) != 0 {
		t.Fatalf("GetOrCreate() = (%v, %v), want empty result", configs, err)
	}
	if err := cp.DeletePods([]types.UID{"pod-a"}); err != nil {
		t.Fatalf("DeletePods() error: %v", err)
	}
	if err := cp.Store("pod-a", "device", SecondaryNICPodConfig{}); !errors.Is(err, bolt.ErrBucketNotFound) {
		t.Fatalf("Store() error = %v, want bucket-not-found", err)
	}
}

func TestSecondaryNICPodConfigStore_DeleteByClaimPersists(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "secondary-nic.db")
	claimA := types.NamespacedName{Namespace: "ns", Name: "claim-a"}
	claimB := types.NamespacedName{Namespace: "ns", Name: "claim-b"}
	cp, err := newSecondaryNICBoltCheckpointer(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	store, err := newSecondaryNICPodConfigStore(cp)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Set("pod-a", "device", SecondaryNICPodConfig{}, claimA); err != nil {
		t.Fatal(err)
	}
	if err := store.Set("pod-a-2", "device", SecondaryNICPodConfig{}, claimA); err != nil {
		t.Fatal(err)
	}
	if err := store.Set("pod-b", "device", SecondaryNICPodConfig{}, claimB); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteByClaim(claimA); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	cp, err = newSecondaryNICBoltCheckpointer(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	store, err = newSecondaryNICPodConfigStore(cp)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, found := store.Get("pod-a"); found {
		t.Fatal("claim A pod reappeared after reopen")
	}
	if _, found := store.Get("pod-a-2"); found {
		t.Fatal("second claim A pod reappeared after reopen")
	}
	if _, found := store.Get("pod-b"); !found {
		t.Fatal("claim B pod was deleted")
	}
}

func TestSecondaryNICPodConfigStore_DeletePersists(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "secondary-nic.db")
	cp, err := newSecondaryNICBoltCheckpointer(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	store, err := newSecondaryNICPodConfigStore(cp)
	if err != nil {
		t.Fatal(err)
	}
	claim := types.NamespacedName{Namespace: "ns", Name: "claim"}
	if err := store.Set("pod-a", "device", SecondaryNICPodConfig{}, claim); err != nil {
		t.Fatal(err)
	}
	if err := store.Set("pod-b", "device", SecondaryNICPodConfig{}, claim); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete("pod-a"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	cp, err = newSecondaryNICBoltCheckpointer(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	store, err = newSecondaryNICPodConfigStore(cp)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, found := store.Get("pod-a"); found {
		t.Fatal("deleted pod reappeared after reopen")
	}
	if _, found := store.Get("pod-b"); !found {
		t.Fatal("Delete removed an unrelated pod")
	}
}

func TestSecondaryNICPodConfigStore_OverwriteClaimPersists(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "secondary-nic.db")
	cp, err := newSecondaryNICBoltCheckpointer(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	store, err := newSecondaryNICPodConfigStore(cp)
	if err != nil {
		t.Fatal(err)
	}
	claimA := types.NamespacedName{Namespace: "ns", Name: "claim-a"}
	claimB := types.NamespacedName{Namespace: "ns", Name: "claim-b"}
	if err := store.Set("pod-a", "device", SecondaryNICPodConfig{Mode: NICModeShared}, claimA); err != nil {
		t.Fatal(err)
	}
	if err := store.Set("pod-a", "device", SecondaryNICPodConfig{Mode: NICModeExclusive}, claimB); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteByClaim(claimA); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	cp, err = newSecondaryNICBoltCheckpointer(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	store, err = newSecondaryNICPodConfigStore(cp)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	configs, found := store.Get("pod-a")
	if !found {
		t.Fatal("restored pod-a config not found")
	}
	got := configs["device"]
	if got.Mode != NICModeExclusive || got.Claim != claimB {
		t.Fatalf("restored config = %+v, want exclusive config owned by %v", got, claimB)
	}
}

func TestSecondaryNICBoltCheckpointer_BucketStructure(t *testing.T) {
	cp, err := newSecondaryNICBoltCheckpointer(filepath.Join(t.TempDir(), "secondary-nic.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()
	if err := cp.Store("pod-a", "device-a", SecondaryNICPodConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := cp.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(secondaryNICPodConfigsBucket)
		if root == nil {
			return errors.New("missing secondary NIC root bucket")
		}
		pod := root.Bucket([]byte("pod-a"))
		if pod == nil {
			return errors.New("missing pod bucket")
		}
		devices := pod.Bucket(secondaryNICDevicesBucket)
		if devices == nil || devices.Get([]byte("device-a")) == nil {
			return errors.New("missing device config")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSecondaryNICBoltCheckpointer_PathErrors(t *testing.T) {
	t.Run("creates parent directory", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "nested", "secondary-nic.db")
		cp, err := newSecondaryNICBoltCheckpointer(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer cp.Close()
		if _, err := os.Stat(filepath.Dir(dbPath)); err != nil {
			t.Fatalf("parent directory was not created: %v", err)
		}
	})

	t.Run("rejects directory as database", func(t *testing.T) {
		if _, err := newSecondaryNICBoltCheckpointer(t.TempDir()); err == nil {
			t.Fatal("expected invalid database path error")
		}
	})

	t.Run("rejects file as parent directory", func(t *testing.T) {
		parent := filepath.Join(t.TempDir(), "parent-file")
		if err := os.WriteFile(parent, []byte("file"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := newSecondaryNICBoltCheckpointer(filepath.Join(parent, "secondary-nic.db")); err == nil {
			t.Fatal("expected parent directory creation error")
		}
	})
}

func TestSecondaryNICPodConfigStore_ThreadSafetyWithBolt(t *testing.T) {
	cp, err := newSecondaryNICBoltCheckpointer(filepath.Join(t.TempDir(), "secondary-nic.db"))
	if err != nil {
		t.Fatal(err)
	}
	store, err := newSecondaryNICPodConfigStore(cp)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	const goroutines = 50
	var waitGroup sync.WaitGroup
	for index := 0; index < goroutines; index++ {
		waitGroup.Add(1)
		go func(index int) {
			defer waitGroup.Done()
			podUID := types.UID(fmt.Sprintf("pod-%d", index))
			device := fmt.Sprintf("device-%d", index)
			claim := types.NamespacedName{Namespace: "ns", Name: fmt.Sprintf("claim-%d", index)}
			config := SecondaryNICPodConfig{Mode: NICModeShared, NIC: NICConfig{MAC: fmt.Sprintf("00:00:00:00:00:%02x", index)}}
			if err := store.Set(podUID, device, config, claim); err != nil {
				t.Errorf("Set(%s) error: %v", podUID, err)
				return
			}
			configs, found := store.Get(podUID)
			if !found {
				t.Errorf("Get(%s) config not found", podUID)
				return
			}
			if got := configs[device]; got.NIC.MAC != config.NIC.MAC {
				t.Errorf("Get(%s) MAC = %q, want %q", podUID, got.NIC.MAC, config.NIC.MAC)
			}
		}(index)
	}
	waitGroup.Wait()
}

func TestSecondaryNICPodConfigStore_CorruptCheckpoint(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "secondary-nic.db")
	cp, err := newSecondaryNICBoltCheckpointer(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := cp.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(secondaryNICPodConfigsBucket)
		pod, err := root.CreateBucketIfNotExists([]byte("pod-1"))
		if err != nil {
			return err
		}
		devices, err := pod.CreateBucketIfNotExists(secondaryNICDevicesBucket)
		if err != nil {
			return err
		}
		return devices.Put([]byte("device"), []byte("not-json"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := cp.Close(); err != nil {
		t.Fatal(err)
	}

	cp, err = newSecondaryNICBoltCheckpointer(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()
	if _, err := newSecondaryNICPodConfigStore(cp); err == nil {
		t.Fatal("expected corrupted checkpoint error")
	}
}

type failingSecondaryNICCheckpointer struct {
	loadErr   error
	storeErr  error
	deleteErr error
}

func (f *failingSecondaryNICCheckpointer) GetOrCreate() (map[types.UID]map[string]SecondaryNICPodConfig, error) {
	return map[types.UID]map[string]SecondaryNICPodConfig{}, f.loadErr
}

func (f *failingSecondaryNICCheckpointer) Store(types.UID, string, SecondaryNICPodConfig) error {
	return f.storeErr
}

func (f *failingSecondaryNICCheckpointer) DeletePods([]types.UID) error {
	return f.deleteErr
}

func (f *failingSecondaryNICCheckpointer) Close() error { return nil }

func TestSecondaryNICPodConfigStore_WriteFailureDoesNotUpdateMemory(t *testing.T) {
	wantErr := errors.New("write failed")
	store, err := newSecondaryNICPodConfigStore(&failingSecondaryNICCheckpointer{storeErr: wantErr})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Set("pod-1", "device", SecondaryNICPodConfig{}, types.NamespacedName{Name: "claim"}); !errors.Is(err, wantErr) {
		t.Fatalf("Set() error = %v, want %v", err, wantErr)
	}
	if _, found := store.Get("pod-1"); found {
		t.Fatal("failed write updated memory")
	}
}

func TestSecondaryNICPodConfigStore_RestoreFailure(t *testing.T) {
	wantErr := errors.New("restore failed")
	if _, err := newSecondaryNICPodConfigStore(&failingSecondaryNICCheckpointer{loadErr: wantErr}); !errors.Is(err, wantErr) {
		t.Fatalf("newSecondaryNICPodConfigStore() error = %v, want %v", err, wantErr)
	}
}

func TestSecondaryNICPodConfigStore_DirectDeleteFailureRetainsMemory(t *testing.T) {
	wantErr := errors.New("delete failed")
	checkpointer := &failingSecondaryNICCheckpointer{}
	store, err := newSecondaryNICPodConfigStore(checkpointer)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Set("pod-1", "device", SecondaryNICPodConfig{}, types.NamespacedName{Name: "claim"}); err != nil {
		t.Fatal(err)
	}
	checkpointer.deleteErr = wantErr
	if err := store.Delete("pod-1"); !errors.Is(err, wantErr) {
		t.Fatalf("Delete() error = %v, want %v", err, wantErr)
	}
	if _, found := store.Get("pod-1"); !found {
		t.Fatal("failed direct delete removed memory state")
	}
}

func TestSecondaryNICPodConfigStore_DeleteFailureRetainsMemory(t *testing.T) {
	wantErr := errors.New("delete failed")
	checkpointer := &failingSecondaryNICCheckpointer{deleteErr: wantErr}
	store, err := newSecondaryNICPodConfigStore(checkpointer)
	if err != nil {
		t.Fatal(err)
	}
	checkpointer.deleteErr = nil
	if err := store.Set("pod-1", "device", SecondaryNICPodConfig{}, types.NamespacedName{Name: "claim"}); err != nil {
		t.Fatal(err)
	}
	checkpointer.deleteErr = wantErr
	if err := store.DeleteByClaim(types.NamespacedName{Name: "claim"}); !errors.Is(err, wantErr) {
		t.Fatalf("DeleteByClaim() error = %v, want %v", err, wantErr)
	}
	if _, found := store.Get("pod-1"); !found {
		t.Fatal("failed delete removed memory state")
	}
}
