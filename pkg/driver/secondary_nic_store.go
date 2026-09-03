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
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/dranet/pkg/apis"
)

// NICMode indicates whether a NIC is shared (multiple pods per physical NIC)
// or exclusive (1:1 mapping between pod and physical NIC).
type NICMode string

const (
	// NICModeShared indicates the NIC is shared across multiple pods via IPVLAN L3.
	NICModeShared NICMode = "shared"
	// NICModeExclusive indicates the NIC is exclusive to a single pod.
	NICModeExclusive NICMode = "exclusive"
)

// NICConfig holds the configuration needed to plumb a NIC into a pod's
// network namespace. The NICMode field on SecondaryNICPodConfig determines which
// fields are relevant:
//   - Shared mode uses MAC (to find the parent), PodIP, GatewayIP, PodUID
//   - Exclusive mode uses MAC (to find the NIC), GatewayIP, and Addresses
type NICConfig struct {
	// MAC is the NIC's MAC address, used to look up the NIC on the host (matching
	// the CNI SecondaryEndpointClient behavior of identifying NICs by MAC). It is
	// the same physical NIC in both modes, differing only in role: in shared mode
	// it is the IPVLAN parent shared by all IPVLAN children; in exclusive mode it
	// is moved whole into the pod's network namespace.
	MAC string
	// GatewayIP is the virtual gateway IP for routing (e.g., "169.254.2.1").
	GatewayIP string
	// Addresses are the IP/CIDR addresses to assign in exclusive mode
	// (e.g., ["10.244.2.50/24"]).
	Addresses []string
	// PodIP is the IP address assigned to this pod (shared mode only, e.g., "10.0.1.10").
	PodIP string
	// PodUID is used for naming the IPVLAN child interface (shared mode only, first 8 chars).
	PodUID string
}

// SecondaryNICPodConfig holds the pre-computed networking configuration for a single
// device assigned to a pod using a secondary NIC. The DRANET Driver populates this
// during PrepareResourceClaims and the NRI Plugin reads it during RunPodSandbox.
type SecondaryNICPodConfig struct {
	// Mode indicates whether this secondary NIC is shared or exclusive.
	Mode NICMode

	// NIC holds the NIC identification and routing configuration.
	// Which fields are relevant depends on Mode.
	NIC NICConfig

	// InterfaceConfig contains the pre-computed IP addresses and routes to apply
	// inside the pod's network namespace. Reuses the existing apis.NetworkConfig
	// type for compatibility with the upstream route/address application logic.
	InterfaceConfig apis.NetworkConfig

	// Claim identifies the ResourceClaim this device was allocated from.
	// Mirrors PodConfig.Claim so the NRI plugin can publish
	// AllocatedDeviceStatus (interface name, MAC, IPs, conditions) back to
	// the claim's status after the NIC is attached.
	Claim types.NamespacedName

	// ShareID is the per-allocation-share identifier the kube-scheduler
	// assigns when the device has allowMultipleAllocations=true (DRA
	// ConsumableCapacity). Empty when the device is not shared. The
	// ResourceClaim status validator requires the (Driver, Pool, Device,
	// ShareID) tuple of each status.devices[*] entry to exactly match an
	// entry in status.allocation.devices.results, so we round-trip it from
	// the allocation result through the secondary NIC store into the apply config.
	ShareID string
}

// SecondaryNICCheckpointer persists secondary NIC configurations so NRI can
// restore attachment intent after a driver restart.
type SecondaryNICCheckpointer interface {
	GetOrCreate() (map[types.UID]map[string]SecondaryNICPodConfig, error)
	Store(podUID types.UID, deviceName string, config SecondaryNICPodConfig) error
	DeletePods(podUIDs []types.UID) error
	Close() error
}

// SecondaryNICPodConfigStore provides pod-UID-keyed storage for NRI plugin lookups.
// CNS claim preparation writes this store and the NRI plugin reads it when the
// pod sandbox starts or stops.
//
// This store lives in its own file (secondary_nic_store.go) that upstream never touches,
// so git rebase on upstream changes never conflicts with it.
type SecondaryNICPodConfigStore struct {
	mu              sync.RWMutex
	store           map[types.UID]map[string]SecondaryNICPodConfig // podUID → deviceName → config
	nriLastActivity map[types.UID]time.Time
	checkpointer    SecondaryNICCheckpointer
}

// NewSecondaryNICPodConfigStore creates an in-memory secondary NIC store.
func NewSecondaryNICPodConfigStore() *SecondaryNICPodConfigStore {
	return &SecondaryNICPodConfigStore{
		store:           make(map[types.UID]map[string]SecondaryNICPodConfig),
		nriLastActivity: make(map[types.UID]time.Time),
	}
}

// newSecondaryNICPodConfigStore creates a store and restores any checkpointed
// secondary NIC configurations into memory.
func newSecondaryNICPodConfigStore(checkpointer SecondaryNICCheckpointer) (*SecondaryNICPodConfigStore, error) {
	store := &SecondaryNICPodConfigStore{
		store:           make(map[types.UID]map[string]SecondaryNICPodConfig),
		nriLastActivity: make(map[types.UID]time.Time),
		checkpointer:    checkpointer,
	}
	if checkpointer == nil {
		return store, nil
	}

	saved, err := checkpointer.GetOrCreate()
	if err != nil {
		return nil, err
	}
	for podUID, configs := range saved {
		store.store[podUID] = configs
		// Activity is process-local. A restored prepared pod has not yet been
		// observed by an NRI callback in this process.
		store.nriLastActivity[podUID] = time.Time{}
	}
	return store, nil
}

// Close closes the persistent backend when configured.
func (s *SecondaryNICPodConfigStore) Close() error {
	if s.checkpointer != nil {
		return s.checkpointer.Close()
	}
	return nil
}

// Set stores the configuration for a specific device under a given pod UID.
// If a configuration for the pod UID or device name already exists, it will be overwritten.
// Set is transactional for one device and relies on callers allowing only one
// secondary NIC per pod. Multi-NIC support requires a pod-level batch transaction.
// The claim is stored in the authoritative config so claim cleanup can scan the
// store without maintaining a separate reverse index.
func (s *SecondaryNICPodConfigStore) Set(podUID types.UID, device string, cfg SecondaryNICPodConfig, claimKey types.NamespacedName) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg.Claim = claimKey
	if s.checkpointer != nil {
		if err := s.checkpointer.Store(podUID, device, cfg); err != nil {
			return err
		}
	}
	if s.store[podUID] == nil {
		s.store[podUID] = make(map[string]SecondaryNICPodConfig)
	}
	s.store[podUID][device] = cfg
	if s.nriLastActivity == nil {
		s.nriLastActivity = make(map[types.UID]time.Time)
	}
	if _, found := s.nriLastActivity[podUID]; !found {
		s.nriLastActivity[podUID] = time.Time{}
	}
	return nil
}

// Get retrieves all device configurations for a given pod UID.
// Returns a copy of the map to prevent external modification of internal state.
func (s *SecondaryNICPodConfigStore) Get(podUID types.UID) (map[string]SecondaryNICPodConfig, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	configs, found := s.store[podUID]
	if !found {
		return nil, false
	}
	// Return a copy to prevent external modification of the internal map
	configsCopy := make(map[string]SecondaryNICPodConfig, len(configs))
	for k, v := range configs {
		configsCopy[k] = v
	}
	return configsCopy, true
}

// UpdateLastNRIActivity records when an NRI hook last processed this prepared
// pod. Activity is process-local and intentionally not checkpointed.
func (s *SecondaryNICPodConfigStore) UpdateLastNRIActivity(podUID types.UID, timestamp time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, found := s.store[podUID]; !found {
		return
	}
	if s.nriLastActivity == nil {
		s.nriLastActivity = make(map[types.UID]time.Time)
	}
	s.nriLastActivity[podUID] = timestamp
}

// GetPodNRIActivities returns one activity timestamp for every prepared pod.
// A zero timestamp means no secondary-NIC NRI callback has been observed in
// this process.
func (s *SecondaryNICPodConfigStore) GetPodNRIActivities() map[types.UID]time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	activities := make(map[types.UID]time.Time, len(s.store))
	for podUID := range s.store {
		activities[podUID] = s.nriLastActivity[podUID]
	}
	return activities
}

// HasSharedPodForMAC reports whether any stored pod uses the physical NIC as a
// shared IPVLAN parent.
func (s *SecondaryNICPodConfigStore) HasSharedPodForMAC(mac string) bool {
	return s.hasPodForMACAndMode(mac, NICModeShared)
}

// HasExclusivePodForMAC reports whether any stored pod uses the physical NIC
// exclusively.
func (s *SecondaryNICPodConfigStore) HasExclusivePodForMAC(mac string) bool {
	return s.hasPodForMACAndMode(mac, NICModeExclusive)
}

func (s *SecondaryNICPodConfigStore) hasPodForMACAndMode(mac string, mode NICMode) bool {
	targetMAC := normalizedMACKey(mac)

	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, configs := range s.store {
		for _, cfg := range configs {
			if cfg.Mode == mode && normalizedMACKey(cfg.NIC.MAC) == targetMAC {
				return true
			}
		}
	}
	return false
}

// Delete removes all configurations associated with a given pod UID. Persistent
// deletion happens before memory is changed so a failed delete can be retried.
func (s *SecondaryNICPodConfigStore) Delete(podUID types.UID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.checkpointer != nil {
		if err := s.checkpointer.DeletePods([]types.UID{podUID}); err != nil {
			return err
		}
	}
	delete(s.store, podUID)
	delete(s.nriLastActivity, podUID)
	return nil
}

// DeleteByClaim removes all pod configurations associated with the given claim.
// This is called during UnprepareResourceClaims when the pod is actually deleted.
func (s *SecondaryNICPodConfigStore) DeleteByClaim(claimKey types.NamespacedName) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	podUIDs := make([]types.UID, 0)
	for podUID, configs := range s.store {
		for _, cfg := range configs {
			if cfg.Claim == claimKey {
				podUIDs = append(podUIDs, podUID)
				break
			}
		}
	}
	if len(podUIDs) == 0 {
		return nil
	}
	if s.checkpointer != nil {
		if err := s.checkpointer.DeletePods(podUIDs); err != nil {
			return err
		}
	}
	for _, uid := range podUIDs {
		delete(s.store, uid)
		delete(s.nriLastActivity, uid)
	}
	return nil
}
