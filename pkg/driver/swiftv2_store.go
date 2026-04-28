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

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/dranet/pkg/apis"
)

// NICMode indicates whether a NIC is shared (up to 16 pods per physical NIC)
// or dedicated (1:1 mapping between pod and physical NIC).
type NICMode string

const (
	// NICModeShared indicates the NIC is shared across multiple pods via ipvlan L3.
	NICModeShared NICMode = "shared"
	// NICModeDedicated indicates the NIC is dedicated to a single pod.
	NICModeDedicated NICMode = "dedicated"
)

// NICConfig holds the configuration needed to plumb a NIC into a pod's
// network namespace. The NICMode field on SwiftV2PodConfig determines which
// fields are relevant:
//   - Shared mode uses MAC (to find the parent), PodIP, GatewayIP, SubnetPrefix, PodUID
//   - Dedicated mode uses MAC (to find the NIC), GatewayIP, and Addresses
type NICConfig struct {
	// MAC is the NIC's MAC address. For shared mode, this identifies the parent
	// NIC (shared by all ipvlan children). For dedicated mode, this identifies
	// the physical NIC to move into the pod namespace. Matching the CNI
	// SecondaryEndpointClient behavior of looking up NICs by MAC.
	MAC string
	// GatewayIP is the virtual gateway IP for routing (e.g., "169.254.2.1").
	GatewayIP string
	// Addresses are the IP/CIDR addresses to assign for dedicated NIC mode
	// (e.g., ["10.244.2.50/24"]).
	Addresses []string
	// PodIP is the IP address assigned to this pod (shared mode only, e.g., "10.0.1.10").
	PodIP string
	// SubnetPrefix is the subnet prefix length (shared mode only, e.g., 24).
	SubnetPrefix int
	// PodUID is used for naming the ipvlan child interface (shared mode only, first 8 chars).
	PodUID string
}

// SwiftV2PodConfig holds the pre-computed networking configuration for a single
// device assigned to a SwiftV2-managed pod. The DRANET Driver populates this
// during PrepareResourceClaims and the NRI Plugin reads it during RunPodSandbox.
type SwiftV2PodConfig struct {
	// Mode indicates whether this device uses a shared or dedicated NIC.
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
}

// SwiftV2PodConfigStore provides pod-UID-keyed storage for NRI plugin lookups.
// The DRANET Driver writes to both the upstream PodConfigStore (for DRA protocol)
// and this store (for NRI). The NRI Plugin reads only from this store.
//
// This store lives in its own file (swiftv2_store.go) that upstream never touches,
// so git rebase on upstream changes never conflicts with it.
type SwiftV2PodConfigStore struct {
	mu    sync.RWMutex
	store map[types.UID]map[string]SwiftV2PodConfig // podUID → deviceName → config

	// claimToPods tracks which pod UIDs are associated with each claim,
	// so UnprepareResourceClaims can clean up by claim name.
	claimToPods map[types.NamespacedName][]types.UID // claimKey → []podUID
}

// NewSwiftV2PodConfigStore creates and returns a new SwiftV2PodConfigStore.
func NewSwiftV2PodConfigStore() *SwiftV2PodConfigStore {
	return &SwiftV2PodConfigStore{
		store:       make(map[types.UID]map[string]SwiftV2PodConfig),
		claimToPods: make(map[types.NamespacedName][]types.UID),
	}
}

// Set stores the configuration for a specific device under a given pod UID.
// If a configuration for the pod UID or device name already exists, it will be overwritten.
// The claimKey is used to track which pods are associated with a claim for cleanup.
func (s *SwiftV2PodConfigStore) Set(podUID types.UID, device string, cfg SwiftV2PodConfig, claimKey types.NamespacedName) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.store[podUID] == nil {
		s.store[podUID] = make(map[string]SwiftV2PodConfig)
	}
	s.store[podUID][device] = cfg

	// Track claim → pod UID mapping (avoid duplicates).
	for _, uid := range s.claimToPods[claimKey] {
		if uid == podUID {
			return
		}
	}
	s.claimToPods[claimKey] = append(s.claimToPods[claimKey], podUID)
}

// Get retrieves all device configurations for a given pod UID.
// Returns nil if the pod is not SwiftV2-managed (no entry exists).
// Returns a copy of the map to prevent external modification of internal state.
func (s *SwiftV2PodConfigStore) Get(podUID types.UID) map[string]SwiftV2PodConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	configs, found := s.store[podUID]
	if !found {
		return nil
	}
	// Return a copy to prevent external modification of the internal map
	configsCopy := make(map[string]SwiftV2PodConfig, len(configs))
	for k, v := range configs {
		configsCopy[k] = v
	}
	return configsCopy
}

// Delete removes all configurations associated with a given pod UID.
func (s *SwiftV2PodConfigStore) Delete(podUID types.UID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.store, podUID)
}

// DeleteByClaim removes all pod configurations associated with the given claim.
// This is called during UnprepareResourceClaims when the pod is actually deleted.
func (s *SwiftV2PodConfigStore) DeleteByClaim(claimKey types.NamespacedName) {
	s.mu.Lock()
	defer s.mu.Unlock()
	podUIDs := s.claimToPods[claimKey]
	for _, uid := range podUIDs {
		delete(s.store, uid)
	}
	delete(s.claimToPods, claimKey)
}
