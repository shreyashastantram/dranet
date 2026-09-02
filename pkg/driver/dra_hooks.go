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
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"sigs.k8s.io/dranet/pkg/apis"
	"sigs.k8s.io/dranet/pkg/cnsclient"
	"sigs.k8s.io/dranet/pkg/features"
	"sigs.k8s.io/dranet/pkg/filter"
	"sigs.k8s.io/dranet/pkg/inventory"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/Mellanox/rdmamap"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"
	"sigs.k8s.io/dranet/internal/nlwrap"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	"k8s.io/klog/v2"
)

const (
	rdmaCmPath = "/dev/infiniband/rdma_cm"

	// CNS NIC resource attribute keys (per dra.pdf ResourceSlice spec)
	cnsAttrNIC       = "networking.azure.com/nic"
	cnsAttrSubnet    = "networking.azure.com/subnet"
	cnsAttrNetworkID = "networking.azure.com/networkID"
	cnsAttrMac       = "networking.azure.com/mac"
	cnsAttrShared    = "networking.azure.com/shared"

	// Consumable capacity key (KEP-5075)
	cnsCapSlots        = "networking.azure.com/slots"
	cnsTaintNoCapacity = "networking.azure.com/no-capacity"

	// secondaryNICsPoolName is the ResourceSlice pool name used by the CNS
	// publisher to expose all secondary NIC devices for a node under a
	// single, well-known pool. Keep the existing wire-visible value for
	// compatibility with allocated ResourceClaims.
	secondaryNICsPoolName = "exclusive-nics"

	// cnsPrepareRetryInterval is how often prepareCNSResourceClaim re-calls
	// CNS while waiting for the MultitenantPodNetworkConfig to become ready.
	cnsPrepareRetryInterval = 500 * time.Millisecond

	// cnsPrepareRetryTimeout is the maximum wall-clock time prepareCNSResourceClaim
	// will spend retrying transient "MTPNC not ready" errors inside a single
	// NodePrepareResources call. Kept well below kubelet's gRPC plugin timeout
	// so we never get cancelled mid-retry. Tuned for the common case where
	// MTPNC settles within ~1-3s after pod scheduling; if it takes longer we
	// fall back to kubelet's pod sync retry (SyncFrequency, ~60-90s).
	cnsPrepareRetryTimeout = 10 * time.Second
)

// isCNSNotReadyErr returns true when err indicates the per-pod CNS state
// (MTPNC / pod IP config) has not yet been provisioned. This is the common
// startup race where kubelet calls NodePrepareResources before CNS has
// finished reconciling the MultitenantPodNetworkConfig CRD.
func isCNSNotReadyErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "mtpnc is not ready") ||
		strings.Contains(msg, "network is not ready")
}

// DRA hooks exposes Network Devices to Kubernetes, the Network devices and its attributes are
// obtained via the netdb to decouple the discovery of the interfaces with the execution.
// The exposed devices can be allocated to one or mod pods via Claim, the Claim lifecycle is
// the ones that defines the lifecycle of a device assigned to a Pod.
// The hooks NodePrepareResources and NodeUnprepareResources are needed to collect the necessary
// information so the NRI hooks can perform the configuration and attachment of Pods at runtime.

func (np *NetworkDriver) PublishResources(ctx context.Context) {
	klog.V(2).Infof("Publishing resources")
	for {
		select {
		// Wait for updates from the host-discovered (live) device inventory
		case live := <-np.netdb.GetResources(ctx):
			klog.V(3).Infof("Got %d devices from inventory: %s", len(live), formatDeviceNames(live, 15))

			// Fetch device snapshots from BoltDB store and merge
			merged := live
			if features.DefaultFeatureGate.Enabled(features.PersistentResourceSliceAttributes) {
				snapshots := np.podConfigStore.GetAllocatedDeviceSnapshots()
				merged = mergeDevices(live, snapshots)
			}

			// Apply filtering on the merged set of devices
			filtered := filter.FilterDevices(np.celProgram, merged)

			klog.V(3).Infof("After database merging and filtering, publishing %d devices in ResourceSlice(s): %s", len(filtered), formatDeviceNames(filtered, 15))

			np.publishResourcesPrometheusMetrics(filtered)

			np.mu.Lock()
			np.inventoryPools = map[string]resourceslice.Pool{
				np.nodeName: {Slices: []resourceslice.Slice{{Devices: filtered}}},
			}
			np.mu.Unlock()

			err := np.publishInventoryResources(ctx)
			if err != nil {
				klog.Error(err, "unexpected error trying to publish resources")
			} else {
				lastPublishedTime.SetToCurrentTime()
			}
		case <-ctx.Done():
			klog.Error(ctx.Err(), "context canceled")
			return
		}
	}
}

// PublishCNSResources polls the CNS REST API for NIC resources and publishes
// them as ResourceSlice pools alongside the inventory-discovered devices.
func (np *NetworkDriver) PublishCNSResources(ctx context.Context) {
	klog.V(2).Infof("Starting CNS resource publishing loop")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// The ticker's first tick is at t=5s. Run one pass eagerly so the secondary NIC
	// masquerade-exemption reconcile (and the ResourceSlice publish) happen at
	// startup rather than 5s later, restoring exemptions for already-running
	// shared pods promptly after a dranet restart or node reboot.
	if err := np.publishCNSResources(ctx); err != nil {
		klog.Errorf("failed to publish CNS resources (initial): %v", err)
	}

	for {
		select {
		case <-ticker.C:
			if err := np.publishCNSResources(ctx); err != nil {
				klog.Errorf("failed to publish CNS resources: %v", err)
			}
		case <-ctx.Done():
			klog.V(2).Infof("CNS resource publishing loop stopped")
			return
		}
	}
}

// publishCNSResources fetches the current NIC resources from CNS, checks for
// NICNetworkConfig CRDs to determine capacity, and triggers a merged publish.
func (np *NetworkDriver) publishCNSResources(ctx context.Context) error {
	nicResources, err := np.cnsClient.GetNICResources(ctx)
	if err != nil {
		return fmt.Errorf("failed to get NIC resources from CNS: %w", err)
	}
	klog.V(3).Infof("Got %d NIC resources from CNS", len(nicResources))

	np.mu.Lock()
	prev := np.lastCNSNICs
	np.lastCNSNICs = nicResources
	np.mu.Unlock()

	logCNSNICChanges(prev, nicResources)

	// Reconcile host masquerade exemptions for shared-mode secondary NICs.
	// This is the durability backstop for the per-attach ensure: it re-asserts the
	// rule for shared NICs whose pods are already running (NRI does not replay
	// attach across a dranet restart) and after an external nat flush or node
	// reboot. Ensure-only and check-first, so steady state is read-only.
	reconcileSharedNICNATExemptions(nicResources)

	return np.publishCNSPools(ctx, np.buildCNSPools(nicResources))
}

// buildCNSPools builds the secondary-NIC ResourceSlice pool from the given CNS
// NIC resources. The published pool name remains "exclusive-nics" for compatibility.
func (np *NetworkDriver) buildCNSPools(nicResources []cnsclient.NICResource) map[string]resourceslice.Pool {
	pools := make(map[string]resourceslice.Pool, 1)
	allDevices := make([]resourceapi.Device, 0, len(nicResources))
	for i := range nicResources {
		nic := &nicResources[i]
		klog.V(3).Infof("CNS NIC[%d]: InterfaceName=%q MacAddress=%q VMUniqueID=%q NetworkID=%q SubnetName=%q SubnetGUID=%q Capacity=%d",
			i, nic.InterfaceName, nic.MacAddress, nic.VMUniqueID, nic.NetworkID, nic.SubnetName, nic.SubnetGUID, nic.Capacity)
		allDevices = append(allDevices, np.buildCNSDevices(nic)...)
	}
	if len(allDevices) > 0 {
		pools[secondaryNICsPoolName] = resourceslice.Pool{
			Slices: []resourceslice.Slice{{Devices: allDevices}},
		}
	}
	return pools
}

// mergeNICResource overlays the fields RequestClaimResourceInfo returns for a NIC
// onto dst and returns the result. Capacity is always authoritative and is carried
// forward even when 0 (not schedulable). networkID/subnet are updated when present
// but never blanked (they are empty for exclusive MTPNC-only NICs).
// InterfaceName/InterfaceCompartmentID/VMUniqueID are not returned by
// RequestClaimResourceInfo, so they are preserved from dst (the GetNICResources poll).
func mergeNICResource(dst, src cnsclient.NICResource) cnsclient.NICResource {
	dst.Capacity = src.Capacity
	if src.NetworkID != "" {
		dst.NetworkID = src.NetworkID
	}
	if src.SubnetGUID != "" {
		dst.SubnetGUID = src.SubnetGUID
	}
	if src.SubnetName != "" {
		dst.SubnetName = src.SubnetName
	}
	return dst
}

// updateCNSResourceSlicesForClaim merges the NIC resources returned by
// RequestClaimResourceInfo into the secondary-NIC pool and republishes
// it. Merging (rather than replacing) updates capacity/subnet/vnet for the
// claim's MACs and adds any new MACs without deleting existing devices or fields.
// Because both GetNICResources (the poll) and RequestClaimResourceInfo return the
// latest CNS state, the periodic poll converges on the same data.
func (np *NetworkDriver) updateCNSResourceSlicesForClaim(ctx context.Context, claimNICs []cnsclient.NICResource) error {
	if len(claimNICs) == 0 {
		return nil
	}

	np.mu.Lock()
	merged := make([]cnsclient.NICResource, len(np.lastCNSNICs))
	copy(merged, np.lastCNSNICs)
	indexByMAC := make(map[string]int, len(merged))
	for i := range merged {
		indexByMAC[merged[i].MacAddress] = i
	}
	for _, cn := range claimNICs {
		if idx, ok := indexByMAC[cn.MacAddress]; ok {
			merged[idx] = mergeNICResource(merged[idx], cn)
		} else {
			indexByMAC[cn.MacAddress] = len(merged)
			merged = append(merged, cn)
		}
	}
	np.lastCNSNICs = merged
	np.mu.Unlock()

	return np.publishCNSPools(ctx, np.buildCNSPools(merged))
}

// reconcileSharedNICNATExemptions ensures the host masquerade-exemption rule is
// present for every shared-mode secondary NIC currently reported by CNS.
//
// A NIC is shared-and-host-visible when CNS reports Capacity > 1 (the MAC has a
// NICNetworkConfig CRD) and a non-empty InterfaceName (the NIC is in the host
// namespace, not already moved into a pod netns). Exclusive NICs (Capacity == 1)
// and NICs already inside a pod netns (empty InterfaceName) are skipped: they
// have no host-namespace egress to protect.
//
// The pass is ensure-only and check-first (see ensureSharedNICNATExemption), so it
// is cheap to run on every CNS poll and never prunes; a stale exemption left
// after a NIC leaves shared mode is inert.
func reconcileSharedNICNATExemptions(nics []cnsclient.NICResource) {
	for i := range nics {
		nic := &nics[i]
		if nic.Capacity <= 1 || nic.InterfaceName == "" {
			continue
		}
		if err := ensureSharedNICNATExemption(nic.InterfaceName); err != nil {
			klog.Errorf("secondary NIC: failed to reconcile NAT exemption for shared NIC %s (MAC %s): %v",
				nic.InterfaceName, nic.MacAddress, err)
		}
	}
}

// logCNSNICChanges compares the previous and current NIC resource lists from
// CNS and logs any additions, removals, or field-level changes.
func logCNSNICChanges(prev, curr []cnsclient.NICResource) {
	if prev == nil {
		klog.V(2).Infof("CNS ResourceSlice: initial publish with %d NIC(s)", len(curr))
		return
	}

	prevByMAC := make(map[string]cnsclient.NICResource, len(prev))
	for _, n := range prev {
		prevByMAC[n.MacAddress] = n
	}
	currByMAC := make(map[string]cnsclient.NICResource, len(curr))
	for _, n := range curr {
		currByMAC[n.MacAddress] = n
	}

	changed := false

	// Detect additions
	for mac, n := range currByMAC {
		if _, ok := prevByMAC[mac]; !ok {
			changed = true
			klog.Infof("CNS ResourceSlice ADDED NIC: InterfaceName=%q MAC=%q VMUniqueID=%q NetworkID=%q SubnetName=%q SubnetGUID=%q Capacity=%d",
				n.InterfaceName, n.MacAddress, n.VMUniqueID, n.NetworkID, n.SubnetName, n.SubnetGUID, n.Capacity)
		}
	}

	// Detect removals
	for mac, n := range prevByMAC {
		if _, ok := currByMAC[mac]; !ok {
			changed = true
			klog.Infof("CNS ResourceSlice REMOVED NIC: InterfaceName=%q MAC=%q VMUniqueID=%q NetworkID=%q SubnetName=%q SubnetGUID=%q Capacity=%d",
				n.InterfaceName, n.MacAddress, n.VMUniqueID, n.NetworkID, n.SubnetName, n.SubnetGUID, n.Capacity)
		}
	}

	// Detect field changes on existing NICs
	for mac, c := range currByMAC {
		p, ok := prevByMAC[mac]
		if !ok {
			continue
		}
		var diffs []string
		if p.InterfaceName != c.InterfaceName {
			diffs = append(diffs, fmt.Sprintf("InterfaceName: %q -> %q", p.InterfaceName, c.InterfaceName))
		}
		if p.VMUniqueID != c.VMUniqueID {
			diffs = append(diffs, fmt.Sprintf("VMUniqueID: %q -> %q", p.VMUniqueID, c.VMUniqueID))
		}
		if p.NetworkID != c.NetworkID {
			diffs = append(diffs, fmt.Sprintf("NetworkID: %q -> %q", p.NetworkID, c.NetworkID))
		}
		if p.SubnetGUID != c.SubnetGUID {
			diffs = append(diffs, fmt.Sprintf("SubnetGUID: %q -> %q", p.SubnetGUID, c.SubnetGUID))
		}
		if p.SubnetName != c.SubnetName {
			diffs = append(diffs, fmt.Sprintf("SubnetName: %q -> %q", p.SubnetName, c.SubnetName))
		}
		if p.Capacity != c.Capacity {
			diffs = append(diffs, fmt.Sprintf("Capacity: %d -> %d", p.Capacity, c.Capacity))
		}
		if len(diffs) > 0 {
			changed = true
			klog.Infof("CNS ResourceSlice CHANGED NIC MAC=%q: %s", mac, strings.Join(diffs, ", "))
		}
	}

	if !changed {
		klog.V(4).Infof("CNS ResourceSlice: no changes (%d NICs)", len(curr))
	}
}

// buildCNSDevices converts a single CNS NICResource into a DRA Device
// per the ResourceSlice spec in dra.pdf (KEP-5075 Consumable Capacity).
//
// Each NIC becomes one device with:
//   - allowMultipleAllocations: true
//   - attributes: networking.azure.com/nic (NIC name), networking.azure.com/subnet (subnet GUID),
//     networking.azure.com/networkID,
//     networking.azure.com/mac, networking.azure.com/shared
//   - capacity: networking.azure.com/slots with requestPolicy default=1, validRange min=1 max=1
//     when the NIC has schedulable capacity
//   - taint: networking.azure.com/no-capacity=true:NoSchedule when capacity is zero
func (np *NetworkDriver) buildCNSDevices(nic *cnsclient.NICResource) []resourceapi.Device {
	deviceName := cnsNICDeviceName(nic)

	// networking.azure.com/nic = human-readable interface name (e.g., "eth1").
	// Prefer InterfaceName, then Name, then fall back to MAC-derived deviceName.
	nicName := deviceName
	if nic.InterfaceName != "" {
		nicName = nic.InterfaceName
	}
	// networking.azure.com/subnet = customer subnet GUID provided by CNS.
	subnet := nic.SubnetGUID
	// networking.azure.com/networkID = network ID from CNS
	networkID := nic.NetworkID

	macAddr := nic.MacAddress
	allowMultiAttr := true
	attrs := map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
		cnsAttrNIC:    {StringValue: &nicName},
		cnsAttrSubnet: {StringValue: &subnet},
		cnsAttrMac:    {StringValue: &macAddr},
		cnsAttrShared: {BoolValue: &allowMultiAttr},
	}
	// Only add networkID if it's non-empty
	if networkID != "" {
		attrs[cnsAttrNetworkID] = resourceapi.DeviceAttribute{StringValue: &networkID}
	}

	// CNS capacity is authoritative, including zero when the NIC is not schedulable.
	slots := int64(nic.Capacity)
	allowMulti := true
	var requestPolicy *resourceapi.CapacityRequestPolicy
	var taints []resourceapi.DeviceTaint
	if slots == 0 {
		taints = []resourceapi.DeviceTaint{{
			Key:    cnsTaintNoCapacity,
			Value:  "true",
			Effect: resourceapi.DeviceTaintEffectNoSchedule,
		}}
	} else {
		defaultQty := resource.MustParse("1")
		minQty := resource.MustParse("1")
		maxQty := resource.MustParse("1")
		requestPolicy = &resourceapi.CapacityRequestPolicy{
			Default: &defaultQty,
			ValidRange: &resourceapi.CapacityRequestPolicyRange{
				Min: &minQty,
				Max: &maxQty,
			},
		}
	}

	return []resourceapi.Device{
		{
			Name:                     deviceName,
			AllowMultipleAllocations: &allowMulti,
			Attributes:               attrs,
			Taints:                   taints,
			Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
				cnsCapSlots: {
					Value:         *resource.NewQuantity(slots, resource.DecimalSI),
					RequestPolicy: requestPolicy,
				},
			},
		},
	}
}

// publishInventoryResources publishes inventory-discovered devices via the
// main DRA plugin (driver: dra.net).
func (np *NetworkDriver) publishInventoryResources(ctx context.Context) error {
	np.mu.Lock()
	pools := make(map[string]resourceslice.Pool, len(np.inventoryPools))
	for k, v := range np.inventoryPools {
		pools[k] = v
	}
	np.mu.Unlock()

	return np.draPlugin.PublishResources(ctx, resourceslice.DriverResources{Pools: pools})
}

// publishCNSPools publishes CNS NIC resources via the separate CNS plugin
// (driver: networking.azure.com).
func (np *NetworkDriver) publishCNSPools(ctx context.Context, pools map[string]resourceslice.Pool) error {
	if np.cnsPlugin == nil {
		return fmt.Errorf("CNS plugin not initialized")
	}
	return np.cnsPlugin.PublishResources(ctx, resourceslice.DriverResources{Pools: pools})
}

// sanitizeMACForK8s converts a MAC address to a Kubernetes-compatible name
// by lowercasing and replacing colons with hyphens.
func sanitizeMACForK8s(mac string) string {
	return strings.ReplaceAll(strings.ToLower(mac), ":", "-")
}

func (np *NetworkDriver) publishResourcesPrometheusMetrics(devices []resourceapi.Device) {
	rdmaCount := 0
	for _, device := range devices {
		if attr, ok := device.Attributes[apis.AttrRDMA]; ok && attr.BoolValue != nil && *attr.BoolValue {
			rdmaCount++
		}
	}
	publishedDevicesTotal.WithLabelValues("rdma").Set(float64(rdmaCount))
	publishedDevicesTotal.WithLabelValues("total").Set(float64(len(devices)))
}

func (np *NetworkDriver) PrepareResourceClaims(ctx context.Context, claims []*resourceapi.ResourceClaim) (map[types.UID]kubeletplugin.PrepareResult, error) {
	klog.V(2).Infof("PrepareResourceClaims is called: number of claims: %d", len(claims))
	start := time.Now()
	defer func() {
		draPluginRequestsLatencySeconds.WithLabelValues(methodPrepareResourceClaims).Observe(time.Since(start).Seconds())
	}()
	result, err := np.prepareResourceClaims(ctx, claims)
	if err != nil {
		draPluginRequestsTotal.WithLabelValues(methodPrepareResourceClaims, statusFailed).Inc()
		return result, err
	}
	// identify errors and log metrics
	isError := false
	for _, res := range result {
		if res.Err != nil {
			isError = true
			break
		}
	}
	if isError {
		draPluginRequestsTotal.WithLabelValues(methodPrepareResourceClaims, statusFailed).Inc()
	} else {
		draPluginRequestsTotal.WithLabelValues(methodPrepareResourceClaims, statusSuccess).Inc()
	}
	return result, err
}

func (np *NetworkDriver) prepareResourceClaims(ctx context.Context, claims []*resourceapi.ResourceClaim) (map[types.UID]kubeletplugin.PrepareResult, error) {
	if len(claims) == 0 {
		return nil, nil
	}
	result := make(map[types.UID]kubeletplugin.PrepareResult)

	for _, claim := range claims {
		isCNS := np.isCNSClaim(claim)
		klog.Infof("NodePrepareResources: Claim %s/%s UID=%s isCNS=%v cnsDriverName=%q allocatedDevices=%d reservedFor=%d",
			claim.Namespace, claim.Name, claim.UID, isCNS, np.cnsDriverName,
			len(claim.Status.Allocation.Devices.Results), len(claim.Status.ReservedFor))
		for i, result := range claim.Status.Allocation.Devices.Results {
			klog.Infof("  Claim %s/%s DeviceResult[%d]: Driver=%q Pool=%q Device=%q Request=%q",
				claim.Namespace, claim.Name, i, result.Driver, result.Pool, result.Device, result.Request)
		}
		for i, ref := range claim.Status.ReservedFor {
			klog.Infof("  Claim %s/%s ReservedFor[%d]: Resource=%q Name=%q UID=%s",
				claim.Namespace, claim.Name, i, ref.Resource, ref.Name, ref.UID)
		}
		if isCNS {
			result[claim.UID] = np.prepareCNSResourceClaim(ctx, claim)
		} else {
			result[claim.UID] = np.prepareResourceClaim(ctx, claim)
		}
	}
	return result, nil
}

// isCNSClaim returns true if the claim has any device results managed by the CNS driver.
func (np *NetworkDriver) isCNSClaim(claim *resourceapi.ResourceClaim) bool {
	if np.cnsDriverName == "" {
		klog.V(4).Infof("isCNSClaim: cnsDriverName is empty, returning false for claim %s/%s", claim.Namespace, claim.Name)
		return false
	}
	if claim.Status.Allocation == nil {
		klog.V(4).Infof("isCNSClaim: allocation is nil for claim %s/%s", claim.Namespace, claim.Name)
		return false
	}
	for _, result := range claim.Status.Allocation.Devices.Results {
		if result.Driver == np.cnsDriverName {
			klog.V(4).Infof("isCNSClaim: matched CNS driver %q on device %q for claim %s/%s",
				result.Driver, result.Device, claim.Namespace, claim.Name)
			return true
		}
	}
	klog.V(4).Infof("isCNSClaim: no device matched cnsDriverName=%q for claim %s/%s", np.cnsDriverName, claim.Namespace, claim.Name)
	return false
}

// prepareCNSResourceClaim is the fast path for CNS-managed secondary NIC claims.
// It queries CNS GetClaimResourceInfo once per claim for the pod's networking
// goal state (pod IP configs and the claim's NIC resources), populates the
// SecondaryNICPodConfigStore for downstream NRI use, and updates the externally named exclusive-nics
// ResourceSlice with the NIC resources CNS reported. All heavy DRA work (routes,
// rules, DHCP, ethtool, RDMA, eBPF) is skipped because the NRI hook
// (runPodSandboxSecondaryNICs) handles network plumbing from CNS goal state.
func (np *NetworkDriver) prepareCNSResourceClaim(ctx context.Context, claim *resourceapi.ResourceClaim) kubeletplugin.PrepareResult {
	klog.V(2).Infof("prepareCNSResourceClaim Claim %s/%s (fast path)", claim.Namespace, claim.Name)
	start := time.Now()
	defer func() {
		klog.V(2).Infof("prepareCNSResourceClaim Claim %s/%s took %v", claim.Namespace, claim.Name, time.Since(start))
	}()

	podConsumers := getPodConsumers(claim)
	klog.Infof("prepareCNSResourceClaim: claim %s/%s has %d pod consumers", claim.Namespace, claim.Name, len(podConsumers))
	for i, pod := range podConsumers {
		klog.Infof("prepareCNSResourceClaim: consumer[%d] pod=%s/%s UID=%s", i, pod.Namespace, pod.Name, pod.UID)
	}
	if len(podConsumers) == 0 {
		klog.Infof("no pods allocated to CNS claim %s/%s", claim.Namespace, claim.Name)
		return kubeletplugin.PrepareResult{}
	}
	if len(podConsumers) > 1 {
		return kubeletplugin.PrepareResult{
			Err: fmt.Errorf("CNS claim %s/%s has %d pod consumers; only one is supported",
				claim.Namespace, claim.Name, len(podConsumers)),
		}
	}

	// The CNS client is required to fetch the claim's networking goal state.
	// If CNS is unreachable the pod stays Pending and kubelet retries.
	if np.cnsClient == nil {
		return kubeletplugin.PrepareResult{
			Err: fmt.Errorf("CNS client not configured for claim %s/%s", claim.Namespace, claim.Name),
		}
	}

	secondaryNICCount := 0
	for _, result := range claim.Status.Allocation.Devices.Results {
		if result.Driver == np.cnsDriverName {
			secondaryNICCount++
		}
	}
	if secondaryNICCount != 1 {
		return kubeletplugin.PrepareResult{
			Err: fmt.Errorf("CNS claim %s/%s has %d secondary NIC devices; exactly one is supported",
				claim.Namespace, claim.Name, secondaryNICCount),
		}
	}

	var errorList []error
	goalStateByClaim := map[types.UID]claimGoalState{}

	klog.Infof("prepareCNSResourceClaim: iterating %d device results for claim %s/%s",
		len(claim.Status.Allocation.Devices.Results), claim.Namespace, claim.Name)
	for _, result := range claim.Status.Allocation.Devices.Results {
		if result.Driver != np.cnsDriverName {
			klog.V(4).Infof("prepareCNSResourceClaim: skipping device %q (driver=%q, not CNS driver %q)",
				result.Device, result.Driver, np.cnsDriverName)
			continue
		}

		deviceName := result.Device

		// Capture the per-share identifier assigned by the scheduler for
		// devices with DRAConsumableCapacity (allowMultipleAllocations=true).
		// The API server's ResourceClaim status validator requires the
		// (driver, pool, device, shareID) tuple to exactly match an entry
		// in status.allocation.devices.results, so we must thread shareID
		// through to the NRI hook that publishes status.devices.
		var shareID string
		if result.ShareID != nil {
			shareID = string(*result.ShareID)
		}

		// Query CNS GetClaimResourceInfo (by claim UID) and store the
		// per-pod networking config in SecondaryNICPodConfigStore for the NRI hook.
		for _, pod := range podConsumers {
			klog.Infof("prepareCNSResourceClaim: populating secondary NIC store for pod %s/%s UID=%s device=%q shareID=%q",
				pod.Namespace, pod.Name, pod.UID, deviceName, shareID)
			claimKey := types.NamespacedName{Namespace: claim.Namespace, Name: claim.Name}

			// Retry locally on transient "MTPNC not ready" errors so the pod can
			// start immediately once CNS finishes reconciling, instead of waiting
			// 60-90s for kubelet's next pod sync. Bounded by cnsPrepareRetryTimeout.
			var lastErr error
			deadline := time.Now().Add(cnsPrepareRetryTimeout)
		retryLoop:
			for attempt := 1; ; attempt++ {
				lastErr = np.populateSecondaryNICStoreForDevice(ctx, claim.UID, pod, deviceName, claimKey, shareID, goalStateByClaim)
				if lastErr == nil {
					if attempt > 1 {
						klog.Infof("prepareCNSResourceClaim: populateSecondaryNICStoreForDevice succeeded on attempt %d for pod %s/%s device %q",
							attempt, pod.Namespace, pod.Name, deviceName)
					}
					break
				}
				if !isCNSNotReadyErr(lastErr) {
					break
				}
				if !time.Now().Before(deadline) {
					klog.Warningf("prepareCNSResourceClaim: gave up after %d attempts (%v) waiting for CNS for pod %s/%s device %q: %v",
						attempt, cnsPrepareRetryTimeout, pod.Namespace, pod.Name, deviceName, lastErr)
					break
				}
				klog.V(2).Infof("prepareCNSResourceClaim: CNS not ready (attempt %d) for pod %s/%s device %q, retrying in %v: %v",
					attempt, pod.Namespace, pod.Name, deviceName, cnsPrepareRetryInterval, lastErr)
				select {
				case <-ctx.Done():
					lastErr = ctx.Err()
					break retryLoop
				case <-time.After(cnsPrepareRetryInterval):
				}
			}
			if lastErr != nil {
				klog.Errorf("prepareCNSResourceClaim: populateSecondaryNICStoreForDevice failed for pod %s/%s device %q: %v",
					pod.Namespace, pod.Name, deviceName, lastErr)
				errorList = append(errorList, lastErr)
			}
		}
	}

	// Update the externally named exclusive-nics ResourceSlice with the fresh per-NIC state
	// (capacity/subnet/vnet) CNS returned for this claim's NICs. Best-effort:
	// a failure here does not fail pod preparation.
	if gs, ok := goalStateByClaim[claim.UID]; ok && len(gs.nicResources) > 0 {
		if err := np.updateCNSResourceSlicesForClaim(ctx, gs.nicResources); err != nil {
			klog.Warningf("prepareCNSResourceClaim: failed to update ResourceSlices for claim %s/%s: %v", claim.Namespace, claim.Name, err)
		}
	}

	if len(errorList) > 0 {
		klog.Infof("CNS claim %s contain errors: %v", claim.UID, errors.Join(errorList...))
		return kubeletplugin.PrepareResult{
			Err: fmt.Errorf("CNS claim %s contain errors: %w", claim.UID, errors.Join(errorList...)),
		}
	}
	klog.Infof("prepareCNSResourceClaim: claim %s/%s completed successfully", claim.Namespace, claim.Name)
	return kubeletplugin.PrepareResult{}
}

// cnsNICDeviceName returns the stable ResourceSlice device and pool suffix for
// a CNS NICResource. MAC is the canonical NIC identity because CNS may report
// Name as either an interface name or a MAC address across NIC types.
func cnsNICDeviceName(nic *cnsclient.NICResource) string {
	return sanitizeMACForK8s(nic.MacAddress)
}

// prepareResourceClaim gets all the configuration required to be applied at runtime and passes it downs to the handlers.
// This happens in the kubelet so it can be a "slow" operation, so we can execute fast in RunPodsandbox, that happens in the
// container runtime and has strong expectactions to be executed fast (default hook timeout is 2 seconds).
//
// TODO(#290): This function has grown too large and needs to be split apart.
func (np *NetworkDriver) prepareResourceClaim(ctx context.Context, claim *resourceapi.ResourceClaim) kubeletplugin.PrepareResult {
	klog.V(2).Infof("PrepareResourceClaim Claim %s/%s", claim.Namespace, claim.Name)
	start := time.Now()
	defer func() {
		klog.V(2).Infof("PrepareResourceClaim Claim %s/%s  took %v", claim.Namespace, claim.Name, time.Since(start))
	}()
	if len(claim.Status.ReservedFor) == 0 {
		klog.Infof("no pods allocated to claim %s/%s", claim.Namespace, claim.Name)
		return kubeletplugin.PrepareResult{}
	}
	if len(claim.Status.ReservedFor) > 1 {
		return kubeletplugin.PrepareResult{
			Err: fmt.Errorf("driver only supports one pod per claim, got %d", len(claim.Status.ReservedFor)),
		}
	}

	// One ResourceClaim is consumed by a single pod, regardless of whether the device is allocated in
	// exclusive or shared (e.g., ipvlan, macvlan) way, so we process the first and only consumer in ReservedFor.
	reserved := claim.Status.ReservedFor[0]
	if reserved.Resource != "pods" || reserved.APIGroup != "" {
		return kubeletplugin.PrepareResult{
			Err: fmt.Errorf("driver only supports Pods, unsupported reference %#v", reserved),
		}
	}
	podUID := reserved.UID

	nlHandle, err := nlwrap.NewHandle()
	if err != nil {
		return kubeletplugin.PrepareResult{
			Err: fmt.Errorf("error creating netlink handle %v", err),
		}
	}

	rulesByTable, err := getRuleInfo(nlHandle)
	if err != nil {
		return kubeletplugin.PrepareResult{
			Err: fmt.Errorf("error getting rule info: %v", err),
		}
	}

	var errorList []error
	charDevices := sets.New[string]()
	for _, result := range claim.Status.Allocation.Devices.Results {
		// A single ResourceClaim can have devices managed by distinct DRA
		// drivers. One common use case for this is device topology alignment
		// (think NIC and GPU alignment). In such cases, we should ignore the
		// devices which are not managed by our driver.
		//
		// TODO: Test running a different driver alongside DraNet in e2e. This
		//   requires an easy way to spin up a mock DRA driver.
		if result.Driver != np.driverName {
			continue
		}
		requestName := result.Request
		userConf := &apis.NetworkConfig{}
		for _, config := range claim.Status.Allocation.Devices.Config {
			// Check there is a config associated to this device
			if config.Opaque == nil ||
				config.Opaque.Driver != np.driverName ||
				len(config.Requests) > 0 && !slices.Contains(config.Requests, requestName) {
				continue
			}
			// Check if there is a custom configuration
			conf, errs := apis.ValidateConfig(&config.Opaque.Parameters)
			if len(errs) > 0 {
				errorList = append(errorList, errs...)
				continue
			}
			// TODO: define a strategy for multiple configs
			if conf != nil {
				userConf = conf
				break
			}
		}

		mergedConf, err := np.getDeviceNetworkConfig(result.Device, claim.UID, userConf)
		if err != nil {
			errorList = append(errorList, err)
			continue
		}

		netconf := *mergedConf

		klog.V(4).Infof("PrepareResourceClaim %s/%s final Configuration %#v", claim.Namespace, claim.Name, netconf)
		// Query the local discovery database (netdb) for the card's clean attributes
		var deviceSnapshot *resourceapi.Device
		if device, ok := np.netdb.GetDevice(result.Device); ok {
			deviceSnapshot = &device
		} else {
			klog.Warningf("Failed to find device %s in inventory for claim %s", result.Device, claim.UID)
		}

		deviceCfg := DeviceConfig{
			Claim: types.NamespacedName{
				Namespace: claim.Namespace,
				Name:      claim.Name,
			},
			NetworkInterfaceConfigInPod: netconf,
			DeviceSnapshot:              deviceSnapshot,
		}

		// Store early to guarantee profile cleanup on subsequent failures within this loop.
		// If the preparation fails later, Kubelet will call UnprepareResourceClaims,
		// which will find this early config and release the allocated profile.
		if netconf.Profile != "" {
			if err := np.podConfigStore.SetDeviceConfig(podUID, result.Device, deviceCfg); err != nil {
				errorList = append(errorList, fmt.Errorf("failed to persist early device config for pod %s device %s: %v", podUID, result.Device, err))
				// If we can't store it, we MUST release it immediately to prevent a leak.
				if relErr := np.netdb.ReleaseProfileConfig(result.Device, claim.UID, &netconf); relErr != nil {
					klog.Errorf("failed to rollback profile config for claim %v device %v: %v", claim.UID, result.Device, relErr)
				}
				continue
			}
		}

		// IB-only path: device has RDMA capability but no netdev interface.
		if np.netdb.IsIBOnlyDevice(result.Device) {
			// Reject any network-specific config fields for RDMA-only devices.
			for _, config := range claim.Status.Allocation.Devices.Config {
				if config.Opaque == nil ||
					config.Opaque.Driver != np.driverName ||
					len(config.Requests) > 0 && !slices.Contains(config.Requests, requestName) {
					continue
				}
				if errs := apis.ValidateRDMAOnlyConfig(&config.Opaque.Parameters); len(errs) > 0 {
					errorList = append(errorList, errs...)
				}
			}
			if len(errorList) > 0 {
				continue
			}
			rdmaDevName, err := np.netdb.GetRDMADeviceName(result.Device)
			if err != nil {
				errorList = append(errorList, fmt.Errorf("failed to get RDMA device name for IB-only device %s: %v", result.Device, err))
				continue
			}
			deviceCfg.RDMADevice = buildRDMAConfig(rdmaDevName, charDevices)
			if err := np.podConfigStore.SetDeviceConfig(podUID, result.Device, deviceCfg); err != nil {
				errorList = append(errorList, fmt.Errorf("failed to persist device config for pod %s device %s: %v", podUID, result.Device, err))
			}
			klog.V(4).Infof("IB-only claim resources for pod %s : %#v", podUID, deviceCfg)
			continue
		}

		ifName, err := np.netdb.GetNetInterfaceName(result.Device)
		if err != nil {
			errorList = append(errorList, fmt.Errorf("failed to get network interface name for device %s: %v", result.Device, err))
			continue
		}
		// Get Network configuration and merge it
		link, err := nlHandle.LinkByName(ifName)
		if err != nil {
			errorList = append(errorList, fmt.Errorf("failed to get netlink to interface %s: %v", ifName, err))
			continue
		}
		deviceCfg.NetworkInterfaceConfigInHost.Interface.Name = ifName

		if deviceCfg.NetworkInterfaceConfigInPod.Interface.Name == "" {
			// If the interface name was not explicitly overridden, use the same
			// interface name within the pod's network namespace.
			deviceCfg.NetworkInterfaceConfigInPod.Interface.Name = ifName
		}

		// For SR-IOV VFs, the requested MTU must not exceed the parent PF's MTU.
		// Otherwise the claim is rejected so the Pod fails fast instead of being
		// created with an illegal MTU configuration.
		if deviceCfg.NetworkInterfaceConfigInPod.Interface.MTU != nil && inventory.IsSriovVf(ifName) {
			pfName, err := inventory.GetPFInterfaceName(ifName)
			if err != nil {
				errorList = append(errorList, fmt.Errorf("failed to determine parent PF for SR-IOV VF %s: %v", ifName, err))
				continue
			}
			pfLink, err := nlHandle.LinkByName(pfName)
			if err != nil {
				errorList = append(errorList, fmt.Errorf("failed to get netlink to parent PF %s of VF %s: %v", pfName, ifName, err))
				continue
			}
			requestedMTU := int(*deviceCfg.NetworkInterfaceConfigInPod.Interface.MTU)
			if err := validateVFMTU(ifName, pfName, requestedMTU, pfLink.Attrs().MTU); err != nil {
				errorList = append(errorList, err)
				continue
			}
		}

		// If DHCP is requested, do a DHCP request to gather the network parameters (IPs and Routes)
		// ... but we DO NOT apply them in the root namespace
		if deviceCfg.NetworkInterfaceConfigInPod.Interface.DHCP != nil && *deviceCfg.NetworkInterfaceConfigInPod.Interface.DHCP {
			klog.V(2).Infof("trying to get network configuration via DHCP")
			contextCancel, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			ip, routes, err := getDHCP(contextCancel, ifName)
			if err != nil {
				errorList = append(errorList, fmt.Errorf("fail to get configuration via DHCP for %s: %w", ifName, err))
			} else {
				deviceCfg.NetworkInterfaceConfigInPod.Interface.Addresses = []string{ip}
				deviceCfg.NetworkInterfaceConfigInPod.Routes = append(deviceCfg.NetworkInterfaceConfigInPod.Routes, routes...)
			}
		} else if len(deviceCfg.NetworkInterfaceConfigInPod.Interface.Addresses) == 0 {
			// If there is no custom addresses and no DHCP, then use the existing ones
			// get the existing IP addresses
			nlAddresses, err := nlHandle.AddrList(link, netlink.FAMILY_ALL)
			if err != nil {
				errorList = append(errorList, fmt.Errorf("fail to get ip addresses for interface %s : %w", ifName, err))
			} else {
				for _, address := range nlAddresses {
					// Only move IP addresses with global scope because those are not host-specific, auto-configured,
					// or have limited network scope, making them unsuitable inside the container namespace.
					// Ref: https://www.ietf.org/rfc/rfc3549.txt
					if address.Scope != unix.RT_SCOPE_UNIVERSE {
						continue
					}
					deviceCfg.NetworkInterfaceConfigInPod.Interface.Addresses = append(deviceCfg.NetworkInterfaceConfigInPod.Interface.Addresses, address.IPNet.String())
				}
			}
		}

		// Obtain the existing supported ethtool features and validate the config
		if deviceCfg.NetworkInterfaceConfigInPod.Ethtool != nil {
			client, err := newEthtoolClient(0)
			if err != nil {
				errorList = append(errorList, fmt.Errorf("fail to create ethtool client %v", err))
				continue
			}
			defer client.Close()

			ifFeatures, err := client.GetFeatures(ifName)
			if err != nil {
				errorList = append(errorList, fmt.Errorf("fail to get ethtool features %v", err))
				continue
			}

			// translate features to the actual kernel names
			ethtoolFeatures := map[string]bool{}
			for feature, value := range deviceCfg.NetworkInterfaceConfigInPod.Ethtool.Features {
				aliases := ifFeatures.Get(feature)
				if len(aliases) == 0 {
					errorList = append(errorList, fmt.Errorf("feature %s not supported by interface", feature))
					continue
				}
				for _, alias := range aliases {
					ethtoolFeatures[alias] = value
				}
			}
			deviceCfg.NetworkInterfaceConfigInPod.Ethtool.Features = ethtoolFeatures
		}

		// Obtain the routes and rules associated with the interface.
		routes, tables, err := getRouteInfo(nlHandle, ifName, link)
		if err != nil {
			errorList = append(errorList, err)
			continue
		}
		deviceCfg.NetworkInterfaceConfigInPod.Routes = append(deviceCfg.NetworkInterfaceConfigInPod.Routes, routes...)

		// If VRF is enabled, we do not need to copy the rules from the host
		// because the VRF handles the routing table lookup.
		if deviceCfg.NetworkInterfaceConfigInPod.Interface.VRF == nil {
			for _, table := range tables.UnsortedList() {
				if rules, ok := rulesByTable[table]; ok {
					klog.V(5).Infof("Adding %d rules for table %d associated with interface %s", len(rules), table, ifName)
					deviceCfg.NetworkInterfaceConfigInPod.Rules = append(deviceCfg.NetworkInterfaceConfigInPod.Rules, rules...)
					// Avoid adding the same rule twice
					delete(rulesByTable, table)
				}
			}
		}

		// Obtain the neighbors associated to the interface
		neighs, err := nlHandle.NeighList(link.Attrs().Index, netlink.FAMILY_ALL)
		if err != nil {
			klog.Infof("failed to get neighbors for interface %s: %v", ifName, err)
		}
		for _, neigh := range neighs {
			if neigh.IP == nil || neigh.HardwareAddr == nil {
				continue
			}
			// We are only interested in permanent neighbor entries
			if neigh.State != netlink.NUD_PERMANENT {
				continue
			}
			neighCfg := apis.NeighborConfig{
				Destination:  neigh.IP.String(),
				HardwareAddr: neigh.HardwareAddr.String(),
			}
			deviceCfg.NetworkInterfaceConfigInPod.Neighbors = append(deviceCfg.NetworkInterfaceConfigInPod.Neighbors, neighCfg)
		}

		// Get RDMA configuration: link and char devices
		if rdmaDev, err := inventory.GetRdmaDevice(ifName); err == nil && rdmaDev != "" {
			klog.V(2).Infof("RunPodSandbox processing RDMA device: %s", rdmaDev)
			deviceCfg.RDMADevice = buildRDMAConfig(rdmaDev, charDevices)
		}

		// Remove the pinned programs before the NRI hooks since it
		// has to walk the entire bpf virtual filesystem and is slow
		// TODO: check if there is some other way to do this
		if deviceCfg.NetworkInterfaceConfigInPod.Interface.DisableEBPFPrograms != nil &&
			*deviceCfg.NetworkInterfaceConfigInPod.Interface.DisableEBPFPrograms {
			err := unpinBPFPrograms(ifName)
			if err != nil {
				klog.Infof("error unpinning ebpf programs for %s : %v", ifName, err)
			}
		}

		if err := np.podConfigStore.SetDeviceConfig(podUID, result.Device, deviceCfg); err != nil {
			errorList = append(errorList, fmt.Errorf("failed to persist device config for pod %s device %s: %v", podUID, result.Device, err))
		}
		klog.V(4).Infof("Claim Resources for pod %s : %#v", podUID, deviceCfg)
	}

	if len(errorList) > 0 {
		joinedErr := errors.Join(errorList...)
		klog.Infof("claim %s contain errors: %v", claim.UID, joinedErr)
		np.eventRecorder.Eventf(claim, v1.EventTypeWarning, "ClaimPrepareFailed", "%v", joinedErr)
		return kubeletplugin.PrepareResult{
			Err: fmt.Errorf("claim %s contain errors: %w", claim.UID, joinedErr),
		}
	}
	return kubeletplugin.PrepareResult{}
}

func (np *NetworkDriver) UnprepareResourceClaims(ctx context.Context, claims []kubeletplugin.NamespacedObject) (map[types.UID]error, error) {
	klog.V(2).Infof("UnprepareResourceClaims is called: number of claims: %d", len(claims))
	start := time.Now()
	defer func() {
		draPluginRequestsLatencySeconds.WithLabelValues(methodUnprepareResourceClaims).Observe(time.Since(start).Seconds())
	}()
	result, err := np.unprepareResourceClaims(ctx, claims)
	if err != nil {
		draPluginRequestsTotal.WithLabelValues(methodUnprepareResourceClaims, statusFailed).Inc()
		return result, err
	}
	// identify errors and log metrics
	isError := false
	for _, res := range result {
		if res != nil {
			isError = true
			break
		}
	}
	if isError {
		draPluginRequestsTotal.WithLabelValues(methodUnprepareResourceClaims, statusFailed).Inc()
	} else {
		draPluginRequestsTotal.WithLabelValues(methodUnprepareResourceClaims, statusSuccess).Inc()
	}
	return result, err
}

func (np *NetworkDriver) unprepareResourceClaims(ctx context.Context, claims []kubeletplugin.NamespacedObject) (map[types.UID]error, error) {
	if len(claims) == 0 {
		return nil, nil
	}

	result := make(map[types.UID]error)
	for _, claim := range claims {
		err := np.unprepareResourceClaim(ctx, claim)
		result[claim.UID] = err
		if err != nil {
			klog.Infof("error unpreparing ressources for claim %s/%s : %v", claim.Namespace, claim.Name, err)
		}
	}
	return result, nil
}

func (np *NetworkDriver) unprepareResourceClaim(_ context.Context, claim kubeletplugin.NamespacedObject) error {
	for _, podUID := range np.podConfigStore.ListPods() {
		podCfg, ok := np.podConfigStore.GetPodConfig(podUID)
		if !ok {
			continue
		}
		for deviceName, devCfg := range podCfg.DeviceConfigs {
			if devCfg.Claim.Namespace == claim.Namespace && devCfg.Claim.Name == claim.Name {
				if devCfg.NetworkInterfaceConfigInPod.Profile != "" {
					if err := np.netdb.ReleaseProfileConfig(deviceName, claim.UID, &devCfg.NetworkInterfaceConfigInPod); err != nil {
						klog.Errorf("failed to release profile config for claim %v: %v", claim.NamespacedName, err)
					}
				}
			}
		}
	}

	np.podConfigStore.DeleteClaim(claim.NamespacedName)
	if np.secondaryNICStore != nil {
		if err := np.secondaryNICStore.DeleteByClaim(claim.NamespacedName); err != nil {
			return fmt.Errorf("delete persisted secondary NIC config for claim %s/%s: %w", claim.Namespace, claim.Name, err)
		}
	}
	klog.V(2).Infof("UnprepareResourceClaim: cleaned up DRA and secondary NIC stores for claim %s/%s", claim.Namespace, claim.Name)
	return nil
}

func (np *NetworkDriver) HandleError(ctx context.Context, err error, msg string) {
	// For now we just follow the advice documented in the DRAPlugin API docs.
	// See: https://pkg.go.dev/k8s.io/apimachinery/pkg/util/runtime#HandleErrorWithContext
	runtime.HandleErrorWithContext(ctx, err, msg)
}

func formatDeviceNames(devices []resourceapi.Device, max int) string {
	deviceNames := make([]string, len(devices))
	for i := range devices {
		deviceNames[i] = devices[i].Name
	}

	if len(deviceNames) <= max {
		return strings.Join(deviceNames, ", ")
	}

	return fmt.Sprintf("%s, and %d more", strings.Join(deviceNames[:max], ", "), len(deviceNames)-max)
}

// buildRDMAConfig populates an RDMAConfig for the given rdma device name.
// It inserts the rdma_cm and per-device character device paths into charDevices,
// then resolves each path to a LinuxDevice entry.
func buildRDMAConfig(rdmaDevName string, charDevices sets.Set[string]) RDMAConfig {
	cfg := RDMAConfig{LinkDev: rdmaDevName}
	charDevices.Insert(rdmaCmPath)
	charDevices.Insert(rdmamap.GetRdmaCharDevices(rdmaDevName)...)
	for _, devpath := range charDevices.UnsortedList() {
		dev, err := GetDeviceInfo(devpath)
		if err != nil {
			klog.Infof("fail to get device info for %s : %v", devpath, err)
		} else {
			cfg.DevChars = append(cfg.DevChars, dev)
		}
	}
	return cfg
}

// validateVFMTU returns an error if the MTU requested for an SR-IOV VF exceeds
// the parent PF's MTU, which is an illegal configuration. vfName and pfName are
// only used to build a descriptive error message.
func validateVFMTU(vfName, pfName string, requestedMTU, pfMTU int) error {
	if requestedMTU > pfMTU {
		return fmt.Errorf("requested MTU %d for SR-IOV VF %s exceeds parent PF %s MTU %d",
			requestedMTU, vfName, pfName, pfMTU)
	}
	return nil
}

// getRuleInfo lists all IP rules in the host network namespace and groups them
// by the route table they are associated with. It returns a map where keys are
// table IDs and values are slices of RuleConfig. Rules associated with the
// main or local tables are ignored.
func getRuleInfo(nlHandle nlwrap.Handle) (map[int][]apis.RuleConfig, error) {
	rulesByTable := make(map[int][]apis.RuleConfig)
	rules, err := nlHandle.RuleList(netlink.FAMILY_ALL)
	if err != nil {
		return nil, fmt.Errorf("failed to get ip rules: %w", err)
	}
	for _, rule := range rules {
		ruleCfg := apis.RuleConfig{
			Priority: rule.Priority,
			Table:    rule.Table,
		}
		if rule.Src != nil {
			ruleCfg.Source = rule.Src.String()
		}
		if rule.Dst != nil {
			ruleCfg.Destination = rule.Dst.String()
		}
		// Only care about rules with route tables associated, and exclude main and local tables.
		if rule.Table > 0 && rule.Table != unix.RT_TABLE_MAIN && rule.Table != unix.RT_TABLE_LOCAL {
			klog.V(5).Infof("Found rule %s for table %d", rule.String(), rule.Table)
			rulesByTable[rule.Table] = append(rulesByTable[rule.Table], ruleCfg)
		}
	}
	return rulesByTable, nil
}

// getRouteInfo retrieves all routes associated with a given network interface.
// It filters out routes that are not suitable for pod namespaces, such as
// routes in the local table. It returns the list of suitable routes and a set
// of the route table IDs to which they belong.
func getRouteInfo(nlHandle nlwrap.Handle, ifName string, link netlink.Link) ([]apis.RouteConfig, sets.Set[int], error) {
	routes := []apis.RouteConfig{}
	tables := sets.Set[int]{}
	filter := &netlink.Route{
		LinkIndex: link.Attrs().Index,
	}
	rl, err := nlHandle.RouteListFiltered(netlink.FAMILY_ALL, filter, netlink.RT_FILTER_OIF|netlink.RT_FILTER_TABLE)
	if err != nil {
		return nil, nil, fmt.Errorf("fail to get ip routes for interface %s : %w", ifName, err)
	}
	for _, route := range rl {
		routeCfg := apis.RouteConfig{}
		// routes need a destination
		if route.Dst == nil {
			klog.V(5).Infof("Skipping route %s for interface %s because it has no destination", route.String(), ifName)
			continue
		}
		// Do not copy routes from the local table because they are specific
		// to the host and the kernel will manage the local routing
		// table within the pod's network namespace.
		if route.Table == unix.RT_TABLE_LOCAL {
			klog.V(5).Infof("Skipping route %s for interface %s because it is in the local table", route.String(), ifName)
			continue
		}
		// Discard IPv6 link-local routes, but allow IPv4 link-local.
		if route.Dst.IP.To4() == nil {
			if route.Dst.IP.IsLinkLocalUnicast() {
				klog.V(5).Infof("Skipping IPv6 link-local route %s for interface %s", route.String(), ifName)
				continue
			}
			// Discard IPv6 proto=kernel routes
			if route.Protocol == unix.RTPROT_KERNEL {
				klog.V(5).Infof("Skipping IPv6 proto=kernel route %s for interface %s", route.String(), ifName)
				continue
			}
		}
		routeCfg.Destination = route.Dst.String()
		if route.Gw != nil {
			routeCfg.Gateway = route.Gw.String()
		}
		if route.Src != nil {
			routeCfg.Source = route.Src.String()
		}
		routeCfg.Scope = uint8(route.Scope)
		routeCfg.Table = route.Table
		routes = append(routes, routeCfg)
		// Collect table IDs for rules lookup later.
		if route.Table > 0 {
			klog.V(5).Infof("Found route table %d for interface %s", route.Table, ifName)
			tables.Insert(route.Table)
		}
	}
	return routes, tables, nil
}

// getDeviceNetworkConfig merges the user configuration with the cloud provider configuration and resolves the dynamic profile.
// User configuration always takes precedence in case of conflicts.
func (np *NetworkDriver) getDeviceNetworkConfig(device string, claimUID types.UID, userConf *apis.NetworkConfig) (*apis.NetworkConfig, error) {
	cloudConf, ok := np.netdb.GetDeviceConfig(device)
	if ok && cloudConf != nil {
		klog.V(4).Infof("Found cloud provider configuration for device %s: %#v", device, cloudConf)
	}
	mergedConf := apis.MergeNetworkConfig(userConf, cloudConf)

	if mergedConf.Profile != "" {
		profileConf, err := np.netdb.GetProfileConfig(device, claimUID, mergedConf)
		if err != nil {
			return nil, fmt.Errorf("failed to get profile config: %v", err)
		}
		mergedConf = apis.MergeNetworkConfig(mergedConf, profileConf)
	}
	return mergedConf, nil
}

// mergeDevices merges live host devices with database snapshots,
// giving precedence to live attributes and capacities where they overlap.
func mergeDevices(live, snapshot []resourceapi.Device) []resourceapi.Device {
	merged := make(map[string]resourceapi.Device)

	// 1. Initial Load from Host Scan (Live devices)
	for _, dev := range live {
		merged[dev.Name] = dev
	}

	// 2. Merge database snapshots
	for _, dev := range snapshot {
		liveDev, exists := merged[dev.Name]
		if !exists {
			// Device is completely missing from host (e.g. virtual interface in pod namespace).
			// Use the snapshot as-is.
			merged[dev.Name] = dev
			continue
		}

		// Device is in both (e.g. physical device). Merge them, giving precedence to liveDev.
		merged[dev.Name] = mergeDeviceStructs(liveDev, dev)
	}

	// 3. Convert map to sorted slice
	result := make([]resourceapi.Device, 0, len(merged))
	for _, dev := range merged {
		result = append(result, dev)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

func mergeDeviceStructs(live, snap resourceapi.Device) resourceapi.Device {
	merged := *live.DeepCopy()

	if merged.Attributes == nil {
		merged.Attributes = make(map[resourceapi.QualifiedName]resourceapi.DeviceAttribute)
	}
	for k, v := range snap.Attributes {
		if _, exists := merged.Attributes[k]; !exists {
			merged.Attributes[k] = *v.DeepCopy()
		}
	}

	if merged.Capacity == nil {
		merged.Capacity = make(map[resourceapi.QualifiedName]resourceapi.DeviceCapacity)
	}
	for k, v := range snap.Capacity {
		if _, exists := merged.Capacity[k]; !exists {
			merged.Capacity[k] = *v.DeepCopy()
		}
	}

	return merged
}
