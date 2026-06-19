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
	"strings"
	"time"

	"sigs.k8s.io/dranet/pkg/apis"
	"sigs.k8s.io/dranet/pkg/cnsclient"
	"sigs.k8s.io/dranet/pkg/filter"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/Mellanox/rdmamap"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"
	"sigs.k8s.io/dranet/internal/nlwrap"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	cnsCapSlots = "networking.azure.com/slots"

	// delegatedNICsPoolName is the ResourceSlice pool name used by the CNS
	// publisher to expose all delegated NIC devices for a node under a
	// single, well-known pool.
	delegatedNICsPoolName = "delegated-nics"

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
		case devices := <-np.netdb.GetResources(ctx):
			klog.V(3).Infof("Got %d devices from inventory: %s", len(devices), formatDeviceNames(devices, 15))
			devices = filter.FilterDevices(np.celProgram, devices)
			klog.V(3).Infof("After filtering, publishing %d devices in ResourceSlice(s): %s", len(devices), formatDeviceNames(devices, 15))

			np.publishResourcesPrometheusMetrics(devices)

			np.mu.Lock()
			np.inventoryPools = map[string]resourceslice.Pool{
				np.nodeName: {Slices: []resourceslice.Slice{{Devices: devices}}},
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

	// The ticker's first tick is at t=5s. Run one pass eagerly so the SwiftV2
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

	// Reconcile the SwiftV2 host masquerade exemptions for shared delegated NICs.
	// This is the durability backstop for the per-attach ensure: it re-asserts the
	// rule for shared NICs whose pods are already running (NRI does not replay
	// attach across a dranet restart) and after an external nat flush or node
	// reboot. Ensure-only and check-first, so steady state is read-only.
	reconcileSwiftV2NATExemptions(nicResources)

	pools := make(map[string]resourceslice.Pool, 1)
	allDevices := make([]resourceapi.Device, 0, len(nicResources))
	for i := range nicResources {
		nic := &nicResources[i]
		klog.V(3).Infof("CNS NIC[%d]: Name=%q InterfaceName=%q MacAddress=%q VMUniqueID=%q NetworkID=%q SubnetID=%q Capacity=%d",
			i, nic.Name, nic.InterfaceName, nic.MacAddress, nic.VMUniqueID, nic.NetworkID, nic.SubnetID, nic.Capacity)
		allDevices = append(allDevices, np.buildCNSDevices(nic)...)
	}
	if len(allDevices) > 0 {
		pools[delegatedNICsPoolName] = resourceslice.Pool{
			Slices: []resourceslice.Slice{{Devices: allDevices}},
		}
	}

	return np.publishCNSPools(ctx, pools)
}

// reconcileSwiftV2NATExemptions ensures the host masquerade-exemption rule is
// present for every shared delegated NIC currently reported by CNS.
//
// A NIC is shared-and-host-visible when CNS reports Capacity > 1 (the MAC has a
// NICNetworkConfig CRD) and a non-empty InterfaceName (the NIC is in the host
// namespace, not already moved into a pod netns). Dedicated NICs (Capacity == 1)
// and NICs already inside a pod netns (empty InterfaceName) are skipped: they
// have no host-namespace egress to protect.
//
// The pass is ensure-only and check-first (see ensureSwiftV2NATExemption), so it
// is cheap to run on every CNS poll and never prunes; a stale exemption left
// after a NIC leaves shared mode is inert.
func reconcileSwiftV2NATExemptions(nics []cnsclient.NICResource) {
	for i := range nics {
		nic := &nics[i]
		if nic.Capacity <= 1 || nic.InterfaceName == "" {
			continue
		}
		if err := ensureSwiftV2NATExemption(nic.InterfaceName); err != nil {
			klog.Errorf("SwiftV2: failed to reconcile NAT exemption for shared NIC %s (MAC %s): %v",
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
			klog.Infof("CNS ResourceSlice ADDED NIC: Name=%q InterfaceName=%q MAC=%q VMUniqueID=%q NetworkID=%q SubnetID=%q Capacity=%d",
				n.Name, n.InterfaceName, n.MacAddress, n.VMUniqueID, n.NetworkID, n.SubnetID, n.Capacity)
		}
	}

	// Detect removals
	for mac, n := range prevByMAC {
		if _, ok := currByMAC[mac]; !ok {
			changed = true
			klog.Infof("CNS ResourceSlice REMOVED NIC: Name=%q InterfaceName=%q MAC=%q VMUniqueID=%q NetworkID=%q SubnetID=%q Capacity=%d",
				n.Name, n.InterfaceName, n.MacAddress, n.VMUniqueID, n.NetworkID, n.SubnetID, n.Capacity)
		}
	}

	// Detect field changes on existing NICs
	for mac, c := range currByMAC {
		p, ok := prevByMAC[mac]
		if !ok {
			continue
		}
		var diffs []string
		if p.Name != c.Name {
			diffs = append(diffs, fmt.Sprintf("Name: %q -> %q", p.Name, c.Name))
		}
		if p.InterfaceName != c.InterfaceName {
			diffs = append(diffs, fmt.Sprintf("InterfaceName: %q -> %q", p.InterfaceName, c.InterfaceName))
		}
		if p.VMUniqueID != c.VMUniqueID {
			diffs = append(diffs, fmt.Sprintf("VMUniqueID: %q -> %q", p.VMUniqueID, c.VMUniqueID))
		}
		if p.NetworkID != c.NetworkID {
			diffs = append(diffs, fmt.Sprintf("NetworkID: %q -> %q", p.NetworkID, c.NetworkID))
		}
		if p.SubnetID != c.SubnetID {
			diffs = append(diffs, fmt.Sprintf("SubnetID: %q -> %q", p.SubnetID, c.SubnetID))
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
//   - attributes: networking.azure.com/nic (NIC name), networking.azure.com/subnet (full ARM URI),
//     networking.azure.com/subnetName (extracted name), networking.azure.com/networkID,
//     networking.azure.com/mac, networking.azure.com/shared
//   - capacity: networking.azure.com/slots with requestPolicy default=1, validRange min=1 max=1
func (np *NetworkDriver) buildCNSDevices(nic *cnsclient.NICResource) []resourceapi.Device {
	deviceName := cnsNICDeviceName(nic)

	// networking.azure.com/nic = human-readable interface name (e.g., "eth1").
	// Prefer InterfaceName, then Name, then fall back to MAC-derived deviceName.
	nicName := deviceName
	if nic.InterfaceName != "" {
		nicName = nic.InterfaceName
	} else if nic.Name != "" {
		nicName = nic.Name
	}
	// networking.azure.com/subnet = extracted subnet name from ARM URI (max 64 bytes for K8s attribute)
	subnet := extractSubnetName(nic.SubnetID)
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

	// Capacity defaults to 1 (pristine/placeholder); CNS sets blockSize when NICNC exists
	slots := int64(1)
	if nic.Capacity > 0 {
		slots = int64(nic.Capacity)
	}
	allowMulti := true
	defaultQty := resource.MustParse("1")
	minQty := resource.MustParse("1")
	maxQty := resource.MustParse("1")

	return []resourceapi.Device{
		{
			Name:                     deviceName,
			AllowMultipleAllocations: &allowMulti,
			Attributes:               attrs,
			Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
				cnsCapSlots: {
					Value: *resource.NewQuantity(slots, resource.DecimalSI),
					RequestPolicy: &resourceapi.CapacityRequestPolicy{
						Default: &defaultQty,
						ValidRange: &resourceapi.CapacityRequestPolicyRange{
							Min: &minQty,
							Max: &maxQty,
						},
					},
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

// publishCNSPools publishes CNS NIC resources via the dedicated CNS plugin
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

// prepareCNSResourceClaim is the fast path for Swift v2 CNS-managed claims.
// It reads NIC state from CNS (GetNICResources) to resolve the MAC address
// for each allocated NIC, queries CNS GetPodGoalState for per-pod networking
// config, and populates the SwiftV2PodConfigStore for downstream NRI use.
// All heavy DRA work (routes, rules, DHCP, ethtool, RDMA, eBPF) is skipped
// because the NRI hook (runPodSandboxSwiftV2) handles network plumbing from
// CNS goal state.
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

	// Read NIC state from CNS — this is the source of truth for NIC
	// metadata (MAC, subnet, capacity). If CNS is unreachable the pod
	// stays Pending and kubelet retries.
	if np.cnsClient == nil {
		return kubeletplugin.PrepareResult{
			Err: fmt.Errorf("CNS client not configured for claim %s/%s", claim.Namespace, claim.Name),
		}
	}
	nicResources, err := np.cnsClient.GetNICResources(ctx)
	if err != nil {
		klog.Errorf("prepareCNSResourceClaim: failed to get NIC resources from CNS for claim %s/%s: %v", claim.Namespace, claim.Name, err)
		return kubeletplugin.PrepareResult{
			Err: fmt.Errorf("failed to get NIC resources from CNS for claim %s/%s: %w", claim.Namespace, claim.Name, err),
		}
	}
	klog.Infof("prepareCNSResourceClaim: CNS returned %d NIC resources", len(nicResources))

	// Build primary lookup map from MAC address → NICResource (from CNS GetNICResources).
	// MAC addresses are the canonical unique identifier for each NIC.
	nicByMAC := make(map[string]cnsclient.NICResource, len(nicResources))
	deviceNameToMAC := make(map[string]string, len(nicResources))
	for i, nic := range nicResources {
		name := cnsNICDeviceName(&nic)
		mac := nic.MacAddress
		nicByMAC[mac] = nic
		deviceNameToMAC[name] = mac
		klog.Infof("prepareCNSResourceClaim: processing nicResources[%d]: cnsNICDeviceName=%q Name=%q InterfaceName=%q MAC=%s VMUniqueID=%q NetworkID=%q SubnetID=%q Capacity=%d",
			i, name, nic.Name, nic.InterfaceName, nic.MacAddress, nic.VMUniqueID, nic.NetworkID, nic.SubnetID, nic.Capacity)
	}

	// Build fallback deviceName→MAC map from ResourceSlices published by this driver on this node.
	// This is the authoritative source for device name → MAC mapping since it reflects what the
	// scheduler allocated against.
	rsDeviceNameToMAC := np.buildResourceSliceDeviceNameToMAC(ctx)

	// Log both maps for diagnostics.
	klog.Infof("prepareCNSResourceClaim: deviceNameToMAC (from CNS GetNICResources, %d entries):", len(deviceNameToMAC))
	for name, mac := range deviceNameToMAC {
		klog.Infof("  CNS map: deviceName=%q -> MAC=%q", name, mac)
	}
	klog.Infof("prepareCNSResourceClaim: rsDeviceNameToMAC (from ResourceSlices, %d entries):", len(rsDeviceNameToMAC))
	for name, mac := range rsDeviceNameToMAC {
		klog.Infof("  ResourceSlice map: deviceName=%q -> MAC=%q", name, mac)
	}

	var errorList []error
	goalStateByPod := map[types.UID][]cnsclient.PodIPInfo{}

	klog.Infof("prepareCNSResourceClaim: iterating %d device results for claim %s/%s",
		len(claim.Status.Allocation.Devices.Results), claim.Namespace, claim.Name)
	for _, result := range claim.Status.Allocation.Devices.Results {
		if result.Driver != np.cnsDriverName {
			klog.V(4).Infof("prepareCNSResourceClaim: skipping device %q (driver=%q, not CNS driver %q)",
				result.Device, result.Driver, np.cnsDriverName)
			continue
		}

		// Look up the NIC by resolving deviceName → MAC, then MAC → NICResource.
		// Try the primary CNS map first, then fall back to the ResourceSlice map.
		deviceName := result.Device
		mac, macFound := deviceNameToMAC[deviceName]
		if macFound {
			klog.Infof("prepareCNSResourceClaim: device %q -> MAC %q (from CNS map)", deviceName, mac)
		} else {
			mac, macFound = rsDeviceNameToMAC[deviceName]
			if macFound {
				klog.Infof("prepareCNSResourceClaim: device %q -> MAC %q (from ResourceSlice fallback map)", deviceName, mac)
			} else {
				klog.Errorf("prepareCNSResourceClaim: device %q not found in CNS map (%d entries) or ResourceSlice map (%d entries) for claim %s/%s",
					deviceName, len(deviceNameToMAC), len(rsDeviceNameToMAC), claim.Namespace, claim.Name)
				errorList = append(errorList, fmt.Errorf("CNS NIC resource not found for device %s in any map", deviceName))
				continue
			}
		}
		nic, ok := nicByMAC[mac]
		if !ok {
			klog.Errorf("prepareCNSResourceClaim: MAC %q (device %q) not found in nicByMAC (%d entries) for claim %s/%s",
				mac, deviceName, len(nicByMAC), claim.Namespace, claim.Name)
			errorList = append(errorList, fmt.Errorf("CNS NIC resource not found for MAC %s (device %s)", mac, deviceName))
			continue
		}
		deviceMAC := nic.MacAddress
		klog.Infof("prepareCNSResourceClaim: device %q resolved from CNS NIC state: MAC=%q SubnetID=%q",
			deviceName, deviceMAC, nic.SubnetID)

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

		// Query CNS GetPodGoalState for each pod consumer and store the
		// per-pod networking config in SwiftV2PodConfigStore for the NRI hook.
		// The NIC's host-underlay PrimaryIP (when CNS provides it via the
		// NICNetworkConfig CRD) is threaded through here so the prepare
		// hook can assign it to the parent NIC on the host.
		for _, pod := range podConsumers {
			klog.Infof("prepareCNSResourceClaim: populating SwiftV2 store for pod %s/%s UID=%s device=%q MAC=%q hostPrimaryIP=%q shareID=%q",
				pod.Namespace, pod.Name, pod.UID, deviceName, deviceMAC, nic.PrimaryIP, shareID)
			claimKey := types.NamespacedName{Namespace: claim.Namespace, Name: claim.Name}

			// Retry locally on transient "MTPNC not ready" errors so the pod can
			// start immediately once CNS finishes reconciling, instead of waiting
			// 60-90s for kubelet's next pod sync. Bounded by cnsPrepareRetryTimeout.
			var lastErr error
			deadline := time.Now().Add(cnsPrepareRetryTimeout)
		retryLoop:
			for attempt := 1; ; attempt++ {
				lastErr = np.populateSwiftV2StoreForDevice(ctx, pod, deviceName, deviceMAC, nic.PrimaryIP, claimKey, shareID, goalStateByPod)
				if lastErr == nil {
					if attempt > 1 {
						klog.Infof("prepareCNSResourceClaim: populateSwiftV2StoreForDevice succeeded on attempt %d for pod %s/%s device %q",
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
				klog.Errorf("prepareCNSResourceClaim: populateSwiftV2StoreForDevice failed for pod %s/%s device %q: %v",
					pod.Namespace, pod.Name, deviceName, lastErr)
				errorList = append(errorList, lastErr)
			}
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

// extractSubnetName extracts the subnet name from an Azure ARM resource URI.
// For example, given:
//
//	/subscriptions/.../resourceGroups/.../providers/Microsoft.Network/virtualNetworks/.../subnets/mySubnet
//
// it returns "mySubnet". If the input is not an ARM URI (no "/subnets/" segment),
// it returns the original input as-is (handles plain subnet names).
func extractSubnetName(subnetID string) string {
	if subnetID == "" {
		return ""
	}
	// Look for the "/subnets/" segment in ARM URIs (case-insensitive)
	lower := strings.ToLower(subnetID)
	const marker = "/subnets/"
	idx := strings.LastIndex(lower, marker)
	if idx < 0 {
		// Not an ARM URI — return the original value as the subnet name
		return subnetID
	}
	name := subnetID[idx+len(marker):]
	// Trim any trailing slash
	name = strings.TrimRight(name, "/")
	if name == "" {
		return subnetID
	}
	return name
}

// buildResourceSliceDeviceNameToMAC lists ResourceSlices for the CNS driver on this node
// and builds a deviceName → MAC map from the networking.azure.com/nic and networking.azure.com/mac
// device attributes. This serves as a fallback when the CNS GetNICResources device name
// convention doesn't match what the scheduler allocated against.
func (np *NetworkDriver) buildResourceSliceDeviceNameToMAC(ctx context.Context) map[string]string {
	result := make(map[string]string)
	if np.kubeClient == nil || np.cnsDriverName == "" {
		klog.Infof("buildResourceSliceDeviceNameToMAC: skipping (kubeClient=%v, cnsDriverName=%q)", np.kubeClient != nil, np.cnsDriverName)
		return result
	}

	slices, err := np.kubeClient.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("buildResourceSliceDeviceNameToMAC: failed to list ResourceSlices: %v", err)
		return result
	}

	for _, rs := range slices.Items {
		// Only look at slices for the CNS driver on this node.
		if rs.Spec.Driver != np.cnsDriverName {
			continue
		}
		if rs.Spec.NodeName == nil || *rs.Spec.NodeName != np.nodeName {
			continue
		}
		klog.Infof("buildResourceSliceDeviceNameToMAC: processing ResourceSlice %q (driver=%q, node=%q, pool=%q, %d devices)",
			rs.Name, rs.Spec.Driver, *rs.Spec.NodeName, rs.Spec.Pool.Name, len(rs.Spec.Devices))
		for _, dev := range rs.Spec.Devices {
			nicAttr, hasNIC := dev.Attributes[cnsAttrNIC]
			macAttr, hasMAC := dev.Attributes[cnsAttrMac]
			if !hasNIC || nicAttr.StringValue == nil || !hasMAC || macAttr.StringValue == nil {
				klog.V(4).Infof("buildResourceSliceDeviceNameToMAC: device %q missing nic/mac attributes, skipping", dev.Name)
				continue
			}
			deviceName := dev.Name
			mac := *macAttr.StringValue
			result[deviceName] = mac
			klog.Infof("buildResourceSliceDeviceNameToMAC: device %q -> MAC=%q (nic attr=%q)", deviceName, mac, *nicAttr.StringValue)
		}
	}
	klog.Infof("buildResourceSliceDeviceNameToMAC: built %d entries from ResourceSlices", len(result))
	return result
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
	// TODO: shared devices may allocate the same device to multiple pods, i.e. macvlan, ipvlan, ...
	podConsumers := getPodConsumers(claim)
	podUIDs := make([]types.UID, 0, len(podConsumers))
	for _, pod := range podConsumers {
		podUIDs = append(podUIDs, pod.UID)
	}
	if len(podUIDs) == 0 {
		klog.Infof("no pods allocated to claim %s/%s", claim.Namespace, claim.Name)
		return kubeletplugin.PrepareResult{}
	}

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

		// Get network configuration from the cloud provider (if any) and merge it with the user configuration.
		// User configuration always takes precedence in case of conflicts.
		cloudConf, ok := np.netdb.GetDeviceConfig(result.Device)
		if ok && cloudConf != nil {
			klog.V(4).Infof("Found cloud provider configuration for device %s: %#v", result.Device, cloudConf)
		}
		mergedConf := apis.MergeNetworkConfig(userConf, cloudConf)
		netconf := *mergedConf

		klog.V(4).Infof("PrepareResourceClaim %s/%s final Configuration %#v", claim.Namespace, claim.Name, netconf)
		podCfg := PodConfig{
			Claim: types.NamespacedName{
				Namespace: claim.Namespace,
				Name:      claim.Name,
			},
			NetworkInterfaceConfigInPod: netconf,
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
		podCfg.NetworkInterfaceConfigInHost.Interface.Name = ifName

		if podCfg.NetworkInterfaceConfigInPod.Interface.Name == "" {
			// If the interface name was not explicitly overridden, use the same
			// interface name within the pod's network namespace.
			podCfg.NetworkInterfaceConfigInPod.Interface.Name = ifName
		}

		// If DHCP is requested, do a DHCP request to gather the network parameters (IPs and Routes)
		// ... but we DO NOT apply them in the root namespace
		if podCfg.NetworkInterfaceConfigInPod.Interface.DHCP != nil && *podCfg.NetworkInterfaceConfigInPod.Interface.DHCP {
			klog.V(2).Infof("trying to get network configuration via DHCP")
			contextCancel, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			ip, routes, err := getDHCP(contextCancel, ifName)
			if err != nil {
				errorList = append(errorList, fmt.Errorf("fail to get configuration via DHCP for %s: %w", ifName, err))
			} else {
				podCfg.NetworkInterfaceConfigInPod.Interface.Addresses = []string{ip}
				podCfg.NetworkInterfaceConfigInPod.Routes = append(podCfg.NetworkInterfaceConfigInPod.Routes, routes...)
			}
		} else if len(podCfg.NetworkInterfaceConfigInPod.Interface.Addresses) == 0 {
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
					podCfg.NetworkInterfaceConfigInPod.Interface.Addresses = append(podCfg.NetworkInterfaceConfigInPod.Interface.Addresses, address.IPNet.String())
				}
			}
		}

		// Obtain the existing supported ethtool features and validate the config
		if podCfg.NetworkInterfaceConfigInPod.Ethtool != nil {
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
			for feature, value := range podCfg.NetworkInterfaceConfigInPod.Ethtool.Features {
				aliases := ifFeatures.Get(feature)
				if len(aliases) == 0 {
					errorList = append(errorList, fmt.Errorf("feature %s not supported by interface", feature))
					continue
				}
				for _, alias := range aliases {
					ethtoolFeatures[alias] = value
				}
			}
			podCfg.NetworkInterfaceConfigInPod.Ethtool.Features = ethtoolFeatures
		}

		// Obtain the routes and rules associated with the interface.
		routes, tables, err := getRouteInfo(nlHandle, ifName, link)
		if err != nil {
			errorList = append(errorList, err)
			continue
		}
		podCfg.NetworkInterfaceConfigInPod.Routes = append(podCfg.NetworkInterfaceConfigInPod.Routes, routes...)

		for _, table := range tables.UnsortedList() {
			if rules, ok := rulesByTable[table]; ok {
				klog.V(5).Infof("Adding %d rules for table %d associated with interface %s", len(rules), table, ifName)
				podCfg.NetworkInterfaceConfigInPod.Rules = append(podCfg.NetworkInterfaceConfigInPod.Rules, rules...)
				// Avoid adding the same rule twice
				delete(rulesByTable, table)
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
			podCfg.NetworkInterfaceConfigInPod.Neighbors = append(podCfg.NetworkInterfaceConfigInPod.Neighbors, neighCfg)
		}

		// Get RDMA configuration: link and char devices
		if rdmaDev, _ := rdmamap.GetRdmaDeviceForNetdevice(ifName); rdmaDev != "" {
			klog.V(2).Infof("RunPodSandbox processing RDMA device: %s", rdmaDev)
			podCfg.RDMADevice.LinkDev = rdmaDev
			// Obtain the char devices associated to the rdma device
			charDevices.Insert(rdmaCmPath)
			charDevices.Insert(rdmamap.GetRdmaCharDevices(rdmaDev)...)
			for _, devpath := range charDevices.UnsortedList() {
				dev, err := GetDeviceInfo(devpath)
				if err != nil {
					klog.Infof("fail to get device info for %s : %v", devpath, err)
				} else {
					podCfg.RDMADevice.DevChars = append(podCfg.RDMADevice.DevChars, dev)
				}
			}
		}

		// Remove the pinned programs before the NRI hooks since it
		// has to walk the entire bpf virtual filesystem and is slow
		// TODO: check if there is some other way to do this
		if podCfg.NetworkInterfaceConfigInPod.Interface.DisableEBPFPrograms != nil &&
			*podCfg.NetworkInterfaceConfigInPod.Interface.DisableEBPFPrograms {
			err := unpinBPFPrograms(ifName)
			if err != nil {
				klog.Infof("error unpinning ebpf programs for %s : %v", ifName, err)
			}
		}

		// TODO: support for multiple pods sharing the same device
		// we'll create the subinterface here
		for _, uid := range podUIDs {
			np.podConfigStore.Set(uid, result.Device, podCfg)
		}
		klog.V(4).Infof("Claim Resources for pods %v : %#v", podUIDs, podCfg)
	}

	if len(errorList) > 0 {
		klog.Infof("claim %s contain errors: %v", claim.UID, errors.Join(errorList...))
		return kubeletplugin.PrepareResult{
			Err: fmt.Errorf("claim %s contain errors: %w", claim.UID, errors.Join(errorList...)),
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
	np.podConfigStore.DeleteClaim(claim.NamespacedName)
	if np.swiftV2Store != nil {
		np.swiftV2Store.DeleteByClaim(claim.NamespacedName)
	}
	klog.V(2).Infof("UnprepareResourceClaim: cleaned up DRA and SwiftV2 stores for claim %s/%s", claim.Namespace, claim.Name)
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
