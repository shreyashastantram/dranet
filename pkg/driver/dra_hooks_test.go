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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/client_golang/prometheus/testutil"
	resourcev1 "k8s.io/api/resource/v1"
	k8sresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/dranet/pkg/apis"
	"sigs.k8s.io/dranet/pkg/cloudprovider"
	"sigs.k8s.io/dranet/pkg/cloudprovider/webhook"
	"sigs.k8s.io/dranet/pkg/cnsclient"
)

func TestPublishResourcesPrometheusMetrics(t *testing.T) {
	testCases := []struct {
		name          string
		devices       []resourcev1.Device
		expectedRdma  float64
		expectedTotal float64
	}{
		{
			name:          "No devices",
			devices:       []resourcev1.Device{},
			expectedRdma:  0,
			expectedTotal: 0,
		},
		{
			name: "Only RDMA devices",
			devices: []resourcev1.Device{
				{Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
					apis.AttrRDMA: {BoolValue: func() *bool { b := true; return &b }()},
				}},
			},
			expectedRdma:  1,
			expectedTotal: 1,
		},
		{
			name: "Only non-RDMA devices",
			devices: []resourcev1.Device{
				{Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
					apis.AttrRDMA: {BoolValue: func() *bool { b := false; return &b }()},
				}},
			},
			expectedRdma:  0,
			expectedTotal: 1,
		},
		{
			name: "Mixed RDMA and non-RDMA devices",
			devices: []resourcev1.Device{
				{Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
					apis.AttrRDMA: {BoolValue: func() *bool { b := true; return &b }()},
				}},
				{Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
					apis.AttrRDMA: {BoolValue: func() *bool { b := true; return &b }()},
				}},
				{Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
					apis.AttrRDMA: {BoolValue: func() *bool { b := false; return &b }()},
				}},
			},
			expectedRdma:  2,
			expectedTotal: 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			publishedDevicesTotal.Reset()
			np := &NetworkDriver{}
			np.publishResourcesPrometheusMetrics(tc.devices)

			if got := testutil.ToFloat64(publishedDevicesTotal.WithLabelValues("rdma")); got != tc.expectedRdma {
				t.Errorf("Expected %f for RDMA devices, got %f", tc.expectedRdma, got)
			}
			if got := testutil.ToFloat64(publishedDevicesTotal.WithLabelValues("total")); got != tc.expectedTotal {
				t.Errorf("Expected %f for Total devices, got %f", tc.expectedTotal, got)
			}
		})
	}
}

func TestPrepareResourceClaimsMetrics(t *testing.T) {
	ctx := context.Background()

	t.Run("Success Case", func(t *testing.T) {
		draPluginRequestsTotal.Reset()
		draPluginRequestsLatencySeconds.Reset()

		np := &NetworkDriver{}
		if _, err := np.PrepareResourceClaims(ctx, []*resourcev1.ResourceClaim{}); err != nil {
			t.Fatalf("PrepareResourceClaims failed: %v", err)
		}

		if got := testutil.ToFloat64(draPluginRequestsTotal.WithLabelValues(methodPrepareResourceClaims, statusSuccess)); got != float64(1) {
			t.Errorf("Expected 1 success, got %f", got)
		}
		if got := testutil.ToFloat64(draPluginRequestsTotal.WithLabelValues(methodPrepareResourceClaims, statusFailed)); got != float64(0) {
			t.Errorf("Expected 0 failures, got %f", got)
		}

		expected := `
			# HELP dranet_driver_dra_plugin_requests_latency_seconds DRA plugin request latency in seconds.
			# TYPE dranet_driver_dra_plugin_requests_latency_seconds histogram
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="PrepareResourceClaims",le="0.005"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="PrepareResourceClaims",le="0.01"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="PrepareResourceClaims",le="0.025"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="PrepareResourceClaims",le="0.05"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="PrepareResourceClaims",le="0.1"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="PrepareResourceClaims",le="0.25"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="PrepareResourceClaims",le="0.5"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="PrepareResourceClaims",le="1"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="PrepareResourceClaims",le="2.5"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="PrepareResourceClaims",le="5"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="PrepareResourceClaims",le="10"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="PrepareResourceClaims",le="+Inf"} 1
		`
		if err := testutil.CollectAndCompare(draPluginRequestsLatencySeconds, strings.NewReader(expected), "dranet_driver_dra_plugin_requests_latency_seconds_bucket"); err != nil {
			t.Fatalf("CollectAndCompare failed: %v", err)
		}
	})

	t.Run("Failure Case", func(t *testing.T) {
		draPluginRequestsTotal.Reset()
		draPluginRequestsLatencySeconds.Reset()

		np := &NetworkDriver{
			netdb:         newFakeInventoryDB(),
			driverName:    "test.driver",
			eventRecorder: record.NewFakeRecorder(100),
		}

		claims := []*resourcev1.ResourceClaim{
			{
				ObjectMeta: metav1.ObjectMeta{UID: "claim-uid-1"},
				Status: resourcev1.ResourceClaimStatus{
					ReservedFor: []resourcev1.ResourceClaimConsumerReference{
						{APIGroup: "", Resource: "pods", Name: "test-pod", UID: "pod-uid-1"},
					},
					Allocation: &resourcev1.AllocationResult{
						Devices: resourcev1.DeviceAllocationResult{
							Results: []resourcev1.DeviceRequestAllocationResult{
								{Driver: "test.driver", Device: "device-does-not-exist"},
							},
						},
					},
				},
			},
		}

		res, err := np.PrepareResourceClaims(ctx, claims)
		if err != nil {
			t.Fatalf("PrepareResourceClaims failed: %v", err)
		}
		if res["claim-uid-1"].Err == nil {
			t.Errorf("Expected an error for claim-uid-1, but got none")
		}

		if got := testutil.ToFloat64(draPluginRequestsTotal.WithLabelValues(methodPrepareResourceClaims, statusSuccess)); got != float64(0) {
			t.Errorf("Expected 0 successes, got %f", got)
		}
		if got := testutil.ToFloat64(draPluginRequestsTotal.WithLabelValues(methodPrepareResourceClaims, statusFailed)); got != float64(1) {
			t.Errorf("Expected 1 failure, got %f", got)
		}

		if count := testutil.CollectAndCount(draPluginRequestsLatencySeconds); count != 1 {
			t.Errorf("Expected 1 latency metric, got %d", count)
		}
	})
}

func TestUnprepareResourceClaimsMetrics(t *testing.T) {
	ctx := context.Background()

	t.Run("Success Case", func(t *testing.T) {
		draPluginRequestsTotal.Reset()
		draPluginRequestsLatencySeconds.Reset()

		np := &NetworkDriver{
			podConfigStore: mustNewPodConfigStore(),
		}
		claimName := types.NamespacedName{Name: "test-claim", Namespace: "test-ns"}
		np.podConfigStore.SetDeviceConfig("pod-uid-1", "device-a", DeviceConfig{Claim: claimName})

		claims := []kubeletplugin.NamespacedObject{
			{NamespacedName: claimName, UID: "claim-uid-1"},
		}

		if _, err := np.UnprepareResourceClaims(ctx, claims); err != nil {
			t.Fatalf("UnprepareResourceClaims failed: %v", err)
		}

		// Verify the claim was removed from the store
		if _, ok := np.podConfigStore.GetPodConfig("pod-uid-1"); ok {
			t.Errorf("Pod config should have been removed, but was found")
		}

		if got := testutil.ToFloat64(draPluginRequestsTotal.WithLabelValues(methodUnprepareResourceClaims, statusSuccess)); got != float64(1) {
			t.Errorf("Expected 1 success, got %f", got)
		}
		if got := testutil.ToFloat64(draPluginRequestsTotal.WithLabelValues(methodUnprepareResourceClaims, statusFailed)); got != float64(0) {
			t.Errorf("Expected 0 failures, got %f", got)
		}

		expected := `
			# HELP dranet_driver_dra_plugin_requests_latency_seconds DRA plugin request latency in seconds.
			# TYPE dranet_driver_dra_plugin_requests_latency_seconds histogram
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="UnprepareResourceClaims",le="0.005"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="UnprepareResourceClaims",le="0.01"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="UnprepareResourceClaims",le="0.025"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="UnprepareResourceClaims",le="0.05"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="UnprepareResourceClaims",le="0.1"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="UnprepareResourceClaims",le="0.25"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="UnprepareResourceClaims",le="0.5"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="UnprepareResourceClaims",le="1"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="UnprepareResourceClaims",le="2.5"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="UnprepareResourceClaims",le="5"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="UnprepareResourceClaims",le="10"} 1
			dranet_driver_dra_plugin_requests_latency_seconds_bucket{method="UnprepareResourceClaims",le="+Inf"} 1
		`
		if err := testutil.CollectAndCompare(draPluginRequestsLatencySeconds, strings.NewReader(expected), "dranet_driver_dra_plugin_requests_latency_seconds_bucket"); err != nil {
			t.Fatalf("CollectAndCompare failed: %v", err)
		}
	})
}

func TestUnprepareResourceClaim_SecondaryNICDeleteFailure(t *testing.T) {
	deleteErr := errors.New("checkpoint delete failed")
	checkpointer := &failingSecondaryNICCheckpointer{}
	secondaryStore, err := newSecondaryNICPodConfigStore(checkpointer)
	if err != nil {
		t.Fatal(err)
	}
	claim := types.NamespacedName{Namespace: "ns", Name: "claim"}
	if err := secondaryStore.Set("pod-1", "device", SecondaryNICPodConfig{}, claim); err != nil {
		t.Fatal(err)
	}
	checkpointer.deleteErr = deleteErr
	np := &NetworkDriver{
		podConfigStore:    mustNewPodConfigStore(),
		secondaryNICStore: secondaryStore,
	}

	err = np.unprepareResourceClaim(context.Background(), kubeletplugin.NamespacedObject{NamespacedName: claim, UID: "claim-uid"})
	if !errors.Is(err, deleteErr) {
		t.Fatalf("unprepare error = %v, want %v", err, deleteErr)
	}
	if _, found := secondaryStore.Get("pod-1"); !found {
		t.Fatal("failed unprepare removed secondary NIC memory state")
	}
}

func TestClaimPrepareFailedEvent(t *testing.T) {
	ctx := context.Background()
	fakeRecorder := record.NewFakeRecorder(10)

	np := &NetworkDriver{
		netdb:          newFakeInventoryDB(),
		driverName:     "test.driver",
		eventRecorder:  fakeRecorder,
		podConfigStore: mustNewPodConfigStore(),
	}

	claims := []*resourcev1.ResourceClaim{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-claim",
				Namespace: "default",
				UID:       "claim-uid-1",
			},
			Status: resourcev1.ResourceClaimStatus{
				ReservedFor: []resourcev1.ResourceClaimConsumerReference{
					{APIGroup: "", Resource: "pods", Name: "test-pod", UID: "pod-uid-1"},
				},
				Allocation: &resourcev1.AllocationResult{
					Devices: resourcev1.DeviceAllocationResult{
						Results: []resourcev1.DeviceRequestAllocationResult{
							{Driver: "test.driver", Device: "device-does-not-exist"},
						},
					},
				},
			},
		},
	}

	res, err := np.PrepareResourceClaims(ctx, claims)
	if err != nil {
		t.Fatalf("PrepareResourceClaims returned unexpected error: %v", err)
	}
	if res["claim-uid-1"].Err == nil {
		t.Fatal("expected per-claim error, got none")
	}

	select {
	case event := <-fakeRecorder.Events:
		if !strings.Contains(event, "ClaimPrepareFailed") {
			t.Errorf("expected ClaimPrepareFailed event, got: %s", event)
		}
	default:
		t.Error("expected a ClaimPrepareFailed event to be emitted, but none was received")
	}
}

func TestPublishResourcesMetrics(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	fakeDraPlugin := newFakePluginHelper()
	fakeNetDB := newFakeInventoryDB()

	np := &NetworkDriver{
		draPlugin: fakeDraPlugin,
		netdb:     fakeNetDB,
		nodeName:  "test-node",
	}

	go np.PublishResources(ctx)

	t.Run("Success", func(t *testing.T) {
		lastPublishedTime.Set(0)
		fakeNetDB.resources <- []resourcev1.Device{}
		<-fakeDraPlugin.publishCalled

		if testutil.ToFloat64(lastPublishedTime) == 0 {
			t.Errorf("lastPublishedTime should have been updated, but it is 0")
		}
	})

	t.Run("Failure", func(t *testing.T) {
		lastPublishedTime.Set(0)
		fakeDraPlugin.publishErr = fmt.Errorf("mock publish error")
		fakeNetDB.resources <- []resourcev1.Device{}
		<-fakeDraPlugin.publishCalled

		if testutil.ToFloat64(lastPublishedTime) != 0 {
			t.Errorf("lastPublishedTime should not have been updated, but it is %f", testutil.ToFloat64(lastPublishedTime))
		}
	})
}

func TestPublishCNSResourcesPublishesSubnetGUID(t *testing.T) {
	const (
		subnetGUID = "369682de-a9c0-4f95-bdb0-5b033e2c9360"
		subnetName = "workload-subnet"
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/network/nicresources" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(cnsclient.GetNICResourcesResponse{
			Response: cnsclient.Response{ReturnCode: 0},
			NICResources: []cnsclient.NICResource{{
				MacAddress: "aa:bb:cc:dd:ee:01",
				SubnetGUID: subnetGUID,
				SubnetName: subnetName,
				Capacity:   1,
			}},
		})
	}))
	defer server.Close()

	client, err := cnsclient.New(server.URL, 0)
	if err != nil {
		t.Fatalf("failed to create CNS client: %v", err)
	}
	plugin := newFakePluginHelper()
	np := &NetworkDriver{cnsClient: client, cnsPlugin: plugin}

	if err := np.publishCNSResources(context.Background()); err != nil {
		t.Fatalf("publishCNSResources: %v", err)
	}

	devices := plugin.publishedResources.Pools[secondaryNICsPoolName].Slices[0].Devices
	subnet := devices[0].Attributes[cnsAttrSubnet].StringValue
	if subnet == nil || *subnet != subnetGUID {
		t.Fatalf("published subnet attribute = %v, want GUID %q (not name %q)", subnet, subnetGUID, subnetName)
	}
}

func TestValidateVFMTU(t *testing.T) {
	testCases := []struct {
		name         string
		requestedMTU int
		pfMTU        int
		wantErr      bool
	}{
		{
			name:         "requested MTU below PF MTU is allowed",
			requestedMTU: 1500,
			pfMTU:        9000,
			wantErr:      false,
		},
		{
			name:         "requested MTU equal to PF MTU is allowed",
			requestedMTU: 9000,
			pfMTU:        9000,
			wantErr:      false,
		},
		{
			name:         "requested MTU above PF MTU is rejected",
			requestedMTU: 9000,
			pfMTU:        1500,
			wantErr:      true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateVFMTU("eth1", "eth0", tc.requestedMTU, tc.pfMTU)
			if (err != nil) != tc.wantErr {
				t.Errorf("validateVFMTU() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestDynamicProfiles(t *testing.T) {
	ctx := context.Background()

	t.Run("Success Case", func(t *testing.T) {
		fakeDB := newFakeInventoryDB()
		fakeDB.GetProfileConfigFunc = func(deviceName string, claimUID types.UID, config *apis.NetworkConfig) (*apis.NetworkConfig, error) {
			return &apis.NetworkConfig{
				Interface: apis.InterfaceConfig{
					Addresses: []string{"10.0.0.1/24"},
				},
			}, nil
		}
		fakeDB.GetDeviceConfigFunc = func(deviceName string) (*apis.NetworkConfig, bool) {
			return &apis.NetworkConfig{Profile: "my-profile"}, true
		}
		fakeDB.GetNetInterfaceNameFunc = func(deviceName string) (string, error) {
			return "eth0", nil
		}
		fakeDB.IsIBOnlyDeviceFunc = func(deviceName string) bool {
			return true
		}

		np := &NetworkDriver{
			netdb:          fakeDB,
			driverName:     "test.driver",
			podConfigStore: mustNewPodConfigStore(),
		}

		claims := []*resourcev1.ResourceClaim{
			{
				ObjectMeta: metav1.ObjectMeta{UID: "claim-uid-1", Namespace: "default", Name: "claim1"},
				Status: resourcev1.ResourceClaimStatus{
					ReservedFor: []resourcev1.ResourceClaimConsumerReference{
						{APIGroup: "", Resource: "pods", Name: "test-pod", UID: "pod-uid-1"},
					},
					Allocation: &resourcev1.AllocationResult{
						Devices: resourcev1.DeviceAllocationResult{
							Results: []resourcev1.DeviceRequestAllocationResult{
								{Driver: "test.driver", Device: "device-1", Request: "req-1"},
							},
							Config: []resourcev1.DeviceAllocationConfiguration{},
						},
					},
				},
			},
		}

		res, err := np.PrepareResourceClaims(ctx, claims)
		if err != nil {
			t.Fatalf("PrepareResourceClaims failed: %v", err)
		}
		if res["claim-uid-1"].Err != nil {
			t.Fatalf("Expected no error, got %v", res["claim-uid-1"].Err)
		}

		// Verify merge success
		podCfg, ok := np.podConfigStore.GetPodConfig("pod-uid-1")
		if !ok {
			t.Fatalf("Expected pod config to be stored")
		}
		devCfg := podCfg.DeviceConfigs["device-1"]
		if len(devCfg.NetworkInterfaceConfigInPod.Interface.Addresses) == 0 || devCfg.NetworkInterfaceConfigInPod.Interface.Addresses[0] != "10.0.0.1/24" {
			t.Errorf("Expected address 10.0.0.1/24 to be merged into pod config, got %v", devCfg.NetworkInterfaceConfigInPod.Interface.Addresses)
		}
	})

	t.Run("Unsupported Provider Case", func(t *testing.T) {
		fakeDB := newFakeInventoryDB()
		fakeDB.GetProfileConfigFunc = func(deviceName string, claimUID types.UID, config *apis.NetworkConfig) (*apis.NetworkConfig, error) {
			return nil, fmt.Errorf("current cloud provider does not support dynamic profiles")
		}
		fakeDB.GetDeviceConfigFunc = func(deviceName string) (*apis.NetworkConfig, bool) {
			return &apis.NetworkConfig{Profile: "my-profile"}, true
		}
		fakeDB.GetNetInterfaceNameFunc = func(deviceName string) (string, error) {
			return "eth0", nil
		}
		fakeDB.IsIBOnlyDeviceFunc = func(deviceName string) bool {
			return true
		}

		np := &NetworkDriver{
			netdb:          fakeDB,
			driverName:     "test.driver",
			podConfigStore: mustNewPodConfigStore(),
			eventRecorder:  record.NewFakeRecorder(100),
		}

		claims := []*resourcev1.ResourceClaim{
			{
				ObjectMeta: metav1.ObjectMeta{UID: "claim-uid-unsupported", Namespace: "default", Name: "claim-unsup"},
				Status: resourcev1.ResourceClaimStatus{
					ReservedFor: []resourcev1.ResourceClaimConsumerReference{
						{APIGroup: "", Resource: "pods", Name: "test-pod", UID: "pod-uid-unsupported"},
					},
					Allocation: &resourcev1.AllocationResult{
						Devices: resourcev1.DeviceAllocationResult{
							Results: []resourcev1.DeviceRequestAllocationResult{
								{Driver: "test.driver", Device: "device-1", Request: "req-1"},
							},
							Config: []resourcev1.DeviceAllocationConfiguration{},
						},
					},
				},
			},
		}

		res, err := np.PrepareResourceClaims(ctx, claims)
		if err != nil {
			t.Fatalf("PrepareResourceClaims failed: %v", err)
		}
		if res["claim-uid-unsupported"].Err == nil || !strings.Contains(res["claim-uid-unsupported"].Err.Error(), "does not support dynamic profiles") {
			t.Fatalf("Expected unsupported profile error, got %v", res["claim-uid-unsupported"].Err)
		}
	})

	t.Run("Allocation Failure Case", func(t *testing.T) {
		fakeDB := newFakeInventoryDB()
		fakeDB.GetProfileConfigFunc = func(deviceName string, claimUID types.UID, config *apis.NetworkConfig) (*apis.NetworkConfig, error) {
			return nil, fmt.Errorf("ipam allocation failed")
		}
		fakeDB.GetDeviceConfigFunc = func(deviceName string) (*apis.NetworkConfig, bool) {
			return &apis.NetworkConfig{Profile: "my-profile"}, true
		}
		fakeDB.GetNetInterfaceNameFunc = func(deviceName string) (string, error) {
			return "eth0", nil
		}
		fakeDB.IsIBOnlyDeviceFunc = func(deviceName string) bool {
			return true
		}

		np := &NetworkDriver{
			netdb:          fakeDB,
			driverName:     "test.driver",
			podConfigStore: mustNewPodConfigStore(),
			eventRecorder:  record.NewFakeRecorder(100),
		}

		claims := []*resourcev1.ResourceClaim{
			{
				ObjectMeta: metav1.ObjectMeta{UID: "claim-uid-fail", Namespace: "default", Name: "claim-fail"},
				Status: resourcev1.ResourceClaimStatus{
					ReservedFor: []resourcev1.ResourceClaimConsumerReference{
						{APIGroup: "", Resource: "pods", Name: "test-pod", UID: "pod-uid-fail"},
					},
					Allocation: &resourcev1.AllocationResult{
						Devices: resourcev1.DeviceAllocationResult{
							Results: []resourcev1.DeviceRequestAllocationResult{
								{Driver: "test.driver", Device: "device-1", Request: "req-1"},
							},
							Config: []resourcev1.DeviceAllocationConfiguration{},
						},
					},
				},
			},
		}

		res, err := np.PrepareResourceClaims(ctx, claims)
		if err != nil {
			t.Fatalf("PrepareResourceClaims failed: %v", err)
		}
		if res["claim-uid-fail"].Err == nil || !strings.Contains(res["claim-uid-fail"].Err.Error(), "ipam allocation failed") {
			t.Fatalf("Expected ipam allocation failed error, got %v", res["claim-uid-fail"].Err)
		}
	})

	t.Run("Teardown Success Case", func(t *testing.T) {
		released := false
		fakeDB := newFakeInventoryDB()
		fakeDB.ReleaseProfileConfigFunc = func(deviceName string, claimUID types.UID, config *apis.NetworkConfig) error {
			released = true
			if config.Profile != "my-profile" {
				t.Errorf("Expected profile 'my-profile', got %v", config.Profile)
			}
			if claimUID != "claim-uid-td" {
				t.Errorf("Expected claimUID 'claim-uid-td', got %v", claimUID)
			}
			return nil
		}

		np := &NetworkDriver{
			netdb:          fakeDB,
			driverName:     "test.driver",
			podConfigStore: mustNewPodConfigStore(),
		}

		claimName := types.NamespacedName{Namespace: "default", Name: "claim-td"}
		// Inject a profile in pod config store
		np.podConfigStore.SetDeviceConfig("pod-uid-td", "device-1", DeviceConfig{
			Claim:                       claimName,
			NetworkInterfaceConfigInPod: apis.NetworkConfig{Profile: "my-profile"},
		})

		claims := []kubeletplugin.NamespacedObject{
			{NamespacedName: claimName, UID: "claim-uid-td"},
		}

		_, err := np.UnprepareResourceClaims(ctx, claims)
		if err != nil {
			t.Fatalf("UnprepareResourceClaims failed: %v", err)
		}

		if !released {
			t.Errorf("Expected releaseProfileConfigFunc to be called")
		}
	})

	t.Run("Early Store Profile Release on Subsequent Failure", func(t *testing.T) {
		fakeDB := newFakeInventoryDB()
		fakeDB.GetProfileConfigFunc = func(deviceName string, claimUID types.UID, config *apis.NetworkConfig) (*apis.NetworkConfig, error) {
			return &apis.NetworkConfig{
				Interface: apis.InterfaceConfig{
					Addresses: []string{"10.0.0.1/24"},
				},
			}, nil
		}
		fakeDB.GetDeviceConfigFunc = func(deviceName string) (*apis.NetworkConfig, bool) {
			return &apis.NetworkConfig{Profile: "my-profile"}, true
		}
		// Cause a failure AFTER GetProfileConfig
		fakeDB.GetNetInterfaceNameFunc = func(deviceName string) (string, error) {
			return "", fmt.Errorf("simulated failure getting interface name")
		}
		fakeDB.IsIBOnlyDeviceFunc = func(deviceName string) bool {
			return false
		}

		np := &NetworkDriver{
			netdb:          fakeDB,
			driverName:     "test.driver",
			podConfigStore: mustNewPodConfigStore(),
			eventRecorder:  record.NewFakeRecorder(100),
		}

		claims := []*resourcev1.ResourceClaim{
			{
				ObjectMeta: metav1.ObjectMeta{UID: "claim-uid-leak", Namespace: "default", Name: "claim-leak"},
				Status: resourcev1.ResourceClaimStatus{
					ReservedFor: []resourcev1.ResourceClaimConsumerReference{
						{APIGroup: "", Resource: "pods", Name: "test-pod", UID: "pod-uid-leak"},
					},
					Allocation: &resourcev1.AllocationResult{
						Devices: resourcev1.DeviceAllocationResult{
							Results: []resourcev1.DeviceRequestAllocationResult{
								{Driver: "test.driver", Device: "device-1", Request: "req-1"},
							},
							Config: []resourcev1.DeviceAllocationConfiguration{},
						},
					},
				},
			},
		}

		res, err := np.PrepareResourceClaims(ctx, claims)
		if err != nil {
			t.Fatalf("PrepareResourceClaims failed: %v", err)
		}
		if res["claim-uid-leak"].Err == nil || !strings.Contains(res["claim-uid-leak"].Err.Error(), "simulated failure") {
			t.Fatalf("Expected simulated failure, got %v", res["claim-uid-leak"].Err)
		}

		// Verify the early device config was stored so Kubelet's call to UnprepareResourceClaims will clean it up
		podCfg, ok := np.podConfigStore.GetPodConfig("pod-uid-leak")
		if !ok {
			t.Fatalf("Expected pod config to be stored early")
		}
		devCfg := podCfg.DeviceConfigs["device-1"]
		if devCfg.NetworkInterfaceConfigInPod.Profile != "my-profile" {
			t.Errorf("Expected profile 'my-profile' to be saved for cleanup, got '%v'", devCfg.NetworkInterfaceConfigInPod.Profile)
		}
	})
}

func TestGetDeviceNetworkConfigWithWebhook(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name              string
		userConf          *apis.NetworkConfig
		cloudConfResponse *apis.NetworkConfig
		profileResponse   *apis.NetworkConfig
		profileStatusCode int
		expectedError     bool
		expectedAddresses []string
		expectedMTU       int32
		expectedProfile   string
	}{
		{
			name:              "No configurations provided",
			userConf:          &apis.NetworkConfig{},
			cloudConfResponse: nil,
			profileResponse:   nil,
			expectedError:     false,
		},
		{
			name: "User configuration only",
			userConf: &apis.NetworkConfig{
				Interface: apis.InterfaceConfig{MTU: ptr.To[int32](1400)},
			},
			expectedMTU:   1400,
			expectedError: false,
		},
		{
			name:     "Cloud configuration only",
			userConf: &apis.NetworkConfig{},
			cloudConfResponse: &apis.NetworkConfig{
				Interface: apis.InterfaceConfig{MTU: ptr.To[int32](1500)},
			},
			expectedMTU:   1500,
			expectedError: false,
		},
		{
			name: "User configuration overrides Cloud configuration",
			userConf: &apis.NetworkConfig{
				Interface: apis.InterfaceConfig{MTU: ptr.To[int32](1400)},
			},
			cloudConfResponse: &apis.NetworkConfig{
				Interface: apis.InterfaceConfig{MTU: ptr.To[int32](1500)},
			},
			expectedMTU:   1400,
			expectedError: false,
		},
		{
			name:     "Profile configuration adds IP address",
			userConf: &apis.NetworkConfig{},
			cloudConfResponse: &apis.NetworkConfig{
				Profile: "cloud-profile",
			},
			profileResponse: &apis.NetworkConfig{
				Interface: apis.InterfaceConfig{
					Addresses: []string{"192.168.1.10/24"},
				},
			},
			profileStatusCode: http.StatusOK,
			expectedAddresses: []string{"192.168.1.10/24"},
			expectedProfile:   "cloud-profile",
			expectedError:     false,
		},
		{
			name: "User configuration overrides Profile configuration",
			userConf: &apis.NetworkConfig{
				Interface: apis.InterfaceConfig{MTU: ptr.To[int32](1400)},
			},
			cloudConfResponse: &apis.NetworkConfig{
				Profile: "cloud-profile",
			},
			profileResponse: &apis.NetworkConfig{
				Interface: apis.InterfaceConfig{
					MTU:       ptr.To[int32](1500),
					Addresses: []string{"192.168.1.10/24"},
				},
			},
			profileStatusCode: http.StatusOK,
			expectedAddresses: []string{"192.168.1.10/24"},
			expectedMTU:       1400,
			expectedProfile:   "cloud-profile",
			expectedError:     false,
		},
		{
			name:     "Webhook blocks Profile configuration",
			userConf: &apis.NetworkConfig{},
			cloudConfResponse: &apis.NetworkConfig{
				Profile: "cloud-profile",
			},
			profileStatusCode: http.StatusForbidden,
			expectedError:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == webhook.PathHealth {
					json.NewEncoder(w).Encode(webhook.Capabilities{CloudProvider: true, ProfileProvider: true})
					return
				}
				if r.URL.Path == webhook.PathGetDeviceAttributes {
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(`{}`))
					return
				}
				if r.URL.Path == webhook.PathGetDeviceConfig {
					if tc.cloudConfResponse != nil {
						w.WriteHeader(http.StatusOK)
						json.NewEncoder(w).Encode(tc.cloudConfResponse)
					} else {
						w.WriteHeader(http.StatusNotFound)
					}
					return
				}
				if r.URL.Path == webhook.PathGetProfileConfig {
					if tc.profileStatusCode != 0 && tc.profileStatusCode != http.StatusOK {
						w.WriteHeader(tc.profileStatusCode)
						w.Write([]byte(`{"error": "forbidden"}`))
						return
					}
					if tc.profileResponse != nil {
						w.WriteHeader(http.StatusOK)
						json.NewEncoder(w).Encode(tc.profileResponse)
					} else {
						w.WriteHeader(http.StatusNotFound)
					}
					return
				}
				w.WriteHeader(http.StatusNotFound)
			}))
			defer srv.Close()

			provider, err := webhook.NewWebhookProvider(ctx, srv.URL)
			if err != nil {
				t.Fatalf("Failed to create webhook provider: %v", err)
			}

			fakeDB := newFakeInventoryDB()
			fakeDB.GetProfileConfigFunc = func(deviceName string, claimUID types.UID, config *apis.NetworkConfig) (*apis.NetworkConfig, error) {
				id := cloudprovider.DeviceIdentifiers{Name: deviceName}
				return provider.GetProfileConfig(id, claimUID, config)
			}
			fakeDB.GetDeviceConfigFunc = func(deviceName string) (*apis.NetworkConfig, bool) {
				id := cloudprovider.DeviceIdentifiers{Name: deviceName}
				conf := provider.GetDeviceConfig(id)
				return conf, conf != nil
			}

			np := &NetworkDriver{
				netdb:          fakeDB,
				driverName:     "test.driver",
				podConfigStore: mustNewPodConfigStore(),
			}

			mergedConf, err := np.getDeviceNetworkConfig("device-1", "claim-uid-1", tc.userConf)

			if tc.expectedError {
				if err == nil {
					t.Fatalf("Expected an error but got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if mergedConf == nil {
				t.Fatalf("Merged configuration is nil")
			}

			if tc.expectedMTU > 0 {
				if mergedConf.Interface.MTU == nil || *mergedConf.Interface.MTU != tc.expectedMTU {
					t.Errorf("Expected MTU %d, got %v", tc.expectedMTU, mergedConf.Interface.MTU)
				}
			} else if mergedConf.Interface.MTU != nil {
				t.Errorf("Expected nil MTU, got %d", *mergedConf.Interface.MTU)
			}

			if len(tc.expectedAddresses) > 0 {
				if len(mergedConf.Interface.Addresses) != len(tc.expectedAddresses) {
					t.Errorf("Expected addresses %v, got %v", tc.expectedAddresses, mergedConf.Interface.Addresses)
				} else {
					for i, addr := range tc.expectedAddresses {
						if mergedConf.Interface.Addresses[i] != addr {
							t.Errorf("Expected address %v, got %v", addr, mergedConf.Interface.Addresses[i])
						}
					}
				}
			} else if len(mergedConf.Interface.Addresses) > 0 {
				t.Errorf("Expected no addresses, got %v", mergedConf.Interface.Addresses)
			}

			if tc.expectedProfile != "" {
				if mergedConf.Profile != tc.expectedProfile {
					t.Errorf("Expected profile %s, got %s", tc.expectedProfile, mergedConf.Profile)
				}
			} else if mergedConf.Profile != "" {
				t.Errorf("Expected empty profile, got %s", mergedConf.Profile)
			}
		})
	}
}

func TestMergeDevices(t *testing.T) {
	stringAttr := func(val string) resourcev1.DeviceAttribute {
		return resourcev1.DeviceAttribute{
			StringValue: &val,
		}
	}

	qtyCap := func(val string) resourcev1.DeviceCapacity {
		return resourcev1.DeviceCapacity{
			Value: k8sresource.MustParse(val),
		}
	}

	pciDev := resourcev1.Device{
		Name: "0000:c0:14.0",
		Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
			resourcev1.QualifiedName(apis.AttrPCIAddress): stringAttr("0000:c0:14.0"),
		},
	}

	pciDevSnapshot := resourcev1.Device{
		Name: "0000:c0:14.0",
		Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
			resourcev1.QualifiedName(apis.AttrPCIAddress):    stringAttr("0000:c0:14.0"),
			resourcev1.QualifiedName(apis.AttrInterfaceName): stringAttr("eth1"),
			resourcev1.QualifiedName(apis.AttrMTU):           stringAttr("1500"),
		},
	}

	tests := []struct {
		name     string
		live     []resourcev1.Device
		snapshot []resourcev1.Device
		expected []resourcev1.Device
	}{
		{
			name:     "Only live devices returned",
			live:     []resourcev1.Device{pciDev},
			snapshot: nil,
			expected: []resourcev1.Device{pciDev},
		},
		{
			name:     "Snapshot device returned when not live",
			live:     nil,
			snapshot: []resourcev1.Device{pciDevSnapshot},
			expected: []resourcev1.Device{pciDevSnapshot},
		},
		{
			name: "Live device attribute takes precedence over snapshot",
			live: []resourcev1.Device{{
				Name: "0000:c0:14.0",
				Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
					resourcev1.QualifiedName(apis.AttrPCIAddress):    stringAttr("0000:c0:14.0"),
					resourcev1.QualifiedName(apis.AttrInterfaceName): stringAttr("eth-live"),
					resourcev1.QualifiedName(apis.AttrMTU):           stringAttr("9000"),
				},
				Capacity: map[resourcev1.QualifiedName]resourcev1.DeviceCapacity{
					"network-bandwidth": qtyCap("10G"),
				},
			}},
			snapshot: []resourcev1.Device{{
				Name: "0000:c0:14.0",
				Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
					resourcev1.QualifiedName(apis.AttrPCIAddress):    stringAttr("0000:c0:14.0"),
					resourcev1.QualifiedName(apis.AttrInterfaceName): stringAttr("eth-snap"),
					resourcev1.QualifiedName(apis.AttrMTU):           stringAttr("1500"),
					resourcev1.QualifiedName(apis.AttrMac):           stringAttr("aa:bb:cc:dd:ee:ff"),
				},
				Capacity: map[resourcev1.QualifiedName]resourcev1.DeviceCapacity{
					"network-bandwidth": qtyCap("1G"),
					"other-capacity":    qtyCap("50"),
				},
			}},
			expected: []resourcev1.Device{{
				Name: "0000:c0:14.0",
				Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
					resourcev1.QualifiedName(apis.AttrPCIAddress):    stringAttr("0000:c0:14.0"),
					resourcev1.QualifiedName(apis.AttrInterfaceName): stringAttr("eth-live"),
					resourcev1.QualifiedName(apis.AttrMTU):           stringAttr("9000"),
					resourcev1.QualifiedName(apis.AttrMac):           stringAttr("aa:bb:cc:dd:ee:ff"),
				},
				Capacity: map[resourcev1.QualifiedName]resourcev1.DeviceCapacity{
					"network-bandwidth": qtyCap("10G"),
					"other-capacity":    qtyCap("50"),
				},
			}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := mergeDevices(tc.live, tc.snapshot)
			if diff := cmp.Diff(tc.expected, result); diff != "" {
				t.Errorf("mergeDevices result mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBuildCNSDevicesPublishesAllCapacities(t *testing.T) {
	for _, capacity := range []int{0, 1, 16} {
		t.Run(fmt.Sprintf("capacity-%d", capacity), func(t *testing.T) {
			np := &NetworkDriver{}
			devices := np.buildCNSDevices(&cnsclient.NICResource{
				MacAddress: "aa:bb:cc:dd:ee:01",
				Capacity:   capacity,
			})

			capacitySpec := devices[0].Capacity[cnsCapSlots]
			slots := capacitySpec.Value
			if got := slots.Value(); got != int64(capacity) {
				t.Fatalf("published slots = %d, want CNS capacity %d", got, capacity)
			}
			if capacity == 0 {
				if capacitySpec.RequestPolicy != nil {
					t.Fatalf("zero capacity request policy = %+v, want nil", capacitySpec.RequestPolicy)
				}
				wantTaints := []resourcev1.DeviceTaint{{
					Key:    cnsTaintNoCapacity,
					Value:  "true",
					Effect: resourcev1.DeviceTaintEffectNoSchedule,
				}}
				if diff := cmp.Diff(wantTaints, devices[0].Taints); diff != "" {
					t.Fatalf("zero capacity taints mismatch (-want +got):\n%s", diff)
				}
				return
			}
			if capacitySpec.RequestPolicy == nil {
				t.Fatal("positive capacity request policy is nil")
			}
			if len(devices[0].Taints) != 0 {
				t.Fatalf("positive capacity taints = %+v, want none", devices[0].Taints)
			}
		})
	}
}

func TestIsCNSNotReadyErr(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{name: "nil error"},
		{name: "unrelated error", err: errors.New("device not found")},
		{name: "claim resource info failure", err: errors.New("failed to get CNS claim resource info for claim claim-uid: CNS returned HTTP 400"), expected: true},
		{name: "MTPNC not ready", err: errors.New("mtpnc is not ready"), expected: true},
		{name: "network not ready", err: errors.New("network is not ready"), expected: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isCNSNotReadyErr(test.err); got != test.expected {
				t.Errorf("isCNSNotReadyErr() = %v, want %v", got, test.expected)
			}
		})
	}
}

func TestIsCNSClaim(t *testing.T) {
	tests := []struct {
		name          string
		cnsDriverName string
		drivers       []string
		hasAllocation bool
		expected      bool
	}{
		{name: "no CNS driver name configured", drivers: []string{"networking.azure.com"}},
		{name: "nil allocation", cnsDriverName: "networking.azure.com"},
		{name: "no matching driver", cnsDriverName: "networking.azure.com", drivers: []string{"dra.net"}, hasAllocation: true},
		{name: "CNS driver match", cnsDriverName: "networking.azure.com", drivers: []string{"networking.azure.com"}, hasAllocation: true, expected: true},
		{name: "mixed drivers", cnsDriverName: "networking.azure.com", drivers: []string{"dra.net", "networking.azure.com"}, hasAllocation: true, expected: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			claim := &resourcev1.ResourceClaim{}
			if test.hasAllocation || len(test.drivers) > 0 {
				claim.Status.Allocation = &resourcev1.AllocationResult{}
				for _, driver := range test.drivers {
					claim.Status.Allocation.Devices.Results = append(claim.Status.Allocation.Devices.Results,
						resourcev1.DeviceRequestAllocationResult{Driver: driver, Device: "eth1"})
				}
			}
			np := &NetworkDriver{cnsDriverName: test.cnsDriverName}
			if got := np.isCNSClaim(claim); got != test.expected {
				t.Errorf("isCNSClaim() = %v, want %v", got, test.expected)
			}
		})
	}
}

func TestPrepareResourceClaims_RoutesCNSClaimToFastPath(t *testing.T) {
	draPluginRequestsTotal.Reset()
	np := &NetworkDriver{
		driverName:        "dra.net",
		cnsDriverName:     "networking.azure.com",
		netdb:             newFakeInventoryDB(),
		podConfigStore:    mustNewPodConfigStore(),
		secondaryNICStore: NewSecondaryNICPodConfigStore(),
	}
	claim := &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "cns-claim", Namespace: "default", UID: types.UID("cns-claim-uid")},
		Status: resourcev1.ResourceClaimStatus{Allocation: &resourcev1.AllocationResult{
			Devices: resourcev1.DeviceAllocationResult{Results: []resourcev1.DeviceRequestAllocationResult{
				{Driver: "networking.azure.com", Device: "eth1", Request: "nic"},
			}},
		}},
	}

	result, err := np.PrepareResourceClaims(context.Background(), []*resourcev1.ResourceClaim{claim})
	if err != nil {
		t.Fatalf("PrepareResourceClaims failed: %v", err)
	}
	if prepareResult, ok := result[claim.UID]; ok && prepareResult.Err != nil {
		t.Fatalf("expected no error for CNS claim, got: %v", prepareResult.Err)
	}
	if _, ok := np.podConfigStore.GetPodConfig("some-uid"); ok {
		t.Error("podConfigStore should be empty for CNS fast path")
	}
}

func TestPrepareResourceClaims_CNSFastPathRejectsMultiplePodConsumers(t *testing.T) {
	draPluginRequestsTotal.Reset()
	np := &NetworkDriver{
		driverName:        "dra.net",
		cnsDriverName:     "networking.azure.com",
		secondaryNICStore: NewSecondaryNICPodConfigStore(),
	}
	claim := &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "cns-claim", Namespace: "default", UID: types.UID("cns-claim-uid")},
		Status: resourcev1.ResourceClaimStatus{
			ReservedFor: []resourcev1.ResourceClaimConsumerReference{
				{Resource: "pods", Name: "pod-a", UID: "pod-uid-a"},
				{Resource: "pods", Name: "pod-b", UID: "pod-uid-b"},
			},
			Allocation: &resourcev1.AllocationResult{Devices: resourcev1.DeviceAllocationResult{
				Results: []resourcev1.DeviceRequestAllocationResult{{Driver: "networking.azure.com", Device: "eth1", Request: "nic"}},
			}},
		},
	}

	result, err := np.PrepareResourceClaims(context.Background(), []*resourcev1.ResourceClaim{claim})
	if err != nil {
		t.Fatalf("PrepareResourceClaims failed: %v", err)
	}
	if result[claim.UID].Err == nil || !strings.Contains(result[claim.UID].Err.Error(), "has 2 pod consumers; only one is supported") {
		t.Fatalf("claim error = %v, want multiple-consumer rejection", result[claim.UID].Err)
	}
	_, foundA := np.secondaryNICStore.Get(types.UID("pod-uid-a"))
	_, foundB := np.secondaryNICStore.Get(types.UID("pod-uid-b"))
	if foundA || foundB {
		t.Fatal("secondary NIC store was populated for rejected multi-consumer claim")
	}
}

func TestPrepareResourceClaims_CNSFastPathRejectsMultipleSecondaryNICsBeforeWrite(t *testing.T) {
	draPluginRequestsTotal.Reset()
	cnsCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		cnsCalled = true
	}))
	defer server.Close()

	client, err := cnsclient.New(server.URL, 0)
	if err != nil {
		t.Fatal(err)
	}
	store := NewSecondaryNICPodConfigStore()
	np := &NetworkDriver{
		driverName:        "dra.net",
		cnsDriverName:     "networking.azure.com",
		cnsClient:         client,
		secondaryNICStore: store,
	}
	claim := &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "cns-claim", Namespace: "default", UID: types.UID("cns-claim-uid")},
		Status: resourcev1.ResourceClaimStatus{
			ReservedFor: []resourcev1.ResourceClaimConsumerReference{
				{Resource: "pods", Name: "pod-a", UID: "pod-uid-a"},
			},
			Allocation: &resourcev1.AllocationResult{Devices: resourcev1.DeviceAllocationResult{
				Results: []resourcev1.DeviceRequestAllocationResult{
					{Driver: "networking.azure.com", Device: "device-a", Request: "nic-a"},
					{Driver: "networking.azure.com", Device: "device-b", Request: "nic-b"},
				},
			}},
		},
	}

	result, err := np.PrepareResourceClaims(context.Background(), []*resourcev1.ResourceClaim{claim})
	if err != nil {
		t.Fatalf("PrepareResourceClaims failed: %v", err)
	}
	if result[claim.UID].Err == nil || !strings.Contains(result[claim.UID].Err.Error(), "has 2 secondary NIC devices; exactly one is supported") {
		t.Fatalf("claim error = %v, want multiple-secondary-NIC rejection", result[claim.UID].Err)
	}
	if cnsCalled {
		t.Fatal("CNS was called for rejected multi-secondary-NIC claim")
	}
	if _, found := store.Get("pod-uid-a"); found {
		t.Fatal("secondary NIC store was populated for rejected multi-secondary-NIC claim")
	}
}

func TestPrepareResourceClaims_CNSFastPathNICNotFound(t *testing.T) {
	draPluginRequestsTotal.Reset()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/network/requestclaimresourceinfo" {
			http.NotFound(w, request)
			return
		}
		_ = json.NewEncoder(w).Encode(cnsclient.ClaimResourceInfoResponse{
			Response: cnsclient.Response{ReturnCode: 0},
			PodIPInfo: []cnsclient.PodIPInfo{{
				PodIPConfig: cnsclient.IPSubnet{IPAddress: "10.0.0.10", PrefixLength: 24},
				MacAddress:  "aa:bb:cc:dd:ee:99",
			}},
		})
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
	claim := cnsFastPathClaim("cns-claim-uid", "pod-uid-1", "eth1")

	result, err := np.PrepareResourceClaims(context.Background(), []*resourcev1.ResourceClaim{claim})
	if err != nil {
		t.Fatalf("PrepareResourceClaims failed: %v", err)
	}
	if result[claim.UID].Err == nil || !strings.Contains(result[claim.UID].Err.Error(), "no CNS PodIPInfo matched") {
		t.Fatalf("claim error = %v, want unmatched CNS PodIPInfo error", result[claim.UID].Err)
	}
}

func TestPrepareResourceClaims_CNSFastPathNilClient(t *testing.T) {
	draPluginRequestsTotal.Reset()
	np := &NetworkDriver{
		driverName:        "dra.net",
		cnsDriverName:     "networking.azure.com",
		netdb:             newFakeInventoryDB(),
		podConfigStore:    mustNewPodConfigStore(),
		secondaryNICStore: NewSecondaryNICPodConfigStore(),
	}
	claim := cnsFastPathClaim("cns-claim-uid", "pod-uid-1", "eth1")

	result, err := np.PrepareResourceClaims(context.Background(), []*resourcev1.ResourceClaim{claim})
	if err != nil {
		t.Fatalf("PrepareResourceClaims failed: %v", err)
	}
	if result[claim.UID].Err == nil || !strings.Contains(result[claim.UID].Err.Error(), "CNS client not configured") {
		t.Fatalf("claim error = %v, want CNS client error", result[claim.UID].Err)
	}
}

func cnsFastPathClaim(claimUID, podUID, device string) *resourcev1.ResourceClaim {
	return &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "cns-claim", Namespace: "default", UID: types.UID(claimUID)},
		Status: resourcev1.ResourceClaimStatus{
			ReservedFor: []resourcev1.ResourceClaimConsumerReference{{APIGroup: "", Resource: "pods", Name: "test-pod", UID: types.UID(podUID)}},
			Allocation: &resourcev1.AllocationResult{Devices: resourcev1.DeviceAllocationResult{
				Results: []resourcev1.DeviceRequestAllocationResult{{Driver: "networking.azure.com", Device: device, Request: "nic"}},
			}},
		},
	}
}

func TestPrepareResourceClaims_DRAClaimStillUsesFullPath(t *testing.T) {
	draPluginRequestsTotal.Reset()
	np := &NetworkDriver{
		driverName:        "dra.net",
		cnsDriverName:     "networking.azure.com",
		netdb:             newFakeInventoryDB(),
		podConfigStore:    mustNewPodConfigStore(),
		secondaryNICStore: NewSecondaryNICPodConfigStore(),
		eventRecorder:     record.NewFakeRecorder(10),
	}
	claim := &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "dra-claim", Namespace: "default", UID: types.UID("dra-claim-uid")},
		Status: resourcev1.ResourceClaimStatus{
			ReservedFor: []resourcev1.ResourceClaimConsumerReference{{APIGroup: "", Resource: "pods", Name: "test-pod", UID: "pod-uid-1"}},
			Allocation: &resourcev1.AllocationResult{Devices: resourcev1.DeviceAllocationResult{
				Results: []resourcev1.DeviceRequestAllocationResult{{Driver: "dra.net", Device: "nonexistent-device", Request: "req1"}},
			}},
		},
	}

	result, err := np.PrepareResourceClaims(context.Background(), []*resourcev1.ResourceClaim{claim})
	if err != nil {
		t.Fatalf("PrepareResourceClaims failed: %v", err)
	}
	if result[claim.UID].Err == nil {
		t.Error("expected error for DRA claim with nonexistent device via full path")
	}
}

func TestUpdateCNSResourceSlicesForClaim(t *testing.T) {
	plugin := newFakePluginHelper()
	np := &NetworkDriver{
		cnsPlugin: plugin,
		lastCNSNICs: []cnsclient.NICResource{
			{MacAddress: "aa:bb:cc:dd:ee:01", InterfaceName: "eth1", SubnetGUID: "old-guid", SubnetName: "old-subnet", NetworkID: "net-1", Capacity: 16},
		},
	}
	claimNICs := []cnsclient.NICResource{
		{MacAddress: "aa:bb:cc:dd:ee:01", SubnetGUID: "new-guid", SubnetName: "new-subnet", NetworkID: "net-2", Capacity: 0},
		{MacAddress: "aa:bb:cc:dd:ee:02", SubnetGUID: "guid-2", SubnetName: "sn2", Capacity: 1},
	}

	if err := np.updateCNSResourceSlicesForClaim(context.Background(), claimNICs); err != nil {
		t.Fatalf("updateCNSResourceSlicesForClaim: %v", err)
	}
	byMAC := make(map[string]cnsclient.NICResource, len(np.lastCNSNICs))
	for _, nic := range np.lastCNSNICs {
		byMAC[nic.MacAddress] = nic
	}
	if len(np.lastCNSNICs) != 2 {
		t.Fatalf("expected 2 NICs after merge, got %d: %+v", len(np.lastCNSNICs), np.lastCNSNICs)
	}
	got := byMAC["aa:bb:cc:dd:ee:01"]
	if got.Capacity != 0 {
		t.Errorf("Capacity 0 must be carried forward, got %d", got.Capacity)
	}
	if got.SubnetGUID != "new-guid" || got.SubnetName != "new-subnet" || got.NetworkID != "net-2" {
		t.Errorf("subnet/vnet not updated: %+v", got)
	}
	if got.InterfaceName != "eth1" {
		t.Errorf("InterfaceName should be preserved, got %q", got.InterfaceName)
	}
	added, ok := byMAC["aa:bb:cc:dd:ee:02"]
	if !ok || added.Capacity != 1 || added.SubnetName != "sn2" {
		t.Errorf("new NIC not added correctly: %+v (ok=%v)", added, ok)
	}

	devices := plugin.publishedResources.Pools[secondaryNICsPoolName].Slices[0].Devices
	subnet := devices[0].Attributes[cnsAttrSubnet].StringValue
	if subnet == nil || *subnet != "new-guid" {
		t.Fatalf("prepare-time published subnet attribute = %v, want GUID %q", subnet, "new-guid")
	}
	capacity := devices[0].Capacity[cnsCapSlots]
	if slots := capacity.Value.Value(); slots != 0 {
		t.Fatalf("prepare-time published slots = %d, want authoritative CNS capacity 0", slots)
	}
}
