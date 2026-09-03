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

package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"sigs.k8s.io/dranet/pkg/cloudprovider"
)

func TestIsNeuronInstance(t *testing.T) {
	tests := []struct {
		name         string
		instanceType string
		expected     bool
	}{
		{name: "trn1 instance", instanceType: "trn1.32xlarge", expected: true},
		{name: "trn2 instance", instanceType: "trn2.48xlarge", expected: true},
		{name: "inf1 instance", instanceType: "inf1.24xlarge", expected: true},
		{name: "inf2 instance", instanceType: "inf2.48xlarge", expected: true},
		{name: "p5 instance", instanceType: "p5.48xlarge", expected: false},
		{name: "g5 instance", instanceType: "g5.xlarge", expected: false},
		{name: "m5 instance", instanceType: "m5.xlarge", expected: false},
		{name: "c6i instance", instanceType: "c6i.large", expected: false},
		{name: "empty instance type", instanceType: "", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isNeuronInstance(tt.instanceType)
			if result != tt.expected {
				t.Errorf("isNeuronInstance(%q) = %v, want %v", tt.instanceType, result, tt.expected)
			}
		})
	}
}

func TestGetDeviceAttributes_NonNeuron(t *testing.T) {
	instance := &AWSInstance{
		InstanceType:     "p5.48xlarge",
		IsNeuronInstance: false,
	}
	attrs := instance.GetDeviceAttributes(cloudprovider.DeviceIdentifiers{
		PCIAddress: "0000:c9:00.0",
	})
	if len(attrs) != 0 {
		t.Errorf("expected empty attributes, got %d entries", len(attrs))
	}
}

func TestGetDeviceConfig(t *testing.T) {
	instance := &AWSInstance{
		InstanceType:     "p5.48xlarge",
		IsNeuronInstance: false,
	}
	config := instance.GetDeviceConfig(cloudprovider.DeviceIdentifiers{
		PCIAddress: "0000:c9:00.0",
		MAC:        "00:11:22:33:44:55",
		Name:       "eth1",
	})
	if config != nil {
		t.Errorf("expected nil config, got %+v", config)
	}
}

func TestAWSInstanceImplementsCloudInstance(t *testing.T) {
	// Compile-time check is already in aws.go via the var _ line,
	// but this confirms it at test time too.
	var _ cloudprovider.CloudInstance = (*AWSInstance)(nil)
}

// fakeIMDSServer creates a test HTTP server that mimics the EC2 IMDS identity document endpoint.
func fakeIMDSServer(t *testing.T, instanceType, region string, statusCode int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// IMDS token endpoint (IMDSv2)
		if r.URL.Path == "/latest/api/token" {
			w.Header().Set("Content-Type", "text/plain")
			fmt.Fprint(w, "fake-token")
			return
		}
		// Identity document endpoint
		if r.URL.Path == "/latest/dynamic/instance-identity/document" {
			w.WriteHeader(statusCode)
			if statusCode == http.StatusOK {
				doc := map[string]string{
					"instanceType": instanceType,
					"region":       region,
				}
				json.NewEncoder(w).Encode(doc)
			}
			return
		}
		http.NotFound(w, r)
	}))
}

// newTestIMDSClient creates an IMDS client pointing at the fake server.
func newTestIMDSClient(t *testing.T, serverURL string) *imds.Client {
	t.Helper()
	return imds.New(imds.Options{
		Endpoint:          serverURL,
		ClientEnableState: imds.ClientEnabled,
	})
}

// overrideIMDSClient replaces getIMDSClient for the duration of a test and restores it on cleanup.
func overrideIMDSClient(t *testing.T, client *imds.Client) {
	t.Helper()
	orig := getIMDSClient
	getIMDSClient = func(ctx context.Context) (*imds.Client, error) {
		return client, nil
	}
	t.Cleanup(func() { getIMDSClient = orig })
}

// overrideIMDSClientError replaces getIMDSClient with one that always returns an error.
func overrideIMDSClientError(t *testing.T, err error) {
	t.Helper()
	orig := getIMDSClient
	getIMDSClient = func(ctx context.Context) (*imds.Client, error) {
		return nil, err
	}
	t.Cleanup(func() { getIMDSClient = orig })
}

func TestGetInstance(t *testing.T) {
	tests := []struct {
		name             string
		instanceType     string
		region           string
		statusCode       int
		wantInstanceType string
		wantNeuron       bool
		wantErr          bool
	}{
		{
			name:             "neuron instance",
			instanceType:     "trn1.32xlarge",
			region:           "us-west-2",
			statusCode:       http.StatusOK,
			wantInstanceType: "trn1.32xlarge",
			wantNeuron:       true,
		},
		{
			name:             "gpu instance",
			instanceType:     "p5.48xlarge",
			region:           "us-east-1",
			statusCode:       http.StatusOK,
			wantInstanceType: "p5.48xlarge",
			wantNeuron:       false,
		},
		{
			name:             "standard instance",
			instanceType:     "m5.xlarge",
			region:           "eu-west-1",
			statusCode:       http.StatusOK,
			wantInstanceType: "m5.xlarge",
			wantNeuron:       false,
		},
		{
			name:       "IMDS returns error",
			statusCode: http.StatusInternalServerError,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := fakeIMDSServer(t, tt.instanceType, tt.region, tt.statusCode)
			defer server.Close()

			overrideIMDSClient(t, newTestIMDSClient(t, server.URL))

			instance, err := GetInstance(context.Background())
			if (err != nil) != tt.wantErr {
				t.Fatalf("GetInstance() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			awsInstance, ok := instance.(*AWSInstance)
			if !ok {
				t.Fatalf("expected *AWSInstance, got %T", instance)
			}
			if awsInstance.InstanceType != tt.wantInstanceType {
				t.Errorf("InstanceType = %q, want %q", awsInstance.InstanceType, tt.wantInstanceType)
			}
			if awsInstance.IsNeuronInstance != tt.wantNeuron {
				t.Errorf("IsNeuronInstance = %v, want %v", awsInstance.IsNeuronInstance, tt.wantNeuron)
			}
		})
	}
}

func TestGetInstance_IMDSClientError(t *testing.T) {
	overrideIMDSClientError(t, fmt.Errorf("simulated IMDS client creation failure"))

	_, err := GetInstance(context.Background())
	if err == nil {
		t.Fatal("expected error when IMDS client creation fails, got nil")
	}
}

func TestOnAWS(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		want       bool
	}{
		{
			name:       "on EC2",
			statusCode: http.StatusOK,
			want:       true,
		},
		{
			name:       "not on EC2",
			statusCode: http.StatusNotFound,
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := fakeIMDSServer(t, "m5.xlarge", "us-west-2", tt.statusCode)
			defer server.Close()

			overrideIMDSClient(t, newTestIMDSClient(t, server.URL))

			got := OnAWS(context.Background())
			if got != tt.want {
				t.Errorf("OnAWS() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOnAWS_IMDSClientError(t *testing.T) {
	overrideIMDSClientError(t, fmt.Errorf("simulated IMDS client creation failure"))

	if OnAWS(context.Background()) {
		t.Error("expected OnAWS() = false when IMDS client creation fails")
	}
}

func TestGetDeviceAttributes_NeuronSuccess(t *testing.T) {
	orig := isEFADevice
	isEFADevice = func(string) bool { return true }
	t.Cleanup(func() { isEFADevice = orig })

	origLookup := getEFADeviceGroupIDs
	getEFADeviceGroupIDs = func(string) (map[string]string, error) {
		return map[string]string{"resource.aws.com/devicegroup1_id": "0000:10:19.0"}, nil
	}
	t.Cleanup(func() { getEFADeviceGroupIDs = origLookup })

	instance := &AWSInstance{InstanceType: "trn1.32xlarge", IsNeuronInstance: true}
	attrs := instance.GetDeviceAttributes(cloudprovider.DeviceIdentifiers{PCIAddress: "0000:10:19.0"})
	if len(attrs) != 1 {
		t.Errorf("expected 1 attribute, got %d", len(attrs))
	}
}

func TestGetDeviceAttributes_NeuronLookupError(t *testing.T) {
	orig := isEFADevice
	isEFADevice = func(string) bool { return true }
	t.Cleanup(func() { isEFADevice = orig })

	origLookup := getEFADeviceGroupIDs
	getEFADeviceGroupIDs = func(string) (map[string]string, error) {
		return nil, fmt.Errorf("simulated lookup failure")
	}
	t.Cleanup(func() { getEFADeviceGroupIDs = origLookup })

	instance := &AWSInstance{InstanceType: "trn1.32xlarge", IsNeuronInstance: true}
	attrs := instance.GetDeviceAttributes(cloudprovider.DeviceIdentifiers{PCIAddress: "0000:00:00.0"})
	if len(attrs) != 0 {
		t.Errorf("expected empty attributes on error, got %d entries", len(attrs))
	}
}

func TestOnAWS_Timeout(t *testing.T) {
	// Simulate a slow IMDS client that blocks until context cancellation.
	// OnAWS internally calls context.WithTimeout(ctx, onAWSTimeout), so the
	// effective deadline is min(parent, child). By passing a 100ms parent
	// context we exercise the same cancellation path without waiting the
	// full 5s onAWSTimeout.
	orig := getIMDSClient
	getIMDSClient = func(ctx context.Context) (*imds.Client, error) {
		// Block until the context is cancelled by the onAWSTimeout
		<-ctx.Done()
		return nil, ctx.Err()
	}
	t.Cleanup(func() { getIMDSClient = orig })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	got := OnAWS(ctx)
	elapsed := time.Since(start)

	if got {
		t.Error("expected OnAWS() = false when IMDS client exceeds timeout")
	}
	if elapsed > 1*time.Second {
		t.Errorf("OnAWS() took %v, expected to return within ~100ms", elapsed)
	}
}

func TestGetDeviceAttributes_NeuronNotEFA(t *testing.T) {
	// Neuron instance but device is not bound to EFA driver — should skip lookup
	orig := isEFADevice
	isEFADevice = func(string) bool { return false }
	t.Cleanup(func() { isEFADevice = orig })

	instance := &AWSInstance{InstanceType: "trn1.32xlarge", IsNeuronInstance: true}
	attrs := instance.GetDeviceAttributes(cloudprovider.DeviceIdentifiers{PCIAddress: "0000:c9:00.0"})
	if len(attrs) != 0 {
		t.Errorf("expected empty attributes for non-EFA device, got %d entries", len(attrs))
	}
}

func TestGetInstance_Timeout(t *testing.T) {
	// Simulate a slow IMDS client that blocks until context cancellation.
	// GetInstance internally calls context.WithTimeout(ctx, getInstanceTimeout),
	// so the effective deadline is min(parent, child). By passing a 100ms parent
	// context we exercise the same cancellation path without waiting the
	// full 15s getInstanceTimeout.
	orig := getIMDSClient
	getIMDSClient = func(ctx context.Context) (*imds.Client, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	t.Cleanup(func() { getIMDSClient = orig })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := GetInstance(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error when GetInstance exceeds timeout, got nil")
	}
	if elapsed > 1*time.Second {
		t.Errorf("GetInstance() took %v, expected to return within ~100ms", elapsed)
	}
}
