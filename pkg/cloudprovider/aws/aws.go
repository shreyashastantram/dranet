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
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	lookup "github.com/aws-neuron/connected-device-maps-over-efa-for-neuron/lookup"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/dranet/pkg/apis"
	"sigs.k8s.io/dranet/pkg/cloudprovider"
)

const (
	// IMDS client configuration
	imdsHTTPTimeout = 10 * time.Second
	imdsMaxRetries  = 10

	// OnAWS detection timeout — caps total time spent probing IMDS on non-AWS instances
	onAWSTimeout = 5 * time.Second

	// GetInstance timeout — caps total time spent fetching instance metadata
	getInstanceTimeout = 15 * time.Second
)

var _ cloudprovider.CloudInstance = (*AWSInstance)(nil)

// AWSInstance holds the AWS specific instance data.
type AWSInstance struct {
	InstanceType     string
	IsNeuronInstance bool
}

// isNeuronInstance checks whether the EC2 instance type is a Neuron-based instance
// (Trainium or Inferentia) by examining the instance type prefix.
func isNeuronInstance(instanceType string) bool {
	prefix := strings.ToLower(instanceType)
	for i, c := range prefix {
		if c >= '0' && c <= '9' {
			prefix = prefix[:i]
			break
		}
	}
	return prefix == "trn" || prefix == "inf"
}

// isEFADevice checks whether the PCI device is bound to the EFA driver.
// It is a variable so tests can override it.
var isEFADevice = func(pciAddress string) bool {
	driver, err := os.Readlink(filepath.Join("/sys/bus/pci/devices", pciAddress, "driver"))
	if err != nil {
		klog.V(4).Infof("could not read driver for PCI device %s: %v", pciAddress, err)
		return false
	}
	return filepath.Base(driver) == "efa"
}

// getEFADeviceGroupIDs is a variable so tests can override it.
var getEFADeviceGroupIDs = lookup.GetEFADeviceGroupIDs

// GetDeviceAttributes fetches all attributes related to the provided device.
func (a *AWSInstance) GetDeviceAttributes(id cloudprovider.DeviceIdentifiers) map[resourceapi.QualifiedName]resourceapi.DeviceAttribute {
	attributes := make(map[resourceapi.QualifiedName]resourceapi.DeviceAttribute)

	if a.IsNeuronInstance && isEFADevice(id.PCIAddress) {
		deviceGroupAttributes, err := getEFADeviceGroupIDs(id.PCIAddress)
		if err != nil {
			klog.Warningf("failed to get EFA device group IDs for PCI address %s: %v", id.PCIAddress, err)
			return attributes
		}

		for attrName, attrValue := range deviceGroupAttributes {
			attributes[resourceapi.QualifiedName(attrName)] = resourceapi.DeviceAttribute{
				StringValue: &attrValue,
			}
		}
	}

	return attributes
}

// GetDeviceConfig returns infrastructure-specific network configuration for a device.
func (a *AWSInstance) GetDeviceConfig(id cloudprovider.DeviceIdentifiers) *apis.NetworkConfig {
	// TODO: return AWS-specific network configuration if needed
	return nil
}

// getIMDSClient creates or returns a cached IMDS client with retry and timeout configuration.
// It is a variable so tests can override it.
var getIMDSClient = func() func(ctx context.Context) (*imds.Client, error) {
	var cachedClient *imds.Client

	return func(ctx context.Context) (*imds.Client, error) {
		if cachedClient != nil {
			return cachedClient, nil
		}
		cfg, err := config.LoadDefaultConfig(ctx,
			config.WithHTTPClient(&http.Client{Timeout: imdsHTTPTimeout}),
			config.WithRetryMaxAttempts(imdsMaxRetries),
		)
		if err != nil {
			return nil, err
		}
		cachedClient = imds.NewFromConfig(cfg)
		return cachedClient, nil
	}
}()

// GetInstance retrieves AWS instance properties by querying the EC2 instance metadata service (IMDS).
func GetInstance(ctx context.Context) (cloudprovider.CloudInstance, error) {
	ctx, cancel := context.WithTimeout(ctx, getInstanceTimeout)
	defer cancel()

	client, err := getIMDSClient(ctx)
	if err != nil {
		return nil, err
	}
	output, err := client.GetInstanceIdentityDocument(ctx, &imds.GetInstanceIdentityDocumentInput{})
	if err != nil {
		klog.Errorf("failed to get instance identity document from IMDS: %v", err)
		return nil, err
	}

	isNeuron := isNeuronInstance(output.InstanceType)
	klog.Infof("AWS EC2 instance type: %s, region: %s, neuron: %v", output.InstanceType, output.Region, isNeuron)

	return &AWSInstance{
		InstanceType:     output.InstanceType,
		IsNeuronInstance: isNeuron,
	}, nil
}

// OnAWS checks whether the current instance is running on AWS EC2
// by probing the instance metadata service.
func OnAWS(ctx context.Context) bool {
	probeCtx, cancel := context.WithTimeout(ctx, onAWSTimeout)
	defer cancel()

	client, err := getIMDSClient(probeCtx)
	if err != nil {
		klog.Infof("could not create IMDS client for EC2 detection: %v", err)
		return false
	}
	_, err = client.GetInstanceIdentityDocument(probeCtx, &imds.GetInstanceIdentityDocumentInput{})
	if err != nil {
		klog.Infof("could not reach IMDS for EC2 detection: %v", err)
		return false
	}
	return true
}
