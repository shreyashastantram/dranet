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

package cnsclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

const (
	defaultBaseURL       = "http://localhost:10090"
	defaultTimeout       = 5 * time.Second
	getNICResourcePath   = "/network/nicresources"
	requestIPConfigsPath = "/network/requestipconfigs"
)

// NICResource represents a network interface resource from the VM.
type NICResource struct {
	Name          string `json:"name"`
	MacAddress    string `json:"macAddress"`
	InterfaceName string `json:"interfaceName,omitempty"`
	NetworkID     string `json:"networkID,omitempty"`
	VMUniqueID    string `json:"vmUniqueID,omitempty"`
	SubnetID      string `json:"subnetID,omitempty"`
	Capacity      int    `json:"capacity,omitempty"`
}

// Response represents the CNS API response status.
type Response struct {
	ReturnCode int    `json:"returnCode"`
	Message    string `json:"message"`
}

// GetNICResourcesResponse describes the response for GetNICResources API.
type GetNICResourcesResponse struct {
	Response     Response      `json:"response"`
	NICResources []NICResource `json:"nicResources"`
}

// KubernetesPodInfo identifies a pod in CNS orchestrator context.
type KubernetesPodInfo struct {
	PodName      string `json:"podName"`
	PodNamespace string `json:"podNamespace"`
	PodUID       string `json:"podUID,omitempty"`
}

// IPSubnet describes an IP and its prefix length.
type IPSubnet struct {
	IPAddress    string `json:"ipAddress"`
	PrefixLength uint8  `json:"prefixLength"`
}

// IPConfiguration carries network container IP details returned by CNS.
type IPConfiguration struct {
	IPSubnet          IPSubnet `json:"ipSubnet"`
	GatewayIPAddress  string   `json:"gatewayIPAddress"`
	PrimaryIP         string   `json:"primaryIP,omitempty"`
	SecondaryIPConfig bool     `json:"secondaryIPConfig,omitempty"`
}

// Route describes an interface route returned by CNS.
type Route struct {
	IPAddress        string `json:"ipAddress"`
	GatewayIPAddress string `json:"gatewayIPAddress"`
	InterfaceToUse   string `json:"interfaceToUse"`
}

// PodIPInfo describes a pod-facing IP configuration returned by CNS.
type PodIPInfo struct {
	PodIPConfig                     IPSubnet        `json:"podIPConfig"`
	NetworkContainerPrimaryIPConfig IPConfiguration `json:"networkContainerPrimaryIPConfig"`
	NICType                         string          `json:"nicType"`
	InterfaceName                   string          `json:"interfaceName,omitempty"`
	SharedNIC                       bool            `json:"sharedNIC,omitempty"`
	MacAddress                      string          `json:"macAddress,omitempty"`
	SkipDefaultRoutes               bool            `json:"skipDefaultRoutes,omitempty"`
	Routes                          []Route         `json:"routes,omitempty"`
	NetworkContainerID              string          `json:"networkContainerID,omitempty"`
}

// IPConfigsRequest is the CNS request body for pod IP configuration lookup.
type IPConfigsRequest struct {
	DesiredIPAddresses           []string        `json:"desiredIPAddresses"`
	PodInterfaceID               string          `json:"podInterfaceID"`
	InfraContainerID             string          `json:"infraContainerID"`
	OrchestratorContext          json.RawMessage `json:"orchestratorContext"`
	Ifname                       string          `json:"ifname"`
	SecondaryInterfacesExist     bool            `json:"secondaryInterfacesExist"`
	BackendInterfaceExist        bool            `json:"BackendInterfaceExist"`
	BackendInterfaceMacAddresses []string        `json:"BacknendInterfaceMacAddress"`
}

// IPConfigsResponse is the CNS response for pod IP configuration lookup.
type IPConfigsResponse struct {
	PodIPInfo []PodIPInfo `json:"podIPInfo"`
	Response  Response    `json:"response"`
}

// Client is an HTTP client for communicating with the CNS REST API.
type Client struct {
	httpClient     *http.Client
	nicResourceURL url.URL
	ipConfigsURL   url.URL
}

// New creates a new CNS client configured with the given base URL and timeout.
// If baseURL is empty, it defaults to http://localhost:10090.
// If timeout is zero, it defaults to 5 seconds.
func New(baseURL string, timeout time.Duration) (*Client, error) {
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	if timeout == 0 {
		timeout = defaultTimeout
	}

	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CNS base URL %q: %w", baseURL, err)
	}

	nicURL := *base
	nicURL.Path = getNICResourcePath
	ipConfigsURL := *base
	ipConfigsURL.Path = requestIPConfigsPath

	return &Client{
		httpClient: &http.Client{
			Timeout: timeout,
		},
		nicResourceURL: nicURL,
		ipConfigsURL:   ipConfigsURL,
	}, nil
}

// GetNICResources calls the CNS GetNICResources REST API and returns
// the list of NIC resources on this node.
func (c *Client) GetNICResources(ctx context.Context) ([]NICResource, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.nicResourceURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %w", err)
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("CNS HTTP request failed: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("CNS returned HTTP %d", res.StatusCode)
	}

	var resp GetNICResourcesResponse
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		return nil, fmt.Errorf("failed to decode CNS response: %w", err)
	}

	if resp.Response.ReturnCode != 0 {
		return nil, fmt.Errorf("CNS error (code %d): %s", resp.Response.ReturnCode, resp.Response.Message)
	}

	return resp.NICResources, nil
}

// GetPodGoalState returns the CNS pod IP configurations for a pod.
func (c *Client) GetPodGoalState(ctx context.Context, podName, podNamespace string) ([]PodIPInfo, error) {
	return c.GetPodIPConfig(ctx, podName, podNamespace, "")
}

// GetPodIPConfig returns the CNS pod IP configurations for a pod identified
// by name, namespace, and UID. It calls the CNS RequestIPConfigs endpoint
// which is idempotent: it returns existing IP assignments or allocates new
// ones if none exist. The podUID is included in the orchestrator context
// for future CNS-side disambiguation but is currently ignored by CNS.
func (c *Client) GetPodIPConfig(ctx context.Context, podName, podNamespace, podUID string) ([]PodIPInfo, error) {
	orchestratorContext, err := json.Marshal(KubernetesPodInfo{
		PodName:      podName,
		PodNamespace: podNamespace,
		PodUID:       podUID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal orchestrator context: %w", err)
	}

	reqBody := IPConfigsRequest{
		OrchestratorContext: orchestratorContext,
	}

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(reqBody); err != nil {
		return nil, fmt.Errorf("failed to encode CNS IPConfigsRequest: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.ipConfigsURL.String(), &body)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("CNS HTTP request failed: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("CNS returned HTTP %d", res.StatusCode)
	}

	var resp IPConfigsResponse
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		return nil, fmt.Errorf("failed to decode CNS IPConfigsResponse: %w", err)
	}

	if resp.Response.ReturnCode != 0 {
		return nil, fmt.Errorf("CNS error (code %d): %s", resp.Response.ReturnCode, resp.Response.Message)
	}

	return resp.PodIPInfo, nil
}
