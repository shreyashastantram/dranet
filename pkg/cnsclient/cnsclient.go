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
	"strconv"
	"time"
)

const (
	defaultBaseURL               = "http://localhost:10090"
	defaultTimeout               = 5 * time.Second
	getNICResourcePath           = "/network/nicresources"
	requestClaimResourceInfoPath = "/network/requestclaimresourceinfo"
)

// NICResource represents a network interface resource from the VM, as returned
// by the CNS GetNICResources API.
type NICResource struct {
	MacAddress             string `json:"macAddress"`
	InterfaceName          string `json:"interfaceName,omitempty"`
	InterfaceCompartmentID string `json:"interfaceCompartmentID,omitempty"`
	NetworkID              string `json:"networkID,omitempty"`
	VMUniqueID             string `json:"vmUniqueID,omitempty"`
	// SubnetGUID is the GUID of the customer subnet the NIC belongs to.
	SubnetGUID string `json:"subnetGUID,omitempty"`
	// SubnetName is the subnet name extracted from the subnet ARM resource ID.
	SubnetName string `json:"subnetName,omitempty"`
	// Capacity is the resource-slice capacity (number of pods schedulable on the
	// NIC). CNS sends it as a string on the wire ("0"/"1"/"16"); the custom
	// (Un)MarshalJSON below (de)serialize it to/from int so callers use an int.
	Capacity int `json:"-"`
}

// MarshalJSON renders Capacity as the wire string field "capacity" while keeping
// the Go field an int.
func (n NICResource) MarshalJSON() ([]byte, error) {
	type alias NICResource
	return json.Marshal(struct {
		alias
		Capacity string `json:"capacity"`
	}{
		alias:    alias(n),
		Capacity: strconv.Itoa(n.Capacity),
	})
}

// UnmarshalJSON parses the wire string field "capacity" into the int Capacity
// field. A missing or non-numeric value decodes to 0 (not schedulable).
func (n *NICResource) UnmarshalJSON(data []byte) error {
	type alias NICResource
	aux := struct {
		*alias
		Capacity string `json:"capacity"`
	}{alias: (*alias)(n)}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	n.Capacity = 0
	if c, err := strconv.Atoi(aux.Capacity); err == nil {
		n.Capacity = c
	}
	return nil
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

// ClaimResourceInfoRequest is the CNS request body for the RequestClaimResourceInfo
// API. ClaimUID is the DRA ResourceClaim UID; CNS resolves it to the owning pod.
type ClaimResourceInfoRequest struct {
	ClaimUID string `json:"claimUID"`
}

// ClaimResourceInfoResponse is the CNS response for the RequestClaimResourceInfo
// API. It returns the owning pod's IP configs plus the resource-slice properties
// of every NIC allocated to the pod.
type ClaimResourceInfoResponse struct {
	Response     Response      `json:"response"`
	PodIPInfo    []PodIPInfo   `json:"podIPInfo"`
	NICResources []NICResource `json:"nicResources"`
}

// Client is an HTTP client for communicating with the CNS REST API.
type Client struct {
	httpClient           *http.Client
	nicResourceURL       url.URL
	claimResourceInfoURL url.URL
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
	claimURL := *base
	claimURL.Path = requestClaimResourceInfoPath

	return &Client{
		httpClient: &http.Client{
			Timeout: timeout,
		},
		nicResourceURL:       nicURL,
		claimResourceInfoURL: claimURL,
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

// GetClaimResourceInfo calls the CNS RequestClaimResourceInfo endpoint for the
// given DRA ResourceClaim UID. CNS resolves the claim UID to the owning pod via
// its MTPNC and returns the pod's IP configurations together with the
// resource-slice properties of every NIC allocated to the pod.
func (c *Client) GetClaimResourceInfo(ctx context.Context, claimUID string) (*ClaimResourceInfoResponse, error) {
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(ClaimResourceInfoRequest{ClaimUID: claimUID}); err != nil {
		return nil, fmt.Errorf("failed to encode CNS ClaimResourceInfoRequest: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.claimResourceInfoURL.String(), &body)
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

	var resp ClaimResourceInfoResponse
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		return nil, fmt.Errorf("failed to decode CNS ClaimResourceInfoResponse: %w", err)
	}

	if resp.Response.ReturnCode != 0 {
		return nil, fmt.Errorf("CNS error (code %d): %s", resp.Response.ReturnCode, resp.Response.Message)
	}

	return &resp, nil
}
