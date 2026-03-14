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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

const (
	defaultBaseURL     = "http://localhost:10090"
	defaultTimeout     = 5 * time.Second
	getNICResourcePath = "/network/nicresources"
)

// NICResource represents a network interface resource from the VM.
type NICResource struct {
	MacAddress             string `json:"macAddress"`
	InterfaceCompartmentID string `json:"interfaceCompartmentID,omitempty"`
	NetworkID              string `json:"networkID,omitempty"`
	VMUniqueID             string `json:"vmUniqueID,omitempty"`
	SubnetName             string `json:"subnetName,omitempty"`
	SharedNIC              bool   `json:"sharedNIC,omitempty"`
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

// Client is an HTTP client for communicating with the CNS REST API.
type Client struct {
	httpClient     *http.Client
	nicResourceURL url.URL
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

	return &Client{
		httpClient: &http.Client{
			Timeout: timeout,
		},
		nicResourceURL: nicURL,
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
