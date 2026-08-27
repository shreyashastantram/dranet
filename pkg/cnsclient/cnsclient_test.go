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
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetClaimResourceInfo(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method %s", r.Method)
		}
		if r.URL.Path != requestClaimResourceInfoPath {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}

		var req ClaimResourceInfoRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("failed to decode request: %v", err)
		}
		if req.ClaimUID != "claim-123" {
			t.Fatalf("unexpected claim UID: %q", req.ClaimUID)
		}

		_ = json.NewEncoder(w).Encode(ClaimResourceInfoResponse{
			Response: Response{ReturnCode: 0},
			PodIPInfo: []PodIPInfo{{
				PodIPConfig: IPSubnet{IPAddress: "10.0.0.10", PrefixLength: 24},
				MacAddress:  "aa:bb:cc:dd:ee:ff",
				SharedNIC:   true,
				NICType:     "DelegatedVMNIC",
			}},
		})
	}))
	defer server.Close()

	client, err := New(server.URL, 0)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	resp, err := client.GetClaimResourceInfo(context.Background(), "claim-123")
	if err != nil {
		t.Fatalf("GetClaimResourceInfo() failed: %v", err)
	}
	if len(resp.PodIPInfo) != 1 {
		t.Fatalf("expected 1 pod IP info, got %d", len(resp.PodIPInfo))
	}
	if resp.PodIPInfo[0].MacAddress != "aa:bb:cc:dd:ee:ff" {
		t.Fatalf("unexpected MAC %s", resp.PodIPInfo[0].MacAddress)
	}
	if !resp.PodIPInfo[0].SharedNIC {
		t.Fatal("expected SharedNIC to be true")
	}
}
