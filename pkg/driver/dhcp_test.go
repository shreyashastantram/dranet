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
	"net"
	"strings"
	"testing"
)

// issueDHCPDiscover is I/O-bound: it locks the OS thread, captures the host
// netns, enters the pod netns, binds an AF_PACKET socket, and runs a
// DISCOVER/OFFER exchange. The only behavior that is unit-testable without root,
// a real pod netns, and a DHCP server is its input validation: an unopenable
// containerNsPath must fail fast — before the function enters any namespace or
// touches a socket — with a clear error. The successful DISCOVER path is
// exercised by the root-only integration tests.
func TestIssueDHCPDiscover_InvalidNetnsPath(t *testing.T) {
	mac, err := net.ParseMAC("00:0d:3a:12:34:56")
	if err != nil {
		t.Fatalf("failed to parse test MAC: %v", err)
	}

	for _, tc := range []struct {
		name   string
		nsPath string
	}{
		{"empty path", ""},
		{"nonexistent path", "/var/run/netns/dranet-dhcp-does-not-exist"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := issueDHCPDiscover(tc.nsPath, "eth1", mac)
			if err == nil {
				t.Fatalf("expected an error for netns path %q, got nil", tc.nsPath)
			}
			if !strings.Contains(err.Error(), "failed to open netns") {
				t.Errorf("expected a %q error, got: %v", "failed to open netns", err)
			}
		})
	}
}
