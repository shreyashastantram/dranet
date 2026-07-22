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
	"os"
	"strings"
	"testing"

	"github.com/vishvananda/netlink"
)

func TestTruncateUID(t *testing.T) {
	tests := []struct {
		name string
		uid  string
		want string
	}{
		{
			name: "long UID",
			uid:  "abcdef12-3456-7890-abcd-ef1234567890",
			want: "abcdef12",
		},
		{
			name: "exactly 8 chars",
			uid:  "abcdef12",
			want: "abcdef12",
		},
		{
			name: "shorter than 8 chars",
			uid:  "abc",
			want: "abc",
		},
		{
			name: "empty",
			uid:  "",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncateUID(tt.uid)
			if got != tt.want {
				t.Errorf("truncateUID(%q) = %q, want %q", tt.uid, got, tt.want)
			}
		})
	}
}

func TestNsAttachIPVlanL3_NilConfig(t *testing.T) {
	_, err := nsAttachIPVlanL3(nil, "/proc/1/ns/net")
	if err == nil {
		t.Fatal("expected error for nil config, got nil")
	}
}

func TestNsAttachIPVlanL3_ParentNotFound(t *testing.T) {
	cfg := &NICConfig{
		MAC:       "00:00:00:00:ff:ff",
		PodIP:     "10.0.1.10",
		GatewayIP: "169.254.2.1",
		PodUID:    "test-uid",
	}
	_, err := nsAttachIPVlanL3(cfg, "/proc/1/ns/net")
	if err == nil {
		t.Fatal("expected error for nonexistent parent NIC, got nil")
	}
}

func TestNsAttachIPVlanL3_InputValidationBeforeNetlink(t *testing.T) {
	tests := []struct {
		name      string
		cfg       *NICConfig
		wantError string
	}{
		{
			name: "invalid MAC",
			cfg: &NICConfig{
				MAC:       "not-a-mac",
				PodIP:     "10.0.1.10",
				GatewayIP: "169.254.2.1",
				PodUID:    "test-uid",
			},
			wantError: "invalid MAC",
		},
		{
			name: "invalid gateway IP",
			cfg: &NICConfig{
				MAC:       "02:00:00:00:00:01",
				PodIP:     "10.0.1.10",
				GatewayIP: "not-an-ip",
				PodUID:    "test-uid",
			},
			wantError: "invalid gateway IP",
		},
		{
			name: "IPv6 gateway rejected",
			cfg: &NICConfig{
				MAC:       "02:00:00:00:00:01",
				PodIP:     "10.0.1.10",
				GatewayIP: "fd00::1",
				PodUID:    "test-uid",
			},
			wantError: "requires an IPv4 gateway IP",
		},
		{
			name: "invalid pod IP",
			cfg: &NICConfig{
				MAC:       "02:00:00:00:00:01",
				PodIP:     "not-an-ip",
				GatewayIP: "169.254.2.1",
				PodUID:    "test-uid",
			},
			wantError: "invalid pod IP",
		},
		{
			name: "IPv6 pod IP rejected",
			cfg: &NICConfig{
				MAC:       "02:00:00:00:00:01",
				PodIP:     "fd00::2",
				GatewayIP: "169.254.2.1",
				PodUID:    "test-uid",
			},
			wantError: "requires an IPv4 pod IP",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, attachErr := nsAttachIPVlanL3(test.cfg, "/proc/1/ns/net")
			if attachErr == nil {
				t.Fatalf("expected error containing %q, got nil", test.wantError)
			}
			if !strings.Contains(attachErr.Error(), test.wantError) {
				t.Fatalf("error = %q, want substring %q", attachErr.Error(), test.wantError)
			}
		})
	}
}

func TestNsAttachDedicatedNIC_NilConfig(t *testing.T) {
	_, err := nsAttachDedicatedNIC(nil, "/proc/1/ns/net")
	if err == nil {
		t.Fatal("expected error for nil config, got nil")
	}
}

func TestNsAttachDedicatedNIC_InvalidMAC(t *testing.T) {
	cfg := &NICConfig{
		MAC:       "not-a-mac",
		GatewayIP: "169.254.2.1",
	}
	_, err := nsAttachDedicatedNIC(cfg, "/proc/1/ns/net")
	if err == nil {
		t.Fatal("expected error for invalid MAC, got nil")
	}
}

func TestNicExistsInNetns_InvalidPath(t *testing.T) {
	// Non-existent netns path should return false
	exists := nicExistsInNetns("/proc/99999999/ns/net", "00:11:22:33:44:55")
	if exists {
		t.Error("expected false for non-existent netns path")
	}
}

func TestNicExistsInNetns_InvalidMAC(t *testing.T) {
	// Invalid MAC should return false.
	if exists := nicExistsInNetns("/proc/1/ns/net", "not-a-mac"); exists {
		t.Error("expected false for invalid MAC")
	}
}

func TestCleanupIPVlanL3_NilConfig(t *testing.T) {
	// Should not panic with nil config
	cleanupIPVlanL3(nil)
}

func TestCleanupIPVlanL3_InvalidMAC(t *testing.T) {
	cleanupIPVlanL3(&NICConfig{MAC: "not-a-mac", PodIP: "10.0.1.10"})
}

func TestSwiftV2VRFName(t *testing.T) {
	tests := []struct {
		name string
		mac  string
		want string
	}{
		{"colon-separated lowercase", "60:45:bd:70:e4:89", "sv26045bd70e489"},
		{"colon-separated uppercase", "60:45:BD:70:E4:89", "sv26045bd70e489"},
		{"hyphen-separated", "60-45-bd-70-e4-89", "sv26045bd70e489"},
		// invalid input is normalized best-effort (lowercased, separators stripped)
		{"invalid passthrough", "not-a-mac", "sv2notamac"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := swiftV2VRFName(tt.mac); got != tt.want {
				t.Errorf("swiftV2VRFName(%q) = %q, want %q", tt.mac, got, tt.want)
			}
		})
	}
}

func TestSwiftV2VRFName_FitsLinuxIFNameLimit(t *testing.T) {
	name := swiftV2VRFName("60:45:bd:70:e4:89")
	if len(name) > 15 {
		t.Fatalf("VRF name %q length = %d, want <= 15", name, len(name))
	}
}

func TestSwiftV2ParentLock_SameMACReturnsSameMutex(t *testing.T) {
	// Different MAC string forms that normalize to the same key must yield the
	// same lock, otherwise concurrent attaches to the same parent NIC would race.
	a := swiftV2ParentLock("60:45:bd:70:e4:89")
	b := swiftV2ParentLock("60:45:BD:70:E4:89")
	c := swiftV2ParentLock("60-45-bd-70-e4-89")
	if a != b || a != c {
		t.Errorf("expected same mutex for equivalent MACs, got %p / %p / %p", a, b, c)
	}
	// Different MACs must yield different locks.
	d := swiftV2ParentLock("aa:bb:cc:dd:ee:ff")
	if a == d {
		t.Errorf("expected different mutex for different MAC")
	}
}

func TestSwiftV2VRFTable_StablePerMAC(t *testing.T) {
	a, err := swiftV2VRFTable("60:45:bd:70:e4:89")
	if err != nil {
		t.Fatalf("swiftV2VRFTable failed: %v", err)
	}
	b, err := swiftV2VRFTable("60:45:BD:70:E4:89")
	if err != nil {
		t.Fatalf("swiftV2VRFTable failed: %v", err)
	}
	if a != b {
		t.Fatalf("same MAC in different case returned different tables: %d vs %d", a, b)
	}
	if a < 0x100000 {
		t.Fatalf("table = %d, want outside low reserved range", a)
	}
}

func TestSwiftV2VRFTable_InvalidMAC(t *testing.T) {
	_, tableErr := swiftV2VRFTable("not-a-mac")
	if tableErr == nil {
		t.Fatal("expected error for invalid MAC, got nil")
	}
}

func TestSwiftV2ParentVRF_CloseCallsUnlock(t *testing.T) {
	called := false
	parentVRF := &swiftV2ParentVRF{unlock: func() { called = true }}
	parentVRF.Close()
	if !called {
		t.Fatal("Close did not call unlock")
	}
}

func TestReleaseSwiftV2VRFForDedicated_NoMasterNoop(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("requires root privileges for VRF state cleanup")
	}
	mac, err := net.ParseMAC("02:00:00:00:00:01")
	if err != nil {
		t.Fatalf("failed to parse test MAC: %v", err)
	}
	hostLink := &netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Name: "eth-test", HardwareAddr: mac}}
	got, err := releaseSwiftV2VRFForDedicated(hostLink, mac.String())
	if err != nil {
		t.Fatalf("releaseSwiftV2VRFForDedicated returned error for no-master link: %v", err)
	}
	if got != hostLink {
		t.Fatalf("releaseSwiftV2VRFForDedicated returned %v, want original link", got)
	}
}

func TestReleaseSwiftV2VRFForDedicated_NilLink(t *testing.T) {
	_, err := releaseSwiftV2VRFForDedicated(nil, "02:00:00:00:00:01")
	if err == nil {
		t.Fatal("expected error for nil link, got nil")
	}
}
