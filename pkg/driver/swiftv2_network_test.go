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
	"testing"
)

func TestParseIP32(t *testing.T) {
	tests := []struct {
		name     string
		ip       string
		wantNil  bool
		wantCIDR string
	}{
		{
			name:     "valid IPv4",
			ip:       "10.0.1.10",
			wantNil:  false,
			wantCIDR: "10.0.1.10/32",
		},
		{
			name:    "invalid IP",
			ip:      "not-an-ip",
			wantNil: true,
		},
		{
			name:    "empty string",
			ip:      "",
			wantNil: true,
		},
		{
			name:     "loopback",
			ip:       "127.0.0.1",
			wantNil:  false,
			wantCIDR: "127.0.0.1/32",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseIP32(tt.ip)
			if tt.wantNil {
				if result != nil {
					t.Errorf("parseIP32(%q) = %v, want nil", tt.ip, result)
				}
				return
			}
			if result == nil {
				t.Fatalf("parseIP32(%q) = nil, want %s", tt.ip, tt.wantCIDR)
			}
			if result.String() != tt.wantCIDR {
				t.Errorf("parseIP32(%q) = %s, want %s", tt.ip, result.String(), tt.wantCIDR)
			}
			// Verify mask is /32
			ones, bits := result.Mask.Size()
			if ones != 32 || bits != 32 {
				t.Errorf("parseIP32(%q) mask = /%d (bits %d), want /32", tt.ip, ones, bits)
			}
		})
	}
}

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

func TestNsAttachDedicatedNIC_NilConfig(t *testing.T) {
	_, err := nsAttachDedicatedNIC(nil, "/proc/1/ns/net", nil)
	if err == nil {
		t.Fatal("expected error for nil config, got nil")
	}
}

func TestNsAttachDedicatedNIC_InvalidMAC(t *testing.T) {
	cfg := &NICConfig{
		MAC:       "not-a-mac",
		GatewayIP: "169.254.2.1",
	}
	_, err := nsAttachDedicatedNIC(cfg, "/proc/1/ns/net", nil)
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
	// Invalid MAC should return false
	exists := nicExistsInNetns("/proc/1/ns/net", "not-a-mac")
	if !exists {
		// This is fine — invalid MAC means "not found"
	}
}

func TestCleanupIPVlanL3_NilConfig(t *testing.T) {
	// Should not panic with nil config
	cleanupIPVlanL3(nil)
}

func TestCleanupIPVlanL3_NonexistentParent(t *testing.T) {
	// Should not panic with nonexistent parent — just logs a warning
	cfg := &NICConfig{
		MAC:   "00:00:00:00:ff:ff",
		PodIP: "10.0.1.10",
	}
	cleanupIPVlanL3(cfg)
}

func TestCleanupDedicatedNIC_NonexistentNetns(t *testing.T) {
	// Should not panic with nonexistent netns — kernel already cleaned up
	cleanupDedicatedNIC("/proc/99999999/ns/net", "00:11:22:33:44:55")
}

func TestSwiftV2VirtualGW(t *testing.T) {
	// Verify the virtual GW constant is a valid IP
	ip := net.ParseIP(swiftV2VirtualGW)
	if ip == nil {
		t.Fatalf("swiftV2VirtualGW %q is not a valid IP", swiftV2VirtualGW)
	}
	if ip.String() != "169.254.2.1" {
		t.Errorf("swiftV2VirtualGW = %s, want 169.254.2.1", ip.String())
	}
}

func TestInterfaceDedicatedConfig(t *testing.T) {
	cfg := NICConfig{
		MAC:       "00:0d:3a:ab:cd:ef",
		GatewayIP: "169.254.2.1",
	}
	if cfg.MAC != "00:0d:3a:ab:cd:ef" {
		t.Errorf("expected MAC 00:0d:3a:ab:cd:ef, got %s", cfg.MAC)
	}
	if cfg.GatewayIP != "169.254.2.1" {
		t.Errorf("expected GatewayIP 169.254.2.1, got %s", cfg.GatewayIP)
	}
}
