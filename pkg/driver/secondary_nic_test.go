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
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/vishvananda/netlink"
)

func TestNsAttachSecondaryNICDispatch(t *testing.T) {
	tests := []struct {
		name      string
		mode      NICMode
		wantError string
	}{
		{name: "shared", mode: NICModeShared, wantError: "NICConfig is nil"},
		{name: "exclusive", mode: NICModeExclusive, wantError: "NICConfig is nil"},
		{name: "unsupported", mode: NICMode("unknown"), wantError: "unsupported secondary NIC mode"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := nsAttachSecondaryNIC(test.mode, nil, "/proc/1/ns/net")
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want substring %q", err, test.wantError)
			}
		})
	}
}

func TestIsLinkNotFound(t *testing.T) {
	notFound := netlink.LinkNotFoundError{}
	if !isLinkNotFound(notFound) {
		t.Fatal("expected LinkNotFoundError to be recognized")
	}
	if !isLinkNotFound(fmt.Errorf("wrapped: %w", notFound)) {
		t.Fatal("expected wrapped LinkNotFoundError to be recognized")
	}
	if isLinkNotFound(errors.New("other error")) {
		t.Fatal("unexpected match for unrelated error")
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
			name: "unexpected IPv4 gateway rejected",
			cfg: &NICConfig{
				MAC:       "02:00:00:00:00:01",
				PodIP:     "10.0.1.10",
				GatewayIP: "169.254.2.2",
				PodUID:    "test-uid",
			},
			wantError: "unsupported secondary NIC gateway IP",
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

func TestNsAttachExclusiveNIC_NilConfig(t *testing.T) {
	_, err := nsAttachExclusiveNIC(nil, "/proc/1/ns/net")
	if err == nil {
		t.Fatal("expected error for nil config, got nil")
	}
}

func TestNsAttachExclusiveNIC_InvalidMAC(t *testing.T) {
	cfg := &NICConfig{
		MAC:       "not-a-mac",
		GatewayIP: "169.254.2.1",
	}
	_, err := nsAttachExclusiveNIC(cfg, "/proc/1/ns/net")
	if err == nil {
		t.Fatal("expected error for invalid MAC, got nil")
	}
}

func TestValidateExclusiveNICConfigTable(t *testing.T) {
	tests := []struct {
		name      string
		cfg       *NICConfig
		wantError string
	}{
		{
			name: "valid",
			cfg: &NICConfig{
				MAC:       "02:00:00:00:00:01",
				GatewayIP: secondaryNICVirtualGateway,
				Addresses: []string{"10.0.1.10/24"},
			},
		},
		{
			name:      "nil config",
			wantError: "NICConfig is nil",
		},
		{
			name: "unexpected gateway",
			cfg: &NICConfig{
				MAC:       "02:00:00:00:00:01",
				GatewayIP: "169.254.2.2",
				Addresses: []string{"10.0.1.10/24"},
			},
			wantError: "unsupported secondary NIC gateway IP",
		},
		{
			name: "invalid address",
			cfg: &NICConfig{
				MAC:       "02:00:00:00:00:01",
				GatewayIP: secondaryNICVirtualGateway,
				Addresses: []string{"not-a-cidr"},
			},
			wantError: "invalid exclusive NIC address",
		},
		{
			name: "IPv6 address",
			cfg: &NICConfig{
				MAC:       "02:00:00:00:00:01",
				GatewayIP: secondaryNICVirtualGateway,
				Addresses: []string{"fd00::1/64"},
			},
			wantError: "requires IPv4 addresses",
		},
		{
			name: "no addresses",
			cfg: &NICConfig{
				MAC:       "02:00:00:00:00:01",
				GatewayIP: secondaryNICVirtualGateway,
			},
			wantError: "requires exactly one IPv4 address",
		},
		{
			name: "multiple addresses",
			cfg: &NICConfig{
				MAC:       "02:00:00:00:00:01",
				GatewayIP: secondaryNICVirtualGateway,
				Addresses: []string{"10.0.1.10/24", "10.0.1.11/32"},
			},
			wantError: "requires exactly one IPv4 address",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mac, gateway, addresses, err := validateExclusiveNICConfig(test.cfg)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("error = %v, want substring %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateExclusiveNICConfig failed: %v", err)
			}
			if mac.String() != "02:00:00:00:00:01" || gateway.String() != secondaryNICVirtualGateway || len(addresses) != 1 {
				t.Fatalf("unexpected parsed config: MAC=%s gateway=%s addresses=%v", mac, gateway, addresses)
			}
		})
	}
}

func TestSharedNICVRFName(t *testing.T) {
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
			if got := sharedNICVRFName(tt.mac); got != tt.want {
				t.Errorf("sharedNICVRFName(%q) = %q, want %q", tt.mac, got, tt.want)
			}
		})
	}
}

func TestSharedNICVRFName_FitsLinuxIFNameLimit(t *testing.T) {
	name := sharedNICVRFName("60:45:bd:70:e4:89")
	if len(name) > 15 {
		t.Fatalf("VRF name %q length = %d, want <= 15", name, len(name))
	}
}

func TestSecondaryNICLock_SameMACReturnsSameMutex(t *testing.T) {
	// Different MAC string forms that normalize to the same key must yield the
	// same lock, otherwise concurrent attaches to the same parent NIC would race.
	a := secondaryNICLock("60:45:bd:70:e4:89")
	b := secondaryNICLock("60:45:BD:70:E4:89")
	c := secondaryNICLock("60-45-bd-70-e4-89")
	if a != b || a != c {
		t.Errorf("expected same mutex for equivalent MACs, got %p / %p / %p", a, b, c)
	}
	// Different MACs must yield different locks.
	d := secondaryNICLock("aa:bb:cc:dd:ee:ff")
	if a == d {
		t.Errorf("expected different mutex for different MAC")
	}
}

func TestSharedNICVRFTable_StablePerMAC(t *testing.T) {
	a, err := sharedNICVRFTable("60:45:bd:70:e4:89")
	if err != nil {
		t.Fatalf("sharedNICVRFTable failed: %v", err)
	}
	b, err := sharedNICVRFTable("60:45:BD:70:E4:89")
	if err != nil {
		t.Fatalf("sharedNICVRFTable failed: %v", err)
	}
	if a != b {
		t.Fatalf("same MAC in different case returned different tables: %d vs %d", a, b)
	}
	if a < 0x100000 {
		t.Fatalf("table = %d, want outside low reserved range", a)
	}
}

func TestSharedNICVRFTable_InvalidMAC(t *testing.T) {
	_, tableErr := sharedNICVRFTable("not-a-mac")
	if tableErr == nil {
		t.Fatal("expected error for invalid MAC, got nil")
	}
}

func TestValidateSharedNICIPVlanChild(t *testing.T) {
	parent := &netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Name: "parent0", Index: 10}}
	tests := []struct {
		name      string
		link      netlink.Link
		wantError string
	}{
		{
			name: "valid",
			link: &netlink.IPVlan{
				LinkAttrs: netlink.LinkAttrs{Name: "ipvl-test", ParentIndex: 10},
				Mode:      netlink.IPVLAN_MODE_L3,
			},
		},
		{
			name:      "nil",
			wantError: "is nil",
		},
		{
			name:      "wrong link type",
			link:      &netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Name: "ipvl-test"}},
			wantError: "expected ipvlan",
		},
		{
			name: "wrong mode",
			link: &netlink.IPVlan{
				LinkAttrs: netlink.LinkAttrs{Name: "ipvl-test", ParentIndex: 10},
				Mode:      netlink.IPVLAN_MODE_L2,
			},
			wantError: "expected L3",
		},
		{
			name: "wrong parent",
			link: &netlink.IPVlan{
				LinkAttrs: netlink.LinkAttrs{Name: "ipvl-test", ParentIndex: 11},
				Mode:      netlink.IPVLAN_MODE_L3,
			},
			wantError: "expected parent0 index 10",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateIPVlanChild(test.link, parent)
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("validateIPVlanChild failed: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want substring %q", err, test.wantError)
			}
		})
	}
}

func TestDetachNICFromSharedVRF_NoMasterNoop(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("requires root privileges for VRF state cleanup")
	}
	mac, err := net.ParseMAC("02:00:00:00:00:01")
	if err != nil {
		t.Fatalf("failed to parse test MAC: %v", err)
	}
	hostLink := &netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Name: "eth-test", HardwareAddr: mac}}
	got, err := detachNICFromSharedVRF(hostLink, mac.String())
	if err != nil {
		t.Fatalf("detachNICFromSharedVRF returned error for no-master link: %v", err)
	}
	if got != hostLink {
		t.Fatalf("detachNICFromSharedVRF returned %v, want original link", got)
	}
}

func TestDetachNICFromSharedVRF_MissingVRFRejectsUnexpectedMaster(t *testing.T) {
	mac := "02:00:00:ff:ff:fe"
	if _, err := netlink.LinkByName(sharedNICVRFName(mac)); err == nil {
		t.Skip("expected shared NIC VRF name is already present on the host")
	}
	hardwareAddr, err := net.ParseMAC(mac)
	if err != nil {
		t.Fatalf("failed to parse test MAC: %v", err)
	}
	hostLink := &netlink.Dummy{LinkAttrs: netlink.LinkAttrs{
		Name:         "eth-test",
		HardwareAddr: hardwareAddr,
		MasterIndex:  4242,
	}}

	_, err = detachNICFromSharedVRF(hostLink, mac)
	if err == nil || !strings.Contains(err.Error(), "expected shared NIC VRF") {
		t.Fatalf("detach error = %v, want missing expected VRF error", err)
	}
}

func TestDetachNICFromSharedVRF_NilLink(t *testing.T) {
	_, err := detachNICFromSharedVRF(nil, "02:00:00:00:00:01")
	if err == nil {
		t.Fatal("expected error for nil link, got nil")
	}
}
