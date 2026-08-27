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

	"github.com/vishvananda/netlink"
)

func TestValidateExclusiveNICConfigParsedValues(t *testing.T) {
	cfg := &NICConfig{
		MAC:       "02:00:00:00:00:01",
		GatewayIP: secondaryNICVirtualGateway,
		Addresses: []string{"10.0.1.10/24"},
	}

	mac, gateway, addresses, err := validateExclusiveNICConfig(cfg)
	if err != nil {
		t.Fatalf("validateExclusiveNICConfig failed: %v", err)
	}
	if mac.String() != cfg.MAC {
		t.Fatalf("MAC = %s, want %s", mac, cfg.MAC)
	}
	if gateway.String() != secondaryNICVirtualGateway {
		t.Fatalf("gateway = %s, want %s", gateway, secondaryNICVirtualGateway)
	}
	if len(addresses) != 1 {
		t.Fatalf("addresses = %d, want 1", len(addresses))
	}
	if addresses[0].cidr != "10.0.1.10/24" || addresses[0].address.String() != "10.0.1.10/24" || addresses[0].subnet.String() != "10.0.1.0/24" {
		t.Fatalf("unexpected parsed address: %+v", addresses[0])
	}
}

func TestValidateExclusiveNICConfigRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name      string
		cfg       *NICConfig
		wantError string
	}{
		{
			name: "invalid MAC",
			cfg: &NICConfig{
				MAC:       "not-a-mac",
				GatewayIP: secondaryNICVirtualGateway,
				Addresses: []string{"10.0.1.10/24"},
			},
			wantError: "invalid MAC",
		},
		{
			name: "IPv6 gateway",
			cfg: &NICConfig{
				MAC:       "02:00:00:00:00:01",
				GatewayIP: "fd00::1",
				Addresses: []string{"10.0.1.10/24"},
			},
			wantError: "requires an IPv4 gateway IP",
		},
		{
			name: "unexpected IPv4 gateway",
			cfg: &NICConfig{
				MAC:       "02:00:00:00:00:01",
				GatewayIP: "169.254.2.2",
				Addresses: []string{"10.0.1.10/24"},
			},
			wantError: "unsupported secondary NIC gateway IP",
		},
		{
			name: "IPv6 address",
			cfg: &NICConfig{
				MAC:       "02:00:00:00:00:01",
				GatewayIP: secondaryNICVirtualGateway,
				Addresses: []string{"fd00::10/64"},
			},
			wantError: "requires IPv4 addresses",
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
			_, _, _, err := validateExclusiveNICConfig(test.cfg)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want substring %q", err, test.wantError)
			}
		})
	}
}

func TestDetachNICFromSharedVRFRejectsInvalidMACBeforeNetlink(t *testing.T) {
	mac, err := net.ParseMAC("02:00:00:00:00:01")
	if err != nil {
		t.Fatalf("failed to parse test MAC: %v", err)
	}
	hostLink := &netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Name: "eth-test", HardwareAddr: mac}}

	if _, err := detachNICFromSharedVRF(hostLink, "not-a-mac"); err == nil {
		t.Fatal("expected invalid MAC to fail before VRF netlink operations")
	}
}
