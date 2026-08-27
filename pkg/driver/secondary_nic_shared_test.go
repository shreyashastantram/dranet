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
	"strings"
	"testing"
)

func TestSecondaryNICMACKey(t *testing.T) {
	tests := []struct {
		name string
		mac  string
		want string
	}{
		{name: "lowercase colon separated", mac: "60:45:bd:70:e4:89", want: "6045bd70e489"},
		{name: "uppercase colon separated", mac: "60:45:BD:70:E4:89", want: "6045bd70e489"},
		{name: "hyphen separated", mac: "60-45-bd-70-e4-89", want: "6045bd70e489"},
		{name: "invalid best effort", mac: "NOT-A-MAC", want: "notamac"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := normalizedMACKey(test.mac); got != test.want {
				t.Fatalf("normalizedMACKey(%q) = %q, want %q", test.mac, got, test.want)
			}
		})
	}
}

func TestEnsureSharedNICParentVRFRejectsInvalidMACBeforeNetlink(t *testing.T) {
	if _, err := ensureSharedNICParentVRF("not-a-mac"); err == nil {
		t.Fatal("expected invalid MAC to fail before host netlink operations")
	}
}

func TestConfigureSharedNICParentRoutingRejectsNilInput(t *testing.T) {
	if err := configureSharedNICParentRouting(nil, nil); err == nil || !strings.Contains(err.Error(), "parent routing is nil") {
		t.Fatalf("configure error = %v, want nil parent routing error", err)
	}
}
