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
	"os/exec"
	"strings"

	"k8s.io/klog/v2"
)

// runIPTables runs an iptables command, passing -w so it waits (up to 5s) for
// the xtables lock instead of failing when another component (kube-proxy,
// ip-masq-agent, CNS) is mutating the table concurrently.
func runIPTables(args ...string) error {
	full := append([]string{"-w", "5"}, args...)
	output, err := exec.Command("iptables", full...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("iptables %s failed: %w: %s", strings.Join(full, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

var executeIPTables = runIPTables

// isIPTablesRuleNotFound reports the exit code used by iptables when a delete
// request has no matching rule. runIPTables preserves the ExitError through %w.
func isIPTablesRuleNotFound(err error) bool {
	var exitErr *exec.ExitError
	return errors.As(err, &exitErr) && exitErr.ExitCode() == 1
}

// ensureSharedNICNATExemption makes sure the masquerade-exemption rule for the
// given exclusive parent NIC is present in nat POSTROUTING, so customer traffic
// leaving that NIC keeps the real pod source IP instead of
// being SNATed by host masquerade (ip-masq-agent / kube-proxy).
//
// It is check-first and convergent: when the rule already exists it does
// nothing, so steady-state attach and reconcile calls do not churn the host nat
// table. Concurrent callers can both pass the check and insert a harmless
// duplicate; cleanupSharedNICNATExemption removes all copies. When absent, the
// rule is inserted at the top of POSTROUTING ahead of later MASQUERADE rules.
// ACCEPT is an explicit terminal exemption; RETURN would hand control back to
// the calling chain or built-in policy, making the result depend on surrounding
// rules that could still lead to MASQUERADE.
func ensureSharedNICNATExemption(parentName string) error {
	// iptables -w 5 -t nat -C POSTROUTING -o <parentName> -j ACCEPT
	if executeIPTables("-t", "nat", "-C", "POSTROUTING", "-o", parentName, "-j", "ACCEPT") == nil {
		return nil
	}
	// iptables -w 5 -t nat -I POSTROUTING 1 -o <parentName> -j ACCEPT
	if err := executeIPTables("-t", "nat", "-I", "POSTROUTING", "1", "-o", parentName, "-j", "ACCEPT"); err != nil {
		return fmt.Errorf("failed to add shared NIC NAT exemption for %s: %w", parentName, err)
	}
	return nil
}

// cleanupSharedNICNATExemption deletes the masquerade-exemption rule for the given
// parent NIC, repeating until no matching rule remains so any duplicates are all
// removed. A delete that finds no matching rule completes cleanup normally;
// other failures are logged.
func cleanupSharedNICNATExemption(parentName string) {
	for {
		// iptables -w 5 -t nat -D POSTROUTING -o <parentName> -j ACCEPT
		err := executeIPTables("-t", "nat", "-D", "POSTROUTING", "-o", parentName, "-j", "ACCEPT")
		if err == nil {
			continue
		}
		if isIPTablesRuleNotFound(err) {
			return
		}
		klog.Warningf("failed to remove shared NIC NAT exemption for %s: %v", parentName, err)
		return
	}
}
