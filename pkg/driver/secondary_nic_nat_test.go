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
	"reflect"
	"strings"
	"testing"
)

type scriptedIPTables struct {
	errors []error
	calls  [][]string
}

func (script *scriptedIPTables) execute(args ...string) error {
	script.calls = append(script.calls, append([]string(nil), args...))
	callIndex := len(script.calls) - 1
	if callIndex < len(script.errors) {
		return script.errors[callIndex]
	}
	return nil
}

func useScriptedIPTables(t *testing.T, commandErrors []error) *scriptedIPTables {
	t.Helper()
	script := &scriptedIPTables{errors: commandErrors}
	previous := executeIPTables
	executeIPTables = script.execute
	t.Cleanup(func() {
		executeIPTables = previous
	})
	return script
}

func commandExitError(t *testing.T, exitCode int) error {
	t.Helper()
	err := exec.Command("sh", "-c", fmt.Sprintf("exit %d", exitCode)).Run()
	if err == nil {
		t.Fatalf("expected command to exit with code %d", exitCode)
	}
	return err
}

func TestIsIPTablesRuleNotFound(t *testing.T) {
	notFound := commandExitError(t, 1)
	if !isIPTablesRuleNotFound(notFound) {
		t.Fatal("expected exit code 1 to mean rule not found")
	}
	if !isIPTablesRuleNotFound(fmt.Errorf("wrapped: %w", notFound)) {
		t.Fatal("expected wrapped exit code 1 to mean rule not found")
	}
	if isIPTablesRuleNotFound(commandExitError(t, 2)) {
		t.Fatal("unexpected rule-not-found match for exit code 2")
	}
	if isIPTablesRuleNotFound(errors.New("execution failed")) {
		t.Fatal("unexpected rule-not-found match for non-exit error")
	}
}

func TestEnsureSharedNICNATExemption(t *testing.T) {
	absent := errors.New("rule absent")
	insertFailure := errors.New("insert failed")
	tests := []struct {
		name      string
		errors    []error
		wantCalls [][]string
		wantError string
	}{
		{
			name:   "rule already exists",
			errors: []error{nil},
			wantCalls: [][]string{
				{"-t", "nat", "-C", "POSTROUTING", "-o", "eth1", "-j", "ACCEPT"},
			},
		},
		{
			name:   "missing rule is inserted",
			errors: []error{absent, nil},
			wantCalls: [][]string{
				{"-t", "nat", "-C", "POSTROUTING", "-o", "eth1", "-j", "ACCEPT"},
				{"-t", "nat", "-I", "POSTROUTING", "1", "-o", "eth1", "-j", "ACCEPT"},
			},
		},
		{
			name:      "insert failure is returned",
			errors:    []error{absent, insertFailure},
			wantError: "failed to add shared NIC NAT exemption",
			wantCalls: [][]string{
				{"-t", "nat", "-C", "POSTROUTING", "-o", "eth1", "-j", "ACCEPT"},
				{"-t", "nat", "-I", "POSTROUTING", "1", "-o", "eth1", "-j", "ACCEPT"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			script := useScriptedIPTables(t, test.errors)
			err := ensureSharedNICNATExemption("eth1")
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("ensureSharedNICNATExemption failed: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want substring %q", err, test.wantError)
			}
			if !reflect.DeepEqual(script.calls, test.wantCalls) {
				t.Fatalf("calls = %#v, want %#v", script.calls, test.wantCalls)
			}
		})
	}
}

func TestCleanupSharedNICNATExemptionRemovesDuplicates(t *testing.T) {
	script := useScriptedIPTables(t, []error{nil, nil, commandExitError(t, 1)})
	cleanupSharedNICNATExemption("eth1")

	wantCalls := [][]string{
		{"-t", "nat", "-D", "POSTROUTING", "-o", "eth1", "-j", "ACCEPT"},
		{"-t", "nat", "-D", "POSTROUTING", "-o", "eth1", "-j", "ACCEPT"},
		{"-t", "nat", "-D", "POSTROUTING", "-o", "eth1", "-j", "ACCEPT"},
	}
	if !reflect.DeepEqual(script.calls, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", script.calls, wantCalls)
	}
}

func TestCleanupSharedNICNATExemptionStopsOnDeleteFailure(t *testing.T) {
	deleteFailure := errors.New("delete failed")
	script := useScriptedIPTables(t, []error{deleteFailure})
	cleanupSharedNICNATExemption("eth1")

	wantCalls := [][]string{
		{"-t", "nat", "-D", "POSTROUTING", "-o", "eth1", "-j", "ACCEPT"},
	}
	if !reflect.DeepEqual(script.calls, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", script.calls, wantCalls)
	}
}
