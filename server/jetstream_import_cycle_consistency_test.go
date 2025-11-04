//go:build !skip_js_tests

package server

import (
	"strings"
	"testing"
)

// TestJetStreamStreamImportCycleConsistency verifies that the cycle detection
// is now consistent and no longer subject to race conditions
func TestJetStreamStreamImportCycleConsistency(t *testing.T) {
	conf := createConfFile(t, []byte(`
		accounts: {
			SYS: {
				users: [
					{ user:s, password:x }
					{ user:l-s, password:x }  
				]
			}
			MDC: {
				jetstream: true
				users: [ 
					{ user:a, password:x }
				]
				exports: [
					{ stream: "broadcast.>" }
					{ stream: "vdc.*.>" }
				]
				imports: [
					{ stream: { account: VDC-1, subject: "sensor.temp.>" }, to: "sensor.vdc.1.temp.>" }
					{ stream: { account: VDC-2, subject: "sensor.temp.>" }, to: "sensor.vdc.2.temp.>" }
				]
			}
			VDC-1: {
				jetstream: false
				users: [ 
					{ user:sniff-1, password:x }
					{ user:l-VDC-1, password:x, permissions: {
						publish: { }
						subscribe: { }
					} }
				]
				exports: [
					{ stream: "sensor.temp.>" }
				]
				imports: [
					{ stream: { account: MDC, subject: "broadcast.>" }, to: ">" }
					{ stream: { account: MDC, subject: "vdc.1.>" }, to: ">" }
				]
			}
			VDC-2: {
				jetstream: false
				users: [ 
					{ user:sniff-2, password:x }
					{ user:l-VDC-2, password:x, permissions: {
						publish: { }
						subscribe: { }
					} }
				]
				exports: [
					{ stream: "sensor.temp.>" }
				]
				imports: [
					{ stream: { account: MDC, subject: "broadcast.>" }, to: ">" }
					{ stream: { account: MDC, subject: "vdc.2.>" }, to: ">" }
				]
			}
		}
		system_account: SYS
	`))

	// Test consistency: should always detect the cycle now
	cycleErrorCount := 0
	totalAttempts := 100
	
	for i := 0; i < totalAttempts; i++ {
		_, err := ProcessConfigFile(conf)
		if err != nil && strings.Contains(err.Error(), ErrImportFormsCycle.Error()) {
			cycleErrorCount++
		}
	}
	
	// With the fix, we should detect the cycle in 100% of attempts
	if cycleErrorCount != totalAttempts {
		t.Fatalf("Expected cycle to be detected in all %d attempts, but only detected in %d attempts", totalAttempts, cycleErrorCount)
	}
	
	t.Logf("Success: Cycle detected consistently in all %d attempts", totalAttempts)
}