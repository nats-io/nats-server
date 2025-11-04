//go:build !skip_js_tests

package server

import (
	"strings"
	"testing"
)

func TestJetStreamStreamImportCycle(t *testing.T) {
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

	// With the fix, the cycle error should be detected consistently
	_, err := ProcessConfigFile(conf)
	if err == nil || !strings.Contains(err.Error(), ErrImportFormsCycle.Error()) {
		t.Fatalf("Expected error containing %q, got: %v", ErrImportFormsCycle.Error(), err)
	}
	
	t.Logf("Cycle error correctly detected: %v", err)
}