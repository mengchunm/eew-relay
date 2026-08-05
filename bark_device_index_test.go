package main

import "testing"

func TestSelfHostedBarkDeviceIndexRespectsStorageCaseMatching(t *testing.T) {
	tests := []struct {
		name            string
		caseInsensitive bool
		lookup          string
		wantExists      bool
	}{
		{name: "mysql case insensitive collation", caseInsensitive: true, lookup: "Mixed-Bark-Key", wantExists: true},
		{name: "bbolt exact key", caseInsensitive: false, lookup: "Mixed-Bark-Key", wantExists: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index := selfHostedBarkDeviceIndex{
				devices:         map[string]bool{"mixed-bark-key": true},
				caseInsensitive: test.caseInsensitive,
			}
			usable, exists := index.lookup(test.lookup)
			if exists != test.wantExists || usable != test.wantExists {
				t.Fatalf("lookup usable=%v exists=%v, want exists=%v", usable, exists, test.wantExists)
			}
		})
	}
}

func TestSelfHostedBarkDeviceIndexPreservesEmptyToken(t *testing.T) {
	index := selfHostedBarkDeviceIndex{
		devices:         map[string]bool{"tokenless-key": false},
		caseInsensitive: true,
	}
	usable, exists := index.lookup("TOKENLESS-KEY")
	if !exists || usable {
		t.Fatalf("lookup usable=%v exists=%v, want existing key with empty token", usable, exists)
	}
}
