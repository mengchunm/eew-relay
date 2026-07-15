package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRelayAuthorized(t *testing.T) {
	if !relayAuthorized("Bearer secret", "secret") {
		t.Fatal("expected valid bearer token")
	}
	for _, header := range []string{"", "Bearer", "Bearer wrong", "Basic secret"} {
		if relayAuthorized(header, "secret") {
			t.Fatalf("unexpected authorization for %q", header)
		}
	}
}

func TestRelayCachesCompletedBatch(t *testing.T) {
	var pushes atomic.Int32
	bark := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		pushes.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":200}`))
	}))
	defer bark.Close()

	notifier := NewNotifier(BarkConfig{
		Server:                   bark.URL,
		SelfHostedServer:         bark.URL,
		SelfHostedInternalServer: bark.URL,
	})
	runtime := &fanoutRelayRuntime{
		cfg:      RelayConfig{Token: "secret", Concurrency: 2, MaxBatchSize: 10},
		notifier: notifier,
		sem:      make(chan struct{}, 2),
		cache:    make(map[string]relayCacheEntry),
	}
	relay := httptest.NewServer(http.HandlerFunc(runtime.handleFanout))
	defer relay.Close()

	payload := relayFanoutRequest{
		ID: "batch-1",
		Targets: []relayPushTarget{
			{Server: bark.URL, Key: "key-1", Body: "one"},
			{Server: bark.URL, Key: "key-2", Body: "two"},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	client := &FanoutRelayClient{
		cfg:    RelayConfig{URL: relay.URL, Token: "secret"},
		client: relay.Client(),
	}
	for i := 0; i < 2; i++ {
		response, err := client.doBatchRequest(context.Background(), body)
		if err != nil {
			t.Fatal(err)
		}
		if len(response.Results) != 2 || !response.Results[0].Pushed || !response.Results[1].Pushed {
			t.Fatalf("unexpected relay response: %+v", response)
		}
	}
	if got := pushes.Load(); got != 2 {
		t.Fatalf("expected cached retry to avoid duplicate pushes, got %d requests", got)
	}
}

func TestSplitRelayTargetsIsStable(t *testing.T) {
	notifier := &Notifier{relay: &FanoutRelayClient{cfg: RelayConfig{SharePercent: 40}}}
	targets := make([]fanoutTarget, 100)
	for i := range targets {
		targets[i].Sub.BarkID = string(rune(i + 1000))
	}
	localA, remoteA := splitRelayTargets(notifier, targets)
	localB, remoteB := splitRelayTargets(notifier, targets)
	if len(localA) != len(localB) || len(remoteA) != len(remoteB) {
		t.Fatalf("unstable split sizes: %d/%d vs %d/%d", len(localA), len(remoteA), len(localB), len(remoteB))
	}
	if len(remoteA) < 20 || len(remoteA) > 60 {
		t.Fatalf("unexpected 40%% split size: %d", len(remoteA))
	}
	for i := range remoteA {
		if remoteA[i].Sub.BarkID != remoteB[i].Sub.BarkID {
			t.Fatal("relay assignment changed between runs")
		}
	}
}

func TestRelayFailureFallsBackToLocal(t *testing.T) {
	var pushes atomic.Int32
	bark := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		pushes.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":200}`))
	}))
	defer bark.Close()

	cfg := Config{
		Bark: BarkConfig{
			Server:                   bark.URL,
			SelfHostedServer:         bark.URL,
			SelfHostedInternalServer: bark.URL,
		},
		Alert: AlertConfig{SelfHostedConcurrency: 2, SendRetryAttempts: 1, SendRetryDelayMS: 1},
	}
	notifier := NewNotifier(cfg.Bark, cfg.Alert)
	notifier.relay = &FanoutRelayClient{
		cfg:    RelayConfig{URL: "http://127.0.0.1:1", Token: "secret", SharePercent: 100, BatchSize: 2, MaxInflightBatches: 1},
		client: &http.Client{Timeout: 50 * time.Millisecond},
	}
	targets := []fanoutTarget{
		{Sub: Subscription{BarkID: "key-1", BarkServer: bark.URL}, Body: "one"},
		{Sub: Subscription{BarkID: "key-2", BarkServer: bark.URL}, Body: "two"},
	}
	pushed, skipped, failed, _ := sendFanout(context.Background(), cfg, notifier, Event{EventID: "event-1", ReportNum: 1}, targets)
	if pushed != 2 || skipped != 0 || failed != 0 {
		t.Fatalf("unexpected fallback result pushed=%d skipped=%d failed=%d", pushed, skipped, failed)
	}
	if got := pushes.Load(); got != 2 {
		t.Fatalf("expected two local fallback pushes, got %d", got)
	}
}
