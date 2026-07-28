package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueuedTargetRetriesTransientFailuresUntilSuccess(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) <= 3 {
			http.Error(w, "device database temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":200}`))
	}))
	defer server.Close()

	notifier := NewNotifier(BarkConfig{
		Server:                server.URL,
		SelfHostedServer:      server.URL,
		RequestTimeoutSeconds: 1,
	}, AlertConfig{SendRetryAttempts: 1, SendRetryDelayMS: 1})
	result := sendQueuedTargetWithRetry(context.Background(), notifier, relayPushTarget{
		Server: server.URL,
		Key:    "test-device-key",
		Title:  "test",
		Body:   "body",
	}, 0, make(chan struct{}, 1), QueueConfig{
		RetryAttempts:      3,
		RetryBaseDelayMS:   1,
		RetryMaxDelayMS:    2,
		RetryWindowSeconds: 1,
	})

	if !result.Pushed || result.Error != "" {
		t.Fatalf("expected durable retry success, result=%#v", result)
	}
	if got := requests.Load(); got != 4 {
		t.Fatalf("expected four HTTP attempts, got %d", got)
	}
	if result.Retries != 3 {
		t.Fatalf("expected three retries, got %d", result.Retries)
	}
}

func TestQueuedTargetDoesNotRetryPermanentFailure(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		http.Error(w, "device key not found", http.StatusNotFound)
	}))
	defer server.Close()

	notifier := NewNotifier(BarkConfig{
		Server:                server.URL,
		SelfHostedServer:      server.URL,
		RequestTimeoutSeconds: 1,
	}, AlertConfig{SendRetryAttempts: 1, SendRetryDelayMS: 1})
	result := sendQueuedTargetWithRetry(context.Background(), notifier, relayPushTarget{
		Server: server.URL,
		Key:    "missing-device-key",
		Body:   "body",
	}, 0, make(chan struct{}, 1), QueueConfig{
		RetryAttempts:      3,
		RetryBaseDelayMS:   1,
		RetryMaxDelayMS:    2,
		RetryWindowSeconds: 1,
	})

	if result.Pushed || result.Error == "" {
		t.Fatalf("expected permanent failure, result=%#v", result)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("permanent failure must not retry, got %d requests", got)
	}
}

func TestQueuedRetryDelayIsDeterministicAndBounded(t *testing.T) {
	cfg := QueueConfig{RetryBaseDelayMS: 100, RetryMaxDelayMS: 400}
	first := queuedRetryDelay(cfg, 3, "device-key")
	second := queuedRetryDelay(cfg, 3, "device-key")
	if first != second {
		t.Fatalf("retry jitter must be deterministic: %s != %s", first, second)
	}
	if first < 400*time.Millisecond || first >= 500*time.Millisecond {
		t.Fatalf("retry delay outside expected capped jitter range: %s", first)
	}
}
