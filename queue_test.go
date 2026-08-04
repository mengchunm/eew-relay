package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
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
	if result.FirstAttemptDoneAtUnixMS <= 0 {
		t.Fatalf("first attempt completion was not recorded: %#v", result)
	}
	if !result.FirstAttemptKnown || result.FirstAttemptOK {
		t.Fatalf("first failed attempt was not recorded separately from retry success: %#v", result)
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
	if result.FirstAttemptDoneAtUnixMS <= 0 {
		t.Fatalf("first attempt completion was not recorded: %#v", result)
	}
	if !result.FirstAttemptKnown || result.FirstAttemptOK {
		t.Fatalf("permanent first-attempt failure was not recorded: %#v", result)
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

func TestQueueWorkerHealthDetectsMissingAndStaleWorkers(t *testing.T) {
	now := time.Now()
	client := &PushQueueClient{
		cfg: QueueConfig{ExpectedWorkers: 2, WorkerHeartbeatSecond: 5},
		workers: map[string]queueWorkerObservation{
			"instance-a": {Heartbeat: queueWorkerHeartbeat{ID: "worker-a", InstanceID: "instance-a", NodeID: "node-a", State: "running", ControlVersion: 1, StartedAt: now.Add(-time.Hour), Concurrency: 500, Batches: 3, Targets: 1200}, Received: now.Add(-2 * time.Second)},
			"instance-b": {Heartbeat: queueWorkerHeartbeat{ID: "worker-b", InstanceID: "instance-b", NodeID: "node-b", State: "paused", ControlVersion: 1, StartedAt: now.Add(-time.Hour), Concurrency: 500}, Received: now.Add(-time.Minute)},
		},
	}
	workers, expected, staleAfter := client.workerHealth(now)
	if expected != 2 || staleAfter != 20*time.Second || len(workers) != 2 || !workers[0].Active || workers[1].Active {
		t.Fatalf("unexpected worker health: workers=%#v expected=%d stale=%s", workers, expected, staleAfter)
	}
	monitor := &adminServiceMonitor{notifier: &Notifier{queue: client}}
	result := monitor.probePushWorkers(context.Background())
	if result.Status != "degraded" || result.Metrics["active"] != 1 || result.Metrics["expected"] != 2 {
		t.Fatalf("missing worker was not degraded: %#v", result)
	}
}

func TestQueueWorkerHeartbeatContainsOnlyOperationalMetadata(t *testing.T) {
	runtime := newQueueWorkerRuntime(QueueConfig{WorkerID: "worker-a", NodeID: "node-west", WorkerConcurrency: 500})
	runtime.batches.Store(4)
	runtime.targets.Store(1500)
	runtime.failedTargets.Store(2)
	runtime.lastBatchUnix.Store(time.Now().Unix())
	heartbeat := runtime.heartbeat()
	if heartbeat.ID != "worker-a" || heartbeat.InstanceID == "" || heartbeat.NodeID != "node-west" || heartbeat.State != "running" || heartbeat.ControlVersion != 1 || heartbeat.SentAt.IsZero() || heartbeat.StartedAt.IsZero() || heartbeat.Concurrency != 500 || heartbeat.Batches != 4 || heartbeat.Targets != 1500 || heartbeat.FailedTargets != 2 || heartbeat.LastBatchAt.IsZero() {
		t.Fatalf("unexpected worker heartbeat: %#v", heartbeat)
	}
}

func TestQueueWorkerControlProtectsLastRunningWorker(t *testing.T) {
	workers := []queueWorkerHealth{
		{ID: "worker-a", InstanceID: "instance-a", NodeID: "node-a", State: "running", Active: true, SupportsControl: true, LastSeenAge: 1},
		{ID: "worker-b", InstanceID: "instance-b", NodeID: "node-b", State: "paused", Active: true, SupportsControl: true, LastSeenAge: 1},
	}
	if _, err := validateQueueWorkerControlTarget(workers, "instance-a", "drain", 20*time.Second); !errors.Is(err, errLastRunningWorker) {
		t.Fatalf("last running worker was not protected: %v", err)
	}
	workers[1].State = "running"
	if _, err := validateQueueWorkerControlTarget(workers, "instance-a", "drain", 20*time.Second); err != nil {
		t.Fatalf("worker should be drainable when another worker is running: %v", err)
	}
}

func TestQueueWorkerControllerPauseAndDrainState(t *testing.T) {
	runtime := newQueueWorkerRuntime(QueueConfig{WorkerID: "worker-a", NodeID: "node-a", WorkerConcurrency: 10})
	controller := &queueWorkerController{runtime: runtime}
	if err := controller.stopAccepting(queueWorkerStatePaused); err != nil || runtime.state.Load() != queueWorkerStatePaused {
		t.Fatalf("pause state failed state=%s err=%v", queueWorkerStateName(runtime.state.Load()), err)
	}
	runtime.inflight.Store(2)
	if err := controller.stopAccepting(queueWorkerStateDraining); err != nil || runtime.state.Load() != queueWorkerStateDraining {
		t.Fatalf("drain state failed state=%s err=%v", queueWorkerStateName(runtime.state.Load()), err)
	}
	runtime.inflight.Store(0)
	if err := controller.stopAccepting(queueWorkerStateDraining); err != nil || runtime.state.Load() != queueWorkerStatePaused {
		t.Fatalf("empty drain must settle paused state=%s err=%v", queueWorkerStateName(runtime.state.Load()), err)
	}
}

func TestQueueWorkerControlSubjectIsStableAndInstanceSpecific(t *testing.T) {
	cfg := QueueConfig{Subject: "eew.push.task"}
	first := queueWorkerControlSubject(cfg, "instance-a")
	if first != queueWorkerControlSubject(cfg, "instance-a") || first == queueWorkerControlSubject(cfg, "instance-b") {
		t.Fatalf("unexpected control subjects: %q", first)
	}
}

func TestQueueWorkerDeliverySubjectIsStableForSharedConsumer(t *testing.T) {
	cfg := QueueConfig{Stream: "EEW_PUSH", Subject: "eew.push.task", Durable: "eew-push-workers"}
	first := queueWorkerDeliverySubject(cfg)
	second := queueWorkerDeliverySubject(cfg)
	if first != second {
		t.Fatalf("delivery subject is not stable: %q != %q", first, second)
	}
	if first == queueWorkerDeliverySubject(QueueConfig{Stream: cfg.Stream, Subject: cfg.Subject, Durable: "other-workers"}) {
		t.Fatal("different durable consumers share a delivery subject")
	}
}

func TestValidateQueueWorkerConsumerRejectsIncompatibleSharedConsumer(t *testing.T) {
	cfg := QueueConfig{Stream: "EEW_PUSH", Subject: "eew.push.task", Durable: "eew-push-workers"}
	valid := &nats.ConsumerInfo{Config: nats.ConsumerConfig{
		Durable: cfg.Durable, DeliverSubject: queueWorkerDeliverySubject(cfg), DeliverGroup: cfg.Durable,
		FilterSubject: cfg.Subject, AckPolicy: nats.AckExplicitPolicy,
	}}
	if err := validateQueueWorkerConsumer(valid, cfg); err != nil {
		t.Fatalf("valid consumer rejected: %v", err)
	}
	invalid := *valid
	invalid.Config.DeliverGroup = "different-workers"
	if err := validateQueueWorkerConsumer(&invalid, cfg); err == nil {
		t.Fatal("incompatible delivery group accepted")
	}
}
