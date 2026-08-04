package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

type PushQueueClient struct {
	cfg             QueueConfig
	nc              *nats.Conn
	js              nats.JetStreamContext
	controlMu       sync.Mutex
	workerMu        sync.RWMutex
	workers         map[string]queueWorkerObservation
	workerHeartbeat *nats.Subscription
}

type queueWorkerHeartbeat struct {
	ID             string    `json:"id"`
	InstanceID     string    `json:"instance_id,omitempty"`
	NodeID         string    `json:"node_id,omitempty"`
	State          string    `json:"state,omitempty"`
	ControlVersion int       `json:"control_version,omitempty"`
	StartedAt      time.Time `json:"started_at"`
	SentAt         time.Time `json:"sent_at"`
	GoVersion      string    `json:"go_version"`
	Concurrency    int       `json:"concurrency"`
	Inflight       int64     `json:"inflight"`
	Batches        int64     `json:"batches"`
	Targets        int64     `json:"targets"`
	FailedTargets  int64     `json:"failed_targets"`
	LastBatchAt    time.Time `json:"last_batch_at,omitempty"`
}

type queueWorkerObservation struct {
	Heartbeat queueWorkerHeartbeat
	Received  time.Time
}

type queueWorkerHealth struct {
	ID              string `json:"id"`
	InstanceID      string `json:"instance_id"`
	NodeID          string `json:"node_id"`
	State           string `json:"state"`
	Active          bool   `json:"active"`
	SupportsControl bool   `json:"supports_control"`
	StartedAt       string `json:"started_at,omitempty"`
	LastSeenAt      string `json:"last_seen_at,omitempty"`
	LastSeenAge     int64  `json:"last_seen_age_seconds"`
	LastBatchAt     string `json:"last_batch_at,omitempty"`
	GoVersion       string `json:"go_version,omitempty"`
	Concurrency     int    `json:"concurrency"`
	Inflight        int64  `json:"inflight"`
	Batches         int64  `json:"batches"`
	Targets         int64  `json:"targets"`
	FailedTargets   int64  `json:"failed_targets"`
}

type queueWorkerRuntime struct {
	id            string
	instanceID    string
	nodeID        string
	startedAt     time.Time
	concurrency   int
	state         atomic.Int32
	inflight      atomic.Int64
	batches       atomic.Int64
	targets       atomic.Int64
	failedTargets atomic.Int64
	lastBatchUnix atomic.Int64
}

const (
	queueWorkerStateRunning int32 = iota
	queueWorkerStateDraining
	queueWorkerStatePaused
)

const queueWorkerControlVersion = 1

var (
	errQueueWorkerUnavailable = errors.New("worker is offline or no longer observable")
	errQueueWorkerUnsupported = errors.New("worker does not support remote control")
	errLastRunningWorker      = errors.New("refusing to pause the last running worker")
)

type queueWorkerControlCommand struct {
	ID       string    `json:"id"`
	Action   string    `json:"action"`
	IssuedAt time.Time `json:"issued_at"`
}

type queueWorkerControlResponse struct {
	CommandID  string `json:"command_id"`
	Accepted   bool   `json:"accepted"`
	InstanceID string `json:"instance_id"`
	State      string `json:"state"`
	Inflight   int64  `json:"inflight"`
	Error      string `json:"error,omitempty"`
}

func NewPushQueueClient(cfg QueueConfig) (*PushQueueClient, error) {
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, nil
	}
	nc, js, err := connectPushQueue(cfg, "eew-dispatcher")
	if err != nil {
		return nil, err
	}
	client := &PushQueueClient{cfg: cfg, nc: nc, js: js, workers: make(map[string]queueWorkerObservation)}
	if err := client.startWorkerHeartbeatMonitor(); err != nil {
		nc.Close()
		return nil, fmt.Errorf("subscribe to queue worker heartbeats: %w", err)
	}
	return client, nil
}

func connectPushQueue(cfg QueueConfig, name string) (*nats.Conn, nats.JetStreamContext, error) {
	options := []nats.Option{
		nats.Name(name),
		nats.Timeout(3 * time.Second),
		nats.PingInterval(20 * time.Second),
		nats.MaxPingsOutstanding(3),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	}
	if strings.TrimSpace(cfg.Token) != "" {
		options = append(options, nats.Token(strings.TrimSpace(cfg.Token)))
	}
	nc, err := nats.Connect(cfg.URL, options...)
	if err != nil {
		return nil, nil, err
	}
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		nc.Close()
		return nil, nil, err
	}
	if _, err := js.StreamInfo(cfg.Stream); err != nil {
		if !errors.Is(err, nats.ErrStreamNotFound) {
			nc.Close()
			return nil, nil, err
		}
		_, err = js.AddStream(&nats.StreamConfig{
			Name:      cfg.Stream,
			Subjects:  []string{cfg.Subject},
			Retention: nats.WorkQueuePolicy,
			Storage:   nats.FileStorage,
			MaxAge:    10 * time.Minute,
			Discard:   nats.DiscardOld,
		})
		if err != nil {
			nc.Close()
			return nil, nil, err
		}
	}
	return nc, js, nil
}

func (c *PushQueueClient) Close() {
	if c == nil || c.nc == nil {
		return
	}
	if c.workerHeartbeat != nil {
		_ = c.workerHeartbeat.Unsubscribe()
	}
	_ = c.nc.Drain()
	c.nc.Close()
}

func queueWorkerHeartbeatSubject(cfg QueueConfig) string {
	subject := strings.Trim(strings.TrimSpace(cfg.Subject), ".")
	if subject == "" {
		subject = "eew.push.task"
	}
	return subject + ".worker-heartbeat"
}

func queueWorkerControlSubject(cfg QueueConfig, instanceID string) string {
	subject := strings.Trim(strings.TrimSpace(cfg.Subject), ".")
	if subject == "" {
		subject = "eew.push.task"
	}
	return subject + fmt.Sprintf(".worker-control.%016x", fnvString64(strings.TrimSpace(instanceID)))
}

func queueWorkerDeliverySubject(cfg QueueConfig) string {
	subject := strings.Trim(strings.TrimSpace(cfg.Subject), ".")
	if subject == "" {
		subject = "eew.push.task"
	}
	return subject + fmt.Sprintf(".worker-delivery.%016x", fnvString64(strings.TrimSpace(cfg.Stream)+"\x00"+strings.TrimSpace(cfg.Durable)))
}

func queueWorkerAckWait(cfg QueueConfig) time.Duration {
	ackWait := time.Duration(cfg.RetryWindowSeconds+30) * time.Second
	if ackWait < 2*time.Minute {
		ackWait = 2 * time.Minute
	}
	return ackWait
}

func ensureQueueWorkerConsumer(js nats.JetStreamContext, cfg QueueConfig) error {
	if js == nil {
		return errors.New("JetStream worker consumer is unavailable")
	}
	info, err := js.ConsumerInfo(cfg.Stream, cfg.Durable)
	if err == nil {
		return validateQueueWorkerConsumer(info, cfg)
	}
	if !errors.Is(err, nats.ErrConsumerNotFound) {
		return err
	}
	_, err = js.AddConsumer(cfg.Stream, &nats.ConsumerConfig{
		Durable:        cfg.Durable,
		DeliverSubject: queueWorkerDeliverySubject(cfg),
		DeliverGroup:   cfg.Durable,
		FilterSubject:  cfg.Subject,
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        queueWorkerAckWait(cfg),
		MaxAckPending:  64,
	})
	if err == nil {
		return nil
	}
	// Another worker may have created the same durable consumer concurrently.
	info, lookupErr := js.ConsumerInfo(cfg.Stream, cfg.Durable)
	if lookupErr != nil {
		return err
	}
	return validateQueueWorkerConsumer(info, cfg)
}

func validateQueueWorkerConsumer(info *nats.ConsumerInfo, cfg QueueConfig) error {
	if info == nil {
		return errors.New("JetStream worker consumer metadata is empty")
	}
	consumer := info.Config
	if consumer.Durable != cfg.Durable || consumer.FilterSubject != cfg.Subject || consumer.DeliverSubject == "" || consumer.DeliverGroup != cfg.Durable || consumer.AckPolicy != nats.AckExplicitPolicy {
		return fmt.Errorf("JetStream durable consumer %q is incompatible with the worker queue", cfg.Durable)
	}
	return nil
}

func normalizeQueueWorkerIdentity(value, fallback string, maxLength int) string {
	value = strings.TrimSpace(value)
	if value == "" {
		value = strings.TrimSpace(fallback)
	}
	if value == "" {
		value = "worker"
	}
	if len(value) > maxLength {
		value = value[:maxLength]
	}
	return value
}

func queueWorkerStateName(state int32) string {
	switch state {
	case queueWorkerStateDraining:
		return "draining"
	case queueWorkerStatePaused:
		return "paused"
	default:
		return "running"
	}
}

func normalizeQueueWorkerState(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "draining":
		return "draining"
	case "paused":
		return "paused"
	default:
		return "running"
	}
}

func (c *PushQueueClient) startWorkerHeartbeatMonitor() error {
	if c == nil || c.nc == nil {
		return nil
	}
	subscription, err := c.nc.Subscribe(queueWorkerHeartbeatSubject(c.cfg), func(msg *nats.Msg) {
		var heartbeat queueWorkerHeartbeat
		if json.Unmarshal(msg.Data, &heartbeat) != nil {
			return
		}
		heartbeat.ID = strings.TrimSpace(heartbeat.ID)
		heartbeat.InstanceID = strings.TrimSpace(heartbeat.InstanceID)
		if heartbeat.ID == "" || len(heartbeat.ID) > 80 || len(heartbeat.InstanceID) > 120 || heartbeat.Concurrency < 0 || heartbeat.Inflight < 0 {
			return
		}
		if heartbeat.InstanceID == "" {
			heartbeat.InstanceID = heartbeat.ID
		}
		heartbeat.NodeID = normalizeQueueWorkerIdentity(heartbeat.NodeID, "unknown-node", 80)
		heartbeat.State = normalizeQueueWorkerState(heartbeat.State)
		now := time.Now()
		if heartbeat.SentAt.After(now.Add(5 * time.Minute)) {
			return
		}
		c.workerMu.Lock()
		c.workers[heartbeat.InstanceID] = queueWorkerObservation{Heartbeat: heartbeat, Received: now}
		c.workerMu.Unlock()
	})
	if err != nil {
		return err
	}
	c.workerHeartbeat = subscription
	return c.nc.FlushTimeout(2 * time.Second)
}

func (c *PushQueueClient) workerHealth(now time.Time) ([]queueWorkerHealth, int, time.Duration) {
	if c == nil {
		return nil, 0, 0
	}
	heartbeatEvery := time.Duration(c.cfg.WorkerHeartbeatSecond) * time.Second
	if heartbeatEvery <= 0 {
		heartbeatEvery = 10 * time.Second
	}
	staleAfter := 3*heartbeatEvery + 5*time.Second
	c.workerMu.RLock()
	observations := make([]queueWorkerObservation, 0, len(c.workers))
	for _, observation := range c.workers {
		observations = append(observations, observation)
	}
	c.workerMu.RUnlock()
	workers := make([]queueWorkerHealth, 0, len(observations))
	for _, observation := range observations {
		age := now.Sub(observation.Received)
		if age < 0 {
			age = 0
		}
		if age > 10*staleAfter {
			continue
		}
		heartbeat := observation.Heartbeat
		worker := queueWorkerHealth{
			ID:              heartbeat.ID,
			InstanceID:      heartbeat.InstanceID,
			NodeID:          heartbeat.NodeID,
			State:           normalizeQueueWorkerState(heartbeat.State),
			Active:          age <= staleAfter,
			SupportsControl: heartbeat.ControlVersion >= queueWorkerControlVersion,
			LastSeenAt:      observation.Received.UTC().Format(time.RFC3339),
			LastSeenAge:     int64(age.Seconds()),
			GoVersion:       heartbeat.GoVersion,
			Concurrency:     heartbeat.Concurrency,
			Inflight:        heartbeat.Inflight,
			Batches:         heartbeat.Batches,
			Targets:         heartbeat.Targets,
			FailedTargets:   heartbeat.FailedTargets,
		}
		if !heartbeat.StartedAt.IsZero() {
			worker.StartedAt = heartbeat.StartedAt.UTC().Format(time.RFC3339)
		}
		if !heartbeat.LastBatchAt.IsZero() {
			worker.LastBatchAt = heartbeat.LastBatchAt.UTC().Format(time.RFC3339)
		}
		workers = append(workers, worker)
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].ID < workers[j].ID })
	expected := c.cfg.ExpectedWorkers
	if expected <= 0 {
		expected = len(workers)
		if expected == 0 {
			expected = 1
		}
	}
	return workers, expected, staleAfter
}

func (c *PushQueueClient) controlWorker(ctx context.Context, instanceID, action string) (queueWorkerControlResponse, error) {
	var empty queueWorkerControlResponse
	if c == nil || c.nc == nil || !c.nc.IsConnected() {
		return empty, errors.New("NATS worker control is unavailable")
	}
	instanceID = strings.TrimSpace(instanceID)
	action = strings.ToLower(strings.TrimSpace(action))
	if instanceID == "" || len(instanceID) > 120 {
		return empty, errors.New("invalid worker instance")
	}
	if action != "pause" && action != "drain" && action != "resume" {
		return empty, errors.New("invalid worker action")
	}
	c.controlMu.Lock()
	defer c.controlMu.Unlock()
	now := time.Now()
	workers, _, staleAfter := c.workerHealth(now)
	_, err := validateQueueWorkerControlTarget(workers, instanceID, action, staleAfter)
	if err != nil {
		return empty, err
	}
	command := queueWorkerControlCommand{
		ID:       fmt.Sprintf("%x-%x", now.UnixNano(), fnvString64(instanceID+"\x00"+action)),
		Action:   action,
		IssuedAt: now.UTC(),
	}
	payload, err := json.Marshal(command)
	if err != nil {
		return empty, err
	}
	message, err := c.nc.RequestWithContext(ctx, queueWorkerControlSubject(c.cfg, instanceID), payload)
	if err != nil {
		return empty, fmt.Errorf("worker control request: %w", err)
	}
	var response queueWorkerControlResponse
	if err := json.Unmarshal(message.Data, &response); err != nil {
		return empty, fmt.Errorf("decode worker control response: %w", err)
	}
	if response.CommandID != command.ID || response.InstanceID != instanceID {
		return empty, errors.New("worker control response identity mismatch")
	}
	if !response.Accepted {
		if response.Error == "" {
			response.Error = "worker rejected the command"
		}
		return response, errors.New(response.Error)
	}
	c.workerMu.Lock()
	if observation, ok := c.workers[instanceID]; ok {
		observation.Heartbeat.State = normalizeQueueWorkerState(response.State)
		observation.Heartbeat.Inflight = response.Inflight
		observation.Received = time.Now()
		c.workers[instanceID] = observation
	}
	c.workerMu.Unlock()
	return response, nil
}

func validateQueueWorkerControlTarget(workers []queueWorkerHealth, instanceID, action string, staleAfter time.Duration) (*queueWorkerHealth, error) {
	var target *queueWorkerHealth
	runningOthers := 0
	for index := range workers {
		worker := &workers[index]
		if worker.InstanceID == instanceID {
			target = worker
			continue
		}
		if worker.Active && worker.State == "running" {
			runningOthers++
		}
	}
	if target == nil || !target.Active || target.LastSeenAge > int64(staleAfter.Seconds()) {
		return nil, errQueueWorkerUnavailable
	}
	if !target.SupportsControl {
		return nil, errQueueWorkerUnsupported
	}
	if (action == "pause" || action == "drain") && target.State == "running" && runningOthers == 0 {
		return nil, errLastRunningWorker
	}
	return target, nil
}

func fnvString64(value string) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(value))
	return hash.Sum64()
}

func newQueueWorkerRuntime(cfg QueueConfig) *queueWorkerRuntime {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		hostname = fmt.Sprintf("host-%d", os.Getpid())
	}
	id := normalizeQueueWorkerIdentity(cfg.WorkerID, hostname, 80)
	nodeID := normalizeQueueWorkerIdentity(cfg.NodeID, hostname, 80)
	startedAt := time.Now().UTC()
	instanceID := fmt.Sprintf("%s-%x-%x", id, startedAt.UnixNano(), os.Getpid())
	if len(instanceID) > 120 {
		instanceID = instanceID[len(instanceID)-120:]
	}
	runtime := &queueWorkerRuntime{id: id, instanceID: instanceID, nodeID: nodeID, startedAt: startedAt, concurrency: cfg.WorkerConcurrency}
	runtime.state.Store(queueWorkerStateRunning)
	return runtime
}

func (r *queueWorkerRuntime) heartbeat() queueWorkerHeartbeat {
	heartbeat := queueWorkerHeartbeat{
		ID:             r.id,
		InstanceID:     r.instanceID,
		NodeID:         r.nodeID,
		State:          queueWorkerStateName(r.state.Load()),
		ControlVersion: queueWorkerControlVersion,
		StartedAt:      r.startedAt,
		SentAt:         time.Now().UTC(),
		GoVersion:      runtime.Version(),
		Concurrency:    r.concurrency,
		Inflight:       r.inflight.Load(),
		Batches:        r.batches.Load(),
		Targets:        r.targets.Load(),
		FailedTargets:  r.failedTargets.Load(),
	}
	if unix := r.lastBatchUnix.Load(); unix > 0 {
		heartbeat.LastBatchAt = time.Unix(unix, 0).UTC()
	}
	return heartbeat
}

func publishQueueWorkerHeartbeat(nc *nats.Conn, cfg QueueConfig, worker *queueWorkerRuntime) {
	if nc == nil || worker == nil {
		return
	}
	data, err := json.Marshal(worker.heartbeat())
	if err == nil {
		_ = nc.Publish(queueWorkerHeartbeatSubject(cfg), data)
	}
}

func maintainQueueWorkerHeartbeat(ctx context.Context, nc *nats.Conn, cfg QueueConfig, worker *queueWorkerRuntime) {
	every := time.Duration(cfg.WorkerHeartbeatSecond) * time.Second
	if every <= 0 {
		every = 10 * time.Second
	}
	publishQueueWorkerHeartbeat(nc, cfg, worker)
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			publishQueueWorkerHeartbeat(nc, cfg, worker)
		}
	}
}

type queueWorkerController struct {
	ctx      context.Context
	cfg      QueueConfig
	notifier *Notifier
	nc       *nats.Conn
	js       nats.JetStreamContext
	runtime  *queueWorkerRuntime
	sem      chan struct{}
	mu       sync.Mutex
	taskSub  *nats.Subscription
}

func (c *queueWorkerController) subscribeLocked() error {
	if c.taskSub != nil && c.taskSub.IsValid() {
		return nil
	}
	if err := ensureQueueWorkerConsumer(c.js, c.cfg); err != nil {
		return err
	}
	subscription, err := c.js.QueueSubscribe(c.cfg.Subject, c.cfg.Durable, c.handleTask,
		nats.Bind(c.cfg.Stream, c.cfg.Durable), nats.ManualAck())
	if err != nil {
		return err
	}
	c.taskSub = subscription
	return nil
}

func (c *queueWorkerController) handleTask(msg *nats.Msg) {
	if c.runtime.state.Load() != queueWorkerStateRunning {
		_ = msg.NakWithDelay(500 * time.Millisecond)
		return
	}
	c.runtime.inflight.Add(1)
	defer func() {
		remaining := c.runtime.inflight.Add(-1)
		if remaining == 0 && c.runtime.state.CompareAndSwap(queueWorkerStateDraining, queueWorkerStatePaused) {
			publishQueueWorkerHeartbeat(c.nc, c.cfg, c.runtime)
		}
	}()
	var payload relayFanoutRequest
	if err := json.Unmarshal(msg.Data, &payload); err != nil || payload.ID == "" || len(payload.Targets) == 0 || len(payload.Targets) > c.cfg.BatchSize {
		_ = msg.Term()
		return
	}
	started := time.Now()
	response := processQueuedFanout(c.ctx, c.notifier, payload, c.sem, c.cfg)
	c.runtime.batches.Add(1)
	c.runtime.targets.Add(int64(len(payload.Targets)))
	for _, result := range response.Results {
		if !result.Pushed {
			c.runtime.failedTargets.Add(1)
		}
	}
	c.runtime.lastBatchUnix.Store(time.Now().Unix())
	log.Printf("push queue batch processed id=%s targets=%d duration=%s", payload.ID, len(payload.Targets), time.Since(started))
	data, err := json.Marshal(response)
	if err != nil {
		_ = msg.Nak()
		return
	}
	if payload.ResultSubject == "" || c.nc.Publish(payload.ResultSubject, data) != nil || c.nc.Flush() != nil {
		_ = msg.NakWithDelay(250 * time.Millisecond)
		return
	}
	_ = msg.AckSync()
}

func (c *queueWorkerController) stopAccepting(state int32) error {
	c.runtime.state.Store(state)
	c.mu.Lock()
	subscription := c.taskSub
	c.taskSub = nil
	c.mu.Unlock()
	if subscription != nil {
		if err := subscription.Unsubscribe(); err != nil && !errors.Is(err, nats.ErrBadSubscription) {
			return err
		}
	}
	if state == queueWorkerStateDraining && c.runtime.inflight.Load() == 0 {
		c.runtime.state.Store(queueWorkerStatePaused)
	}
	return nil
}

func (c *queueWorkerController) resume() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.taskSub != nil && c.taskSub.IsValid() {
		c.runtime.state.Store(queueWorkerStateRunning)
		return nil
	}
	c.runtime.state.Store(queueWorkerStateRunning)
	if err := c.subscribeLocked(); err != nil {
		c.runtime.state.Store(queueWorkerStatePaused)
		return err
	}
	return nil
}

func (c *queueWorkerController) handleControl(msg *nats.Msg) {
	response := queueWorkerControlResponse{InstanceID: c.runtime.instanceID, State: queueWorkerStateName(c.runtime.state.Load()), Inflight: c.runtime.inflight.Load()}
	var command queueWorkerControlCommand
	if err := json.Unmarshal(msg.Data, &command); err != nil {
		response.Error = "invalid control command"
	} else {
		response.CommandID = strings.TrimSpace(command.ID)
		action := strings.ToLower(strings.TrimSpace(command.Action))
		now := time.Now()
		if response.CommandID == "" || len(response.CommandID) > 120 || command.IssuedAt.IsZero() || command.IssuedAt.Before(now.Add(-2*time.Minute)) || command.IssuedAt.After(now.Add(30*time.Second)) {
			response.Error = "expired or invalid control command"
		} else {
			var err error
			switch action {
			case "pause":
				err = c.stopAccepting(queueWorkerStatePaused)
			case "drain":
				err = c.stopAccepting(queueWorkerStateDraining)
			case "resume":
				err = c.resume()
			default:
				err = errors.New("unsupported worker action")
			}
			if err != nil {
				response.Error = err.Error()
			} else {
				response.Accepted = true
			}
		}
	}
	response.State = queueWorkerStateName(c.runtime.state.Load())
	response.Inflight = c.runtime.inflight.Load()
	publishQueueWorkerHeartbeat(c.nc, c.cfg, c.runtime)
	if data, err := json.Marshal(response); err == nil && msg.Reply != "" {
		_ = msg.Respond(data)
	}
}

func sendFanoutQueueGroup(ctx context.Context, notifier *Notifier, event Event, targets []fanoutTarget, out chan<- fanoutResult) {
	if len(targets) == 0 || notifier == nil || notifier.queue == nil {
		return
	}
	batchSize := notifier.queue.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 500
	}
	type queueBatch struct {
		index   int
		targets []fanoutTarget
	}
	var batches []queueBatch
	for start := 0; start < len(targets); start += batchSize {
		end := start + batchSize
		if end > len(targets) {
			end = len(targets)
		}
		batches = append(batches, queueBatch{index: len(batches), targets: targets[start:end]})
	}
	jobs := make(chan queueBatch)
	workers := notifier.queue.cfg.MaxInflightBatches
	if workers <= 0 {
		workers = 8
	}
	if workers > len(batches) {
		workers = len(batches)
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range jobs {
				results, err := notifier.queue.sendBatch(ctx, event, batch.index, batch.targets)
				if err != nil {
					log.Printf("push queue batch fallback id=%s report=%d batch=%d size=%d: %v", event.EventID, event.ReportNum, batch.index, len(batch.targets), err)
					sendFanoutGroup(ctx, notifier, event, batch.targets, len(batch.targets), out)
					continue
				}
				for _, result := range results {
					out <- result
				}
			}
		}()
	}
	for _, batch := range batches {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return
		case jobs <- batch:
		}
	}
	close(jobs)
	wg.Wait()
}

func sendFanoutQueuePlans(ctx context.Context, cfg Config, notifier *Notifier, alertCache *AlertCache, event Event, plans []queuedFanoutPlan, collect bool) (int, int, int, []fanoutResult) {
	if len(plans) == 0 || notifier == nil || notifier.queue == nil {
		return 0, 0, 0, nil
	}
	batchSize := notifier.queue.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 250
	}
	type planBatch struct {
		index int
		plans []queuedFanoutPlan
	}
	var batches []planBatch
	for start := 0; start < len(plans); start += batchSize {
		end := start + batchSize
		if end > len(plans) {
			end = len(plans)
		}
		batches = append(batches, planBatch{index: len(batches), plans: plans[start:end]})
	}
	jobs := make(chan planBatch)
	results := make(chan fanoutResult, batchSize)
	workers := notifier.queue.cfg.MaxInflightBatches
	if workers <= 0 {
		workers = 2
	}
	if workers > len(batches) {
		workers = len(batches)
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range jobs {
				targets := make([]fanoutTarget, len(batch.plans))
				for index, plan := range batch.plans {
					options := pushOptions(cfg, event, plan.Sub, plan.Decision)
					targets[index] = buildFanoutTarget(cfg, event, alertCache, plan.Sub, plan.Decision, options, plan.Level, plan.Priority)
				}
				batchResults, err := notifier.queue.sendBatch(ctx, event, batch.index, targets)
				if err != nil {
					log.Printf("push queue batch fallback id=%s report=%d batch=%d size=%d: %v", event.EventID, event.ReportNum, batch.index, len(targets), err)
					fallback := make(chan fanoutResult, len(targets))
					go func() {
						sendFanoutGroup(ctx, notifier, event, targets, len(targets), fallback)
						close(fallback)
					}()
					for result := range fallback {
						results <- result
					}
					continue
				}
				for _, result := range batchResults {
					results <- result
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, batch := range batches {
			select {
			case <-ctx.Done():
				return
			case jobs <- batch:
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()
	verbose := len(plans) <= 10000
	var pushed, skipped, failed int
	var collected []fanoutResult
	if collect {
		collected = make([]fanoutResult, 0, len(plans))
	}
	for result := range results {
		if collect {
			collected = append(collected, result)
		}
		switch {
		case result.Pushed:
			pushed++
			if verbose {
				log.Printf("pushed id=%s report=%d bark=%s server=%s type=%s level=%s elapsed=%s retries=%d", event.EventID, event.ReportNum, maskKey(result.Target.Sub.BarkID), result.Target.Sub.BarkServer, event.Type, result.Target.Level, result.Elapsed, result.Retries)
			}
		case result.Skipped:
			skipped++
		default:
			failed++
			log.Printf("bark send failed id=%s bark=%s server=%s type=%s level=%s: %v", event.EventID, maskKey(result.Target.Sub.BarkID), result.Target.Sub.BarkServer, event.Type, result.Target.Level, result.Err)
		}
	}
	return pushed, skipped, failed, collected
}

func (c *PushQueueClient) sendBatch(ctx context.Context, event Event, batchIndex int, targets []fanoutTarget) ([]fanoutResult, error) {
	payload := relayFanoutRequest{ID: relayBatchID(event, batchIndex, targets), Targets: make([]relayPushTarget, len(targets))}
	for i, target := range targets {
		payload.Targets[i] = relayPushTarget{
			Server: target.Sub.BarkServer, Key: target.Sub.BarkID, Title: target.Title,
			Subtitle: target.Subtitle, Body: target.Body, Params: target.Params, Options: target.Options,
		}
	}
	inbox := nats.NewInbox()
	payload.ResultSubject = inbox
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	sub, err := c.nc.SubscribeSync(inbox)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()
	if err := c.nc.Flush(); err != nil {
		return nil, err
	}
	msg := &nats.Msg{Subject: c.cfg.Subject, Data: data, Header: nats.Header{}}
	msg.Header.Set(nats.MsgIdHdr, payload.ID)
	if _, err := c.js.PublishMsg(msg); err != nil {
		return nil, err
	}
	timeout := time.Duration(c.cfg.RequestTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resultMsg, err := sub.NextMsgWithContext(waitCtx)
	if err != nil {
		return nil, err
	}
	var response relayFanoutResponse
	if err := json.Unmarshal(resultMsg.Data, &response); err != nil {
		return nil, err
	}
	return fanoutResultsFromResponse(payload, response, targets)
}

func fanoutResultsFromResponse(payload relayFanoutRequest, response relayFanoutResponse, targets []fanoutTarget) ([]fanoutResult, error) {
	if response.ID != payload.ID || len(response.Results) != len(targets) {
		return nil, errors.New("queue response mismatch")
	}
	results := make([]fanoutResult, len(targets))
	seen := make([]bool, len(targets))
	for _, item := range response.Results {
		if item.Index < 0 || item.Index >= len(targets) || seen[item.Index] {
			return nil, errors.New("queue result index mismatch")
		}
		seen[item.Index] = true
		result := fanoutResult{Target: targets[item.Index], Pushed: item.Pushed, Retries: item.Retries, Elapsed: time.Duration(item.ElapsedMS) * time.Millisecond}
		if item.FirstAttemptDoneAtUnixMS > 0 {
			result.FirstAttemptDoneAt = time.UnixMilli(item.FirstAttemptDoneAtUnixMS)
		}
		result.FirstAttemptOK = item.FirstAttemptOK
		result.FirstAttemptKnown = item.FirstAttemptKnown
		if item.Error != "" {
			result.Err = errors.New(item.Error)
		}
		results[item.Index] = result
	}
	return results, nil
}

func servePushQueueWorker(ctx context.Context, cfg Config, notifier *Notifier) error {
	if strings.TrimSpace(cfg.Queue.URL) == "" {
		return errors.New("queue URL is required")
	}
	nc, js, err := connectPushQueue(cfg.Queue, "eew-push-worker")
	if err != nil {
		return err
	}
	defer nc.Close()
	workerRuntime := newQueueWorkerRuntime(cfg.Queue)
	controller := &queueWorkerController{
		ctx: ctx, cfg: cfg.Queue, notifier: notifier, nc: nc, js: js,
		runtime: workerRuntime, sem: make(chan struct{}, cfg.Queue.WorkerConcurrency),
	}
	controller.mu.Lock()
	err = controller.subscribeLocked()
	controller.mu.Unlock()
	if err != nil {
		return err
	}
	controlSubscription, err := nc.Subscribe(queueWorkerControlSubject(cfg.Queue, workerRuntime.instanceID), controller.handleControl)
	if err != nil {
		return fmt.Errorf("subscribe to worker control: %w", err)
	}
	defer controlSubscription.Unsubscribe()
	if err := nc.FlushTimeout(2 * time.Second); err != nil {
		return fmt.Errorf("activate worker control: %w", err)
	}
	go maintainQueueWorkerHeartbeat(ctx, nc, cfg.Queue, workerRuntime)
	log.Printf("push queue worker id=%s instance=%s node=%s subject=%s durable=%s concurrency=%d retry_attempts=%d retry_window=%ds",
		workerRuntime.id, workerRuntime.instanceID, workerRuntime.nodeID, cfg.Queue.Subject, cfg.Queue.Durable, cfg.Queue.WorkerConcurrency, cfg.Queue.RetryAttempts, cfg.Queue.RetryWindowSeconds)
	<-ctx.Done()
	_ = controller.stopAccepting(queueWorkerStateDraining)
	_ = nc.Drain()
	return nil
}

func processQueuedFanout(ctx context.Context, notifier *Notifier, payload relayFanoutRequest, sem chan struct{}, cfg QueueConfig) relayFanoutResponse {
	response := relayFanoutResponse{ID: payload.ID, Results: make([]relayItemResult, len(payload.Targets))}
	var wg sync.WaitGroup
	for index := range payload.Targets {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			target := payload.Targets[index]
			response.Results[index] = sendQueuedTargetWithRetry(ctx, notifier, target, index, sem, cfg)
		}(index)
	}
	wg.Wait()
	return response
}

func sendQueuedTargetWithRetry(ctx context.Context, notifier *Notifier, target relayPushTarget, index int, sem chan struct{}, cfg QueueConfig) relayItemResult {
	started := time.Now()
	deadline := started.Add(time.Duration(cfg.RetryWindowSeconds) * time.Second)
	totalRetries := 0
	durableRetries := 0
	var lastErr error
	var firstAttemptDoneAt time.Time
	var firstAttemptOK bool
	for {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			lastErr = ctx.Err()
			item := relayItemResult{Index: index, Error: lastErr.Error(), Retries: totalRetries, ElapsedMS: time.Since(started).Milliseconds()}
			if !firstAttemptDoneAt.IsZero() {
				item.FirstAttemptDoneAtUnixMS = firstAttemptDoneAt.UnixMilli()
				item.FirstAttemptOK = firstAttemptOK
				item.FirstAttemptKnown = true
			}
			return item
		}
		if durableRetries > 0 {
			totalRetries++
		}
		retries, roundFirstAttemptDoneAt, roundFirstAttemptOK, err := notifier.SendWithRetryTiming(ctx, target.Server, target.Key, target.Title, target.Subtitle, target.Body, target.Params, target.Options)
		if firstAttemptDoneAt.IsZero() {
			firstAttemptDoneAt = roundFirstAttemptDoneAt
			firstAttemptOK = roundFirstAttemptOK
		}
		<-sem
		totalRetries += retries
		if err == nil {
			return relayItemResult{Index: index, Pushed: true, Retries: totalRetries, ElapsedMS: time.Since(started).Milliseconds(), FirstAttemptDoneAtUnixMS: firstAttemptDoneAt.UnixMilli(), FirstAttemptOK: firstAttemptOK, FirstAttemptKnown: true}
		}
		lastErr = err
		if !retryableBarkError(err) || durableRetries >= cfg.RetryAttempts {
			break
		}
		durableRetries++
		delay := queuedRetryDelay(cfg, durableRetries, target.Key)
		if !deadline.After(time.Now().Add(delay)) {
			break
		}
		select {
		case <-ctx.Done():
			lastErr = ctx.Err()
			break
		case <-time.After(delay):
			continue
		}
		break
	}
	item := relayItemResult{Index: index, Retries: totalRetries, ElapsedMS: time.Since(started).Milliseconds()}
	if !firstAttemptDoneAt.IsZero() {
		item.FirstAttemptDoneAtUnixMS = firstAttemptDoneAt.UnixMilli()
		item.FirstAttemptOK = firstAttemptOK
		item.FirstAttemptKnown = true
	}
	if lastErr != nil {
		item.Error = lastErr.Error()
	}
	return item
}

func queuedRetryDelay(cfg QueueConfig, attempt int, key string) time.Duration {
	base := time.Duration(cfg.RetryBaseDelayMS) * time.Millisecond
	maximum := time.Duration(cfg.RetryMaxDelayMS) * time.Millisecond
	if base <= 0 {
		base = time.Second
	}
	if maximum < base {
		maximum = base
	}
	delay := base
	for i := 1; i < attempt && delay < maximum; i++ {
		delay *= 2
		if delay > maximum {
			delay = maximum
		}
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))
	_, _ = hash.Write([]byte{byte(attempt)})
	jitterRange := delay / 4
	if jitterRange <= 0 {
		return delay
	}
	return delay + time.Duration(hash.Sum32()%uint32(jitterRange))
}
