package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type FanoutRelayClient struct {
	cfg    RelayConfig
	client *http.Client
}

type relayPushTarget struct {
	Server   string            `json:"server"`
	Key      string            `json:"key"`
	Title    string            `json:"title"`
	Subtitle string            `json:"subtitle"`
	Body     string            `json:"body"`
	Params   map[string]string `json:"params,omitempty"`
	Options  PushOptions       `json:"options"`
}

type relayFanoutRequest struct {
	ID            string            `json:"id"`
	ResultSubject string            `json:"result_subject,omitempty"`
	Targets       []relayPushTarget `json:"targets"`
}

type relayItemResult struct {
	Index                    int    `json:"index"`
	Pushed                   bool   `json:"pushed"`
	Error                    string `json:"error,omitempty"`
	Retries                  int    `json:"retries"`
	ElapsedMS                int64  `json:"elapsed_ms"`
	FirstAttemptDoneAtUnixMS int64  `json:"first_attempt_done_at_unix_ms,omitempty"`
	FirstAttemptOK           bool   `json:"first_attempt_ok,omitempty"`
	FirstAttemptKnown        bool   `json:"first_attempt_known,omitempty"`
}

type relayFanoutResponse struct {
	ID      string            `json:"id"`
	Results []relayItemResult `json:"results"`
}

type relayCacheEntry struct {
	response  relayFanoutResponse
	createdAt time.Time
}

type fanoutRelayRuntime struct {
	cfg      RelayConfig
	notifier *Notifier
	sem      chan struct{}
	mu       sync.Mutex
	cache    map[string]relayCacheEntry
	order    []string
}

func NewFanoutRelayClient(cfg RelayConfig) *FanoutRelayClient {
	if strings.TrimSpace(cfg.URL) == "" || strings.TrimSpace(cfg.Token) == "" || cfg.SharePercent <= 0 {
		return nil
	}
	timeout := time.Duration(cfg.RequestTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 8 * time.Second
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&netDialer).DialContext,
		TLSHandshakeTimeout:   3 * time.Second,
		ResponseHeaderTimeout: timeout,
		MaxIdleConns:          32,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
		ForceAttemptHTTP2:     true,
	}
	return &FanoutRelayClient{cfg: cfg, client: &http.Client{Timeout: timeout, Transport: transport}}
}

var netDialer = net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}

func serveFanoutRelay(ctx context.Context, cfg Config, notifier *Notifier) error {
	if strings.TrimSpace(cfg.Relay.Token) == "" {
		return errors.New("relay token is required")
	}
	concurrency := cfg.Relay.Concurrency
	if concurrency <= 0 {
		concurrency = 400
	}
	runtime := &fanoutRelayRuntime{
		cfg:      cfg.Relay,
		notifier: notifier,
		sem:      make(chan struct{}, concurrency),
		cache:    make(map[string]relayCacheEntry),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "ok"})
	})
	mux.HandleFunc("POST /internal/fanout", runtime.handleFanout)
	server := &http.Server{
		Addr:              cfg.Relay.Listen,
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      25 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdownCtx)
		case <-done:
		}
	}()
	log.Printf("fanout relay listening=%s concurrency=%d max_batch=%d", cfg.Relay.Listen, concurrency, cfg.Relay.MaxBatchSize)
	err := server.ListenAndServe()
	close(done)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (r *fanoutRelayRuntime) handleFanout(w http.ResponseWriter, req *http.Request) {
	if !relayAuthorized(req.Header.Get("Authorization"), r.cfg.Token) {
		writeJSON(w, http.StatusUnauthorized, APIResponse{Success: false, Message: "unauthorized"})
		return
	}
	maxBatch := r.cfg.MaxBatchSize
	if maxBatch <= 0 {
		maxBatch = 500
	}
	req.Body = http.MaxBytesReader(w, req.Body, 8<<20)
	defer req.Body.Close()
	var payload relayFanoutRequest
	decoder := json.NewDecoder(req.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "invalid request"})
		return
	}
	if strings.TrimSpace(payload.ID) == "" || len(payload.Targets) == 0 || len(payload.Targets) > maxBatch {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "invalid batch"})
		return
	}
	if cached, ok := r.cached(payload.ID); ok {
		writeRelayJSON(w, http.StatusOK, cached)
		return
	}

	response := relayFanoutResponse{ID: payload.ID, Results: make([]relayItemResult, len(payload.Targets))}
	var wg sync.WaitGroup
	for i := range payload.Targets {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			select {
			case r.sem <- struct{}{}:
				defer func() { <-r.sem }()
			case <-req.Context().Done():
				response.Results[index] = relayItemResult{Index: index, Error: req.Context().Err().Error()}
				return
			}
			target := payload.Targets[index]
			started := time.Now()
			retries, firstAttemptDoneAt, firstAttemptOK, err := r.notifier.SendWithRetryTiming(req.Context(), target.Server, target.Key, target.Title, target.Subtitle, target.Body, target.Params, target.Options)
			result := relayItemResult{Index: index, Pushed: err == nil, Retries: retries, ElapsedMS: time.Since(started).Milliseconds()}
			if !firstAttemptDoneAt.IsZero() {
				result.FirstAttemptDoneAtUnixMS = firstAttemptDoneAt.UnixMilli()
				result.FirstAttemptOK = firstAttemptOK
				result.FirstAttemptKnown = true
			}
			if err != nil {
				result.Error = err.Error()
			}
			response.Results[index] = result
		}(i)
	}
	wg.Wait()
	r.store(payload.ID, response)
	writeRelayJSON(w, http.StatusOK, response)
}

func writeRelayJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func relayAuthorized(header, token string) bool {
	got := strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
	want := strings.TrimSpace(token)
	if got == "" || want == "" || len(got) != len(want) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func (r *fanoutRelayRuntime) cached(id string) (relayFanoutResponse, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked(time.Now())
	entry, ok := r.cache[id]
	return entry.response, ok
}

func (r *fanoutRelayRuntime) store(id string, response relayFanoutResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked(time.Now())
	if _, exists := r.cache[id]; !exists {
		r.order = append(r.order, id)
	}
	r.cache[id] = relayCacheEntry{response: response, createdAt: time.Now()}
	for len(r.order) > 256 {
		delete(r.cache, r.order[0])
		r.order = r.order[1:]
	}
}

func (r *fanoutRelayRuntime) pruneLocked(now time.Time) {
	cutoff := now.Add(-10 * time.Minute)
	keep := r.order[:0]
	for _, id := range r.order {
		entry, ok := r.cache[id]
		if !ok || entry.createdAt.Before(cutoff) {
			delete(r.cache, id)
			continue
		}
		keep = append(keep, id)
	}
	r.order = keep
}

func splitRelayTargets(notifier *Notifier, targets []fanoutTarget) ([]fanoutTarget, []fanoutTarget) {
	if notifier == nil || notifier.relay == nil {
		return targets, nil
	}
	local := make([]fanoutTarget, 0, len(targets))
	remote := make([]fanoutTarget, 0, len(targets)*notifier.relay.cfg.SharePercent/100)
	for _, target := range targets {
		if relayOwnsBarkID(notifier, target.Sub.BarkID) {
			remote = append(remote, target)
		} else {
			local = append(local, target)
		}
	}
	return local, remote
}

func relayOwnsBarkID(notifier *Notifier, barkID string) bool {
	if notifier == nil || notifier.relay == nil || notifier.relay.cfg.SharePercent <= 0 {
		return false
	}
	sum := sha256.Sum256([]byte(barkID))
	bucket := int(sum[0])<<8 | int(sum[1])
	return bucket%100 < notifier.relay.cfg.SharePercent
}

func sendFanoutRelayGroup(ctx context.Context, notifier *Notifier, event Event, targets []fanoutTarget, out chan<- fanoutResult) {
	if len(targets) == 0 || notifier == nil || notifier.relay == nil {
		return
	}
	batchSize := notifier.relay.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 250
	}
	type relayBatch struct {
		index   int
		targets []fanoutTarget
	}
	var batches []relayBatch
	for start := 0; start < len(targets); start += batchSize {
		end := start + batchSize
		if end > len(targets) {
			end = len(targets)
		}
		batches = append(batches, relayBatch{index: len(batches), targets: targets[start:end]})
	}
	jobs := make(chan relayBatch)
	workers := notifier.relay.cfg.MaxInflightBatches
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
				results, err := notifier.relay.sendBatch(ctx, event, batch.index, batch.targets)
				if err != nil {
					log.Printf("fanout relay batch fallback id=%s report=%d batch=%d size=%d: %v", event.EventID, event.ReportNum, batch.index, len(batch.targets), err)
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

func (c *FanoutRelayClient) sendBatch(ctx context.Context, event Event, batchIndex int, targets []fanoutTarget) ([]fanoutResult, error) {
	payload := relayFanoutRequest{ID: relayBatchID(event, batchIndex, targets), Targets: make([]relayPushTarget, len(targets))}
	for i, target := range targets {
		payload.Targets[i] = relayPushTarget{
			Server:   target.Sub.BarkServer,
			Key:      target.Sub.BarkID,
			Title:    target.Title,
			Subtitle: target.Subtitle,
			Body:     target.Body,
			Params:   target.Params,
			Options:  target.Options,
		}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	var response relayFanoutResponse
	for attempt := 0; attempt < 2; attempt++ {
		response, err = c.doBatchRequest(ctx, body)
		if err == nil {
			break
		}
		if attempt == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if response.ID != payload.ID || len(response.Results) != len(targets) {
		return nil, errors.New("relay response mismatch")
	}
	results := make([]fanoutResult, len(targets))
	seen := make([]bool, len(targets))
	for _, item := range response.Results {
		if item.Index < 0 || item.Index >= len(targets) || seen[item.Index] {
			return nil, errors.New("relay result index mismatch")
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

func (c *FanoutRelayClient) doBatchRequest(ctx context.Context, body []byte) (relayFanoutResponse, error) {
	endpoint := strings.TrimRight(c.cfg.URL, "/") + "/internal/fanout"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return relayFanoutResponse{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "eew-bark-relay/1.0")
	res, err := c.client.Do(req)
	if err != nil {
		return relayFanoutResponse{}, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(res.Body, 2048))
		return relayFanoutResponse{}, fmt.Errorf("relay http %d: %s", res.StatusCode, strings.TrimSpace(string(message)))
	}
	var response relayFanoutResponse
	if err := json.NewDecoder(io.LimitReader(res.Body, 8<<20)).Decode(&response); err != nil {
		return relayFanoutResponse{}, err
	}
	return response, nil
}

func relayBatchID(event Event, batchIndex int, targets []fanoutTarget) string {
	hash := sha256.New()
	_, _ = io.WriteString(hash, event.EventID)
	_, _ = io.WriteString(hash, "|")
	_, _ = io.WriteString(hash, strconv.Itoa(event.ReportNum))
	_, _ = io.WriteString(hash, "|")
	_, _ = io.WriteString(hash, strconv.Itoa(batchIndex))
	for _, target := range targets {
		_, _ = io.WriteString(hash, "|")
		_, _ = io.WriteString(hash, target.Sub.BarkID)
	}
	return hex.EncodeToString(hash.Sum(nil))
}
