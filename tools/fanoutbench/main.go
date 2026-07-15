package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	elapsed time.Duration
	retries int
	status  string
}

func main() {
	total := flag.Int("n", 2773, "number of subscriptions to simulate")
	concurrency := flag.Int("concurrency", 3000, "fanout worker concurrency")
	latencyMS := flag.Int("latency-ms", 80, "base mock Bark response latency in milliseconds")
	jitterMS := flag.Int("jitter-ms", 40, "deterministic per-key latency jitter in milliseconds")
	transientRate := flag.Float64("transient-rate", 0.01, "fraction of keys that fail once with HTTP 502 before succeeding")
	permanentRate := flag.Float64("permanent-rate", 0.005, "fraction of keys that always fail with HTTP 400")
	retryAttempts := flag.Int("retry-attempts", 1, "extra retry attempts for transient failures")
	retryDelayMS := flag.Int("retry-delay-ms", 300, "base retry delay in milliseconds")
	timeoutMS := flag.Int("timeout-ms", 3000, "HTTP client timeout in milliseconds")
	http2 := flag.Bool("http2", true, "serve the mock Bark endpoint over HTTPS with HTTP/2")
	flag.Parse()

	if *total <= 0 {
		log.Fatal("-n must be positive")
	}
	if *concurrency <= 0 {
		log.Fatal("-concurrency must be positive")
	}
	if *concurrency > *total {
		*concurrency = *total
	}
	if *retryAttempts < 0 {
		*retryAttempts = 0
	}

	var requests atomic.Int64
	attempts := sync.Map{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		requests.Add(1)
		sleep := time.Duration(*latencyMS+stableBucket(key, max(1, *jitterMS+1))) * time.Millisecond
		time.Sleep(sleep)
		switch keyClass(key, *transientRate, *permanentRate) {
		case "permanent":
			http.Error(w, "BadDeviceToken", http.StatusBadRequest)
		case "transient":
			value, _ := attempts.LoadOrStore(key, new(atomic.Int64))
			if value.(*atomic.Int64).Add(1) == 1 {
				http.Error(w, "temporary upstream failure", http.StatusBadGateway)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"code":200}`)
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"code":200}`)
		}
	})
	var server *httptest.Server
	if *http2 {
		server = httptest.NewUnstartedServer(handler)
		server.EnableHTTP2 = true
		server.StartTLS()
	} else {
		server = httptest.NewServer(handler)
	}
	defer server.Close()

	transport := server.Client().Transport.(*http.Transport).Clone()
	transport.Proxy = http.ProxyFromEnvironment
	transport.DialContext = (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	transport.TLSHandshakeTimeout = 3 * time.Second
	transport.ResponseHeaderTimeout = time.Duration(*timeoutMS) * time.Millisecond
	transport.ExpectContinueTimeout = time.Second
	transport.MaxIdleConns = max(2000, *concurrency*2)
	transport.MaxIdleConnsPerHost = max(1200, *concurrency)
	transport.IdleConnTimeout = 90 * time.Second
	transport.ForceAttemptHTTP2 = true
	client := &http.Client{Timeout: time.Duration(*timeoutMS) * time.Millisecond, Transport: transport}

	jobs := make(chan string)
	results := make(chan result, *total)
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range jobs {
				results <- sendWithRetry(context.Background(), client, server.URL, key, *retryAttempts, time.Duration(*retryDelayMS)*time.Millisecond)
			}
		}()
	}
	for i := 0; i < *total; i++ {
		jobs <- fmt.Sprintf("key-%06d", i)
	}
	close(jobs)
	wg.Wait()
	close(results)
	duration := time.Since(start)

	var pushed, failed, permanent, retryCount int
	var elapsed []int64
	for res := range results {
		elapsed = append(elapsed, res.elapsed.Milliseconds())
		retryCount += res.retries
		switch res.status {
		case "pushed":
			pushed++
		case "permanent":
			permanent++
		default:
			failed++
		}
	}
	sort.Slice(elapsed, func(i, j int) bool { return elapsed[i] < elapsed[j] })
	fmt.Printf("subscriptions=%d concurrency=%d requests=%d pushed=%d permanent=%d failed=%d retries=%d duration=%s\n",
		*total, *concurrency, requests.Load(), pushed, permanent, failed, retryCount, duration.Round(time.Millisecond))
	fmt.Printf("elapsed_ms p50=%d p90=%d p99=%d max=%d\n",
		percentile(elapsed, 50), percentile(elapsed, 90), percentile(elapsed, 99), elapsed[len(elapsed)-1])
}

func sendWithRetry(ctx context.Context, client *http.Client, server, key string, maxRetries int, retryDelay time.Duration) result {
	start := time.Now()
	retries := 0
	for {
		err := send(ctx, client, server, key)
		if err == nil {
			return result{elapsed: time.Since(start), retries: retries, status: "pushed"}
		}
		if !retryable(err) {
			return result{elapsed: time.Since(start), retries: retries, status: "permanent"}
		}
		if retries >= maxRetries {
			return result{elapsed: time.Since(start), retries: retries, status: "failed"}
		}
		retries++
		select {
		case <-ctx.Done():
			return result{elapsed: time.Since(start), retries: retries, status: "failed"}
		case <-time.After(retryDelay * time.Duration(retries)):
		}
	}
}

func send(ctx context.Context, client *http.Client, server, key string) error {
	form := url.Values{}
	form.Set("title", "EEW")
	form.Set("body", "test")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server+"/"+url.PathEscape(key), bytes.NewBufferString(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 2048))
		return httpStatusError(res.StatusCode)
	}
	return nil
}

type httpStatusError int

func (e httpStatusError) Error() string {
	return "http " + strconv.Itoa(int(e))
}

func retryable(err error) bool {
	var status httpStatusError
	if asHTTPStatus(err, &status) {
		return int(status) == http.StatusTooManyRequests || int(status) >= 500
	}
	return true
}

func asHTTPStatus(err error, target *httpStatusError) bool {
	if err == nil {
		return false
	}
	status, ok := err.(httpStatusError)
	if !ok {
		return false
	}
	*target = status
	return true
}

func keyClass(key string, transientRate, permanentRate float64) string {
	bucket := stableBucket(key, 10000)
	permanentCutoff := int(math.Round(permanentRate * 10000))
	transientCutoff := permanentCutoff + int(math.Round(transientRate*10000))
	if bucket < permanentCutoff {
		return "permanent"
	}
	if bucket < transientCutoff {
		return "transient"
	}
	return "ok"
}

func stableBucket(value string, modulo int) int {
	if modulo <= 0 || uint64(modulo) > math.MaxUint32 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(value))
	return int(h.Sum32() % uint32(modulo)) // #nosec G115 -- modulo is range-checked above.
}

func percentile(sorted []int64, p int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(p)/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
