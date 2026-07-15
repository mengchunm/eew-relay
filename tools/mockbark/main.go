package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

func main() {
	listen := flag.String("listen", ":8080", "listen address")
	latency := flag.Duration("latency", 20*time.Millisecond, "response latency")
	flag.Parse()
	var requests atomic.Uint64
	var active atomic.Int64
	var maxActive atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]int64{"requests": int64(requests.Load()), "active": active.Load(), "max_active": maxActive.Load()})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		requests.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		current := active.Add(1)
		defer active.Add(-1)
		for {
			peak := maxActive.Load()
			if current <= peak || maxActive.CompareAndSwap(peak, current) {
				break
			}
		}
		time.Sleep(*latency)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":200,"message":"success"}`))
	})
	server := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 3 * time.Second}
	log.Printf("mock Bark listening=%s latency=%s", *listen, *latency)
	log.Fatal(server.ListenAndServe())
}
