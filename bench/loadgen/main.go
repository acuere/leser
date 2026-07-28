// loadgen hammers the leser ingest endpoint and reports client-side latency
// percentiles, acceptance/shed counts, and asserts the robustness gates
// (order-2 §6): bounded p99 under overload and flat server memory.
//
//	loadgen -url http://127.0.0.1:PORT -key DSNKEY -project 1 \
//	        -conc 64 -duration 20s -pid <server pid>
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func rssKB(pid int) int64 {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return -1
	}
	n, _ := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	return n
}

func main() {
	base := flag.String("url", "http://127.0.0.1:8080", "server base URL")
	key := flag.String("key", "", "DSN public key")
	project := flag.String("project", "1", "project id")
	conc := flag.Int("conc", 64, "concurrent senders")
	dur := flag.Duration("duration", 20*time.Second, "test duration")
	pid := flag.Int("pid", 0, "server pid for RSS sampling")
	maxP99 := flag.Duration("max-p99", 250*time.Millisecond, "p99 gate for ACCEPTED requests")
	maxRSSGrowth := flag.Int64("max-rss-growth-kb", 150*1024, "allowed RSS growth")
	flag.Parse()

	endpoint := fmt.Sprintf("%s/api/%s/envelope/?sentry_key=%s", *base, *project, *key)
	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: *conc},
	}

	var accepted, shed, failed atomic.Int64
	var mu sync.Mutex
	var lats []time.Duration
	failKinds := map[string]int{}

	startRSS := rssKB(*pid)
	deadline := time.Now().Add(*dur)
	var wg sync.WaitGroup
	var seq atomic.Int64

	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				n := seq.Add(1)
				id := fmt.Sprintf("l%031d", n)
				event := fmt.Sprintf(`{"event_id":%q,"message":"load","exception":{"values":[{"type":"LoadErr","value":"v%d"}]}}`,
					id, rand.Intn(50))
				body := fmt.Sprintf("{\"event_id\":%q}\n{\"type\":\"event\",\"length\":%d}\n%s\n", id, len(event), event)

				t0 := time.Now()
				resp, err := client.Post(endpoint, "application/x-sentry-envelope", bytes.NewReader([]byte(body)))
				lat := time.Since(t0)
				if err != nil {
					failed.Add(1)
					mu.Lock()
					if len(failKinds) < 5 {
						failKinds[fmt.Sprintf("transport: %.80s", err.Error())]++
					}
					mu.Unlock()
					continue
				}
				// Drain before close or keep-alive dies and we dial-storm the OS.
				_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
				resp.Body.Close()
				switch resp.StatusCode {
				case http.StatusOK:
					accepted.Add(1)
					mu.Lock()
					lats = append(lats, lat)
					mu.Unlock()
				case http.StatusTooManyRequests:
					shed.Add(1) // backpressure working as designed
				default:
					failed.Add(1)
					mu.Lock()
					failKinds[fmt.Sprintf("http %d", resp.StatusCode)]++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	endRSS := rssKB(*pid)

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	pct := func(p float64) time.Duration {
		if len(lats) == 0 {
			return 0
		}
		return lats[int(float64(len(lats)-1)*p)]
	}

	secs := dur.Seconds()
	fmt.Printf("accepted: %d (%.0f/s)  shed(429): %d  failed: %d\n",
		accepted.Load(), float64(accepted.Load())/secs, shed.Load(), failed.Load())
	for k, v := range failKinds {
		fmt.Printf("  fail kind: %s ×%d\n", k, v)
	}
	fmt.Printf("latency accepted: p50=%v p95=%v p99=%v\n", pct(0.50), pct(0.95), pct(0.99))
	if startRSS > 0 && endRSS > 0 {
		fmt.Printf("server RSS: start=%dKB end=%dKB growth=%dKB\n", startRSS, endRSS, endRSS-startRSS)
	}

	bad := false
	if failed.Load() > accepted.Load()/100 { // >1% hard failures
		fmt.Println("GATE FAIL: hard failures exceed 1%")
		bad = true
	}
	if p99 := pct(0.99); p99 > *maxP99 {
		fmt.Printf("GATE FAIL: p99 %v > %v\n", p99, *maxP99)
		bad = true
	}
	if startRSS > 0 && endRSS-startRSS > *maxRSSGrowth {
		fmt.Printf("GATE FAIL: RSS grew %dKB > %dKB — memory not flat under load\n", endRSS-startRSS, *maxRSSGrowth)
		bad = true
	}
	if accepted.Load() == 0 {
		fmt.Println("GATE FAIL: nothing accepted")
		bad = true
	}
	if bad {
		os.Exit(1)
	}
	fmt.Println("load PASS: p99 bounded, memory flat, shedding clean")
}
