package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	ringSize    = 1000
	logDir      = "/tmp/faas-logs"
	dockerSock  = "/var/run/docker.sock"
	listenAddr  = ":9200"
	labelKey    = "faas.function"
)

// LogEntry is a single log line from a container.
type LogEntry struct {
	Time     time.Time `json:"time"`
	Function string    `json:"function"`
	Stream   string    `json:"stream"` // "stdout" or "stderr"
	Line     string    `json:"line"`
}

// ring is a fixed-size circular buffer of log entries.
type ring struct {
	mu      sync.RWMutex
	entries [ringSize]LogEntry
	head    int // next write position
	count   int
	subs    []chan LogEntry // SSE subscribers
}

func (r *ring) push(e LogEntry) {
	r.mu.Lock()
	r.entries[r.head] = e
	r.head = (r.head + 1) % ringSize
	if r.count < ringSize {
		r.count++
	}
	subs := make([]chan LogEntry, len(r.subs))
	copy(subs, r.subs)
	r.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- e:
		default: // slow subscriber, drop
		}
	}
}

// tail returns the last n entries (or all if n <= 0).
func (r *ring) tail(n int) []LogEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.count == 0 {
		return nil
	}
	if n <= 0 || n > r.count {
		n = r.count
	}
	out := make([]LogEntry, n)
	start := (r.head - n + ringSize) % ringSize
	for i := 0; i < n; i++ {
		out[i] = r.entries[(start+i)%ringSize]
	}
	return out
}

func (r *ring) subscribe() chan LogEntry {
	ch := make(chan LogEntry, 64)
	r.mu.Lock()
	r.subs = append(r.subs, ch)
	r.mu.Unlock()
	return ch
}

func (r *ring) unsubscribe(ch chan LogEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, s := range r.subs {
		if s == ch {
			r.subs = append(r.subs[:i], r.subs[i+1:]...)
			return
		}
	}
}

// daemon holds all per-function rings and file handles.
type daemon struct {
	mu    sync.RWMutex
	rings map[string]*ring  // funcName → ring
	files map[string]*os.File
}

func newDaemon() *daemon {
	os.MkdirAll(logDir, 0755)
	return &daemon{
		rings: make(map[string]*ring),
		files: make(map[string]*os.File),
	}
}

func (d *daemon) ringFor(funcName string) *ring {
	d.mu.RLock()
	r, ok := d.rings[funcName]
	d.mu.RUnlock()
	if ok {
		return r
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if r, ok = d.rings[funcName]; ok {
		return r
	}
	r = &ring{}
	d.rings[funcName] = r

	// open append-only log file
	path := filepath.Join(logDir, funcName+".log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("[daemon] open log file %s: %v", path, err)
	} else {
		d.files[funcName] = f
	}
	return r
}

func (d *daemon) write(e LogEntry) {
	r := d.ringFor(e.Function)
	r.push(e)

	d.mu.RLock()
	f := d.files[e.Function]
	d.mu.RUnlock()
	if f != nil {
		line, _ := json.Marshal(e)
		f.Write(append(line, '\n'))
	}
}

// --- Docker API via Unix socket ---

func dockerDo(method, path string, body io.Reader) (*http.Response, error) {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", dockerSock)
		},
	}
	client := &http.Client{Transport: transport}
	req, err := http.NewRequest(method, "http://docker"+path, body)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

// watchEvents subscribes to Docker events and calls onStart for each
// container start event that has the faas.function label.
func (d *daemon) watchEvents(ctx context.Context) {
	filters := url.QueryEscape(`{"event":["start","die"],"label":["` + labelKey + `"]}`)
	for {
		resp, err := dockerDo("GET", "/events?filters="+filters, nil)
		if err != nil {
			log.Printf("[daemon] events error: %v — retrying in 3s", err)
			time.Sleep(3 * time.Second)
			continue
		}

		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			var ev struct {
				Status string `json:"status"`
				ID     string `json:"id"`
				Actor  struct {
					Attributes map[string]string `json:"Attributes"`
				} `json:"Actor"`
			}
			if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
				continue
			}
			funcName := ev.Actor.Attributes[labelKey]
			if funcName == "" {
				continue
			}
			switch ev.Status {
			case "start":
				log.Printf("[daemon] container started: %s (%s)", ev.ID[:12], funcName)
				go d.collectLogs(ctx, ev.ID, funcName)
			case "die":
				log.Printf("[daemon] container stopped: %s (%s)", ev.ID[:12], funcName)
			}
		}
		resp.Body.Close()

		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(1 * time.Second)
		}
	}
}

// collectLogs streams logs from a container using the Docker multiplexed log format.
func (d *daemon) collectLogs(ctx context.Context, containerID, funcName string) {
	path := fmt.Sprintf("/containers/%s/logs?follow=1&stdout=1&stderr=1&timestamps=1", containerID)
	resp, err := dockerDo("GET", path, nil)
	if err != nil {
		log.Printf("[daemon] logs attach %s: %v", containerID[:12], err)
		return
	}
	defer resp.Body.Close()

	// Docker multiplexed stream: 8-byte header per frame
	// [stream_type(1)] [0 0 0(3)] [size(4 big-endian)] [payload...]
	hdr := make([]byte, 8)
	for {
		if _, err := io.ReadFull(resp.Body, hdr); err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				log.Printf("[daemon] log stream %s ended: %v", containerID[:12], err)
			}
			return
		}
		streamType := hdr[0] // 1=stdout, 2=stderr
		size := binary.BigEndian.Uint32(hdr[4:])
		payload := make([]byte, size)
		if _, err := io.ReadFull(resp.Body, payload); err != nil {
			return
		}

		stream := "stdout"
		if streamType == 2 {
			stream = "stderr"
		}

		// Docker prepends an RFC3339Nano timestamp when timestamps=1
		line := strings.TrimRight(string(payload), "\n")
		ts := time.Now()
		if idx := strings.Index(line, " "); idx > 0 {
			if t, err := time.Parse(time.RFC3339Nano, line[:idx]); err == nil {
				ts = t
				line = line[idx+1:]
			}
		}

		d.write(LogEntry{
			Time:     ts,
			Function: funcName,
			Stream:   stream,
			Line:     line,
		})
	}
}

// --- HTTP API ---

func (d *daemon) serveHTTP() {
	mux := http.NewServeMux()

	// GET /logs/{funcName}?tail=50&stream=stderr
	mux.HandleFunc("/logs/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/logs/")

		// SSE: GET /logs/{funcName}/stream
		if strings.HasSuffix(path, "/stream") {
			funcName := strings.TrimSuffix(path, "/stream")
			d.handleStream(w, r, funcName)
			return
		}

		funcName := path
		d.mu.RLock()
		ring, ok := d.rings[funcName]
		d.mu.RUnlock()
		if !ok {
			http.Error(w, "no logs for function "+funcName, http.StatusNotFound)
			return
		}

		tail := 50
		if v := r.URL.Query().Get("tail"); v != "" {
			fmt.Sscanf(v, "%d", &tail)
		}
		filterStream := r.URL.Query().Get("stream") // "stdout", "stderr", or ""

		entries := ring.tail(tail)
		if filterStream != "" {
			filtered := entries[:0]
			for _, e := range entries {
				if e.Stream == filterStream {
					filtered = append(filtered, e)
				}
			}
			entries = filtered
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(entries)
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok"))
	})

	log.Printf("[daemon] HTTP API listening on %s", listenAddr)
	if err := http.ListenAndServe(listenAddr, mux); err != nil {
		log.Fatal(err)
	}
}

// handleStream pushes new log entries as Server-Sent Events.
func (d *daemon) handleStream(w http.ResponseWriter, r *http.Request, funcName string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	ring := d.ringFor(funcName)
	ch := ring.subscribe()
	defer ring.unsubscribe(ch)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// send recent history first
	for _, e := range ring.tail(20) {
		data, _ := json.Marshal(e)
		fmt.Fprintf(w, "data: %s\n\n", data)
	}
	flusher.Flush()

	for {
		select {
		case e := <-ch:
			data, _ := json.Marshal(e)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func main() {
	d := newDaemon()
	ctx := context.Background()

	// 收集启动前已存在的 faas 容器
	go d.collectExisting(ctx)
	go d.watchEvents(ctx)

	d.serveHTTP()
}

// collectExisting attaches to any faas containers already running when the daemon starts.
func (d *daemon) collectExisting(ctx context.Context) {
	filters := url.QueryEscape(`{"label":["` + labelKey + `"],"status":["running"]}`)
	resp, err := dockerDo("GET", "/containers/json?filters="+filters, nil)
	if err != nil {
		log.Printf("[daemon] list existing containers: %v", err)
		return
	}
	defer resp.Body.Close()

	var containers []struct {
		ID     string            `json:"Id"`
		Labels map[string]string `json:"Labels"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&containers); err != nil {
		return
	}
	for _, c := range containers {
		funcName := c.Labels[labelKey]
		if funcName == "" {
			continue
		}
		log.Printf("[daemon] attaching to existing container %s (%s)", c.ID[:12], funcName)
		go d.collectLogs(ctx, c.ID, funcName)
	}
}
