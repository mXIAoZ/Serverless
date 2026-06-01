package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
)

// LogEntry is a single log line from a container.
type LogEntry struct {
	Time     time.Time `json:"time"`
	Function string    `json:"function"`
	Stream   string    `json:"stream"`
	Line     string    `json:"line"`
}

type ring struct {
	mu      sync.RWMutex
	entries [ringSize]LogEntry
	head    int
	count   int
	subs    []chan LogEntry
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
		default:
		}
	}
}

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

type daemon struct {
	mu    sync.RWMutex
	rings map[string]*ring
	files map[string]*os.File
	proxy *proxyClient
}

func newDaemon() *daemon {
	_ = os.MkdirAll(logDir, 0755)
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
		_, _ = f.Write(append(line, '\n'))
	}
}

func (d *daemon) serveHTTP(mode string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/logs/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/logs/")
		if strings.HasSuffix(path, "/stream") {
			funcName := strings.TrimSuffix(path, "/stream")
			d.handleStream(w, r, funcName, mode)
			return
		}

		funcName := path
		tail := parseTail(r, 50)
		filterStream := r.URL.Query().Get("stream")

		entries, err := d.entriesFor(funcName, tail, filterStream, mode)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		if len(entries) == 0 {
			http.Error(w, "no logs for function "+funcName, http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(entries)
	})

	mux.HandleFunc("/local/logs/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/local/logs/")
		if strings.HasSuffix(path, "/stream") {
			funcName := strings.TrimSuffix(path, "/stream")
			d.handleStream(w, r, funcName, "collector")
			return
		}
		funcName := path
		tail := parseTail(r, 50)
		filterStream := r.URL.Query().Get("stream")
		entries := d.localEntries(funcName, tail, filterStream)
		if len(entries) == 0 {
			http.Error(w, "no local logs for function "+funcName, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(entries)
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	log.Printf("[daemon] HTTP API listening on %s (mode=%s)", listenAddr, mode)
	if err := http.ListenAndServe(listenAddr, mux); err != nil {
		log.Fatal(err)
	}
}

func (d *daemon) entriesFor(funcName string, tail int, filterStream, mode string) ([]LogEntry, error) {
	if mode == "proxy" && d.proxy != nil {
		entries, err := d.proxy.fetchLogs(funcName, tail, filterStream)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			d.write(e)
		}
		return entries, nil
	}
	return d.localEntries(funcName, tail, filterStream), nil
}

func (d *daemon) localEntries(funcName string, tail int, filterStream string) []LogEntry {
	d.mu.RLock()
	ring, ok := d.rings[funcName]
	d.mu.RUnlock()
	if !ok {
		return nil
	}
	entries := ring.tail(tail)
	if filterStream == "" {
		return entries
	}
	filtered := entries[:0]
	for _, e := range entries {
		if e.Stream == filterStream {
			filtered = append(filtered, e)
		}
	}
	return filtered
}

func (d *daemon) handleStream(w http.ResponseWriter, r *http.Request, funcName, mode string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}
	if mode == "proxy" && d.proxy != nil {
		d.proxy.streamLogs(w, r, funcName)
		return
	}

	ring := d.ringFor(funcName)
	ch := ring.subscribe()
	defer ring.unsubscribe(ch)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	for _, e := range ring.tail(20) {
		data, _ := json.Marshal(e)
		_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
	}
	flusher.Flush()

	for {
		select {
		case e := <-ch:
			data, _ := json.Marshal(e)
			_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func parseTail(r *http.Request, def int) int {
	tail := def
	if v := r.URL.Query().Get("tail"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			tail = n
		}
	}
	return tail
}

type proxyClient struct {
	gatewayAddr  string
	gatewayToken string
	namespace    string
	httpClient   *http.Client
	k8sClient    kubernetes.Interface
	restConfig   *rest.Config
}

type instanceInfo struct {
	ID       string `json:"id"`
	FuncName string `json:"func_name"`
	NodeName string `json:"node_name"`
	State    string `json:"state"`
}

type collectorPod struct {
	NodeName string
	PodName  string
}

func newProxyClient() *proxyClient {
	ns := os.Getenv("K8S_NAMESPACE")
	if ns == "" {
		ns = "default"
	}
	gatewayAddr := os.Getenv("GATEWAY_INTERNAL_ADDR")
	if gatewayAddr == "" {
		gatewayAddr = "localhost:8081"
	}
	client, restConfig, err := newK8sClient()
	if err != nil {
		log.Fatalf("[daemon] k8s client: %v", err)
	}
	return &proxyClient{
		gatewayAddr:  gatewayAddr,
		gatewayToken: os.Getenv("INTERNAL_API_TOKEN"),
		namespace:    ns,
		httpClient:   &http.Client{Timeout: 10 * time.Second},
		k8sClient:    client,
		restConfig:   restConfig,
	}
}

func newK8sClient() (kubernetes.Interface, *rest.Config, error) {
	cfg, err := newK8sRestConfig()
	if err != nil {
		return nil, nil, err
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	return client, cfg, nil
}

func newK8sRestConfig() (*rest.Config, error) {
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		if home, err := os.UserHomeDir(); err == nil {
			kubeconfig = filepath.Join(home, ".kube", "config")
		}
	}
	if kubeconfig != "" {
		if cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig); err == nil {
			return cfg, nil
		}
	}
	return rest.InClusterConfig()
}

func (p *proxyClient) fetchLogs(funcName string, tail int, filterStream string) ([]LogEntry, error) {
	instances, err := p.instances(funcName)
	if err != nil {
		return nil, err
	}
	collectors, err := p.collectorsByNode()
	if err != nil {
		return nil, err
	}

	var all []LogEntry
	seen := make(map[string]struct{})
	for _, inst := range instances {
		if inst.NodeName == "" {
			continue
		}
		pod, ok := collectors[inst.NodeName]
		if !ok || pod.PodName == "" {
			continue
		}
		if _, ok := seen[pod.PodName]; ok {
			continue
		}
		seen[pod.PodName] = struct{}{}
		entries, err := p.fetchCollectorLogs(pod.PodName, funcName, tail, filterStream)
		if err != nil {
			log.Printf("[daemon] collector logs %s (%s): %v", pod.PodName, inst.NodeName, err)
			continue
		}
		all = append(all, entries...)
	}
	if len(all) == 0 {
		return nil, nil
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Time.Before(all[j].Time) })
	if tail > 0 && len(all) > tail {
		all = all[len(all)-tail:]
	}
	return all, nil
}

func (p *proxyClient) streamLogs(w http.ResponseWriter, r *http.Request, funcName string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, _ := w.(http.Flusher)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	last := time.Time{}
	for {
		entries, err := p.fetchLogs(funcName, 20, "")
		if err == nil {
			for _, e := range entries {
				if !e.Time.After(last) {
					continue
				}
				data, _ := json.Marshal(e)
				_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
				last = e.Time
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		select {
		case <-ticker.C:
		case <-r.Context().Done():
			return
		}
	}
}

func (p *proxyClient) instances(funcName string) ([]instanceInfo, error) {
	req, err := http.NewRequest(http.MethodGet, "http://"+p.gatewayAddr+"/internal/instances/"+funcName, nil)
	if err != nil {
		return nil, err
	}
	if p.gatewayToken != "" {
		req.Header.Set("Authorization", "Bearer "+p.gatewayToken)
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var instances []instanceInfo
	if err := json.NewDecoder(resp.Body).Decode(&instances); err != nil {
		return nil, err
	}
	return instances, nil
}

func (p *proxyClient) fetchCollectorLogs(podName, funcName string, tail int, filterStream string) ([]LogEntry, error) {
	url := fmt.Sprintf("http://127.0.0.1%s/local/logs/%s?tail=%d", listenAddr, funcName, tail)
	if filterStream != "" {
		url += "&stream=" + filterStream
	}
	out, errOut, err := p.execCollector(podName, []string{"python3", "-c", fmt.Sprintf("import urllib.request,sys;print(urllib.request.urlopen(%q, timeout=10).read().decode())", url)})
	if err != nil {
		return nil, fmt.Errorf("exec collector %s: %w: %s", podName, err, strings.TrimSpace(errOut.String()))
	}
	var entries []LogEntry
	if err := json.Unmarshal(out.Bytes(), &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

var podExecParameterCodec = func() runtime.ParameterCodec {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	return runtime.NewParameterCodec(scheme)
}()

func (p *proxyClient) execCollector(podName string, command []string) (*bytes.Buffer, *bytes.Buffer, error) {
	req := p.k8sClient.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(p.namespace).
		SubResource("exec")
	req.VersionedParams(&corev1.PodExecOptions{
		Container: "collector",
		Command:   command,
		Stdout:    true,
		Stderr:    true,
	}, podExecParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(p.restConfig, http.MethodPost, req.URL())
	if err != nil {
		return nil, nil, err
	}
	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(context.Background(), remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return &stdout, &stderr, err
}

func (p *proxyClient) collectorsByNode() (map[string]collectorPod, error) {
	pods, err := p.k8sClient.CoreV1().Pods(p.namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: "app=faas-log-collector",
	})
	if err != nil {
		return nil, fmt.Errorf("list collector pods: %w", err)
	}
	collectors := make(map[string]collectorPod, len(pods.Items))
	for _, pod := range pods.Items {
		if pod.Spec.NodeName == "" || pod.Name == "" {
			continue
		}
		collectors[pod.Spec.NodeName] = collectorPod{NodeName: pod.Spec.NodeName, PodName: pod.Name}
	}
	return collectors, nil
}

func streamLines(ctx context.Context, rc io.ReadCloser, onLine func(string)) {
	defer rc.Close()
	scanner := bufio.NewScanner(rc)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
			onLine(scanner.Text())
		}
	}
}
