package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type collectorState struct {
	namespace string
	nodeName  string
	client    *http.Client
	baseURL   string
	mu        sync.Mutex
	seen      map[string]struct{}
}

type podInfo struct {
	Name     string
	Function string
	NodeName string
	Phase    string
}

func (d *daemon) runCollector(ctx context.Context) {
	state, err := newCollectorState()
	if err != nil {
		log.Printf("[daemon] collector init: %v", err)
		return
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		state.syncPods(ctx, d)
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func newCollectorState() (*collectorState, error) {
	namespace := envOr("K8S_NAMESPACE", "default")
	nodeName := os.Getenv("MY_NODE_NAME")
	if nodeName == "" {
		return nil, fmt.Errorf("collector mode requires MY_NODE_NAME")
	}

	tokenBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return nil, fmt.Errorf("read serviceaccount token: %w", err)
	}
	caBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("read serviceaccount ca: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		return nil, fmt.Errorf("append kubernetes CA")
	}

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool},
	}
	client := &http.Client{Transport: transport, Timeout: 15 * time.Second}
	baseURL := strings.TrimRight("https://kubernetes.default.svc", "/")

	state := &collectorState{
		namespace: namespace,
		nodeName:  nodeName,
		client:    client,
		baseURL:   baseURL,
		seen:      make(map[string]struct{}),
	}
	state.client.Transport = roundTripperWithBearer{base: transport, token: strings.TrimSpace(string(tokenBytes))}
	return state, nil
}

func (s *collectorState) syncPods(ctx context.Context, d *daemon) {
	pods, err := s.listLocalFunctionPods()
	if err != nil {
		log.Printf("[daemon] collector list pods: %v", err)
		return
	}
	for _, pod := range pods {
		if pod.NodeName != s.nodeName || pod.Phase != "Running" || pod.Function == "" {
			continue
		}

		s.mu.Lock()
		_, ok := s.seen[pod.Name]
		if !ok {
			s.seen[pod.Name] = struct{}{}
		}
		s.mu.Unlock()
		if ok {
			continue
		}

		go s.followPod(ctx, d, pod)
	}
}

func (s *collectorState) listLocalFunctionPods() ([]podInfo, error) {
	selector := url.QueryEscape("faas.managed-by=local-faas")
	endpoint := fmt.Sprintf("%s/api/v1/namespaces/%s/pods?labelSelector=%s", s.baseURL, s.namespace, selector)
	resp, err := s.client.Get(endpoint)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list pods: %s %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var payload struct {
		Items []struct {
			Metadata struct {
				Name   string            `json:"name"`
				Labels map[string]string `json:"labels"`
			} `json:"metadata"`
			Spec struct {
				NodeName string `json:"nodeName"`
			} `json:"spec"`
			Status struct {
				Phase string `json:"phase"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	pods := make([]podInfo, 0, len(payload.Items))
	for _, item := range payload.Items {
		pods = append(pods, podInfo{
			Name:     item.Metadata.Name,
			Function: item.Metadata.Labels[labelKey],
			NodeName: item.Spec.NodeName,
			Phase:    item.Status.Phase,
		})
	}
	return pods, nil
}

func (s *collectorState) followPod(ctx context.Context, d *daemon, pod podInfo) {
	endpoint := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s/log?follow=true&timestamps=true", s.baseURL, s.namespace, url.PathEscape(pod.Name))
	resp, err := s.client.Get(endpoint)
	if err != nil {
		log.Printf("[daemon] collector logs %s: %v", pod.Name, err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		log.Printf("[daemon] collector logs %s: %s %s", pod.Name, resp.Status, strings.TrimSpace(string(body)))
		return
	}

	streamLines(ctx, resp.Body, func(line string) {
		ts := time.Now()
		payload := line
		if idx := strings.Index(line, " "); idx > 0 {
			if t, err := time.Parse(time.RFC3339Nano, line[:idx]); err == nil {
				ts = t
				payload = line[idx+1:]
			}
		}
		d.write(LogEntry{Time: ts, Function: pod.Function, Stream: "stdout", Line: payload})
	})
}

type roundTripperWithBearer struct {
	base  http.RoundTripper
	token string
}

func (rt roundTripperWithBearer) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Header.Set("Authorization", "Bearer "+rt.token)
	return rt.base.RoundTrip(clone)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func isContextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

var _ = filepath.Separator
