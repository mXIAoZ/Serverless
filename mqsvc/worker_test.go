package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWorkerHandleMessageAckAndRelease(t *testing.T) {
	released := false
	runtime := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/events" {
			t.Fatalf("runtime path = %s, want /events", r.URL.Path)
		}
		var event EventEnvelope
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Fatal(err)
		}
		if event.MQID != "rabbitmq-main" || event.TriggerID != "orders-trigger" || event.ID != "msg-1" {
			t.Fatalf("unexpected event: %+v", event)
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(runtimeResponse{StatusCode: http.StatusOK, Body: json.RawMessage(`{}`)})
	}))
	defer runtime.Close()

	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/internal/functions/order-handler/instances/acquire":
			json.NewEncoder(w).Encode(InstanceLease{LeaseID: "lease-1", Function: "order-handler", InstanceID: "inst-1", Address: strings.TrimPrefix(runtime.URL, "http://"), TimeoutSeconds: 30})
		case r.URL.Path == "/internal/leases/lease-1/release":
			released = true
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("gateway path = %s", r.URL.Path)
		}
	}))
	defer gateway.Close()

	worker := NewWorker(NewGatewayClient(strings.TrimPrefix(gateway.URL, "http://")))
	result := worker.HandleMessage(context.Background(), Trigger{ID: "orders-trigger", Type: "mq", Broker: "rabbitmq", MQID: "rabbitmq-main", Queue: "orders", Function: "order-handler"}, Message{ID: "msg-1", Body: []byte(`{"orderId":"o-1"}`)})
	if !result.Ack || result.DLQ || result.Reason != "" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if !released {
		t.Fatal("lease was not released")
	}
}

func TestWorkerRetriesFunctionErrorAndReleases(t *testing.T) {
	released := false
	runtime := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(runtimeResponse{StatusCode: http.StatusInternalServerError, Body: json.RawMessage(`{"error":"boom"}`)})
	}))
	defer runtime.Close()

	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/internal/functions/order-handler/instances/acquire":
			json.NewEncoder(w).Encode(InstanceLease{LeaseID: "lease-1", Address: strings.TrimPrefix(runtime.URL, "http://"), TimeoutSeconds: 30})
		case r.URL.Path == "/internal/leases/lease-1/release":
			released = true
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("gateway path = %s", r.URL.Path)
		}
	}))
	defer gateway.Close()

	worker := NewWorker(NewGatewayClient(strings.TrimPrefix(gateway.URL, "http://")))
	result := worker.HandleMessage(context.Background(), Trigger{ID: "orders-trigger", Type: "mq", Broker: "rabbitmq", MQID: "rabbitmq-main", Queue: "orders", Function: "order-handler", MaxAttempts: 3}, Message{ID: "msg-1", Body: []byte(`{}`), Attempts: 0})
	if result.Ack || result.DLQ || result.Reason == "" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if !released {
		t.Fatal("lease was not released")
	}
}

func TestWorkerRetriesWhenReleaseFails(t *testing.T) {
	runtime := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(runtimeResponse{StatusCode: http.StatusOK, Body: json.RawMessage(`{}`)})
	}))
	defer runtime.Close()

	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/internal/functions/order-handler/instances/acquire":
			json.NewEncoder(w).Encode(InstanceLease{LeaseID: "lease-1", Address: strings.TrimPrefix(runtime.URL, "http://"), TimeoutSeconds: 30})
		case r.URL.Path == "/internal/leases/lease-1/release":
			http.Error(w, "lease busy", http.StatusInternalServerError)
		default:
			t.Fatalf("gateway path = %s", r.URL.Path)
		}
	}))
	defer gateway.Close()

	worker := NewWorker(NewGatewayClient(strings.TrimPrefix(gateway.URL, "http://")))
	result := worker.HandleMessage(context.Background(), Trigger{ID: "orders-trigger", Type: "mq", Broker: "rabbitmq", MQID: "rabbitmq-main", Queue: "orders", Function: "order-handler"}, Message{ID: "msg-1", Body: []byte(`{}`)})
	if result.Ack || result.DLQ || result.Reason == "" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestWorkerReleasesCanceledInvocationAsAbandoned(t *testing.T) {
	releaseStatus := ""
	runtimeStarted := make(chan struct{})
	runtimeUnblock := make(chan struct{})
	releaseCalled := make(chan struct{}, 1)
	runtime := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(runtimeStarted)
		<-runtimeUnblock
	}))
	defer runtime.Close()

	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/internal/functions/order-handler/instances/acquire":
			json.NewEncoder(w).Encode(InstanceLease{LeaseID: "lease-1", Address: strings.TrimPrefix(runtime.URL, "http://"), TimeoutSeconds: 30})
		case r.URL.Path == "/internal/leases/lease-1/release":
			var body struct {
				Status string `json:"status"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			releaseStatus = body.Status
			w.WriteHeader(http.StatusNoContent)
			releaseCalled <- struct{}{}
		default:
			t.Fatalf("gateway path = %s", r.URL.Path)
		}
	}))
	defer gateway.Close()

	ctx, cancel := context.WithCancel(context.Background())
	worker := NewWorker(NewGatewayClient(strings.TrimPrefix(gateway.URL, "http://")))
	resultCh := make(chan MessageResult, 1)
	go func() {
		resultCh <- worker.HandleMessage(ctx, Trigger{ID: "orders-trigger", Type: "mq", Broker: "rabbitmq", MQID: "rabbitmq-main", Queue: "orders", Function: "order-handler"}, Message{ID: "msg-1", Body: []byte(`{}`)})
	}()

	<-runtimeStarted
	cancel()
	close(runtimeUnblock)

	select {
	case <-releaseCalled:
	case <-time.After(time.Second):
		t.Fatal("lease was not released")
	}
	if releaseStatus != "abandoned" {
		t.Fatalf("release status = %q, want abandoned", releaseStatus)
	}
	result := <-resultCh
	if result.Ack || result.DLQ || result.Reason == "" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestWorkerDLQsAfterAcquireFailureAtMaxAttempts(t *testing.T) {
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/functions/order-handler/instances/acquire" {
			t.Fatalf("gateway path = %s", r.URL.Path)
		}
		http.Error(w, "invalid trigger", http.StatusBadRequest)
	}))
	defer gateway.Close()

	worker := NewWorker(NewGatewayClient(strings.TrimPrefix(gateway.URL, "http://")))
	result := worker.HandleMessage(context.Background(), Trigger{ID: "orders-trigger", Type: "mq", Broker: "rabbitmq", MQID: "rabbitmq-main", Queue: "orders", Function: "order-handler", MaxAttempts: 2, DLQ: "orders.dlq"}, Message{ID: "msg-1", Body: []byte(`{}`), Attempts: 1})
	if result.Ack || !result.DLQ || result.Reason == "" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestWorkerDLQsAfterMaxAttempts(t *testing.T) {
	runtime := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(runtimeResponse{StatusCode: http.StatusInternalServerError})
	}))
	defer runtime.Close()

	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/internal/functions/order-handler/instances/acquire":
			json.NewEncoder(w).Encode(InstanceLease{LeaseID: "lease-1", Address: strings.TrimPrefix(runtime.URL, "http://"), TimeoutSeconds: 30})
		case r.URL.Path == "/internal/leases/lease-1/release":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("gateway path = %s", r.URL.Path)
		}
	}))
	defer gateway.Close()

	worker := NewWorker(NewGatewayClient(strings.TrimPrefix(gateway.URL, "http://")))
	result := worker.HandleMessage(context.Background(), Trigger{ID: "orders-trigger", Type: "mq", Broker: "rabbitmq", MQID: "rabbitmq-main", Queue: "orders", Function: "order-handler", MaxAttempts: 2, DLQ: "orders.dlq"}, Message{ID: "msg-1", Body: []byte(`{}`), Attempts: 1})
	if result.Ack || !result.DLQ || result.Reason == "" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestWorkerRetriesAfterMaxAttemptsWithoutDLQ(t *testing.T) {
	runtime := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(runtimeResponse{StatusCode: http.StatusInternalServerError})
	}))
	defer runtime.Close()

	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/internal/functions/order-handler/instances/acquire":
			json.NewEncoder(w).Encode(InstanceLease{LeaseID: "lease-1", Address: strings.TrimPrefix(runtime.URL, "http://"), TimeoutSeconds: 30})
		case r.URL.Path == "/internal/leases/lease-1/release":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("gateway path = %s", r.URL.Path)
		}
	}))
	defer gateway.Close()

	worker := NewWorker(NewGatewayClient(strings.TrimPrefix(gateway.URL, "http://")))
	result := worker.HandleMessage(context.Background(), Trigger{ID: "orders-trigger", Type: "mq", Broker: "rabbitmq", MQID: "rabbitmq-main", Queue: "orders", Function: "order-handler", MaxAttempts: 2}, Message{ID: "msg-1", Body: []byte(`{}`), Attempts: 1})
	if result.Ack || result.DLQ || result.Reason == "" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestWorkerRuntimeClientTimeoutFollowsLease(t *testing.T) {
	worker := NewWorker(NewGatewayClient("localhost:1"))
	client := worker.clientForLease(InstanceLease{TimeoutSeconds: 90})
	if client.Timeout != 95*time.Second {
		t.Fatalf("timeout = %s, want 1m35s", client.Timeout)
	}
}

func TestBuildEventBase64EncodesNonJSON(t *testing.T) {
	event := buildEvent(Trigger{ID: "trigger", Type: "mq", Broker: "rabbitmq", MQID: "mq", Queue: "q"}, Message{ID: "msg", Body: []byte{0xff}})
	if !event.IsBase64Encoded || event.BodyBase64 == "" || len(event.Body) != 0 {
		t.Fatalf("unexpected event: %+v", event)
	}
}
