package entrypoints

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"serverless/gateway/scheduler"
)

func TestLeaseRoutesAcquireAndRelease(t *testing.T) {
	sched := testLeaseScheduler()
	mux := http.NewServeMux()
	registerLeaseRoutes(mux, sched)

	body := []byte(`{"source":"mq","mq_id":"rabbitmq-main","trigger_id":"orders-trigger","message_id":"msg-1"}`)
	req := httptest.NewRequest(http.MethodPost, "/internal/functions/order-handler/instances/acquire", bytes.NewReader(body))
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("acquire status = %d body=%s", res.Code, res.Body.String())
	}
	var lease scheduler.InstanceLease
	if err := json.NewDecoder(res.Body).Decode(&lease); err != nil {
		t.Fatal(err)
	}

	req = httptest.NewRequest(http.MethodPost, "/internal/leases/"+lease.LeaseID+"/release", bytes.NewReader([]byte(`{"status":"success"}`)))
	res = httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusNoContent {
		t.Fatalf("release status = %d body=%s", res.Code, res.Body.String())
	}
}

func TestLeaseRoutesInvalidTriggerReturnsBadRequest(t *testing.T) {
	mux := http.NewServeMux()
	registerLeaseRoutes(mux, testLeaseScheduler())
	body := []byte(`{"source":"mq","mq_id":"wrong","trigger_id":"orders-trigger"}`)
	req := httptest.NewRequest(http.MethodPost, "/internal/functions/order-handler/instances/acquire", bytes.NewReader(body))
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", res.Code, res.Body.String())
	}
}

func TestLeaseRoutesAcquireRejectsMissingMQFields(t *testing.T) {
	mux := http.NewServeMux()
	registerLeaseRoutes(mux, testLeaseScheduler())

	req := httptest.NewRequest(http.MethodPost, "/internal/functions/order-handler/instances/acquire", bytes.NewReader([]byte(`{"source":"http"}`)))
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", res.Code, res.Body.String())
	}
}

func TestLeaseRoutesFunctionNotFoundReturnsNotFound(t *testing.T) {
	mux := http.NewServeMux()
	registerLeaseRoutes(mux, testLeaseScheduler())

	body := []byte(`{"source":"mq","mq_id":"rabbitmq-main","trigger_id":"orders-trigger"}`)
	req := httptest.NewRequest(http.MethodPost, "/internal/functions/missing/instances/acquire", bytes.NewReader(body))
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusNotFound {
		t.Fatalf("status = %d body=%s", res.Code, res.Body.String())
	}
}

func TestLeaseRoutesAcquireBadBodyReturnsBadRequest(t *testing.T) {
	mux := http.NewServeMux()
	registerLeaseRoutes(mux, testLeaseScheduler())

	req := httptest.NewRequest(http.MethodPost, "/internal/functions/order-handler/instances/acquire", bytes.NewReader([]byte(`{`)))
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", res.Code, res.Body.String())
	}
}

func TestLeaseRoutesAcquireWrongMethodReturnsMethodNotAllowed(t *testing.T) {
	mux := http.NewServeMux()
	registerLeaseRoutes(mux, testLeaseScheduler())

	req := httptest.NewRequest(http.MethodGet, "/internal/functions/order-handler/instances/acquire", nil)
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d body=%s", res.Code, res.Body.String())
	}
}

func TestLeaseRoutesReleaseMissingLeaseReturnsNotFound(t *testing.T) {
	mux := http.NewServeMux()
	registerLeaseRoutes(mux, testLeaseScheduler())

	req := httptest.NewRequest(http.MethodPost, "/internal/leases/missing/release", bytes.NewReader([]byte(`{"status":"success"}`)))
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusNotFound {
		t.Fatalf("status = %d body=%s", res.Code, res.Body.String())
	}
}

func TestLeaseRoutesAcquireFailureReturnsServiceUnavailable(t *testing.T) {
	s := scheduler.NewForTesting(fakeFailingLeaseBackend{})
	if err := s.Register(scheduler.FunctionConfig{Name: "order-handler", Triggers: []scheduler.TriggerConfig{{ID: "orders-trigger", Type: "mq", Enabled: true, MQID: "rabbitmq-main", Queue: "orders"}}}); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	registerLeaseRoutes(mux, s)

	body := []byte(`{"source":"mq","mq_id":"rabbitmq-main","trigger_id":"orders-trigger"}`)
	req := httptest.NewRequest(http.MethodPost, "/internal/functions/order-handler/instances/acquire", bytes.NewReader(body))
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body=%s", res.Code, res.Body.String())
	}
}

func TestLeaseRoutesInvalidReleaseStatusReturnsBadRequest(t *testing.T) {
	sched := testLeaseScheduler()
	mux := http.NewServeMux()
	registerLeaseRoutes(mux, sched)

	body := []byte(`{"source":"mq","mq_id":"rabbitmq-main","trigger_id":"orders-trigger","message_id":"msg-1"}`)
	req := httptest.NewRequest(http.MethodPost, "/internal/functions/order-handler/instances/acquire", bytes.NewReader(body))
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("acquire status = %d body=%s", res.Code, res.Body.String())
	}
	var lease scheduler.InstanceLease
	if err := json.NewDecoder(res.Body).Decode(&lease); err != nil {
		t.Fatal(err)
	}

	req = httptest.NewRequest(http.MethodPost, "/internal/leases/"+lease.LeaseID+"/release", bytes.NewReader([]byte(`{"status":"bad"}`)))
	res = httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("release status = %d body=%s", res.Code, res.Body.String())
	}
}

func testLeaseScheduler() *scheduler.Scheduler {
	s := scheduler.NewForTesting(fakeLeaseBackend{})
	_ = s.Register(scheduler.FunctionConfig{Name: "order-handler", Triggers: []scheduler.TriggerConfig{{ID: "orders-trigger", Type: "mq", Enabled: true, MQID: "rabbitmq-main", Queue: "orders"}}})
	s.AddIdleTestInstance("order-handler", "inst-1", "127.0.0.1:9001")
	return s
}

type fakeLeaseBackend struct{}

func (fakeLeaseBackend) Name() string { return "fake" }

func (fakeLeaseBackend) Start(context.Context, scheduler.FunctionConfig) (*scheduler.RuntimeInstance, error) {
	return nil, nil
}

func (fakeLeaseBackend) Stop(context.Context, string) error { return nil }

type fakeFailingLeaseBackend struct{}

func (fakeFailingLeaseBackend) Name() string { return "fake" }

func (fakeFailingLeaseBackend) Start(context.Context, scheduler.FunctionConfig) (*scheduler.RuntimeInstance, error) {
	return nil, context.Canceled
}

func (fakeFailingLeaseBackend) Stop(context.Context, string) error { return nil }
