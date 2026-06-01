package entrypoints

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"serverless/gateway/scheduler"
)

type acquireInstanceRequest struct {
	Source         string `json:"source"`
	MQID           string `json:"mq_id"`
	TriggerID      string `json:"trigger_id"`
	MessageID      string `json:"message_id"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

type releaseLeaseRequest struct {
	Status string `json:"status"`
}

func registerLeaseRoutes(mux *http.ServeMux, sched *scheduler.Scheduler) {
	mux.HandleFunc("/internal/functions/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		path := strings.TrimPrefix(req.URL.Path, "/internal/functions/")
		name, ok := strings.CutSuffix(path, "/instances/acquire")
		if !ok || name == "" || strings.Contains(name, "/") {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		var body acquireInstanceRequest
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		lease, err := sched.AcquireInstance(scheduler.AcquireInstanceRequest{
			Function:       name,
			Source:         body.Source,
			MQID:           body.MQID,
			TriggerID:      body.TriggerID,
			MessageID:      body.MessageID,
			TimeoutSeconds: body.TimeoutSeconds,
		})
		if err != nil {
			writeAcquireError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(lease)
	})

	mux.HandleFunc("/internal/leases/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		path := strings.TrimPrefix(req.URL.Path, "/internal/leases/")
		leaseID, ok := strings.CutSuffix(path, "/release")
		if !ok || leaseID == "" || strings.Contains(leaseID, "/") {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		var body releaseLeaseRequest
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		if body.Status == "" {
			body.Status = "success"
		}
		if err := sched.ReleaseInstance(leaseID, body.Status); err != nil {
			writeReleaseError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

func writeAcquireError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, scheduler.ErrFunctionNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, scheduler.ErrInvalidTrigger):
		http.Error(w, err.Error(), http.StatusBadRequest)
	default:
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	}
}

func writeReleaseError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, scheduler.ErrInvalidLeaseStatus):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, scheduler.ErrLeaseNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
