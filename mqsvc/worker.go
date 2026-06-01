package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

type Worker struct {
	gateway *GatewayClient
	client  *http.Client
	metrics *Metrics
}

type runtimeResponse struct {
	StatusCode int             `json:"statusCode"`
	Body       json.RawMessage `json:"body"`
}

type EventEnvelope struct {
	Version         string            `json:"version"`
	Type            string            `json:"type"`
	Source          string            `json:"source"`
	MQID            string            `json:"mq_id"`
	TriggerID       string            `json:"trigger_id"`
	ID              string            `json:"id"`
	Time            string            `json:"time"`
	Subject         string            `json:"subject"`
	Headers         map[string]string `json:"headers,omitempty"`
	Body            json.RawMessage   `json:"body,omitempty"`
	BodyBase64      string            `json:"bodyBase64,omitempty"`
	IsBase64Encoded bool              `json:"isBase64Encoded,omitempty"`
}

func NewWorker(gateway *GatewayClient) *Worker {
	return &Worker{gateway: gateway, client: &http.Client{Timeout: 35 * time.Second}, metrics: NewMetrics()}
}

func (w *Worker) SetMetrics(metrics *Metrics) {
	if metrics != nil {
		w.metrics = metrics
	}
}

func (w *Worker) HandleMessage(ctx context.Context, trigger Trigger, msg Message) MessageResult {
	if w.metrics != nil {
		w.metrics.Received(trigger.ID)
	}
	lease, err := w.gateway.Acquire(ctx, trigger, msg)
	if err != nil {
		if w.shouldDLQ(trigger, msg) {
			if w.metrics != nil {
				w.metrics.DLQed(trigger.ID, err.Error())
			}
			return MessageResult{DLQ: true, Reason: err.Error()}
		}
		if w.metrics != nil {
			w.metrics.Retried(trigger.ID, err.Error())
		}
		return MessageResult{Reason: err.Error()}
	}
	status := "success"

	invokeCtx := ctx
	cancel := func() {}
	if lease.TimeoutSeconds > 0 {
		invokeCtx, cancel = context.WithTimeout(ctx, time.Duration(lease.TimeoutSeconds)*time.Second)
	}
	defer cancel()

	if err := w.invokeRuntime(invokeCtx, lease, trigger, msg); err != nil {
		status = "error"
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(invokeCtx.Err(), context.DeadlineExceeded) {
			status = "timeout"
		} else if errors.Is(err, context.Canceled) || errors.Is(invokeCtx.Err(), context.Canceled) {
			status = "abandoned"
		}
		if releaseErr := w.releaseLease(lease.LeaseID, status); releaseErr != nil {
			reason := releaseErr.Error()
			if w.metrics != nil {
				w.metrics.Retried(trigger.ID, reason)
			}
			return MessageResult{Reason: reason}
		}
		if w.shouldDLQ(trigger, msg) {
			if w.metrics != nil {
				w.metrics.DLQed(trigger.ID, err.Error())
			}
			return MessageResult{DLQ: true, Reason: err.Error()}
		}
		if w.metrics != nil {
			w.metrics.Retried(trigger.ID, err.Error())
		}
		return MessageResult{Reason: err.Error()}
	}
	if releaseErr := w.releaseLease(lease.LeaseID, status); releaseErr != nil {
		reason := releaseErr.Error()
		if w.metrics != nil {
			w.metrics.Retried(trigger.ID, reason)
		}
		return MessageResult{Reason: reason}
	}
	if w.metrics != nil {
		w.metrics.Acked(trigger.ID)
	}
	return MessageResult{Ack: true}
}
func (w *Worker) releaseLease(leaseID string, status string) error {
	releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return w.gateway.Release(releaseCtx, leaseID, status)
}

func (w *Worker) shouldDLQ(trigger Trigger, msg Message) bool {
	return trigger.MaxAttempts > 0 && msg.Attempts+1 >= trigger.MaxAttempts && trigger.DLQ != ""
}

func (w *Worker) clientForLease(lease InstanceLease) *http.Client {
	if lease.TimeoutSeconds <= 0 {
		return w.client
	}
	return &http.Client{Timeout: time.Duration(lease.TimeoutSeconds+5) * time.Second}
}

func (w *Worker) invokeRuntime(ctx context.Context, lease InstanceLease, trigger Trigger, msg Message) error {
	event := buildEvent(trigger, msg)
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s/events", lease.Address)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Event-Type", "mq")
	req.Header.Set("X-Trigger-ID", trigger.ID)
	req.Header.Set("X-Message-ID", msg.ID)
	if lease.TimeoutSeconds > 0 {
		req.Header.Set("X-Function-Timeout", fmt.Sprintf("%d", lease.TimeoutSeconds))
	}
	resp, err := w.clientForLease(lease).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		return fmt.Errorf("runtime returned %s", resp.Status)
	}
	var result runtimeResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}
	if result.StatusCode >= 500 {
		return fmt.Errorf("function returned status %d", result.StatusCode)
	}
	return nil
}

func buildEvent(trigger Trigger, msg Message) EventEnvelope {
	broker := trigger.Broker
	if broker == "" && trigger.Type != "mq" {
		broker = trigger.Type
	}
	if broker == "" {
		broker = "rabbitmq"
	}
	env := EventEnvelope{
		Version:   "1.0",
		Type:      "mq",
		Source:    broker,
		MQID:      trigger.MQID,
		TriggerID: trigger.ID,
		ID:        msg.ID,
		Time:      time.Now().UTC().Format(time.RFC3339),
		Subject:   trigger.Queue,
		Headers:   msg.Headers,
	}
	if json.Valid(msg.Body) {
		env.Body = append(json.RawMessage(nil), msg.Body...)
	} else {
		env.BodyBase64 = base64.StdEncoding.EncodeToString(msg.Body)
		env.IsBase64Encoded = true
	}
	return env
}
