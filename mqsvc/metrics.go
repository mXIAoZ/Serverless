package main

import "sync"

type TriggerMetrics struct {
	Received  int64  `json:"received"`
	Inflight  int64  `json:"inflight"`
	Acked     int64  `json:"acked"`
	Retried   int64  `json:"retried"`
	DLQ       int64  `json:"dlq"`
	Errors    int64  `json:"errors"`
	LastError string `json:"last_error,omitempty"`
}

type Metrics struct {
	mu       sync.Mutex
	triggers map[string]TriggerMetrics
}

func NewMetrics() *Metrics {
	return &Metrics{triggers: make(map[string]TriggerMetrics)}
}

func (m *Metrics) Received(triggerID string) {
	m.update(triggerID, func(v *TriggerMetrics) { v.Received++; v.Inflight++ })
}
func (m *Metrics) Acked(triggerID string) {
	m.update(triggerID, func(v *TriggerMetrics) {
		v.Acked++
		if v.Inflight > 0 {
			v.Inflight--
		}
	})
}
func (m *Metrics) Retried(triggerID, reason string) {
	m.update(triggerID, func(v *TriggerMetrics) {
		v.Retried++
		v.Errors++
		v.LastError = reason
		if v.Inflight > 0 {
			v.Inflight--
		}
	})
}
func (m *Metrics) DLQed(triggerID, reason string) {
	m.update(triggerID, func(v *TriggerMetrics) {
		v.DLQ++
		v.Errors++
		v.LastError = reason
		if v.Inflight > 0 {
			v.Inflight--
		}
	})
}

func (m *Metrics) update(triggerID string, fn func(*TriggerMetrics)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := m.triggers[triggerID]
	fn(&v)
	m.triggers[triggerID] = v
}

func (m *Metrics) Snapshot() map[string]TriggerMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]TriggerMetrics, len(m.triggers))
	for k, v := range m.triggers {
		out[k] = v
	}
	return out
}
