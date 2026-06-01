package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"
)

type GatewayClient struct {
	addr   string
	token  string
	client *http.Client
}

type acquireRequest struct {
	Source         string `json:"source"`
	MQID           string `json:"mq_id"`
	TriggerID      string `json:"trigger_id"`
	MessageID      string `json:"message_id"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
}

type InstanceLease struct {
	LeaseID        string `json:"lease_id"`
	Function       string `json:"function"`
	InstanceID     string `json:"instance_id"`
	Address        string `json:"address"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

func NewGatewayClient(addr string) *GatewayClient {
	return &GatewayClient{addr: addr, token: os.Getenv("INTERNAL_API_TOKEN"), client: &http.Client{Timeout: 10 * time.Second}}
}

func (c *GatewayClient) Acquire(ctx context.Context, trigger Trigger, msg Message) (InstanceLease, error) {
	body, _ := json.Marshal(acquireRequest{Source: "mq", MQID: trigger.MQID, TriggerID: trigger.ID, MessageID: msg.ID})
	url := fmt.Sprintf("http://%s/internal/functions/%s/instances/acquire", c.addr, url.PathEscape(trigger.Function))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return InstanceLease{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	c.authorize(req)
	resp, err := c.client.Do(req)
	if err != nil {
		return InstanceLease{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return InstanceLease{}, fmt.Errorf("gateway acquire returned %s", resp.Status)
	}
	var lease InstanceLease
	if err := json.NewDecoder(resp.Body).Decode(&lease); err != nil {
		return InstanceLease{}, err
	}
	return lease, nil
}

func (c *GatewayClient) Release(ctx context.Context, leaseID string, status string) error {
	body, _ := json.Marshal(map[string]string{"status": status})
	url := fmt.Sprintf("http://%s/internal/leases/%s/release", c.addr, url.PathEscape(leaseID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	c.authorize(req)
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("gateway release returned %s", resp.Status)
	}
	return nil
}

func (c *GatewayClient) Triggers(ctx context.Context) ([]Trigger, error) {
	url := fmt.Sprintf("http://%s/internal/triggers", c.addr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	c.authorize(req)
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("gateway triggers returned %s", resp.Status)
	}
	var triggers []Trigger
	if err := json.NewDecoder(resp.Body).Decode(&triggers); err != nil {
		return nil, err
	}
	return triggers, nil
}

func (c *GatewayClient) authorize(req *http.Request) {
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
}
