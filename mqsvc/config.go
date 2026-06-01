package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	GatewayAddr string       `json:"gateway_addr"`
	MQInstances []MQInstance `json:"mq_instances"`
	Triggers    []Trigger    `json:"triggers"`
}

type MQInstance struct {
	MQID   string `json:"mq_id"`
	Broker string `json:"broker"`
	URL    string `json:"url"`
}

type Trigger struct {
	ID             string `json:"id"`
	Type           string `json:"type"`
	Enabled        bool   `json:"enabled"`
	MQID           string `json:"mq_id"`
	Queue          string `json:"queue"`
	Function       string `json:"function"`
	MaxConcurrency int    `json:"max_concurrency"`
	Prefetch       int    `json:"prefetch"`
	MaxAttempts    int    `json:"max_attempts"`
	RetryBackoffMS int    `json:"retry_backoff_ms"`
	DLQ            string `json:"dlq"`
	Broker         string `json:"-"`
}

func loadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	if cfg.GatewayAddr == "" {
		cfg.GatewayAddr = envString("GATEWAY_INTERNAL_ADDR", "localhost:8081")
	}
	if err := validateConfig(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func validateConfig(cfg Config) error {
	mqIDs := make(map[string]struct{}, len(cfg.MQInstances))
	for _, inst := range cfg.MQInstances {
		if inst.MQID == "" || inst.Broker == "" || inst.URL == "" {
			return fmt.Errorf("mq instance requires mq_id, broker, and url")
		}
		if inst.Broker != "rabbitmq" {
			return fmt.Errorf("unsupported mq broker %q", inst.Broker)
		}
		if err := validateBrokerURL(inst); err != nil {
			return err
		}
		if _, exists := mqIDs[inst.MQID]; exists {
			return fmt.Errorf("duplicate mq_id %q", inst.MQID)
		}
		mqIDs[inst.MQID] = struct{}{}
	}

	triggerIDs := make(map[string]struct{}, len(cfg.Triggers))
	for _, trigger := range cfg.Triggers {
		if trigger.ID == "" {
			return fmt.Errorf("trigger id is required")
		}
		if trigger.ID != dnsLabel(trigger.ID) {
			return fmt.Errorf("trigger %q has invalid id", trigger.ID)
		}
		if _, exists := triggerIDs[trigger.ID]; exists {
			return fmt.Errorf("duplicate trigger id %q", trigger.ID)
		}
		triggerIDs[trigger.ID] = struct{}{}
		if trigger.Type != "mq" {
			return fmt.Errorf("unsupported trigger type %q", trigger.Type)
		}
		if trigger.MaxConcurrency < 0 {
			return fmt.Errorf("trigger %q max_concurrency must be >= 0", trigger.ID)
		}
		if trigger.MaxAttempts < 0 {
			return fmt.Errorf("trigger %q max_attempts must be >= 0", trigger.ID)
		}
		if trigger.Prefetch < 0 {
			return fmt.Errorf("trigger %q prefetch must be >= 0", trigger.ID)
		}
		if trigger.RetryBackoffMS < 0 {
			return fmt.Errorf("trigger %q retry_backoff_ms must be >= 0", trigger.ID)
		}
		if !trigger.Enabled {
			continue
		}
		if trigger.MQID == "" || trigger.Queue == "" || trigger.Function == "" {
			return fmt.Errorf("enabled trigger %q requires mq_id, queue, and function", trigger.ID)
		}
		if _, ok := mqIDs[trigger.MQID]; !ok {
			return fmt.Errorf("trigger %q references unknown mq_id %q", trigger.ID, trigger.MQID)
		}
		if !isDNSLabel(trigger.Function) {
			return fmt.Errorf("trigger %q has invalid function %q", trigger.ID, trigger.Function)
		}
	}
	return nil
}

func validateBrokerURL(inst MQInstance) error {
	u, err := url.ParseRequestURI(inst.URL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("mq instance %q has invalid url", inst.MQID)
	}
	if inst.Broker == "rabbitmq" && u.Scheme != "amqp" && u.Scheme != "amqps" {
		return fmt.Errorf("mq instance %q rabbitmq url must use amqp or amqps", inst.MQID)
	}
	return nil
}

func normalizeTrigger(trigger Trigger, inst MQInstance) Trigger {
	trigger.Broker = inst.Broker
	if trigger.MaxConcurrency <= 0 {
		trigger.MaxConcurrency = 1
	}
	if trigger.Prefetch <= 0 {
		trigger.Prefetch = trigger.MaxConcurrency
	}
	if trigger.RetryBackoffMS <= 0 {
		trigger.RetryBackoffMS = 1000
	}
	return trigger
}

func instanceByID(cfg Config) map[string]MQInstance {
	instances := make(map[string]MQInstance, len(cfg.MQInstances))
	for _, inst := range cfg.MQInstances {
		instances[inst.MQID] = inst
	}
	return instances
}

func envString(key string, def string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return def
}

func envInt(key string, def int) int {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return def
}

func dnsLabel(s string) string {
	var b strings.Builder
	lastDash := false
	for _, r := range strings.ToLower(s) {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if r == '-' && b.Len() > 0 && !lastDash {
			b.WriteRune('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if len(out) > 63 {
		out = strings.TrimRight(out[:63], "-")
	}
	return out
}

func isDNSLabel(s string) bool {
	if s == "" || len(s) > 63 || strings.HasPrefix(s, "-") || strings.HasSuffix(s, "-") {
		return false
	}
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			continue
		}
		return false
	}
	return true
}
