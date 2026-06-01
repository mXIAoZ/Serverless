package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"
)

type consumerState struct {
	trigger Trigger
	cancel  context.CancelFunc
}

type Service struct {
	cfg     Config
	worker  *Worker
	metrics *Metrics

	mu        sync.RWMutex
	triggers  []Trigger
	brokers   map[string]Broker
	consumers map[string]consumerState
}

func main() {
	cfg := defaultConfig()
	configPath := os.Getenv("MQ_CONFIG_PATH")
	if configPath == "" {
		log.Println("[mqsvc] MQ_CONFIG_PATH not set; syncing triggers from gateway")
	} else {
		loaded, err := loadConfig(configPath)
		if err != nil {
			log.Fatalf("[mqsvc] load config: %v", err)
		}
		cfg = loaded
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	svc := NewService(cfg)
	if err := svc.Start(ctx); err != nil {
		log.Fatalf("[mqsvc] start: %v", err)
	}
	defer svc.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/triggers", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(svc.Triggers())
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(svc.metrics.Snapshot())
	})

	server := &http.Server{Addr: envString("MQSVC_LISTEN", ":9400"), Handler: mux}
	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()
	log.Printf("[mqsvc] listening on %s", server.Addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func defaultConfig() Config {
	cfg := Config{GatewayAddr: envString("GATEWAY_INTERNAL_ADDR", "localhost:8081")}
	if rabbitURL := os.Getenv("RABBITMQ_URL"); rabbitURL != "" {
		cfg.MQInstances = []MQInstance{{MQID: "rabbitmq-main", Broker: "rabbitmq", URL: rabbitURL}}
	}
	return cfg
}

func NewService(cfg Config) *Service {
	metrics := NewMetrics()
	worker := NewWorker(NewGatewayClient(cfg.GatewayAddr))
	worker.SetMetrics(metrics)
	return &Service{
		cfg:       cfg,
		worker:    worker,
		metrics:   metrics,
		brokers:   make(map[string]Broker),
		consumers: make(map[string]consumerState),
	}
}

func (s *Service) Start(ctx context.Context) error {
	if err := s.syncTriggers(ctx); err != nil {
		if hasEnabledTriggers(s.cfg.Triggers) {
			return err
		}
		log.Printf("[mqsvc] initial trigger sync: %v", err)
	}
	go s.reconcile(ctx)
	return nil
}

func (s *Service) reconcile(ctx context.Context) {
	interval := time.Duration(envInt("MQ_TRIGGER_SYNC_INTERVAL_MS", 5000)) * time.Millisecond
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := s.syncTriggers(ctx); err != nil {
				log.Printf("[mqsvc] sync triggers: %v", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *Service) syncTriggers(ctx context.Context) error {
	triggers := s.cfg.Triggers
	if discovered, err := s.worker.gateway.Triggers(ctx); err == nil {
		triggers = mergeTriggers(s.cfg.Triggers, discovered)
	} else if len(triggers) == 0 {
		return err
	} else {
		log.Printf("[mqsvc] gateway trigger sync failed; using config triggers: %v", err)
	}
	return s.reconcileTriggers(ctx, triggers)
}

func mergeTriggers(local []Trigger, remote []Trigger) []Trigger {
	merged := make(map[string]Trigger, len(local)+len(remote))
	for _, trigger := range local {
		merged[triggerKey(trigger)] = trigger
	}
	for _, trigger := range remote {
		merged[triggerKey(trigger)] = trigger
	}
	keys := make([]string, 0, len(merged))
	for key := range merged {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	triggers := make([]Trigger, 0, len(keys))
	for _, key := range keys {
		triggers = append(triggers, merged[key])
	}
	return triggers
}

func (s *Service) reconcileTriggers(ctx context.Context, triggers []Trigger) error {
	instances := instanceByID(s.cfg)
	desired := make(map[string]Trigger)
	keys := make([]string, 0, len(triggers))
	for _, trigger := range triggers {
		if !trigger.Enabled {
			continue
		}
		inst, ok := instances[trigger.MQID]
		if !ok {
			return fmt.Errorf("trigger %q references unknown mq_id %q", trigger.ID, trigger.MQID)
		}
		trigger = normalizeTrigger(trigger, inst)
		key := triggerKey(trigger)
		desired[key] = trigger
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var start []Trigger
	s.mu.Lock()
	for key, consumer := range s.consumers {
		trigger, ok := desired[key]
		if !ok || !sameTrigger(consumer.trigger, trigger) {
			consumer.cancel()
			delete(s.consumers, key)
		}
	}
	for _, key := range keys {
		trigger := desired[key]
		consumer, ok := s.consumers[key]
		if !ok || !sameTrigger(consumer.trigger, trigger) {
			start = append(start, trigger)
		}
	}
	s.triggers = make([]Trigger, 0, len(keys))
	for _, key := range keys {
		s.triggers = append(s.triggers, desired[key])
	}
	s.mu.Unlock()

	for _, trigger := range start {
		if err := s.startConsumer(ctx, trigger); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) startConsumer(ctx context.Context, trigger Trigger) error {
	inst := instanceByID(s.cfg)[trigger.MQID]
	broker, err := s.brokerFor(inst)
	if err != nil {
		return err
	}
	consumerCtx, cancel := context.WithCancel(ctx)
	key := triggerKey(trigger)

	s.mu.Lock()
	if existing, ok := s.consumers[key]; ok {
		s.mu.Unlock()
		cancel()
		if sameTrigger(existing.trigger, trigger) {
			return nil
		}
		return fmt.Errorf("trigger %q is already being replaced", trigger.ID)
	}
	s.consumers[key] = consumerState{trigger: trigger, cancel: cancel}
	s.mu.Unlock()

	go func(b Broker, t Trigger) {
		handler := func(ctx context.Context, msg Message) MessageResult {
			return s.worker.HandleMessage(ctx, t, msg)
		}
		err := b.Subscribe(consumerCtx, t, handler)
		if err != nil && consumerCtx.Err() == nil {
			log.Printf("[mqsvc] trigger %s stopped: %v", t.ID, err)
		}
		s.mu.Lock()
		if current, ok := s.consumers[key]; ok && sameTrigger(current.trigger, t) {
			delete(s.consumers, key)
		}
		s.mu.Unlock()
	}(broker, trigger)
	return nil
}

func (s *Service) brokerFor(inst MQInstance) (Broker, error) {
	s.mu.RLock()
	broker := s.brokers[inst.MQID]
	s.mu.RUnlock()
	if broker != nil {
		return broker, nil
	}

	created, err := newBroker(inst)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if broker = s.brokers[inst.MQID]; broker != nil {
		_ = created.Close()
		return broker, nil
	}
	s.brokers[inst.MQID] = created
	return created, nil
}

func (s *Service) Triggers() []Trigger {
	s.mu.RLock()
	defer s.mu.RUnlock()
	triggers := make([]Trigger, len(s.triggers))
	copy(triggers, s.triggers)
	return triggers
}

func (s *Service) Close() {
	s.mu.Lock()
	consumers := make([]consumerState, 0, len(s.consumers))
	for _, consumer := range s.consumers {
		consumers = append(consumers, consumer)
	}
	brokers := make([]Broker, 0, len(s.brokers))
	for _, broker := range s.brokers {
		brokers = append(brokers, broker)
	}
	s.mu.Unlock()

	for _, consumer := range consumers {
		consumer.cancel()
	}
	for _, broker := range brokers {
		_ = broker.Close()
	}
}

func newBroker(inst MQInstance) (Broker, error) {
	switch inst.Broker {
	case "rabbitmq":
		return NewRabbitMQBroker(inst)
	default:
		return nil, http.ErrNotSupported
	}
}

func triggerKey(trigger Trigger) string {
	return trigger.MQID + ":" + trigger.Function + ":" + trigger.ID
}

func sameTrigger(a Trigger, b Trigger) bool {
	return a.ID == b.ID &&
		a.Type == b.Type &&
		a.Enabled == b.Enabled &&
		a.MQID == b.MQID &&
		a.Queue == b.Queue &&
		a.Function == b.Function &&
		a.MaxConcurrency == b.MaxConcurrency &&
		a.Prefetch == b.Prefetch &&
		a.MaxAttempts == b.MaxAttempts &&
		a.RetryBackoffMS == b.RetryBackoffMS &&
		a.DLQ == b.DLQ &&
		a.Broker == b.Broker
}

func hasEnabledTriggers(triggers []Trigger) bool {
	for _, trigger := range triggers {
		if trigger.Enabled {
			return true
		}
	}
	return false
}
