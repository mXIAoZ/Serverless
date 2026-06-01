package main

import (
	"context"
	"testing"
	"time"
)

func TestHasEnabledTriggers(t *testing.T) {
	if hasEnabledTriggers([]Trigger{{ID: "orders", Enabled: false}}) {
		t.Fatal("disabled triggers should not require broker startup")
	}
	if !hasEnabledTriggers([]Trigger{{ID: "orders", Enabled: true}}) {
		t.Fatal("enabled trigger should require broker startup")
	}
}

type fakeBroker struct {
	started  chan Trigger
	canceled chan string
}

func (b *fakeBroker) Subscribe(ctx context.Context, trigger Trigger, _ func(context.Context, Message) MessageResult) error {
	if b.started != nil {
		b.started <- trigger
	}
	<-ctx.Done()
	if b.canceled != nil {
		b.canceled <- triggerKey(trigger)
	}
	return ctx.Err()
}

func (b *fakeBroker) Close() error { return nil }

func TestMergeTriggersPrefersRemote(t *testing.T) {
	triggers := mergeTriggers(
		[]Trigger{{ID: "orders", Type: "mq", Enabled: true, MQID: "rabbitmq-main", Queue: "local", Function: "order-fn"}},
		[]Trigger{{ID: "orders", Type: "mq", Enabled: true, MQID: "rabbitmq-main", Queue: "remote", Function: "order-fn"}},
	)
	if len(triggers) != 1 || triggers[0].Queue != "remote" || triggers[0].Function != "order-fn" {
		t.Fatalf("unexpected triggers: %+v", triggers)
	}
}

func TestMergeTriggersKeepsSameIDForDifferentFunctions(t *testing.T) {
	triggers := mergeTriggers(
		nil,
		[]Trigger{
			{ID: "orders", Type: "mq", Enabled: true, MQID: "rabbitmq-main", Queue: "orders-a", Function: "order-a"},
			{ID: "orders", Type: "mq", Enabled: true, MQID: "rabbitmq-main", Queue: "orders-b", Function: "order-b"},
		},
	)
	if len(triggers) != 2 {
		t.Fatalf("trigger count = %d, want 2: %+v", len(triggers), triggers)
	}
}

func TestReconcileTriggersCancelsRemovedTrigger(t *testing.T) {
	broker := &fakeBroker{started: make(chan Trigger, 1), canceled: make(chan string, 1)}
	svc := NewService(Config{MQInstances: []MQInstance{{MQID: "rabbitmq-main", Broker: "rabbitmq", URL: "amqp://guest:guest@localhost:5672/"}}})
	svc.brokers["rabbitmq-main"] = broker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	trigger := Trigger{ID: "orders", Type: "mq", Enabled: true, MQID: "rabbitmq-main", Queue: "orders", Function: "order-handler"}
	if err := svc.reconcileTriggers(ctx, []Trigger{trigger}); err != nil {
		t.Fatal(err)
	}
	<-broker.started
	if err := svc.reconcileTriggers(ctx, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case key := <-broker.canceled:
		if key != "rabbitmq-main:order-handler:orders" {
			t.Fatalf("canceled key = %q", key)
		}
	case <-time.After(time.Second):
		t.Fatal("trigger was not canceled")
	}
}

func TestReconcileTriggersRestartsChangedTrigger(t *testing.T) {
	broker := &fakeBroker{started: make(chan Trigger, 2), canceled: make(chan string, 1)}
	svc := NewService(Config{MQInstances: []MQInstance{{MQID: "rabbitmq-main", Broker: "rabbitmq", URL: "amqp://guest:guest@localhost:5672/"}}})
	svc.brokers["rabbitmq-main"] = broker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	trigger := Trigger{ID: "orders", Type: "mq", Enabled: true, MQID: "rabbitmq-main", Queue: "orders", Function: "order-handler"}
	if err := svc.reconcileTriggers(ctx, []Trigger{trigger}); err != nil {
		t.Fatal(err)
	}
	<-broker.started
	trigger.Queue = "orders-v2"
	if err := svc.reconcileTriggers(ctx, []Trigger{trigger}); err != nil {
		t.Fatal(err)
	}
	<-broker.canceled
	started := <-broker.started
	if started.Queue != "orders-v2" {
		t.Fatalf("started trigger = %+v", started)
	}
}
