package main

import "testing"

func TestValidateConfigRejectsUnknownMQID(t *testing.T) {
	err := validateConfig(Config{Triggers: []Trigger{{ID: "orders", Type: "mq", Enabled: true, MQID: "missing", Queue: "orders", Function: "order-handler"}}})
	if err == nil {
		t.Fatal("expected unknown mq_id to fail")
	}
}

func TestValidateConfigRejectsInvalidFunction(t *testing.T) {
	err := validateConfig(Config{
		MQInstances: []MQInstance{{MQID: "rabbitmq-main", Broker: "rabbitmq", URL: "amqp://guest:guest@localhost:5672/"}},
		Triggers:    []Trigger{{ID: "orders", Type: "mq", Enabled: true, MQID: "rabbitmq-main", Queue: "orders", Function: "Bad_Function"}},
	})
	if err == nil {
		t.Fatal("expected invalid function to fail")
	}
}

func TestValidateConfigAcceptsEnabledTrigger(t *testing.T) {
	err := validateConfig(Config{
		MQInstances: []MQInstance{{MQID: "rabbitmq-main", Broker: "rabbitmq", URL: "amqp://guest:guest@localhost:5672/"}},
		Triggers:    []Trigger{{ID: "orders", Type: "mq", Enabled: true, MQID: "rabbitmq-main", Queue: "orders", Function: "order-handler"}},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestValidateConfigRejectsUnsupportedBroker(t *testing.T) {
	err := validateConfig(Config{MQInstances: []MQInstance{{MQID: "rabbitmq-main", Broker: "nats", URL: "nats://localhost:4222"}}})
	if err == nil {
		t.Fatal("expected unsupported broker to fail")
	}
}

func TestValidateConfigRejectsMissingBrokerURL(t *testing.T) {
	err := validateConfig(Config{MQInstances: []MQInstance{{MQID: "rabbitmq-main", Broker: "rabbitmq"}}})
	if err == nil {
		t.Fatal("expected missing url to fail")
	}
}

func TestValidateConfigRejectsInvalidBrokerURL(t *testing.T) {
	err := validateConfig(Config{MQInstances: []MQInstance{{MQID: "rabbitmq-main", Broker: "rabbitmq", URL: "://bad"}}})
	if err == nil {
		t.Fatal("expected invalid url to fail")
	}
}

func TestValidateConfigRejectsWrongRabbitMQScheme(t *testing.T) {
	err := validateConfig(Config{MQInstances: []MQInstance{{MQID: "rabbitmq-main", Broker: "rabbitmq", URL: "http://localhost:5672"}}})
	if err == nil {
		t.Fatal("expected non-amqp rabbitmq url to fail")
	}
}

func TestNormalizeTriggerDefaults(t *testing.T) {
	trigger := normalizeTrigger(Trigger{ID: "orders", Type: "mq", MQID: "rabbitmq-main", Queue: "orders", Function: "order-handler"}, MQInstance{MQID: "rabbitmq-main", Broker: "rabbitmq"})
	if trigger.Broker != "rabbitmq" || trigger.MaxConcurrency != 1 || trigger.Prefetch != 1 || trigger.RetryBackoffMS != 1000 {
		t.Fatalf("unexpected normalized trigger: %+v", trigger)
	}
}

func TestValidateConfigRejectsInvalidTriggerID(t *testing.T) {
	err := validateConfig(Config{
		MQInstances: []MQInstance{{MQID: "rabbitmq-main", Broker: "rabbitmq", URL: "amqp://guest:guest@localhost:5672/"}},
		Triggers:    []Trigger{{ID: "Bad Trigger", Type: "mq", Enabled: true, MQID: "rabbitmq-main", Queue: "orders", Function: "order-handler"}},
	})
	if err == nil {
		t.Fatal("expected invalid trigger id to fail")
	}
}
