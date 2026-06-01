package main

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestPublishingForDeliveryPreservesFallbackMessageID(t *testing.T) {
	delivery := amqp.Delivery{
		ContentType: "application/json",
		Body:        []byte(`{"ok":true}`),
		Headers:     amqp.Table{"existing": "value"},
	}

	publishing := publishingForDelivery(delivery, "orders-trigger-42", 2, "boom")

	if publishing.MessageId != "orders-trigger-42" {
		t.Fatalf("MessageId = %q, want fallback id", publishing.MessageId)
	}
	if publishing.ContentType != delivery.ContentType || string(publishing.Body) != string(delivery.Body) {
		t.Fatalf("unexpected publishing payload: %+v", publishing)
	}
	if publishing.Headers[attemptsHeader] != int32(2) || publishing.Headers["x-faas-reason"] != "boom" || publishing.Headers["existing"] != "value" {
		t.Fatalf("unexpected headers: %+v", publishing.Headers)
	}
	if _, ok := delivery.Headers[attemptsHeader]; ok {
		t.Fatalf("original delivery headers were mutated: %+v", delivery.Headers)
	}
}
