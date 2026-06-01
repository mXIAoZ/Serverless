package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const attemptsHeader = "x-faas-attempts"

type RabbitMQBroker struct {
	inst MQInstance

	mu   sync.Mutex
	conn *amqp.Connection
}

func NewRabbitMQBroker(inst MQInstance) (*RabbitMQBroker, error) {
	broker := &RabbitMQBroker{inst: inst}
	if err := broker.connect(); err != nil {
		return nil, err
	}
	return broker, nil
}

func (b *RabbitMQBroker) Subscribe(ctx context.Context, trigger Trigger, handler func(context.Context, Message) MessageResult) error {
	backoff := 2 * time.Second
	for {
		if err := b.subscribeOnce(ctx, trigger, handler); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("[mqsvc] rabbitmq trigger=%s: %v; reconnecting", trigger.ID, err)
			b.reconnect()
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		return nil
	}
}

func (b *RabbitMQBroker) subscribeOnce(ctx context.Context, trigger Trigger, handler func(context.Context, Message) MessageResult) error {
	ch, err := b.channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if _, err := ch.QueueDeclare(trigger.Queue, true, false, false, false, nil); err != nil {
		return err
	}
	if trigger.DLQ != "" {
		if _, err := ch.QueueDeclare(trigger.DLQ, true, false, false, false, nil); err != nil {
			return err
		}
	}
	if err := ch.Qos(trigger.Prefetch, 0, false); err != nil {
		return err
	}
	deliveries, err := ch.Consume(trigger.Queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	sem := make(chan struct{}, trigger.MaxConcurrency)
	var ackMu sync.Mutex
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("delivery channel closed")
			}
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			go func(delivery amqp.Delivery) {
				defer func() { <-sem }()
				b.handleDelivery(ctx, trigger, handler, delivery, &ackMu)
			}(d)
		}
	}
}

func (b *RabbitMQBroker) handleDelivery(ctx context.Context, trigger Trigger, handler func(context.Context, Message) MessageResult, d amqp.Delivery, ackMu *sync.Mutex) {
	msg := Message{ID: d.MessageId, Body: d.Body, Headers: stringHeaders(d.Headers), Attempts: attemptsFromHeaders(d.Headers)}
	if msg.ID == "" {
		msg.ID = fmt.Sprintf("%s-%d", trigger.ID, d.DeliveryTag)
	}
	result := handler(ctx, msg)
	switch {
	case result.Ack:
		ackDelivery(ackMu, d)
	case result.DLQ:
		if trigger.DLQ == "" {
			nackDelivery(ackMu, d, true)
			return
		}
		if err := b.publishWithConfirm(ctx, trigger.DLQ, d, msg.ID, msg.Attempts+1, result.Reason); err != nil {
			log.Printf("[mqsvc] dlq publish failed trigger=%s msg=%s: %v", trigger.ID, msg.ID, err)
			nackDelivery(ackMu, d, true)
			return
		}
		ackDelivery(ackMu, d)
	default:
		backoff := time.Duration(trigger.RetryBackoffMS)*time.Millisecond + time.Duration(rand.Intn(250))*time.Millisecond
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			nackDelivery(ackMu, d, true)
			return
		}
		if err := b.publishWithConfirm(ctx, trigger.Queue, d, msg.ID, msg.Attempts+1, result.Reason); err != nil {
			log.Printf("[mqsvc] retry publish failed trigger=%s msg=%s: %v", trigger.ID, msg.ID, err)
			nackDelivery(ackMu, d, true)
			return
		}
		ackDelivery(ackMu, d)
	}
}

func ackDelivery(mu *sync.Mutex, d amqp.Delivery) {
	mu.Lock()
	defer mu.Unlock()
	_ = d.Ack(false)
}

func nackDelivery(mu *sync.Mutex, d amqp.Delivery, requeue bool) {
	mu.Lock()
	defer mu.Unlock()
	_ = d.Nack(false, requeue)
}

func (b *RabbitMQBroker) publishWithConfirm(ctx context.Context, queue string, d amqp.Delivery, messageID string, attempts int, reason string) error {
	ch, err := b.channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if err := ch.Confirm(false); err != nil {
		return err
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	if err := ch.PublishWithContext(ctx, "", queue, false, false, publishingForDelivery(d, messageID, attempts, reason)); err != nil {
		return err
	}
	select {
	case confirm := <-confirms:
		if !confirm.Ack {
			return fmt.Errorf("publish not confirmed")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return fmt.Errorf("publish confirm timeout")
	}
}

func publishingForDelivery(d amqp.Delivery, messageID string, attempts int, reason string) amqp.Publishing {
	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers[attemptsHeader] = int32(attempts)
	if reason != "" {
		headers["x-faas-reason"] = reason
	}
	return amqp.Publishing{
		ContentType:  d.ContentType,
		MessageId:    messageID,
		Body:         d.Body,
		DeliveryMode: amqp.Persistent,
		Headers:      headers,
	}
}

func (b *RabbitMQBroker) channel() (*amqp.Channel, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn == nil || b.conn.IsClosed() {
		if err := b.connectLocked(); err != nil {
			return nil, err
		}
	}
	return b.conn.Channel()
}

func (b *RabbitMQBroker) connect() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.connectLocked()
}

func (b *RabbitMQBroker) connectLocked() error {
	conn, err := amqp.Dial(b.inst.URL)
	if err != nil {
		return err
	}
	b.conn = conn
	return nil
}

func (b *RabbitMQBroker) reconnect() {
	b.mu.Lock()
	if b.conn != nil {
		_ = b.conn.Close()
		b.conn = nil
	}
	b.mu.Unlock()
}

func (b *RabbitMQBroker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn != nil {
		return b.conn.Close()
	}
	return nil
}

func stringHeaders(headers amqp.Table) map[string]string {
	out := make(map[string]string, len(headers))
	for k, v := range headers {
		out[k] = fmt.Sprint(v)
	}
	return out
}

func attemptsFromHeaders(headers amqp.Table) int {
	value, ok := headers[attemptsHeader]
	if !ok {
		return 0
	}
	switch v := value.(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case string:
		n, _ := strconv.Atoi(v)
		return n
	default:
		return 0
	}
}
