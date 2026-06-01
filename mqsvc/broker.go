package main

import "context"

type Message struct {
	ID       string
	Body     []byte
	Headers  map[string]string
	Attempts int
}

type Broker interface {
	Subscribe(ctx context.Context, trigger Trigger, handler func(context.Context, Message) MessageResult) error
	Close() error
}

type MessageResult struct {
	Ack    bool
	DLQ    bool
	Reason string
}
