package scheduler

import "context"

type RuntimeInstance struct {
	ID       string
	Addr     string
	FuncName string
	NodeName string
}

type RuntimeBackend interface {
	Start(ctx context.Context, cfg FunctionConfig) (*RuntimeInstance, error)
	Stop(ctx context.Context, id string) error
	Name() string
}
