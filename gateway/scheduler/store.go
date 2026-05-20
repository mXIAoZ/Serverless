package scheduler

import "context"

type FunctionStore interface {
	LoadFunctions(ctx context.Context) ([]FunctionConfig, error)
	SaveFunction(ctx context.Context, cfg FunctionConfig) error
	DeleteFunction(ctx context.Context, name string) error
	Close(ctx context.Context) error
}

type memoryFunctionStore struct{}

func newMemoryFunctionStore() FunctionStore { return memoryFunctionStore{} }

func (memoryFunctionStore) LoadFunctions(context.Context) ([]FunctionConfig, error) {
	return nil, nil
}

func (memoryFunctionStore) SaveFunction(context.Context, FunctionConfig) error {
	return nil
}

func (memoryFunctionStore) DeleteFunction(context.Context, string) error {
	return nil
}

func (memoryFunctionStore) Close(context.Context) error {
	return nil
}
