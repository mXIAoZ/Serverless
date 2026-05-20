package main

import "context"

type ScaleStore interface {
	LoadLatestMetrics(ctx context.Context) (map[string]ContainerMetrics, error)
	LoadLatestDecisions(ctx context.Context) (map[string]*ScaleDecision, error)
	SaveMetrics(ctx context.Context, m ContainerMetrics) error
	SaveDecision(ctx context.Context, d ScaleDecision) error
	SaveStatus(ctx context.Context, s ScaleStatus) error
	Close(ctx context.Context) error
}

type memoryScaleStore struct{}

func newMemoryScaleStore() ScaleStore { return memoryScaleStore{} }

func (memoryScaleStore) LoadLatestMetrics(context.Context) (map[string]ContainerMetrics, error) {
	return nil, nil
}

func (memoryScaleStore) LoadLatestDecisions(context.Context) (map[string]*ScaleDecision, error) {
	return nil, nil
}

func (memoryScaleStore) SaveMetrics(context.Context, ContainerMetrics) error {
	return nil
}

func (memoryScaleStore) SaveDecision(context.Context, ScaleDecision) error {
	return nil
}

func (memoryScaleStore) SaveStatus(context.Context, ScaleStatus) error {
	return nil
}

func (memoryScaleStore) Close(context.Context) error {
	return nil
}
