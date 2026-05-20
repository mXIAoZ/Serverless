package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type mongoScaleStore struct {
	client    *mongo.Client
	metrics   *mongo.Collection
	decisions *mongo.Collection
	statuses  *mongo.Collection
	timeout   time.Duration
}

type metricDocument struct {
	ContainerMetrics `bson:",inline"`
	ID               string    `bson:"_id"`
	UpdatedAt        time.Time `bson:"updated_at"`
}

type decisionDocument struct {
	ScaleDecision `bson:",inline"`
}

type statusDocument struct {
	ScaleStatus `bson:",inline"`
	ID          string    `bson:"_id"`
	UpdatedAt   time.Time `bson:"updated_at"`
}

func newScaleStoreFromEnv() (ScaleStore, error) {
	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		return newMemoryScaleStore(), nil
	}

	timeout := mongoTimeout()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}

	dbName := os.Getenv("MONGO_DB")
	if dbName == "" {
		dbName = "faas"
	}
	db := client.Database(dbName)
	store := &mongoScaleStore{
		client:    client,
		metrics:   db.Collection("container_metrics"),
		decisions: db.Collection("scale_decisions"),
		statuses:  db.Collection("scale_status"),
		timeout:   timeout,
	}
	if err := store.ensureIndexes(ctx); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	return store, nil
}

func (s *mongoScaleStore) ensureIndexes(ctx context.Context) error {
	_, err := s.decisions.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "func_name", Value: 1}, {Key: "time", Value: -1}}})
	return err
}

func (s *mongoScaleStore) LoadLatestMetrics(ctx context.Context) (map[string]ContainerMetrics, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	cur, err := s.metrics.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	metrics := make(map[string]ContainerMetrics)
	for cur.Next(ctx) {
		var doc metricDocument
		if err := cur.Decode(&doc); err != nil {
			return nil, err
		}
		metrics[doc.ContainerID] = doc.ContainerMetrics
	}
	return metrics, cur.Err()
}

func (s *mongoScaleStore) LoadLatestDecisions(ctx context.Context) (map[string]*ScaleDecision, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	cur, err := s.statuses.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	decisions := make(map[string]*ScaleDecision)
	for cur.Next(ctx) {
		var doc statusDocument
		if err := cur.Decode(&doc); err != nil {
			return nil, err
		}
		if doc.LastDecision != nil {
			decisions[doc.FuncName] = doc.LastDecision
		}
	}
	return decisions, cur.Err()
}

func (s *mongoScaleStore) SaveMetrics(ctx context.Context, m ContainerMetrics) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	doc := metricDocument{ContainerMetrics: m, ID: m.ContainerID, UpdatedAt: time.Now().UTC()}
	_, err := s.metrics.ReplaceOne(ctx, bson.D{{Key: "_id", Value: m.ContainerID}}, doc, options.Replace().SetUpsert(true))
	return err
}

func (s *mongoScaleStore) SaveDecision(ctx context.Context, d ScaleDecision) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	_, err := s.decisions.InsertOne(ctx, decisionDocument{ScaleDecision: d})
	return err
}

func (s *mongoScaleStore) SaveStatus(ctx context.Context, status ScaleStatus) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	doc := statusDocument{ScaleStatus: status, ID: status.FuncName, UpdatedAt: time.Now().UTC()}
	_, err := s.statuses.ReplaceOne(ctx, bson.D{{Key: "_id", Value: status.FuncName}}, doc, options.Replace().SetUpsert(true))
	return err
}

func (s *mongoScaleStore) Close(ctx context.Context) error {
	return s.client.Disconnect(ctx)
}

func (s *mongoScaleStore) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, s.timeout)
}

func mongoTimeout() time.Duration {
	ms := 3000
	if v := os.Getenv("MONGO_TIMEOUT_MS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			panic(fmt.Sprintf("invalid MONGO_TIMEOUT_MS %q", v))
		}
		ms = n
	}
	return time.Duration(ms) * time.Millisecond
}
