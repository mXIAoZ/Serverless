package scheduler

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

type mongoFunctionStore struct {
	client     *mongo.Client
	collection *mongo.Collection
	timeout    time.Duration
}

type functionDocument struct {
	ID        string    `bson:"_id"`
	Name      string    `bson:"name"`
	Image     string    `bson:"image"`
	Runtime   string    `bson:"runtime"`
	Timeout   int       `bson:"timeout"`
	Memory    int       `bson:"memory"`
	Handler   string    `bson:"handler"`
	CodeDir   string    `bson:"code_dir"`
	CodeKey   string    `bson:"code_key"`
	CodeURL   string    `bson:"code_url"`
	CreatedAt time.Time `bson:"created_at"`
	UpdatedAt time.Time `bson:"updated_at"`
}

func newFunctionStoreFromEnv() (FunctionStore, error) {
	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		return newMemoryFunctionStore(), nil
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
	return &mongoFunctionStore{
		client:     client,
		collection: client.Database(dbName).Collection("functions"),
		timeout:    timeout,
	}, nil
}

func (s *mongoFunctionStore) LoadFunctions(ctx context.Context) ([]FunctionConfig, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	cur, err := s.collection.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var docs []functionDocument
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}

	configs := make([]FunctionConfig, 0, len(docs))
	for _, doc := range docs {
		configs = append(configs, FunctionConfig{
			Name:    doc.Name,
			Image:   doc.Image,
			Runtime: doc.Runtime,
			Timeout: doc.Timeout,
			Memory:  doc.Memory,
			Handler: doc.Handler,
			CodeDir: doc.CodeDir,
			CodeKey: doc.CodeKey,
			CodeURL: doc.CodeURL,
		})
	}
	return configs, nil
}

func (s *mongoFunctionStore) SaveFunction(ctx context.Context, cfg FunctionConfig) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	now := time.Now().UTC()
	update := bson.D{{Key: "$set", Value: bson.D{
		{Key: "name", Value: cfg.Name},
		{Key: "image", Value: cfg.Image},
		{Key: "runtime", Value: cfg.Runtime},
		{Key: "timeout", Value: cfg.Timeout},
		{Key: "memory", Value: cfg.Memory},
		{Key: "handler", Value: cfg.Handler},
		{Key: "code_dir", Value: cfg.CodeDir},
		{Key: "code_key", Value: cfg.CodeKey},
		{Key: "code_url", Value: cfg.CodeURL},
		{Key: "updated_at", Value: now},
	}}, {Key: "$setOnInsert", Value: bson.D{{Key: "created_at", Value: now}}}}

	_, err := s.collection.UpdateByID(ctx, cfg.Name, update, options.Update().SetUpsert(true))
	return err
}

func (s *mongoFunctionStore) DeleteFunction(ctx context.Context, name string) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	_, err := s.collection.DeleteOne(ctx, bson.D{{Key: "_id", Value: name}})
	return err
}

func (s *mongoFunctionStore) Close(ctx context.Context) error {
	return s.client.Disconnect(ctx)
}

func (s *mongoFunctionStore) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
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
