package scheduler

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type CodeObject struct {
	Bucket string
	Key    string
	URL    string
}

type CodeStore interface {
	SaveCode(ctx context.Context, name string, zipData []byte) (CodeObject, error)
}

type minioCodeStore struct {
	client         *minio.Client
	downloadClient *minio.Client
	bucket         string
}

func newMinioCodeStoreFromEnv() (CodeStore, error) {
	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		return nil, nil
	}
	accessKey := envOrDefault("MINIO_ACCESS_KEY", "minioadmin")
	secretKey := envOrDefault("MINIO_SECRET_KEY", "minioadmin")
	bucket := envOrDefault("MINIO_BUCKET", "faas-code")
	useSSL, _ := strconv.ParseBool(os.Getenv("MINIO_USE_SSL"))

	creds := credentials.NewStaticV4(accessKey, secretKey, "")
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  creds,
		Secure: useSSL,
		Region: "us-east-1",
	})
	if err != nil {
		return nil, err
	}
	downloadClient := client
	if podEndpoint := os.Getenv("MINIO_POD_ENDPOINT"); podEndpoint != "" {
		downloadClient, err = minio.New(podEndpoint, &minio.Options{
			Creds:  creds,
			Secure: useSSL,
			Region: "us-east-1",
		})
		if err != nil {
			return nil, err
		}
	}
	store := &minioCodeStore{client: client, downloadClient: downloadClient, bucket: bucket}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := store.ensureBucket(ctx, os.Getenv("MINIO_PUBLIC_READ") == "true"); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *minioCodeStore) ensureBucket(ctx context.Context, publicRead bool) error {
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return err
	}
	if !exists {
		if err := s.client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{}); err != nil {
			return err
		}
	}
	if !publicRead {
		return nil
	}
	return s.client.SetBucketPolicy(ctx, s.bucket, fmt.Sprintf(`{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"AWS": ["*"]},
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::%s/*"]
    }
  ]
}`, s.bucket))
}

func (s *minioCodeStore) SaveCode(ctx context.Context, name string, zipData []byte) (CodeObject, error) {
	key := fmt.Sprintf("functions/%s/%d.zip", dnsLabel(name), time.Now().UnixNano())
	_, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(zipData), int64(len(zipData)), minio.PutObjectOptions{
		ContentType: "application/zip",
	})
	if err != nil {
		return CodeObject{}, err
	}
	codeURL, err := s.downloadClient.PresignedGetObject(ctx, s.bucket, key, 24*time.Hour, url.Values{})
	if err != nil {
		return CodeObject{}, err
	}
	return CodeObject{Bucket: s.bucket, Key: key, URL: codeURL.String()}, nil
}

func envOrDefault(name, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	return value
}
