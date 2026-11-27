//go:build integration

package blobstore

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"

	"github.com/spec-studio-app/plugin-temporal/internal/settings"
)

// bucketFromEnv gives the MinIO of the integration tests (see CI).
func bucketFromEnv(t *testing.T) *settings.Bucket {
	t.Helper()
	ep := os.Getenv("TEST_BUCKET_ENDPOINT")
	if ep == "" {
		t.Skip("TEST_BUCKET_ENDPOINT is not set")
	}
	return &settings.Bucket{Endpoint: ep, Name: os.Getenv("TEST_BUCKET_NAME"), AccessKey: os.Getenv("TEST_BUCKET_ACCESS_KEY"), SecretKey: os.Getenv("TEST_BUCKET_SECRET_KEY"), Region: "auto"}
}

// R8.S2
func TestBucket(t *testing.T) {
	b, err := NewBucket(bucketFromEnv(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	exists, err := b.c.BucketExists(ctx, b.name)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		if err := b.c.MakeBucket(ctx, b.name, minio.MakeBucketOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	exercise(t, b)
	data, sha := randomBlob(t, 10)
	if err := b.Put(ctx, sha, 10, strings.NewReader(string(data))); err != nil {
		t.Fatal(err)
	}
	if _, err := b.c.StatObject(ctx, b.name, "_plugin-temporal/blobs/"+sha, minio.StatObjectOptions{}); err != nil {
		t.Fatalf("the blob is not under _plugin-temporal/blobs/: %v", err)
	}
}
