package blobstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/spec-studio-app/plugin-temporal/internal/settings"
)

// Prefix is the start of every key that the plugin writes in a bucket.
const Prefix = "_plugin-temporal/"

// Bucket keeps the data in an S3-compatible bucket.
type Bucket struct {
	c        *minio.Client
	name     string
	endpoint string
}

// NewBucket makes a client for the bucket. It does not call the bucket.
func NewBucket(b *settings.Bucket) (*Bucket, error) {
	u, err := url.Parse(b.Endpoint)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("the setting %s is not a valid URL", settings.BucketEndpoint)
	}
	region := b.Region
	if region == "auto" {
		// minio-go finds the region of AWS; "auto" is for R2 and others.
		region = ""
		if !strings.HasSuffix(u.Hostname(), "amazonaws.com") {
			region = "auto"
		}
	}
	c, err := minio.New(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4(b.AccessKey, b.SecretKey, ""),
		Secure:       u.Scheme == "https",
		Region:       region,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		return nil, errors.New("cannot make the bucket client: check the bucket settings")
	}
	return &Bucket{c: c, name: b.Name, endpoint: u.Host}, nil
}

func blobKey(sha string) string { return Prefix + "blobs/" + sha }

// isNotFound is true for a missing key.
func isNotFound(err error) bool {
	var r minio.ErrorResponse
	if errors.As(err, &r) {
		return r.Code == "NoSuchKey" || r.StatusCode == 404
	}
	return false
}

// clean gives an error with no request details, so no signature or key shows.
func clean(op string, err error) error {
	var r minio.ErrorResponse
	if errors.As(err, &r) {
		return fmt.Errorf("the bucket refused %s: %s (HTTP %d)", op, r.Code, r.StatusCode)
	}
	return fmt.Errorf("the bucket did not answer %s", op)
}

// Where gives the endpoint and the bucket name.
func (b *Bucket) Where() string { return "bucket " + b.name + " at " + b.endpoint }

// Check writes, reads and removes a small object.
func (b *Bucket) Check(ctx context.Context) error {
	key := Prefix + "check"
	if _, err := b.c.PutObject(ctx, b.name, key, bytes.NewReader([]byte("ok")), 2, minio.PutObjectOptions{}); err != nil {
		return clean("a write", err)
	}
	if err := b.c.RemoveObject(ctx, b.name, key, minio.RemoveObjectOptions{}); err != nil {
		return clean("a delete", err)
	}
	return nil
}

// Has checks the blob key.
func (b *Bucket) Has(ctx context.Context, sha string) (bool, error) {
	_, err := b.c.StatObject(ctx, b.name, blobKey(sha), minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, clean("a stat", err)
}

// Put writes the blob.
func (b *Bucket) Put(ctx context.Context, sha string, size int64, r io.Reader) error {
	_, err := b.c.PutObject(ctx, b.name, blobKey(sha), r, size, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		return clean("a write", err)
	}
	return nil
}

// Get reads a range of the blob.
func (b *Bucket) Get(ctx context.Context, sha string, offset int64, length int) ([]byte, int64, error) {
	info, err := b.c.StatObject(ctx, b.name, blobKey(sha), minio.StatObjectOptions{})
	if isNotFound(err) {
		return nil, 0, ErrNotFound
	}
	if err != nil {
		return nil, 0, clean("a stat", err)
	}
	size := info.Size
	if offset >= size || length == 0 {
		return []byte{}, size, nil
	}
	end := offset + int64(length) - 1
	if end >= size {
		end = size - 1
	}
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(offset, end); err != nil {
		return nil, 0, err
	}
	obj, err := b.c.GetObject(ctx, b.name, blobKey(sha), opts)
	if err != nil {
		return nil, 0, clean("a read", err)
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		if isNotFound(err) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, clean("a read", err)
	}
	return data, size, nil
}

// Delete removes the blob.
func (b *Bucket) Delete(ctx context.Context, sha string) (bool, error) {
	ok, err := b.Has(ctx, sha)
	if err != nil || !ok {
		return false, err
	}
	if err := b.c.RemoveObject(ctx, b.name, blobKey(sha), minio.RemoveObjectOptions{}); err != nil {
		return false, clean("a delete", err)
	}
	return true, nil
}

// PutObject writes a named object.
func (b *Bucket) PutObject(ctx context.Context, name string, r io.Reader, size int64) error {
	_, err := b.c.PutObject(ctx, b.name, Prefix+name, r, size, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		return clean("a write", err)
	}
	return nil
}

// GetObject opens a named object.
func (b *Bucket) GetObject(ctx context.Context, name string) (io.ReadCloser, error) {
	if _, err := b.c.StatObject(ctx, b.name, Prefix+name, minio.StatObjectOptions{}); err != nil {
		if isNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, clean("a stat", err)
	}
	obj, err := b.c.GetObject(ctx, b.name, Prefix+name, minio.GetObjectOptions{})
	if err != nil {
		return nil, clean("a read", err)
	}
	return obj, nil
}

// DeleteObject removes a named object.
func (b *Bucket) DeleteObject(ctx context.Context, name string) error {
	if err := b.c.RemoveObject(ctx, b.name, Prefix+name, minio.RemoveObjectOptions{}); err != nil && !isNotFound(err) {
		return clean("a delete", err)
	}
	return nil
}
