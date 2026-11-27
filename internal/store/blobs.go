package store

import (
	"context"
	"errors"
	"io"

	"github.com/spec-studio-app/plugin-sdk/go/protocol"

	"github.com/spec-studio-app/plugin-temporal/internal/blobstore"
)

func blobErr(err error) error {
	if errors.Is(err, blobstore.ErrNotFound) {
		return protocol.NewError(protocol.NotFound, "The blob does not exist.")
	}
	return protocol.NewError(protocol.Unavailable, "The blob store did not do the call: %v", err)
}

// HasBlobs gives the hashes that the blob store has.
func (s *Store) HasBlobs(ctx context.Context, sha256s []string) ([]string, error) {
	present := []string{}
	for _, h := range sha256s {
		ok, err := s.blobs.Has(ctx, h)
		if err != nil {
			return nil, blobErr(err)
		}
		if ok {
			present = append(present, h)
		}
	}
	return present, nil
}

// PutBlob keeps a blob whose hash the SDK checked.
func (s *Store) PutBlob(ctx context.Context, sha256 string, size int64, r io.Reader) error {
	if err := s.blobs.Put(ctx, sha256, size, r); err != nil {
		return blobErr(err)
	}
	return nil
}

// GetBlob reads part of a blob.
func (s *Store) GetBlob(ctx context.Context, sha256 string, offset int64, length int) ([]byte, int64, error) {
	b, size, err := s.blobs.Get(ctx, sha256, offset, length)
	if err != nil {
		return nil, 0, blobErr(err)
	}
	return b, size, nil
}

// DeleteBlob removes a blob.
func (s *Store) DeleteBlob(ctx context.Context, sha256 string) (bool, error) {
	ok, err := s.blobs.Delete(ctx, sha256)
	if err != nil {
		return false, blobErr(err)
	}
	return ok, nil
}
