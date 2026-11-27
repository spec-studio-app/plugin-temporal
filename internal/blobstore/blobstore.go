// Package blobstore keeps the blobs (by SHA-256), the history exports and the
// chain manifests, in an S3-compatible bucket or in a local folder.
//
// In a bucket every key starts with "_plugin-temporal/". A relay deploy ID
// cannot start with "_", so the keys never touch the relay's attachments
// ("<deployId>/..."). In a folder: blobs/, exports/ and chains/.
package blobstore

import (
	"context"
	"errors"
	"io"
	"net/url"
)

// ErrNotFound is the error for a missing blob or object.
var ErrNotFound = errors.New("not found")

// Store is a blob store.
type Store interface {
	// Check returns an error when the store cannot be reached or written.
	Check(ctx context.Context) error
	// Has is true when the store has the blob.
	Has(ctx context.Context, sha256 string) (bool, error)
	// Put keeps a blob whose hash the caller checked.
	Put(ctx context.Context, sha256 string, size int64, r io.Reader) error
	// Get returns up to length bytes at offset, and the size of the blob.
	Get(ctx context.Context, sha256 string, offset int64, length int) ([]byte, int64, error)
	// Delete is false when the store did not have the blob.
	Delete(ctx context.Context, sha256 string) (bool, error)
	// PutObject writes a named object (an export or a manifest). It replaces
	// an object with the same name.
	PutObject(ctx context.Context, name string, r io.Reader, size int64) error
	// GetObject opens a named object, or returns ErrNotFound.
	GetObject(ctx context.Context, name string) (io.ReadCloser, error)
	// DeleteObject removes a named object. A missing object is not an error.
	DeleteObject(ctx context.Context, name string) error
	// Where says where the store keeps its data, for the log. No secret.
	Where() string
}

// ExportName is the object name of a run's history export.
func ExportName(workflowID, runID string) string {
	return "exports/" + url.PathEscape(workflowID) + "/" + url.PathEscape(runID) + ".json.gz"
}

// ChainName is the object name of a workflow's chain manifest.
func ChainName(workflowID string) string {
	return "chains/" + url.PathEscape(workflowID) + ".json"
}
