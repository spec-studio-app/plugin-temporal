package blobstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Folder keeps the data in a local folder.
type Folder struct {
	dir string
}

// NewFolder makes the folder if it does not exist.
func NewFolder(dir string) (*Folder, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return nil, fmt.Errorf("cannot make the blob folder: %w", err)
	}
	return &Folder{dir: abs}, nil
}

func (f *Folder) blobPath(sha string) string {
	return filepath.Join(f.dir, "blobs", sha[:2], sha)
}

// objectPath keeps the path in the folder. The names come from ExportName
// and ChainName, which escape each part.
func (f *Folder) objectPath(name string) (string, error) {
	p := filepath.Join(f.dir, filepath.FromSlash(name))
	if !strings.HasPrefix(p, f.dir+string(filepath.Separator)) {
		return "", fmt.Errorf("the object name %q is not in the blob folder", name)
	}
	return p, nil
}

// Where gives the folder.
func (f *Folder) Where() string { return "folder " + f.dir }

// Check writes and removes a file.
func (f *Folder) Check(ctx context.Context) error {
	tmp, err := os.CreateTemp(f.dir, ".check-*")
	if err != nil {
		return fmt.Errorf("cannot write to the blob folder: %w", err)
	}
	tmp.Close()
	return os.Remove(tmp.Name())
}

// writeAtomic writes to a temp file in the same folder and renames it, so a
// reader never sees part of a file.
func writeAtomic(path string, r io.Reader) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := io.Copy(tmp, r); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// Has checks the blob file.
func (f *Folder) Has(ctx context.Context, sha string) (bool, error) {
	_, err := os.Stat(f.blobPath(sha))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

// Put writes the blob file.
func (f *Folder) Put(ctx context.Context, sha string, size int64, r io.Reader) error {
	return writeAtomic(f.blobPath(sha), r)
}

// Get reads part of the blob file.
func (f *Folder) Get(ctx context.Context, sha string, offset int64, length int) ([]byte, int64, error) {
	file, err := os.Open(f.blobPath(sha))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, 0, ErrNotFound
	}
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()
	st, err := file.Stat()
	if err != nil {
		return nil, 0, err
	}
	size := st.Size()
	if offset >= size {
		return []byte{}, size, nil
	}
	n := int64(length)
	if offset+n > size {
		n = size - offset
	}
	buf := make([]byte, n)
	if _, err := file.ReadAt(buf, offset); err != nil && !errors.Is(err, io.EOF) {
		return nil, 0, err
	}
	return buf, size, nil
}

// Delete removes the blob file.
func (f *Folder) Delete(ctx context.Context, sha string) (bool, error) {
	err := os.Remove(f.blobPath(sha))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

// PutObject writes a named object.
func (f *Folder) PutObject(ctx context.Context, name string, r io.Reader, size int64) error {
	p, err := f.objectPath(name)
	if err != nil {
		return err
	}
	return writeAtomic(p, r)
}

// GetObject opens a named object.
func (f *Folder) GetObject(ctx context.Context, name string) (io.ReadCloser, error) {
	p, err := f.objectPath(name)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotFound
	}
	return file, err
}

// DeleteObject removes a named object.
func (f *Folder) DeleteObject(ctx context.Context, name string) error {
	p, err := f.objectPath(name)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}
