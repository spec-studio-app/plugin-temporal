package blobstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"
)

func randomBlob(t *testing.T, n int) ([]byte, string) {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b)
	return b, hex.EncodeToString(h[:])
}

// exercise runs the same checks on any Store.
func exercise(t *testing.T, s Store) {
	ctx := context.Background()
	if err := s.Check(ctx); err != nil {
		t.Fatalf("check: %v", err)
	}
	data, sha := randomBlob(t, 3<<20)
	if ok, err := s.Has(ctx, sha); err != nil || ok {
		t.Fatalf("has before put: %v %v", ok, err)
	}
	if err := s.Put(ctx, sha, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatalf("put: %v", err)
	}
	if ok, err := s.Has(ctx, sha); err != nil || !ok {
		t.Fatalf("has after put: %v %v", ok, err)
	}
	var back []byte
	for off := int64(0); ; {
		chunk, size, err := s.Get(ctx, sha, off, 1<<20)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if size != int64(len(data)) {
			t.Fatalf("size %d", size)
		}
		if len(chunk) == 0 {
			break
		}
		back = append(back, chunk...)
		off += int64(len(chunk))
	}
	if !bytes.Equal(back, data) {
		t.Fatal("the bytes differ")
	}
	if ok, err := s.Delete(ctx, sha); err != nil || !ok {
		t.Fatalf("delete: %v %v", ok, err)
	}
	if ok, _ := s.Delete(ctx, sha); ok {
		t.Fatal("a second delete is true")
	}
	if _, _, err := s.Get(ctx, sha, 0, 10); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get after delete: %v", err)
	}

	name := ExportName("doc/prod/notes/a1", "run-1")
	if _, err := s.GetObject(ctx, name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing object: %v", err)
	}
	for _, text := range []string{"first", "second"} {
		if err := s.PutObject(ctx, name, bytes.NewReader([]byte(text)), int64(len(text))); err != nil {
			t.Fatalf("put object: %v", err)
		}
	}
	r, err := s.GetObject(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	r.Close()
	if string(got) != "second" {
		t.Fatalf("object %q", got)
	}
	if err := s.DeleteObject(ctx, name); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteObject(ctx, name); err != nil {
		t.Fatalf("delete of a missing object: %v", err)
	}
}

// R8.S1
func TestFolder(t *testing.T) {
	f, err := NewFolder(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	exercise(t, f)
}

func TestNames(t *testing.T) {
	if ExportName("doc/p/c/k", "r") != "exports/doc%2Fp%2Fc%2Fk/r.json.gz" {
		t.Fatal(ExportName("doc/p/c/k", "r"))
	}
	if ChainName("spec-p-abc-fix..login") != "chains/spec-p-abc-fix..login.json" {
		t.Fatal(ChainName("spec-p-abc-fix..login"))
	}
	f, _ := NewFolder(t.TempDir())
	if _, err := f.objectPath("../x"); err == nil {
		t.Fatal("a path out of the folder")
	}
}
