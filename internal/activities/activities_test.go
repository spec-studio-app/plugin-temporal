package activities

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/spec-studio-app/plugin-temporal/internal/blobstore"
	"github.com/spec-studio-app/plugin-temporal/internal/codec"
	"github.com/spec-studio-app/plugin-temporal/internal/settings"
	"github.com/spec-studio-app/plugin-temporal/internal/workflows"
)

// A state object is encrypted, and reads back one document at a time.
func TestSealedState(t *testing.T) {
	dir := t.TempDir()
	f, _ := blobstore.NewFolder(dir)
	c, _ := codec.New([]settings.PayloadKey{{ID: "k", Key: bytes.Repeat([]byte{5}, 32)}})
	a := &Activities{Blobs: f, DC: codec.Converter(c)}
	docs := []any{
		workflows.Doc{Collection: "specs", Key: "a:b", Value: json.RawMessage(`"PLAIN-ONE"`)},
		workflows.Doc{Collection: "app-specs", Key: "a:b", Value: json.RawMessage(`"PLAIN-TWO"`)},
	}
	name := blobstore.StateName("spec-x", "run-1")
	if err := a.writeSealed(context.Background(), name, docs); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, name))
	if bytes.Contains(raw, []byte("PLAIN-")) {
		t.Fatal("the state object is not encrypted")
	}
	for i, want := range []string{`"PLAIN-ONE"`, `"PLAIN-TWO"`} {
		res, err := a.LoadState(context.Background(), workflows.LoadStateInput{Name: name, Index: i})
		if err != nil || res.Count != 2 || res.Doc == nil || string(res.Doc.Value) != want {
			t.Fatalf("load %d: %+v %v", i, res, err)
		}
	}
	if err := a.DeleteObject(context.Background(), name); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
		t.Fatal("the state object is still there")
	}
}

// A catalog page stops at its byte limit, and the next page goes on.
func TestCatalogPages(t *testing.T) {
	f, _ := blobstore.NewFolder(t.TempDir())
	c, _ := codec.New([]settings.PayloadKey{{ID: "k", Key: bytes.Repeat([]byte{5}, 32)}})
	a := &Activities{Blobs: f, DC: codec.Converter(c)}
	var items []any
	for _, k := range []string{"a", "b", "c", "d"} {
		items = append(items, workflows.PageItem{Key: k, Entry: workflows.CatalogEntry{Attributes: map[string]any{"big": string(bytes.Repeat([]byte("x"), 400))}}})
	}
	_ = a.writeSealed(context.Background(), "catalog/x/r.bin", items)
	var got []string
	after := ""
	for pages := 0; ; pages++ {
		res, err := a.LoadCatalog(context.Background(), workflows.LoadCatalogInput{Name: "catalog/x/r.bin", After: after, MaxBytes: 900})
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range res.Items {
			got = append(got, it.Key)
			after = it.Key
		}
		if !res.More {
			if pages < 1 {
				t.Fatal("one page holds more than its byte limit")
			}
			break
		}
	}
	if len(got) != 4 {
		t.Fatalf("keys %v", got)
	}
}
