package codec

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"testing"

	commonpb "go.temporal.io/api/common/v1"

	"github.com/spec-studio-app/plugin-temporal/internal/settings"
)

func key(id string, b byte) settings.PayloadKey {
	return settings.PayloadKey{ID: id, Key: bytes.Repeat([]byte{b}, 32)}
}

func plain(text string) *commonpb.Payload {
	return &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte(text)}
}

// A payload that the relay's TypeScript codec encrypted decrypts here (R3.S4).
func TestRelayVector(t *testing.T) {
	raw, err := os.ReadFile("testdata/relay-vector.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		KeyID, KeyBase64, DataBase64, PlainEncoding, PlainData string
		Metadata                                               map[string]string
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	k, _ := base64.StdEncoding.DecodeString(v.KeyBase64)
	data, _ := base64.StdEncoding.DecodeString(v.DataBase64)
	c, err := New([]settings.PayloadKey{{ID: v.KeyID, Key: k}})
	if err != nil {
		t.Fatal(err)
	}
	md := map[string][]byte{}
	for name, val := range v.Metadata {
		md[name] = []byte(val)
	}
	out, err := c.Decode([]*commonpb.Payload{{Metadata: md, Data: data}})
	if err != nil {
		t.Fatal(err)
	}
	if string(out[0].Metadata["encoding"]) != v.PlainEncoding || string(out[0].Data) != v.PlainData {
		t.Fatalf("decoded %q %q", out[0].Metadata["encoding"], out[0].Data)
	}
}

func TestRoundTripAndOldKey(t *testing.T) {
	old, _ := New([]settings.PayloadKey{key("old", 1)})
	cur, _ := New([]settings.PayloadKey{key("new", 2), key("old", 1)})
	enc, err := old.Encode([]*commonpb.Payload{plain(`"a"`)})
	if err != nil {
		t.Fatal(err)
	}
	if string(enc[0].Metadata["encoding"]) != Encoding || string(enc[0].Metadata[KeyIDHeader]) != "old" {
		t.Fatalf("metadata %v", enc[0].Metadata)
	}
	if bytes.Contains(enc[0].Data, []byte(`"a"`)) {
		t.Fatal("the data is not encrypted")
	}
	dec, err := cur.Decode(enc)
	if err != nil || string(dec[0].Data) != `"a"` {
		t.Fatalf("decode with an old key: %v", err)
	}
	enc2, _ := cur.Encode([]*commonpb.Payload{plain(`"b"`)})
	if string(enc2[0].Metadata[KeyIDHeader]) != "new" {
		t.Fatal("the first key does not encrypt")
	}
}

func TestRefusals(t *testing.T) {
	c, _ := New([]settings.PayloadKey{key("k1", 3)})
	if _, err := c.Decode([]*commonpb.Payload{plain(`"secret text"`)}); err != ErrNotEncrypted {
		t.Fatalf("plain payload: %v", err)
	}
	other, _ := New([]settings.PayloadKey{key("k2", 4)})
	enc, _ := other.Encode([]*commonpb.Payload{plain(`"secret text"`)})
	_, err := c.Decode(enc)
	if err == nil || !strings.Contains(err.Error(), "k2") || strings.Contains(err.Error(), "secret text") {
		t.Fatalf("unknown key: %v", err)
	}
	// The key ID is the additional data: a changed ID does not decrypt.
	c2, _ := New([]settings.PayloadKey{key("k2", 4), {ID: "k3", Key: bytes.Repeat([]byte{4}, 32)}})
	enc[0].Metadata[KeyIDHeader] = []byte("k3")
	if _, err := c2.Decode(enc); err == nil {
		t.Fatal("a payload with a changed key ID decrypts")
	}
}

// TestWriteGoVector writes testdata/go-vector.json, a payload that this
// codec encrypted. The relay's codec decrypts it in the handoff fixture
// script (R3.S4). It runs only with WRITE_GO_VECTOR=1.
func TestWriteGoVector(t *testing.T) {
	if os.Getenv("WRITE_GO_VECTOR") != "1" {
		t.Skip("WRITE_GO_VECTOR is not 1")
	}
	k := key("go-test", 9)
	c, _ := New([]settings.PayloadKey{k})
	text := `{"hello":"plugin-temporal","n":2}`
	enc, err := c.Encode([]*commonpb.Payload{plain(text)})
	if err != nil {
		t.Fatal(err)
	}
	out, _ := json.MarshalIndent(map[string]any{
		"keyId":         k.ID,
		"keyBase64":     base64.StdEncoding.EncodeToString(k.Key),
		"metadata":      map[string]string{"encoding": string(enc[0].Metadata["encoding"]), KeyIDHeader: string(enc[0].Metadata[KeyIDHeader])},
		"dataBase64":    base64.StdEncoding.EncodeToString(enc[0].Data),
		"plainEncoding": "json/plain",
		"plainData":     text,
	}, "", "  ")
	if err := os.WriteFile("testdata/go-vector.json", append(out, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
