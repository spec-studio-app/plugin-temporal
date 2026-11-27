// Package codec is the payload codec of plugin-temporal. It is byte-compatible
// with the relay's AesGcmCodec (src/codec.ts in spec-studio-app/relay):
//
//   - the plain text is the proto encoding of the whole payload;
//   - the data is a 12-byte IV, the AES-256-GCM cipher text and the 16-byte tag;
//   - the additional data is the key ID, which is in the metadata too;
//   - the metadata has encoding "binary/encrypted" and "encryption-key-id".
//
// The codec refuses a payload that is not encrypted, or that has a key ID it
// does not know, so a person who can write to the namespace but has no key
// cannot make new input. Its errors name the key ID only, never a payload or
// a key.
package codec

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"

	"github.com/spec-studio-app/plugin-temporal/internal/settings"
)

// The metadata names and values, as in the relay.
const (
	Encoding    = "binary/encrypted"
	KeyIDHeader = "encryption-key-id"
	ivBytes     = 12
	tagBytes    = 16
)

// ErrNotEncrypted is the error for a payload with no encryption.
var ErrNotEncrypted = errors.New("the plugin refuses a payload that is not encrypted with a payload key")

// Codec is an AES-256-GCM payload codec.
type Codec struct {
	encryptID string
	aeads     map[string]cipher.AEAD
}

var _ converter.PayloadCodec = (*Codec)(nil)

// New makes a codec: the first key encrypts, and every key decrypts.
func New(keys []settings.PayloadKey) (*Codec, error) {
	if len(keys) == 0 {
		return nil, errors.New("the codec needs at least one key")
	}
	c := &Codec{encryptID: keys[0].ID, aeads: map[string]cipher.AEAD{}}
	for _, k := range keys {
		block, err := aes.NewCipher(k.Key)
		if err != nil {
			return nil, fmt.Errorf("the payload key %s is not an AES-256 key", k.ID)
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("the payload key %s cannot make AES-GCM", k.ID)
		}
		c.aeads[k.ID] = aead
	}
	return c, nil
}

// Data makes the codec of the data keys.
func Data(k settings.Keys) (*Codec, error) { return New(k.Data) }

// Config makes the codec of the config keys.
func Config(k settings.Keys) (*Codec, error) { return New(k.Config) }

// Encode encrypts each payload with the first key.
func (c *Codec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	aead := c.aeads[c.encryptID]
	out := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		plain, err := proto.Marshal(p)
		if err != nil {
			return nil, errors.New("the codec cannot encode a payload")
		}
		iv := make([]byte, ivBytes)
		if _, err := rand.Read(iv); err != nil {
			return nil, errors.New("the codec cannot read random bytes")
		}
		data := aead.Seal(iv, iv, plain, []byte(c.encryptID))
		out[i] = &commonpb.Payload{
			Metadata: map[string][]byte{
				converter.MetadataEncoding: []byte(Encoding),
				KeyIDHeader:                []byte(c.encryptID),
			},
			Data: data,
		}
	}
	return out, nil
}

// Decode decrypts each payload with the key that its metadata names.
func (c *Codec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	out := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		if string(p.GetMetadata()[converter.MetadataEncoding]) != Encoding {
			return nil, ErrNotEncrypted
		}
		id := string(p.GetMetadata()[KeyIDHeader])
		aead, ok := c.aeads[id]
		if !ok {
			return nil, fmt.Errorf("no payload key has the ID %q", id)
		}
		data := p.GetData()
		if len(data) < ivBytes+tagBytes {
			return nil, errors.New("the encrypted payload is too short")
		}
		plain, err := aead.Open(nil, data[:ivBytes], data[ivBytes:], []byte(id))
		if err != nil {
			return nil, fmt.Errorf("the payload does not decrypt with the key %q", id)
		}
		var dec commonpb.Payload
		if err := proto.Unmarshal(plain, &dec); err != nil {
			return nil, errors.New("the decrypted payload is not a Temporal payload")
		}
		out[i] = &dec
	}
	return out, nil
}

// Converter makes a data converter: the default JSON converter, then the codec.
func Converter(c *Codec) converter.DataConverter {
	return converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), c)
}
