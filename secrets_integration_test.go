//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spec-studio-app/plugin-sdk/go/client"
	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func key() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return base64.StdEncoding.EncodeToString(b)
}

// R2.S5: no secret value and no plain payload in stderr, an error or a result.
func TestSecretsStayOut(t *testing.T) {
	addr := os.Getenv("TEST_TEMPORAL_ADDRESS")
	if addr == "" {
		t.Skip("TEST_TEMPORAL_ADDRESS is not set")
	}
	bin := buildPlugin(t)
	dataKey, configKey := key(), key()
	apiKey, bucketSecret := "API-CANARY-"+key(), "BUCKET-CANARY-"+key()
	plain := "PLAIN-CANARY-" + key()
	canaries := []string{dataKey, configKey, apiKey, bucketSecret, plain}
	var stderr lockedBuffer
	var outputs []string
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	run := func(settings map[string]string, work func(*client.Process)) {
		p, err := client.Start(ctx, []string{bin}, &stderr)
		if err != nil {
			t.Fatal(err)
		}
		defer p.Kill()
		res, err := p.Handshake(ctx, protocol.HandshakeParams{Host: protocol.Host{Kind: "test", Version: "1"}, ProtocolVersions: []string{"1.0"}, Settings: settings})
		b, _ := json.Marshal(res)
		outputs = append(outputs, string(b))
		if err != nil {
			outputs = append(outputs, err.Error())
			return
		}
		work(p)
		_ = p.Shutdown(ctx, 5*time.Second)
	}
	keys := "data:canary-d:" + dataKey + ",config:canary-c:" + configKey

	// A session with real work.
	run(map[string]string{"address": addr, "payload_keys": keys, "blob_dir": t.TempDir(), "deploy_id": fmt.Sprintf("s%x", time.Now().UnixNano())}, func(p *client.Process) {
		val, _ := json.Marshal(map[string]string{"text": plain})
		res, err := p.Put(ctx, protocol.StatePutParams{Collection: "specs", Key: "1a2b3c4d5e6f:s", Value: val})
		if err != nil {
			t.Fatal(err)
		}
		exp := "wrong"
		_, err = p.Put(ctx, protocol.StatePutParams{Collection: "specs", Key: "1a2b3c4d5e6f:s", Value: val, ExpectedVersion: &exp})
		outputs = append(outputs, err.Error(), res.Version)
		if _, err := p.Append(ctx, "spec:1a2b3c4d5e6f:s", protocol.NewEvent{Type: "t", Data: val}); err != nil {
			t.Fatal(err)
		}
	})
	// Failed handshakes with each secret set.
	run(map[string]string{"address": addr, "payload_keys": keys, "api_key": apiKey, "blob_dir": t.TempDir()}, nil)
	run(map[string]string{"address": addr, "payload_keys": keys, "bucket_endpoint": "http://127.0.0.1:1", "bucket_name": "b", "bucket_access_key": "a", "bucket_secret_key": bucketSecret}, nil)
	run(map[string]string{"address": addr, "payload_keys": "data:x:" + dataKey}, nil)

	all := stderr.String() + "\n" + strings.Join(outputs, "\n")
	for _, c := range canaries {
		if strings.Contains(all, c) {
			t.Fatalf("a secret or a plain payload is in the output: %s...", c[:12])
		}
	}
	if !strings.Contains(all, "conflict") && !strings.Contains(all, "expected version") {
		t.Fatalf("the work did not run: %s", all)
	}
}
