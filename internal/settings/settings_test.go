package settings

import (
	"strings"
	"testing"

	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

const testKeys = "data:d1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=,config:c1:AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="

func base() map[string]string {
	return map[string]string{Address: "localhost:7233", PayloadKeys: testKeys, BlobDir: "/tmp/blobs"}
}

func with(m map[string]string, kv ...string) map[string]string {
	for i := 0; i < len(kv); i += 2 {
		if kv[i+1] == "" {
			delete(m, kv[i])
		} else {
			m[kv[i]] = kv[i+1]
		}
	}
	return m
}

func wantBad(t *testing.T, m map[string]string, names ...string) string {
	t.Helper()
	_, err := Parse(m)
	if err == nil {
		t.Fatalf("Parse: no error")
	}
	if !protocol.Is(err, protocol.BadSettings) {
		t.Fatalf("code %s, want bad_settings: %v", protocol.CodeOf(err), err)
	}
	for _, n := range names {
		if !strings.Contains(err.Error(), n) {
			t.Fatalf("message %q does not name %s", err.Error(), n)
		}
	}
	return err.Error()
}

func TestDefaults(t *testing.T) {
	s, err := Parse(base())
	if err != nil {
		t.Fatal(err)
	}
	if s.Namespace != "default" || s.DeployID != "default" || s.TaskQueue != "relay-default" || s.ConfigTaskQueue != "relay-default-config" || s.Auth != AuthPlain {
		t.Fatalf("defaults: %+v", s)
	}
	if len(s.Keys.Data) != 1 || s.Keys.Data[0].ID != "d1" || len(s.Keys.Config) != 1 || s.Keys.Config[0].Key[0] != 1 {
		t.Fatalf("keys: %+v", s.Keys)
	}
}

func TestDeployIDGivesTaskQueue(t *testing.T) {
	s, err := Parse(with(base(), DeployID, "prod"))
	if err != nil {
		t.Fatal(err)
	}
	if s.TaskQueue != "relay-prod" || s.ConfigTaskQueue != "relay-prod-config" {
		t.Fatalf("task queues: %s %s", s.TaskQueue, s.ConfigTaskQueue)
	}
	wantBad(t, with(base(), DeployID, "Prod_1"), DeployID)
}

// R2.S1
func TestMissingPayloadKeys(t *testing.T) {
	wantBad(t, with(base(), PayloadKeys, ""), PayloadKeys)
}

func TestBadPayloadKeys(t *testing.T) {
	for _, v := range []string{
		"data:d1:AAAA",
		"data:d1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
		"config:c1:AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=",
		"data:d 1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=,config:c1:AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=",
		"data:x:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=,config:x:AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=",
		"other:x:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
	} {
		msg := wantBad(t, with(base(), PayloadKeys, v), PayloadKeys)
		if strings.Contains(msg, "AAAA") || strings.Contains(msg, "AQEB") {
			t.Fatalf("the message shows key material: %s", msg)
		}
	}
}

// R2.S2
func TestAPIKeyAndCert(t *testing.T) {
	msg := wantBad(t, with(base(), APIKey, "k", TLSCert, "-----BEGIN CERTIFICATE-----x", TLSKey, "-----BEGIN KEY-----y"), APIKey, TLSCert)
	if !strings.Contains(msg, "not both") {
		t.Fatalf("message: %s", msg)
	}
}

func TestAuth(t *testing.T) {
	s, err := Parse(with(base(), APIKey, "k"))
	if err != nil || s.Auth != AuthAPIKey {
		t.Fatalf("api key: %v %+v", err, s)
	}
	s, err = Parse(with(base(), TLSCert, "-----BEGIN CERTIFICATE-----x", TLSKey, "-----BEGIN KEY-----y"))
	if err != nil || s.Auth != AuthMTLS {
		t.Fatalf("mtls: %v %+v", err, s)
	}
	s, err = Parse(with(base(), AuthName, "tls"))
	if err != nil || s.Auth != AuthTLS {
		t.Fatalf("tls: %v", err)
	}
	wantBad(t, with(base(), AuthName, "api-key"), APIKey)
	wantBad(t, with(base(), AuthName, "mtls"), TLSCert)
	wantBad(t, with(base(), AuthName, "nope"), AuthName)
	wantBad(t, with(base(), AuthName, "plain", APIKey, "k"), AuthName)
	wantBad(t, with(base(), TLSCert, "-----BEGIN CERTIFICATE-----x"), TLSKey)
	wantBad(t, with(base(), TLSCert, "/no/such/file"), TLSCert)
}

// R2.S3
func TestPartBucket(t *testing.T) {
	wantBad(t, with(base(), BucketEndpoint, "https://s3.example.com", BucketName, "b"), BucketAccessKey, BucketSecretKey)
}

func TestBucket(t *testing.T) {
	s, err := Parse(with(base(), BlobDir, "", BucketEndpoint, "https://s3.example.com", BucketName, "b", BucketAccessKey, "a", BucketSecretKey, "s"))
	if err != nil {
		t.Fatal(err)
	}
	if s.Bucket == nil || s.Bucket.Region != "auto" || s.Bucket.Name != "b" {
		t.Fatalf("bucket: %+v", s.Bucket)
	}
	wantBad(t, with(base(), BucketEndpoint, "not a url", BucketName, "b", BucketAccessKey, "a", BucketSecretKey, "s"), BucketEndpoint)
}

// R2.S4
func TestNoBlobPlace(t *testing.T) {
	wantBad(t, with(base(), BlobDir, ""), BlobDir, BucketEndpoint)
}

func TestDeclared(t *testing.T) {
	secret := map[string]bool{}
	for _, d := range Declared() {
		if !protocol.ValidSettingName(d.Name) {
			t.Fatalf("setting name %q", d.Name)
		}
		if d.Secret {
			secret[d.Name] = true
		}
	}
	for _, n := range []string{APIKey, TLSKey, PayloadKeys, BucketAccessKey, BucketSecretKey} {
		if !secret[n] {
			t.Fatalf("%s is not secret", n)
		}
	}
}
