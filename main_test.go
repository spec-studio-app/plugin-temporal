package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/spec-studio-app/plugin-sdk/go/client"
	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

// buildPlugin builds the binary once for the tests of this package.
func buildPlugin(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "plugin-temporal")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("build: %v", err)
	}
	return bin
}

// R1.S1: describe before the handshake.
func TestDescribe(t *testing.T) {
	bin := buildPlugin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p, err := client.Start(ctx, []string{bin}, os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Kill()
	d, err := p.Describe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if d.Plugin.Name != "plugin-temporal" || d.Plugin.Repo != "https://github.com/spec-studio-app/plugin-temporal" || d.Plugin.Version == "" {
		t.Fatalf("info: %+v", d.Plugin)
	}
	if len(d.ProtocolVersions) != 1 || d.ProtocolVersions[0] != "1.0" || !d.Durable {
		t.Fatalf("versions %v durable %v", d.ProtocolVersions, d.Durable)
	}
	caps := map[protocol.Capability]bool{}
	for _, c := range d.Capabilities {
		caps[c] = true
	}
	for _, c := range []protocol.Capability{protocol.CapState, protocol.CapEvents, protocol.CapBlobs, protocol.CapWatch} {
		if !caps[c] {
			t.Fatalf("no capability %s", c)
		}
	}
	if len(d.TestHooks) != 1 || d.TestHooks[0] != protocol.HookContinueStream {
		t.Fatalf("test hooks %v", d.TestHooks)
	}
	required := map[string]bool{}
	secret := map[string]bool{}
	for _, s := range d.Settings {
		if s.Description == "" {
			t.Fatalf("setting %s has no description", s.Name)
		}
		required[s.Name], secret[s.Name] = s.Required, s.Secret
	}
	if !required["address"] || !required["payload_keys"] || !secret["payload_keys"] || !secret["api_key"] || !secret["bucket_secret_key"] || len(d.Settings) != 15 {
		t.Fatalf("settings: %+v", d.Settings)
	}
}

func TestVersionFlag(t *testing.T) {
	out, err := exec.Command(buildPlugin(t), "--version").Output()
	if err != nil || !bytes.Equal(bytes.TrimSpace(out), []byte(version)) {
		t.Fatalf("--version: %q %v", out, err)
	}
}
