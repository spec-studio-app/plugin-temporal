// Package store is the plugin.Store of plugin-temporal. The plugin process is
// a Temporal client and a Temporal worker: it runs the workflows that hold
// the data, on the data task queue and on the config task queue.
//
// The plugin of the studio CLI (host kind cli) lives for one command. It
// starts a worker on a task queue only when no other worker polls it (the
// app or the relay is closed), or when a call gets no answer in time. So it
// does not take the work of the app's or the relay's workflows.
package store

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	tlog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"

	"github.com/spec-studio-app/plugin-sdk/go/plugin"
	"github.com/spec-studio-app/plugin-sdk/go/protocol"

	"github.com/spec-studio-app/plugin-temporal/internal/blobstore"
	"github.com/spec-studio-app/plugin-temporal/internal/codec"
	"github.com/spec-studio-app/plugin-temporal/internal/route"
	"github.com/spec-studio-app/plugin-temporal/internal/settings"
)

// Options are the test-only knobs of the main flags. They go into the start
// input of each new workflow.
type Options struct {
	RunLimit        int
	Idle            time.Duration
	CatalogRunLimit int
	// InlineBytes is the largest state in a continue-as-new input (tests only).
	InlineBytes int
}

// Store is the plugin's storage.
type Store struct {
	optsFn   func() Options
	opts     Options
	set      *settings.Settings
	router   route.Router
	data     client.Client
	config   client.Client
	dataDC   converter.DataConverter
	configDC converter.DataConverter
	blobs    blobstore.Store
	// namespaceID is the ID of the namespace (from DescribeNamespace), for
	// the storeId.
	namespaceID string
	log         *slog.Logger

	// lazy is true for the host kind cli: the workers start only when
	// needed (startWorker).
	lazy bool
	// wmu guards workers, started and closed.
	wmu sync.Mutex
	// workers are the running workers, by task queue.
	workers map[string]worker.Worker
	// started is the time that each worker started.
	started map[string]time.Time
	closed  bool
}

// New makes a store. Open connects it, and reads the options then (after the
// flags are parsed).
func New(opts func() Options) *Store { return &Store{optsFn: opts} }

var (
	_ plugin.Store           = (*Store)(nil)
	_ plugin.EventStore      = (*Store)(nil)
	_ plugin.BlobStore       = (*Store)(nil)
	_ plugin.Watcher         = (*Store)(nil)
	_ plugin.StreamContinuer = (*Store)(nil)
	_ plugin.Durable         = (*Store)(nil)
	_ plugin.Committer       = (*Store)(nil)
	_ plugin.StoreIdentity   = (*Store)(nil)
)

// Durable is true: Temporal and the blob store keep the data.
func (s *Store) Durable() bool { return true }

// minServer is the first Temporal Server version with update-with-start.
const minServer = "1.27.0"

// unreachable is the message when Temporal does not answer. It names the
// settings, never their values.
const unreachable = "The plugin cannot reach Temporal. Check the setting address and the auth settings (auth, api_key, tls_cert, tls_key)."

// warnHandler drops the Temporal SDK's log lines below Warn.
type warnHandler struct{ slog.Handler }

func (h warnHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= slog.LevelWarn && h.Handler.Enabled(ctx, l)
}

// Open connects to Temporal and the blob store, and starts the workers.
func (s *Store) Open(ctx context.Context, o plugin.OpenOptions) error {
	if s.optsFn != nil {
		s.opts = s.optsFn()
	}
	set, err := settings.Parse(o.Settings)
	if err != nil {
		return err
	}
	s.set = set
	s.log = o.Log
	s.router = route.Router{DeployID: set.DeployID, TaskQueue: set.TaskQueue, ConfigTaskQueue: set.ConfigTaskQueue}

	dataCodec, err := codec.Data(set.Keys)
	if err != nil {
		return protocol.NewError(protocol.BadSettings, "The setting %s has a key that is not valid.", settings.PayloadKeys)
	}
	configCodec, err := codec.Config(set.Keys)
	if err != nil {
		return protocol.NewError(protocol.BadSettings, "The setting %s has a key that is not valid.", settings.PayloadKeys)
	}
	s.dataDC, s.configDC = codec.Converter(dataCodec), codec.Converter(configCodec)

	opts := client.Options{
		HostPort:      set.Address,
		Namespace:     set.Namespace,
		DataConverter: s.dataDC,
		Logger:        tlog.NewStructuredLogger(slog.New(warnHandler{o.Log.Handler()})),
	}
	switch set.Auth {
	case settings.AuthTLS:
		opts.ConnectionOptions.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	case settings.AuthAPIKey:
		opts.ConnectionOptions.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
		opts.Credentials = client.NewAPIKeyStaticCredentials(set.APIKey)
	case settings.AuthMTLS:
		pair, err := tls.X509KeyPair(set.TLSCert, set.TLSKey)
		if err != nil {
			return protocol.NewError(protocol.BadSettings, "The settings %s and %s are not a valid certificate and key.", settings.TLSCert, settings.TLSKey)
		}
		opts.ConnectionOptions.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}
	}

	dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	s.data, err = client.DialContext(dialCtx, opts)
	if err != nil {
		return protocol.NewError(protocol.Unavailable, unreachable)
	}
	if err := s.checkServer(dialCtx); err != nil {
		s.data.Close()
		return err
	}
	configOpts := opts
	configOpts.DataConverter = s.configDC
	s.config, err = client.NewClientFromExisting(s.data, configOpts)
	if err != nil {
		s.data.Close()
		return protocol.NewError(protocol.Unavailable, unreachable)
	}

	if err := s.openBlobs(dialCtx); err != nil {
		s.closeClients()
		return err
	}

	s.lazy = o.Host.Kind == protocol.HostCLI
	s.wmu.Lock()
	s.workers = map[string]worker.Worker{}
	s.started = map[string]time.Time{}
	s.closed = false
	s.wmu.Unlock()
	if err := s.openWorkers(dialCtx); err != nil {
		s.stopWorkers()
		s.closeClients()
		return err
	}
	o.Log.Info("open", "settings", set.String(), "blobs", s.blobs.Where(), "workers", s.workerQueues())
	return nil
}

// checkServer checks the namespace and the server version.
func (s *Store) checkServer(ctx context.Context) error {
	svc := s.data.WorkflowService()
	ns, err := svc.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: s.set.Namespace})
	if err != nil {
		var nf *serviceerror.NamespaceNotFound
		var nf2 *serviceerror.NotFound
		if errors.As(err, &nf) || errors.As(err, &nf2) {
			return protocol.NewError(protocol.BadSettings, "Temporal has no namespace with the name in the setting %s.", settings.Namespace)
		}
		var denied *serviceerror.PermissionDenied
		if errors.As(err, &denied) {
			return protocol.NewError(protocol.BadSettings, "Temporal refused the credentials. Check the settings api_key, tls_cert and tls_key, and the setting %s.", settings.Namespace)
		}
		return protocol.NewError(protocol.Unavailable, unreachable)
	}
	s.namespaceID = ns.GetNamespaceInfo().GetId()
	info, err := svc.GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
	if err == nil && info.GetServerVersion() != "" && versionLess(info.GetServerVersion(), minServer) {
		return protocol.NewError(protocol.Unavailable, "The Temporal server is version %s. The plugin needs %s or later (update-with-start). Update the server, or use Temporal Cloud.", info.GetServerVersion(), minServer)
	}
	return nil
}

// versionLess compares two versions "a.b.c". A version that does not parse
// is not less.
func versionLess(a, b string) bool {
	pa, okA := parseVersion(a)
	pb, okB := parseVersion(b)
	if !okA || !okB {
		return false
	}
	for i := range 3 {
		if pa[i] != pb[i] {
			return pa[i] < pb[i]
		}
	}
	return false
}

func parseVersion(v string) ([3]int, bool) {
	var out [3]int
	v = strings.TrimPrefix(v, "v")
	if i := strings.IndexAny(v, "-+ "); i >= 0 {
		v = v[:i]
	}
	parts := strings.Split(v, ".")
	if len(parts) < 2 {
		return out, false
	}
	for i := 0; i < 3 && i < len(parts); i++ {
		n, err := strconv.Atoi(parts[i])
		if err != nil {
			return out, false
		}
		out[i] = n
	}
	return out, true
}

func (s *Store) openBlobs(ctx context.Context) error {
	if b := s.set.Bucket; b != nil {
		bucket, err := blobstore.NewBucket(b)
		if err != nil {
			return protocol.NewError(protocol.BadSettings, "The setting %s is not valid.", settings.BucketEndpoint)
		}
		if err := bucket.Check(ctx); err != nil {
			return protocol.NewError(protocol.Unavailable, "The plugin cannot write to the bucket. Check the setting %s and the other bucket settings. (%v)", settings.BucketEndpoint, err)
		}
		s.blobs = bucket
		return nil
	}
	f, err := blobstore.NewFolder(s.set.BlobDir)
	if err == nil {
		err = f.Check(ctx)
	}
	if err != nil {
		return protocol.NewError(protocol.BadSettings, "The plugin cannot write to the folder in the setting %s.", settings.BlobDir)
	}
	s.blobs = f
	return nil
}

// stopWorkers stops the workers, all at once, and returns when they
// stopped. Stop is graceful: it tells Temporal that the worker stops, so
// Temporal gives its sticky workflows to another worker at once, and it
// waits for the activities in progress to end and report (WorkerStopTimeout).
// No worker starts after it.
func (s *Store) stopWorkers() {
	s.wmu.Lock()
	ws := s.workers
	s.workers = nil
	s.closed = true
	s.wmu.Unlock()
	var wg sync.WaitGroup
	for _, w := range ws {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.Stop()
		}()
	}
	wg.Wait()
}

func (s *Store) closeClients() {
	if s.config != nil {
		s.config.Close()
	}
	if s.data != nil {
		s.data.Close()
	}
}

// Close stops the workers and closes the clients.
func (s *Store) Close(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.stopWorkers()
		s.closeClients()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("the workers did not stop in time")
	}
}
