# Plan: The Temporal backend plugin

## Approach

`plugin-temporal` is a Go binary built on the plugin-sdk Go SDK (`github.com/spec-studio-app/plugin-sdk/go`, pinned to `main` at 394b06f, because the SDK has no tag yet). The SDK gives the transports, the handshake, the settings check, the uploads and the shutdown. This repo gives one `plugin.Store` that implements `StateStore`, `EventStore`, `BlobStore`, `Watcher`, `StreamContinuer` and `Durable`. The plugin process is a Temporal client and a Temporal worker: it runs the workflows that hold the data, so a host needs no other process.

### Parts

| Package | What it does |
|---|---|
| `main.go` | `plugin.Main` with the info, the settings and the store. Flags `--version`, and the test-only flags `--run-limit` and `--idle` (they go into the workflow start input). |
| `internal/settings` | The settings of R2: declare, parse and check them with the relay's rules and messages (`src/config.ts`). Parses `payload_keys` in the relay's `data:<id>:<base64>,config:<id>:<base64>` format. |
| `internal/codec` | A `converter.PayloadCodec` that is byte-compatible with the relay's `AesGcmCodec` (`src/codec.ts`): AES-256-GCM, a 12-byte IV, the 16-byte tag at the end, the proto-encoded payload as the plain text, the key ID as the additional data, the metadata `encoding: binary/encrypted` and `encryption-key-id`. A data codec and a config codec, each with all keys of its scope for decryption and the first key for encryption. A payload with no encryption or an unknown key ID is refused. Errors name the key ID only. |
| `internal/route` | Maps a collection and key, or a stream, to a target: the workflow ID, the task queue and the key scope (R3). `specs`, `app-specs` and `closed-specs` with key `<repoKey>:<slug>` (split at the first `:`) and the stream `spec:<repoKey>:<slug>` go to `spec-<deploy_id>-<repoKey>-<slug>`. `desks/<repoKey>` and `desk:<repoKey>` go to `desk-<deploy_id>-<repoKey>`. `relay/config` goes to `relay-config-<deploy_id>` on `<task_queue>-config` with the config key. Any other document goes to `doc-<deploy_id>-<collection>-<key>`, and any other stream to `stream-<deploy_id>-<stream>`. Each collection has a catalog workflow `catalog-<deploy_id>-<collection>` in the scope of its documents. |
| `internal/workflows` | Two Go workflow types: `SpecStudioEntity` (the data of one target) and `SpecStudioCatalog` (the list index of one collection). Both run on the data task queue and on the config task queue. |
| `internal/activities` | `ExportRun` (writes one run's history to the blob store and updates the chain manifest), `CatalogPut` (update-with-start on a catalog), `DeleteExports`. |
| `internal/history` | Reads the protocol events of a run: from Temporal with `GetWorkflowHistory`, or from an export. Encodes and decodes cursors. |
| `internal/blobstore` | Two back ends behind one interface: an S3-compatible bucket (`github.com/minio/minio-go/v7`) and a local folder. The content-addressed blobs, the exports and the chain manifests. |
| `internal/store` | The `plugin.Store`: opens the two clients and the two workers, and maps each protocol call to workflow updates, queries and reads. |

### The entity workflow

One `SpecStudioEntity` run holds, in memory: the documents of its target (value as raw JSON, attributes, version, `updatedAt`), the stream's next sequence number and the first sequence number that is still kept, the chain (the earlier runs, each with its run ID, its export key, and its first and last sequence number and time), and a chain nonce.

- **Writes are updates.** `put` and `delete` check the expected version in the update validator, so a conflict is refused before Temporal writes an event (R4.S2). The handler writes, then runs the `CatalogPut` activity, and returns only after the catalog has the change (so a list right after a write shows it, R5.S1). Two updates with the same expected version: the workflow runs one validator at a time, so the second one sees the new version and gets `conflict` (R4.S1). The validator's refusal carries the stored version, and the store maps it to `protocol.ConflictError`.
- **Versions.** A version is `<nonce>.<n>`, where `n` grows by one at each write of the target and goes into the next run's input, and the nonce comes from the first run of a chain (`workflow.SideEffect` with a random value). A new chain after an end reads the manifest and keeps the nonce and `n`. If the manifest is gone, the new nonce still makes every version new (R4.S3).
- **Events are updates.** `append` takes the events of one `events.append`, gives each one the next sequence number and its time (the host's `at`, or the workflow time), and returns them. The update's input and result are in the history, so the history is the event log. A cursor is the base64url of `{nonce, seq}`. A cursor with another nonce, or one that does not decode, gets `refused` (R6.S3).
- **Reads go to the history.** The store queries the run for its chain and sequence ranges (a query adds no event), then reads each needed run: the live run and runs that Temporal still has with `GetWorkflowHistory`, and older runs from their exports. It takes the accepted `append` updates whose completion succeeded, in order, and filters by cursor and by `from <= at < to`. A run whose range is all before the cursor, or out of the time range, is not read.
- **Watch is an update that waits**, as the relay's `read` update does today. Its validator refuses it when the condition is already true, so a wait that ends at once adds no event. The handler waits with `workflow.AwaitWithTimeout` up to the timeout. A run that starts to end stops its waits with `changed: false`; the store then waits again on the next run, until the host's timeout ends.
- **Continue-as-new and ends** (R9). The main loop continues as new when `GetContinueAsNewSuggested` is true or the run has taken `runLimit` updates (default 2000, as the relay). A run with no documents ends after `idle` with no update (default 7 days). Before either, the run (1) sets `closing`, so each validator refuses new updates with a retryable reason that the store retries, (2) waits for `workflow.AllHandlersFinished`, (3) runs `ExportRun` on its own history, which now holds every protocol event of the run, (4) puts the export in the chain, and (5) continues as new with the chain, or ends. `ExportRun` writes the history as protobuf JSON, gzipped, to `exports/<workflow ID>/<run ID>.json.gz`, and writes the chain to `chains/<workflow ID>.json`. The export comes before the run closes, so the namespace's retention can never remove a run before it is exported.
- **A start after an end.** The store starts a target with update-with-start (`client.UpdateWithStartWorkflow`, conflict policy use-existing). Before the start, it reads `chains/<workflow ID>.json` and puts the chain in the start input, so the new run links to the chain (R9.S4).
- **`events.delete`** is an update that sets the first kept sequence number to the next one, removes the stream's exports with `DeleteExports`, and clears the chain's export list. Later appends get new sequence numbers, so no cursor is used again (R9.S5).
- **`test.continueStream`** is an update that makes the run continue as new at once, through the same steps (R9.S1).
- **Determinism.** Each later change of a workflow's shape uses `workflow.GetVersion`. Replay tests (R11) protect it.

### The catalog workflow

`SpecStudioCatalog` keeps, for one collection, each key with its attributes, version, `updatedAt` and the write number `n`. `CatalogPut` applies an entry only when its `n` is higher than the stored one, so a retry or a late activity cannot go back. `state.list` queries the catalog, filters by attribute equality (a missing attribute is `null`), sorts by key in byte order, and pages with a cursor that is the last key. With `withValues`, the store queries the entity workflows of the page, at most 16 at a time. Visibility and search attributes are not used, because visibility is eventually consistent. The catalog continues as new like an entity.

### The handoff from the relay (R10)

The user chose the handoff by continue-as-new. The relay's three TypeScript workflows get a patched step (in the relay-plugin-host spec) that continues each open run as new into `SpecStudioEntity`, with the same workflow ID and task queue, and this input:

```json
{ "handoff": { "version": 1, "workflowId": "spec-prod-1a2b3c4d5e6f-fix-login",
  "documents": [{ "collection": "specs", "key": "1a2b3c4d5e6f:fix-login", "value": {}, "attributes": {} }] } }
```

The relay makes the documents with its own adapter's mapping (protocol §14), so the plugin knows no relay rule. The Go run checks that `workflowId` is its own ID, else it ends at once with no document (R10.S4, as the relay's workflows do). It exports the relay's last run first (`ContinuedExecutionRunID` in the workflow info; that run is closed, so its history is whole), puts that export as the first run of the chain with no events, writes each document with a new version, and runs `CatalogPut` for each. `HANDOFF.md` documents the input for the relay.

The fixtures: `testdata/relay/` has a Node script that checks out the relay at a pinned commit, applies `handoff.patch` (the patched step, which the relay-plugin-host spec takes over), runs the relay's spec, desk and config workflows against a dev server with test keys, makes syncs, a message for the Mac and a message for Claude AI, queries each state, and runs the handoff. It saves each relay run's history, the expected documents and a TypeScript-encrypted payload. The Go tests use the saved files. A CI job runs the whole script against the plugin's worker, end to end.

### Choices made without the user

- **A catalog workflow for lists, not visibility.** The protocol needs a list to show a write that returned. Visibility lags by seconds.
- **Watch as an update that waits, not a query poll.** It is the relay's model today, with the same cost on Temporal Cloud, and a wait that can end at once adds no event.
- **The export before the run closes, from the run itself.** The run refuses new updates first, so the export is whole. A later reader never races the retention.
- **minio-go for the bucket.** It is small, has no CGO, and talks to any S3-compatible service (R2, S3, MinIO).
- **One workflow type for all targets**, with the target in the input. The relay's three types become one, because the plugin knows no rule.
- **Update-with-start** needs Temporal Server 1.27 or later (or Temporal Cloud). The README says so, and the handshake checks the server version and answers `unavailable` with that message on an older server.
- **Prefix in the bucket: `_plugin-temporal/`.** A relay deploy ID cannot start with `_`, so it never touches the relay's attachments (`<deployId>/…`). In `blob_dir`: `blobs/`, `exports/` and `chains/`.
- **GoReleaser** builds the release and pushes the formula to the tap with the `HOMEBREW_TAP_TOKEN` secret, as the app's pipeline updates its cask. The tap's PR adds the README row; the first release writes `Formula/plugin-temporal.rb`.
- **Go 1.24** (the SDK's CI uses 1.24). Temporal Go SDK `go.temporal.io/sdk` at its latest release.

## Repos
| Repo | Why it changes | Merge order |
|---|---|---|
| plugin-temporal | The plugin, its tests, CI, the release and the README | 1 |
| homebrew-tap | The README row for the `plugin-temporal` formula, which the plugin's release writes | 2 |

## Steps
1. plugin-temporal: the Go module `github.com/spec-studio-app/plugin-temporal`, `main.go`, `internal/settings` with its tests, and `plugin.describe` with the settings. Covers R1, R2.
2. plugin-temporal: `internal/codec`, compatible with the relay's codec, with test vectors from the relay (a payload that TypeScript encrypted decrypts in Go). Covers R3, R2.
3. plugin-temporal: `internal/blobstore` (folder and bucket), with tests against a temp folder and, with the `integration` tag, MinIO. Covers R8.
4. plugin-temporal: `internal/route` with its tests. Covers R3.
5. plugin-temporal: `internal/workflows` and `internal/activities`: the entity and catalog workflows (documents, versions, append, waits, continue-as-new, end, export, handoff), with workflow tests in the Go test suite. Covers R4, R6, R7, R9, R10, R11.
6. plugin-temporal: `internal/history` (history reads, cursors, exports) and `internal/store` (the `plugin.Store`, two clients, two workers, retries on a closing run, update-with-start), with integration tests against a dev server. Covers R1, R3, R4, R5, R6, R7, R8, R9.
7. plugin-temporal: replay tests: `testdata/histories/` from the workflow tests and the dev-server tests, `replay_test.go`, and `scripts/capture-histories.sh` to save new ones on purpose. Covers R11.
8. plugin-temporal: the handoff fixtures (`testdata/relay/`, `handoff.patch`, the Node script), `HANDOFF.md`, and the Go tests that read them. Covers R10.
9. plugin-temporal: CI (`.github/workflows/ci.yml`): vet and unit tests; integration tests with `temporal server start-dev` and a MinIO service; the conformance runner built from plugin-sdk at the pinned commit, over stdio (with `blob_dir` and with the bucket) and over HTTP; the end-to-end handoff job. Covers R12, R9, R10.
10. plugin-temporal: release (`.goreleaser.yaml`, `.github/workflows/release.yml`): four static binaries, checksums, the version in `--version`, and the formula push to the tap. Covers R13.
11. plugin-temporal: the README in ASD-STE100: what it does, the settings, history and replay, retention and the export, the handoff, Temporal Cloud and self-hosted, the server version, and the link to plugin-sdk. No license. Covers R14.
12. homebrew-tap: the README row for `plugin-temporal` (`brew install spec-studio-app/tap/plugin-temporal`) and a line that the plugin's release pipeline writes the formula. Covers R13.

## Tests
- R1.S1: `internal/store/describe_test.go` calls `plugin.describe` through the SDK's stdio transport and checks the name, version, repo, capabilities, test hook and settings.
- R1.S2: integration test `internal/store/store_integration_test.go`: handshake against the dev server; and the conformance runner's handshake test in CI.
- R1.S3: integration test with an address where nothing listens: `unavailable`, and the message names `address`.
- R1.S4: the CI job that runs the conformance runner over HTTP with `--listen 127.0.0.1:7070 --token`.
- R2.S1: `internal/settings/settings_test.go`, no `payload_keys`: `bad_settings` that names it.
- R2.S2: `settings_test.go`, `api_key` and `tls_cert` both set.
- R2.S3: `settings_test.go`, part of the bucket settings.
- R2.S4: `settings_test.go`, no bucket and no `blob_dir`.
- R2.S5: the conformance runner's secret test, and an integration test that runs a session with canary values for `api_key`, `payload_keys` and `bucket_secret_key` and searches stderr, errors and results for them and for a known plain payload.
- R3.S1: integration test with two `Store` instances (two clients and two workers) on one namespace: each appends to `spec:1a2b3c4d5e6f:fix-login`; both read both events, and `DescribeWorkflowExecution` finds `spec-default-1a2b3c4d5e6f-fix-login`.
- R3.S2: integration test with `deploy_id=prod`: describe both workflows and check their IDs and task queues. `internal/route/route_test.go` checks the same mapping offline.
- R3.S3: integration test: put and get `notes/a1`.
- R3.S4: integration test reads the raw histories of the spec and config workflows and checks the encoding and key ID of every payload; the end-to-end handoff job decrypts a Go-made payload with the relay's `src/codec.ts`.
- R4.S1: integration test with two `Store` instances that put with the same expected version at the same time, 20 times: exactly one success each time. The conformance runner's race test too.
- R4.S2: workflow test: a put with a stale version gets `conflict` with the stored version, and the history has no new update event.
- R4.S3: integration test: write, delete, let the workflow end (short `--idle`), write again; the new version differs from every earlier one. A workflow test with a removed manifest too.
- R5.S1: integration test: three documents, then a fourth with `pendingForMac` true and an immediate list.
- R5.S2: integration test with 250 documents and pages of 100; and the conformance runner's list tests.
- R6.S1: the conformance runner's order test, and a workflow test of `append`.
- R6.S2: integration test: append with `at` 10:00 and read the range 09:59 to 10:01.
- R6.S3: `internal/history/cursor_test.go` and the conformance runner.
- R7.S1: integration test: one `Store` waits on a document, another one writes it; the wait ends with the new version in less than 5 seconds.
- R7.S2: the conformance runner's watch timeout test, and a workflow test of the wait timeout.
- R8.S1: `internal/blobstore/folder_test.go` with a 3 MiB blob; the conformance runner's large blob test with `blob_dir`.
- R8.S2: `internal/blobstore/bucket_integration_test.go` against MinIO: the key is under `_plugin-temporal/blobs/`; the conformance runner with the bucket settings.
- R8.S3: the conformance runner's wrong-hash test.
- R9.S1: the conformance runner's continued-run test, and an integration test that reads after the cursor of B.
- R9.S2: integration test: A and B, `test.continueStream`, C; then `DeleteWorkflowExecution` on the first run (as retention does); reads from the start and after A return the right events from the export and the live run.
- R9.S3: integration test with `--run-limit 5`: append 12 events, check that the workflow has three or more runs, and read every event in order.
- R9.S4: integration test with `--idle 2s`: delete the documents, wait for the end, check the export, read the stream, then write a document and check that the new run's chain names the old runs.
- R9.S5: integration test: events in two runs, one exported; `events.delete`; the read is empty and the export key is gone from the blob store.
- R10.S1: `internal/store/handoff_test.go`: start `SpecStudioEntity` with the handoff input from the fixture's continue-as-new event (decrypted with the test keys), and compare `specs/<repoKey>:<slug>` with the expected document from the relay's query. The CI end-to-end job does the same with the real relay run.
- R10.S2: the same test for the desk and the config fixtures.
- R10.S3: end-to-end job: after the handoff, delete the relay's run and check that its export is in the blob store and is the first run of the chain.
- R10.S4: workflow test: an input whose `workflowId` is another ID ends the run with no document and no catalog entry.
- R11.S1: `internal/workflows/replay_test.go` replays every file in `testdata/histories/` with `worker.NewWorkflowReplayer`; a check of the test makes a non-deterministic copy of the workflow and expects the replay to fail with the file name.
- R12.S1: the CI jobs `conformance-stdio` and `conformance-http`.
- R13.S1: the release workflow on a tag; checked by `goreleaser release --snapshot --clean` in CI, and by the first real tag after merge (a person pushes it).
- R13.S2: a manual check after the first release: `brew install spec-studio-app/tap/plugin-temporal` and `plugin-temporal --version`.
- R14.S1: a review of the README against the steps that the CI jobs run (the same settings and commands).

## Risks
- **Update-with-start and older self-hosted servers.** A server before 1.27 cannot do it. The handshake checks the server version and says what to do.
- **Two workers on one task queue during the handoff.** If the relay's old TypeScript worker and the Go worker poll the same queue at once, a task can reach a worker without its workflow type, fail, and retry. The relay-plugin-host spec runs the handoff with the old worker first and starts the plugin after; `HANDOFF.md` says so.
- **The export of a large history.** A run can have up to about 50 MB of history. The export streams it in pages to a gzip writer and the blob store, and never holds it whole in memory.
- **Latency of a write.** A put is an update, an activity and a catalog update: about 3 round trips to Temporal. The relay's rules need one write per call, so this is acceptable; the integration tests log the time.
- **History size from waits.** Each wait that does not end at once adds events. The run limit and Temporal's suggestion continue the run as new, as the relay's workflows do today.
- **The SDK has no tag.** The module pins a pseudo-version of `main`. When plugin-sdk tags `go/v1.0.0`, a later change moves to it.
- **A secret in a Temporal error.** The SDK redacts secret values in its log and errors; the store also wraps every Temporal and S3 error in a message of its own, with no payload.
