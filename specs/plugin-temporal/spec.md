# The Temporal backend plugin

Spec Studio's relay has a Temporal store that is built into the relay, and the app has no Temporal store. This spec makes `plugin-temporal`: a Go backend plugin, by the protocol in spec-studio-app/plugin-sdk, that keeps each spec in one Temporal workflow for both hosts. Relay messages and app events then share one history, and the history stays complete across continued and closed runs, so the app can replay a spec from its start.

## Scope

### In scope
- A Go plugin binary, `plugin-temporal`, built on the plugin-sdk Go SDK, over stdio and over HTTP.
- The capabilities `state`, `events`, `watch` and `blobs`, and the test hook `continueStream`.
- One workflow per spec, one per desk and one for the relay config, with the relay's workflow IDs, task queues, payload encryption and codec.
- The workflow history as the event log, read by opaque cursor or by time range.
- Continue-as-new, the export of each ended run's history to blobs, and reads across the chain of runs, also after Temporal's retention removes a run.
- Blobs in an S3-compatible bucket, or in a local folder when no bucket is set.
- The handoff input: the input with which a running relay workflow continues as new into the plugin's workflow, and the plugin's side of that move. Fixture histories made by the relay's current Temporal store, and the tests that read them.
- Unit tests, workflow tests and replay tests with the Temporal Go SDK test suite.
- CI: the tests, and the plugin-sdk conformance runner against `temporal server start-dev`, over stdio and over HTTP.
- A release on a tag: static binaries for macOS and Linux (arm64, amd64) on a GitHub release.
- A Homebrew formula `plugin-temporal` in spec-studio-app/homebrew-tap.
- The README, in ASD-STE100 Simplified Technical English, with a link back to plugin-sdk.

### Out of scope
- The relay's change that runs the handoff (the patched step in the relay's TypeScript workflows, and the switch to the plugin). The relay-plugin-host spec in spec-studio-app/relay does it with the handoff input that this spec defines.
- The app's change to use the plugin, and the scrubber UI.
- A translation of the relay's old update events into protocol events. A run from before the handoff has no protocol events; its history stays in an export.
- A change to the protocol or to the plugin-sdk.
- A license file.

## Requirements

### R1 The plugin and its handshake
The binary is a plugin by the protocol version 1.0. It gives the capabilities `state`, `events`, `watch` and `blobs`, the test hook `continueStream`, and `durable: true`. It names itself `plugin-temporal`, with its version and the URL https://github.com/spec-studio-app/plugin-temporal.

#### R1.S1 Describe before the handshake
- Given the plugin started over stdio
- When the host calls `plugin.describe`
- Then the result has the name `plugin-temporal`, its version, its repo URL, the protocol version 1.0, the four capabilities and the test hook `continueStream`
- And the result lists each setting of R2 with its description, and says which settings are required and which are secret

#### R1.S2 A handshake with a reachable Temporal
- Given a Temporal dev server and valid settings
- When a host of kind `relay` sends the handshake
- Then the plugin answers with the version 1.0, the four capabilities and `durable: true`

#### R1.S3 Temporal does not answer
- Given settings with an address where no Temporal server listens
- When the host sends the handshake
- Then the plugin answers with the error code `unavailable`
- And the message names the `address` setting and says to check it and the auth settings

#### R1.S4 The same plugin over HTTP
- Given the plugin started with `--listen 127.0.0.1:7070` and a token
- When the host sends the handshake and then `state.get` over HTTP with the token and the session
- Then both calls succeed as they do over stdio

### R2 The settings
The plugin takes these settings, with the same meaning and defaults as the relay's Temporal settings: `address` (required), `namespace` (default `default`), `auth` (`plain`, `tls`, `mtls` or `api-key`), `api_key` (secret), `tls_cert`, `tls_key` (secret), `payload_keys` (required, secret, the relay's `PAYLOAD_KEYS` format), `deploy_id` (default `default`), `task_queue` (default `relay-<deploy_id>`), the bucket settings `bucket_endpoint`, `bucket_name`, `bucket_access_key` (secret), `bucket_secret_key` (secret) and `bucket_region` (default `auto`), and `blob_dir`. The plugin checks them as the relay does, and never writes a secret value or a payload to its log, to an error or to a result.

#### R2.S1 A missing required setting
- Given settings with no `payload_keys`
- When the host sends the handshake
- Then the plugin answers with the error code `bad_settings`
- And the message names `payload_keys`

#### R2.S2 A wrong combination of auth settings
- Given settings with both `api_key` and `tls_cert`
- When the host sends the handshake
- Then the plugin answers with `bad_settings`, and the message says to set one of them, not both

#### R2.S3 Only some bucket settings
- Given settings with `bucket_endpoint` and `bucket_name` but no `bucket_access_key`
- When the host sends the handshake
- Then the plugin answers with `bad_settings`, and the message names `bucket_access_key`

#### R2.S4 No place for blobs
- Given settings with no bucket settings and no `blob_dir`
- When the host sends the handshake
- Then the plugin answers with `bad_settings`, and the message says to set the bucket settings or `blob_dir`

#### R2.S5 A secret stays out of every output
- Given settings where `api_key`, `payload_keys` and `bucket_secret_key` have known values
- When the plugin runs the conformance runner's tests and a failed handshake
- Then no line on stderr, no error message and no result contains one of those values or a decrypted payload

### R3 One workflow per spec, per desk and for the relay config
The plugin keeps the data of one spec in one workflow, with the relay's workflow ID `spec-<deploy_id>-<repoKey>-<slug>`. The documents `specs/<repoKey>:<slug>`, `app-specs/<repoKey>:<slug>` and `closed-specs/<repoKey>:<slug>` and the stream `spec:<repoKey>:<slug>` all live in that workflow. The desk document `desks/<repoKey>` and the stream `desk:<repoKey>` live in the workflow `desk-<deploy_id>-<repoKey>`. The document `relay/config` lives in the workflow `relay-config-<deploy_id>`, on the task queue `<task_queue>-config`, with the config payload key. Every other collection, key or stream gets its own workflow with an ID that the plugin makes from the name.

#### R3.S1 The relay and the app write to one workflow
- Given a relay process and an app process, each with its own plugin and the same settings
- When the relay appends a message event to `spec:1a2b3c4d5e6f:fix-login` and the app appends an activity event to the same stream
- Then the workflow `spec-default-1a2b3c4d5e6f-fix-login` holds both events in one history
- And a read of the stream from either process returns both events in the order of their appends

#### R3.S2 The relay's IDs and task queue
- Given the settings `deploy_id=prod` and no `task_queue`
- When the host writes `specs/1a2b3c4d5e6f:fix-login` and `relay/config`
- Then Temporal shows the workflow `spec-prod-1a2b3c4d5e6f-fix-login` on the task queue `relay-prod`
- And the workflow `relay-config-prod` on the task queue `relay-prod-config`

#### R3.S3 A name that has no relay workflow
- Given no workflow for the collection `notes`
- When the host writes `notes/a1` and then reads it
- Then the read returns the value that the host wrote

#### R3.S4 The payloads are encrypted
- Given the settings with a data key and a config key
- When the host writes `specs/1a2b3c4d5e6f:fix-login` and `relay/config`
- Then every payload in the two histories has the encoding `binary/encrypted` and the key ID in its metadata
- And the spec workflow's payloads use the data key, and the config workflow's payloads use the config key
- And the relay's codec (`src/codec.ts`) decrypts them

### R4 The state capability with an atomic compare and swap
Each `state.put` and `state.delete` is one workflow update. The update checks the expected version and writes in one step, so two writes with the same expected version never both succeed, also from two processes. A refused write adds no event to the history. A version is never used again for the same collection and key, also after a delete and after the workflow ends and starts again.

#### R4.S1 Two writes at the same time from two processes
- Given the document `specs/1a2b3c4d5e6f:fix-login` at version V1, and two plugin processes on the same namespace
- When each process writes it with the expected version V1 at the same time
- Then exactly one write succeeds and the other gets `conflict` with `data.version` set to the new version

#### R4.S2 A conflict leaves no trace
- Given the document at version V2
- When the host writes it with the expected version V1
- Then the plugin answers `conflict`, the document stays at V2, and the workflow history has no new update event

#### R4.S3 No version is used again
- Given the document `notes/a1` written, deleted and written again, and its workflow ended between the delete and the second write
- When the host reads the document
- Then its version is not equal to any version that the plugin gave before for `notes/a1`

### R5 Lists by attribute
`state.list` returns the documents of a collection that match the filter, in the order of their keys, in pages. A list that follows a successful write shows that write.

#### R5.S1 A list right after a write
- Given three documents in `specs`, two with the attribute `pendingForMac` true
- When the host writes a fourth document with `pendingForMac` true and lists at once with that filter
- Then the result has the three documents with `pendingForMac` true, with their keys, attributes and versions

#### R5.S2 A long list
- Given 250 documents in `specs`
- When the host lists with a page size of 100
- Then it gets each document once, in key order, in three pages

### R6 The history is the event log
Each `events.append` is one workflow update, so the events are in the workflow history. `events.read` returns them in order, after an opaque cursor or in a time range `from <= at < to`. The time of an event is the host's `at`, or the time the plugin stored it.

#### R6.S1 Append and read in order
- Given an empty stream `spec:1a2b3c4d5e6f:fix-login`
- When the host appends A, B and C in one call and D in a second call
- Then a read from the start returns A, B, C and D in that order, each with a cursor, its type, its data and its time

#### R6.S2 A late event with its own time
- Given an app that was offline at 10:00
- When it appends an event with `at` 10:00 at 10:30
- Then a read of the range 09:59 to 10:01 returns that event with the time 10:00

#### R6.S3 A cursor that the plugin did not make
- Given a stream with events
- When the host reads after the cursor `abc`
- Then the plugin answers `refused`

### R7 Watch
`watch.wait` ends when a document's version is no longer the version that the host has, or when a stream has an event after the host's cursor, or at the timeout.

#### R7.S1 A wait that ends at a write from another process
- Given a host that waits on `specs/1a2b3c4d5e6f:fix-login` at version V1 with a timeout of 30 seconds
- When another process writes the document
- Then the wait ends with `changed: true` and the new version, before the timeout

#### R7.S2 A wait on a stream that times out
- Given a host that waits on a stream after its last cursor with a timeout of 1 second
- When no event comes
- Then the wait ends with `changed: false` after about 1 second

### R8 Blobs in a bucket or a folder
The plugin keeps blobs in the S3-compatible bucket of the bucket settings, under a prefix that does not touch the relay's attachments. With no bucket, it keeps them in `blob_dir`. The plugin checks the size and the SHA-256 of each upload before it keeps it.

#### R8.S1 A blob in the local folder
- Given the settings with `blob_dir` and no bucket
- When the host uploads a 3 MiB blob in chunks and reads it back in chunks
- Then the bytes are the same, and the file is in `blob_dir`

#### R8.S2 A blob in the bucket
- Given the settings of an S3-compatible bucket (MinIO in CI)
- When the host uploads a blob and calls `blobs.has` with its SHA-256
- Then the result has the SHA-256, and the bucket holds the blob outside the relay's attachment keys

#### R8.S3 A wrong hash
- Given an upload whose bytes do not match the SHA-256 that the host gave
- When the host commits it
- Then the plugin answers `refused` and keeps nothing

### R9 Continue-as-new, the export and reads across runs
A workflow continues as new when its history gets large (Temporal's suggestion, or the plugin's own limit of events). Before a run continues as new or ends, the plugin exports that run's full history to blobs and records the export in the next run's input. An `events.read` follows the chain of runs, reads a run from Temporal while Temporal has it and from its export after that, and never shows a run boundary to the host.

#### R9.S1 A read across a forced continue-as-new
- Given a stream with the events A and B
- When the host calls `test.continueStream` and then appends C
- Then a read from the start returns A, B and C, and a read after the cursor of B returns C

#### R9.S2 A read after retention removed the old run
- Given a stream whose first run continued as new after A and B, and whose next run holds C
- And the first run is gone from Temporal (deleted in the test, as the namespace's retention does)
- When the host reads from the start, and after the cursor of A
- Then the reads return A, B and C, and B and C, from the export and the live run

#### R9.S3 A continue-as-new from the history size
- Given the plugin's event limit set low for the test
- When the host appends more events than the limit
- Then the workflow continues as new by itself, and every event stays readable in order

#### R9.S4 A workflow that ends
- Given a spec workflow whose documents the host deleted and whose stream holds events
- When the workflow is idle for the plugin's idle time (set short for the test) and ends
- Then the plugin exported its last run first, and a later read of the stream returns every event
- And a later write to the spec starts a new run that links to the chain

#### R9.S5 Delete a stream
- Given a stream with events in two runs, one of them exported
- When the host calls `events.delete`
- Then a read returns no events, and the plugin removed the exports of that stream

### R10 The handoff from the relay's Temporal store
A relay that used its built-in Temporal store moves to the plugin with the same namespace, workflow IDs, task queues and payload keys, with no step from the owner. Each running relay workflow continues as new into the plugin's workflow type with the handoff input. The handoff input holds the documents and attributes that the relay's protocol adapter would write. The plugin starts from it, keeps the relay run's history in an export, and links it as the first run of the chain.

#### R10.S1 The state after a handoff is the same
- Given a fixture history made by the relay's current Temporal store, where a spec workflow took syncs, a message for the Mac and a message for Claude AI, and then continued as new into the plugin with the handoff input
- When the plugin's worker runs the next run and the host reads `specs/<repoKey>:<slug>`
- Then the value and the attributes are equal to the state that the relay's workflow query gave before the handoff

#### R10.S2 The desk and the relay config
- Given fixture histories of a desk workflow and the relay-config workflow that continued as new into the plugin
- When the host reads `desks/<repoKey>` and `relay/config`
- Then each value is equal to the state that the relay's query gave before the handoff

#### R10.S3 The relay's history is kept
- Given a spec workflow after the handoff
- When the relay's last run is gone from Temporal
- Then the export of that run is still in blobs, and the chain names it as the first run

#### R10.S4 A start input from another workflow
- Given a handoff input copied from one spec's workflow to another workflow ID
- When the plugin's workflow starts with it
- Then the workflow refuses the input and keeps no document from it, as the relay's workflows do

### R11 Determinism and versioning
Each change to a workflow keeps running workflows working. A change of a workflow's shape uses Temporal versioning (patches). CI replays stored histories of each plugin workflow, so a change that breaks determinism fails CI.

#### R11.S1 A change that breaks replay
- Given the stored histories in the repo
- When a code change makes a workflow give other commands for the same history
- Then the replay test fails in CI and names the history

### R12 Conformance in CI
CI runs the plugin-sdk conformance runner against the plugin and a Temporal dev server (`temporal server start-dev`), over stdio and over HTTP. The tests include reads across a continued run and across an exported run.

#### R12.S1 The conformance runner passes
- Given a Temporal dev server in CI
- When CI runs `plugin-conformance -- plugin-temporal` with the dev server's settings, and the same over HTTP
- Then every conformance test passes

### R13 Release and install
A tag `vX.Y.Z` builds static binaries for macOS (arm64, amd64) and Linux (arm64, amd64) and attaches them, with their SHA-256 sums, to a GitHub release. A Homebrew formula installs the binary.

#### R13.S1 A tag makes a release
- Given a commit on `main` with passing CI
- When a maintainer pushes the tag `v0.1.0`
- Then the GitHub release `v0.1.0` has four archives and a checksum file, and each binary prints `0.1.0` with `--version`

#### R13.S2 Install with Homebrew
- Given the formula `plugin-temporal` in spec-studio-app/homebrew-tap
- When a user runs `brew install spec-studio-app/tap/plugin-temporal`
- Then `plugin-temporal --version` prints the released version

### R14 The README
The README says what the plugin does, lists its settings, explains how history and replay work, the retention rule and the export, the handoff from the relay's Temporal store, and how to run the plugin with Temporal Cloud and with a self-hosted Temporal. It links to plugin-sdk. It is in ASD-STE100 Simplified Technical English.

#### R14.S1 A new owner runs the plugin
- Given an owner with a Temporal Cloud namespace and an API key
- When the owner follows the README
- Then the owner can run the plugin with the conformance runner, with no other document

## Edge cases
- A slug that contains `..` or `:`, as `fix..login`: the plugin splits the key at the first `:` only, and the workflow ID is the relay's ID.
- Two plugin processes on one namespace (the relay and `relay pair-code`, or the relay and the app): each runs a worker on the same task queue; the compare and swap stays atomic because the update runs in the workflow.
- An update that reaches a run while it continues as new: Temporal sends it to the next run, and the host sees no error.
- A read whose cursor names a run that the plugin has not exported yet and that Temporal already removed: the plugin answers `unavailable` and logs the run ID, and never returns a partial list as complete.
- A blob that is already present: `blobs.begin` answers `exists: true` and the upload stops.
- An event or a document near `maxMessageBytes`: the SDK limits apply, and Temporal's payload limit (2 MB by default) is never reached because the SDK's limit is lower.
- A payload with a key ID that is not in `payload_keys`: the worker fails the task and logs the key ID only; it never logs the payload.
- An old payload key: a key that stays in `payload_keys` after the first position still decrypts, as in the relay.
- The bucket is not reachable at the handshake: `unavailable`, with a message that names `bucket_endpoint`.

## Assumptions
- The handoff is by continue-as-new (the user's choice): the relay's next release adds a patched step to its three TypeScript workflows that continues each open run as new into the plugin's Go workflow type, with the same workflow ID and task queue. This spec defines the handoff input, documents it, and tests it with fixture histories. The relay-plugin-host spec adds the step to the relay. Closed relay specs (already ended) get their `closed-specs` documents from the relay's handoff, through the protocol.
- A Go worker cannot replay a TypeScript history, so "versioning" applies to the plugin's own Go workflows from the first release on. The plugin's workflow types have new names, so a relay workflow from before the handoff never reaches the Go worker's code by mistake.
- The relay's old update events are not translated into protocol events. The events of a spec start at the handoff. The old history stays whole in an export, so nothing is lost.
- The plugin knows the relay's names only to choose a workflow (R3). It applies no Spec Studio rule, and it does not change values or attributes.
- The plugin runs its own Temporal worker in its process. Two processes on one namespace share the task queue.
- The setting names are the relay's names in lowercase, with no `TEMPORAL_` prefix, as the protocol's setting names must be `a-z 0-9 _`.
- The exports and the manifest of each chain live in the blob store under a plugin prefix (`plugin-temporal/`), apart from the content-addressed blobs and from the relay's attachments. An export keeps the payloads encrypted, as Temporal stores them.
- The fixture histories are made once by a Node script in the repo, against the relay at a pinned commit, and are committed. CI does not need the relay to run the Go tests.
- Go 1.23 or later. Releases use GoReleaser.
- The Homebrew formula goes to the tap's `Formula/` folder, which the tap does not have yet.
