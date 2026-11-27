# plugin-temporal

The Temporal backend plugin of Spec Studio. It keeps each spec in one Temporal workflow, for both Spec Studio hosts: the relay and the Mac app. The relay's messages and the app's events go into the same workflow, so each spec has one full history. The app can replay a spec from its start.

When a host uses this plugin, the plugin is the store of the host. It is not a copy of other data. The Temporal event history is the one source of truth of each spec. Only the Claude Code transcripts, git and the content-addressed blobs are outside it.

The plugin speaks the backend plugin protocol of [plugin-sdk](https://github.com/spec-studio-app/plugin-sdk). Read [PROTOCOL.md](https://github.com/spec-studio-app/plugin-sdk/blob/main/PROTOCOL.md) for the calls, the errors and the rules.

## Contents

1. [What the plugin does](#what-the-plugin-does)
2. [Install](#install)
3. [Settings](#settings)
4. [Run with Temporal Cloud](#run-with-temporal-cloud)
5. [Run with a self-hosted Temporal](#run-with-a-self-hosted-temporal)
6. [How history and replay work](#how-history-and-replay-work)
   - [state.commit](#statecommit)
7. [The retention rule and the export](#the-retention-rule-and-the-export)
8. [Move a relay from its built-in Temporal store](#move-a-relay-from-its-built-in-temporal-store)
9. [Development](#development)

## What the plugin does

- It speaks the protocol versions 1.0 and 1.1. It gives the capabilities `state`, `events`, `watch` and `blobs`, the call `state.commit` of 1.1 (see [state.commit](#statecommit)), and the test hook `continueStream`.
- When the handshake chooses 1.1, the result has `storeId`: `temporal:<namespace>/<namespace ID>/<deploy_id>`. The namespace ID comes from Temporal (`DescribeNamespace`), so two servers that each have a namespace `default` give two values. The address is not part of it: one namespace has many addresses (the namespace endpoint and the regional endpoint of Temporal Cloud, a cluster service name and a public host, `localhost` and `127.0.0.1`). Two plugin processes on the same namespace and deploy ID use the same workflows, so they give the same `storeId`. The app and the relay compare it to know that they share one store.
- It keeps one workflow for each spec, one for each desk and one for the relay config. It uses the relay's workflow IDs and task queues:

  | Data | Workflow ID | Task queue |
  |---|---|---|
  | `specs`, `app-specs` and `closed-specs` documents `<repoKey>:<slug>`, and the stream `spec:<repoKey>:<slug>` | `spec-<deploy_id>-<repoKey>-<slug>` | `<task_queue>` |
  | The `desks` document `<repoKey>`, and the stream `desk:<repoKey>` | `desk-<deploy_id>-<repoKey>` | `<task_queue>` |
  | The `relay` document `config` | `relay-config-<deploy_id>` | `<task_queue>-config` |
  | Any other `relay` document | `doc/<deploy_id>/relay/<key>` | `<task_queue>-config` |
  | Any other document | `doc/<deploy_id>/<collection>/<key>` | `<task_queue>` |
  | Any other stream | `stream/<deploy_id>/<stream>` | `<task_queue>` |

  A repo key gets the relay's IDs only when it has the relay's form: 12 lowercase hex digits. Any other key gets the generic ID of the last two rows.

- Each write is one workflow update. The update checks the expected version and writes in one step, so a compare and swap is atomic, also between two processes. `state.commit` is one update too: it writes a document and appends its events.
- Each collection has a list workflow, `catalog/<deploy_id>/<collection>`. A list shows each write that returned. The list workflow of the `relay` collection is on `<task_queue>-config`.
- The plugin encrypts every payload with the owner's payload keys. It uses the relay's codec (AES-256-GCM), so the relay's codec endpoints for the Temporal Web UI still work. The workflows of the `relay` collection (the relay config, other `relay` documents and their list workflow) use the config key. All other workflows use the data key.
- The plugin never writes a key, a secret setting or a payload to its log, to an error or to a result.
- It keeps blobs (files) in an S3-compatible bucket, or in a local folder.
- The plugin process is a Temporal client and a Temporal worker. It runs the workflows itself. You do not run another worker.

The plugin knows no Spec Studio rule. The host applies its rules and stores the result. The plugin knows the relay's names only to choose a workflow.

## Install

With Homebrew (macOS and Linux):

```sh
brew install spec-studio-app/tap/plugin-temporal
plugin-temporal --version
```

Or get an archive from the [releases](https://github.com/spec-studio-app/plugin-temporal/releases): macOS and Linux, arm64 and amd64. Each binary is static.

The host starts the plugin:

```sh
plugin-temporal                                   # stdio (the app, and the relay by default)
plugin-temporal --listen 127.0.0.1:7070 --token T # HTTP (a sidecar of the relay)
```

Over HTTP, set a token with `--token` or `PLUGIN_TOKEN`. Without a token, the plugin listens on a loopback address only.

## Settings

The host sends the settings in the handshake. The names are the relay's settings in lowercase.

| Setting | Required | Secret | Meaning |
|---|---|---|---|
| `address` | yes | | The Temporal address, `host:port`. |
| `namespace` | | | The namespace. Default: `default`. |
| `auth` | | | `plain`, `tls`, `mtls` or `api-key`. Default: `api-key` when `api_key` is set, `mtls` when `tls_cert` is set, else `plain`. |
| `api_key` | | yes | The Temporal Cloud API key. |
| `tls_cert` | | | The mTLS client certificate: PEM text, or the path of a PEM file. |
| `tls_key` | | yes | The mTLS client key: PEM text, or the path of a PEM file. |
| `payload_keys` | yes | yes | The payload keys, in the relay's `PAYLOAD_KEYS` format: `data:<id>:<base64 key>,config:<id>:<base64 key>`. The first key of each scope encrypts. Every key decrypts, so you can add a new key in front and keep the old one. `relay keys` makes a value. |
| `deploy_id` | | | The relay's deploy ID. It is part of each workflow ID. Default: `default`. |
| `task_queue` | | | The task queue. Default: `relay-<deploy_id>`. |
| `bucket_endpoint` | | | The URL of an S3-compatible service. |
| `bucket_name` | | | The bucket. |
| `bucket_access_key` | | yes | The access key ID. |
| `bucket_secret_key` | | yes | The secret access key. |
| `bucket_region` | | | The region. Default: `auto`. |
| `blob_dir` | | | A local folder for blobs and exports, when you set no bucket. |

Set the four bucket settings, or none. Set a bucket or `blob_dir`.

- On a server, use a bucket. Use the bucket of the relay's attachments if you like: the plugin writes only under `_plugin-temporal/`. A relay deploy ID cannot start with `_`, so the plugin never touches the attachments.
- The app on one Mac can use `blob_dir`.
- When the relay and the app share a spec, give them the same bucket. Then each one can read the exports that the other one wrote.

A wrong or missing setting stops the handshake with `bad_settings`, and the message names the setting. A Temporal or a bucket that does not answer gives `unavailable`.

## Run with Temporal Cloud

1. Make an API key in Temporal Cloud for a service account with write access to the namespace.
2. Get the endpoint of the namespace, for example `my-ns.a1b2c.tmprl.cloud:7233`, or the regional endpoint for API keys.
3. Set the settings:

   ```sh
   plugin-conformance \
     --setting address=my-ns.a1b2c.tmprl.cloud:7233 \
     --setting namespace=my-ns.a1b2c \
     --setting api_key="$TEMPORAL_API_KEY" \
     --setting payload_keys="$PAYLOAD_KEYS" \
     --setting blob_dir="$HOME/.spec-studio/blobs" \
     -- plugin-temporal
   ```

   `plugin-conformance` is the test runner of plugin-sdk. It does a handshake and every call of the protocol. When all tests pass, the plugin works with your namespace. Use a test `deploy_id` for this check, so the test data stays apart from your specs.

For mTLS, set `tls_cert` and `tls_key` in place of `api_key`.

## Run with a self-hosted Temporal

The plugin needs Temporal Server 1.27 or later, because it uses update-with-start. On an older server, the handshake fails with `unavailable` and says the version.

```sh
temporal server start-dev --headless   # a local dev server, for a test
plugin-conformance \
  --setting address=127.0.0.1:7233 \
  --setting payload_keys="$PAYLOAD_KEYS" \
  --setting blob_dir=/tmp/blobs \
  -- plugin-temporal
```

For a server with TLS, set `auth=tls`. The plugin needs no search attribute.

## How history and replay work

The workflow history is the event log.

- Each `events.append` is one workflow update. The update's input holds the events. Its result holds their sequence numbers and times. Both stay in the history.
- An `events.read` reads the history of each run of the workflow, takes the completed `append` updates, and returns their events in order.
- A cursor is opaque. It names the event's sequence number in its chain of runs. A cursor stays valid after a run ends.
- A read can go by cursor (`after`) or by time (`from` and `to`). An event's time is the host's `at`, or the time that the plugin stored it. The app sets `at` when it sends events late, after it was offline.

The relay and the app write to the same spec workflow. So the relay's messages and the app's activity lines are in one ordered history. The app's scrubber reads the events of a time range, and reads a snapshot of a document from `blobs` by the SHA-256 that an event holds.

### state.commit

`state.commit` (protocol 1.1) writes a document and appends its events to a stream in one atomic step. The plugin sends one `commit` update to the workflow of the document.

- The update's validator checks the expected version. A stale version gets `conflict` with the stored version. The validator refuses the update, so the history has no event of it: no document and no event changes.
- A commit holds its document until it ends. Another `put`, `delete` or `commit` of the same document waits, then checks its version again. So nothing comes between a commit's check and its write.
- A failed commit writes nothing: no document and no event.
- A spec's document and its stream have the same workflow: `app-specs`, `specs` or `closed-specs` / `<repoKey>:<slug>` and the stream `spec:<repoKey>:<slug>` are all in `spec-<deploy_id>-<repoKey>-<slug>`. The update handler first puts the document's entry in the list workflow. Then, in the workflow task that completes the update, it sets the document and gives the events their sequence numbers. So no reader sees a sequence number (from `watch.wait`, or after an `append` that came later) before `events.read` can return its event. When the list put fails, the commit writes nothing. The update's input (the document and the events) and its result (the version and the sequence numbers) are in the history together. So the history holds both, or neither. An `events.read` takes the events of the completed `commit` updates and of the `append` updates, in order.
- A document and a stream with two workflows (any other names, for example a document `notes/a` and a stream `notes-log`): the update first sends the events to the stream's workflow with the `Deliver` activity. Temporal runs the activity again until the stream takes the events. Only after the delivery does the update set the document and put its list entry, and then it completes. When the delivery fails (the stream refuses it), the commit writes nothing. The stream keeps the last 64 delivery IDs (`<document workflow>/<version>`) in its chain, so a delivery that comes again adds no event. For a short time at the end of the commit, a reader can see the events before the new document. When the list put fails after the delivery, the commit stands (the document and the events are written), and the next write of the document puts its list entry. The commits of one document go one at a time, in the order of their versions.
- A document of the `relay` collection uses the config key. A commit of such a document with events in a stream of the data key gets `refused`.
- `events` can be empty. Then the update writes only the document, and the result has no cursor.

Limits of one commit:

- The whole request is at most `maxMessageBytes` (1.5 MiB), as for `events.append`, so the update's input stays under Temporal's payload limit (2 MB). The plugin-sdk refuses a larger request. No other limit applies to the number of events: split a large batch into more commits.
- A commit is one update of the run. It counts toward the run limit (2000) and the continue-as-new rules, as an `append` does. A run that closes refuses the update, and the plugin sends it again to the next run.
- The document counts toward the 512 KiB of documents in a continue-as-new input, as for `state.put`.

### Update every plugin-temporal of a deploy together

The app's plugin and the relay's plugin are Temporal workers on the same task queue, `relay-<deploy_id>`. Each one can run a workflow task of any spec workflow. So they must run the same version of plugin-temporal. An older plugin-temporal (one with no `commit` update) on the same task queue:

- does not know the `commit` update. When it gets one, it rejects it: the commit writes nothing, and the caller gets `unavailable` with a text that says to update every plugin-temporal of the deploy.
- cannot replay a history that has a `commit` update. Its workflow task fails, and Temporal gives the task again, maybe to the same worker. It has no `Deliver` activity, so that activity waits for a newer worker.
- drops the delivery IDs of a stream's chain when it continues the run as new. Then a delivery that comes again can add its events a second time.

The plugin cannot prevent this by itself. So update the relay's plugin-temporal and every app on the same deploy together, before the app uses protocol 1.1 (`state.commit`). Each change to the workflow code uses `workflow.GetVersion`, so a newer worker replays the histories of an older one.

## The retention rule and the export

A workflow cannot grow for ever. A run continues as new when Temporal suggests it, or after 2000 updates. A run with no document ends after 7 days with no update. Temporal deletes a closed run after the namespace's retention period (30 days by default on a self-hosted server; set on each namespace in Temporal Cloud).

So that no event is lost:

1. Before a run continues as new or ends, it refuses new updates. The plugin sends a refused update again, and the next run takes it.
2. The run waits for its updates in progress.
3. The run exports its own history to the blob store: `exports/<workflow ID>/<run ID>.json.gz`. The export is the Temporal JSON format (the format of `temporal workflow show --output json`), gzipped. The payloads stay encrypted, as Temporal keeps them.
4. The run writes the chain of runs to `chains/<workflow ID>.json`, and gives the chain to the next run.

When the documents of a run are larger than 512 KiB, they do not go into the next run's input. The run writes them to `state/<workflow ID>/<run ID>.bin`, one Temporal payload for each document, encrypted with the payload key. The next run reads them one at a time. A list workflow does the same with its entries (`catalog/<workflow ID>/<run ID>.bin`).

The export comes before the run closes. So the retention never removes a run that has no export.

A read follows the chain. It reads a run from Temporal while Temporal has the run, and from its export after that. The host never sees a run boundary. A write to a workflow that ended starts a new run, which reads the chain and links to it.

`events.delete` removes every event of the stream and the exports of its workflow.

## Move a relay from its built-in Temporal store

A relay that used its built-in Temporal store moves to this plugin with the same namespace, workflow IDs, task queues and payload keys. It keeps its data. Each open relay workflow continues as new into the plugin's workflow type, `SpecStudioEntity`, and gives its state as documents. The relay does this once, at start. Read [HANDOFF.md](HANDOFF.md).

## Development

- Go 1.26 or later.
- `go test ./...`: the unit tests, the workflow tests (the Temporal Go SDK test suite), and the replay tests.
- The replay tests replay each history in `internal/workflows/testdata/histories`. The `v2-*` histories have `commit` updates and a `Deliver` activity. The `v3-*` histories have the remote commit that delivers first (`commit-deliver-first`). A change that makes a workflow give other commands for a stored history fails them. To change a workflow's shape, use `workflow.GetVersion`, then add new histories with `scripts/capture-histories.sh <name>`, for example `v2`. The script writes `<name>-*.json` and never replaces the files of an earlier shape: they must still replay. The repo is public, so the script replaces the worker identities and the sticky task queue names (they hold the computer's name) with `test-worker`; a test fails on a history that still has them.
- The integration tests need a Temporal dev server, and for the bucket test an S3-compatible server:

  ```sh
  temporal server start-dev --headless &
  scripts/ci-s3.sh   # SeaweedFS on 127.0.0.1:9000 (Docker)
  TEST_TEMPORAL_ADDRESS=127.0.0.1:7233 \
  TEST_BUCKET_ENDPOINT=http://127.0.0.1:9000 TEST_BUCKET_NAME=test \
  TEST_BUCKET_ACCESS_KEY=any TEST_BUCKET_SECRET_KEY=any \
    go test -tags integration ./...
  ```

- `scripts/handoff-fixtures.sh` runs the relay's Temporal store with the handoff step, and writes the fixtures of the handoff tests.
- CI runs the plugin-sdk conformance runner against a dev server, over stdio (with a folder and with a bucket) and over HTTP.
- A tag `vX.Y.Z` makes a release: the four binaries on the GitHub release, and the formula in [spec-studio-app/homebrew-tap](https://github.com/spec-studio-app/homebrew-tap). The formula push needs the `HOMEBREW_TAP_TOKEN` secret.
- The test-only flags `--run-limit`, `--idle` and `--catalog-run-limit` make runs continue or end sooner.
