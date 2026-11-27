# The handoff from the relay's Temporal store

This document is for the relay. It tells how a relay that uses its built-in Temporal store moves to `plugin-temporal` with the same namespace, and keeps all its data.

## Why a handoff

The relay's spec, desk and relay-config workflows are TypeScript. They keep their state in workflow memory. A Go worker cannot replay a TypeScript history, and it cannot query a TypeScript workflow. So each open relay workflow gives its state to the plugin itself: it continues as new into the plugin's workflow type.

## What stays the same

- The namespace.
- The workflow IDs: `spec-<deployId>-<repoKey>-<slug>`, `desk-<deployId>-<repoKey>` and `relay-config-<deployId>`.
- The task queues: `<taskQueue>` (default `relay-<deployId>`) and `<taskQueue>-config`.
- The payload keys (`PAYLOAD_KEYS`, the plugin setting `payload_keys`) and the codec. The handoff input is encrypted with the same keys.

## The steps in the relay

The reference change is `testdata/relay/handoff.patch`. It applies to the relay at commit `3f50e4a` (`scripts/handoff-fixtures.sh` uses it).

1. Add the update `handoff` to the three workflows. Its only argument is the relay's `Bound` for the workflow. A relay from before the handoff never sends it, so old histories replay the same.
2. When the update comes, the workflow refuses new updates, waits for its handlers, and continues as new with the workflow type `SpecStudioEntity`, the same task queue, and the handoff input below. Use `makeContinueAsNewFunc({ workflowType: "SpecStudioEntity" })`.
3. At start, the relay version that changes to the plugin runs the handoff once:
   1. It runs its old TypeScript worker. For each open spec, desk and relay-config workflow of its deploy ID, it sends `handoff`.
   2. It stops the old worker.
   3. It starts the plugin. The plugin's worker takes the new runs.

   Do not run the old worker and the plugin at the same time on one task queue: a workflow task can go to a worker that does not have its workflow type, fail, and retry.
4. For each spec that is already closed (in the namespace's retention), the relay writes the document `closed-specs/<repoKey>:<slug>` through the protocol, from the closed run's memo.

## The handoff input

The input of the new run is one JSON value:

```json
{
  "handoff": {
    "version": 1,
    "workflowId": "spec-prod-1a2b3c4d5e6f-fix-login",
    "documents": [
      {
        "collection": "specs",
        "key": "1a2b3c4d5e6f:fix-login",
        "value": { "...": "the SpecWorkflowState" },
        "attributes": { "repoKey": "1a2b3c4d5e6f", "open": true, "pendingForMac": true, "pendingForCoordinator": false, "deployId": "prod" }
      }
    ]
  }
}
```

- `workflowId` must be the ID of the workflow. The plugin's run ends at once, with no data, when it is another ID. So a copy of an input from one workflow cannot write to another.
- `documents` are the documents that the relay's protocol adapter writes for this workflow (PROTOCOL.md section 14 in plugin-sdk). The plugin stores them as they are. It knows no relay rule.
- The relay-config workflow uses the config key; the other workflows use the data key.

## What the plugin does with it

1. It exports the relay's last run (it is closed, so its history is whole) to the blob store, and makes it the first run of the workflow's chain. That run has no protocol events.
2. It writes each document with a new version, and puts it in the list of its collection.
3. From then on, the workflow is a plugin workflow. The relay's old history stays in the export after the namespace's retention removes it.

The events of a spec (`spec:<repoKey>:<slug>`) start at the handoff. The relay's old updates are not changed into protocol events.

## Tests

- `internal/workflows/handoff_fixture_test.go` reads `testdata/relay/fixtures`: the relay runs' histories and the expected documents, made by the relay's own Temporal store. It decrypts the handoff input, runs the plugin's workflow on it, and compares each document with the state that the relay's query gave.
- `internal/store/handoff_integration_test.go` runs the whole move against a Temporal server: the relay hands off, the plugin's worker takes over, the relay's run is deleted, and its export stays. CI runs it (the `handoff` job).
