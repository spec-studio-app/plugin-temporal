// The handoff fixtures of plugin-temporal. scripts/handoff-fixtures.sh copies
// this file into a relay checkout (at RELAY_COMMIT, with handoff.patch) and
// runs it against a Temporal server. It drives the relay's own Temporal store
// (spec, desk and relay-config workflows), queries each state, sends the
// handoff update, and writes the relay run's history and the expected
// documents to HANDOFF_OUT.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { historyToJSON } from "@temporalio/common/proto-utils";
import type { temporal } from "@temporalio/proto";
import type { Client } from "@temporalio/client";
import { harness } from "./temporal.js";
import { REQUIRED_SEARCH_ATTRIBUTES, makePairCode, pairCodeHash, temporalAdmin } from "../src/startup.js";
import { repoKeyOf, specAddress } from "../src/store/address.js";
import { boundFor } from "../src/workflows/bound.js";
import { configDocuments, deskDocuments, handoffUpdate, specDocuments, type HandoffDoc } from "../src/workflows/handoff.js";
import { deskWorkflowId, relayConfigWorkflowId, specWorkflowId } from "../src/workflows/types.js";
import { configStateQuery } from "../src/workflows/relayConfig.js";
import { stateQuery } from "../src/workflows/spec.js";
import { deskStateQuery } from "../src/workflows/desk.js";
import { configTaskQueue } from "../src/temporal/connection.js";
import { AesGcmCodec } from "../src/codec.js";

// R3.S4: the relay's codec decrypts a payload that plugin-temporal encrypted.
test("the relay's codec decrypts a payload of the plugin", async () => {
  const path = process.env.GO_VECTOR;
  assert.ok(path, "GO_VECTOR is not set");
  const v = JSON.parse(readFileSync(path, "utf8"));
  const codec = new AesGcmCodec({ encryptKey: { id: v.keyId, key: Buffer.from(v.keyBase64, "base64") } });
  const metadata = Object.fromEntries(Object.entries(v.metadata as Record<string, string>).map(([k, val]) => [k, new TextEncoder().encode(val)]));
  const [plain] = await codec.decode([{ metadata, data: Buffer.from(v.dataBase64, "base64") }]);
  assert.equal(new TextDecoder().decode(plain.metadata?.encoding ?? new Uint8Array()), v.plainEncoding);
  assert.equal(new TextDecoder().decode(plain.data ?? new Uint8Array()), v.plainData);
});

async function history(client: Client, workflowId: string, runId: string): Promise<temporal.api.history.v1.IHistory> {
  const events: temporal.api.history.v1.IHistoryEvent[] = [];
  let nextPageToken: Uint8Array | undefined;
  do {
    const page = await client.workflowService.getWorkflowExecutionHistory({
      namespace: "default",
      execution: { workflowId, runId },
      nextPageToken,
    });
    events.push(...(page.history?.events ?? []));
    nextPageToken = page.nextPageToken?.length ? page.nextPageToken : undefined;
  } while (nextPageToken);
  return { events };
}

async function handOff(client: Client, workflowId: string): Promise<string> {
  const handle = client.workflow.getHandle(workflowId);
  const runId = (await handle.describe()).runId;
  await handle.executeUpdate(handoffUpdate, { args: [boundFor(workflowId)] });
  const end = Date.now() + 30_000;
  for (;;) {
    const d = await client.workflow.getHandle(workflowId, runId).describe();
    if (d.status.name === "CONTINUED_AS_NEW") return runId;
    if (Date.now() > end) throw new Error(`${workflowId} did not continue as new: ${d.status.name}`);
    await new Promise((r) => setTimeout(r, 100));
  }
}

test("handoff fixtures", async () => {
  const out = process.env.HANDOFF_OUT;
  assert.ok(out, "HANDOFF_OUT is not set");
  mkdirSync(out, { recursive: true });
  const env = await TestWorkflowEnvironment.createFromExistingServer({ address: process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233" });
  try {
    await temporalAdmin(env.connection).addSearchAttributes("default", REQUIRED_SEARCH_ATTRIBUTES);
  } catch {
    // They exist already.
  }
  const h = await harness(env, { keysText: process.env.HANDOFF_KEYS, deployId: process.env.HANDOFF_DEPLOY, graceMs: 3_600_000 });
  try {
    const repo = "/Users/me/code/fixture-app";
    const spec = specAddress(repo, "fix-login");
    const repoKey = repoKeyOf(repo);

    // A spec: syncs, a message for Claude AI (a message line), a message for the Mac.
    await h.mirror.sync(spec, {
      lines: [
        { line: 1, event: { t: "2026-11-27T10:00:00Z", kind: "status" } },
        { line: 2, event: { t: "2026-11-27T10:01:00Z", kind: "message", id: "r1", text: "CI is green on main." } },
      ],
      first: true,
      state: { slug: "fix-login", title: "Fix login", stage: "build", status: "working", mode: "autopilot", createdAt: "2026-11-27T09:00:00Z" },
      turn: null,
    });
    await h.mirror.sync(spec, { lines: [{ line: 3, event: { t: "2026-11-27T10:02:00Z", kind: "status" } }] });
    await h.mirror.send(spec, { text: "How is it going?" });

    // A desk with a message for the Mac.
    await h.desks.report({ repoKey, repo, open: true, turn: { turn: "idle", needsYou: false }, canMessage: true, reason: null });
    await h.desks.send({ repoKey }, { text: "Is CI green on main?" });

    // The relay config: a paired agent and the repos.
    await h.startConfig();
    const code = await makePairCode(h.clients.config, h.deployId);
    assert.ok(await h.configStore.pair(pairCodeHash(code), "a".repeat(64), new Date("2026-11-27T10:03:00Z")));
    await h.configStore.setRepos([repo], new Date("2026-11-27T10:04:00Z"));

    const specId = specWorkflowId(h.deployId, spec.repoKey, spec.slug);
    const deskId = deskWorkflowId(h.deployId, repoKey);
    const configId = relayConfigWorkflowId(h.deployId);
    const workflows: Array<{ name: string; client: Client; id: string; taskQueue: string; scope: string; docs: () => Promise<HandoffDoc[]> }> = [
      { name: "spec", client: h.clients.data, id: specId, taskQueue: h.taskQueue, scope: "data", docs: async () => specDocuments(await h.clients.data.workflow.getHandle(specId).query(stateQuery)) },
      { name: "desk", client: h.clients.data, id: deskId, taskQueue: h.taskQueue, scope: "data", docs: async () => deskDocuments(await h.clients.data.workflow.getHandle(deskId).query(deskStateQuery)) },
      { name: "config", client: h.clients.config, id: configId, taskQueue: configTaskQueue(h.taskQueue), scope: "config", docs: async () => configDocuments(await h.clients.config.workflow.getHandle(configId).query(configStateQuery)) },
    ];

    const expected = [];
    for (const w of workflows) {
      // The state that the relay's query gives, as documents.
      const documents = await w.docs();
      const relayRunId = await handOff(w.client, w.id);
      writeFileSync(join(out, `${w.name}-relay-run.json`), historyToJSON(await history(w.client, w.id, relayRunId)));
      expected.push({ name: w.name, workflowId: w.id, taskQueue: w.taskQueue, scope: w.scope, relayRunId, documents });
    }
    writeFileSync(join(out, "expected.json"), JSON.stringify({ keys: h.keysText, deployId: h.deployId, workflows: expected }, null, 2));
  } finally {
    await h.close();
    await env.teardown();
  }
});
