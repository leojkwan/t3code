import * as NodeFS from "node:fs";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as Orchestrator from "./Orchestrator.ts";
import type {
  ProviderAdapterV2Event,
  ProviderAdapterV2InterruptInput,
  ProviderAdapterV2Shape,
  ProviderAdapterV2TurnInput,
} from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "./testkit/ReplayFixtureWorkspace.ts";

// Stop falsifier harness (shadow row ~dg03). Nonbillable: the provider is a stub
// that counts every turn it is asked to start. The stub request count is the
// observable for "nothing was dispatched". Runs only against an in-process
// orchestrator on a scratch workspace; it never touches a live goal, schedule,
// service or provider. Each case reports what the current source did (OBS lines)
// and each guard has a negative control that must turn the check red.
//
// Cases here cover the spec cases that need no goal-level hold: F1, F2 (queue
// promotion and a send to a stopped thread), F3. Cases F4-F6, B1-B3 and C1 are
// NOT RUN.

const driver = ProviderDriverKind.make("codex");
const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "test-model" };

interface Stub {
  readonly adapter: ProviderAdapterV2Shape;
  readonly started: ProviderAdapterV2TurnInput[];
  readonly interrupts: ProviderAdapterV2InterruptInput[];
  readonly events: Queue.Queue<ProviderAdapterV2Event>;
}

const makeStub = (cwd: string) =>
  Effect.gen(function* () {
    const events = yield* Queue.unbounded<ProviderAdapterV2Event>();
    const started: ProviderAdapterV2TurnInput[] = [];
    const interrupts: ProviderAdapterV2InterruptInput[] = [];
    const adapter: ProviderAdapterV2Shape = {
      instanceId,
      driver,
      getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
      openSession: (input) =>
        Effect.gen(function* () {
          const now = yield* DateTime.now;
          return {
            instanceId,
            driver,
            providerSessionId: input.providerSessionId,
            providerSession: {
              id: input.providerSessionId,
              driver,
              providerInstanceId: instanceId,
              status: "ready",
              cwd,
              model: modelSelection.model,
              capabilities: CodexProviderCapabilitiesV2,
              createdAt: now,
              updatedAt: now,
              lastError: null,
            },
            events: Stream.fromQueue(events),
            ensureThread: ({ threadId }) =>
              Effect.succeed({
                id: ProviderThreadId.make(`provider-thread:codex:${threadId}`),
                driver,
                providerInstanceId: instanceId,
                providerSessionId: input.providerSessionId,
                appThreadId: threadId,
                ownerNodeId: null,
                nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                createdAt: now,
                updatedAt: now,
              }),
            resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
            startTurn: (turn) =>
              Effect.gen(function* () {
                started.push(turn);
                yield* Queue.offer(events, {
                  type: "provider_turn.updated",
                  driver,
                  providerTurn: {
                    id: ProviderTurnId.make(`provider-turn:${turn.attemptId}`),
                    providerThreadId: turn.providerThread.id,
                    nodeId: turn.rootNodeId,
                    runAttemptId: turn.attemptId,
                    nativeTurnRef: {
                      driver,
                      nativeId: `native:${turn.attemptId}`,
                      strength: "strong",
                    },
                    ordinal: turn.providerTurnOrdinal,
                    status: "running",
                    startedAt: now,
                    completedAt: null,
                  },
                });
              }),
            steerTurn: () => Effect.die("unused"),
            interruptTurn: (interrupt) =>
              Effect.sync(() => {
                interrupts.push(interrupt);
              }),
            respondToRuntimeRequest: () => Effect.die("unused"),
            readThreadSnapshot: () => Effect.die("unused"),
            rollbackThread: () => Effect.die("unused"),
            forkThread: () => Effect.die("unused"),
          };
        }),
    };
    return { adapter, started, interrupts, events } satisfies Stub;
  });

interface Observation {
  readonly case: string;
  readonly runStatuses: ReadonlyArray<string>;
  readonly queueHeld: ReadonlyArray<boolean>;
  readonly stubStarts: number;
  readonly interrupts: number;
}

// The runner hides console output, so results go to a file named by DG03_OBS_FILE.
const report = (verdict: string, obs: Observation) => {
  const file = process.env.DG03_OBS_FILE;
  if (file !== undefined) NodeFS.appendFileSync(file, `${verdict} ${JSON.stringify(obs)}\n`);
};

// Pure checkers, so a negative control can feed them a broken observation.
const stopCheck = (obs: Observation, startsBeforeStop: number) =>
  obs.runStatuses.every((status) => status === "interrupted") &&
  obs.interrupts >= 1 &&
  obs.stubStarts === startsBeforeStop;
const heldQueueCheck = (obs: Observation, startsBeforeStop: number) =>
  obs.runStatuses.at(-1) === "queued" &&
  obs.queueHeld.at(-1) === true &&
  obs.stubStarts === startsBeforeStop;

const withHarness = <A, E>(
  name: string,
  body: (input: {
    readonly orchestrator: Orchestrator.OrchestratorV2Shape;
    readonly worker: EffectWorker.OrchestrationEffectWorkerV2Shape;
    readonly stub: Stub;
    readonly threadId: ThreadId;
    readonly observe: (caseName: string) => Effect.Effect<Observation>;
    readonly send: (
      id: string,
      mode: "start_immediately" | "queue_after_active",
    ) => Effect.Effect<void>;
  }) => Effect.Effect<A, E, never>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace(name);
      const stub = yield* makeStub(cwd);
      return yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = ThreadId.make(`thread:${name}`);
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`create:${name}`),
          threadId,
          projectId: ProjectId.make(`project:${name}`),
          title: name,
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        const send = (id: string, mode: "start_immediately" | "queue_after_active") =>
          orchestrator
            .dispatch({
              type: "message.dispatch",
              commandId: CommandId.make(`cmd:${id}`),
              threadId,
              messageId: MessageId.make(`message:${id}`),
              text: id,
              attachments: [],
              dispatchMode: { type: mode },
              createdBy: "user",
              creationSource: "web",
            })
            .pipe(Effect.asVoid, Effect.orDie);
        const observe = (caseName: string) =>
          Effect.gen(function* () {
            const projection = yield* orchestrator.getThreadProjection(threadId);
            return {
              case: caseName,
              runStatuses: projection.runs.map((run) => run.status),
              queueHeld: projection.runs.map((run) => run.queueHeld === true),
              stubStarts: stub.started.length,
              interrupts: stub.interrupts.length,
            } satisfies Observation;
          }).pipe(Effect.orDie);
        return yield* body({ orchestrator, worker, stub, threadId, observe, send });
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name },
            ProviderAdapterRegistry.layerSingle(stub.adapter),
            { runEffectWorker: false },
          ),
        ),
      );
    }),
  );

const waitForRunning = (orchestrator: Orchestrator.OrchestratorV2Shape) =>
  orchestrator.streamDomainEvents.pipe(
    Stream.filter(
      (event: OrchestrationV2DomainEvent) =>
        event.type === "provider-turn.updated" && event.payload.status === "running",
    ),
    Stream.take(1),
    Stream.runDrain,
    Effect.forkScoped,
  );

// F3: Stop while the stub is mid-call interrupts the turn, and the stub is asked
// to start nothing afterward.
it.effect("F3 thread.stop mid-call interrupts the active turn", () =>
  withHarness("stop-f3", ({ orchestrator, worker, stub, threadId, observe, send }) =>
    Effect.gen(function* () {
      const running = yield* waitForRunning(orchestrator);
      yield* send("first", "start_immediately");
      yield* worker.drain();
      yield* Fiber.join(running);
      const before = yield* observe("F3-before-stop");
      assert.equal(before.stubStarts, 1);
      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop-f3"),
        threadId,
      });
      yield* worker.drain();
      const after = yield* observe("F3-after-stop");
      report(stopCheck(after, 1) ? "PASS" : "FAIL", after);
      assert.ok(stopCheck(after, 1), JSON.stringify(after));
      // Negative control: the same checker on the pre-stop observation must be red.
      assert.ok(!stopCheck(before, 1), "negative control: an unstopped run must fail the check");
      assert.equal(stub.started.length, 1);
    }),
  ),
);

// F2 (existing per-run hold): a queued follow-up stays queued and held after Stop,
// and the stub is asked to start nothing more.
it.effect("F2-queue thread.stop holds a queued follow-up", () =>
  withHarness("stop-f2q", ({ orchestrator, worker, threadId, observe, send }) =>
    Effect.gen(function* () {
      const running = yield* waitForRunning(orchestrator);
      yield* send("first", "start_immediately");
      yield* worker.drain();
      yield* Fiber.join(running);
      yield* send("follow-up", "queue_after_active");
      const queuedBefore = yield* observe("F2-queue-before-stop");
      assert.equal(queuedBefore.runStatuses.at(-1), "queued");
      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop-f2q"),
        threadId,
      });
      yield* worker.drain();
      const after = yield* observe("F2-queue-after-stop");
      report(heldQueueCheck(after, 1) ? "PASS" : "FAIL", after);
      assert.ok(heldQueueCheck(after, 1), JSON.stringify(after));
      // Negative control: before Stop the follow-up is queued but not held.
      assert.ok(!heldQueueCheck(queuedBefore, 1), "negative control: an unheld queue must fail");
    }),
  ),
);

// F2-send (the gap the design names): after Stop, a new message to the same thread.
// This case records what the current source does; it does not assert a refusal,
// because no goal-level hold exists yet. A start here is the finding.
it.effect("F2-send a new message to a stopped thread is recorded, not assumed refused", () =>
  withHarness("stop-f2s", ({ orchestrator, worker, threadId, observe, send }) =>
    Effect.gen(function* () {
      const running = yield* waitForRunning(orchestrator);
      yield* send("first", "start_immediately");
      yield* worker.drain();
      yield* Fiber.join(running);
      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop-f2s"),
        threadId,
      });
      yield* worker.drain();
      yield* send("after-stop", "start_immediately");
      yield* worker.drain();
      const after = yield* observe("F2-send-after-stop");
      const started = after.stubStarts > 1;
      report(started ? "GAP-send-after-stop-starts-a-turn" : "PASS-send-after-stop-refused", after);
      // Pins the observation, so a source change that adds a hold turns this red on purpose.
      assert.equal(started, true, JSON.stringify(after));
    }),
  ),
);
