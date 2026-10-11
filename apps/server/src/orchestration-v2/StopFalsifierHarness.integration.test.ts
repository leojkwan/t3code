// @effect-diagnostics-next-line nodeBuiltinImport:off -- Synchronous fixture observations survive the isolated child process exit.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- This diagnostic harness constructs scratch paths before its Effect runtime exists.
import * as NodePath from "node:path";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- This harness must exercise two isolated OS processes, never the live server.
import * as NodeChildProcess from "node:child_process";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ScheduledTaskId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Scope from "effect/Scope";
import * as Deferred from "effect/Deferred";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ThreadLaunchService from "./ThreadLaunchService.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as Tracer from "effect/Tracer";
import * as Option from "effect/Option";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { makeProviderFailure } from "./ProviderFailure.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as EventSink from "./EventSink.ts";
import type {
  ProviderAdapterV2Event,
  ProviderAdapterV2InterruptInput,
  ProviderAdapterV2Shape,
  ProviderAdapterV2TurnInput,
} from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import type * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import { checkpointWorkspace } from "./testkit/ReplayFixtureWorkspace.ts";

// Stop falsifier harness (shadow row ~dg03). Nonbillable: the provider is a stub
// that counts every turn it is asked to start. The stub request count is the
// observable for "nothing was dispatched". Runs only against an in-process
// orchestrator on a scratch workspace; it never touches a live goal, schedule,
// service or provider. Each case reports what the current source did (OBS lines)
// and each guard has a negative control that must turn the check red.
//
// Covers local dispatch boundaries, tree policy, lock ordering and budget grants.
// F5 keeps the original in-process checks and adds two isolated OS processes
// over one scratch database/workspace. Native tool/process-tree cancellation,
// F6 and the complete C1 evolving-plan launch flow remain NOT RUN. C1-outbox
// proves only that an already admitted provider attempt cannot be replayed.

const driver = ProviderDriverKind.make("codex");
const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "test-model" };

interface Stub {
  readonly adapter: ProviderAdapterV2Shape;
  readonly started: ProviderAdapterV2TurnInput[];
  readonly interrupts: ProviderAdapterV2InterruptInput[];
  readonly steered: string[];
  readonly events: Queue.Queue<ProviderAdapterV2Event>;
}

const makeStub = (
  cwd: string,
  beforeProviderCall?: (type: "start" | "steer", threadId: ThreadId) => Effect.Effect<void>,
  emitTerminalOnInterrupt = false,
  withInterruptAcknowledgement?: (
    turn: ProviderAdapterV2TurnInput,
    emitTerminal: Effect.Effect<void>,
  ) => Effect.Effect<void>,
) =>
  Effect.gen(function* () {
    const events = yield* Queue.unbounded<ProviderAdapterV2Event>();
    const started: ProviderAdapterV2TurnInput[] = [];
    const interrupts: ProviderAdapterV2InterruptInput[] = [];
    const steered: string[] = [];
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
                yield* beforeProviderCall?.("start", turn.threadId) ?? Effect.void;
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
            steerTurn: (input) =>
              Effect.gen(function* () {
                steered.push(input.message.messageId);
                yield* beforeProviderCall?.("steer", input.threadId) ?? Effect.void;
              }),
            interruptTurn: (interrupt) =>
              Effect.gen(function* () {
                interrupts.push(interrupt);
                if (!emitTerminalOnInterrupt) return;
                const turn = started.find((candidate) =>
                  `provider-turn:${candidate.attemptId}` === interrupt.providerTurnId,
                );
                if (turn === undefined) {
                  return yield* Effect.die("The inert interrupt must target an actually started turn");
                }
                const emitTerminal = Effect.gen(function* () {
                  const stoppedAt = yield* DateTime.now;
                  yield* Queue.offer(events, {
                    type: "provider_turn.updated",
                    driver,
                    providerTurn: {
                      id: interrupt.providerTurnId,
                      providerThreadId: interrupt.providerThread.id,
                      nodeId: turn.rootNodeId,
                      runAttemptId: turn.attemptId,
                      nativeTurnRef: { driver, nativeId: `native:${turn.attemptId}`, strength: "strong" },
                      ordinal: turn.providerTurnOrdinal,
                      status: "interrupted",
                      startedAt: now,
                      completedAt: stoppedAt,
                    },
                  });
                  yield* Queue.offer(events, {
                    type: "turn.terminal",
                    driver,
                    providerThreadId: interrupt.providerThread.id,
                    providerTurnId: interrupt.providerTurnId,
                    runOrdinal: turn.runOrdinal,
                    status: "interrupted",
                    failure: null,
                    threadDisposition: "reusable",
                  });
                });
                yield* withInterruptAcknowledgement === undefined
                  ? emitTerminal
                  : withInterruptAcknowledgement(turn, emitTerminal);
              }),
            respondToRuntimeRequest: () => Effect.die("unused"),
            readThreadSnapshot: () => Effect.die("unused"),
            rollbackThread: () => Effect.die("unused"),
            forkThread: () => Effect.die("unused"),
          };
        }),
    };
    return { adapter, started, interrupts, steered, events } satisfies Stub;
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

interface PhaseOptions {
  readonly emitTerminalOnInterrupt?: boolean;
  readonly acknowledgeInterruptIngestion?: boolean;
  readonly captureBaseline?: Effect.Effect<void>;
  readonly checkpointRepositoryCheck?: CheckpointStore.CheckpointStore["Service"]["isGitRepository"];
  readonly inspectSetupSpawner?: boolean;
  readonly observeInterrupt?: (observation: ProviderReplayHarness.InterruptPhaseObservation) => void;
  readonly observeProviderDispatch?: (observation: ProviderReplayHarness.ProviderDispatchObservation) => void;
  readonly beforeProviderCall?: (
    type: "start" | "steer",
    threadId: ThreadId,
  ) => Effect.Effect<void>;
  readonly workspacePath?: string;
  // A second runtime over the same stored data: skip thread creation, own layer name.
  readonly existingThread?: boolean;
  readonly layerName?: string;
  readonly databasePath?: string;
  readonly recoverOnStartup?: boolean;
}

it.effect("F2-outbox a committed provider start observes a later durable hold", () =>
  withHarness("outbox-held", ({ orchestrator, worker, stub, threadId }) =>
    Effect.gen(function* () {
      yield* orchestrator.dispatch(messageCommand(threadId, "outbox-before-hold"));
      yield* orchestrator.dispatch({
        type: "goal.hold",
        commandId: CommandId.make("outbox-hold"),
        threadId,
        hold: "stop-before-provider",
      });
      yield* worker.drain();
      assert.equal(stub.started.length, 0);
      yield* orchestrator.dispatch({
        type: "goal.resume",
        commandId: CommandId.make("outbox-resume"),
        threadId,
      });
      yield* orchestrator.dispatch(messageCommand(threadId, "outbox-after-resume"));
      yield* worker.drain();
      assert.equal(stub.started.length, 1, "Explicit Resume must permit a real stub start");
    }),
  ),
);

it.effect("B2-outbox a lowered budget refuses an already committed provider request", () =>
  withHarness("outbox-budget", ({ orchestrator, worker, stub, threadId }) =>
    Effect.gen(function* () {
      yield* orchestrator.dispatch({
        type: "goal.budget.set",
        commandId: CommandId.make("outbox-budget-one"),
        threadId,
        limit: 1,
      });
      yield* orchestrator.dispatch(messageCommand(threadId, "outbox-reserved"));
      yield* orchestrator.dispatch({
        type: "goal.budget.set",
        commandId: CommandId.make("outbox-budget-zero"),
        threadId,
        limit: 0,
      });
      yield* worker.drain();
      assert.equal(stub.started.length, 0);
    }),
  ),
);

it.effect("B2-retry a recovered outbox attempt requires another provider budget slot", () =>
  withHarness("outbox-retry", ({ orchestrator, worker, stub, threadId, sql }) =>
    Effect.gen(function* () {
      yield* orchestrator.dispatch({
        type: "goal.budget.set",
        commandId: CommandId.make("retry-budget-one"),
        threadId,
        limit: 1,
      });
      yield* orchestrator.dispatch(messageCommand(threadId, "retry-reserved"));
      // An interrupted worker persists its attempted lease before a later worker
      // reclaims it. Exercise that persisted retry state through the real worker.
      yield* sql`UPDATE orchestration_v2_effect_outbox SET attempt_count = 1 WHERE effect_type = 'provider-turn.start'`;
      yield* worker.drain();
      assert.equal(stub.started.length, 0);
      const receipts =
        yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE command_type = 'goal.provider-dispatch'`;
      assert.equal(receipts.length, 0, "Refused retries must not record provider admission");
    }),
  ),
);

it.effect.each(["restart_active", "steer_active"] as const)(
  "F2-outbox $0 rechecks the hold at its provider call",
  (type) =>
    withHarness(`outbox-${type}`, ({ orchestrator, worker, stub, threadId, sink }) =>
      Effect.gen(function* () {
        const running = yield* waitForRunning(orchestrator);
        yield* orchestrator.dispatch(messageCommand(threadId, `${type}-first`));
        yield* worker.drain();
        yield* Fiber.join(running);
        assert.equal(stub.started.length, 1);
        const before = yield* orchestrator.getThreadProjection(threadId);
        const run = before.runs[0]!;
        yield* orchestrator.dispatch({
          ...messageCommand(threadId, `${type}-pending`),
          dispatchMode: { type, targetRunId: run.id },
        });
        if (type === "restart_active") {
          // A restart waits for the external provider's old turn to end. Supply
          // that acknowledgement explicitly so this case reaches the new start
          // rather than passing because the restart was unable to proceed.
          const now = yield* DateTime.now;
          yield* sink.write({
            events: [
              {
                id: EventId.make("restart-provider-ack"),
                type: "provider-turn.updated",
                threadId,
                runId: run.id,
                occurredAt: now,
                payload: { ...before.providerTurns[0]!, status: "interrupted", completedAt: now },
              },
            ],
          });
        }
        yield* orchestrator.dispatch({
          type: "goal.hold",
          commandId: CommandId.make(`${type}-hold`),
          threadId,
          hold: "stop-before-call",
        });
        yield* worker.drain();
        assert.equal(stub.started.length, 1);
        assert.equal(stub.steered.length, 0);
      }),
    ),
);

it.effect("C1-outbox an admitted attempt cannot call its provider again on replay", () =>
  withHarness("outbox-replay", ({ orchestrator, worker, stub, threadId, sql }) =>
    Effect.gen(function* () {
      yield* orchestrator.dispatch(messageCommand(threadId, "outbox-once"));
      yield* worker.drain();
      assert.equal(stub.started.length, 1);
      const rows = yield* sql<{ effect_id: string; attempt_count: number }>`
        SELECT effect_id, attempt_count FROM orchestration_v2_effect_outbox
        WHERE effect_type = 'provider-turn.start'`;
      assert.equal(rows.length, 1);
      const effect = rows[0]!;
      // Re-present the same claimed attempt, as a stale executor would. The
      // durable provider-admission receipt must refuse its external callback.
      yield* sql`UPDATE orchestration_v2_effect_outbox SET status = 'running' WHERE effect_id = ${effect.effect_id}`;
      let calls = 0;
      const replay = yield* Effect.result(
        orchestrator.withProviderDispatch(
          {
            threadId,
            effectId: effect.effect_id,
            attemptCount: effect.attempt_count,
          },
          Effect.sync(() => {
            calls += 1;
          }),
        ),
      );
      assert.equal(replay._tag, "Failure");
      assert.equal(calls, 0);
      assert.equal(stub.started.length, 1);
    }),
  ),
);

it.live("F5-process Stop survives a process exit and explicit Resume starts the stub", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const phase = process.env.DG04_STOP_PROCESS_PHASE;
      if (phase !== undefined) {
        assert.ok(phase === "write" || phase === "read");
        const databasePath = process.env.DG04_STOP_PROCESS_DB!;
        assert.ok(databasePath.includes("dg04-isolated-process-"));
        return yield* withHarness(
          "process-stop",
          ({ orchestrator, worker, stub, threadId, send }) =>
            Effect.gen(function* () {
              if (phase === "write") {
                yield* orchestrator.dispatch({
                  type: "goal.budget.set",
                  commandId: CommandId.make("process-budget"),
                  threadId,
                  limit: 2,
                });
                const running = yield* waitForRunning(orchestrator);
                yield* orchestrator.dispatch(messageCommand(threadId, "process-first"));
                yield* worker.drain();
                yield* Fiber.join(running);
                yield* send("process-queued", "queue_after_active");
                yield* orchestrator.dispatch({
                  type: "goal.hold",
                  commandId: CommandId.make("process-hold"),
                  threadId,
                  hold: "operator-stop",
                });
                yield* orchestrator.dispatch({
                  type: "thread.stop",
                  commandId: CommandId.make("process-stop"),
                  threadId,
                });
                yield* worker.drain();
                assert.equal(stub.started.length, 1);
              } else {
                const held = yield* Effect.result(
                  orchestrator.dispatch(messageCommand(threadId, "process-held-send")),
                );
                assert.equal(held._tag, "Failure");
                if (held._tag === "Failure")
                  assert.equal(held.failure._tag, "GoalDispatchHeldError");
                yield* worker.drain();
                assert.equal(stub.started.length, 0);
                yield* orchestrator.dispatch({
                  type: "goal.resume",
                  commandId: CommandId.make("process-resume"),
                  threadId,
                });
                yield* worker.drain();
                assert.equal(stub.started.length, 0, "Goal Resume must preserve the held queue");
                const running = yield* waitForRunning(orchestrator);
                yield* orchestrator.dispatch({
                  type: "queue.resume",
                  commandId: CommandId.make("process-queue-resume"),
                  threadId,
                });
                yield* worker.drain();
                yield* Fiber.join(running);
                assert.equal(stub.started.length, 1);
              }
              const projection = yield* orchestrator.getThreadProjection(threadId);
              NodeFS.writeFileSync(
                `${databasePath}.${phase}.json`,
                JSON.stringify({
                  phase,
                  pid: process.pid,
                  starts: stub.started.length,
                  runs: projection.runs.map((r) => r.status),
                  runIds: projection.runs.map((r) => r.id),
                  queueHeld: projection.runs.map((r) => r.queueHeld === true),
                  startedRunIds: stub.started.map((turn) => turn.runId),
                }),
              );
            }),
          {
            databasePath,
            workspacePath: process.env.DG04_STOP_PROCESS_CWD!,
            existingThread: phase === "read",
            recoverOnStartup: phase === "read",
          },
        );
      }
      const directory = yield* Effect.acquireRelease(
        Effect.sync(() =>
          NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "dg04-isolated-process-")),
        ),
        (directory) =>
          Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
      );
      const databasePath = NodePath.join(directory, "state.sqlite");
      // Both runtimes must use a workspace that survives the first process. A
      // phase-local checkpointWorkspace is deleted when that phase's scope ends.
      const workspacePath = yield* checkpointWorkspace("process-stop-shared");
      for (const phase of ["write", "read"] as const) {
        yield* Effect.promise(
          (signal) =>
            new Promise<void>((resolve, reject) => {
              NodeChildProcess.execFile(
                NodePath.resolve("node_modules/.bin/vp"),
                [
                  "test",
                  "run",
                  "apps/server/src/orchestration-v2/StopFalsifierHarness.integration.test.ts",
                  "-t",
                  "^F5-process Stop",
                  "--maxWorkers=1",
                  "--no-file-parallelism",
                  "--maxConcurrency=1",
                  "--testTimeout=10000",
                  "--retry=0",
                ],
                {
                  cwd: process.cwd(),
                  signal,
                  timeout: 40_000,
                  env: {
                    ...process.env,
                    DG04_STOP_PROCESS_PHASE: phase,
                    DG04_STOP_PROCESS_DB: databasePath,
                    DG04_STOP_PROCESS_CWD: workspacePath,
                  },
                },
                (error, stdout, stderr) =>
                  error ? reject({ phase, error, stdout, stderr }) : resolve(),
              );
            }),
        );
      }
      const written = JSON.parse(NodeFS.readFileSync(`${databasePath}.write.json`, "utf8"));
      const read = JSON.parse(NodeFS.readFileSync(`${databasePath}.read.json`, "utf8"));
      assert.notEqual(written.pid, read.pid);
      assert.notEqual(written.pid, process.pid);
      assert.notEqual(read.pid, process.pid);
      assert.equal(read.starts, 1);
      assert.equal(written.starts, 1);
      assert.deepEqual(written.runs, ["interrupted", "queued"]);
      assert.deepEqual(written.queueHeld, [false, true]);
      assert.deepEqual(read.runIds, written.runIds, "Resume must reuse the persisted queued run");
      assert.deepEqual(read.startedRunIds, [written.runIds[1]]);
      assert.equal(read.runs.length, written.runs.length);
      assert.equal(read.runs.at(-1), "running");
      assert.deepEqual(read.queueHeld, [false, false]);
      report("PASS-whole-process-queued-resume", {
        case: `F5-process-parent:${process.pid}-write:${written.pid}-read:${read.pid}`,
        runStatuses: read.runs,
        queueHeld: read.queueHeld,
        stubStarts: read.starts,
        interrupts: 0,
      });
    }),
  ),
);

const withHarness = <A, E>(
  name: string,
  body: (input: {
    readonly orchestrator: Orchestrator.OrchestratorV2Shape;
    readonly worker: EffectWorker.OrchestrationEffectWorkerV2Shape;
    readonly sink: EventSink.EventSinkV2["Service"];
    readonly sql: SqlClient.SqlClient;
    readonly executor: ThreadCommandExecutor.ThreadCommandExecutor["Service"];
    readonly management: ThreadManagementService.ThreadManagementServiceShape;
    readonly scheduled: ScheduledTaskService.ScheduledTaskService["Service"];
    readonly pollScheduled: Effect.Effect<void>;

    readonly stub: Stub;
    readonly setupSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"] | undefined;
    readonly threadId: ThreadId;
    readonly observe: (caseName: string) => Effect.Effect<Observation>;
    readonly send: (
      id: string,
      mode: "start_immediately" | "queue_after_active",
    ) => Effect.Effect<void>;
  }) => Effect.Effect<A, E, Scope.Scope>,
  options: PhaseOptions = {},
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = options.workspacePath ?? (yield* checkpointWorkspace(name));
      let interruptEventSink: EventSink.EventSinkV2["Service"] | undefined;
      const stub = yield* makeStub(
        cwd,
        options.beforeProviderCall,
        options.emitTerminalOnInterrupt,
        options.acknowledgeInterruptIngestion !== true ? undefined : (turn, emitTerminal) =>
          Effect.scoped(Effect.gen(function* () {
            if (interruptEventSink === undefined) {
              return yield* Effect.die("Interrupt acknowledgement requires the real EventSink");
            }
            const providerTurnId = ProviderTurnId.make(`provider-turn:${turn.attemptId}`);
            const pull = yield* interruptEventSink.stream({ threadId: turn.threadId }).pipe(
              Stream.filter(({ event }) =>
                (event.type === "provider-turn.updated" && event.payload.id === providerTurnId &&
                  (event.payload.status === "running" || event.payload.status === "interrupted")) ||
                (event.type === "run-attempt.updated" && event.payload.id === turn.attemptId &&
                  event.payload.status === "interrupted")),
              Stream.toPull,
            );
            // Pulling the persisted running event proves EventSink's live subscription
            // is installed before emission; toPull alone is lazy and does not.
            const running = yield* pull;
            assert.isTrue(running.every(({ event }) =>
              event.type === "provider-turn.updated" && event.payload.status === "running"));
            yield* emitTerminal;
            let turnCommitted = false;
            let attemptCommitted = false;
            while (!turnCommitted || !attemptCommitted) {
              for (const { event } of yield* pull) {
                if (event.type === "provider-turn.updated" && event.payload.status === "interrupted") {
                  turnCommitted = true;
                }
                if (event.type === "run-attempt.updated" && event.payload.status === "interrupted") {
                  attemptCommitted = true;
                }
              }
            }
          })).pipe(Effect.orDie),
      );
      let pollScheduled: Effect.Effect<void> = Effect.void;
      return yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const sink = yield* EventSink.EventSinkV2;
        interruptEventSink = sink;
        const threadId = ThreadId.make(`thread:${name}`);
        if (options.existingThread !== true) {
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
        }
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
        const sql = yield* SqlClient.SqlClient;
        const executor = yield* ThreadCommandExecutor.ThreadCommandExecutor;
        const management = yield* ThreadManagementService.ThreadManagementService;
        const scheduled = yield* ScheduledTaskService.ScheduledTaskService;
        const setupSpawner = options.inspectSetupSpawner === true
          ? yield* ChildProcessSpawner.ChildProcessSpawner
          : undefined;
        return yield* body({
          setupSpawner,
          orchestrator,
          worker,
          sink,
          stub,
          threadId,
          observe,
          send,
          sql,
          executor,
          management,
          scheduled,
          pollScheduled: Effect.suspend(() => pollScheduled),
        });
      }).pipe(
        Effect.provide(
          (() => {
            const databaseLayer =
              options.databasePath === undefined
                ? SqlitePersistence.layerMemory
                : SqlitePersistence.layerFromPath(options.databasePath).pipe(
                    Layer.provide(NodeServices.layer),
                  );
            const runtime = ProviderReplayHarness.layerWithRegistry(
              { name: options.layerName ?? name },
              ProviderAdapterRegistry.layerSingle(stub.adapter),
              {
                runEffectWorker: false,
                databaseLayer,
                recoverOnStartup: options.recoverOnStartup ?? false,
                ...(options.captureBaseline === undefined ? {} : { captureBaseline: options.captureBaseline }),
                ...(options.checkpointRepositoryCheck === undefined ? {} : { checkpointRepositoryCheck: options.checkpointRepositoryCheck }),
                ...(options.observeInterrupt === undefined ? {} : { observeInterrupt: options.observeInterrupt }),
                ...(options.observeProviderDispatch === undefined ? {} : { observeProviderDispatch: options.observeProviderDispatch }),
              },
            );
            const management = ThreadManagementService.layer.pipe(Layer.provide(runtime));
            const scheduled = ScheduledTaskService.layer.pipe(
              Layer.provide(
                Layer.mergeAll(
                  databaseLayer,
                  management,
                  NodeCrypto.layer,
                  Layer.mock(ThreadLaunchService.ThreadLaunchService)({
                    launch: () => Effect.die("bound task only"),
                  }),
                  Layer.mock(SecretRequests.SecretRequests)({}),
                  Layer.succeed(
                    Scheduler.Scheduler,
                    Scheduler.Scheduler.of({
                      register: <E, R>(_name: string, run: Effect.Effect<void, E, R>) =>
                        Effect.context<R>().pipe(
                          Effect.andThen((context) =>
                            Effect.sync(() => {
                              pollScheduled = run.pipe(
                                Effect.provideContext(context),
                                Effect.orDie,
                              );
                            }),
                          ),
                        ),
                    }),
                  ),
                ),
              ),
            );
            const exposedRuntime = options.inspectSetupSpawner === true
              ? runtime.pipe(Layer.provideMerge(NodeServices.layer))
              : runtime;
            return Layer.mergeAll(
              exposedRuntime,
              databaseLayer,
              ThreadCommandExecutor.layer,
              management,
              scheduled,
            );
          })(),
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

// F3: Stop an active turn after its provider start has returned.
it.effect("F3 thread.stop interrupts an active turn after its start call returned", () =>
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

class StopProgressInconclusive extends Data.TaggedError("StopProgressInconclusive")<{
  readonly stage: string;
  readonly schedulerRounds: number;
}> {
  override get message() {
    return `${this.stage}: finite scheduler work exhausted; INCONCLUSIVE / NOT QUALIFIED`;
  }
}

// A held attempt must stay resumable. The marker is after preparation, not adapter entry.
it.effect("F4 preparation hold resumes the same admitted attempt exactly once after release", () =>
  Effect.gen(function* () {
    const maxSchedulerRounds = 256;
    const entered = yield* Deferred.make<void>();
    const preparationReady = yield* Deferred.make<void>();
    const interrupted = yield* Deferred.make<void>();
    let continuations = 0;
    let ownedThread: ThreadId | undefined;
    let executing: Fiber.Fiber<unknown, unknown> | undefined;
    const gate: Array<ProviderReplayHarness.ProviderDispatchObservation & { order: number }> = [];
    let overflow = false;
    const observeGate = (observation: ProviderReplayHarness.ProviderDispatchObservation) => {
      if (observation.threadId !== ownedThread) return;
      if (gate.length === maxSchedulerRounds) { overflow = true; return; }
      gate.push({ ...observation, order: gate.length + 1 });
    };
    const pollFrozen = <A, E>(fiber: Fiber.Fiber<A, E>) => Effect.gen(function* () {
      for (let rounds = 0; rounds <= maxSchedulerRounds; rounds++) {
        const exit = yield* Effect.sync(() => fiber.pollUnsafe());
        if (exit !== undefined) return { exit, rounds };
        if (rounds < maxSchedulerRounds) yield* Effect.yieldNow;
      }
      return { exit: undefined, rounds: maxSchedulerRounds };
    });
    const bounded = <A, E, R>(work: Effect.Effect<A, E, R>, stage: string) =>
      Effect.gen(function* () {
        const fiber = yield* work.pipe(Effect.forkScoped);
        const progress = yield* pollFrozen(fiber);
        if (progress.exit === undefined) return yield* Effect.fail(
          new StopProgressInconclusive({ stage, schedulerRounds: progress.rounds }));
        return yield* progress.exit;
      });
    yield* withHarness("hold-release", ({ orchestrator, worker, threadId, sql, stub }) =>
      Effect.gen(function* () {
        ownedThread = threadId;
        const startCommand = messageCommand(threadId, "hold-release-start");
        const started = yield* bounded(orchestrator.dispatch(startCommand), "start-dispatch");
        executing = yield* worker.drain().pipe(Effect.forkScoped);
        yield* bounded(Deferred.await(entered), "preparation-entered");
        const claimed = yield* bounded(sql<{
          effect_id: string; thread_id: string; effect_type: string;
          attempt_count: number; status: string; lease_owner: string | null;
        }>`SELECT effect_id, thread_id, effect_type, attempt_count, status, lease_owner
           FROM orchestration_v2_effect_outbox WHERE thread_id = ${threadId}
             AND effect_type = 'provider-turn.start' AND status = 'running'`, "claimed-effect");
        assert.equal(claimed.length, 1);
        const exact = claimed[0]!;
        const admissionId = `goal-provider:${exact.effect_id}:${exact.attempt_count}`;
        const readCommand = (id: string) => sql<{
          command_id: string; aggregate_id: string; command_type: string;
          status: string; result_sequence: number; error: string | null;
        }>`SELECT command_id, aggregate_id, command_type, status, result_sequence, error
           FROM orchestration_command_receipts WHERE command_id = ${id}`;
        const snapshot = () => Effect.gen(function* () {
          const outbox = yield* sql<{
            effect_id: string; thread_id: string; effect_type: string;
            attempt_count: number; status: string; lease_owner: string | null;
          }>`SELECT effect_id, thread_id, effect_type, attempt_count, status, lease_owner
             FROM orchestration_v2_effect_outbox WHERE effect_id = ${exact.effect_id}`;
          const policies = yield* sql<{
            sequence: number; command_id: string | null; hold: string | null;
            request_limit: number | null; used: number; grant_id: string | null;
          }>`SELECT sequence, command_id, json_extract(payload_json, '$.hold') AS hold,
               json_extract(payload_json, '$.limit') AS request_limit,
               json_extract(payload_json, '$.used') AS used,
               json_extract(payload_json, '$.grant') AS grant_id
             FROM orchestration_events WHERE aggregate_kind = 'thread' AND stream_id = ${threadId}
               AND event_type = 'goal.dispatch-policy.updated' ORDER BY sequence LIMIT 257`;
          if (policies.length > maxSchedulerRounds) return yield* Effect.fail(
            new StopProgressInconclusive({ stage: "policy-observation-overflow", schedulerRounds: maxSchedulerRounds }));
          const controls = yield* sql<{
            command_id: string; command_type: string; status: string; result_sequence: number;
          }>`SELECT command_id, command_type, status, result_sequence
             FROM orchestration_command_receipts WHERE aggregate_id = ${threadId}
               AND command_type IN ('thread.stop', 'run.interrupt', 'goal.budget.set')
             ORDER BY command_id`;
          const admission = yield* readCommand(admissionId);
          const projection = yield* orchestrator.getThreadProjection(threadId);
          return {
            outbox, policies, controls, admission, continuations,
            interrupted: yield* Deferred.isDone(interrupted),
            providerInterruptCalls: stub.interrupts.length, adapterEntries: stub.started.length,
            runIdentity: projection.runs.map((run) => ({ id: run.id, activeAttemptId: run.activeAttemptId })),
            sessionIdentity: projection.providerThreads.map((thread) => ({ id: thread.id, sessionId: thread.providerSessionId })),
            clock: DateTime.toEpochMillis(yield* DateTime.now),
          };
        });
        const before = yield* bounded(snapshot(), "before-hold-snapshot");
        assert.equal(before.clock, 0);
        assert.equal(before.admission.length, 1);
        assert.equal(before.admission[0]!.status, "accepted");
        assert.equal(before.admission[0]!.command_type, "goal.provider-dispatch");
        assert.equal(before.continuations, 0);
        assert.isFalse(before.interrupted);
        assert.equal(before.adapterEntries, 1);
        const budget = (policy: (typeof before.policies)[number]) => ({
          limit: policy.request_limit, used: policy.used, grant: policy.grant_id,
        });
        const initialPolicy = before.policies.at(-1)!;
        assert.isDefined(initialPolicy);
        assert.isNull(initialPolicy.hold);
        const holdCommand = {
          type: "goal.hold" as const, commandId: CommandId.make("hold-release-hold"),
          threadId, hold: "preparation-hold",
        };
        const held = yield* bounded(orchestrator.dispatch(holdCommand), "hold-commit");
        const holdReceipt = yield* bounded(readCommand(holdCommand.commandId), "hold-receipt");
        assert.equal(holdReceipt.length, 1);
        assert.equal(holdReceipt[0]!.command_type, "goal.hold");
        assert.equal(holdReceipt[0]!.status, "accepted");
        assert.equal(holdReceipt[0]!.result_sequence, held.sequence);
        assert.isTrue(before.admission[0]!.result_sequence < held.sequence);
        const afterHold = yield* bounded(snapshot(), "hold-snapshot");
        assert.equal(afterHold.policies.at(-1)!.hold, "preparation-hold");
        assert.equal(afterHold.continuations, 0);
        // Preparation is now unconditionally ready while the REAL hold is still active.
        // This fixture creates no refusal, second gate call, hold latch or sleep.
        const heldGateStart = gate.length;
        yield* Deferred.succeed(preparationReady, undefined);
        const matchingRefusal = () => gate.slice(heldGateStart).find((o) =>
          o.phase === "failure" && o.effectId === exact.effect_id &&
          o.attemptCount === exact.attempt_count && o.threadId === threadId &&
          o.causeTag === "GoalDispatchHeldError" && o.causeThreadId === threadId &&
          o.causeGoalThreadId === threadId);
        let heldRounds = 0;
        for (; heldRounds <= maxSchedulerRounds; heldRounds++) {
          if (overflow || continuations !== 0 || matchingRefusal() !== undefined ||
              (yield* Effect.sync(() => executing!.pollUnsafe())) !== undefined) break;
          if (heldRounds < maxSchedulerRounds) yield* Effect.yieldNow;
        }
        const heldAtBound = {
          continuations, refusal: matchingRefusal() ?? null,
          workerExit: (yield* Effect.sync(() => executing!.pollUnsafe()))?._tag ?? "Pending",
          gate: gate.slice(), overflow, schedulerRounds: Math.min(heldRounds, maxSchedulerRounds),
        };
        const whileHeld = yield* bounded(snapshot(), "while-held-snapshot");
        console.error("DG04_HOLD_RELEASE_HELD", JSON.stringify({
          before, afterHold, holdReceipt, heldAtBound, whileHeld, maxSchedulerRounds,
          gateObservationLimit: "Only actual returned errors from the forwarded service; waiting alone is not refusal.",
        }));
        // Clear the actual durable hold, even when the preceding observation is negative.
        // A negative/missing witness is retained; release cannot retroactively qualify it.
        const releaseCommand = {
          type: "goal.resume" as const, commandId: CommandId.make("hold-release-clear"), threadId,
        };
        const released = yield* bounded(orchestrator.dispatch(releaseCommand), "hold-release-commit");
        const releaseReceipt = yield* bounded(readCommand(releaseCommand.commandId), "release-receipt");
        assert.equal(releaseReceipt.length, 1);
        assert.equal(releaseReceipt[0]!.command_type, "goal.resume");
        assert.equal(releaseReceipt[0]!.status, "accepted");
        assert.equal(releaseReceipt[0]!.result_sequence, released.sequence);
        assert.isTrue(held.sequence < released.sequence);
        const completed = yield* pollFrozen(executing!);
        const afterRelease = yield* bounded(snapshot(), "released-snapshot");
        console.error("DG04_HOLD_RELEASE_RELEASED", JSON.stringify({
          releaseReceipt, afterRelease, workerExit: completed.exit?._tag ?? "Pending",
          schedulerRounds: completed.rounds, gate, overflow, maxSchedulerRounds,
        }));
        if (completed.exit === undefined) return yield* Effect.fail(new StopProgressInconclusive({
          stage: "hold-release-worker", schedulerRounds: completed.rounds,
        }));
        assert.isTrue(Exit.isSuccess(completed.exit));
        // Replaying the original accepted command and release must reuse receipts.
        const replayStart = yield* bounded(orchestrator.dispatch(startCommand), "start-replay");
        const replayRelease = yield* bounded(orchestrator.dispatch(releaseCommand), "release-replay");
        yield* bounded(worker.drain(), "replay-worker");
        const afterReplay = yield* bounded(snapshot(), "replay-snapshot");
        console.error("DG04_HOLD_RELEASE_REPLAY", JSON.stringify({
          replayStartSequence: replayStart.sequence, replayReleaseSequence: replayRelease.sequence,
          originalStartSequence: started.sequence, releaseSequence: released.sequence,
          afterReplay, gate, overflow,
        }));
        if (overflow || heldAtBound.overflow) return yield* Effect.fail(new StopProgressInconclusive({
          stage: "gate-observation-overflow", schedulerRounds: maxSchedulerRounds,
        }));
        // Actual continuation under hold disproves the contract, even without a refusal witness.
        assert.equal(heldAtBound.continuations, 0, "Preparation continued before actual hold release");
        assert.equal(whileHeld.continuations, 0, "Snapshot progress cannot bypass the held gate");
        if (heldAtBound.refusal === null) return yield* Effect.fail(new StopProgressInconclusive({
          stage: "held-gate-refusal-unobserved", schedulerRounds: heldAtBound.schedulerRounds,
        }));
        assert.deepEqual(afterHold.outbox, before.outbox);
        assert.deepEqual(whileHeld.outbox, before.outbox, "The same running effect and lease must remain");
        for (const state of [afterHold, whileHeld, afterRelease, afterReplay]) {
          assert.equal(state.clock, 0);
          assert.isFalse(state.interrupted);
          assert.equal(state.providerInterruptCalls, before.providerInterruptCalls);
          assert.deepEqual(state.controls, before.controls, "No Stop, interrupt or budget command may intervene");
          assert.deepEqual(state.admission, before.admission, "No second admission or budget reservation");
          for (const policy of state.policies.filter((p) => p.sequence >= initialPolicy.sequence)) {
            assert.deepEqual(budget(policy), budget(initialPolicy));
          }
          assert.deepEqual(state.runIdentity, before.runIdentity);
          assert.deepEqual(state.sessionIdentity, before.sessionIdentity);
          assert.equal(state.adapterEntries, before.adapterEntries);
        }
        for (const state of [afterRelease, afterReplay]) {
          assert.isNull(state.policies.at(-1)!.hold);
          assert.equal(state.continuations, 1);
          assert.equal(state.outbox.length, 1);
          assert.deepEqual(state.outbox.map(({ status, lease_owner, ...identity }) => identity),
            before.outbox.map(({ status, lease_owner, ...identity }) => identity));
          assert.equal(state.outbox[0]!.status, "succeeded");
        }
        assert.equal(replayStart.sequence, started.sequence);
        assert.equal(replayRelease.sequence, released.sequence);
        assert.deepEqual(afterReplay.policies, afterRelease.policies);
      }).pipe(Effect.ensuring(
        Effect.gen(function* () {
          const cleanup = yield* Effect.gen(function* () {
            yield* Deferred.succeed(preparationReady, undefined);
            if (executing !== undefined) yield* Fiber.interrupt(executing);
          }).pipe(Effect.forkScoped);
          const completed = yield* pollFrozen(cleanup);
          console.error("DG04_HOLD_RELEASE_CLEANUP", JSON.stringify({
            completed: completed.exit !== undefined && Exit.isSuccess(completed.exit),
            exit: completed.exit?._tag ?? "Pending", continuationsAfterCleanup: continuations,
          }));
          if (completed.exit === undefined || Exit.isFailure(completed.exit)) return yield* Effect.die(
            new StopProgressInconclusive({ stage: "hold-release-cleanup", schedulerRounds: maxSchedulerRounds }));
        }).pipe(Effect.orDie),
      )),
      {
        captureBaseline: Effect.void,
        checkpointRepositoryCheck: () => Effect.succeed(false),
        observeProviderDispatch: observeGate,
        beforeProviderCall: () => Effect.gen(function* () {
          yield* Deferred.succeed(entered, undefined);
          yield* Deferred.await(preparationReady).pipe(Effect.onInterrupt(() =>
            Deferred.succeed(interrupted, undefined).pipe(Effect.asVoid)));
          continuations += 1;
        }),
      },
    );
  }).pipe(Effect.onExit((exit) => Effect.sync(() => {
    const error = Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined;
    console.error("DG04_HOLD_RELEASE_VERDICT", JSON.stringify({
      status: Exit.isSuccess(exit) ? "PASS_SAME_ATTEMPT_HOLD_RELEASE"
        : error instanceof StopProgressInconclusive ? "INCONCLUSIVE_NOT_QUALIFIED" : "FAIL",
      stage: error instanceof StopProgressInconclusive ? error.stage : null,
      cause: Exit.isFailure(exit) ? Cause.pretty(exit.cause) : null,
    }));
  }))),
);

it.effect.each(["start", "steer", "descendant-start", "descendant-steer"] as const)(
  "F3 pending provider $0 permits Stop before release",
  (scenario) =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const cancelled = yield* Deferred.make<void>();
      const returned = yield* Deferred.make<void>();
      const type = scenario.endsWith("steer") ? "steer" : "start";
      const descendant = scenario.startsWith("descendant");
      let blockedThread: ThreadId | undefined;
      let stoppingFiber: Fiber.Fiber<unknown, unknown> | undefined;
      let executingFiber: Fiber.Fiber<unknown, unknown> | undefined;
      // A fixed work bound, not a latency target. Never advance time to judge progress.
      const maxSchedulerRounds = 256;
      const interruptPhases: Array<ProviderReplayHarness.InterruptPhaseObservation & { order: number }> = [];
      let interruptPhaseOverflow = false;
      const recordInterruptPhase = (observation: ProviderReplayHarness.InterruptPhaseObservation) => {
        if (interruptPhases.length === maxSchedulerRounds) {
          interruptPhaseOverflow = true;
          return;
        }
        interruptPhases.push({ order: interruptPhases.length + 1, ...observation });
      };
      const setupSpans: Array<{
        name: string;
        spanId: string;
        parentSpanId: string | null;
        startOrder: number;
        startTime: string;
        endOrder: number | null;
        endTime: string | null;
        exit: "Success" | "Failure" | null;
      }> = [];
      let setupSpanSequence = 0;
      let setupSpanOverflow = false;
      const setupTracer = Tracer.make({
        span(options) {
          const span = new Tracer.NativeSpan(options);
          if (setupSpans.length === maxSchedulerRounds) {
            setupSpanOverflow = true;
            return span;
          }
          const observation: (typeof setupSpans)[number] = {
            name: span.name,
            spanId: span.spanId,
            parentSpanId: Option.getOrUndefined(span.parent)?.spanId ?? null,
            startOrder: ++setupSpanSequence,
            startTime: options.startTime.toString(),
            endOrder: null,
            endTime: null,
            exit: null,
          };
          setupSpans.push(observation);
          const end = span.end.bind(span);
          span.end = (endTime, exit) => {
            end(endTime, exit);
            if (observation.endOrder !== null) return;
            observation.endOrder = ++setupSpanSequence;
            observation.endTime = endTime.toString();
            observation.exit = exit._tag;
          };
          return span;
        },
      });
      const setupProcessPhases: Array<{
        order: number;
        callId: number;
        phase: "spawn-entry" | "spawn-return" | "spawn-failure" | "exit-wait" | "exit-result";
        pid: number | null;
        exit: "Success" | "Failure" | null;
        code: number | null;
      }> = [];
      let setupProcessOverflow = false;
      let setupProcessCallId = 0;
      const recordProcessPhase = (phase: Omit<(typeof setupProcessPhases)[number], "order">) => {
        if (setupProcessPhases.length === maxSchedulerRounds) {
          setupProcessOverflow = true;
          return;
        }
        setupProcessPhases.push({ order: setupProcessPhases.length + 1, ...phase });
      };
      let inspectSetupExhaustion:
        | ((workerFiber: Fiber.Fiber<unknown, unknown>) => Effect.Effect<void>)
        | undefined;
      let inspectWorkerCancellationExhaustion:
        | ((workerFiber: Fiber.Fiber<unknown, unknown>) => Effect.Effect<void>)
        | undefined;
      const pollFrozen = <A, E>(fiber: Fiber.Fiber<A, E>) =>
        Effect.gen(function* () {
          for (let rounds = 0; rounds <= maxSchedulerRounds; rounds++) {
            const exit = yield* Effect.sync(() => fiber.pollUnsafe());
            if (exit !== undefined) return { exit, rounds };
            if (rounds < maxSchedulerRounds) yield* Effect.yieldNow;
          }
          return { exit: undefined, rounds: maxSchedulerRounds };
        });
      const finishWithinBound = <A, E>(fiber: Fiber.Fiber<A, E>, stage: string) =>
        Effect.gen(function* () {
          const progress = yield* pollFrozen(fiber);
          if (progress.exit === undefined) {
            if (stage === "setup-worker" && inspectSetupExhaustion !== undefined) {
              yield* inspectSetupExhaustion(fiber);
            }
            if (stage === "worker-cancellation" && inspectWorkerCancellationExhaustion !== undefined) {
              yield* inspectWorkerCancellationExhaustion(fiber);
            }
            return yield* Effect.fail(
              new StopProgressInconclusive({ stage, schedulerRounds: progress.rounds }),
            );
          }
          return yield* progress.exit;
        });
      const runWithinBound = <A, E, R>(work: Effect.Effect<A, E, R>, stage: string) =>
        Effect.gen(function* () {
          const fiber = yield* work.pipe(Effect.forkScoped);
          return yield* finishWithinBound(fiber, stage);
        });
      return yield* withHarness(
        `pending-${scenario}`,
        ({ orchestrator, worker, stub, threadId, sql, executor, setupSpawner }) =>
          Effect.gen(function* () {
            const setupInspector = (
              observedThreadId: ThreadId,
              runningFiber: Fiber.Fiber<unknown, unknown>,
            ) => (workerFiber: Fiber.Fiber<unknown, unknown>) =>
              Effect.gen(function* () {
                const clockAtBound = DateTime.toEpochMillis(yield* DateTime.now);
                const workerAtBound = yield* Effect.sync(() => workerFiber.pollUnsafe());
                const runningAtBound = yield* Effect.sync(() => runningFiber.pollUnsafe());
                const startsAtBound = stub.started.length;
                // Copy before the snapshot reads can let the worker progress.
                // Retain only span identity/lifecycle, never attributes or causes.
                const processPhasesAtBound = setupProcessPhases.map((phase) => ({ ...phase }));
                const processObservation = {
                  status: setupProcessOverflow || processPhasesAtBound.length === 0
                    ? "UNAVAILABLE_NOT_QUALIFIED"
                    : "OBSERVED",
                  overflow: setupProcessOverflow,
                  cap: maxSchedulerRounds,
                  phases: processPhasesAtBound,
                };
                const schedulerObservation = yield* Effect.withFiber((pollingFiber) => Effect.succeed({
                  pollingExecutionMode: pollingFiber.cache.scheduler.executionMode,
                  workerExecutionMode: workerFiber.cache.scheduler.executionMode,
                  pollingSchedulerClass: pollingFiber.cache.scheduler.constructor.name,
                  workerSchedulerClass: workerFiber.cache.scheduler.constructor.name,
                  nodeSetImmediateAvailable: typeof globalThis.setImmediate === "function",
                  processCallbackOpportunity: "SOURCE_ONLY_NOT_CALLBACK_OBSERVATION",
                }));
                const spansAtBound = setupSpans.map((span) => ({ ...span }));
                const activeSpans = spansAtBound.filter((span) => span.endOrder === null);
                const completedSpans = spansAtBound
                  .filter((span) => span.endOrder !== null)
                  .sort((left, right) => left.endOrder! - right.endOrder!);
                const spanObservation = {
                  status: setupSpanOverflow || activeSpans.length === 0
                    ? "UNAVAILABLE_NOT_QUALIFIED"
                    : "OBSERVED",
                  reason: setupSpanOverflow
                    ? "span-cap-exhausted"
                    : activeSpans.length === 0
                      ? "no-active-span"
                      : null,
                  cap: maxSchedulerRounds,
                  overflow: setupSpanOverflow,
                  sequence: setupSpanSequence,
                  activeSpans,
                  completedSpans,
                };
                const snapshot = yield* Effect.exit(runWithinBound(
                  Effect.gen(function* () {
                    const outbox = yield* sql<{
                      effect_id: string;
                      effect_type: string;
                      status: string;
                      attempt_count: number;
                      last_error: string | null;
                    }>`
                      SELECT effect_id, effect_type, status, attempt_count, last_error
                      FROM orchestration_v2_effect_outbox
                      WHERE thread_id = ${observedThreadId}
                      ORDER BY created_at, effect_id
                    `;
                    const projection = yield* orchestrator.getThreadProjection(observedThreadId);
                    return {
                      outbox,
                      runs: projection.runs.map((run) => ({
                        id: run.id,
                        status: run.status,
                        activeAttemptId: run.activeAttemptId,
                      })),
                      providerThreads: projection.providerThreads.map((thread) => ({
                        id: thread.id,
                        status: thread.status,
                        providerSessionId: thread.providerSessionId,
                      })),
                      providerTurns: projection.providerTurns.map((turn) => ({
                        id: turn.id,
                        status: turn.status,
                        runAttemptId: turn.runAttemptId,
                      })),
                      activeKeys: yield* executor.activeKeys,
                    };
                  }),
                  "setup-snapshot",
                ));
                const clockAfterSnapshot = DateTime.toEpochMillis(yield* DateTime.now);
                const runningAfterSnapshot = yield* Effect.sync(() => runningFiber.pollUnsafe());
                const workerAfterSnapshot = yield* Effect.sync(() => workerFiber.pollUnsafe());
                console.error("DG04_SETUP_WORKER_SNAPSHOT", JSON.stringify({
                  scenario,
                  stage: "setup-worker",
                  threadId: observedThreadId,
                  schedulerRounds: maxSchedulerRounds,
                  snapshotSchedulerBound: maxSchedulerRounds,
                  spanObservation,
                  processObservation,
                  schedulerObservation,
                  snapshotStatus: Exit.isSuccess(snapshot) ? "OBSERVED" : "UNAVAILABLE_NOT_QUALIFIED",
                  snapshotCause: Exit.isFailure(snapshot) ? Cause.pretty(snapshot.cause) : null,
                  clockAtBound,
                  clockAfterSnapshot,
                  clockPreserved: clockAtBound === clockAfterSnapshot,
                  stubStartedAtBound: startsAtBound,
                  stubStartedAfterSnapshot: stub.started.length,
                  workerFiberId: workerFiber.id,
                  workerExitAtBound: workerAtBound?._tag ?? "Pending",
                  workerExitAfterSnapshot: workerAfterSnapshot?._tag ?? "Pending",
                  runningEventFiberId: runningFiber.id,
                  runningEventExitAtBound: runningAtBound?._tag ?? "Pending",
                  runningEventExitAfterSnapshot: runningAfterSnapshot?._tag ?? "Pending",
                  snapshot: Exit.isSuccess(snapshot) ? snapshot.value : null,
                }));
              });
            let target = threadId;
            if (descendant || type === "steer") {
              const running = yield* waitForRunning(orchestrator);
              yield* orchestrator.dispatch(messageCommand(threadId, `${scenario}-first`));
              inspectSetupExhaustion = setupInspector(threadId, running);
              if (scenario === "descendant-start" && setupSpawner !== undefined) {
                // Observe the real memoized fixture service only during setup.
                // Forward every command, stream and scope; restore on every exit.
                yield* Effect.acquireUseRelease(
                  Effect.sync(() => {
                    const originalSpawn = setupSpawner.spawn;
                    setupSpawner.spawn = (command) => Effect.suspend(() => {
                      const callId = ++setupProcessCallId;
                      recordProcessPhase({ callId, phase: "spawn-entry", pid: null, exit: null, code: null });
                      return originalSpawn.call(setupSpawner, command).pipe(
                        Effect.tapError(() => Effect.sync(() => {
                          recordProcessPhase({ callId, phase: "spawn-failure", pid: null, exit: "Failure", code: null });
                        })),
                        Effect.map((child) => {
                          const pid = Number(child.pid);
                          recordProcessPhase({ callId, phase: "spawn-return", pid, exit: "Success", code: null });
                          return ChildProcessSpawner.makeHandle({
                            ...child,
                            exitCode: Effect.suspend(() => {
                              recordProcessPhase({ callId, phase: "exit-wait", pid, exit: null, code: null });
                              return child.exitCode.pipe(Effect.onExit((exit) => Effect.sync(() => {
                                recordProcessPhase({
                                  callId, phase: "exit-result", pid, exit: exit._tag,
                                  code: Exit.isSuccess(exit) ? Number(exit.value) : null,
                                });
                              })));
                            }),
                          });
                        }),
                      );
                    });
                    return originalSpawn;
                  }),
                  () => runWithinBound(worker.drain().pipe(Effect.withTracer(setupTracer)), "setup-worker"),
                  (originalSpawn) => Effect.sync(() => { setupSpawner.spawn = originalSpawn; }),
                );
              } else {
                yield* runWithinBound(worker.drain(), "setup-worker");
              }
              yield* finishWithinBound(running, "setup-running-event");
            }
            if (descendant) {
              const parent = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
              yield* orchestrator.dispatch({
                type: "delegated_task.request",
                commandId: CommandId.make(`${scenario}-child`),
                parentThreadId: threadId,
                parentRunId: parent.id,
                parentNodeId: parent.rootNodeId!,
                task: "inert pending provider child",
                modelSelection,
                runtimeMode: "full-access",
                interactionMode: "default",
                createdBy: "agent",
                creationSource: "mcp",
              });
              target = (yield* orchestrator.getThreadProjection(threadId)).subagents[0]!
                .childThreadId!;
              if (type === "steer") {
                const running = yield* waitForRunning(orchestrator);
                inspectSetupExhaustion = setupInspector(target, running);
                yield* runWithinBound(worker.drain(), "setup-worker");
                yield* finishWithinBound(running, "setup-running-event");
              }
            }
            blockedThread = target;
            if (type === "steer") {
              const run = (yield* orchestrator.getThreadProjection(target)).runs[0]!;
              yield* orchestrator.dispatch({
                ...messageCommand(target, `${scenario}-pending`),
                dispatchMode: { type: "steer_active", targetRunId: run.id },
              });
            } else if (!descendant) {
              yield* orchestrator.dispatch(messageCommand(target, `${scenario}-pending`));
            }
            const executing = yield* worker.drain().pipe(Effect.forkScoped);
            executingFiber = executing;
            yield* runWithinBound(Deferred.await(entered), "provider-entered");
            const pending = yield* runWithinBound(
              sql<{ effect_id: string; status: string }>`
                SELECT effect_id, status FROM orchestration_v2_effect_outbox
                WHERE thread_id = ${target}
                  AND effect_type = ${`provider-turn.${type}`} AND status = 'running'
              `,
              "claimed-effect-read",
            );
            assert.equal(pending.length, 1, "The exact pending provider effect must be claimed");
            const pendingEffectId = pending[0]!.effect_id;
            const frozenTime = DateTime.toEpochMillis(yield* DateTime.now);
            if (scenario === "steer") {
              inspectWorkerCancellationExhaustion = (workerFiber) => Effect.gen(function* () {
                const interruptPhasesAtBound = interruptPhases.slice();
                const interruptOverflowAtBound = interruptPhaseOverflow;
                const workerAtBound = yield* Effect.sync(() => workerFiber.pollUnsafe());
                const clockAtBound = DateTime.toEpochMillis(yield* DateTime.now);
                const interruptsAtBound = stub.interrupts.map((interrupt) => ({
                  providerThreadId: interrupt.providerThread.id,
                  providerTurnId: interrupt.providerTurnId,
                }));
                const snapshot = yield* Effect.exit(runWithinBound(
                  Effect.gen(function* () {
                    const outbox = yield* sql<{
                      effect_id: string;
                      effect_type: string;
                      status: string;
                      attempt_count: number;
                      lease_owner: string | null;
                    }>`
                      SELECT effect_id, effect_type, status, attempt_count, lease_owner
                      FROM orchestration_v2_effect_outbox WHERE thread_id = ${target}
                      ORDER BY effect_id LIMIT ${maxSchedulerRounds + 1}
                    `;
                    const projection = yield* orchestrator.getThreadProjection(target);
                    const providerTurns = projection.providerTurns.map((turn) => ({
                      id: turn.id,
                      providerThreadId: turn.providerThreadId,
                      runAttemptId: turn.runAttemptId,
                      status: turn.status,
                    }));
                    const attempts = projection.attempts.map((attempt) => ({
                      id: attempt.id,
                      providerTurnId: attempt.providerTurnId,
                      status: attempt.status,
                    }));
                    const interruptSettlementReceipts = yield* sql<{
                      command_id: string;
                      status: string;
                      result_sequence: number;
                    }>`
                      SELECT command_id, status, result_sequence FROM orchestration_command_receipts
                      WHERE command_id IN (
                        SELECT effect_id || ':background-work-settled'
                        FROM orchestration_v2_effect_outbox
                        WHERE thread_id = ${target} AND effect_type = 'provider-turn.interrupt'
                      ) LIMIT ${maxSchedulerRounds + 1}
                    `;
                    const overflow = [outbox, providerTurns, attempts, interruptsAtBound, interruptSettlementReceipts]
                      .some((rows) => rows.length > maxSchedulerRounds);
                    return overflow
                      ? { status: "UNAVAILABLE_NOT_QUALIFIED" as const, overflow: true }
                      : { status: "OBSERVED" as const, overflow: false, outbox, providerTurns, attempts, interruptSettlementReceipts };
                  }),
                  "worker-cancellation-observation",
                ));
                console.error("DG04_WORKER_CANCELLATION_SNAPSHOT", JSON.stringify({
                  scenario,
                  pendingEffectId,
                  maxSchedulerRounds,
                  exhaustedSchedulerRounds: maxSchedulerRounds,
                  workerExitAtBound: workerAtBound?._tag ?? "Pending",
                  workerExitAfterSnapshot: (yield* Effect.sync(() => workerFiber.pollUnsafe()))?._tag ?? "Pending",
                  clockAtBound,
                  clockAfterSnapshot: DateTime.toEpochMillis(yield* DateTime.now),
                  frozenTime,
                  interruptsAtBound: interruptsAtBound.length <= maxSchedulerRounds ? interruptsAtBound : null,
                  interruptOverflow: interruptsAtBound.length > maxSchedulerRounds,
                  interruptObservation: {
                    status: interruptOverflowAtBound || interruptPhaseOverflow || interruptPhasesAtBound.length === 0
                      ? "UNAVAILABLE_NOT_QUALIFIED" : "OBSERVED",
                    overflowAtBound: interruptOverflowAtBound,
                    overflowAfterSnapshot: interruptPhaseOverflow,
                    atBound: interruptPhasesAtBound,
                    afterSnapshot: interruptPhases.slice(),
                  },
                  snapshot: Exit.isSuccess(snapshot) ? snapshot.value : { status: "UNAVAILABLE_NOT_QUALIFIED" },
                  snapshotExit: snapshot._tag,
                }));
              });
            }
            const starts = stub.started.length;
            const steers = stub.steered.length;
            const stopCommand = {
              type: "thread.stop" as const,
              commandId: CommandId.make(`${scenario}-stop`),
              threadId: target,
            };
            let stopPhase = "root-hold";
            let rootHoldSequence: number | null = null;
            const stopping = yield* Effect.gen(function* () {
              if (descendant) {
                // The child's suspended call must not block controls on its root.
                const held = yield* orchestrator.dispatch({
                  type: "goal.hold",
                  commandId: CommandId.make(`${scenario}-root-hold`),
                  threadId,
                  hold: "stop-pending-descendant",
                });
                rootHoldSequence = held.sequence;
                const accepted = yield* sql<{ status: string; result_sequence: number }>`
                  SELECT status, result_sequence FROM orchestration_command_receipts
                  WHERE command_id = ${CommandId.make(`${scenario}-root-hold`)}
                `;
                assert.equal(accepted.length, 1);
                assert.equal(accepted[0]!.status, "accepted");
                assert.equal(accepted[0]!.result_sequence, held.sequence);
              }
              stopPhase = "child-stop";
              const result = yield* orchestrator.dispatch(stopCommand);
              stopPhase = "completed";
              return result;
            }).pipe(Effect.forkScoped);
            stoppingFiber = stopping;
            const progress = yield* pollFrozen(stopping);
            const phaseAtBound = stopPhase;
            const rootHoldSequenceAtBound = rootHoldSequence;
            // Capture durable state before any cleanup interruption or provider release.
            const snapshot = yield* runWithinBound(
              Effect.gen(function* () {
                const receipt = yield* sql<{
                  command_id: string;
                  aggregate_id: string;
                  command_type: string;
                  status: string;
                  error: string | null;
                  result_sequence: number;
                }>`
                  SELECT command_id, aggregate_id, command_type, status, error, result_sequence
                  FROM orchestration_command_receipts WHERE command_id = ${stopCommand.commandId}
                `;
                const rootReceipt = yield* sql<{ status: string; result_sequence: number }>`
                  SELECT status, result_sequence FROM orchestration_command_receipts
                  WHERE command_id = ${CommandId.make(`${scenario}-root-hold`)}
                `;
                const outbox = yield* sql<{ effect_id: string; status: string }>`
                  SELECT effect_id, status FROM orchestration_v2_effect_outbox
                  WHERE effect_id = ${pendingEffectId}
                `;
                return {
                  receipt,
                  rootReceipt,
                  outbox,
                  callbackInterrupted: yield* Deferred.isDone(cancelled),
                  callbackReturned: yield* Deferred.isDone(returned),
                  activeKeys: yield* executor.activeKeys,
                  providerReleased: yield* Deferred.isDone(release),
                  clock: DateTime.toEpochMillis(yield* DateTime.now),
                };
              }),
              "pre-cleanup-snapshot",
            );
            console.error(
              "DG04_STOP_PROGRESS",
              JSON.stringify({
                scenario,
                phase: phaseAtBound,
                phaseAfterSnapshot: stopPhase,
                pendingEffectId,
                rootHoldSequenceAtBound,
                exit: progress.exit?._tag ?? "Pending",
                cause: progress.exit !== undefined && Exit.isFailure(progress.exit)
                  ? Cause.pretty(progress.exit.cause)
                  : null,
                rootHoldSequence,
                stopSequence: progress.exit !== undefined && Exit.isSuccess(progress.exit)
                  ? progress.exit.value.sequence
                  : null,
                schedulerRounds: progress.rounds,
                maxSchedulerRounds,
                frozenTime,
                ...snapshot,
              }),
            );
            assert.equal(snapshot.clock, frozenTime, "The progress judge must keep time frozen");
            assert.isFalse(snapshot.providerReleased);
            if (progress.exit === undefined) {
              return yield* Effect.fail(
                new StopProgressInconclusive({ stage: phaseAtBound, schedulerRounds: progress.rounds }),
              );
            }
            const stopped = progress.exit;
            assert.isTrue(Exit.isSuccess(stopped), "Stop must commit before provider release");
            assert.isFalse(yield* Deferred.isDone(release));
            assert.equal(snapshot.receipt.length, 1, "Stop must have its durable accepted receipt");
            assert.equal(snapshot.receipt[0]!.command_id, stopCommand.commandId);
            assert.equal(snapshot.receipt[0]!.aggregate_id, target);
            assert.equal(snapshot.receipt[0]!.command_type, "thread.stop");
            assert.equal(snapshot.receipt[0]!.status, "accepted");
            if (Exit.isSuccess(stopped)) {
              assert.equal(snapshot.receipt[0]!.result_sequence, stopped.value.sequence);
            }
            assert.deepEqual(snapshot.outbox, [{ effect_id: pendingEffectId, status: "cancelled" }]);
            const rows = yield* runWithinBound(sql<{ status: string }>`
              SELECT status FROM orchestration_v2_effect_outbox
              WHERE thread_id = ${target}
                AND effect_type = ${`provider-turn.${type}`}
                AND status = 'cancelled'
            `, "cancelled-effect-read");
            assert.equal(rows.length, 1, "The pending provider effect must be durably cancelled");
            yield* finishWithinBound(executing, "worker-cancellation");
            assert.isTrue(yield* Deferred.isDone(cancelled));
            assert.isFalse(yield* Deferred.isDone(returned));
            // Replaying the same Stop receipt must not create another provider call.
            yield* runWithinBound(orchestrator.dispatch(stopCommand), "stop-replay");
            yield* runWithinBound(worker.drain(), "replay-worker");
            assert.equal(stub.started.length, starts);
            assert.equal(stub.steered.length, steers);
            const projection = yield* runWithinBound(
              orchestrator.getThreadProjection(target), "terminal-read",
            );
            assert.equal(projection.runs.at(-1)?.status, "interrupted");
          }).pipe(
            Effect.ensuring(
              Effect.gen(function* () {
                const cleanup = yield* Effect.gen(function* () {
                  yield* Deferred.succeed(release, undefined);
                  if (stoppingFiber !== undefined) yield* Fiber.interrupt(stoppingFiber);
                  if (executingFiber !== undefined) yield* Fiber.interrupt(executingFiber);
                }).pipe(Effect.timeout("1 second"), Effect.forkScoped);
                // First permit finite runnable cleanup; time advances only if interruption needs it.
                let cleaned = yield* pollFrozen(cleanup);
                if (cleaned.exit === undefined) {
                  yield* TestClock.adjust("1 second");
                  cleaned = yield* pollFrozen(cleanup);
                }
                console.error("DG04_STOP_CLEANUP", JSON.stringify({
                  completed: cleaned.exit !== undefined && Exit.isSuccess(cleaned.exit),
                  exit: cleaned.exit?._tag ?? "Pending",
                }));
                if (cleaned.exit === undefined || Exit.isFailure(cleaned.exit)) {
                  return yield* Effect.die(
                    new StopProgressInconclusive({ stage: "cleanup", schedulerRounds: maxSchedulerRounds }),
                  );
                }
              }).pipe(Effect.orDie),
            ),
            Effect.onExit((exit) => Effect.sync(() => {
              const error = Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined;
              console.error("DG04_STOP_VERDICT", JSON.stringify({
                scenario,
                status: Exit.isSuccess(exit)
                  ? "PASS_THIS_INTERLEAVING"
                  : error instanceof StopProgressInconclusive
                    ? "INCONCLUSIVE_NOT_QUALIFIED"
                    : "FAIL",
                tag: error instanceof StopProgressInconclusive ? error._tag : null,
                stage: error instanceof StopProgressInconclusive ? error.stage : null,
                cause: Exit.isFailure(exit) ? Cause.pretty(exit.cause) : null,
              }));
            })),
          ),
        {
          captureBaseline: Effect.void,
          // Exercise the real missing-checkpoint metadata path without filesystem storage.
          checkpointRepositoryCheck: () => Effect.succeed(false),
          emitTerminalOnInterrupt: true,
          acknowledgeInterruptIngestion: true,
          ...(scenario === "steer" ? { observeInterrupt: recordInterruptPhase } : {}),
          inspectSetupSpawner: scenario === "descendant-start",
          beforeProviderCall: (callType, target) =>
            target !== blockedThread || callType !== type
              ? Effect.void
              : Effect.gen(function* () {
                  yield* Deferred.succeed(entered, undefined);
                  yield* Deferred.await(release).pipe(
                    Effect.onInterrupt(() =>
                      Deferred.succeed(cancelled, undefined).pipe(Effect.asVoid),
                    ),
                  );
                  yield* Deferred.succeed(returned, undefined);
                }),
        },
      );
    }),
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

// F2-send: Stop alone is still the negative control; only an explicit goal hold refuses.
it.effect("F2-send goal hold refuses a stopped idle thread and Resume starts work", () =>
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
      // No goal hold: a new send really reaches the provider.
      yield* send("control-after-stop", "start_immediately");
      yield* worker.drain();
      assert.equal((yield* observe("F2-send-no-hold")).stubStarts, 2);
      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop-control-f2s"),
        threadId,
      });
      yield* worker.drain();
      yield* orchestrator.dispatch({
        type: "goal.hold",
        commandId: CommandId.make("hold-f2s"),
        threadId,
        hold: "leo-stop",
      });
      const refused = yield* Effect.result(
        orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("held-send-f2s"),
          threadId,
          messageId: MessageId.make("held-message-f2s"),
          text: "resume please",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        }),
      );
      assert.equal(refused._tag, "Failure");
      if (refused._tag === "Failure") assert.equal(refused.failure._tag, "GoalDispatchHeldError");
      yield* worker.drain();
      const after = yield* observe("F2-send-held");
      report("PASS-goal-hold-refuses-send", after);
      assert.equal(after.stubStarts, 2);
      yield* orchestrator.dispatch({
        type: "goal.resume",
        commandId: CommandId.make("resume-f2s"),
        threadId,
      });
      yield* send("after-explicit-resume", "start_immediately");
      yield* worker.drain();
      assert.equal((yield* observe("F2-send-resumed")).stubStarts, 3);
    }),
  ),
);

// F1 (doc-only flag): a plan or chat that says "stopped" changes nothing in the
// runtime. With no hold set, a queued follow-up starts once the active turn ends,
// so the harness must report that the flag alone did not stop it.
it.effect("F1 without a hold the queued follow-up starts when the active turn completes", () =>
  withHarness("stop-f1", ({ orchestrator, worker, stub, threadId, observe, send }) =>
    Effect.gen(function* () {
      const running = yield* waitForRunning(orchestrator);
      yield* send("first", "start_immediately");
      yield* worker.drain();
      yield* Fiber.join(running);
      yield* send("follow-up", "queue_after_active");
      const before = yield* observe("F1-queued-before-release");
      assert.equal(before.runStatuses.at(-1), "queued");
      const turn = stub.started[0]!;
      const now = yield* DateTime.now;
      const ended = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event: OrchestrationV2DomainEvent) =>
            event.type === "run.updated" &&
            event.payload.status !== "running" &&
            event.payload.status !== "queued" &&
            event.payload.status !== "starting",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      const projectionBefore = yield* orchestrator.getThreadProjection(threadId);
      const providerTurn = projectionBefore.providerTurns[0]!;
      yield* Queue.offer(stub.events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: { ...providerTurn, status: "completed", completedAt: now },
      });
      yield* Queue.offer(stub.events, {
        type: "turn.terminal",
        driver,
        providerThreadId: providerTurn.providerThreadId,
        providerTurnId: providerTurn.id,
        runOrdinal: turn.runOrdinal,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      yield* Fiber.join(ended);
      yield* worker.drain();
      yield* orchestrator.resumeQueuedRuns;
      yield* worker.drain();
      const after = yield* observe("F1-after-release");
      const released = after.stubStarts === 2;
      report(released ? "PASS-doc-only-flag-does-not-stop-the-queued-turn" : "UNEXPECTED", after);
      assert.ok(released, JSON.stringify(after));
      // Negative control: before the release the stub has started one turn only.
      assert.equal(before.stubStarts, 1);
    }),
  ),
);

// F5: restart while stopped. Runtime A stops a thread that has a queued follow-up,
// then runtime B starts over the same stored data with startup recovery. Nothing
// may start until an explicit resume; the explicit resume is the negative control
// that shows the hold, not the absence of work, kept the stub idle.
it.effect("F5 restart while stopped starts nothing until an explicit resume", () => {
  const databasePath = NodePath.join(
    NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "dg03-f5-")),
    "orchestration.sqlite",
  );
  return Effect.scoped(
    Effect.gen(function* () {
      const workspacePath = yield* checkpointWorkspace("stop-f5-shared");
      const beforeRestart = yield* withHarness(
        "stop-f5",
        ({ orchestrator, worker, threadId, observe, send }) =>
          Effect.gen(function* () {
            const running = yield* waitForRunning(orchestrator);
            yield* send("first", "start_immediately");
            yield* worker.drain();
            yield* Fiber.join(running);
            yield* send("follow-up", "queue_after_active");
            yield* orchestrator.dispatch({
              type: "thread.stop",
              commandId: CommandId.make("stop-f5"),
              threadId,
            });
            yield* worker.drain();
            return yield* observe("F5-runtime-A-after-stop");
          }),
        { databasePath, workspacePath, layerName: "stop-f5-a" },
      );
      assert.ok(heldQueueCheck(beforeRestart, 1), JSON.stringify(beforeRestart));
      yield* withHarness(
        "stop-f5",
        ({ orchestrator, worker, threadId, observe }) =>
          Effect.gen(function* () {
            yield* worker.drain();
            const restarted = yield* observe("F5-runtime-B-after-restart");
            report(heldQueueCheck(restarted, 0) ? "PASS" : "FAIL", restarted);
            assert.ok(heldQueueCheck(restarted, 0), JSON.stringify(restarted));
            assert.deepEqual(restarted.runStatuses, beforeRestart.runStatuses);
            // Negative control: the explicit queue.resume command releases the held run.
            // (resumeQueuedRuns alone does not: it leaves a held run held.)
            const running = yield* waitForRunning(orchestrator);
            yield* orchestrator.dispatch({
              type: "queue.resume",
              commandId: CommandId.make("resume-f5"),
              threadId,
            });
            yield* worker.drain();
            yield* Fiber.join(running);
            const resumed = yield* observe("F5-runtime-B-after-explicit-resume");
            // The persisted workspace survives both runtimes. Clearing the hold alone
            // is insufficient: the queued turn must actually reach the provider.
            report("PASS-resume-releases-and-starts", resumed);
            assert.equal(resumed.queueHeld.at(-1), false, JSON.stringify(resumed));
            assert.equal(resumed.runStatuses.at(-1), "running", JSON.stringify(resumed));
            assert.equal(resumed.stubStarts, 1, JSON.stringify(resumed));
          }),
        {
          databasePath,
          workspacePath,
          layerName: "stop-f5-b",
          existingThread: true,
          recoverOnStartup: true,
        },
      );
    }),
  );
});

const messageCommand = (threadId: ThreadId, id: string) => ({
  type: "message.dispatch" as const,
  commandId: CommandId.make(id),
  threadId,
  messageId: MessageId.make(`message:${id}`),
  text: id,
  attachments: [],
  dispatchMode: { type: "start_immediately" as const },
  createdBy: "user" as const,
  creationSource: "web" as const,
});

it.effect("F2-delegate root hold covers existing and future delegated descendants", () =>
  withHarness("goal-delegate", ({ orchestrator, worker, stub, threadId, send }) =>
    Effect.gen(function* () {
      const running = yield* waitForRunning(orchestrator);
      yield* send("parent", "start_immediately");
      yield* worker.drain();
      yield* Fiber.join(running);
      const parent = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
      const delegate = (id: string) =>
        orchestrator.dispatch({
          type: "delegated_task.request",
          commandId: CommandId.make(id),
          parentThreadId: threadId,
          parentRunId: parent.id,
          parentNodeId: parent.rootNodeId!,
          task: "bounded stub child",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          createdBy: "agent",
          creationSource: "mcp",
        });
      yield* orchestrator.dispatch({
        type: "goal.hold",
        commandId: CommandId.make("hold-delegate"),
        threadId,
        hold: "tree-stop",
      });
      const refused = yield* Effect.result(delegate("held-delegate"));
      assert.equal(refused._tag, "Failure");
      if (refused._tag === "Failure") assert.equal(refused.failure._tag, "GoalDispatchHeldError");
      yield* worker.drain();
      assert.equal(stub.started.length, 1);
      yield* orchestrator.dispatch({
        type: "goal.resume",
        commandId: CommandId.make("resume-delegate"),
        threadId,
      });
      yield* delegate("control-delegate");
      yield* worker.drain();
      assert.equal(stub.started.length, 2);
      const child = (yield* orchestrator.getThreadProjection(threadId)).subagents[0]!
        .childThreadId!;
      yield* orchestrator.dispatch({
        type: "goal.hold",
        commandId: CommandId.make("hold-descendant"),
        threadId,
        hold: "ancestor-stop",
      });
      const childSend = yield* Effect.result(
        orchestrator.dispatch(messageCommand(child, "held-child-send")),
      );
      assert.equal(childSend._tag, "Failure");
      if (childSend._tag === "Failure") {
        assert.equal(childSend.failure._tag, "GoalDispatchHeldError");
        if (childSend.failure._tag === "GoalDispatchHeldError")
          assert.equal(childSend.failure.goalThreadId, threadId);
      }
      yield* worker.drain();
      assert.equal(stub.started.length, 2);
    }),
  ),
);

it.effect("F4 a hold queued before dispatch wins the root dispatch lock race", () =>
  withHarness("goal-race", ({ orchestrator, worker, stub, threadId, executor, send }) =>
    Effect.gen(function* () {
      const running = yield* waitForRunning(orchestrator);
      yield* send("race-parent", "start_immediately");
      yield* worker.drain();
      yield* Fiber.join(running);
      const parent = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
      yield* orchestrator.dispatch({
        type: "delegated_task.request",
        commandId: CommandId.make("race-child"),
        parentThreadId: threadId,
        parentRunId: parent.id,
        parentNodeId: parent.rootNodeId!,
        task: "race child",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        createdBy: "agent",
        creationSource: "mcp",
      });
      yield* worker.drain();
      const child = (yield* orchestrator.getThreadProjection(threadId)).subagents[0]!
        .childThreadId!;
      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("race-child-stop"),
        threadId: child,
      });
      yield* worker.drain();
      assert.equal(stub.started.length, 2);
      const acquired = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const gate = yield* executor
        .withLock(
          threadId,
          Deferred.succeed(acquired, undefined).pipe(Effect.andThen(Deferred.await(release))),
        )
        .pipe(Effect.forkScoped);
      yield* Deferred.await(acquired);
      const hold = yield* orchestrator
        .dispatch({
          type: "goal.hold",
          commandId: CommandId.make("race-hold"),
          threadId,
          hold: "race-stop",
        })
        .pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      const pendingSend = yield* Effect.result(
        orchestrator.dispatch(messageCommand(child, "race-send")),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(gate);
      yield* Fiber.join(hold);
      const result = yield* Fiber.join(pendingSend);
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") assert.equal(result.failure._tag, "GoalDispatchHeldError");
      yield* worker.drain();
      assert.equal(stub.started.length, 2);
      yield* orchestrator.dispatch({
        type: "goal.resume",
        commandId: CommandId.make("race-resume"),
        threadId,
      });
      yield* orchestrator.dispatch(messageCommand(child, "race-control"));
      yield* worker.drain();
      assert.equal(stub.started.length, 3);
    }),
  ),
);

it.effect(
  "B1-B2 budget counts committed requests and refuses children and failed-run retries",
  () =>
    withHarness("goal-budget", ({ orchestrator, worker, stub, threadId, send }) =>
      Effect.gen(function* () {
        yield* orchestrator.dispatch({
          type: "goal.budget.set",
          commandId: CommandId.make("budget-1"),
          threadId,
          limit: 1,
        });
        const running = yield* waitForRunning(orchestrator);
        yield* send("budget-first", "start_immediately");
        yield* worker.drain();
        yield* Fiber.join(running);
        assert.equal(stub.started.length, 1);
        // Receipt replay must not count a second request.
        yield* send("budget-first", "start_immediately");
        const parent = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
        const child = yield* Effect.result(
          orchestrator.dispatch({
            type: "delegated_task.request",
            commandId: CommandId.make("budget-child"),
            parentThreadId: threadId,
            parentRunId: parent.id,
            parentNodeId: parent.rootNodeId!,
            task: "budget child",
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            createdBy: "agent",
            creationSource: "mcp",
          }),
        );
        assert.equal(child._tag, "Failure");
        if (child._tag === "Failure") assert.equal(child.failure._tag, "GoalDispatchBudgetError");
        yield* endStubTurn(orchestrator, stub, threadId, "failed");
        assert.equal((yield* orchestrator.getThreadProjection(threadId)).runs[0]!.status, "failed");
        const second = yield* Effect.result(
          orchestrator.dispatch(messageCommand(threadId, "budget-second")),
        );
        assert.equal(second._tag, "Failure");
        if (second._tag === "Failure") assert.equal(second.failure._tag, "GoalDispatchBudgetError");
        yield* worker.drain();
        assert.equal(stub.started.length, 1);
        const granted = yield* orchestrator.dispatch({
          type: "goal.budget.set",
          commandId: CommandId.make("budget-granted"),
          threadId,
          limit: 2,
          grant: "leo-approval-1",
        });
        assert.ok(
          granted.storedEvents.some(
            (stored) =>
              stored.event.type === "goal.dispatch-policy.updated" &&
              stored.event.payload.grant === "leo-approval-1",
          ),
        );
        yield* orchestrator.dispatch(messageCommand(threadId, "budget-control"));
        yield* worker.drain();
        assert.equal(stub.started.length, 2);
        const third = yield* Effect.result(
          orchestrator.dispatch(messageCommand(threadId, "budget-third")),
        );
        assert.equal(third._tag, "Failure");
      }),
    ),
);

it.effect("F2-scheduled records held fire as failed and advances its next due time", () =>
  withHarness(
    "goal-schedule",
    ({ orchestrator, worker, stub, threadId, sql, scheduled, pollScheduled }) =>
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const iso = DateTime.formatIso(now);
        const taskId = ScheduledTaskId.make("goal-held-schedule");
        yield* sql`INSERT INTO scheduled_tasks ${sql.insert({
          task_id: taskId,
          title: "held task",
          prompt: "scheduled stub",
          enabled: 1,
          schedule_json: '{"type":"interval","everyMs":60000}',
          project_id: "project:goal-schedule",
          thread_id: threadId,
          workspace_strategy_json: '{"type":"root"}',
          model_selection_json: JSON.stringify(modelSelection),
          runtime_mode: "full-access",
          interaction_mode: "default",
          created_by: "user",
          creation_source: "web",
          created_at: iso,
          updated_at: iso,
          next_run_at: iso,
          last_run_at: null,
          last_run_status: "never",
          last_run_error: null,
          run_count: 0,
        })}`;
        yield* orchestrator.dispatch({
          type: "goal.hold",
          commandId: CommandId.make("schedule-hold"),
          threadId,
          hold: "scheduled-stop",
        });
        yield* pollScheduled;
        yield* worker.drain();
        const rows = yield* sql<{
          last_run_status: string;
          last_run_error: string;
          run_count: number;
          next_run_at: string;
        }>`SELECT * FROM scheduled_tasks WHERE task_id = ${taskId}`;
        assert.equal(rows[0]!.last_run_status, "failed");
        assert.ok(rows[0]!.last_run_error.includes("scheduled-stop"));
        assert.equal(rows[0]!.run_count, 1);
        assert.ok(Date.parse(rows[0]!.next_run_at) > DateTime.toEpochMillis(now));
        assert.equal(stub.started.length, 0);
        yield* pollScheduled;
        const repeated = yield* sql<{
          run_count: number;
        }>`SELECT run_count FROM scheduled_tasks WHERE task_id = ${taskId}`;
        assert.equal(repeated[0]!.run_count, 1);
        yield* orchestrator.dispatch({
          type: "goal.resume",
          commandId: CommandId.make("schedule-resume"),
          threadId,
        });
        yield* scheduled.runNow({ id: taskId });
        yield* worker.drain();
        assert.equal(stub.started.length, 1);
      }),
  ),
);

const endStubTurn = (
  orchestrator: Orchestrator.OrchestratorV2Shape,
  stub: Stub,
  threadId: ThreadId,
  status: "completed" | "failed",
) =>
  Effect.gen(function* () {
    const projection = yield* orchestrator.getThreadProjection(threadId);
    const providerTurn = projection.providerTurns.at(-1)!;
    const now = yield* DateTime.now;
    const ended = yield* orchestrator.streamDomainEvents.pipe(
      Stream.filter(
        (event) =>
          event.type === "run.updated" &&
          event.threadId === threadId &&
          !["running", "queued", "starting"].includes(event.payload.status),
      ),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
    yield* Queue.offer(stub.events, {
      type: "provider_turn.updated",
      driver,
      providerTurn: { ...providerTurn, status, completedAt: now },
    });
    yield* Queue.offer(stub.events, {
      type: "turn.terminal",
      driver,
      providerThreadId: providerTurn.providerThreadId,
      providerTurnId: providerTurn.id,
      runOrdinal: stub.started.at(-1)!.runOrdinal,
      threadDisposition: "reusable",
      ...(status === "failed"
        ? {
            status,
            failure: makeProviderFailure({ class: "validation_error", message: "stub failure" }),
            failureItemOrdinal: 1,
          }
        : { status, failure: null }),
    });
    yield* Fiber.join(ended);
  });

it.effect("F2-queue-goal hold refuses queue release without failing or unholding queued work", () =>
  withHarness("goal-queue", ({ orchestrator, worker, stub, threadId, send }) =>
    Effect.gen(function* () {
      const running = yield* waitForRunning(orchestrator);
      yield* send("queue-first", "start_immediately");
      yield* worker.drain();
      yield* Fiber.join(running);
      yield* send("queue-second", "queue_after_active");
      yield* orchestrator.dispatch({
        type: "goal.hold",
        commandId: CommandId.make("queue-goal-hold"),
        threadId,
        hold: "queue-goal-stop",
      });
      yield* endStubTurn(orchestrator, stub, threadId, "completed");
      yield* orchestrator.resumeQueuedRuns;
      yield* worker.drain();
      assert.equal(stub.started.length, 1);
      assert.equal(
        (yield* orchestrator.getThreadProjection(threadId)).runs.at(-1)!.status,
        "queued",
      );
      const release = yield* Effect.result(
        orchestrator.dispatch({
          type: "queue.resume",
          commandId: CommandId.make("queue-held-release"),
          threadId,
        }),
      );
      assert.equal(release._tag, "Failure");
      if (release._tag === "Failure") assert.equal(release.failure._tag, "GoalDispatchHeldError");
      yield* orchestrator.dispatch({
        type: "goal.resume",
        commandId: CommandId.make("queue-goal-resume"),
        threadId,
      });
      yield* orchestrator.dispatch({
        type: "queue.resume",
        commandId: CommandId.make("queue-control-release"),
        threadId,
      });
      yield* worker.drain();
      assert.equal(stub.started.length, 2);
    }),
  ),
);

it.effect("B1 tree budget includes child starts in the root total", () =>
  withHarness("goal-tree-budget", ({ orchestrator, worker, stub, threadId, send }) =>
    Effect.gen(function* () {
      yield* orchestrator.dispatch({
        type: "goal.budget.set",
        commandId: CommandId.make("tree-budget"),
        threadId,
        limit: 2,
      });
      const running = yield* waitForRunning(orchestrator);
      yield* send("tree-parent", "start_immediately");
      yield* worker.drain();
      yield* Fiber.join(running);
      const parent = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
      yield* orchestrator.dispatch({
        type: "delegated_task.request",
        commandId: CommandId.make("tree-child"),
        parentThreadId: threadId,
        parentRunId: parent.id,
        parentNodeId: parent.rootNodeId!,
        task: "tree child",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        createdBy: "agent",
        creationSource: "mcp",
      });
      yield* worker.drain();
      assert.equal(stub.started.length, 2);
      const child = (yield* orchestrator.getThreadProjection(threadId)).subagents[0]!
        .childThreadId!;
      for (const target of [threadId, child]) {
        const refused = yield* Effect.result(
          orchestrator.dispatch(messageCommand(target, `tree-exhausted:${target}`)),
        );
        assert.equal(refused._tag, "Failure");
        if (refused._tag === "Failure")
          assert.equal(refused.failure._tag, "GoalDispatchBudgetError");
      }
      yield* worker.drain();
      assert.equal(stub.started.length, 2);
    }),
  ),
);

it.effect("F5-goal policy survives a second runtime over the same SQLite file", () => {
  const databasePath = NodePath.join(
    NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "dg04-goal-")),
    "orchestration.sqlite",
  );
  return Effect.gen(function* () {
    yield* withHarness(
      "goal-durable",
      ({ orchestrator, worker, threadId, send }) =>
        Effect.gen(function* () {
          yield* orchestrator.dispatch({
            type: "goal.budget.set",
            commandId: CommandId.make("durable-budget"),
            threadId,
            limit: 1,
          });
          const running = yield* waitForRunning(orchestrator);
          yield* send("durable-first", "start_immediately");
          yield* worker.drain();
          yield* Fiber.join(running);
          yield* orchestrator.dispatch({
            type: "thread.stop",
            commandId: CommandId.make("durable-stop"),
            threadId,
          });
          yield* worker.drain();
          yield* orchestrator.dispatch({
            type: "goal.hold",
            commandId: CommandId.make("durable-hold"),
            threadId,
            hold: "persisted-stop",
          });
        }),
      { databasePath, layerName: "goal-durable-a" },
    );
    yield* withHarness(
      "goal-durable",
      ({ orchestrator, worker, stub, threadId }) =>
        Effect.gen(function* () {
          const held = yield* Effect.result(
            orchestrator.dispatch(messageCommand(threadId, "durable-held-send")),
          );
          assert.equal(held._tag, "Failure");
          if (held._tag === "Failure") assert.equal(held.failure._tag, "GoalDispatchHeldError");
          yield* orchestrator.dispatch({
            type: "goal.resume",
            commandId: CommandId.make("durable-resume"),
            threadId,
          });
          const exhausted = yield* Effect.result(
            orchestrator.dispatch(messageCommand(threadId, "durable-budget-send")),
          );
          assert.equal(exhausted._tag, "Failure");
          if (exhausted._tag === "Failure")
            assert.equal(exhausted.failure._tag, "GoalDispatchBudgetError");
          yield* worker.drain();
          assert.equal(stub.started.length, 0);
          // This proves persisted policy, not a whole-process restart or provider rebuild.
        }),
      { databasePath, layerName: "goal-durable-b", existingThread: true, recoverOnStartup: true },
    );
  });
});

it.effect(
  "B3 budget increase requires a durable grant and leaves an ungranted limit unchanged",
  () =>
    withHarness("goal-grant", ({ orchestrator, worker, stub, threadId }) =>
      Effect.gen(function* () {
        yield* orchestrator.dispatch({
          type: "goal.budget.set",
          commandId: CommandId.make("grant-zero"),
          threadId,
          limit: 0,
        });
        const ungranted = yield* Effect.result(
          orchestrator.dispatch({
            type: "goal.budget.set",
            commandId: CommandId.make("grant-missing"),
            threadId,
            limit: 1,
          }),
        );
        assert.equal(ungranted._tag, "Failure");
        if (ungranted._tag === "Failure")
          assert.equal(ungranted.failure._tag, "GoalBudgetGrantRequiredError");
        const unchanged = yield* Effect.result(
          orchestrator.dispatch(messageCommand(threadId, "grant-still-zero")),
        );
        assert.equal(unchanged._tag, "Failure");
        if (unchanged._tag === "Failure")
          assert.equal(unchanged.failure._tag, "GoalDispatchBudgetError");
        yield* worker.drain();
        assert.equal(stub.started.length, 0);
        const granted = yield* orchestrator.dispatch({
          type: "goal.budget.set",
          commandId: CommandId.make("grant-recorded"),
          threadId,
          limit: 1,
          grant: "leo-grant-ref",
        });
        assert.ok(
          granted.storedEvents.some(
            (stored) =>
              stored.event.type === "goal.dispatch-policy.updated" &&
              stored.event.payload.grant === "leo-grant-ref",
          ),
        );
        yield* orchestrator.dispatch(messageCommand(threadId, "grant-control"));
        yield* worker.drain();
        assert.equal(stub.started.length, 1);
      }),
    ),
);
