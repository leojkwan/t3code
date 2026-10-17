import { assert, it } from "@effect/vitest";
import {
  type ModelSelection,
  MessageId,
  NodeId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ThreadProjection,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";

import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderTurnControlService from "./ProviderTurnControlService.ts";

const driver = ProviderDriverKind.make("codex");
const providerInstanceId = ProviderInstanceId.make("codex");
const modelSelection = {
  instanceId: providerInstanceId,
  model: "gpt-5.4",
} satisfies ModelSelection;

function makeProjection(input: {
  readonly now: DateTime.Utc;
  readonly threadId: ThreadId;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly providerTurnId: ProviderTurnId;
  readonly attemptId: RunAttemptId;
}): OrchestrationV2ThreadProjection {
  const runId = RunId.make("run:restart-session");
  const nodeId = NodeId.make("node:restart-session");
  return {
    thread: {
      createdBy: "user",
      creationSource: "web",
      id: input.threadId,
      projectId: ProjectId.make("project:restart-session"),
      title: "Restart session",
      providerInstanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: "/workspace",
      activeProviderThreadId: input.providerThread.id,
      lineage: {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: input.threadId,
      },
      forkedFrom: null,
      createdAt: input.now,
      updatedAt: input.now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    },
    runs: [],
    attempts: [
      {
        id: input.attemptId,
        runId,
        attemptOrdinal: 1,
        rootNodeId: nodeId,
        providerInstanceId,
        providerThreadId: input.providerThread.id,
        providerTurnId: input.providerTurnId,
        reason: "initial",
        status: "superseded",
        startedAt: input.now,
        completedAt: input.now,
      },
    ],
    nodes: [],
    subagents: [],
    providerSessions: [],
    providerThreads: [input.providerThread],
    providerTurns: [
      {
        id: input.providerTurnId,
        providerThreadId: input.providerThread.id,
        nodeId,
        runAttemptId: input.attemptId,
        nativeTurnRef: {
          driver,
          nativeId: "native-turn:restart-session",
          strength: "strong",
        },
        ordinal: 1,
        status: "running",
        startedAt: input.now,
        completedAt: null,
      },
    ],
    runtimeRequests: [],
    messages: [],
    plans: [],
    turnItems: [],
    checkpointScopes: [],
    checkpoints: [],
    contextHandoffs: [],
    contextTransfers: [],
    visibleTurnItems: [],
    updatedAt: input.now,
  };
}

const makeNativeGoalControlFixture = Effect.fn(function* () {
  const now = yield* DateTime.now;
  const threadId = ThreadId.make("thread:native-goal-read");
  const providerSessionId = ProviderSessionId.make("session:native-goal-read");
  const providerThreadId = ProviderThreadId.make("provider-thread:native-goal-read");
  const sourceRunId = RunId.make("run:native-goal-read");
  const nativeThreadId = "native-thread:goal-read";
  const providerThread: OrchestrationV2ProviderThread = {
    id: providerThreadId, driver, providerInstanceId, providerSessionId, appThreadId: threadId,
    ownerNodeId: null, nativeThreadRef: { driver, nativeId: nativeThreadId, strength: "strong" },
    nativeConversationHeadRef: null, status: "not_loaded", firstRunOrdinal: 1, lastRunOrdinal: 1,
    handoffIds: [], forkedFrom: null, createdAt: now, updatedAt: now,
    goal: { objective: "Stale mirror", status: "paused", tokensUsed: 9999, tokenBudget: null },
  };
  const state = makeProjection({ now, threadId, providerThread,
    providerTurnId: ProviderTurnId.make("provider-turn:native-goal-read"), attemptId: RunAttemptId.make("attempt:native-goal-read") });
  const projection = yield* Ref.make<OrchestrationV2ThreadProjection>({ ...state,
    runs: [{ id: sourceRunId, threadId, ordinal: 1, providerInstanceId, modelSelection, providerThreadId,
      userMessageId: MessageId.make("input:native-goal-read"), rootNodeId: null, activeAttemptId: null,
      status: "failed", requestedAt: now, startedAt: now, completedAt: now, checkpointId: null, contextHandoffId: null }],
    providerTurns: state.providerTurns.map((turn) => ({ ...turn, status: "failed", completedAt: now })),
  });
  const fresh: ProviderAdapter.ProviderAdapterV2NativeGoal = { threadId: nativeThreadId,
    objective: "Actual native objective", status: "active", tokensUsed: 200, tokenBudget: 1000,
    timeUsedSeconds: 37, createdAt: 1782622440, updatedAt: 1782622490 };
  const raw = yield* Ref.make<ProviderAdapter.ProviderAdapterV2NativeGoal | null>(fresh);
  const afterRead = yield* Ref.make<Effect.Effect<void>>(Effect.void);
  const requests: string[] = [];
  const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
    instanceId: providerInstanceId, driver, providerSessionId,
    providerSession: { id: providerSessionId, driver, providerInstanceId, status: "ready", cwd: "/workspace",
      model: modelSelection.model, capabilities: CodexProviderCapabilitiesV2, createdAt: now, updatedAt: now, lastError: null },
    events: Stream.empty,
    ensureThread: () => Effect.die("native goal read must not ensure"),
    resumeThread: () => Effect.die("native goal read must not resume"),
    startTurn: () => Effect.die("native goal read must not start"),
    steerTurn: () => Effect.die("native goal read must not steer"),
    interruptTurn: () => Effect.die("native goal read must not interrupt"),
    respondToRuntimeRequest: () => Effect.die("native goal read must not answer"),
    readThreadSnapshot: () => Effect.die("native goal read must not use a stale snapshot"),
    rollbackThread: () => Effect.die("native goal read must not rollback"),
    forkThread: () => Effect.die("native goal read must not fork"),
    readNativeGoal: ({ providerThread: target }) => Effect.gen(function* () {
      assert.equal(target.id, providerThreadId);
      requests.push("thread/goal/get");
      yield* (yield* Ref.get(afterRead));
      return yield* Ref.get(raw);
    }),
  };
  const resident = yield* Ref.make<ProviderAdapter.ProviderAdapterV2SessionRuntime | null>(runtime);
  const layer = ProviderTurnControlService.layer.pipe(Layer.provide(Layer.merge(
    Layer.mock(ProjectionStore.ProjectionStoreV2)({ getThreadRecords: () => Ref.get(projection) }),
    Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
      open: () => Effect.die("native goal read must not open a runtime"),
      get: (sid) => Ref.get(resident).pipe(Effect.map((current) =>
        sid === providerSessionId && current !== null ? Option.some(current) : Option.none())),
    }),
  )));
  return { layer, projection, raw, resident, runtime, requests, afterRead, fresh,
    input: { threadId, sourceRunId, providerSessionId, providerThreadId } };
});

const nativeAccountObservation: ProviderAdapter.ProviderAdapterV2NativeAccountObservation = {
  account: { account: { type: "chatgpt", email: null, planType: "pro" }, requiresOpenaiAuth: true },
  rateLimits: { accountId: "workspace:actual", ordinaryUsageAllowed: true,
    rateLimits: { primary: { usedPercent: 0 } },
    rateLimitsByLimitId: { codex: { primary: { usedPercent: 0 } }, review: { primary: { usedPercent: 100 } } } },
  accountObservedAtMs: 1791810000000, usageObservedAtMs: 1791810000001,
};

it.effect("reads backend account permission from the exact resident runtime without opening or resuming it", () =>
  Effect.gen(function* () {
    const fixture = yield* makeNativeGoalControlFixture();
    yield* Ref.set(fixture.resident, { ...fixture.runtime, readNativeAccount: () => {
      fixture.requests.push("account/read", "account/rateLimits/read");
      return Effect.succeed(nativeAccountObservation);
    } });
    yield* Effect.gen(function* () {
      const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
      assert.deepEqual(yield* control.readNativeAccount({ ...fixture.input, expectedAccountId: "workspace:actual" }),
        nativeAccountObservation);
      assert.deepEqual(fixture.requests, ["account/read", "account/rateLimits/read"]);
    }).pipe(Effect.provide(fixture.layer));
  }),
);

it.effect.each(["missing-permission", "null-permission", "denied", "wrong-account", "missing-account", "unauthenticated", "malformed", "unsupported", "refault"] as const)(
  "refuses %s native account evidence without inferring permission from zero percentages", (kind) =>
    Effect.gen(function* () {
      const fixture = yield* makeNativeGoalControlFixture();
      const observation = kind === "missing-permission" ? { ...nativeAccountObservation,
        rateLimits: { accountId: "workspace:actual", rateLimits: { primary: { usedPercent: 0 } } } }
        : kind === "null-permission" ? { ...nativeAccountObservation,
          rateLimits: { ...nativeAccountObservation.rateLimits, ordinaryUsageAllowed: null } }
        : kind === "denied" ? { ...nativeAccountObservation,
          rateLimits: { ...nativeAccountObservation.rateLimits, ordinaryUsageAllowed: false } }
        : kind === "wrong-account" ? { ...nativeAccountObservation,
          rateLimits: { ...nativeAccountObservation.rateLimits, accountId: "workspace:other" } }
        : kind === "missing-account" ? { ...nativeAccountObservation,
          rateLimits: { ...nativeAccountObservation.rateLimits, accountId: null } }
        : kind === "unauthenticated" ? { ...nativeAccountObservation,
          account: { account: null, requiresOpenaiAuth: true } }
        : kind === "malformed" ? { ...nativeAccountObservation, usageObservedAtMs: -1 }
        : nativeAccountObservation;
      yield* Ref.set(fixture.resident, { ...fixture.runtime,
        ...(kind === "unsupported" ? {} : { readNativeAccount: () => kind === "refault"
          ? Effect.fail(new ProviderAdapter.ProviderAdapterProtocolError({ driver, detail: "native read failed" }))
          : Effect.succeed(observation) }),
      });
      yield* Effect.gen(function* () {
        const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
        const failure = yield* control.readNativeAccount({ ...fixture.input, expectedAccountId: "workspace:actual" })
          .pipe(Effect.flip);
        assert.equal(failure.reason, kind === "unsupported" ? "unsupported"
          : kind === "refault" || kind === "malformed" ? "read_failed"
          : kind === "wrong-account" ? "account_mismatch"
          : kind === "unauthenticated" ? "unauthenticated"
          : kind === "denied" ? "usage_denied" : "usage_unavailable");
      }).pipe(Effect.provide(fixture.layer));
    }),
);

it.effect("rejects a changed original run binding while the native account reply is held", () =>
  Effect.gen(function* () {
    const fixture = yield* makeNativeGoalControlFixture();
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    yield* Ref.set(fixture.resident, { ...fixture.runtime, readNativeAccount: () =>
      Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)),
        Effect.as(nativeAccountObservation)),
    });
    yield* Effect.gen(function* () {
      const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
      const reading = yield* control.readNativeAccount({ ...fixture.input, expectedAccountId: "workspace:actual" })
        .pipe(Effect.result, Effect.forkChild);
      yield* Deferred.await(entered);
      // This is a durable-binding read control, not a canonical Stop acceptance fixture.
      yield* Ref.update(fixture.projection, (projection) => ({ ...projection,
        runs: projection.runs.map((run) => ({ ...run, status: "interrupted" as const, queueHeld: true })),
      }));
      yield* Deferred.succeed(release, undefined);
      const result = yield* Fiber.join(reading);
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") assert.equal(result.failure.reason, "identity_mismatch");
    }).pipe(Effect.provide(fixture.layer));
  }),
);

it.effect("reads the live native goal for a failed source without touching its stale projection or creating work", () =>
  Effect.gen(function* () {
    const fixture = yield* makeNativeGoalControlFixture();
    const before = yield* Ref.get(fixture.projection);
    yield* Effect.gen(function* () {
      const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
      assert.deepEqual(yield* control.readNativeGoal(fixture.input), fixture.fresh);
      const changed = { ...fixture.fresh, objective: "Changed native objective", tokensUsed: 300, updatedAt: fixture.fresh.updatedAt + 1 };
      yield* Ref.set(fixture.raw, changed);
      assert.deepEqual(yield* control.readNativeGoal(fixture.input), changed);
    }).pipe(Effect.provide(fixture.layer));
    assert.deepEqual(yield* Ref.get(fixture.projection), before);
    assert.deepEqual(fixture.requests, ["thread/goal/get", "thread/goal/get"]);
  }),
);

it.effect.each([
  ["runtime-missing", "runtime_unavailable"], ["capability-missing", "unsupported"],
  ["wrong-runtime", "identity_mismatch"], ["wrong-thread", "identity_mismatch"],
  ["goal-missing", "goal_unavailable"], ["budget-missing", "budget_unavailable"],
  ["paused", "goal_inactive"],
  ["budget-exhausted", "budget_exhausted"], ["malformed", "read_failed"],
] as const)("refuses %s without fallback or work", (kind, reason) =>
  Effect.gen(function* () {
    const fixture = yield* makeNativeGoalControlFixture();
    const before = yield* Ref.get(fixture.projection);
    if (kind === "runtime-missing") yield* Ref.set(fixture.resident, null);
    if (kind === "capability-missing") {
      const { readNativeGoal: _read, ...unsupported } = fixture.runtime;
      yield* Ref.set(fixture.resident, unsupported);
    }
    if (kind === "wrong-runtime") yield* Ref.set(fixture.resident, { ...fixture.runtime, providerSessionId: ProviderSessionId.make("other-session") });
    if (kind === "wrong-thread") yield* Ref.set(fixture.raw, { ...fixture.fresh, threadId: "other-native-thread" });
    if (kind === "goal-missing") yield* Ref.set(fixture.raw, null);
    if (kind === "budget-missing") {
      const { tokenBudget: _budget, ...missing } = fixture.fresh;
      yield* Ref.set(fixture.raw, missing);
    }
    if (kind === "paused") yield* Ref.set(fixture.raw, { ...fixture.fresh, status: "paused" });
    if (kind === "budget-exhausted") yield* Ref.set(fixture.raw, { ...fixture.fresh, tokensUsed: 1000 });
    if (kind === "malformed") yield* Ref.set(fixture.raw, { ...fixture.fresh, tokensUsed: -1 });
    yield* Effect.gen(function* () {
      const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
      assert.equal((yield* control.readNativeGoal(fixture.input).pipe(Effect.flip)).reason, reason);
    }).pipe(Effect.provide(fixture.layer));
    assert.deepEqual(yield* Ref.get(fixture.projection), before);
    assert.isTrue(fixture.requests.every((method) => method === "thread/goal/get"));
  }),
);

it.effect("preserves explicit native null as no-cap data for the matching protocol without changing goal or accounting", () =>
  Effect.gen(function* () {
    const fixture = yield* makeNativeGoalControlFixture();
    const before = yield* Ref.get(fixture.projection);
    const native = { ...fixture.fresh, tokenBudget: null, tokensUsed: 100_000 };
    yield* Ref.set(fixture.raw, native);
    yield* Effect.gen(function* () {
      const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
      assert.deepEqual(yield* control.readNativeGoal(fixture.input), native);
    }).pipe(Effect.provide(fixture.layer));
    assert.deepEqual(yield* Ref.get(fixture.projection), before);
    assert.deepEqual(fixture.requests, ["thread/goal/get"]);
    // This decoded-data test supplies the matching contract as a native fixture.
    // Installed protocol association and host/account admission remain unproved.
  }),
);

it.effect("refuses a binding changed while the native read is in flight", () =>
  Effect.gen(function* () {
    const fixture = yield* makeNativeGoalControlFixture();
    yield* Ref.set(fixture.afterRead, Ref.update(fixture.projection, (current) => ({ ...current,
      providerThreads: current.providerThreads.map((thread) => ({ ...thread, providerSessionId: ProviderSessionId.make("changed-session") })),
    })));
    yield* Effect.gen(function* () {
      const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
      assert.equal((yield* control.readNativeGoal(fixture.input).pipe(Effect.flip)).reason, "identity_mismatch");
    }).pipe(Effect.provide(fixture.layer));
    assert.deepEqual(fixture.requests, ["thread/goal/get"]);
  }),
);

it.effect("reports Claude's absent native goal read as unsupported rather than trusting its goal mirror", () =>
  Effect.gen(function* () {
    const fixture = yield* makeNativeGoalControlFixture();
    const claude = ProviderDriverKind.make("claudeAgent");
    yield* Ref.update(fixture.projection, (current) => ({ ...current,
      providerThreads: current.providerThreads.map((thread) => ({ ...thread, driver: claude,
        nativeThreadRef: { driver: claude, nativeId: "claude-native-thread", strength: "strong" },
        goal: { objective: "Transcript goal mirror", status: "active", tokensUsed: 0, tokenBudget: 1000 },
      })),
    }));
    const { readNativeGoal: _read, ...unsupported } = fixture.runtime;
    yield* Ref.set(fixture.resident, { ...unsupported, driver: claude,
      providerSession: { ...unsupported.providerSession, driver: claude } });
    const before = yield* Ref.get(fixture.projection);
    yield* Effect.gen(function* () {
      const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
      const failure = yield* control.readNativeGoal(fixture.input).pipe(Effect.flip);
      assert.equal(failure.reason, "unsupported");
      assert.include(String(failure.cause), "claudeAgent");
    }).pipe(Effect.provide(fixture.layer));
    assert.deepEqual(yield* Ref.get(fixture.projection), before);
    assert.deepEqual(fixture.requests, []);
  }),
);

it.effect(
  "interrupts the historical session only for the exact committed restart replacement",
  () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread:restart-session");
      const oldSessionId = ProviderSessionId.make("provider-session:restart-session:old");
      const replacementSessionId = ProviderSessionId.make(
        "provider-session:restart-session:replacement",
      );
      const unrelatedSessionId = ProviderSessionId.make(
        "provider-session:restart-session:unrelated",
      );
      const providerThreadId = ProviderThreadId.make("provider-thread:restart-session");
      const providerTurnId = ProviderTurnId.make("provider-turn:restart-session");
      const attemptId = RunAttemptId.make("run-attempt:restart-session");
      const providerThread: OrchestrationV2ProviderThread = {
        id: providerThreadId,
        driver,
        providerInstanceId,
        // The restart command has already projected this replacement binding
        // before the process-bound restart effect executes.
        providerSessionId: replacementSessionId,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: {
          driver,
          nativeId: "native-thread:restart-session",
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "not_loaded",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };
      const projection = yield* Ref.make(
        makeProjection({ now, threadId, providerThread, providerTurnId, attemptId }),
      );
      const interruptedThread = yield* Ref.make<OrchestrationV2ProviderThread | null>(null);
      const providerSession = {
        id: oldSessionId,
        driver,
        providerInstanceId,
        status: "running" as const,
        cwd: "/workspace",
        model: modelSelection.model,
        capabilities: CodexProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: providerInstanceId,
        driver,
        providerSessionId: oldSessionId,
        providerSession,
        events: Stream.empty,
        ensureThread: () => Effect.die("unused ensureThread"),
        resumeThread: () => Effect.die("unused resumeThread"),
        startTurn: () => Effect.die("unused startTurn"),
        steerTurn: () => Effect.die("unused steerTurn"),
        interruptTurn: ({ providerThread: target }) =>
          Effect.all(
            [
              Ref.set(interruptedThread, target),
              Ref.update(projection, (current) => ({
                ...current,
                providerTurns: current.providerTurns.map((turn) =>
                  turn.id === providerTurnId
                    ? { ...turn, status: "interrupted" as const, completedAt: now }
                    : turn,
                ),
              })),
            ],
            { discard: true },
          ),
        respondToRuntimeRequest: () => Effect.die("unused respondToRuntimeRequest"),
        readThreadSnapshot: () => Effect.die("unused readThreadSnapshot"),
        rollbackThread: () => Effect.die("unused rollbackThread"),
        forkThread: () => Effect.die("unused forkThread"),
      };
      const layerProjection = Layer.succeed(
        ProjectionStore.ProjectionStoreV2,
        ProjectionStore.ProjectionStoreV2.of({
          apply: () => Effect.void,
          getLimitRecoveryCandidates: () => Effect.die("unused getLimitRecoveryCandidates"),
          getShellSnapshot: () => Effect.die("unused getShellSnapshot"),
          readShellSnapshot: () => Effect.die("unused readShellSnapshot"),
          getThreadShell: () => Effect.die("unused getThreadShell"),
          getThread: () => Ref.get(projection).pipe(Effect.map((state) => state.thread)),
          getSettlementCandidates: () => Effect.die("unused getSettlementCandidates"),
          getThreadsWithPullRequests: () => Effect.die("unused getThreadsWithPullRequests"),
          getThreadProjection: () => Effect.die("control effects must not load transcript"),
          getTurnStartContext: () => Effect.die("unused"),
          getTurnStartHistory: () => Effect.die("unused"),
          getRuntimeRecoveryProjection: () => Effect.die("unused getRuntimeRecoveryProjection"),
          getPlan: () => Effect.die("unused"),
          hasUnpairedRunInterruptRequest: () => Effect.die("unused interrupt read"),
          getThreadAttachmentIds: () => Effect.die("Unused attachment lookup"),
          searchThread: () => Effect.die("unused"),
          searchThreadStream: () => Stream.empty,
          getThreadHistoryPage: () => Effect.die("unused"),
          getTimelinePage: () => Effect.die("Unused timeline read"),
          getMessageCount: () => Effect.die("unused message count"),
          getNextTurnItemOrdinal: () => Effect.die("unused ordinal read"),
          getTurnItem: () => Effect.die("unused turn item read"),
          getThreadRecords: () => Effect.die("unused record read"),
          getRuntimeRequest: () => Effect.die("unused getRuntimeRequest"),
          getRunningTurnContext: () => Effect.die("unused getRunningTurnContext"),
          getThreadProviderContext: () => Effect.die("unused getThreadProviderContext"),
          getRuntimeResponseContext: () => Effect.die("unused getRuntimeResponseContext"),
          getPendingNativeUserInputs: () => Effect.die("unused getPendingNativeUserInputs"),
          getProviderControlContext: (_threadId, target) =>
            Ref.get(projection).pipe(
              Effect.map((current) => ({
                providerThread: current.providerThreads.find(
                  (thread) => thread.id === target.providerThreadId,
                ),
                providerTurn: current.providerTurns.find(
                  (turn) => turn.id === target.providerTurnId,
                ),
                attempt: current.attempts.find((attempt) => attempt.id === target.attemptId),
                message: undefined,
                run: undefined,
              })),
            ),
          getCheckpointContext: () => Effect.die("not used"),
          getCheckpointCaptureContext: () => Effect.die("not used"),
          getRunMessage: () => Effect.die("not used"),
          canStartQueuedRun: () => Effect.die("not used"),
          getRecoveryThreadIds: () => Effect.die("unused getRecoveryThreadIds"),
          getUnreadableThreadIds: () => Effect.die("unused getUnreadableThreadIds"),
          getThreadSnapshot: () => Effect.die("unused getThreadSnapshot"),
          getThreadSnapshotWindow: () => Effect.die("unused getThreadSnapshotWindow"),
        }),
      );
      const layerSessionManager = Layer.succeed(
        ProviderSessionManager.ProviderSessionManagerV2,
        ProviderSessionManager.ProviderSessionManagerV2.of({
          shutdown: Effect.void,
          open: () => Effect.die("unused open"),
          get: (providerSessionId) =>
            Effect.succeed(
              providerSessionId === oldSessionId ? Option.some(runtime) : Option.none(),
            ),
          close: () => Effect.void,
          closeInstance: () => Effect.void,
          release: () => Effect.void,
          detach: () => Effect.void,
        }),
      );
      const layerControl = ProviderTurnControlService.layer.pipe(
        Layer.provide(Layer.merge(layerProjection, layerSessionManager)),
      );

      const [ordinaryInterrupt, unrelatedRestart] = yield* Effect.gen(function* () {
        const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
        const ordinary = yield* Effect.exit(
          control.interrupt({
            threadId,
            providerSessionId: oldSessionId,
            providerThreadId,
            providerTurnId,
          }),
        );
        const unrelated = yield* Effect.exit(
          control.interruptAndAwaitTerminal({
            threadId,
            providerSessionId: oldSessionId,
            replacementProviderSessionId: unrelatedSessionId,
            providerThreadId,
            providerTurnId,
            interruptedAttemptId: attemptId,
          }),
        );
        return [ordinary, unrelated] as const;
      }).pipe(Effect.provide(layerControl));

      assert.isTrue(Exit.isFailure(ordinaryInterrupt));
      assert.isTrue(Exit.isFailure(unrelatedRestart));
      assert.isNull(yield* Ref.get(interruptedThread));

      yield* Effect.gen(function* () {
        const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
        yield* control.interruptAndAwaitTerminal({
          threadId,
          providerSessionId: oldSessionId,
          replacementProviderSessionId: replacementSessionId,
          providerThreadId,
          providerTurnId,
          interruptedAttemptId: attemptId,
        });
      }).pipe(Effect.provide(layerControl));

      const interrupted = yield* Ref.get(interruptedThread);
      assert.isNotNull(interrupted);
      assert.equal(interrupted?.providerSessionId, oldSessionId);
      assert.equal(interrupted?.id, providerThreadId);
      assert.equal(interrupted?.nativeThreadRef?.nativeId, "native-thread:restart-session");
    }),
);
