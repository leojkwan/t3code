import { projectComposerContextForProvider } from "@t3tools/shared/composerContextReferences";
import {
  MessageId,
  type NodeId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

const yieldToRuntime = Effect.yieldNow.pipe(
  Effect.andThen(
    Effect.promise(
      () =>
        new Promise<void>((resolve) => {
          setImmediate(resolve);
        }),
    ),
  ),
);

export class ProviderTurnControlError extends Schema.TaggedError<ProviderTurnControlError>()(
  "ProviderTurnControlError",
  {
    threadId: ThreadId,
    operation: Schema.Literals(["interrupt", "restart", "steer"]),
    providerTurnId: ProviderTurnId,
    turnCompleted: Schema.optional(Schema.Boolean),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

const isProviderTurnControlError = Schema.is(ProviderTurnControlError);

export class ProviderNativeGoalReadError extends Schema.TaggedError<ProviderNativeGoalReadError>()(
  "ProviderNativeGoalReadError", {
    threadId: ThreadId,
    reason: Schema.Literals(["target_unavailable", "runtime_unavailable", "unsupported", "read_failed",
      "identity_mismatch", "goal_unavailable", "goal_inactive", "budget_unavailable", "budget_exhausted"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export class ProviderNativeAccountReadError extends Schema.TaggedError<ProviderNativeAccountReadError>()(
  "ProviderNativeAccountReadError", {
    threadId: ThreadId,
    reason: Schema.Literals(["target_unavailable", "runtime_unavailable", "unsupported", "read_failed",
      "identity_mismatch", "account_mismatch", "unauthenticated", "usage_unavailable", "usage_denied"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export interface ProviderTurnControlServiceV2Shape {
  /** Read evidence only. expectedAccountId must come from the native authority owner, not a command body. */
  readonly readNativeAccount: (input: {
    readonly threadId: ThreadId;
    readonly sourceRunId: RunId;
    readonly providerSessionId: ProviderSessionId;
    readonly providerThreadId: ProviderThreadId;
    readonly expectedAccountId: string;
  }) => Effect.Effect<ProviderAdapter.ProviderAdapterV2NativeAccountObservation, ProviderNativeAccountReadError>;
  /** Fresh read of the original resident runtime, including a terminal source run. */
  readonly readNativeGoal: (input: {
    readonly threadId: ThreadId;
    readonly sourceRunId: RunId;
    readonly providerSessionId: ProviderSessionId;
    readonly providerThreadId: ProviderThreadId;
  }) => Effect.Effect<ProviderAdapter.ProviderAdapterV2NativeGoal, ProviderNativeGoalReadError>;
  readonly interrupt: (input: {
    readonly subagent?: { readonly id: NodeId; readonly nativeTaskId: string };
    readonly threadId: ThreadId;
    readonly providerSessionId: ProviderSessionId;
    readonly providerThreadId: ProviderThreadId;
    readonly providerTurnId: ProviderTurnId;
  }) => Effect.Effect<void, ProviderTurnControlError>;
  readonly steer: (input: {
    readonly threadId: ThreadId;
    readonly providerSessionId: ProviderSessionId;
    readonly providerThreadId: ProviderThreadId;
    readonly providerTurnId: ProviderTurnId;
    readonly messageId: MessageId;
  }) => Effect.Effect<void, ProviderTurnControlError>;
  readonly interruptAndAwaitTerminal: (input: {
    readonly threadId: ThreadId;
    readonly providerSessionId: ProviderSessionId;
    readonly replacementProviderSessionId?: ProviderSessionId;
    readonly providerThreadId: ProviderThreadId;
    readonly providerTurnId: ProviderTurnId;
    readonly interruptedAttemptId: RunAttemptId;
  }) => Effect.Effect<void, ProviderTurnControlError>;
}

export class ProviderTurnControlServiceV2 extends Context.Service<
  ProviderTurnControlServiceV2,
  ProviderTurnControlServiceV2Shape
>()("t3/orchestration-v2/ProviderTurnControlService/ProviderTurnControlServiceV2") {}

export const layer: Layer.Layer<
  ProviderTurnControlServiceV2,
  never,
  ProjectionStore.ProjectionStoreV2 | ProviderSessionManager.ProviderSessionManagerV2
> = Layer.effect(
  ProviderTurnControlServiceV2,
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;

    const load = (input: {
      readonly threadId: ThreadId;
      readonly providerSessionId: ProviderSessionId;
      readonly replacementProviderSessionId?: ProviderSessionId;
      readonly providerThreadId: ProviderThreadId;
      readonly providerTurnId: ProviderTurnId;
      readonly operation: "interrupt" | "restart" | "steer";
      readonly messageId?: MessageId;
    }) =>
      Effect.gen(function* () {
        const context = yield* projections.getProviderControlContext(input.threadId, input);
        const { providerThread, providerTurn } = context;
        const targetsRecordedSession =
          providerThread?.providerSessionId === input.providerSessionId;
        const targetsCommittedReplacement =
          input.operation === "restart" &&
          input.replacementProviderSessionId !== undefined &&
          providerThread?.providerSessionId === input.replacementProviderSessionId;
        if (
          providerThread === undefined ||
          providerTurn === undefined ||
          (!targetsRecordedSession && !targetsCommittedReplacement) ||
          providerTurn.providerThreadId !== providerThread.id
        ) {
          return yield* new ProviderTurnControlError({
            threadId: input.threadId,
            operation: input.operation,
            providerTurnId: input.providerTurnId,
            cause: "The recorded provider execution target is no longer valid.",
          });
        }
        // A restart-session command commits the replacement binding before its
        // process-bound effect runs. The old live runtime must still receive
        // the interrupt, but only when the projection matches the exact
        // replacement captured by that same durable effect.
        const interruptProviderThread = targetsRecordedSession
          ? providerThread
          : { ...providerThread, providerSessionId: input.providerSessionId };
        if (providerTurn.status !== "running") {
          if (input.operation === "steer") {
            return yield* new ProviderTurnControlError({
              threadId: input.threadId,
              operation: "steer",
              providerTurnId: input.providerTurnId,
              turnCompleted: providerTurn.status === "completed",
              cause: "The provider turn ended before the steering message was delivered.",
            });
          }
          return {
            context,
            providerThread: interruptProviderThread,
            providerTurn,
            session: Option.none(),
          };
        }
        const session = yield* sessions.get(input.providerSessionId);
        if (Option.isNone(session)) {
          // Interrupt/restart against a already-released session must not fail
          // the durable effect (and retry 5x). The turn may still look running
          // in projection until recovery/finalization; there is no live adapter
          // to interrupt.
          if (input.operation === "interrupt" || input.operation === "restart") {
            yield* Effect.logWarning(
              "Provider interrupt/restart found no live session; treating as already stopped",
              {
                threadId: input.threadId,
                operation: input.operation,
                providerSessionId: input.providerSessionId,
                providerTurnId: input.providerTurnId,
                providerTurnStatus: providerTurn.status,
              },
            );
            return {
              context,
              providerThread: interruptProviderThread,
              providerTurn,
              session: Option.none(),
            };
          }
          return yield* new ProviderTurnControlError({
            threadId: input.threadId,
            operation: input.operation,
            providerTurnId: input.providerTurnId,
            cause: `Provider session ${input.providerSessionId} is not active.`,
          });
        }
        return { context, providerThread: interruptProviderThread, providerTurn, session };
      });

    return ProviderTurnControlServiceV2.of({
      readNativeAccount: (input) => Effect.gen(function* () {
        const refuse = (reason: ProviderNativeAccountReadError["reason"], cause?: unknown) =>
          new ProviderNativeAccountReadError({ threadId: input.threadId, reason, ...(cause === undefined ? {} : { cause }) });
        if (input.expectedAccountId.trim() === "") return yield* refuse("account_mismatch");
        const readRecords = () => projections.getThreadRecords(input.threadId, ["runs", "providerThreads"], {
          runIds: [input.sourceRunId],
        }).pipe(Effect.mapError((cause) => refuse("target_unavailable", cause)));
        const records = yield* readRecords();
        const run = records.runs.find((candidate) => candidate.id === input.sourceRunId);
        const providerThread = records.providerThreads.find((candidate) => candidate.id === input.providerThreadId);
        if (records.thread.id !== input.threadId || records.thread.deletedAt !== null ||
          run?.threadId !== input.threadId || run.providerThreadId !== input.providerThreadId ||
          run.status === "interrupted" || run.status === "cancelled" || run.queueHeld === true ||
          providerThread?.appThreadId !== input.threadId ||
          providerThread.providerSessionId !== input.providerSessionId ||
          run.providerInstanceId !== providerThread.providerInstanceId) {
          return yield* refuse("identity_mismatch");
        }
        const session = yield* sessions.get(input.providerSessionId).pipe(
          Effect.mapError((cause) => refuse("runtime_unavailable", cause)),
        );
        if (Option.isNone(session)) return yield* refuse("runtime_unavailable");
        const runtime = session.value;
        if (runtime.providerSessionId !== input.providerSessionId || runtime.instanceId !== providerThread.providerInstanceId ||
          runtime.driver !== providerThread.driver) return yield* refuse("identity_mismatch");
        if (runtime.readNativeAccount === undefined) return yield* refuse("unsupported");
        const raw = yield* runtime.readNativeAccount({ providerThread }).pipe(
          Effect.mapError((cause) => refuse("read_failed", cause)),
        );
        const observation = yield* Schema.decodeUnknownEffect(ProviderAdapter.ProviderAdapterV2NativeAccountObservation)(raw)
          .pipe(Effect.mapError((cause) => refuse("read_failed", cause)));
        if (observation.usageObservedAtMs < observation.accountObservedAtMs) return yield* refuse("read_failed");
        const account = observation.account.account;
        if (typeof account !== "object" || account === null ||
          !("type" in account) || typeof account.type !== "string" ||
          typeof observation.account.requiresOpenaiAuth !== "boolean") return yield* refuse("unauthenticated");
        if (typeof observation.rateLimits.accountId !== "string" || observation.rateLimits.accountId.trim() === "")
          return yield* refuse("usage_unavailable");
        if (observation.rateLimits.accountId !== input.expectedAccountId) return yield* refuse("account_mismatch");
        if (observation.rateLimits.ordinaryUsageAllowed === false) return yield* refuse("usage_denied");
        if (observation.rateLimits.ordinaryUsageAllowed !== true) return yield* refuse("usage_unavailable");
        // Native I/O holds no canonical or global auth lock. Re-read the actual
        // binding after it returns; this check still does not freeze host policy.
        const current = yield* readRecords();
        const bound = current.providerThreads.find((candidate) => candidate.id === input.providerThreadId);
        const currentRun = current.runs.find((candidate) => candidate.id === input.sourceRunId);
        const currentSession = yield* sessions.get(input.providerSessionId).pipe(
          Effect.mapError((cause) => refuse("runtime_unavailable", cause)),
        );
        if (current.thread.id !== input.threadId || current.thread.deletedAt !== null ||
          bound?.providerSessionId !== input.providerSessionId || bound.appThreadId !== input.threadId ||
          bound.providerInstanceId !== providerThread.providerInstanceId || bound.driver !== providerThread.driver ||
          bound.nativeThreadRef?.nativeId !== providerThread.nativeThreadRef?.nativeId ||
          currentRun?.providerThreadId !== input.providerThreadId || currentRun.status !== run.status ||
          currentRun.activeAttemptId !== run.activeAttemptId || currentRun.queueHeld === true ||
          Option.isNone(currentSession) || currentSession.value !== runtime) return yield* refuse("identity_mismatch");
        return observation;
      }),
      readNativeGoal: (input) => Effect.gen(function* () {
        const refuse = (reason: ProviderNativeGoalReadError["reason"], cause?: unknown) =>
          new ProviderNativeGoalReadError({ threadId: input.threadId, reason, ...(cause === undefined ? {} : { cause }) });
        // Do not use load(): its terminal-turn fast path discards the resident runtime.
        const records = yield* projections.getThreadRecords(input.threadId, ["runs", "providerThreads"], {
          runIds: [input.sourceRunId],
        }).pipe(Effect.mapError((cause) => refuse("target_unavailable", cause)));
        const sourceRun = records.runs.find((run) => run.id === input.sourceRunId);
        const providerThread = records.providerThreads.find((thread) => thread.id === input.providerThreadId);
        if (records.thread.id !== input.threadId || records.thread.deletedAt !== null ||
          sourceRun?.providerThreadId !== input.providerThreadId ||
          providerThread?.appThreadId !== input.threadId ||
          providerThread.providerSessionId !== input.providerSessionId ||
          sourceRun.providerInstanceId !== providerThread.providerInstanceId ||
          providerThread.nativeThreadRef?.strength !== "strong" ||
          providerThread.nativeThreadRef.driver !== providerThread.driver) {
          return yield* refuse("identity_mismatch");
        }
        const session = yield* sessions.get(input.providerSessionId).pipe(
          Effect.mapError((cause) => refuse("runtime_unavailable", cause)),
        );
        if (Option.isNone(session)) return yield* refuse("runtime_unavailable");
        const runtime = session.value;
        if (runtime.providerSessionId !== input.providerSessionId ||
          runtime.instanceId !== providerThread.providerInstanceId || runtime.driver !== providerThread.driver) {
          return yield* refuse("identity_mismatch");
        }
        if (runtime.readNativeGoal === undefined) {
          return yield* refuse("unsupported", `${runtime.driver} has no fresh native goal authority read.`);
        }
        const raw = yield* runtime.readNativeGoal({ providerThread }).pipe(
          Effect.mapError((cause) => refuse("read_failed", cause)),
        );
        if (raw === null) return yield* refuse("goal_unavailable");
        const goal = yield* Schema.decodeUnknownEffect(ProviderAdapter.ProviderAdapterV2NativeGoal)(raw).pipe(
          Effect.mapError((cause) => refuse("read_failed", cause)),
        );
        if (goal.threadId !== providerThread.nativeThreadRef.nativeId) return yield* refuse("identity_mismatch");
        if (goal.status !== "active") return yield* refuse("goal_inactive");
        if (!Object.hasOwn(goal, "tokenBudget") || goal.tokenBudget === undefined) return yield* refuse("budget_unavailable");
        // Explicit raw native null is the matching protocol's no-cap mode, not
        // a default for missing/undecodable data. Host admission still binds the
        // installed protocol and actual account/goal/resource authority.
        if (goal.tokenBudget !== null && goal.tokensUsed >= goal.tokenBudget) return yield* refuse("budget_exhausted");
        // Revalidate the durable binding after the asynchronous native reply. The
        // caller's original-thread lock retains canonical Stop/queue authority.
        const current = yield* projections.getThreadRecords(input.threadId, ["runs", "providerThreads"], {
          runIds: [input.sourceRunId],
        }).pipe(Effect.mapError((cause) => refuse("target_unavailable", cause)));
        const bound = current.providerThreads.find((thread) => thread.id === input.providerThreadId);
        if (bound?.providerSessionId !== input.providerSessionId ||
          bound.nativeThreadRef?.nativeId !== goal.threadId || bound.nativeThreadRef.driver !== providerThread.driver ||
          current.runs.find((run) => run.id === input.sourceRunId)?.providerThreadId !== input.providerThreadId) {
          return yield* refuse("identity_mismatch");
        }
        return goal;
      }),
      interrupt: (input) =>
        Effect.gen(function* () {
          const loaded = yield* load({ ...input, operation: "interrupt" });
          const session = Option.isSome(loaded.session)
            ? loaded.session
            : yield* sessions.get(input.providerSessionId);
          if (Option.isNone(session)) return;
          if (input.subagent !== undefined) {
            if (session.value.stopSubagent === undefined) {
              return yield* new ProviderTurnControlError({
                threadId: input.threadId,
                operation: "interrupt",
                providerTurnId: input.providerTurnId,
                cause: "The provider does not support stopping a native subagent.",
              });
            }
            yield* session.value.stopSubagent({
              providerThread: loaded.providerThread,
              nativeTaskId: input.subagent.nativeTaskId,
            });
            return;
          }
          // A settled turn reaches its adapter too: only the adapter knows
          // whether it still runs work for the thread, and each one either
          // stops it or reports there is nothing left to stop. Background work
          // the projection still shows is settled by the orchestrator after.
          yield* session.value.interruptTurn({
            providerThread: loaded.providerThread,
            providerTurnId: loaded.providerTurn.id,
            requestRuntimeRestart: true,
          });
          // Give native terminal ingestion time to finish before the Stop
          // follow-up repairs a run whose provider no longer reports on it.
          // The wait is real time: ingestion runs on other fibers and never
          // advances a test clock, so a test clock would hold Stop forever.
          yield* Effect.gen(function* () {
            const deadline = (yield* Clock.currentTimeMillis) + 2_000;
            while (
              loaded.providerTurn.status === "running" &&
              (yield* Clock.currentTimeMillis) < deadline
            ) {
              const current = yield* projections.getProviderControlContext(input.threadId, input);
              if (
                current.providerTurn?.status !== "running" &&
                current.attempt?.status !== "running"
              ) {
                return;
              }
              yield* Effect.sleep("10 millis");
            }
          }).pipe(Effect.provideService(Clock.Clock, Clock.Clock.defaultValue()));
        }).pipe(
          Effect.mapError((cause) =>
            isProviderTurnControlError(cause)
              ? cause
              : new ProviderTurnControlError({
                  threadId: input.threadId,
                  operation: "interrupt",
                  providerTurnId: input.providerTurnId,
                  cause,
                }),
          ),
        ),
      interruptAndAwaitTerminal: (input) =>
        Effect.gen(function* () {
          const loaded = yield* load({ ...input, operation: "restart" });
          if (Option.isNone(loaded.session)) {
            // No live adapter: nothing can emit a terminal provider-turn update
            // from interrupt. Do not poll for projection terminalization or the
            // restart effect stalls; let detach/start proceed.
            if (loaded.providerTurn.status === "running") {
              yield* Effect.logWarning(
                "Provider restart interrupt skipped; no live session for a still-projected running turn",
                {
                  threadId: input.threadId,
                  providerSessionId: input.providerSessionId,
                  providerTurnId: input.providerTurnId,
                },
              );
            }
            return;
          }

          yield* loaded.session.value.interruptTurn({
            providerThread: loaded.providerThread,
            providerTurnId: loaded.providerTurn.id,
          });

          for (let remaining = 1_000; remaining > 0; remaining -= 1) {
            const { providerTurn, attempt } = yield* projections.getProviderControlContext(
              input.threadId,
              {
                providerThreadId: input.providerThreadId,
                providerTurnId: input.providerTurnId,
                attemptId: input.interruptedAttemptId,
              },
            );
            if (
              providerTurn !== undefined &&
              providerTurn.status !== "running" &&
              attempt !== undefined &&
              attempt.status !== "running"
            ) {
              return;
            }
            // Provider terminal events are projected on a detached ingestion
            // fiber. Yield through the Node event loop instead of sleeping on
            // Effect's clock so deterministic runtimes cannot deadlock a
            // command that is waiting for that projection.
            yield* yieldToRuntime;
          }
          return yield* new ProviderTurnControlError({
            threadId: input.threadId,
            operation: "restart",
            providerTurnId: input.providerTurnId,
            cause: `Provider turn ${input.providerTurnId} did not terminalize before restart.`,
          });
        }).pipe(
          Effect.mapError((cause) =>
            isProviderTurnControlError(cause)
              ? cause
              : new ProviderTurnControlError({
                  threadId: input.threadId,
                  operation: "restart",
                  providerTurnId: input.providerTurnId,
                  cause,
                }),
          ),
        ),
      steer: (input) =>
        Effect.gen(function* () {
          const context = yield* projections.getProviderControlContext(input.threadId, input);
          const ownership = context.message?.delegatedCompletion;
          if (ownership !== undefined) {
            const projection = yield* projections.getThreadRecords(input.threadId, ["runs"], {
              runIds: [ownership.parentRunId],
            });
            const cohort = projection.runs.find(
              (run) => run.id === ownership.parentRunId,
            )?.delegatedCompletion;
            if (
              cohort?.disposition !== "open" ||
              cohort.delivery?.messageId !== input.messageId ||
              cohort.delivery.generation !== ownership.generation ||
              cohort.delivery.taskIds.length === 0
            )
              return;
          }
          const loaded = yield* load({ ...input, operation: "steer" });
          if (Option.isNone(loaded.session)) return;
          const { message, run } = loaded.context;
          if (message === undefined || run === undefined) {
            return yield* new ProviderTurnControlError({
              threadId: input.threadId,
              operation: "steer",
              providerTurnId: input.providerTurnId,
              cause: "The persisted steering message or target run is missing.",
            });
          }
          yield* loaded.session.value
            .steerTurn({
              threadId: input.threadId,
              runId: run.id,
              providerThread: loaded.providerThread,
              providerTurnId: loaded.providerTurn.id,
              message: {
                messageId: message.id,
                text: projectComposerContextForProvider({
                  text: message.text,
                  records: message.context?.records ?? [],
                }),
                attachments: message.attachments,
                createdBy: message.createdBy,
                creationSource: message.creationSource,
                ...(message.scheduledTaskId === undefined
                  ? {}
                  : { scheduledTaskId: message.scheduledTaskId }),
                ...(message.senderThreadId === undefined
                  ? {}
                  : { senderThreadId: message.senderThreadId }),
              },
            })
            .pipe(
              Effect.catch((cause) =>
                Effect.gen(function* () {
                  const current = yield* projections.getProviderControlContext(
                    input.threadId,
                    input,
                  );
                  return yield* new ProviderTurnControlError({
                    threadId: input.threadId,
                    operation: "steer",
                    providerTurnId: input.providerTurnId,
                    turnCompleted: current.providerTurn?.status === "completed",
                    cause,
                  });
                }),
              ),
            );
        }).pipe(
          Effect.mapError((cause) =>
            isProviderTurnControlError(cause)
              ? cause
              : new ProviderTurnControlError({
                  threadId: input.threadId,
                  operation: "steer",
                  providerTurnId: input.providerTurnId,
                  cause,
                }),
          ),
        ),
    });
  }),
);
