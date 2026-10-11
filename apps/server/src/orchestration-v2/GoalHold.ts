import { ThreadId, type OrchestrationV2GoalDispatchPolicy } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export class GoalProviderDispatchError extends Schema.TaggedError<GoalProviderDispatchError>()(
  "GoalProviderDispatchError",
  {
    threadId: ThreadId,
    cause: Schema.optional(Schema.Defect()),
    heldAfterSequence: Schema.optional(Schema.Number),
  },
) {}

export interface ProviderDispatchGuardShape {
  readonly run: <A, E, R>(
    threadId: ThreadId,
    work: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | GoalProviderDispatchError, R>;
  readonly continue?: ProviderDispatchGuardShape["run"];
}

/** The outbox supplies custody; the provider services place it at the actual call. */
export class ProviderDispatchGuard extends Context.Reference<
  ProviderDispatchGuardShape | undefined
>("t3/GoalHold/ProviderDispatchGuard", {
  defaultValue: (): ProviderDispatchGuardShape | undefined => undefined,
}) {}

export const guardProviderCall = <A, E, R>(threadId: ThreadId, work: Effect.Effect<A, E, R>) =>
  Effect.flatMap(ProviderDispatchGuard, (guard) =>
    guard === undefined ? work : guard.run(threadId, work),
  );

/** Recheck after adapter preparation, without reserving a second request. */
export const guardProviderContinuation = <A, E, R>(threadId: ThreadId, work: Effect.Effect<A, E, R>) =>
  Effect.flatMap(ProviderDispatchGuard, (guard) =>
    guard?.continue === undefined ? work : guard.continue(threadId, work),
  );

export class GoalDispatchHeldError extends Schema.TaggedError<GoalDispatchHeldError>()(
  "GoalDispatchHeldError",
  { threadId: ThreadId, goalThreadId: ThreadId, hold: Schema.String },
) {
  override get message(): string {
    return `Goal ${this.goalThreadId} dispatch is held by ${this.hold} (target ${this.threadId}).`;
  }
}

export class GoalDispatchBudgetError extends Schema.TaggedError<GoalDispatchBudgetError>()(
  "GoalDispatchBudgetError",
  { threadId: ThreadId, goalThreadId: ThreadId, limit: Schema.Number, used: Schema.Number },
) {
  override get message(): string {
    return `Goal ${this.goalThreadId} dispatch budget exhausted: ${this.used}/${this.limit}.`;
  }
}

export class GoalBudgetGrantRequiredError extends Schema.TaggedError<GoalBudgetGrantRequiredError>()(
  "GoalBudgetGrantRequiredError",
  { goalThreadId: ThreadId, limit: Schema.Number },
) {
  override get message(): string {
    return `Raising goal ${this.goalThreadId} dispatch budget to ${this.limit} requires a recorded grant.`;
  }
}

export const emptyPolicy: OrchestrationV2GoalDispatchPolicy = {
  hold: null,
  limit: null,
  used: 0,
  grant: null,
};

export function refusal(
  policy: OrchestrationV2GoalDispatchPolicy,
  threadId: ThreadId,
  goalThreadId: ThreadId,
  requests = 1,
): GoalDispatchHeldError | GoalDispatchBudgetError | undefined {
  if (policy.hold !== null)
    return new GoalDispatchHeldError({ threadId, goalThreadId, hold: policy.hold });
  if (policy.limit !== null && policy.used + requests > policy.limit) {
    return new GoalDispatchBudgetError({
      threadId,
      goalThreadId,
      limit: policy.limit,
      used: policy.used,
    });
  }
  return undefined;
}
