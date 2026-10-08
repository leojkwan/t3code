import { ThreadId, type OrchestrationV2GoalDispatchPolicy } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

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
