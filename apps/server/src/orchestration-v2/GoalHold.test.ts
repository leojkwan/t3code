import { expect, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as GoalHold from "./GoalHold.ts";

it("refuses holds before exhausted budgets and reserves the full request batch", () => {
  const root = ThreadId.make("root");
  const child = ThreadId.make("child");
  const policy = { hold: null, limit: 3, used: 2, grant: null };
  expect(GoalHold.refusal(policy, child, root, 1)).toBeUndefined();
  expect(GoalHold.refusal(policy, child, root, 2)).toMatchObject({
    _tag: "GoalDispatchBudgetError",
    goalThreadId: root,
    threadId: child,
    used: 2,
    limit: 3,
  });
  expect(
    GoalHold.refusal({ ...policy, hold: "operator-stop", used: 3 }, child, root),
  ).toMatchObject({
    _tag: "GoalDispatchHeldError",
    hold: "operator-stop",
    goalThreadId: root,
  });
  expect(GoalHold.refusal({ ...GoalHold.emptyPolicy, used: 100 }, child, root)).toBeUndefined();
});
