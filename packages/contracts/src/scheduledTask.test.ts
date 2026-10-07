import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import {
  MIN_SCHEDULED_TASK_INTERVAL_MS,
  ScheduledTaskSchedule,
  ScheduledTaskUpsertSchedule,
} from "./scheduledTask.ts";

const decodeSchedule = Schema.decodeUnknownSync(ScheduledTaskSchedule);
const decodeUpsertSchedule = Schema.decodeUnknownSync(ScheduledTaskUpsertSchedule);

const feedbackPolicy = {
  type: "agent_feedback",
  ownerThreadId: "thread:product-owner",
  humanMessageId: "message:human-request",
  channelId: "C_PRODUCT",
  threadTs: "1791356867.754099",
  humanRequestTs: "1791356867.754099",
  peers: ["U_HERMES", "U_G"],
};

describe("ScheduledTaskSchedule", () => {
  it("keeps legacy sub-minute persisted schedules readable", () => {
    expect(
      decodeSchedule({
        type: "interval",
        everyMs: MIN_SCHEDULED_TASK_INTERVAL_MS - 1,
      }),
    ).toEqual({
      type: "interval",
      everyMs: MIN_SCHEDULED_TASK_INTERVAL_MS - 1,
    });
  });

  it("still rejects corrupt non-positive persisted intervals", () => {
    expect(() => decodeSchedule({ type: "interval", everyMs: 0 })).toThrow();
  });
});

describe("ScheduledTaskUpsertSchedule", () => {
  it("preserves a fixed owner and human request on an opted-in feedback webhook", () => {
    expect(
      decodeUpsertSchedule({
        type: "webhook",
        signature: { header: "x-feedback-signature", encoding: "hex", prefix: "sha256=" },
        deliveryPolicy: feedbackPolicy,
      }),
    ).toMatchObject({ deliveryPolicy: feedbackPolicy });
  });

  it("rejects feedback policies with more than two invited peers", () => {
    expect(() =>
      decodeUpsertSchedule({
        type: "webhook",
        deliveryPolicy: { ...feedbackPolicy, peers: ["U_HERMES", "U_G", "U_THIRD"] },
      }),
    ).toThrow();
  });
  it("accepts interval schedules at the one-minute minimum", () => {
    expect(
      decodeUpsertSchedule({
        type: "interval",
        everyMs: MIN_SCHEDULED_TASK_INTERVAL_MS,
      }),
    ).toEqual({
      type: "interval",
      everyMs: MIN_SCHEDULED_TASK_INTERVAL_MS,
    });
  });

  it("rejects interval schedules more frequent than once per minute", () => {
    expect(() =>
      decodeUpsertSchedule({
        type: "interval",
        everyMs: MIN_SCHEDULED_TASK_INTERVAL_MS - 1,
      }),
    ).toThrow();
  });
});
