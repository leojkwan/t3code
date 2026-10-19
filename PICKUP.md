# Historical T3 goal and ACP fixes

This branch preserves two local source changes for reference. It does not install or activate them, and neither change is ready to merge into current T3. At upstream main `9077e818d836ec6e9cc373d7469034cccc593a5a`, checked October 19, 2026, `GoalCommand.ts` and `Adapters/AcpAdapterV2.ts` are absent. Locate the current owners and contracts before adapting either fix.

| Patch | Local source commit | Exact parent to which the patch applies |
| --- | --- | --- |
| `patches/codex-goal-command.patch` | `70753be13242e7673186088b631276a61aa41230` | `4ee6bfd50ef4a089440d5c3662db2298da9cc50e` |
| `patches/legacy-acp-model-selection.patch` | `a7141781fafd12a62df380f0917b9183a07e34c0` | `b4381985db55755ff15340d118160c14006ced13` |

The goal patch adds a local `/goal` command for setting, reading, pausing, resuming and clearing a thread goal, plus persisted state and continuation checks. It spans nine source/test files. The older local goal head `28570eaa0215c087675d03beaf1fe429179b9bde` has exactly the same patch body after omitting only object IDs and hunk line coordinates, so it needs no duplicate patch here. Before adoption, review persistence validation and recovery, cancellation, continuation identity, current goal ownership and provider-command routing. The old loader returns an empty store for unreadable or invalid state; that behavior needs explicit review before use.

The ACP patch spans two source/test files. It applies a stored model through the legacy session-model RPC only for an ACP v1 agent that advertises the requested model. It refuses unsupported or unadvertised selections. Adapt against the current ACP implementation and protocol policy before use.

Both named parents are contained by the checked upstream main, so their base source remains available there. Each patch passed `git apply --check` on source copies from its exact parent. Applying both patches reproduced all eleven expected target Git blobs; nine existing parent blobs were independently hashed. This proves historical patch preservation, not present-day compatibility. Unit tests, TypeScript, full builds, installed behavior, providers and E2E were NOT RUN. The included tests use synthetic data/replay frames. No local runtime, configuration, credentials, conversations, customer data or old commit history is included.

The older child-result implementation at `a9b3edf55e67bfd2fb5490039d99dce8a10a7a2f` has the same implementation patch body as the already preserved [child-result branch](https://github.com/leojkwan/t3code/tree/archive/child-result-pairing-20261018) at `113e7204b656e89f5bb389f0a4b2d35a665185b9`. Its test variants differ; no claim is made that the complete histories are identical. Original local refs and checkouts remain retained.

Next owner: the current T3 source maintainer compares these ideas with the current architecture and runs the relevant behavior checks before any adaptation. No automatic merge, runtime change or new scheduler is requested.
