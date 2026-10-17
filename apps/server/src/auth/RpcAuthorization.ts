import {
  CLIENT_GUARDED_RPC_SCOPES,
  type DeviceListInput,
  type AuthSessionPurpose,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2AppThread,
  type OrchestrationV2Run,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderFailure,
  type ScheduledTask,
  CommandId,
  MessageId,
  OrchestrationV2Command,
  clientRpcRequiredScopes,
  authScopeRequiredResponse,
  AssetCreateUrlInput,
  AuthAccessReadScope,
  ServerSettingsPatch,
  ProviderInstanceMutation,
  requiredScopesForServerSettingsPatch,
  requiredScopesForProjectMutation,
  ProjectMutation,
  AuthSettingsWriteScope,
  AuthProvidersManageScope,
  AuthEnvironmentMaintainScope,
  AuthFilesystemReadScope,
  AuthFilesystemWriteScope,
  AuthDiagnosticsReadScope,
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthPreviewOperateScope,
  AuthRelayReadScope,
  AuthRelayWriteScope,
  AuthTerminalOperateScope,
  ORCHESTRATION_V2_WS_METHODS,
  AuthTerminalReadScope,
  type AuthEnvironmentScope,
  EnvironmentAuthorizationError,
  RpcScopeAuthorization,
  WS_METHODS,
  WsRpcGroup,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as Schema from "effect/Schema";
import * as Layer from "effect/Layer";
import type * as RpcGroup from "effect/rpc/RpcGroup";
import type { SessionAuthorization } from "./SessionStore.ts";
import type { ProviderAdapterV2NativeGoal } from "@t3tools/provider-core/server/ProviderAdapter";

type WsRpcMethod = RpcGroup.Rpcs<typeof WsRpcGroup>["_tag"];

/** Carried from verified server auth, never decoded from a command body. */
export const AuthenticatedSessionPurpose = Context.Reference<AuthSessionPurpose | null | undefined>(
  "t3/auth/AuthenticatedSessionPurpose",
  { defaultValue: () => undefined },
);

export const AuthenticatedSession = Context.Reference<SessionAuthorization | undefined>(
  "t3/auth/AuthenticatedSession", { defaultValue: () => undefined },
);

export class OriginalTaskAdmissionRefused extends Schema.TaggedError<OriginalTaskAdmissionRefused>()(
  "OriginalTaskAdmissionRefused", { reason: Schema.String },
) {}

export interface OriginalTaskRecoveryTarget {
  readonly session: SessionAuthorization;
  readonly operationId: CommandId;
  readonly thread: OrchestrationV2AppThread;
  readonly sourceRun: OrchestrationV2Run;
  readonly originalInput: OrchestrationV2ConversationMessage;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly failure: OrchestrationV2ProviderFailure;
  readonly nativeGoal: ProviderAdapterV2NativeGoal;
}

/** A schedule is authorized through its accepted native creation/enable binding, not a failed input. */
export interface ScheduledTaskFireTarget {
  readonly type: "scheduled_fire";
  readonly task: ScheduledTask;
  readonly commandId: CommandId;
  readonly messageId: MessageId;
  readonly firedAt: string;
}

/** Set only inside the native admission callback after current SID verification. */
export const AuthenticatedScheduledTaskFire = Context.Reference<ScheduledTaskFireTarget | undefined>(
  "t3/auth/AuthenticatedScheduledTaskFire", { defaultValue: () => undefined },
);

export interface ScheduledTaskRunTarget {
  readonly type: "scheduled_run";
  readonly effectId: string;
  readonly thread: OrchestrationV2AppThread;
  readonly run: OrchestrationV2Run;
  readonly originalInput: OrchestrationV2ConversationMessage;
  readonly providerThread: OrchestrationV2ProviderThread;
}

export interface ScheduledTaskNativeBinding {
  /** Issuer-owned and currently verified; never a SID supplied in a task or command. */
  readonly session: SessionAuthorization;
  /** Native owner account identity, compared with the backend usage response. */
  readonly expectedAccountId: string;
}

export class ScheduledTaskAdmissionRefused extends Schema.TaggedError<ScheduledTaskAdmissionRefused>()(
  "ScheduledTaskAdmissionRefused", {
    reason: Schema.Literals(["scheduled_binding_unavailable", "session_authority_unavailable",
      "task_changed", "run_binding_changed", "owned_join_unavailable"]),
  },
) {}

/**
 * An existing native host supplies this capability, not a command body. It must
 * bind this verified SID to the actual current Shadow owner/claim and personal
 * target ancestry, validate the freshly read native goal identity/accounting
 * against its actual owning goal/hold/Stop/budget, and hold the existing resource admission
 * through use. Missing or stale authority refuses. No T3 projection, actor,
 * caller receipt text or client presence is an authority substitute.
 * It must bind the installed native runtime/protocol to the explicit-null
 * no-token-cap contract before admitting that mode. Public source or successful
 * schema decoding alone does not prove the installed association or account limits.
 *
 * No installed implementation is assumed. Without the host binding, R2's
 * refusal fallback remains in force. This is admission, not a controller or a
 * lifetime lease on a settled launch effect.
 */
export class OriginalTaskRecoveryAdmission extends Context.Service<
  OriginalTaskRecoveryAdmission,
  { readonly withAdmission: <A, E, R>(
    target: OriginalTaskRecoveryTarget, use: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | OriginalTaskAdmissionRefused, R>;
    /**
     * Optional until the existing native owner implements the accepted task ->
     * SID/Shadow/personal/resource binding. No production positive is supplied
     * by T3. Preparation and invalidation I/O must not hold auth/command locks.
     */
    readonly withScheduledTaskAdmission?: <A, E, R>(
      target: ScheduledTaskFireTarget,
      use: (binding: ScheduledTaskNativeBinding) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | ScheduledTaskAdmissionRefused | OriginalTaskAdmissionRefused, R>;
    /**
     * Resolve the accepted task and exact outbox/run/attempt again. Keep native
     * policy invalidation active until use's owned join completes. A failed or
     * unknown join must not become a resource-release receipt.
     */
    readonly withScheduledRunAdmission?: <A, E, R>(
      target: ScheduledTaskRunTarget,
      use: (binding: ScheduledTaskNativeBinding) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | ScheduledTaskAdmissionRefused | OriginalTaskAdmissionRefused, R>;
  }
>()("t3/auth/OriginalTaskRecoveryAdmission") {}

export function originalTaskRecoveryIdentity(
  threadId: string, request: { readonly sourceRunId: string; readonly originalInputMessageId: string },
) {
  const key = [threadId, request.sourceRunId, request.originalInputMessageId]
    .map(encodeURIComponent).join(":");
  return { commandId: CommandId.make(`automation-recovery:${key}`),
    messageId: MessageId.make(`automation-recovery-input:${key}`) };
}

/** A recovery request carries identities only; new content/options require human intake. */
export function isOriginalTaskRecoveryRequest(command: OrchestrationV2ServerCommand): boolean {
  return command.type === "message.dispatch" && command.automationContinuation !== undefined &&
    command.text === "" && command.attachments.length === 0 && command.context === undefined &&
    command.dispatchMode.type === "start_immediately" && command.deliveryIntent === undefined &&
    command.modelSelection === undefined && command.senderThreadId === undefined &&
    command.scheduledTaskId === undefined && command.sourcePlanRef === undefined &&
    command.notification === undefined && command.delegatedCompletion === undefined &&
    command.titleSeed === undefined && command.restartContinuationOfRunId === undefined &&
    command.usageLimitContinuationOfRunId === undefined && command.manualContinuationOfRunId === undefined &&
    command.usageLimitRecoveryRequestId === undefined;
}

const isClientCommand = Schema.is(OrchestrationV2Command);

export const AUTOMATION_AUTHORITY_UNAVAILABLE =
  "Automated continuation is unavailable: current original-task, Stop, native goal, budget and resource authority are not bound.";

/** Refusal-first until the existing native/Shadow admission bridge is bound. */
export function automationCommandRefusal(
  purpose: AuthSessionPurpose | null | undefined,
  command: OrchestrationV2ServerCommand,
  hasRecoveryAdmission = false,
): string | undefined {
  if (command.type === "message.dispatch" && command.automationContinuation !== undefined) {
    return purpose === "automation"
      ? hasRecoveryAdmission && isOriginalTaskRecoveryRequest(command)
        ? undefined : AUTOMATION_AUTHORITY_UNAVAILABLE
      : "Automated recovery requires an issuer-classified automation session.";
  }
  // Native provider/effect bookkeeping is not client input and must still
  // settle Stop/cancellation. The WS decoder cannot admit these internal commands.
  if (purpose !== "automation" || !isClientCommand(command)) return undefined;
  // Until the real admission bridge exists, only safety commands are permitted.
  // A model/provider change can restart work too, so default-allow is unsafe.
  switch (command.type) {
    case "thread.stop":
    case "run.interrupt":
    case "subagent.stop":
    case "thread.user-input.dismiss":
      return undefined;
    case "runtime-request.respond":
      return (command.decision === "cancel" || command.decision === "decline") &&
        command.answers === undefined && command.additionalAnswers === undefined &&
        command.attachmentsByQuestionId === undefined
        ? undefined
        : AUTOMATION_AUTHORITY_UNAVAILABLE;
    default:
      return AUTOMATION_AUTHORITY_UNAVAILABLE;
  }
}


/**
 * Keep authorization coverage coupled to the RPC group itself. Adding an RPC to
 * `WsRpcGroup` without choosing a scope is a type error instead of a production
 * runtime failure.
 */
export const RPC_REQUIRED_SCOPES = {
  ...CLIENT_GUARDED_RPC_SCOPES,
  [ORCHESTRATION_V2_WS_METHODS.dispatchCommand]: AuthOrchestrationOperateScope,
  [ORCHESTRATION_V2_WS_METHODS.getWorkflowScript]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.stopWorkflow]: AuthOrchestrationOperateScope,
  [ORCHESTRATION_V2_WS_METHODS.getTurnDiff]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.getFullThreadDiff]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.searchThreads]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.searchThread]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.searchThreadStream]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.getArchivedShellSnapshot]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.getThreadProjection]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.getTurnItem]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.launchThread]: AuthOrchestrationOperateScope,
  [ORCHESTRATION_V2_WS_METHODS.getLaunchReceipt]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.subscribeArchivedShell]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.subscribeShell]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.subscribeThread]: AuthOrchestrationReadScope,
  [WS_METHODS.projectsMutate]: AuthOrchestrationOperateScope,
  [WS_METHODS.serverProbe]: AuthOrchestrationReadScope,
  [WS_METHODS.serverGetConfig]: AuthOrchestrationReadScope,
  [WS_METHODS.serverRefreshProviders]: AuthOrchestrationReadScope,
  [WS_METHODS.serverUpdateProvider]: AuthProvidersManageScope,
  [WS_METHODS.providerAuthStart]: AuthProvidersManageScope,
  [WS_METHODS.providerConsumeResetCredit]: AuthProvidersManageScope,
  [WS_METHODS.providerAuthComplete]: AuthProvidersManageScope,
  [WS_METHODS.chatGptReconnectProfile]: AuthProvidersManageScope,
  [WS_METHODS.chatGptImportProfile]: AuthProvidersManageScope,
  [WS_METHODS.chatGptHandoffSubscribe]: AuthProvidersManageScope,
  [WS_METHODS.codexAuthCallbackSubscribe]: AuthProvidersManageScope,
  [WS_METHODS.providerAuthRespond]: AuthProvidersManageScope,
  [WS_METHODS.providerAuthCancel]: AuthProvidersManageScope,
  [WS_METHODS.providerAuthLogout]: AuthProvidersManageScope,
  [WS_METHODS.providerAuthSubscribe]: AuthProvidersManageScope,
  [WS_METHODS.providerInstallStart]: AuthProvidersManageScope,
  [WS_METHODS.providerInstallCancel]: AuthProvidersManageScope,
  [WS_METHODS.providerInstallSubscribe]: AuthOrchestrationReadScope,
  [WS_METHODS.providerInstallRemove]: AuthProvidersManageScope,
  [WS_METHODS.serverUpdateServer]: AuthEnvironmentMaintainScope,
  [WS_METHODS.serverUpdateServerWithProgress]: AuthEnvironmentMaintainScope,
  [WS_METHODS.serverCommitDesktopUpdate]: AuthEnvironmentMaintainScope,
  [WS_METHODS.serverUpsertKeybinding]: AuthSettingsWriteScope,
  [WS_METHODS.serverRemoveKeybinding]: AuthSettingsWriteScope,
  [WS_METHODS.serverGetStorageCleanupReport]: AuthOrchestrationReadScope,
  [WS_METHODS.serverGetSettings]: AuthOrchestrationReadScope,
  [WS_METHODS.serverUpdateSettings]: AuthSettingsWriteScope,
  [WS_METHODS.serverSearchAcpRegistry]: AuthOrchestrationReadScope,
  [WS_METHODS.serverPrepareAcpRegistryAgent]: AuthProvidersManageScope,
  [WS_METHODS.serverUninstallAcpRegistryManagedBinary]: AuthProvidersManageScope,
  [WS_METHODS.serverAcceptAcpRegistryUrlAuth]: AuthProvidersManageScope,
  [WS_METHODS.serverListAcpRegistrySessions]: AuthOrchestrationReadScope,
  [WS_METHODS.serverImportAcpRegistrySession]: AuthOrchestrationOperateScope,
  [WS_METHODS.serverDeleteAcpRegistrySession]: AuthOrchestrationOperateScope,
  [WS_METHODS.serverListAcpRegistryProviders]: AuthOrchestrationReadScope,
  [WS_METHODS.serverSetAcpRegistryProvider]: AuthProvidersManageScope,
  [WS_METHODS.serverDisableAcpRegistryProvider]: AuthProvidersManageScope,
  [WS_METHODS.serverLogoutAcpRegistry]: AuthProvidersManageScope,
  [WS_METHODS.serverDiscoverSourceControl]: AuthOrchestrationReadScope,
  [WS_METHODS.serverGetTraceDiagnostics]: AuthDiagnosticsReadScope,
  [WS_METHODS.serverGetProcessDiagnostics]: AuthDiagnosticsReadScope,
  // Load-balancing new threads reads host load; that is part of operating
  // threads, not of inspecting diagnostics.
  [WS_METHODS.serverGetHostResources]: AuthOrchestrationReadScope,
  [WS_METHODS.serverGetProcessResourceHistory]: AuthDiagnosticsReadScope,
  [WS_METHODS.serverGetResourceTelemetryHistory]: AuthDiagnosticsReadScope,
  [WS_METHODS.serverRetryResourceTelemetry]: AuthDiagnosticsReadScope,
  [WS_METHODS.serverGetUsageSummary]: AuthDiagnosticsReadScope,
  [WS_METHODS.serverRefreshUsageRates]: AuthDiagnosticsReadScope,
  [WS_METHODS.serverSignalProcess]: AuthEnvironmentMaintainScope,
  [WS_METHODS.serverReportClientActivity]: AuthOrchestrationReadScope,
  [WS_METHODS.serverReportHostPowerState]: AuthEnvironmentMaintainScope,
  [WS_METHODS.serverGetBackgroundPolicy]: AuthOrchestrationReadScope,
  [WS_METHODS.scheduledTasksList]: AuthOrchestrationReadScope,
  [WS_METHODS.scheduledTasksSubscribe]: AuthOrchestrationReadScope,
  [WS_METHODS.secretsAnswerRequest]: AuthOrchestrationOperateScope,
  // Delivery logs hold request bodies, so they need the same scope as the URL.
  [WS_METHODS.scheduledTasksListWebhookDeliveries]: AuthOrchestrationOperateScope,
  [WS_METHODS.scheduledTasksGetWebhookDelivery]: AuthOrchestrationOperateScope,
  [WS_METHODS.cloudGetRelayClientStatus]: AuthRelayReadScope,
  [WS_METHODS.cloudInstallRelayClient]: AuthRelayWriteScope,
  [WS_METHODS.pullRequestsList]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsListStats]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsSummary]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsRouting]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsRoutingIdentity]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsStack]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsLinkedThreads]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsDetail]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsPreview]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsChecks]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsActivity]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsReviewGuide]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsThreadComments]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsDiffFileContents]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsFilesViewed]: AuthOrchestrationReadScope,
  // Read scope like the reads it un-caches: refreshing is part of reading, and a read-only
  // client pressing refresh must not be told it may not look again.
  [WS_METHODS.pullRequestsInvalidate]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsReportState]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsSubscribeRefreshes]: AuthOrchestrationReadScope,
  // The candidate list is a read like the detail beside it; asking somebody for a review is a
  // write like every other one.
  [WS_METHODS.pullRequestsReviewerCandidates]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsLabelCandidates]: AuthOrchestrationReadScope,
  [WS_METHODS.sourceControlLookupRepository]: AuthOrchestrationReadScope,
  [WS_METHODS.subscribeProjectClones]: AuthOrchestrationReadScope,
  [WS_METHODS.projectsListEntries]: AuthFilesystemReadScope,
  [WS_METHODS.projectsReadFile]: AuthFilesystemReadScope,
  [WS_METHODS.projectsSearchContents]: AuthFilesystemReadScope,
  [WS_METHODS.projectsSearchEntries]: AuthFilesystemReadScope,
  [WS_METHODS.projectsWriteFile]: AuthFilesystemWriteScope,
  [WS_METHODS.projectsEnsureScratch]: AuthOrchestrationOperateScope,
  [WS_METHODS.projectsCreateNew]: AuthOrchestrationOperateScope,
  [WS_METHODS.shellOpenInEditor]: AuthOrchestrationOperateScope,
  [WS_METHODS.filesystemBrowse]: AuthFilesystemReadScope,
  [WS_METHODS.filesystemGetMetadata]: AuthFilesystemReadScope,
  [WS_METHODS.agentSessionsScan]: AuthOrchestrationReadScope,
  [WS_METHODS.agentSessionsImport]: AuthOrchestrationOperateScope,
  [WS_METHODS.assetsCreateUrl]: AuthOrchestrationReadScope,
  [WS_METHODS.assetsPersistChatAttachments]: AuthOrchestrationOperateScope,
  [WS_METHODS.attachmentsCreateUploadUrl]: AuthOrchestrationOperateScope,
  [WS_METHODS.attachmentsDelete]: AuthOrchestrationOperateScope,
  [WS_METHODS.providerUploadFeedback]: AuthOrchestrationOperateScope,
  // An app's tool calls can change things on its server, like a user action.
  [WS_METHODS.mcpAppsCallTool]: AuthOrchestrationOperateScope,
  [WS_METHODS.mcpAppsToolInfo]: AuthOrchestrationReadScope,
  [WS_METHODS.mcpAppsReadResource]: AuthOrchestrationReadScope,
  [WS_METHODS.mcpAppsUpdateModelContext]: AuthOrchestrationOperateScope,
  [WS_METHODS.subscribeVcsStatus]: AuthOrchestrationReadScope,
  [WS_METHODS.subscribeWorktreeSetup]: AuthOrchestrationReadScope,
  [WS_METHODS.worktreeSetupCancel]: AuthOrchestrationOperateScope,
  [WS_METHODS.subscribeResourceTelemetry]: AuthDiagnosticsReadScope,
  [WS_METHODS.vcsRefreshStatus]: AuthOrchestrationReadScope,
  [WS_METHODS.gitResolvePullRequest]: AuthOrchestrationReadScope,
  [WS_METHODS.vcsListRefs]: AuthOrchestrationReadScope,
  [WS_METHODS.reviewGetDiffPreview]: AuthFilesystemReadScope,
  [WS_METHODS.reviewGetDiffFileContents]: AuthFilesystemReadScope,
  [WS_METHODS.terminalOpen]: AuthTerminalOperateScope,
  [WS_METHODS.terminalAttach]: AuthTerminalOperateScope,
  [WS_METHODS.terminalObserve]: AuthTerminalReadScope,
  [WS_METHODS.terminalWrite]: AuthTerminalOperateScope,
  [WS_METHODS.terminalResize]: AuthTerminalOperateScope,
  [WS_METHODS.terminalClear]: AuthTerminalOperateScope,
  [WS_METHODS.terminalRestart]: AuthTerminalOperateScope,
  [WS_METHODS.terminalClose]: AuthTerminalOperateScope,
  [WS_METHODS.subscribeTerminalEvents]: AuthTerminalReadScope,
  [WS_METHODS.subscribeTerminalMetadata]: AuthTerminalReadScope,
  [WS_METHODS.previewOpen]: AuthPreviewOperateScope,
  [WS_METHODS.previewNavigate]: AuthPreviewOperateScope,
  [WS_METHODS.previewResize]: AuthPreviewOperateScope,
  [WS_METHODS.previewAdjust]: AuthPreviewOperateScope,
  [WS_METHODS.previewRefresh]: AuthPreviewOperateScope,
  [WS_METHODS.previewClose]: AuthPreviewOperateScope,
  [WS_METHODS.previewList]: AuthOrchestrationReadScope,
  [WS_METHODS.previewClearProfile]: AuthPreviewOperateScope,
  [WS_METHODS.previewReportProfiles]: AuthPreviewOperateScope,
  [WS_METHODS.previewReportStatus]: AuthPreviewOperateScope,
  [WS_METHODS.subscribePreviewEvents]: AuthOrchestrationReadScope,
  [WS_METHODS.subscribeDiscoveredLocalServers]: AuthOrchestrationReadScope,
  [WS_METHODS.deviceConfigure]: AuthSettingsWriteScope,
  [WS_METHODS.deviceTestHost]: AuthSettingsWriteScope,
  [WS_METHODS.deviceList]: AuthOrchestrationReadScope,
  [WS_METHODS.deviceOpen]: AuthOrchestrationOperateScope,
  [WS_METHODS.deviceClose]: AuthOrchestrationOperateScope,
  [WS_METHODS.deviceShutdown]: AuthOrchestrationOperateScope,
  [WS_METHODS.deviceDetail]: AuthOrchestrationReadScope,
  [WS_METHODS.deviceAction]: AuthOrchestrationOperateScope,
  [WS_METHODS.subscribeDeviceState]: AuthOrchestrationReadScope,
  [WS_METHODS.subscribeServerConfig]: AuthOrchestrationReadScope,
  [WS_METHODS.subscribeServerLifecycle]: AuthOrchestrationReadScope,
  [WS_METHODS.subscribeAuthAccess]: AuthAccessReadScope,
  [WS_METHODS.subscribeBackgroundPolicy]: AuthOrchestrationReadScope,
} as const satisfies Readonly<Record<WsRpcMethod, AuthEnvironmentScope>>;

export function requiredScopeForRpcMethod(method: string): AuthEnvironmentScope {
  if (!Object.hasOwn(RPC_REQUIRED_SCOPES, method)) {
    throw new Error(`RPC method ${method} has no declared authorization scope.`);
  }
  const requiredScope = RPC_REQUIRED_SCOPES[method as WsRpcMethod];
  if (requiredScope === undefined) {
    throw new Error(`RPC method ${method} has no declared authorization scope.`);
  }
  return requiredScope;
}

export const rpcAuthorizationError = (requiredScope: AuthEnvironmentScope) =>
  new EnvironmentAuthorizationError({
    message: `The authenticated token is missing required scope: ${requiredScope}.`,
    ...authScopeRequiredResponse(requiredScope),
  });

const SettingsUpdate = Schema.Struct({
  patch: ServerSettingsPatch,
  providerInstanceMutation: Schema.optionalKey(ProviderInstanceMutation),
});

const requiredScopesForSettingsUpdate = (payload: unknown) => {
  const input = Schema.decodeUnknownSync(SettingsUpdate)(payload);
  const scopes = requiredScopesForServerSettingsPatch(input.patch);
  if (input.providerInstanceMutation === undefined) return scopes;
  // An atomic provider mutation carries an empty patch unless it also changes settings.
  return Object.values(input.patch).every((value) => value === undefined)
    ? [AuthProvidersManageScope]
    : [...new Set([...scopes, AuthProvidersManageScope])];
};

const decodeProjectMutation = Schema.decodeUnknownSync(ProjectMutation);

const requiredScopesForRpcCall = (
  method: string,
  payload: unknown,
): ReadonlyArray<AuthEnvironmentScope> => {
  if (method === WS_METHODS.serverRetryResourceTelemetry) {
    return [AuthEnvironmentMaintainScope, AuthDiagnosticsReadScope];
  }
  if (method === WS_METHODS.assetsCreateUrl) {
    const { resource } = Schema.decodeUnknownSync(AssetCreateUrlInput)(payload);
    return [
      resource._tag === "workspace-file" ||
      resource._tag === "media-file" ||
      resource._tag === "draft-workspace-file"
        ? AuthFilesystemReadScope
        : AuthOrchestrationReadScope,
    ];
  }
  if (method === WS_METHODS.serverUpdateSettings) return requiredScopesForSettingsUpdate(payload);
  if (method === WS_METHODS.projectsMutate) {
    return requiredScopesForProjectMutation(decodeProjectMutation(payload));
  }
  const guarded = clientRpcRequiredScopes(method, payload);
  if (guarded.length > 0) return guarded;
  return [requiredScopeForRpcMethod(method)];
};

/** Authorizes every RPC on one connection against that connection's session scopes. */
export const layer = (
  scopes: ReadonlyArray<AuthEnvironmentScope>,
  purpose?: AuthSessionPurpose | null,
  checkCurrentAuthorization: Effect.Effect<void, EnvironmentAuthorizationError> = Effect.void,
) =>
  Layer.succeed(RpcScopeAuthorization)((effect, { rpc, payload }) => {
    const requiredScopes = requiredScopesForRpcCall(rpc._tag, payload);
    const requiredScope = requiredScopes.find((scope) => !scopes.includes(scope));
    if (requiredScope !== undefined) return Effect.fail(rpcAuthorizationError(requiredScope));
    // Dispatch performs the same refusal under its command lock. Other write
    // RPCs cannot bypass it through settings, provider or scheduled operations.
    if (
      purpose === "automation" &&
      requiredScopes.some((scope) => !scope.endsWith(":read")) &&
      rpc._tag !== ORCHESTRATION_V2_WS_METHODS.dispatchCommand &&
      rpc._tag !== ORCHESTRATION_V2_WS_METHODS.stopWorkflow
    ) {
      return Effect.fail(new EnvironmentAuthorizationError({
        message: AUTOMATION_AUTHORITY_UNAVAILABLE,
        requiredScope: requiredScopes.find((scope) => !scope.endsWith(":read")) ?? AuthOrchestrationOperateScope,
      }));
    }
    return checkCurrentAuthorization.pipe(
      Effect.andThen(effect), Effect.provideService(AuthenticatedSessionPurpose, purpose),
    );
  });

/** Retrying can install or restart tools even though ordinary listing is readable. */
export const requiredScopeForDeviceList = (input: DeviceListInput): AuthEnvironmentScope =>
  input.retryHostId || input.updateTool
    ? AuthOrchestrationOperateScope
    : AuthOrchestrationReadScope;
