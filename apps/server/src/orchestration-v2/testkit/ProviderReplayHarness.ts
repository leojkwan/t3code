import * as OtelEnvironment from "@t3tools/shared/otelEnvironment";
import { DEFAULT_SIGNAL_EXPORT } from "@t3tools/shared/observability";
import * as NodeServices from "@effect/platform-node/NodeServices";
import type { OrchestrationV2StoredEvent, ProviderDriverKind, ProviderReplayTranscript, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import type * as SqlClient from "effect/sql/SqlClient";
import type { MigrationError } from "effect/sql/Migrator";
import type { SqlError } from "effect/sql/SqlError";

import * as CheckpointStore from "../../checkpointing/CheckpointStore.ts";
import * as ServerConfig from "../../config.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as ThreadManagementService from "../ThreadManagementService.ts";
import * as McpSessionRegistryTestkit from "../../mcp/McpSessionRegistry.testkit.ts";
import * as VcsDriverRegistry from "../../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../../vcs/VcsProcess.ts";
import * as CheckpointCaptureService from "../CheckpointCaptureService.ts";
import * as CheckpointService from "../CheckpointService.ts";
import * as CheckpointRollbackService from "../CheckpointRollbackService.ts";
import * as CommandPolicy from "../CommandPolicy.ts";
import * as CommandReceiptStore from "../CommandReceiptStore.ts";
import * as ContextHandoffService from "../ContextHandoffService.ts";
import * as EffectOutbox from "../EffectOutbox.ts";
import * as EffectWorker from "../EffectWorker.ts";
import * as EventSink from "../EventSink.ts";
import * as EventStore from "../EventStore.ts";
import * as IdAllocator from "../IdAllocator.ts";
import * as Orchestrator from "../Orchestrator.ts";
import * as ProjectionStore from "../ProjectionStore.ts";
import * as ProjectStore from "../ProjectStore.ts";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import * as ProviderAuthService from "../../provider/ProviderAuthService.ts";
import * as ProviderContinuationRequests from "../ProviderContinuationRequests.ts";
import * as ProviderContinuationService from "../ProviderContinuationService.ts";
import * as ProviderEventIngestor from "../ProviderEventIngestor.ts";
import * as ProviderRuntimeRecoveryService from "../ProviderRuntimeRecoveryService.ts";
import * as ProviderSessionManager from "../ProviderSessionManager.ts";
import * as ProviderSwitchService from "../ProviderSwitchService.ts";
import * as ProviderTurnControlService from "../ProviderTurnControlService.ts";
import * as ProviderTurnStartService from "../ProviderTurnStartService.ts";
import * as ProviderTurnStartServiceTestkit from "../ProviderTurnStartService.testkit.ts";
import * as McpAppModelContext from "../../mcpApps/McpAppModelContext.ts";
import * as RunExecutionService from "../RunExecutionService.ts";
import * as RunFinalizationService from "../RunFinalizationService.ts";
import * as ThreadTitleRegenerationService from "../ThreadTitleRegenerationService.ts";
import * as RuntimePolicy from "../RuntimePolicy.ts";
import * as TurnItemPositionStore from "../TurnItemPositionStore.ts";
import * as RuntimeRequestService from "../RuntimeRequestService.ts";
import * as ThreadCommandExecutor from "../ThreadCommandExecutor.ts";
import * as ThreadForkService from "../ThreadForkService.ts";
import {
  runOrchestratorV2Scenario,
  type OrchestratorV2ScenarioStepError,
  type OrchestratorV2Scenario,
  type OrchestratorV2ScenarioResult,
} from "./OrchestratorScenario.ts";
import { makeProviderReplayGate, type ProviderReplayGate } from "./ProviderReplayGate.testkit.ts";

export function makeReplayServerConfig(
  scenario: string,
): Effect.Effect<
  ServerConfig.ServerConfig["Service"],
  PlatformError.PlatformError,
  FileSystem.FileSystem | Path.Path
> {
  const safeScenario = scenario.replace(/[^a-z0-9_-]+/gi, "-");
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const baseDir = yield* fs.makeTempDirectory({
      prefix: `t3-orchestration-v2-replay-${safeScenario}-`,
    });
    const stateDir = path.join(baseDir, "userdata");
    const logsDir = path.join(stateDir, "logs");
    const providerLogsDir = path.join(logsDir, "provider");
    const terminalLogsDir = path.join(logsDir, "terminals");
    const attachmentsDir = path.join(stateDir, "attachments");
    const environmentThemesDir = path.join(stateDir, "themes");
    const worktreesDir = path.join(baseDir, "worktrees");
    const providerStatusCacheDir = path.join(baseDir, "caches");

    for (const directory of [
      stateDir,
      logsDir,
      providerLogsDir,
      terminalLogsDir,
      attachmentsDir,
      environmentThemesDir,
      worktreesDir,
      providerStatusCacheDir,
    ]) {
      yield* fs.makeDirectory(directory, { recursive: true });
    }

    return {
      logLevel: "Error",
      traceMinLevel: "Info",
      traceTimingEnabled: true,
      traceBatchWindowMs: 200,
      traceMaxBytes: 10 * 1024 * 1024,
      traceMaxFiles: 10,
      otelEnvironment: OtelEnvironment.none,
      otlpTracesUrl: undefined,
      otlpMetricsUrl: undefined,
      otlpLogsUrl: undefined,
      otlpTracesExport: DEFAULT_SIGNAL_EXPORT,
      otlpMetricsExport: DEFAULT_SIGNAL_EXPORT,
      otlpLogsExport: DEFAULT_SIGNAL_EXPORT,
      mode: "web",
      port: 0,
      host: undefined,
      cwd: process.cwd(),
      baseDir,
      staticDir: undefined,
      devUrl: undefined,
      devAllowedOrigins: [],
      noBrowser: false,
      startupPresentation: "browser",
      tailscaleServeEnabled: false,
      tailscaleServePort: 443,
      desktopBootstrapToken: undefined,
      autoBootstrapProjectFromCwd: false,
      logWebSocketEvents: false,
      stateDir,
      dbPath: path.join(stateDir, "state.sqlite"),
      keybindingsConfigPath: path.join(stateDir, "keybindings.json"),
      settingsPath: path.join(stateDir, "settings.json"),
      providerStatusCacheDir,
      worktreesDir,
      attachmentsDir,
      browserArtifactsDir: path.join(stateDir, "browser-artifacts"),
      environmentThemesDir,
      logsDir,
      serverLogPath: path.join(logsDir, "server.log"),
      serverTracePath: path.join(logsDir, "server.trace.ndjson"),
      providerLogsDir,
      providerEventLogPath: path.join(providerLogsDir, "events.log"),
      terminalLogsDir,
      anonymousIdPath: path.join(stateDir, "anonymous-id"),
      environmentIdPath: path.join(stateDir, "environment-id"),
      serverRuntimeStatePath: path.join(stateDir, "server-runtime.json"),
      secretsDir: path.join(stateDir, "secrets"),
    };
  });
}

export interface OrchestratorV2ProviderReplayScenario<
  Transcript extends ProviderReplayTranscript = ProviderReplayTranscript,
> extends OrchestratorV2Scenario {
  readonly transcript: Transcript;
  readonly runtimePolicyOverride?: RuntimePolicy.RuntimePolicyV2Override;
}

export interface OrchestratorV2ProviderReplayHarness<
  Transcript extends ProviderReplayTranscript = ProviderReplayTranscript,
  Error = never,
> {
  readonly driver: ProviderDriverKind;
  readonly decodeTranscript: (
    transcript: ProviderReplayTranscript,
  ) => Effect.Effect<Transcript, Error>;
  readonly makeProviderAdapterRegistryLayer: (
    transcript: Transcript,
    options?: { readonly replayGate?: ProviderReplayGate },
  ) => Layer.Layer<ProviderAdapterRegistry.ProviderAdapterRegistryV2, Error>;
}

export function runOrchestratorV2ProviderReplayScenario<
  Transcript extends ProviderReplayTranscript,
  Error,
>(
  scenario: OrchestratorV2ProviderReplayScenario<Transcript>,
  harness: OrchestratorV2ProviderReplayHarness<Transcript, Error>,
  options: {
    readonly databaseLayer?: Layer.Layer<
      SqlClient.SqlClient,
      MigrationError | PlatformError.PlatformError | SqlError
    >;
    readonly runEffectWorker?: boolean;
    // Start continuation runs for provider wake turns, as the live runtime does.
    // Off by default: most fixtures record no wake turn.
    readonly runContinuationWorker?: boolean;
    // Reconcile a previous runtime's state before the effect worker starts,
    // as server startup does after a crash or restart.
    readonly recoverOnStartup?: boolean;
    readonly continueThreadsAfterServerUpdate?: boolean;
  } = {},
): Effect.Effect<
  OrchestratorV2ScenarioResult,
  | Orchestrator.OrchestratorV2Error
  | OrchestratorV2ScenarioStepError
  | Error
  | MigrationError
  | PlatformError.PlatformError
  | SqlError,
  never
> {
  const replayGate = makeProviderReplayGate(
    scenario.steps?.flatMap((step) =>
      step.type === "release_replay_gate" || step.type === "release_replay_gate_after_waiting"
        ? [step.label]
        : [],
    ) ?? [],
  );
  const layer = layerProviderReplay(scenario, harness, {
    ...options,
    replayGate,
  });

  return runOrchestratorV2Scenario(scenario, { replayGate }).pipe(Effect.provide(layer));
}

export function layerProviderReplay<Transcript extends ProviderReplayTranscript, Error>(
  scenario: OrchestratorV2ProviderReplayScenario<Transcript>,
  harness: OrchestratorV2ProviderReplayHarness<Transcript, Error>,
  options: {
    readonly databaseLayer?: Layer.Layer<
      SqlClient.SqlClient,
      MigrationError | PlatformError.PlatformError | SqlError
    >;
    readonly runEffectWorker?: boolean;
    // Start continuation runs for provider wake turns, as the live runtime does.
    // Off by default: most fixtures record no wake turn.
    readonly runContinuationWorker?: boolean;
    // Reconcile a previous runtime's state before the effect worker starts,
    // as server startup does after a crash or restart.
    readonly recoverOnStartup?: boolean;
    readonly continueThreadsAfterServerUpdate?: boolean;
    readonly replayGate?: ProviderReplayGate;
  } = {},
): Layer.Layer<
  Orchestrator.OrchestratorV2 | EffectWorker.OrchestrationEffectWorkerV2 | EventSink.EventSinkV2,
  Error | MigrationError | PlatformError.PlatformError | SqlError
> {
  const layerRegistry = harness.makeProviderAdapterRegistryLayer(
    scenario.transcript,
    options.replayGate === undefined ? {} : { replayGate: options.replayGate },
  );
  return layerWithRegistry(scenario, layerRegistry, options);
}

export interface ProviderDispatchObservation {
  readonly phase: "enter" | "return" | "failure";
  readonly threadId: string;
  readonly effectId: string;
  readonly attemptCount: number;
  readonly continuation?: { readonly leaseOwner: string | null };
  readonly heldAfterSequence?: number | null;
  readonly errorTag?: string | null;
  readonly causeTag?: string | null;
  readonly causeThreadId?: string | null;
  readonly causeGoalThreadId?: string | null;
}

// Default-absent ordering seam for one owned policy-stream consumer only.
export type PolicyStreamInput = NonNullable<Parameters<Orchestrator.OrchestratorV2Shape["streamStoredEventsFrom"]>[0]>;
export type PolicyStreamEntryObservation =
  | { readonly phase: "realSubscriptionEntered" | "realSubscriptionFinalized"; readonly input: PolicyStreamInput }
  | { readonly phase: "storedEventDelivered"; readonly input: PolicyStreamInput; readonly storedEvent: OrchestrationV2StoredEvent };
export interface PolicyStreamEntrySeam {
  readonly threadId: ThreadId;
  readonly beforeSubscribe: (input: PolicyStreamInput) => Effect.Effect<void>;
  readonly observe: (observation: PolicyStreamEntryObservation) => void;
}

export interface InterruptPhaseObservation {
  readonly phase: "interrupt-enter" | "interrupt-exit" | "control-read-enter" | "control-read-exit";
  readonly exit?: "Success" | "Failure";
  readonly providerTurnStatus?: string | null;
  readonly attemptStatus?: string | null;
}

export function layerWithRegistry<Error>(
  scenario: Pick<OrchestratorV2ProviderReplayScenario, "name" | "runtimePolicyOverride">,
  registryLayer: Layer.Layer<ProviderAdapterRegistry.ProviderAdapterRegistryV2, Error>,
  options: {
    readonly databaseLayer?: Layer.Layer<
      SqlClient.SqlClient,
      MigrationError | PlatformError.PlatformError | SqlError
    >;
    readonly runEffectWorker?: boolean;
    // Optional synchronous observation only; original services and results are forwarded.
    readonly observeInterrupt?: (observation: InterruptPhaseObservation) => void;
    // Test-owned observation of actual calls/results only; no extra gate invocation.
    readonly observeProviderDispatch?: (observation: ProviderDispatchObservation) => void;
    readonly policyStreamEntry?: PolicyStreamEntrySeam;
    // Optional setup fixture only; all other checkpoint consumers remain real.
    readonly captureBaseline?: Effect.Effect<void, CheckpointService.CheckpointServiceV2Error>;
    // Optional capability fixture; checkpoint metadata and persistence stay real.
    readonly checkpointRepositoryCheck?: CheckpointStore.CheckpointStore["Service"]["isGitRepository"];
    // Start continuation runs for provider wake turns, as the live runtime does.
    // Off by default: most fixtures record no wake turn.
    readonly runContinuationWorker?: boolean;
    // Reconcile a previous runtime's state before the effect worker starts,
    // as server startup does after a crash or restart.
    readonly recoverOnStartup?: boolean;
    readonly continueThreadsAfterServerUpdate?: boolean;
  } = {},
): Layer.Layer<
  | Orchestrator.OrchestratorV2
  | EffectWorker.OrchestrationEffectWorkerV2
  | EventSink.EventSinkV2
  | ProviderSessionManager.ProviderSessionManagerV2,
  Error | MigrationError | PlatformError.PlatformError | SqlError
> {
  const layerServerConfig = Layer.effect(
    ServerConfig.ServerConfig,
    makeReplayServerConfig(scenario.name).pipe(Effect.orDie),
  ).pipe(Layer.provide(NodeServices.layer));
  const layerRuntime =
    scenario.runtimePolicyOverride === undefined
      ? RuntimePolicy.layer
      : RuntimePolicy.layerWithOverride(scenario.runtimePolicyOverride).pipe(
          Layer.provide(RuntimePolicy.layer),
        );
  const layerDatabase = options.databaseLayer ?? SqlitePersistence.layerMemory;
  // One queue shared by the adapters, the orchestrator, and the worker, like
  // runtimeLayer.ts; layer memoization keeps it a single instance.
  const layerContinuationRequests =
    options.runContinuationWorker === true ? ProviderContinuationRequests.layer : Layer.empty;
  const layerProvidedRegistry = registryLayer.pipe(Layer.provide(layerContinuationRequests));
  const layerServerSettings = ServerSettings.layerTest({
    responseStreamingMode: "turn",
    ...(options.continueThreadsAfterServerUpdate === undefined
      ? {}
      : { continueThreadsAfterServerUpdate: options.continueThreadsAfterServerUpdate }),
  }).pipe(Layer.orDie);
  const layerStores = Layer.mergeAll(
    EventStore.layer,
    ProjectionStore.layer,
    ProjectStore.layer,
    CommandReceiptStore.layer,
    EffectOutbox.layer,
    TurnItemPositionStore.layer,
  ).pipe(Layer.provide(layerDatabase));
  const layerEventSinkProvided = EventSink.layerFromStores.pipe(
    Layer.provide(Layer.mergeAll(layerStores, layerDatabase)),
  );
  const layerCommandReceiptStoreProvided = CommandReceiptStore.layer.pipe(
    Layer.provide(layerDatabase),
  );
  const layerProviderEventIngestorProvided = ProviderEventIngestor.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerStores,
        layerEventSinkProvided,
        IdAllocator.layer,
        ThreadCommandExecutor.layer,
      ),
    ),
  );
  const layerVcsDriverRegistry = VcsDriverRegistry.layer.pipe(
    Layer.provide(VcsProcess.layer),
    Layer.provide(layerServerConfig),
    Layer.provide(NodeServices.layer),
  );
  const layerCheckpointStore = CheckpointStore.layer.pipe(
    Layer.provide(layerVcsDriverRegistry),
    Layer.provide(NodeServices.layer),
  );
  const checkpointRepositoryCheck = options.checkpointRepositoryCheck;
  const layerCheckpointService = checkpointRepositoryCheck === undefined
    ? CheckpointService.layer
    : CheckpointService.layer.pipe(
        Layer.updateService(CheckpointStore.CheckpointStore, (store) => ({
          ...store,
          isGitRepository: checkpointRepositoryCheck,
        })),
      );
  const layerCheckpointServiceProvided = layerCheckpointService.pipe(
    Layer.provide(Layer.mergeAll(layerCheckpointStore, IdAllocator.layer)),
  );
  const layerContextHandoffServiceProvided = ContextHandoffService.layer.pipe(
    Layer.provide(IdAllocator.layer),
  );
  const layerPersistence = Layer.mergeAll(
    layerStores,
    layerEventSinkProvided,
    layerCommandReceiptStoreProvided,
    IdAllocator.layer,
    layerProviderEventIngestorProvided,
  );
  const layerProviderSessionManagerProvided = ProviderSessionManager.layerWithOptions({
    configureMcp: false,
  }).pipe(
    Layer.provide(
      Layer.mergeAll(
        layerProvidedRegistry,
        layerEventSinkProvided,
        IdAllocator.layer,
        McpSessionRegistryTestkit.layer,
        layerProviderEventIngestorProvided,
        layerStores,
      ),
    ),
  );
  const layerProviderSwitchServiceProvided = ProviderSwitchService.layer.pipe(
    Layer.provide(layerProvidedRegistry),
  );
  const captureBaseline = options.captureBaseline;
  const layerRunExecution = captureBaseline === undefined
    ? RunExecutionService.layer
    : RunExecutionService.layer.pipe(
        Layer.updateService(CheckpointService.CheckpointServiceV2, (service) => ({
          ...service,
          captureBaseline: () => captureBaseline,
        })),
      );
  const layerRunExecutionServiceProvided = layerRunExecution.pipe(
    Layer.provide(
      Layer.mergeAll(
        McpAppModelContext.layerEmpty,
        layerCheckpointServiceProvided,
        layerEventSinkProvided,
        IdAllocator.layer,
        layerProviderEventIngestorProvided,
        layerServerSettings,
      ),
    ),
  );
  const layerProviderTurnStartServiceProvided = ProviderTurnStartService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerContextHandoffServiceProvided,
        layerEventSinkProvided,
        IdAllocator.layer,
        layerStores,
        layerProviderSessionManagerProvided,
        Layer.mock(ProviderAuthService.ProviderAuthService)({
          tryHandlePromptCommand: () => Effect.succeed(false),
        }),
        layerRunExecutionServiceProvided,
        layerRuntime,
      ),
    ),
  );
  const observeInterrupt = options.observeInterrupt;
  let observedInterrupt: Parameters<
    ProviderTurnControlService.ProviderTurnControlServiceV2Shape["interrupt"]
  >[0] | undefined;
  const layerProviderTurnControl = observeInterrupt === undefined
    ? ProviderTurnControlService.layer
    : ProviderTurnControlService.layer.pipe(
        Layer.updateService(ProjectionStore.ProjectionStoreV2, (service) => ({
          ...service,
          getProviderControlContext: (threadId, target) => Effect.suspend(() => {
            if (observedInterrupt?.threadId !== threadId ||
              observedInterrupt.providerTurnId !== target.providerTurnId) {
              return service.getProviderControlContext(threadId, target);
            }
            observeInterrupt({ phase: "control-read-enter" });
            return service.getProviderControlContext(threadId, target).pipe(
              Effect.onExit((exit) => Effect.sync(() => observeInterrupt({
                phase: "control-read-exit",
                exit: exit._tag,
                ...(exit._tag === "Success" ? {
                  providerTurnStatus: exit.value.providerTurn?.status ?? null,
                  attemptStatus: exit.value.attempt?.status ?? null,
                } : {}),
              }))),
            );
          }),
        })),
      );
  const layerProviderTurnControlServiceProvided = layerProviderTurnControl.pipe(
    Layer.provide(Layer.merge(layerStores, layerProviderSessionManagerProvided)),
  );
  const layerRuntimeRequestServiceProvided = RuntimeRequestService.layer.pipe(
    Layer.provide(Layer.merge(layerStores, layerProviderSessionManagerProvided)),
  );
  const layerCheckpointRollbackServiceProvided = CheckpointRollbackService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerCheckpointServiceProvided,
        layerEventSinkProvided,
        IdAllocator.layer,
        layerStores,
        layerProviderSessionManagerProvided,
        ThreadCommandExecutor.layer,
        layerRuntime,
      ),
    ),
  );
  const layerCheckpointCaptureServiceProvided = CheckpointCaptureService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerCheckpointServiceProvided,
        layerEventSinkProvided,
        IdAllocator.layer,
        layerStores,
      ),
    ),
  );
  const layerRunFinalizationServiceProvided = RunFinalizationService.layer.pipe(
    Layer.provide(Layer.merge(layerCheckpointCaptureServiceProvided, layerStores)),
  );
  const layerThreadTitleRegenerationTest = Layer.succeed(
    ThreadTitleRegenerationService.ThreadTitleRegenerationService,
    ThreadTitleRegenerationService.ThreadTitleRegenerationService.of({
      execute: () => Effect.void,
    }),
  );
  const layerOrchestratorProvided = Orchestrator.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerCheckpointServiceProvided,
        CommandPolicy.layer,
        layerContextHandoffServiceProvided,
        layerPersistence,
        layerProvidedRegistry,
        layerContinuationRequests,
        layerRuntime,
        layerProviderSessionManagerProvided,
        layerProviderSwitchServiceProvided,
        layerRunExecutionServiceProvided,
        ThreadForkService.layer,
      ),
    ),
  );
  const layerThreadManagementProvided = Layer.unwrap(
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const observe = options.observeProviderDispatch;
      const withProviderDispatch: Orchestrator.OrchestratorV2Shape["withProviderDispatch"] =
        observe === undefined ? orchestrator.withProviderDispatch : (input, work) =>
          Effect.suspend(() => {
            observe({ ...input, phase: "enter" });
            return orchestrator.withProviderDispatch(input, work).pipe(
              Effect.tapError((error) => Effect.sync(() => {
                const outer = typeof error === "object" && error !== null ? error : undefined;
                const cause = outer !== undefined && "cause" in outer ? outer.cause : undefined;
                const inner = typeof cause === "object" && cause !== null ? cause : undefined;
                observe({
                  ...input, phase: "failure",
                  heldAfterSequence: outer !== undefined && "heldAfterSequence" in outer &&
                    typeof outer.heldAfterSequence === "number" ? outer.heldAfterSequence : null,
                  errorTag: outer !== undefined && "_tag" in outer && typeof outer._tag === "string"
                    ? outer._tag : null,
                  causeTag: inner !== undefined && "_tag" in inner && typeof inner._tag === "string"
                    ? inner._tag : null,
                  causeThreadId: inner !== undefined && "threadId" in inner && typeof inner.threadId === "string"
                    ? inner.threadId : null,
                  causeGoalThreadId: inner !== undefined && "goalThreadId" in inner && typeof inner.goalThreadId === "string"
                    ? inner.goalThreadId : null,
                });
              })),
              Effect.tap(() => Effect.sync(() => observe({ ...input, phase: "return" }))),
            );
          });
      const entry = options.policyStreamEntry;
      const streamStoredEventsFrom: Orchestrator.OrchestratorV2Shape["streamStoredEventsFrom"] =
        entry === undefined ? orchestrator.streamStoredEventsFrom : (input) =>
          input?.threadId !== entry.threadId || input.eventType !== "goal.dispatch-policy.updated"
            ? orchestrator.streamStoredEventsFrom(input)
            : Stream.unwrap(Effect.gen(function* () {
                yield* entry.beforeSubscribe(input);
                return Stream.suspend(() => {
                  // Consumption of the real stream starts here, after the ordering action.
                  // This is not a claim about the internal PubSub registration instruction.
                  entry.observe({ phase: "realSubscriptionEntered", input });
                  return orchestrator.streamStoredEventsFrom(input).pipe(
                    Stream.tap((storedEvent) => Effect.sync(() =>
                      entry.observe({ phase: "storedEventDelivered", input, storedEvent }))),
                    Stream.ensuring(Effect.sync(() =>
                      entry.observe({ phase: "realSubscriptionFinalized", input }))),
                  );
                });
              }));
      return Layer.mock(ThreadManagementService.ThreadManagementService)({
        withProviderDispatch,
        streamStoredEventsFrom,
        dispatch: orchestrator.dispatch,
        getThreadRecords: orchestrator.getThreadRecords,
        getThreadProjection: orchestrator.getThreadProjection,
        recoverDelegatedTask: orchestrator.recoverDelegatedTask,
        delegatedTaskResultPending: orchestrator.delegatedTaskResultPending,
      });
    }),
  ).pipe(Layer.provide(layerOrchestratorProvided));
  const layerContinuationWorkerProvided =
    options.runContinuationWorker === true
      ? ProviderContinuationService.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              layerContinuationRequests,
              layerThreadManagementProvided,
              IdAllocator.layer,
            ),
          ),
        )
      : Layer.empty;
  const layerEffectExecutor = observeInterrupt === undefined
    ? EffectWorker.layerExecutor
    : EffectWorker.layerExecutor.pipe(
        Layer.updateService(ProviderTurnControlService.ProviderTurnControlServiceV2, (service) => ({
          ...service,
          interrupt: (input) => Effect.suspend(() => {
            observedInterrupt = input;
            observeInterrupt({ phase: "interrupt-enter" });
            return service.interrupt(input).pipe(
              Effect.onExit((exit) => Effect.sync(() => {
                observeInterrupt({ phase: "interrupt-exit", exit: exit._tag });
                observedInterrupt = undefined;
              })),
            );
          }),
        })),
      );
  const layerEffectExecutorProvided = layerEffectExecutor.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerRunFinalizationServiceProvided,
        layerCheckpointRollbackServiceProvided,
        layerProviderSessionManagerProvided,
        layerProviderTurnControlServiceProvided,
        layerProviderTurnStartServiceProvided,
        layerRuntimeRequestServiceProvided,
        layerThreadTitleRegenerationTest,
        layerServerSettings,
        layerThreadManagementProvided,
      ),
    ),
  );
  const layerEffectWorkerProvided = EffectWorker.layer.pipe(
    Layer.provide(Layer.merge(layerStores, layerEffectExecutorProvided)),
  );
  const layerReplayRuntime = Layer.mergeAll(
    layerOrchestratorProvided,
    layerProviderSessionManagerProvided,
    layerEffectWorkerProvided,
    layerEventSinkProvided,
    layerContinuationWorkerProvided,
  ).pipe(Layer.provide(ProviderTurnStartServiceTestkit.layer), Layer.provide(NodeServices.layer));

  // Build the daemon from the exact worker instance exposed alongside the
  // orchestrator. Keeping this acquisition in the replay layer makes the
  // outbox lifecycle explicit and prevents test-only command-side draining.
  if (options.runEffectWorker === false) {
    return layerReplayRuntime;
  }
  // Built before the runtime it shares stores with, so recovery commits before
  // the effect worker claims anything, as in serverRuntimeStartup.
  const layerStartupRecovery: Layer.Layer<
    never,
    MigrationError | PlatformError.PlatformError | SqlError
  > =
    options.recoverOnStartup === true
      ? Layer.effectDiscard(
          ProviderRuntimeRecoveryService.ProviderRuntimeRecoveryService.use(
            (recovery) => recovery.recover,
          ).pipe(Effect.orDie),
        ).pipe(
          Layer.provide(ProviderRuntimeRecoveryService.layer),
          Layer.provide(
            Layer.mergeAll(
              layerStores,
              layerEventSinkProvided,
              IdAllocator.layer,
              layerServerSettings,
            ),
          ),
        )
      : Layer.empty;
  return Layer.effect(
    Orchestrator.OrchestratorV2,
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      // As in serverRuntimeStartup: after runtime recovery, before the worker.
      yield* orchestrator.recoverDelegatedTasks;
      yield* EffectWorker.runDaemon.pipe(Effect.forkScoped);
      return orchestrator;
    }),
  ).pipe(Layer.provideMerge(layerReplayRuntime), Layer.provide(layerStartupRecovery));
}
