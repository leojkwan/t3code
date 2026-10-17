import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AuthSessionId,
  AuthAdministrativeScopes,
  AuthTokenExchangeGrantType,
  AuthEnvironmentBootstrapTokenType,
  AuthAccessTokenType,
  EnvironmentAuthenticatedAuth,
  EnvironmentHttpApi,
} from "@t3tools/contracts";
import { RelayClientTracer } from "@t3tools/shared/relayTracing";
import { expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as Tracer from "effect/Tracer";
import { HttpServerRequest } from "effect/http";
import * as Etag from "effect/http/Etag";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpRouter from "effect/http/HttpRouter";

import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as EnvironmentAuth from "./EnvironmentAuth.ts";
import * as ServerSecretStore from "./ServerSecretStore.ts";
import * as AuthHttp from "./http.ts";

const DEV_TOKEN = "reusable-dev-auth-token-that-is-long-enough";
class AuthTestApi extends HttpApi.make("environment").add(EnvironmentHttpApi.groups.auth) {}

const layerConfig = Layer.effect(
  ServerConfig.ServerConfig,
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    return {
      ...config,
      mode: "web",
      devUrl: new URL("http://127.0.0.1:5173"),
      devAuthToken: Redacted.make(DEV_TOKEN),
    } satisfies ServerConfig.ServerConfig["Service"];
  }),
).pipe(Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-auth-http-test-" })));

const layerEnvironmentAuth = EnvironmentAuth.layer.pipe(
  Layer.provide(SqlitePersistence.layerMemory),
  Layer.provide(ServerSecretStore.layer),
  Layer.provide(ServerEnvironment.layerIdentity),
  Layer.provide(layerConfig),
);
const layerRoutes = HttpApiBuilder.layer(AuthTestApi).pipe(
  Layer.provide(AuthHttp.layer),
  Layer.provide(AuthHttp.layerAuthenticatedAuth),
  Layer.provideMerge(layerEnvironmentAuth),
  Layer.provide(layerConfig),
  Layer.provideMerge(
    HttpPlatform.layer.pipe(
      Layer.provideMerge(NodeServices.layer),
      Layer.provideMerge(Etag.layerWeak),
    ),
  ),
  Layer.provide(NodeServices.layer),
);

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const postJson = (path: string, body: unknown, headers?: Readonly<Record<string, string>>) =>
  new Request(`http://127.0.0.1${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: encodeJson(body),
  });

it.effect("classifies an existing session through access-write CAS without issuing credentials and refuses automation administration", () =>
  Effect.gen(function* () {
    const context = Context.make(Crypto.Crypto, yield* Crypto.Crypto);
    return yield* Effect.acquireUseRelease(
      Effect.sync(() => HttpRouter.toWebHandler(layerRoutes, { disableLogger: true })),
      (server) => Effect.tryPromise(async () => {
        const login = await server.handler(postJson("/api/auth/browser-session", { credential: DEV_TOKEN }), context);
        const adminCookie = login.headers.getSetCookie().find((cookie) => cookie.startsWith("t3_dev_session_"))!.split(";", 1)[0]!;
        const pairing = await server.handler(postJson("/api/auth/pairing-token", {
          scopes: AuthAdministrativeScopes,
        }, { cookie: adminCookie }), context);
        const targetLogin = await server.handler(postJson("/api/auth/browser-session", {
          credential: (await pairing.json() as { credential: string }).credential,
        }), context);
        const targetCookie = targetLogin.headers.getSetCookie().find((cookie) => cookie.startsWith("t3_session_"))!.split(";", 1)[0]!;
        const currentResponse = await server.handler(new Request("http://127.0.0.1/api/auth/clients", {
          headers: { cookie: targetCookie },
        }), context);
        const before = await currentResponse.json() as Array<{ sessionId: string; current: boolean; purpose?: string | null }>;
        const target = before.find((row) => row.current)!;
        const payload = { sessionId: target.sessionId, expectedPurpose: null, purpose: "automation" };
        const classify = await server.handler(postJson("/api/auth/clients/classify", payload, { cookie: adminCookie }), context);
        expect(classify.status).toBe(200);
        expect(await classify.json()).toEqual({ changed: true });
        expect(classify.headers.getSetCookie()).toHaveLength(0);
        const duplicate = await server.handler(postJson("/api/auth/clients/classify", payload, { cookie: adminCookie }), context);
        expect(await duplicate.json()).toEqual({ changed: false });
        const afterResponse = await server.handler(new Request("http://127.0.0.1/api/auth/clients", {
          headers: { cookie: targetCookie },
        }), context);
        const after = await afterResponse.json() as typeof before;
        expect(after.map((row) => row.sessionId).sort()).toEqual(before.map((row) => row.sessionId).sort());
        expect(after.find((row) => row.current)).toMatchObject({ sessionId: target.sessionId, purpose: "automation" });
        const refused = await server.handler(postJson("/api/auth/clients/classify", {
          sessionId: target.sessionId, expectedPurpose: "automation", purpose: "human",
        }, { cookie: targetCookie }), context);
        expect(refused.status).toBe(403);
        expect(await refused.json()).toMatchObject({ reason: "automation_session_classification_not_allowed" });
      }),
      (server) => Effect.promise(() => server.dispose()),
    );
  }).pipe(Effect.scoped),
);

it.effect("sets the selected browser session cookies through the HTTP route", () =>
  Effect.gen(function* () {
    const crypto = yield* Crypto.Crypto;
    const unusedSecretStore = ServerSecretStore.ServerSecretStore.of({
      get: () => Effect.succeedNone,
      set: () => Effect.void,
      create: () => Effect.void,
      getOrCreateRandom: () => Effect.die("Not used by these routes."),
      remove: () => Effect.void,
    });
    const requestContext = Context.make(Crypto.Crypto, crypto).pipe(
      Context.add(ServerSecretStore.ServerSecretStore, unusedSecretStore),
    );
    return yield* Effect.acquireUseRelease(
      Effect.sync(
        () =>
          [
            HttpRouter.toWebHandler(layerRoutes, { disableLogger: true }),
            HttpRouter.toWebHandler(layerRoutes, { disableLogger: true }),
          ] as const,
      ),
      ([environmentA, environmentB]) =>
        Effect.tryPromise(async () => {
          const devResponse = await environmentA.handler(
            postJson("/api/auth/browser-session", { credential: DEV_TOKEN }),
            requestContext,
          );
          expect(devResponse.status).toBe(200);
          const retiredScopeResponse = await environmentA.handler(
            new Request("http://127.0.0.1/oauth/token", {
              method: "POST",
              body: new URLSearchParams({
                grant_type: AuthTokenExchangeGrantType,
                subject_token: DEV_TOKEN,
                subject_token_type: AuthEnvironmentBootstrapTokenType,
                requested_token_type: AuthAccessTokenType,
                scope: "review:write",
              }),
            }),
            requestContext,
          );
          expect(retiredScopeResponse.status).toBe(400);
          expect(await retiredScopeResponse.json()).toMatchObject({ reason: "invalid_scope" });

          const devCookies = devResponse.headers.getSetCookie();
          const devCookie = devCookies.find((cookie) => cookie.startsWith("t3_dev_session_"));
          expect(devCookie).toContain("HttpOnly");
          expect(devCookie).toContain(`=${DEV_TOKEN};`);
          expect(devCookies).toContainEqual(
            expect.stringMatching(/^t3_session_[^=]*=;.*Max-Age=0/),
          );
          const devCookieHeader = devCookie?.split(";", 1)[0] ?? "";
          const environmentBSession = await environmentB.handler(
            new Request("http://127.0.0.1/api/auth/session", {
              headers: { cookie: devCookieHeader },
            }),
            requestContext,
          );
          expect(environmentBSession.status).toBe(200);
          expect(await environmentBSession.json()).toMatchObject({ authenticated: true });

          const pairingResponse = await environmentA.handler(
            postJson(
              "/api/auth/pairing-token",
              { scopes: ["orchestration:read"] },
              { cookie: devCookieHeader },
            ),
            requestContext,
          );
          expect(pairingResponse.status).toBe(200);
          const pairing = (await pairingResponse.json()) as { credential: string };
          const restrictedResponse = await environmentA.handler(
            postJson("/api/auth/browser-session", { credential: pairing.credential }),
            requestContext,
          );
          expect(restrictedResponse.status).toBe(200);
          const restrictedCookies = restrictedResponse.headers.getSetCookie();
          expect(restrictedCookies).toHaveLength(1);
          expect(restrictedCookies[0]).toMatch(/^t3_session_/);
          expect(restrictedCookies[0]).not.toContain("t3_dev_session_");
        }),
      ([environmentA, environmentB]) =>
        Effect.promise(() => Promise.all([environmentA.dispose(), environmentB.dispose()])),
    );
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("exports only verified T3 Connect requests", () =>
  Effect.gen(function* () {
    const productSpans: Array<string> = [];
    const localSpans: Array<string> = [];
    const collect = (into: Array<string>) =>
      Tracer.make({
        span: (options) => {
          into.push(options.name);
          return new Tracer.NativeSpan(options);
        },
      });
    // "DPoP connect" is a T3 Connect session; any other DPoP token is rejected.
    const environmentAuth = {
      authenticateHttpRequest: (request: HttpServerRequest.HttpServerRequest) =>
        (request.headers.authorization === "DPoP forged"
          ? Effect.fail(
              new EnvironmentAuth.ServerAuthInvalidCredentialError({ diagnostic: "forged" }),
            )
          : Effect.succeed({
              sessionId: AuthSessionId.make("session-1"),
              subject:
                request.headers.authorization === "DPoP connect"
                  ? "cloud-connect"
                  : "cli-issued-session",
              method: "bearer-access-token" as const,
              scopes: ["orchestration:read" as const],
            })
        ).pipe(Effect.withSpan("EnvironmentAuth.authenticateHttpRequest")),
    } as unknown as EnvironmentAuth.EnvironmentAuth["Service"];
    const middleware = yield* Layer.build(AuthHttp.layerAuthenticatedAuth).pipe(
      Effect.provideService(EnvironmentAuth.EnvironmentAuth, environmentAuth),
      Effect.map(Context.get(EnvironmentAuthenticatedAuth)),
    );
    const handle = (authorization: string) =>
      (
        middleware as unknown as (
          effect: Effect.Effect<void>,
        ) => Effect.Effect<void, Error, HttpServerRequest.HttpServerRequest>
      )(Effect.void.pipe(Effect.withSpan("environment.handler"))).pipe(
        Effect.ignore,
        Effect.provideService(
          HttpServerRequest.HttpServerRequest,
          HttpServerRequest.fromWeb(
            new Request("https://environment.example.test/api/orchestration/shell", {
              headers: {
                authorization,
                traceparent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
              },
            }),
          ),
        ),
        Effect.provideService(RelayClientTracer, Option.some(collect(productSpans))),
        Effect.withTracer(collect(localSpans)),
      );

    yield* handle("DPoP connect");
    expect(productSpans).toEqual([
      "environment.relay.request",
      "EnvironmentAuth.authenticateHttpRequest",
      "environment.handler",
    ]);
    expect(localSpans).toEqual(["EnvironmentAuth.authenticateHttpRequest"]);

    productSpans.length = 0;
    localSpans.length = 0;
    yield* handle("DPoP forged");
    expect(productSpans).toEqual([]);
    expect(localSpans).toEqual(["EnvironmentAuth.authenticateHttpRequest"]);

    localSpans.length = 0;
    yield* handle("Bearer access-token");
    expect(productSpans).toEqual([]);
    expect(localSpans).toEqual(["EnvironmentAuth.authenticateHttpRequest", "environment.handler"]);
  }).pipe(Effect.scoped),
);
