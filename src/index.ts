/**
 * pi-server — long-running HTTP bridge around pi-coding-agent
 *
 * One process = one AgentSession = one speaker. The speaker identity is
 * encoded in process.cwd(): pi-server reads auth.json / models.json /
 * settings.json / SYSTEM.md directly from ${cwd}/.
 *
 * Wire protocol: see PROTOCOL.md
 */

import {
	AuthStorage,
	createAgentSession,
	ModelRegistry,
	SessionManager,
	SettingsManager,
	type AgentSession,
	type AgentSessionEvent,
	type ThinkingLevel,
} from "@earendil-works/pi-coding-agent";

// ============================================================================
// Configuration (env vars + cwd-based identity)
// ============================================================================

const PORT = parseInt(process.env.PI_SERVER_PORT ?? "8081", 10);
const CWD = process.cwd();
const ALLOW_BUILTIN_TOOLS = !!process.env.PI_ALLOW_BUILTIN_TOOLS;
const HEARTBEAT_MS = 15_000;

// SDK reads PI_CODING_AGENT_DIR to locate auth.json / models.json / settings.json
// AND to determine where session files are stored.
// Setting it to cwd makes per-speaker state live directly in the speaker dir:
//   - ${cwd}/auth.json, ${cwd}/models.json, ${cwd}/settings.json
//   - ${cwd}/SYSTEM.md (auto-discovered as agentDir-level system prompt, no project trust needed)
//   - ${cwd}/APPEND_SYSTEM.md
//   - ${cwd}/AGENTS.md / ${cwd}/CLAUDE.md (context files; also walked up from cwd)
//   - ${cwd}/.pi/extensions/, ${cwd}/.pi/skills/, etc. (project-local resources)
//   - ${cwd}/sessions/--<encoded-cwd>--/*.jsonl (session history)
process.env.PI_CODING_AGENT_DIR = CWD;

const log = (msg: string): void => console.log(`[pi-server] ${msg}`);
const errLog = (msg: string, e?: unknown): void =>
	console.error(`[pi-server] ${msg}`, e ?? "");

log(`cwd=${CWD}`);
log(
	`builtinTools=${ALLOW_BUILTIN_TOOLS ? "on" : "off"}${
		ALLOW_BUILTIN_TOOLS ? " (overridden by PI_ALLOW_BUILTIN_TOOLS)" : ""
	}`,
);

// ============================================================================
// Initialize AgentSession (fail-fast on any startup error)
// ============================================================================

const settingsManager = SettingsManager.create(CWD);
const authStorage = AuthStorage.create();
const modelRegistry = ModelRegistry.create(authStorage);

// settings.json is the sole source of truth for which model runs in this
// pi-server. No fallback: missing fields, unknown model, or missing auth
// all fail-fast with an actionable error message. The user explicitly
// opted out of "pick any first-available" behavior.
const settingsModel: NonNullable<ReturnType<typeof modelRegistry.find>> = (() => {
	const provider = settingsManager.getDefaultProvider();
	if (!provider) {
		errLog(
			`settings.json is missing "defaultProvider" — add it (e.g. "anthropic") to configure which model to use`,
		);
		process.exit(1);
	}
	const modelId = settingsManager.getDefaultModel();
	if (!modelId) {
		errLog(
			`settings.json has "defaultProvider":"${provider}" but is missing "defaultModel" — add it (e.g. "claude-sonnet-4-5")`,
		);
		process.exit(1);
	}
	const found = modelRegistry.find(provider, modelId);
	if (!found) {
		errLog(
			`settings.json requests ${provider}/${modelId} but no such model exists in the registry`,
		);
		process.exit(1);
	}
	if (!modelRegistry.hasConfiguredAuth(found)) {
		errLog(
			`settings.json requests ${provider}/${modelId} but auth.json has no credentials for ${provider}`,
		);
		process.exit(1);
	}
	return found;
})();

const settingsThinkingLevel: ThinkingLevel | undefined = settingsManager.getDefaultThinkingLevel();

let session: AgentSession;
try {
	const result = await createAgentSession({
		// Disable built-in tools (read/bash/edit/write) by default.
		// Skills, extensions, prompt templates, AGENTS.md discovery all remain enabled —
		// they're driven by files in ${cwd}/, so empty dir = nothing loaded.
		noTools: ALLOW_BUILTIN_TOOLS ? undefined : "builtin",

		// cwd-based session persistence: most recent ${cwd}/sessions/--<encoded-cwd>--/*.jsonl
		// is auto-resumed; no existing file → fresh session with a new id.
		// Note: SessionManager.create() unconditionally opens a new file — use
		// continueRecent() for resume-on-restart semantics.
		sessionManager: SessionManager.continueRecent(CWD),

		// Share these so we don't construct a second SettingsManager / ModelRegistry
		// under the hood. modelRegistry is needed for API key resolution; without
		// it the SDK would build its own from a different AuthStorage instance.
		settingsManager,
		modelRegistry,
		authStorage,

		// Force settings.json's defaultProvider/defaultModel to win over the
		// jsonl's `model_change` entry.
		// Cast: modelRegistry.find() returns Model<Api>, createAgentSession
		// expects Model<any> (variance gap in Model's conditional `compat` field).
		model: settingsModel as never,

		// Force settings.json's defaultThinkingLevel to win over any
		// thinking_level_change entry recorded in the resumed jsonl.
		thinkingLevel: settingsThinkingLevel,
	});
	session = result.session;
} catch (e) {
	errLog("failed to initialize AgentSession", e);
	process.exit(1);
}

log(
	`session loaded id=${session.sessionId} messages=${session.state.messages.length}` +
		` model=${session.model?.provider ?? "?"}/${session.model?.id ?? "?"}` +
		` thinking=${session.thinkingLevel} (source: ${settingsThinkingLevel ? "settings.json" : "sdk default"})`,
);

// ============================================================================
// SSE helpers
// ============================================================================

const enc = new TextEncoder();
const sse = (event: string, data: unknown): Uint8Array =>
	enc.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
const heartbeat = (): Uint8Array => enc.encode(":heartbeat\n\n");

// ============================================================================
// HTTP server
// ============================================================================

const server = Bun.serve({
	port: PORT,
	hostname: "0.0.0.0",

	async fetch(req): Promise<Response> {
		const url = new URL(req.url);

		// GET /health
		if (req.method === "GET" && url.pathname === "/health") {
			return Response.json({
				status: "ok",
				cwd: CWD,
				sessionId: session.sessionId,
				messageCount: session.state.messages.length,
				model: session.model
					? `${session.model.provider}/${session.model.id}`
					: null,
				thinkingLevel: session.thinkingLevel,
				isStreaming: session.isStreaming,
			});
		}

		// POST /chat
		if (req.method === "POST" && url.pathname === "/chat") {
			if (session.isStreaming) {
				return Response.json(
					{ error: "busy", state: "busy" },
					{ status: 503, headers: { "Retry-After": "1" } },
				);
			}
			return handleChat(req);
		}

		return Response.json({ error: "not found" }, { status: 404 });
	},
});

log(`listening on http://0.0.0.0:${server.port}`);

// ============================================================================
// /chat — streaming SSE handler with req-close abort detection
// ============================================================================

function handleChat(req: Request): Response {
	const startedAt = Date.now();
	let responseChars = 0;
	const stream = new ReadableStream({
		async start(controller): Promise<void> {
			let aborted = false;
			let heartbeatTimer: ReturnType<typeof setInterval> | undefined;

			const stopHeartbeat = (): void => {
				if (heartbeatTimer) {
					clearInterval(heartbeatTimer);
					heartbeatTimer = undefined;
				}
			};

			const safeEnqueue = (chunk: Uint8Array): void => {
				if (aborted) return;
				try {
					controller.enqueue(chunk);
				} catch {
					aborted = true;
				}
			};

			const safeClose = (): void => {
				if (aborted) return;
				aborted = true;
				stopHeartbeat();
				try {
					controller.close();
				} catch {
					/* already closed */
				}
			};

			// ---- Parse request body ------------------------------------------
			let prompt: string;
			try {
				const body = (await req.json()) as { prompt?: unknown };
				if (typeof body?.prompt !== "string" || body.prompt.length === 0) {
					safeEnqueue(sse("error", { message: "missing or empty 'prompt' field" }));
					safeClose();
					return;
				}
				prompt = body.prompt;
			} catch {
				safeEnqueue(sse("error", { message: "invalid JSON body" }));
				safeClose();
				return;
			}

			// ---- Log request --------------------------------------------------
			// Session-level info (model, thinking, sessionId, messageCount) is
			// already logged at startup. Per-request we just print the query.
			const promptPreview =
				prompt.length > 200 ? `${prompt.slice(0, 200)}…(${prompt.length} chars)` : prompt;
			log(`→ /chat prompt=${JSON.stringify(promptPreview)}`);

			// ---- Heartbeat (keep proxies from timing out during silence) ------
			heartbeatTimer = setInterval(() => safeEnqueue(heartbeat()), HEARTBEAT_MS);

			// ---- Subscribe BEFORE prompt to avoid missing the first event -----
			const unsub = session.subscribe((event: AgentSessionEvent): void => {
				switch (event.type) {
					case "message_update": {
						const sub = event.assistantMessageEvent;
						switch (sub.type) {
							case "text_delta":
								responseChars += sub.delta.length;
								safeEnqueue(sse("text_delta", { delta: sub.delta }));
								break;
							case "thinking_start":
								safeEnqueue(
									sse("thinking_start", { contentIndex: sub.contentIndex }),
								);
								break;
							case "thinking_delta":
								safeEnqueue(
									sse("thinking_delta", {
										delta: sub.delta,
										contentIndex: sub.contentIndex,
									}),
								);
								break;
							case "thinking_end":
								safeEnqueue(
									sse("thinking_end", {
										content: sub.content,
										contentIndex: sub.contentIndex,
									}),
								);
								break;
							// Drop text_start, text_end, toolcall_*, done, error
						}
						break;
					}
					case "agent_end":
						// willRetry=true → SDK will auto-continue; keep stream open.
						// willRetry=false → prompt finished; close stream.
						if (!event.willRetry) {
							safeEnqueue(sse("agent_end", {}));
							log(
								`← /chat ok ${Date.now() - startedAt}ms ${responseChars} chars`,
							);
							unsub();
							safeClose();
						}
						break;
					case "auto_retry_end":
						if (!event.success) {
							const reason = event.finalError ?? "auto-retry exhausted";
							safeEnqueue(sse("error", { message: reason }));
							log(
								`← /chat failed ${Date.now() - startedAt}ms ${responseChars} chars: ${reason}`,
							);
							unsub();
							safeClose();
						}
						break;
					case "auto_retry_start":
						// Informational: SDK is about to retry the LLM call.
						// No SSE event emitted (client sees silence until next text_delta).
						log(
							`↻ /chat auto-retry ${event.attempt}/${event.maxAttempts} in ${event.delayMs}ms: ${event.errorMessage}`,
						);
						break;
					// Drop everything else: queue_update, compaction_*, session_info_changed…
				}
			});

			// ---- Send prompt (don't await — events signal completion) --------
			session.prompt(prompt).catch((e: unknown) => {
				// Preflight error (no model selected, no API key, etc.).
				// Streaming errors come via the 'error' SSE event above, not here.
				const reason = e instanceof Error ? e.message : String(e);
				safeEnqueue(sse("error", { message: reason }));
				log(`← /chat rejected ${Date.now() - startedAt}ms: ${reason}`);
				unsub();
				safeClose();
			});
		},

		cancel(): void {
			// Client closed the response body → abort the prompt.
			// This is the req-close-as-abort contract documented in PROTOCOL.md §3.1.
			log(`← /chat aborted ${Date.now() - startedAt}ms (client disconnected)`);
			session.abort().catch((e: unknown) => errLog("session.abort error", e));
		},
	});

	return new Response(stream, {
		headers: {
			"Content-Type": "text/event-stream",
			"Cache-Control": "no-cache",
			Connection: "keep-alive",
		},
	});
}

// ============================================================================
// Graceful shutdown
// ============================================================================

let shuttingDown = false;
function shutdown(signal: string): void {
	if (shuttingDown) return;
	shuttingDown = true;
	log(`${signal} received, shutting down`);

	server.stop();

	if (session.isStreaming) {
		log("aborting in-flight prompt");
		session.abort().catch((e: unknown) => errLog("abort error", e));
	}

	// session.dispose() is sync — flushes pending writes, removes listeners.
	try {
		session.dispose();
		log("session disposed");
	} catch (e) {
		errLog("dispose error", e);
	}

	// Give the abort + dispose time to flush, then exit.
	setTimeout(() => process.exit(0), 500);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));