/**
 * pi-server — long-running HTTP/SSE bridge around pi-coding-agent
 *
 * One process hosts many pi agents. The client sends an agent id on each
 * request; pi-server maps it to:
 *   - ${cwd}/agents/<agent-id>/settings.json
 *   - ${cwd}/agents/<agent-id>/SYSTEM.md
 *   - ${cwd}/sessions/<agent-id>.jsonl
 *
 * The agent directory is static configuration. The sessions directory is
 * dynamic append-only history.
 */

import { mkdir, stat, writeFile } from "node:fs/promises";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { dirname, join } from "node:path";
import { Readable } from "node:stream";
import {
	AuthStorage,
	createAgentSession,
	DefaultResourceLoader,
	ModelRegistry,
	SessionManager,
	SettingsManager,
	type AgentSession,
	type AgentSessionEvent,
} from "@earendil-works/pi-coding-agent";

// ============================================================================
// Configuration
// ============================================================================

const PORT = parseInt(process.env.PI_SERVER_PORT ?? "8081", 10);
const ROOT_DIR = process.cwd();
const AGENTS_DIR = join(ROOT_DIR, "agents");
const SESSIONS_DIR = join(ROOT_DIR, "sessions");
const ALLOW_BUILTIN_TOOLS = !!process.env.PI_ALLOW_BUILTIN_TOOLS;
const HEARTBEAT_MS = 15_000;

// Keep SDK global defaults rooted in pi-server's data root. Per-agent calls pass
// explicit agentDir/settings/sessionManager so this is only a safe fallback.
process.env.PI_CODING_AGENT_DIR = ROOT_DIR;

const log = (msg: string): void => console.log(`[pi-server] ${msg}`);
const errLog = (msg: string, e?: unknown): void =>
	console.error(`[pi-server] ${msg}`, e ?? "");

log(`root=${ROOT_DIR}`);
log(`agents=${AGENTS_DIR}`);
log(`sessions=${SESSIONS_DIR}`);
log(
	`builtinTools=${ALLOW_BUILTIN_TOOLS ? "on" : "off"}${
		ALLOW_BUILTIN_TOOLS ? " (overridden by PI_ALLOW_BUILTIN_TOOLS)" : ""
	}`,
);

await mkdir(SESSIONS_DIR, { recursive: true });

// ============================================================================
// Types
// ============================================================================

type Model = NonNullable<ReturnType<ModelRegistry["find"]>>;
type ThinkingLevel = "off" | "minimal" | "low" | "medium" | "high" | "xhigh";

type SseClient = {
	readonly id: number;
	readonly startedAt: number;
	responseChars: number;
	closed: boolean;
	enqueue(chunk: Uint8Array): void;
	close(): void;
};

type AgentRuntime = {
	readonly agentId: string;
	readonly agentDir: string;
	readonly sessionPath: string;
	readonly settingsManager: SettingsManager;
	readonly authStorage: AuthStorage;
	readonly modelRegistry: ModelRegistry;
	readonly session: AgentSession;
	activeClient?: SseClient;
	activeUnsub?: () => void;
};

// ============================================================================
// Agent loading
// ============================================================================

const agents = new Map<string, Promise<AgentRuntime>>();
let nextClientId = 1;

function isValidAgentId(agentId: string): boolean {
	return /^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$/.test(agentId);
}

function getAgentId(req: Request): string | undefined {
	const authorization = req.headers.get("authorization");
	if (authorization) {
		const match = authorization.match(/^Bearer\s+(.+)$/i);
		if (match?.[1]) return match[1].trim();
	}
	return req.headers.get("x-agent-id")?.trim() || undefined;
}

function unauthorized(message = "unknown agent"): Response {
	return Response.json({ error: "unauthorized", message }, { status: 401 });
}

async function pathIsDirectory(path: string): Promise<boolean> {
	try {
		return (await stat(path)).isDirectory();
	} catch {
		return false;
	}
}

async function pathExists(path: string): Promise<boolean> {
	try {
		await stat(path);
		return true;
	} catch {
		return false;
	}
}

async function ensureSessionFile(agentId: string, agentDir: string, sessionPath: string): Promise<void> {
	if (await pathExists(sessionPath)) return;

	await mkdir(dirname(sessionPath), { recursive: true });
	const timestamp = new Date().toISOString();
	const header = {
		type: "session",
		version: 3,
		id: agentId,
		timestamp,
		cwd: agentDir,
	};
	await writeFile(sessionPath, `${JSON.stringify(header)}\n`, { flag: "wx" });
}

async function getAgent(agentId: string): Promise<AgentRuntime | undefined> {
	if (!isValidAgentId(agentId)) return undefined;

	const agentDir = join(AGENTS_DIR, agentId);
	if (!(await pathIsDirectory(agentDir))) return undefined;

	let runtime = agents.get(agentId);
	if (!runtime) {
		runtime = createRuntime(agentId, agentDir).catch((e: unknown) => {
			agents.delete(agentId);
			throw e;
		});
		agents.set(agentId, runtime);
	}
	return runtime;
}

async function createRuntime(agentId: string, agentDir: string): Promise<AgentRuntime> {
	const sessionPath = join(SESSIONS_DIR, `${agentId}.jsonl`);
	await ensureSessionFile(agentId, agentDir, sessionPath);

	const settingsManager = SettingsManager.create(agentDir, agentDir, {
		projectTrusted: false,
	});
	assertSettingsReadable(agentId, settingsManager);

	const authStorage = AuthStorage.create(join(agentDir, "auth.json"));
	const modelRegistry = ModelRegistry.create(authStorage, join(agentDir, "models.json"));
	const model = resolveSettingsModel(agentId, settingsManager, modelRegistry);
	const thinkingLevel = settingsManager.getDefaultThinkingLevel();

	const resourceLoader = new DefaultResourceLoader({
		cwd: agentDir,
		agentDir,
		settingsManager,
	});
	await resourceLoader.reload();

	const result = await createAgentSession({
		cwd: agentDir,
		agentDir,
		noTools: ALLOW_BUILTIN_TOOLS ? undefined : "builtin",
		sessionManager: SessionManager.open(sessionPath, SESSIONS_DIR, agentDir),
		settingsManager,
		modelRegistry,
		authStorage,
		resourceLoader,
		model: model as never,
		thinkingLevel,
	});

	const runtime: AgentRuntime = {
		agentId,
		agentDir,
		sessionPath,
		settingsManager,
		authStorage,
		modelRegistry,
		session: result.session,
	};

	log(
		`agent loaded id=${agentId} messages=${runtime.session.state.messages.length}` +
			` model=${runtime.session.model?.provider ?? "?"}/${runtime.session.model?.id ?? "?"}` +
			` thinking=${runtime.session.thinkingLevel}` +
			` file=${sessionPath}`,
	);

	return runtime;
}

function assertSettingsReadable(agentId: string, settingsManager: SettingsManager): void {
	const errors = settingsManager.drainErrors();
	if (errors.length === 0) return;
	const detail = errors.map(({ scope, error }) => `${scope}: ${error.message}`).join("; ");
	throw new Error(`agent ${agentId} settings.json failed to load: ${detail}`);
}

function resolveSettingsModel(
	agentId: string,
	settingsManager: SettingsManager,
	modelRegistry: ModelRegistry,
): Model {
	const provider = settingsManager.getDefaultProvider();
	if (!provider) {
		throw new Error(
			`agent ${agentId} settings.json is missing "defaultProvider"`,
		);
	}

	const modelId = settingsManager.getDefaultModel();
	if (!modelId) {
		throw new Error(
			`agent ${agentId} settings.json has "defaultProvider":"${provider}" but is missing "defaultModel"`,
		);
	}

	const model = modelRegistry.find(provider, modelId);
	if (!model) {
		throw new Error(
			`agent ${agentId} settings.json requests ${provider}/${modelId} but no such model exists`,
		);
	}

	if (!modelRegistry.hasConfiguredAuth(model)) {
		throw new Error(
			`agent ${agentId} settings.json requests ${provider}/${modelId} but auth.json has no credentials for ${provider}`,
		);
	}

	return model;
}

async function applySettings(runtime: AgentRuntime): Promise<void> {
	await runtime.session.reload();
	assertSettingsReadable(runtime.agentId, runtime.settingsManager);

	const model = resolveSettingsModel(
		runtime.agentId,
		runtime.settingsManager,
		runtime.modelRegistry,
	);
	if (
		!runtime.session.model ||
		runtime.session.model.provider !== model.provider ||
		runtime.session.model.id !== model.id
	) {
		await runtime.session.setModel(model as never);
	}

	const thinkingLevel = runtime.settingsManager.getDefaultThinkingLevel();
	if (thinkingLevel && runtime.session.thinkingLevel !== thinkingLevel) {
		runtime.session.setThinkingLevel(thinkingLevel as ThinkingLevel);
	}
}

// ============================================================================
// SSE helpers
// ============================================================================

const enc = new TextEncoder();
const sse = (event: string, data: unknown): Uint8Array =>
	enc.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
const heartbeat = (): Uint8Array => enc.encode(":heartbeat\n\n");

function makeClient(controller: ReadableStreamDefaultController<Uint8Array>): SseClient {
	let heartbeatTimer: ReturnType<typeof setInterval> | undefined;
	const client: SseClient = {
		id: nextClientId++,
		startedAt: Date.now(),
		responseChars: 0,
		closed: false,
		enqueue(chunk): void {
			if (client.closed) return;
			try {
				controller.enqueue(chunk);
			} catch {
				client.closed = true;
				if (heartbeatTimer) clearInterval(heartbeatTimer);
			}
		},
		close(): void {
			if (client.closed) return;
			client.closed = true;
			if (heartbeatTimer) clearInterval(heartbeatTimer);
			try {
				controller.close();
			} catch {
				/* already closed */
			}
		},
	};
	heartbeatTimer = setInterval(() => client.enqueue(heartbeat()), HEARTBEAT_MS);
	return client;
}

// ============================================================================
// HTTP server
// ============================================================================

const server = createServer((nodeReq, nodeRes) => {
	void handleNodeRequest(nodeReq, nodeRes);
});

server.listen(PORT, "0.0.0.0", () => {
	const address = server.address();
	const port = typeof address === "object" && address ? address.port : PORT;
	log(`listening on http://0.0.0.0:${port}`);
});

async function handleNodeRequest(
	nodeReq: IncomingMessage,
	nodeRes: ServerResponse,
): Promise<void> {
	try {
		const req = toWebRequest(nodeReq);
		const response = await handleRequest(req);
		sendWebResponse(response, nodeRes);
	} catch (e) {
		errLog("request failed", e);
		if (!nodeRes.headersSent) {
			nodeRes.writeHead(500, { "Content-Type": "application/json" });
		}
		nodeRes.end(JSON.stringify({ error: "internal server error" }));
	}
}

function toWebRequest(nodeReq: IncomingMessage): Request {
	const headers = new Headers();
	for (const [key, value] of Object.entries(nodeReq.headers)) {
		if (Array.isArray(value)) {
			for (const item of value) headers.append(key, item);
		} else if (value !== undefined) {
			headers.set(key, value);
		}
	}

	const host = headers.get("host") ?? `127.0.0.1:${PORT}`;
	const url = new URL(nodeReq.url ?? "/", `http://${host}`);
	const method = nodeReq.method ?? "GET";
	const init: RequestInit & { duplex?: "half" } = { method, headers };
	if (method !== "GET" && method !== "HEAD") {
		init.body = Readable.toWeb(nodeReq) as unknown as BodyInit;
		init.duplex = "half";
	}

	return new Request(url, init);
}

function sendWebResponse(response: Response, nodeRes: ServerResponse): void {
	nodeRes.statusCode = response.status;
	response.headers.forEach((value, key) => nodeRes.setHeader(key, value));

	if (!response.body) {
		nodeRes.end();
		return;
	}

	const body = Readable.fromWeb(response.body as never);
	body.on("error", (e: unknown) => {
		errLog("response stream failed", e);
		nodeRes.destroy(e instanceof Error ? e : undefined);
	});
	body.pipe(nodeRes);
}

async function handleRequest(req: Request): Promise<Response> {
	const url = new URL(req.url);

	if (req.method === "GET" && url.pathname === "/health") {
		return Response.json({
			status: "ok",
			root: ROOT_DIR,
			agentsDir: AGENTS_DIR,
			sessionsDir: SESSIONS_DIR,
			loadedAgents: agents.size,
		});
	}

	const agentId = getAgentId(req);
	if (!agentId) return unauthorized("missing agent id");

	const runtime = await getAgent(agentId).catch((e: unknown) => {
		errLog(`failed to load agent ${agentId}`, e);
		return undefined;
	});
	if (!runtime) return unauthorized("unknown agent");

	if (req.method === "GET" && url.pathname === "/agent") {
		return Response.json({
			agentId,
			agentDir: runtime.agentDir,
			sessionPath: runtime.sessionPath,
			sessionId: runtime.session.sessionId,
			messageCount: runtime.session.state.messages.length,
			model: runtime.session.model
				? `${runtime.session.model.provider}/${runtime.session.model.id}`
				: null,
			thinkingLevel: runtime.session.thinkingLevel,
			isStreaming: runtime.session.isStreaming,
		});
	}

	if (req.method === "POST" && url.pathname === "/chat") {
		return handleChat(req, runtime);
	}

	return Response.json({ error: "not found" }, { status: 404 });
}

// ============================================================================
// /chat — streaming SSE handler
// ============================================================================

function handleChat(req: Request, runtime: AgentRuntime): Response {
	let client: SseClient | undefined;
	let prompt = "";

	const stream = new ReadableStream<Uint8Array>({
		async start(controller): Promise<void> {
			client = makeClient(controller);

			try {
				const body = (await req.json()) as { prompt?: unknown };
				if (typeof body?.prompt !== "string" || body.prompt.length === 0) {
					client.enqueue(sse("error", { message: "missing or empty 'prompt' field" }));
					client.close();
					return;
				}
				prompt = body.prompt;
			} catch {
				client.enqueue(sse("error", { message: "invalid JSON body" }));
				client.close();
				return;
			}

			void beginTurn(runtime, prompt, client);
		},

		cancel(): void {
			if (!client || client.closed) return;
			client.closed = true;
			log(
				`← /chat agent=${runtime.agentId} aborted ${Date.now() - client.startedAt}ms (client disconnected)`,
			);
			if (runtime.activeClient === client) {
				runtime.activeUnsub?.();
				runtime.activeUnsub = undefined;
				runtime.activeClient = undefined;
				runtime.session.abort().catch((e: unknown) =>
					errLog(`agent ${runtime.agentId} abort error`, e),
				);
			}
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

function beginTurn(runtime: AgentRuntime, prompt: string, client: SseClient): void {
	void runTurn(runtime, prompt, client).catch((e: unknown) => {
		const reason = e instanceof Error ? e.message : String(e);
		if (!client.closed) client.enqueue(sse("error", { message: reason }));
		log(
			`← /chat agent=${runtime.agentId} rejected ${Date.now() - client.startedAt}ms: ${reason}`,
		);
		if (runtime.activeClient === client) {
			runtime.activeUnsub?.();
			runtime.activeUnsub = undefined;
			runtime.activeClient = undefined;
		}
		client.close();
	});
}

async function runTurn(runtime: AgentRuntime, prompt: string, client: SseClient): Promise<void> {
	const promptPreview =
		prompt.length > 200 ? `${prompt.slice(0, 200)}…(${prompt.length} chars)` : prompt;
	log(`→ /chat agent=${runtime.agentId} prompt=${JSON.stringify(promptPreview)}`);

	const previousClient = runtime.activeClient;
	if (previousClient && previousClient !== client && !previousClient.closed) {
		previousClient.enqueue(sse("error", { message: "superseded by newer request" }));
		previousClient.close();
	}
	runtime.activeUnsub?.();
	runtime.activeUnsub = undefined;
	runtime.activeClient = client;

	if (runtime.session.isStreaming) {
		await runtime.session.abort();
	}

	if (client.closed || runtime.activeClient !== client) return;
	await applySettings(runtime);

	if (client.closed || runtime.activeClient !== client) return;

	const unsub = runtime.session.subscribe((event: AgentSessionEvent): void => {
		if (client.closed || runtime.activeClient !== client) return;
		handleSessionEvent(runtime, client, event, unsub);
	});
	runtime.activeUnsub = unsub;

	await runtime.session.prompt(prompt);
}

function handleSessionEvent(
	runtime: AgentRuntime,
	client: SseClient,
	event: AgentSessionEvent,
	unsub: () => void,
): void {
	switch (event.type) {
		case "message_update": {
			const sub = event.assistantMessageEvent;
			switch (sub.type) {
				case "text_delta":
					client.responseChars += sub.delta.length;
					client.enqueue(sse("text_delta", { delta: sub.delta }));
					break;
				case "thinking_start":
					client.enqueue(sse("thinking_start", { contentIndex: sub.contentIndex }));
					break;
				case "thinking_delta":
					client.enqueue(
						sse("thinking_delta", {
							delta: sub.delta,
							contentIndex: sub.contentIndex,
						}),
					);
					break;
				case "thinking_end":
					client.enqueue(
						sse("thinking_end", {
							content: sub.content,
							contentIndex: sub.contentIndex,
						}),
					);
					break;
			}
			break;
		}
		case "agent_end":
			if (!event.willRetry) {
				client.enqueue(sse("agent_end", {}));
				log(
					`← /chat agent=${runtime.agentId} ok ${Date.now() - client.startedAt}ms ${client.responseChars} chars`,
				);
				unsub();
				if (runtime.activeClient === client) {
					runtime.activeUnsub = undefined;
					runtime.activeClient = undefined;
				}
				client.close();
			}
			break;
		case "auto_retry_end":
			if (!event.success) {
				const reason = event.finalError ?? "auto-retry exhausted";
				client.enqueue(sse("error", { message: reason }));
				log(
					`← /chat agent=${runtime.agentId} failed ${Date.now() - client.startedAt}ms ${client.responseChars} chars: ${reason}`,
				);
				unsub();
				if (runtime.activeClient === client) {
					runtime.activeUnsub = undefined;
					runtime.activeClient = undefined;
				}
				client.close();
			}
			break;
		case "auto_retry_start":
			log(
				`↻ /chat agent=${runtime.agentId} auto-retry ${event.attempt}/${event.maxAttempts} in ${event.delayMs}ms: ${event.errorMessage}`,
			);
			break;
	}
}

// ============================================================================
// Graceful shutdown
// ============================================================================

let shuttingDown = false;
async function shutdown(signal: string): Promise<void> {
	if (shuttingDown) return;
	shuttingDown = true;
	log(`${signal} received, shutting down`);

	server.close();

	const runtimes = await Promise.allSettled([...agents.values()]);
	for (const result of runtimes) {
		if (result.status !== "fulfilled") continue;
		const runtime = result.value;
		if (runtime.session.isStreaming) {
			log(`aborting in-flight prompt agent=${runtime.agentId}`);
			await runtime.session.abort().catch((e: unknown) =>
				errLog(`agent ${runtime.agentId} abort error`, e),
			);
		}
		try {
			runtime.session.dispose();
			log(`agent disposed id=${runtime.agentId}`);
		} catch (e) {
			errLog(`agent ${runtime.agentId} dispose error`, e);
		}
	}

	setTimeout(() => process.exit(0), 500);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));
