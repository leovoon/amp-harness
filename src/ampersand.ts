/**
 * Ampersand (humansand.ai, OpenAI Agents API shape) adapter for pi.
 *
 * Shared by the pi CLI extension and the amp-harness. pi owns the agent loop and
 * executes every tool locally; Ampersand owns the conversation server-side.
 *
 * Wire facts (verified live 2026-10-21):
 * - POST /agents/sessions {agent, environment:{type:"none"}, input, stream:true} -> SSE.
 * - Follow-ups: open GET /agents/sessions/{id}/events (SSE, no query) FIRST, then
 *   POST /agents/sessions/{id}/events (202, empty). The POST takes no `stream` field.
 * - Client tools: declare pi tools as {type:"function"}; the session emits
 *   agent.session.requires_action with session.required_actions[{turn_id,call_id,name,arguments}];
 *   answer with agent.session.input.tool_result {turn_id, call_id, success, output|error}.
 * - Reserved tool names: bash, amp_recurse, amp_send, amp_wake -> renamed on the wire.
 * - Session input accepts user messages only, so history replay is serialized text.
 * - session.usage is cumulative across turns; per-call usage is reported as a delta.
 *
 * Secrets: the API key only ever lives in the Authorization header. Nothing here logs
 * headers or request bodies; error text is scrubbed of bearer tokens.
 */
import { createHash, randomUUID } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import {
	type Api,
	type AssistantMessage,
	type AssistantMessageEventStream,
	collapseSystemMessages,
	createAssistantMessageEventStream,
	createProvider,
	envApiKeyAuth,
	getCurrentSystemPrompt,
	getCurrentTools,
	type Message,
	type Model,
	type SimpleStreamOptions,
	type TextContent,
	type Tool,
	type ToolCall,
	type TranscriptContext,
} from "@earendil-works/pi-ai";

export const AMPERSAND_API = "ampersand-agents";
export const DEFAULT_BASE_URL = "https://ampersand.humansand.ai/v1";
export const DEFAULT_MODEL = "ampersand-v0.1";

const RESERVED_TOOL_NAMES = new Set(["bash", "amp_recurse", "amp_send", "amp_wake"]);
const MAX_REPLAY_CHARS = 3_000_000; // body cap is 4 MiB; leave headroom
const MAX_REPLAY_TOOL_RESULT_CHARS = 20_000;
const MAX_TOOL_RESULT_CHARS = 3_500_000;
const STREAM_IDLE_TIMEOUT_MS = 10 * 60_000;

// ---------------------------------------------------------------------------
// Session mapping store
// ---------------------------------------------------------------------------

export interface UsageTotals {
	input: number;
	output: number;
	cacheRead: number;
	reasoning: number;
}

/** What Ampersand already holds for one transcript prefix. */
export interface SessionEntry {
	sessionId: string;
	configHash: string;
	/** call_id -> turn_id for calls awaiting a tool_result. */
	pending: Record<string, string>;
	/** Cumulative session usage already reported to pi. */
	reported: UsageTotals;
	/**
	 * Set while a request toward transcript hash `inflight` is outstanding. If the process dies,
	 * the same transcript arriving again re-attaches to that server turn instead of re-sending.
	 */
	inflight?: string;
	updatedAt: number;
}

/** Maps a transcript-prefix hash to the Ampersand session that holds exactly that prefix. */
export interface SessionStore {
	get(key: string): SessionEntry | undefined;
	set(key: string, entry: SessionEntry): void;
	/** Drop every key for a session (its server state moved past them, or it broke). */
	deleteSession(sessionId: string): void;
}

export class MemorySessionStore implements SessionStore {
	protected map = new Map<string, SessionEntry>();
	get(key: string) {
		return this.map.get(key);
	}
	set(key: string, entry: SessionEntry) {
		this.map.set(key, entry);
	}
	deleteSession(sessionId: string) {
		for (const [k, v] of this.map) if (v.sessionId === sessionId) this.map.delete(k);
	}
}

/** JSON-file store with atomic writes. Keeps the newest `limit` entries. */
export class FileSessionStore extends MemorySessionStore {
	private readonly path: string;
	private readonly limit: number;
	constructor(path: string, limit = 500) {
		super();
		this.path = path;
		this.limit = limit;
		try {
			if (existsSync(path)) {
				const data = JSON.parse(readFileSync(path, "utf8")) as Record<string, SessionEntry>;
				for (const [k, v] of Object.entries(data)) this.map.set(k, v);
			}
		} catch {
			// A corrupt map only costs a replay; start empty.
		}
	}
	override set(key: string, entry: SessionEntry) {
		super.set(key, entry);
		this.flush();
	}
	override deleteSession(sessionId: string) {
		super.deleteSession(sessionId);
		this.flush();
	}
	private flush() {
		const entries = [...this.map.entries()].sort((a, b) => b[1].updatedAt - a[1].updatedAt).slice(0, this.limit);
		this.map = new Map(entries);
		mkdirSync(dirname(this.path), { recursive: true });
		const tmp = `${this.path}.${process.pid}.tmp`;
		writeFileSync(tmp, JSON.stringify(Object.fromEntries(entries)), { mode: 0o600 });
		renameSync(tmp, this.path);
	}
}

// ---------------------------------------------------------------------------
// Stored credential (`amp-harness login`)
// ---------------------------------------------------------------------------

export function credentialsPath(): string {
	const base = process.env.XDG_CONFIG_HOME || `${process.env.HOME ?? "."}/.config`;
	return process.env.AMP_HARNESS_CREDENTIALS || `${base}/amp-harness/credentials.json`;
}

export function loadStoredKey(): string | undefined {
	try {
		const key = JSON.parse(readFileSync(credentialsPath(), "utf8"))?.apiKey;
		return typeof key === "string" && key ? key : undefined;
	} catch {
		return undefined;
	}
}

export function saveStoredKey(apiKey: string | undefined): void {
	const path = credentialsPath();
	mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
	const tmp = `${path}.${process.pid}.tmp`;
	writeFileSync(tmp, JSON.stringify(apiKey ? { apiKey } : {}), { mode: 0o600 });
	renameSync(tmp, path);
}

/** Key lookup order: $AMPERSAND_API_KEY, then the stored credential. */
export function resolveApiKey(): string {
	return process.env.AMPERSAND_API_KEY || loadStoredKey() || "";
}

// ---------------------------------------------------------------------------
// HTTP client
// ---------------------------------------------------------------------------

export class AmpersandError extends Error {
	readonly status?: number;
	readonly retryAfterMs?: number;
	constructor(message: string, status?: number, retryAfterMs?: number) {
		super(message);
		this.status = status;
		this.retryAfterMs = retryAfterMs;
	}
}

const scrub = (s: string) => s.replace(/Bearer\s+[A-Za-z0-9._\-]+/g, "Bearer [redacted]").replace(/\b(sk|amp)[-_][A-Za-z0-9_\-]{16,}/g, "[redacted]");

function describeHttpError(status: number, bodyText: string): string {
	let msg = bodyText;
	try {
		msg = JSON.parse(bodyText)?.error?.message ?? bodyText;
	} catch {}
	msg = scrub(String(msg)).slice(0, 500);
	if (status === 402) return `Ampersand: insufficient credit (402). ${msg}`;
	if (status === 401 || status === 403) return `Ampersand: authentication failed (${status}); check AMPERSAND_API_KEY. ${msg}`;
	if (status === 429) return `Ampersand: rate limit exceeded (429). ${msg}`;
	if (status >= 500) return `Ampersand: server error ${status} (overloaded/unavailable). ${msg}`;
	return `Ampersand: HTTP ${status}. ${msg}`;
}

function retryAfterMs(res: Response): number | undefined {
	const v = res.headers.get("retry-after");
	if (!v) return undefined;
	const secs = Number(v);
	if (Number.isFinite(secs)) return secs * 1000;
	const at = Date.parse(v);
	return Number.isFinite(at) ? Math.max(0, at - Date.now()) : undefined;
}

/** Ampersand 429 bodies say "retry after 3.63 seconds". */
function retryHintMs(bodyText: string): number | undefined {
	const m = /retry after ([\d.]+) seconds/i.exec(bodyText);
	return m ? Number(m[1]) * 1000 : undefined;
}

const sleep = (ms: number, signal?: AbortSignal) =>
	new Promise<void>((resolve, reject) => {
		const t = setTimeout(resolve, ms);
		signal?.addEventListener("abort", () => (clearTimeout(t), reject(new Error("Request was aborted"))), { once: true });
	});

export interface ClientOptions {
	apiKey: string;
	baseUrl?: string;
	signal?: AbortSignal;
	/** pi request instrumentation hook. */
	onResponse?: (res: Response) => void | Promise<void>;
}

export class AmpersandClient {
	readonly baseUrl: string;
	private readonly opts: ClientOptions;
	constructor(opts: ClientOptions) {
		this.opts = opts;
		if (!opts.apiKey) throw new AmpersandError("Ampersand: AMPERSAND_API_KEY is not set");
		this.baseUrl = (opts.baseUrl ?? DEFAULT_BASE_URL).replace(/\/+$/, "");
	}

	private headers(extra: Record<string, string> = {}): Record<string, string> {
		return {
			Authorization: `Bearer ${this.opts.apiKey}`,
			"OpenAI-Beta": "agents=v1",
			"Content-Type": "application/json",
			...extra,
		};
	}

	/** fetch with 429/5xx retries honoring Retry-After. Only used for idempotent requests. */
	async request(method: string, path: string, body?: unknown, extra: Record<string, string> = {}, attempts = 6): Promise<Response> {
		let lastErr: unknown;
		for (let i = 0; i < attempts; i++) {
			let res: Response;
			try {
				res = await fetch(this.baseUrl + path, {
					method,
					headers: this.headers(extra),
					body: body === undefined ? undefined : JSON.stringify(body),
					signal: this.opts.signal,
				});
			} catch (e) {
				if (this.opts.signal?.aborted) throw new Error("Request was aborted");
				lastErr = e;
				await sleep(500 * 2 ** i, this.opts.signal);
				continue;
			}
			await this.opts.onResponse?.(res);
			if (res.ok) return res;
			const text = await res.text().catch(() => "");
			const err = new AmpersandError(describeHttpError(res.status, text), res.status, retryAfterMs(res) ?? retryHintMs(text));
			if ((res.status === 429 || res.status === 503 || res.status === 502) && i < attempts - 1) {
				lastErr = err;
				await sleep(Math.min((err.retryAfterMs ?? 1000 * 2 ** i) + 250, 60_000), this.opts.signal);
				continue;
			}
			throw err;
		}
		throw lastErr instanceof Error ? lastErr : new AmpersandError(String(lastErr));
	}

	async json<T = any>(method: string, path: string, body?: unknown, extra?: Record<string, string>): Promise<T> {
		const res = await this.request(method, path, body, extra);
		const text = await res.text();
		return (text ? JSON.parse(text) : null) as T;
	}

	/**
	 * Raw SSE response. Network errors are not retried (a session create may have
	 * happened), but 429 is: the server rejected it before doing any work.
	 */
	async openStream(method: string, path: string, body?: unknown, attempts = 6): Promise<Response> {
		for (let i = 0; ; i++) {
			const res = await fetch(this.baseUrl + path, {
				method,
				headers: this.headers({ Accept: "text/event-stream" }),
				body: body === undefined ? undefined : JSON.stringify(body),
				signal: this.opts.signal,
			});
			await this.opts.onResponse?.(res);
			if (res.ok) return res;
			const text = await res.text().catch(() => "");
			const err = new AmpersandError(describeHttpError(res.status, text), res.status, retryAfterMs(res) ?? retryHintMs(text));
			if (res.status === 429 && i < attempts - 1) {
				await sleep(Math.min((err.retryAfterMs ?? 2000 * 2 ** i) + 250, 60_000), this.opts.signal);
				continue;
			}
			throw err;
		}
	}

	models() {
		return this.json<{ data: { id: string }[] }>("GET", "/models");
	}
	balance() {
		return this.json<{ available_micro: number; promotional_micro: number; purchased_micro: number; unsettled_micro: number }>("GET", "/account/balance");
	}
	getSession(id: string) {
		return this.json<any>("GET", `/agents/sessions/${id}`);
	}
	sendEvents(id: string, events: unknown[], idempotencyKey: string = randomUUID()) {
		return this.request("POST", `/agents/sessions/${id}/events`, { events }, { "Idempotency-Key": idempotencyKey });
	}
	cancel(id: string) {
		return this.sendEvents(id, [{ type: "agent.session.input.cancel" }]);
	}
}

/** Parse an SSE body into JSON events. Comment lines (": connected") yield {type:":comment"}. */
export async function* parseSse(res: Response, idleTimeoutMs = STREAM_IDLE_TIMEOUT_MS): AsyncGenerator<any> {
	const reader = res.body!.getReader();
	const dec = new TextDecoder();
	let buf = "";
	try {
		while (true) {
			let timer: ReturnType<typeof setTimeout> | undefined;
			const timeout = new Promise<never>((_, rej) => {
				timer = setTimeout(() => rej(new AmpersandError("Ampersand: event stream idle timeout")), idleTimeoutMs);
			});
			let r: ReadableStreamReadResult<Uint8Array>;
			try {
				r = await Promise.race([reader.read(), timeout]);
			} finally {
				clearTimeout(timer);
			}
			if (r.done) break;
			buf += dec.decode(r.value, { stream: true }).replace(/\r\n/g, "\n");
			let i: number;
			while ((i = buf.indexOf("\n\n")) >= 0) {
				const raw = buf.slice(0, i);
				buf = buf.slice(i + 2);
				const lines = raw.split("\n");
				const data = lines.filter((l) => l.startsWith("data:")).map((l) => l.slice(5).replace(/^ /, "")).join("\n");
				if (!data) {
					if (lines.some((l) => l.startsWith(":"))) yield { type: ":comment", text: raw };
					continue;
				}
				if (data === "[DONE]") return;
				try {
					yield JSON.parse(data);
				} catch {
					// ignore malformed frame
				}
			}
		}
	} finally {
		reader.cancel().catch(() => {});
	}
}

// ---------------------------------------------------------------------------
// Transcript normalization & hashing
// ---------------------------------------------------------------------------

function stableStringify(v: unknown): string {
	if (v === null || typeof v !== "object") return JSON.stringify(v) ?? "null";
	if (Array.isArray(v)) return `[${v.map(stableStringify).join(",")}]`;
	return `{${Object.keys(v as object)
		.sort()
		.filter((k) => (v as any)[k] !== undefined)
		.map((k) => `${JSON.stringify(k)}:${stableStringify((v as any)[k])}`)
		.join(",")}}`;
}

const textOf = (content: string | readonly { type: string; text?: string }[]): string =>
	typeof content === "string" ? content : content.map((c) => (c.type === "text" ? (c.text ?? "") : c.type === "image" ? "[image]" : "")).join("\n");

/** Stable view of a message: only what Ampersand saw/produced, nothing pi-local. */
function normalize(m: Message): unknown {
	switch (m.role) {
		case "user":
			return ["u", textOf(m.content as any)];
		case "assistant":
			return [
				"a",
				m.content.filter((c) => c.type === "text").map((c) => (c as TextContent).text).join(""),
				m.content.filter((c): c is ToolCall => c.type === "toolCall").map((c) => [c.id, c.name, c.arguments]),
			];
		case "toolResult":
			return ["t", m.toolCallId, !!m.isError, textOf(m.content as any)];
		default:
			return ["?", (m as any).role];
	}
}

const sha = (s: string) => createHash("sha256").update(s).digest("hex");
const chainStep = (prev: string, m: Message) => sha(prev + "\u0000" + stableStringify(normalize(m)));

/** hashes[i] = hash of messages[0..i] inclusive. */
function chainHashes(messages: Message[], seed: string): string[] {
	const out: string[] = [];
	let h = seed;
	for (const m of messages) out.push((h = chainStep(h, m)));
	return out;
}

// ---------------------------------------------------------------------------
// Tool mapping
// ---------------------------------------------------------------------------

export interface ToolMap {
	wire: any[];
	toPi: Map<string, string>;
	toWire: Map<string, string>;
}

export function mapTools(tools: Tool[]): ToolMap {
	const toPi = new Map<string, string>();
	const toWire = new Map<string, string>();
	const wire = tools.map((t) => {
		let name = t.name;
		if (RESERVED_TOOL_NAMES.has(name) || name.startsWith("amp_")) name = `pi_${name}`;
		while (toPi.has(name)) name = `${name}_`;
		toPi.set(name, t.name);
		toWire.set(t.name, name);
		// JSON round-trip drops TypeBox symbols.
		const parameters = JSON.parse(JSON.stringify(t.parameters ?? { type: "object", properties: {} }));
		return { type: "function", name, description: t.description ?? "", parameters };
	});
	return { wire, toPi, toWire };
}

// ---------------------------------------------------------------------------
// Input construction
// ---------------------------------------------------------------------------

const userInput = (text: string) => ({ role: "user", content: [{ type: "input_text", text: text || "(empty message)" }] });

function toolResultOutput(m: Extract<Message, { role: "toolResult" }>): string {
	const text = textOf(m.content as any);
	return text.length > MAX_TOOL_RESULT_CHARS ? `${text.slice(0, MAX_TOOL_RESULT_CHARS)}\n[truncated ${text.length - MAX_TOOL_RESULT_CHARS} chars]` : text;
}

function serializeForReplay(messages: Message[]): string {
	const parts: string[] = [];
	for (const m of messages) {
		if (m.role === "user") parts.push(`[user]\n${textOf(m.content as any)}`);
		else if (m.role === "assistant") {
			const lines: string[] = [];
			for (const c of m.content) {
				if (c.type === "text" && c.text.trim()) lines.push(c.text);
				else if (c.type === "toolCall") lines.push(`[tool call ${c.id}] ${c.name} ${JSON.stringify(c.arguments)}`);
			}
			if (lines.length) parts.push(`[assistant]\n${lines.join("\n")}`);
		} else if (m.role === "toolResult") {
			let t = textOf(m.content as any);
			if (t.length > MAX_REPLAY_TOOL_RESULT_CHARS) t = `${t.slice(0, MAX_REPLAY_TOOL_RESULT_CHARS)}\n[truncated]`;
			parts.push(`[tool result ${m.toolCallId} (${m.toolName})${m.isError ? " ERROR" : ""}]\n${t}`);
		}
	}
	// Keep the most recent history if it is too large.
	let total = 0;
	let start = parts.length;
	while (start > 0 && total + parts[start - 1].length < MAX_REPLAY_CHARS) total += parts[--start].length;
	const kept = parts.slice(start);
	if (start > 0) kept.unshift(`[${start} earlier entries omitted for size]`);
	return kept.join("\n\n");
}

/** Build session-create input from the full (non-system) transcript. */
function buildCreateInput(messages: Message[]): any[] {
	if (messages.length === 1 && messages[0].role === "user") return [userInput(textOf(messages[0].content as any))];
	// Trailing user messages are sent as real messages; everything before is replayed history.
	let cut = messages.length;
	while (cut > 0 && messages[cut - 1].role === "user") cut--;
	const history = messages.slice(0, cut);
	const tail = messages.slice(cut);
	const input: any[] = [];
	if (history.length) {
		const note =
			tail.length > 0
				? "The conversation so far is replayed below (the session was re-created). Treat it as real history; tool calls shown were executed by the client. Then answer the next message."
				: "The conversation so far is replayed below (the session was re-created). Treat it as real history; tool calls shown were executed by the client. The last tool results are new: continue the task from there.";
		input.push(userInput(`${note}\n\n<conversation_history>\n${serializeForReplay(history)}\n</conversation_history>`));
	}
	for (const m of tail) input.push(userInput(textOf(m.content as any)));
	return input;
}

/** Events that move an existing session from `entry` to the end of `delta`, or null if impossible. */
function buildDeltaEvents(delta: Message[], entry: SessionEntry): any[] | null {
	if (delta.length === 0) return null;
	const events: any[] = [];
	const answered = new Set<string>();
	for (const m of delta) {
		if (m.role === "toolResult") {
			const turnId = entry.pending[m.toolCallId];
			if (!turnId) return null;
			answered.add(m.toolCallId);
			events.push(
				m.isError
					? { type: "agent.session.input.tool_result", turn_id: turnId, call_id: m.toolCallId, success: false, error: toolResultOutput(m) || "Tool failed" }
					: { type: "agent.session.input.tool_result", turn_id: turnId, call_id: m.toolCallId, success: true, output: toolResultOutput(m) },
			);
		} else if (m.role === "user") {
			events.push({ type: "agent.session.input.message", input: [userInput(textOf(m.content as any))] });
		} else {
			return null; // assistant message we did not produce -> replay
		}
	}
	// Any call pi never answered would leave the session stuck in requires_action.
	for (const [callId, turnId] of Object.entries(entry.pending)) {
		if (!answered.has(callId)) {
			events.unshift({ type: "agent.session.input.tool_result", turn_id: turnId, call_id: callId, success: false, error: "Tool call was not executed by the client." });
		}
	}
	return events;
}

// ---------------------------------------------------------------------------
// Stream implementation
// ---------------------------------------------------------------------------

export interface AdapterOptions {
	store: SessionStore;
	/** Defaults to options.apiKey, then $AMPERSAND_API_KEY. */
	apiKey?: () => string | undefined;
	baseUrl?: string;
	/** Status line for diagnostics (never receives secrets or payloads). */
	debug?: (msg: string) => void;
}

const ZERO: UsageTotals = { input: 0, output: 0, cacheRead: 0, reasoning: 0 };

function usageTotals(u: any): UsageTotals | undefined {
	if (!u) return undefined;
	return {
		input: u.input_tokens ?? 0,
		output: u.output_tokens ?? 0,
		cacheRead: u.input_tokens_details?.cached_tokens ?? 0,
		reasoning: u.output_tokens_details?.reasoning_tokens ?? 0,
	};
}

export function createAmpersandStream(adapter: AdapterOptions) {
	const debug = adapter.debug ?? (() => {});

	return function streamAmpersand(model: Model<Api>, context: TranscriptContext, options?: SimpleStreamOptions): AssistantMessageEventStream {
		const stream = createAssistantMessageEventStream();
		const transcript = collapseSystemMessages(context);
		const instructions = getCurrentSystemPrompt(transcript.messages) ?? "";
		const tools = mapTools(getCurrentTools(transcript.messages) ?? []);
		const messages = transcript.messages.filter((m) => m.role !== "system") as Message[];

		const output: AssistantMessage = {
			role: "assistant",
			content: [],
			api: model.api,
			provider: model.provider,
			model: model.id,
			usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
			stopReason: "pending",
			timestamp: Date.now(),
		};

		(async () => {
			let sessionId: string | undefined;
			let client: AmpersandClient | undefined;
			let started = false;
			try {
				const apiKey = options?.apiKey || adapter.apiKey?.() || process.env.AMPERSAND_API_KEY || "";
				client = new AmpersandClient({
					apiKey,
					baseUrl: model.baseUrl || adapter.baseUrl || process.env.AMPERSAND_BASE_URL,
					signal: options?.signal,
					onResponse: options?.onResponse
						? (res) => options.onResponse!({ status: res.status, headers: Object.fromEntries(res.headers) } as any, model)
						: undefined,
				});

				const configHash = sha(stableStringify({ model: model.id, instructions, tools: tools.wire }));
				const hashes = chainHashes(messages, configHash);
				const target = hashes[hashes.length - 1] ?? configHash;
				const inflightKey = `inflight:${target}`;

				// Longest transcript prefix Ampersand already holds.
				let entry: SessionEntry | undefined;
				let entryKey = "";
				let k = messages.length;
				for (; k >= 1; k--) {
					const e = adapter.store.get(hashes[k - 1]);
					if (e && e.configHash === configHash) {
						entry = e;
						entryKey = hashes[k - 1];
						break;
					}
				}
				let events = entry ? buildDeltaEvents(messages.slice(k), entry) : null;
				if (entry && !events) {
					debug(`ampersand: cannot continue ${entry.sessionId}; replaying`);
					entry = undefined;
				}
				// A previous process created a session for exactly this transcript and died.
				const orphan = adapter.store.get(inflightKey);

				let res: Response | undefined; // undefined = re-attach without streaming
				let reported: UsageTotals = ZERO;
				if (orphan && orphan.configHash === configHash) {
					sessionId = orphan.sessionId;
					reported = orphan.reported;
					debug(`ampersand: re-attach ${sessionId} (session created by an interrupted request)`);
				} else if (entry && events) {
					sessionId = entry.sessionId;
					reported = entry.reported;
					// Same key for the same logical submission: a resend after a crash is deduplicated.
					const idemKey = sha(`${sessionId}:${target}`).slice(0, 48);
					const payload = (await options?.onPayload?.({ events }, model)) as any;
					if (payload?.events) events = payload.events;
					if (entry.inflight === target) {
						debug(`ampersand: re-attach ${sessionId} (resending ${events!.length} events idempotently)`);
						await client.sendEvents(sessionId, events!, idemKey).catch((e) => {
							if (!(e instanceof AmpersandError && e.status === 409)) throw e; // 409: already received
						});
					} else {
						debug(`ampersand: continue ${sessionId} (+${messages.length - k} msgs, ${events!.length} events)`);
						// Subscribe first so the turn's early events are not missed.
						res = await client.openStream("GET", `/agents/sessions/${sessionId}/events`);
						adapter.store.set(entryKey, { ...entry, inflight: target, updatedAt: Date.now() });
						await client.sendEvents(sessionId, events!, idemKey);
					}
				} else {
					// No agent.reasoning: v0.1 only accepts effort "high", which is the default.
					const agent: any = { model: model.id, instructions, tools: tools.wire };
					let body: any = { agent, environment: { type: "none" }, input: buildCreateInput(messages), stream: true };
					const replaced = await options?.onPayload?.(body, model);
					if (replaced) body = replaced;
					debug(`ampersand: new session (${messages.length} msgs [${messages.map((m) => m.role).join(",")}], ${tools.wire.length} tools)`);
					res = await client.openStream("POST", "/agents/sessions", body);
				}

				stream.push({ type: "start", partial: output });
				started = true;

				// item_id:content_index -> content block index
				const textBlocks = new Map<string, number>();
				let toolCalls: ToolCall[] = [];
				let cumulative: UsageTotals | undefined;
				let terminal: "stop" | "toolUse" | undefined;

				const textBlock = (ev: any) => {
					const key = `${ev.item_id}:${ev.content_index ?? 0}`;
					let idx = textBlocks.get(key);
					if (idx === undefined) {
						output.content.push({ type: "text", text: "" });
						idx = output.content.length - 1;
						textBlocks.set(key, idx);
						stream.push({ type: "text_start", contentIndex: idx, partial: output });
					}
					return idx;
				};

				for await (const ev of res ? parseSse(res) : []) {
					await options?.onProviderStreamEvent?.(ev, model);
					const type: string = ev.type;
					if (type === ":comment") continue;
					if (!sessionId && (ev.session?.id ?? ev.session_id)) {
						sessionId = ev.session?.id ?? ev.session_id;
						adapter.store.set(inflightKey, { sessionId: sessionId!, configHash, pending: {}, reported: ZERO, inflight: target, updatedAt: Date.now() });
					}
					// Ignore subagent turns (multi_agent is off, but be safe).
					if (ev.turn?.subagent_id) continue;

					if (type === "agent.session.turn.output_text.delta") {
						const idx = textBlock(ev);
						(output.content[idx] as TextContent).text += ev.delta ?? "";
						stream.push({ type: "text_delta", contentIndex: idx, delta: ev.delta ?? "", partial: output });
					} else if (type === "agent.session.turn.output_text.done") {
						const idx = textBlock(ev);
						const block = output.content[idx] as TextContent;
						if (typeof ev.text === "string" && ev.text !== block.text) {
							// Deltas may be absent or partial: emit the remainder.
							if (ev.text.startsWith(block.text)) {
								const rest = ev.text.slice(block.text.length);
								block.text = ev.text;
								stream.push({ type: "text_delta", contentIndex: idx, delta: rest, partial: output });
							} else block.text = ev.text;
						}
						stream.push({ type: "text_end", contentIndex: idx, content: block.text, partial: output });
					} else if (type === "agent.session.requires_action") {
						const actions = (ev.session?.required_actions ?? []).filter((a: any) => a.type === "function_call");
						if (!actions.length) throw new AmpersandError(`Ampersand: session requires an unsupported action (${(ev.session?.required_actions ?? []).map((a: any) => a.type).join(",") || "unknown"})`);
						for (const a of actions) {
							let args = a.arguments;
							if (typeof args === "string") {
								try {
									args = JSON.parse(args);
								} catch {
									args = {};
								}
							}
							const call: ToolCall & { turnId?: string } = { type: "toolCall", id: a.call_id, name: tools.toPi.get(a.name) ?? a.name, arguments: args ?? {} };
							output.content.push(call);
							const idx = output.content.length - 1;
							stream.push({ type: "toolcall_start", contentIndex: idx, partial: output });
							stream.push({ type: "toolcall_delta", contentIndex: idx, delta: JSON.stringify(call.arguments), partial: output });
							stream.push({ type: "toolcall_end", contentIndex: idx, toolCall: call, partial: output });
							toolCalls.push(call);
							(call as any).__turn = a.turn_id;
						}
						cumulative = usageTotals(ev.session?.usage);
						terminal = "toolUse";
						break;
					} else if (type === "agent.session.turn.completed") {
						terminal = "stop";
						break;
					} else if (type === "agent.session.turn.failed") {
						throw new AmpersandError(`Ampersand turn failed: ${scrub(ev.turn?.error?.message ?? "unknown error")}`);
					} else if (type === "agent.session.turn.cancelled") {
						throw new AmpersandError("Ampersand turn was cancelled");
					} else if (type === "agent.session.failed" || type === "agent.session.environment.failed") {
						throw new AmpersandError(`Ampersand session failed: ${scrub(ev.session?.error?.message ?? type)}`);
					} else if (type === "error") {
						throw new AmpersandError(`Ampersand error: ${scrub(ev.error?.message ?? "unknown")}`);
					}
				}

				if (options?.signal?.aborted) throw new Error("Request was aborted");
				if (!sessionId) throw new AmpersandError("Ampersand: stream ended before a session was created");

				if (!terminal) {
					// Stream dropped mid-turn: recover from saved session state.
					if (res) debug(`ampersand: stream closed early on ${sessionId}; recovering`);
					const rec = await recoverTurn(client, sessionId, options?.signal);
					for (const t of rec.texts) {
						if (output.content.some((c) => c.type === "text" && c.text === t)) continue;
						output.content.push({ type: "text", text: t });
						const idx = output.content.length - 1;
						stream.push({ type: "text_start", contentIndex: idx, partial: output });
						stream.push({ type: "text_delta", contentIndex: idx, delta: t, partial: output });
						stream.push({ type: "text_end", contentIndex: idx, content: t, partial: output });
					}
					for (const a of rec.actions) {
						const call: ToolCall = { type: "toolCall", id: a.call_id, name: tools.toPi.get(a.name) ?? a.name, arguments: a.arguments ?? {} };
						(call as any).__turn = a.turn_id;
						output.content.push(call);
						const idx = output.content.length - 1;
						stream.push({ type: "toolcall_start", contentIndex: idx, partial: output });
						stream.push({ type: "toolcall_end", contentIndex: idx, toolCall: call, partial: output });
						toolCalls.push(call);
					}
					cumulative = rec.usage;
					terminal = rec.actions.length ? "toolUse" : "stop";
				}

				if (!cumulative) cumulative = usageTotals((await client.getSession(sessionId))?.usage);
				const cur = cumulative ?? reported;
				output.usage.input = Math.max(0, cur.input - reported.input - (cur.cacheRead - reported.cacheRead));
				output.usage.cacheRead = Math.max(0, cur.cacheRead - reported.cacheRead);
				output.usage.output = Math.max(0, cur.output - reported.output);
				output.usage.reasoning = Math.max(0, cur.reasoning - reported.reasoning);
				output.usage.totalTokens = output.usage.input + output.usage.cacheRead + output.usage.output;

				// Record what the server now holds: transcript + this assistant message.
				const pending: Record<string, string> = {};
				for (const c of toolCalls) {
					pending[c.id] = (c as any).__turn;
					delete (c as any).__turn;
				}
				output.stopReason = terminal;
				adapter.store.deleteSession(sessionId);
				const key = chainStep(target, output);
				adapter.store.set(key, { sessionId, configHash, pending, reported: cur, updatedAt: Date.now() });

				stream.push({ type: "done", reason: terminal, message: output });
				stream.end();
			} catch (error) {
				for (const c of output.content) delete (c as any).__turn;
				const aborted = !!options?.signal?.aborted;
				if (sessionId) {
					// Server state may now diverge from pi's transcript: never reuse it.
					adapter.store.deleteSession(sessionId);
					if (aborted && client) {
						new AmpersandClient({ apiKey: options?.apiKey || adapter.apiKey?.() || process.env.AMPERSAND_API_KEY || "", baseUrl: client.baseUrl })
							.cancel(sessionId)
							.catch(() => {});
					}
				}
				output.stopReason = aborted ? "aborted" : "error";
				output.errorMessage = aborted ? "Request was aborted" : scrub(error instanceof Error ? error.message : String(error));
				void started; // setup failures may terminate with `error` before `start`
				stream.push({ type: "error", reason: output.stopReason, error: output });
				stream.end();
			}
		})();

		return stream;
	};
}

/** After a dropped stream: wait for the session to settle, then read its outcome. */
async function recoverTurn(client: AmpersandClient, sessionId: string, signal?: AbortSignal) {
	const deadline = Date.now() + STREAM_IDLE_TIMEOUT_MS;
	let s: any;
	for (let delay = 1000; ; delay = Math.min(delay * 1.5, 10_000)) {
		s = await client.getSession(sessionId);
		if (s.status !== "in_progress" && s.status !== "queued") break;
		if (Date.now() > deadline) throw new AmpersandError("Ampersand: turn did not settle after stream loss");
		await sleep(delay, signal);
	}
	if (s.status === "failed") throw new AmpersandError(`Ampersand session failed: ${scrub(s.error?.message ?? "unknown")}`);
	const actions = (s.required_actions ?? []).filter((a: any) => a.type === "function_call");
	// Assistant text produced since the last client input in the latest turn.
	const items = (await client.json<any>("GET", `/agents/sessions/${sessionId}/items?order=desc&limit=50`))?.data ?? [];
	const texts: string[] = [];
	for (const it of items) {
		if (it.type === "message" && it.role === "user") break;
		if (it.type === "function_call_output") break;
		if (it.type === "message" && it.role === "assistant") {
			const t = (it.content ?? []).filter((c: any) => c.type === "output_text").map((c: any) => c.text).join("");
			if (t) texts.unshift(t);
		}
	}
	return { actions, texts, usage: usageTotals(s.usage) };
}

/** A complete pi-ai Provider (for createModels().setProvider), used by the harness. */
export function ampersandProvider(adapter: AdapterOptions) {
	const baseUrl = adapter.baseUrl || process.env.AMPERSAND_BASE_URL || DEFAULT_BASE_URL;
	const streamSimple = createAmpersandStream(adapter);
	// With adapter.apiKey the key need not live in process.env, where tool shells would inherit it.
	const auth = adapter.apiKey
		? {
				name: "Ampersand API key",
				resolve: async () => {
					const key = adapter.apiKey!();
					return key ? { auth: { apiKey: key }, source: "host" } : undefined;
				},
			}
		: envApiKeyAuth("Ampersand API key", ["AMPERSAND_API_KEY"]);
	return createProvider({
		id: "ampersand",
		name: "Ampersand",
		baseUrl,
		auth: { apiKey: auth },
		models: [{ ...ampersandModelEntry(), api: AMPERSAND_API, provider: "ampersand", baseUrl } as any],
		api: { stream: streamSimple as any, streamSimple },
	});
}

/** Provider model entry for pi.registerProvider(). */
export function ampersandModelEntry(id = process.env.AMPERSAND_MODEL || DEFAULT_MODEL) {
	return {
		id,
		name: `Ampersand ${id.replace(/^ampersand-/, "")}`,
		reasoning: true,
		input: ["text"] as ("text" | "image")[],
		// Billing is credit-based (GET /account/balance); per-token prices are not published.
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: 200_000,
		maxTokens: 16_384,
	};
}
