/**
 * amp-harness: a durable pi agent on Ampersand.
 *
 *   amp-harness login              store your API key (hidden prompt, or pipe it on stdin)
 *   amp-harness logout             remove the stored key
 *   amp-harness balance
 *   amp-harness send <text...>     submit input to the root conversation, stream the run
 *   amp-harness new <text...>      reset the context (history kept in storage), then send
 *   amp-harness resume             finish whatever a killed process left unfinished
 *   amp-harness status             pending tasks/submissions
 *   amp-harness history [n]        last n transcript entries (default 20)
 *
 * Options: --dir <path> (state dir, default ./.amp-harness), --cwd <path> (tool cwd), -v (show tool output)
 * State:   <dir>/session.sqlite (pi-durable), <dir>/ampersand-sessions.json (Ampersand session map)
 */
import { randomUUID } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels } from "@earendil-works/pi-ai";
import { createRegistry, Harness, watchEvents } from "@earendil-works/pi-durable";
import { NodeExecutionEnv } from "@earendil-works/pi-durable/env/node";
import { openNodeSqliteStorage } from "@earendil-works/pi-durable/storage/sqlite/node";
import { CodingTools } from "@earendil-works/pi-durable/tools";
import { AmpersandClient, ampersandProvider, credentialsPath, DEFAULT_MODEL, FileSessionStore, resolveApiKey, saveStoredKey } from "./ampersand.ts";

/** Read a secret without echoing it (TTY), or from piped stdin. */
async function readSecret(prompt: string): Promise<string> {
	const stdin = process.stdin;
	if (!stdin.isTTY) {
		let data = "";
		for await (const chunk of stdin) data += chunk;
		return data.trim();
	}
	process.stderr.write(prompt);
	stdin.setRawMode(true);
	stdin.resume();
	stdin.setEncoding("utf8");
	return new Promise((resolvePromise, reject) => {
		let value = "";
		const onData = (s: string) => {
			for (const ch of s) {
				if (ch === "\r" || ch === "\n") {
					stdin.setRawMode(false);
					stdin.pause();
					stdin.off("data", onData);
					process.stderr.write("\n");
					return resolvePromise(value.trim());
				}
				if (ch === "\u0003") {
					stdin.setRawMode(false);
					process.stderr.write("\n");
					return reject(new Error("cancelled"));
				}
				if (ch === "\u007f" || ch === "\b") value = value.slice(0, -1);
				else value += ch;
			}
		};
		stdin.on("data", onData);
	});
}

async function login() {
	const key = await readSecret("Ampersand API key (from https://ampersand.humansand.ai/): ");
	if (!key) throw new Error("no key entered");
	// Validate before saving; never echo the key.
	try {
		await new AmpersandClient({ apiKey: key, baseUrl: process.env.AMPERSAND_BASE_URL }).models();
	} catch (e) {
		throw new Error(`key rejected, not saved: ${(e as Error).message}`);
	}
	saveStoredKey(key);
	info(`saved to ${credentialsPath()} (mode 600)`);
	if (KEY_FROM_ENV) info("note: AMPERSAND_API_KEY in your environment takes precedence over the stored key");
	API_KEY = key;
	secrets.push(key);
	await balance(false);
}

const ctx = BACKGROUND_CONTEXT;

// ---- args -------------------------------------------------------------------
const argv = process.argv.slice(2);
const flag = (name: string) => {
	const i = argv.indexOf(name);
	if (i < 0) return undefined;
	const v = argv[i + 1];
	argv.splice(i, 2);
	return v;
};
const verbose = argv.includes("-v") ? (argv.splice(argv.indexOf("-v"), 1), true) : false;
const dir = resolve(flag("--dir") ?? process.env.AMP_HARNESS_DIR ?? ".amp-harness");
const toolCwd = resolve(flag("--cwd") ?? process.cwd());
const [cmd = "help", ...rest] = argv;
const text = rest.join(" ").trim();

// ---- secrets & output hygiene -------------------------------------------------
// Capture the key, then drop it from process.env: pi-durable's NodeExecutionEnv merges
// process.env into every tool shell, so a model-run `env` would otherwise exfiltrate it.
const KEY_FROM_ENV = !!process.env.AMPERSAND_API_KEY;
let API_KEY = resolveApiKey();
delete process.env.AMPERSAND_API_KEY;
const secrets = [API_KEY].filter((s) => s.length >= 8);
const redact = (s: string) => secrets.reduce((acc, k) => acc.split(k).join("[redacted]"), s);
const out = (s: string) => process.stdout.write(redact(s));
const info = (s: string) => process.stderr.write(redact(`${s}\n`));
const clip = (s: string, n = 160) => (s.length > n ? `${s.slice(0, n)}…` : s);


// ---- harness -----------------------------------------------------------------
async function balance(required: boolean) {
	try {
		const b = await new AmpersandClient({ apiKey: API_KEY, baseUrl: process.env.AMPERSAND_BASE_URL }).balance();
		const usd = (b.available_micro / 1e6).toFixed(4);
		info(`ampersand credit: $${usd} available`);
		if (required && b.available_micro <= 0) {
			info("ampersand: no credit left; top up before running.");
			process.exit(2);
		}
	} catch (e) {
		info(`ampersand balance check failed: ${(e as Error).message}`);
		if (required) process.exit(2);
	}
}

async function open() {
	mkdirSync(dir, { recursive: true });
	const store = new FileSessionStore(join(dir, "ampersand-sessions.json"));
	const models = createModels();
	models.setProvider(
		ampersandProvider({ store, apiKey: () => API_KEY, debug: process.env.AMPERSAND_DEBUG ? (m) => info(m) : undefined }),
	);
	const registry = createRegistry();
	registry.install(CodingTools);
	const harness = await Harness.open(
		await openNodeSqliteStorage(join(dir, "session.sqlite")),
		{
			models,
			registry,
			env: ({ cwd }) => new NodeExecutionEnv({ cwd: cwd ?? toolCwd }),
			settings: { stream: { timeoutMs: 15 * 60_000 }, retry: { maxRetries: 2 } },
			onReport: (e) => info(`harness report: ${(e as Error)?.message ?? String(e)}`),
		},
		ctx,
	);
	const root = await harness.root(ctx, {
		agent: { model: { provider: "ampersand", modelId: process.env.AMPERSAND_MODEL || DEFAULT_MODEL }, cwd: toolCwd },
	});
	return { harness, root };
}

type Opened = Awaited<ReturnType<typeof open>>;

/** Stream a conversation's agent events to the terminal. */
async function follow({ harness, root }: Opened) {
	const events = await watchEvents(harness, root.id, ctx);
	let midLine = false;
	let streamed = ""; // text of the in-flight assistant message already printed
	const nl = () => {
		if (midLine) out("\n");
		midLine = false;
	};
	const print = (s: string) => {
		if (!s) return;
		out(s);
		streamed += s;
		midLine = !s.endsWith("\n");
	};
	events.start(async (batch) => {
		for (const ev of batch) {
			if (ev.type === "message_update") {
				for (const c of ev.changes) {
					if (c.type === "text_delta") print(c.delta);
					// A commit can open a block with text already in it.
					else if (c.type === "text_start" && c.block.type === "text") print(c.block.text);
					else if (c.type === "block" && c.block.type === "text" && c.block.text.startsWith(streamed)) print(c.block.text.slice(streamed.length));
				}
			} else if (ev.type === "message_end") {
				// Short answers can land in one commit as a whole block, without deltas.
				const msg = ((ev.entry as any)?.model ?? []).find((m: any) => m.role === "assistant");
				if (msg) {
					const full = msg.content.filter((c: any) => c.type === "text").map((c: any) => c.text).join("");
					if (full.startsWith(streamed)) print(full.slice(streamed.length));
				}
				streamed = "";
				nl();
			} else if (ev.type === "tool_execution_start") {
				nl();
				info(`⚙ ${ev.toolName} ${clip(JSON.stringify(ev.args))}`);
			} else if (ev.type === "tool_execution_end") {
				const content = (ev.entry as any)?.model?.[0]?.content ?? [];
				const t = content.map((c: any) => c.text ?? "").join("");
				const isErr = (ev.entry as any)?.model?.[0]?.isError;
				info(`${isErr ? "✗" : "✓"} ${ev.toolName} (${t.length} chars)${verbose ? `\n${clip(t, 2000)}` : ""}`);
			} else if (ev.type === "auto_retry_start") {
				nl();
				info(`↻ retry ${ev.attempt}: ${clip(ev.errorMessage, 300)}`);
			} else if (ev.type === "task_failed") {
				nl();
				info(`task failed (${ev.kind}): ${clip(ev.message, 300)}`);
			}
		}
	});
	return events;
}

const lastSubmissionFile = () => join(dir, "last-submission");

async function settle(o: Opened, submissionId: string | undefined) {
	if (submissionId) {
		const sub = await o.harness.submission(Number(submissionId) as any, ctx);
		if (sub) {
			const settled = await sub.wait(ctx);
			if (settled.status !== "done") info(`submission ${settled.status}: ${JSON.stringify((settled as any).reason ?? (settled as any).detail ?? "")}`);
		}
	}
	await o.root.waitForIdle(ctx);
}

async function usageLine(o: Opened) {
	const u = await o.harness.usage(ctx);
	const m = Object.entries((u as any).models ?? {}).map(([k, v]: [string, any]) => `${k}: in ${v.input} (+${v.cacheRead} cached) out ${v.output}`);
	if (m.length) info(`usage ${m.join("; ")}`);
}

async function main() {
	if (cmd === "help" || cmd === "--help" || cmd === "-h") {
		info(readFileSync(new URL(import.meta.url), "utf8").split("*/")[0].replace(/^\/\*\*?|^ \* ?/gm, "").trim());
		return;
	}
	if (cmd === "login") return login();
	if (cmd === "logout") {
		saveStoredKey(undefined);
		info(`removed stored key (${credentialsPath()})`);
		return;
	}
	if (!API_KEY) {
		info("no Ampersand API key: run `amp-harness login` or set AMPERSAND_API_KEY");
		process.exit(2);
	}
	if (cmd === "balance") return balance(false);

	const runs = cmd === "send" || cmd === "new" || cmd === "resume";
	if (runs) await balance(true);
	const o = await open();
	const close = async () => {
		await o.harness.close(ctx).catch(() => {});
	};
	process.once("SIGINT", async () => {
		info("\ninterrupt: aborting the run (state is kept)");
		await o.root.abort(ctx).catch(() => {});
		await close();
		process.exit(130);
	});

	try {
		if (cmd === "send" || cmd === "new") {
			if (!text) throw new Error(`usage: amp-harness ${cmd} <text>`);
			if (cmd === "new") await o.root.reset(undefined, ctx);
			const events = await follow(o);
			// requestId makes a retried `send` after a crash before admission find the same submission.
			const requestId = process.env.AMP_REQUEST_ID ?? randomUUID();
			const sub = await o.root.submit({ type: "input", content: text, requestId }, ctx);
			writeFileSync(lastSubmissionFile(), String(sub.id));
			info(`submission ${sub.id}`);
			await settle(o, String(sub.id));
			await events.stop();
			await usageLine(o);
		} else if (cmd === "resume") {
			const pending = await o.harness.inspect(ctx);
			info(`resume: ${pending.tasks.length} live task(s), ${pending.submissions.length} unsettled submission(s)`);
			const events = await follow(o);
			o.harness.resume();
			let last: string | undefined;
			try {
				last = readFileSync(lastSubmissionFile(), "utf8").trim();
			} catch {}
			await settle(o, last);
			await events.stop();
			const view = await o.root.context(ctx);
			const final = [...view.messages].reverse().find((m) => m.role === "assistant");
			if (final && final.role === "assistant") {
				info("final answer:");
				out(`${final.content.filter((c) => c.type === "text").map((c: any) => c.text).join("")}\n`);
			}
			await usageLine(o);
		} else if (cmd === "status") {
			const s = await o.harness.inspect(ctx);
			out(`scheduling: ${s.scheduling}\n`);
			for (const t of s.tasks) out(`task ${t.record.id} ${(t.record as any).kind ?? ""} ${t.state.kind}\n`);
			for (const sub of s.submissions) out(`submission ${sub.id} ${(sub as any).status}\n`);
			if (!s.tasks.length && !s.submissions.length) out("idle\n");
		} else if (cmd === "history") {
			const n = Number(rest[0] ?? 20);
			const view = await o.root.context(ctx);
			for (const m of view.messages.slice(-n)) {
				if (m.role === "user") out(`[user] ${clip(typeof m.content === "string" ? m.content : m.content.map((c: any) => c.text ?? "[image]").join(""), 300)}\n`);
				else if (m.role === "assistant") {
					for (const c of m.content) {
						if (c.type === "text" && c.text) out(`[assistant] ${clip(c.text, 300)}\n`);
						if (c.type === "toolCall") out(`[tool call ${c.id}] ${c.name} ${clip(JSON.stringify(c.arguments))}\n`);
					}
				} else if (m.role === "toolResult") {
					out(`[tool result ${m.toolCallId}]${m.isError ? " ERROR" : ""} ${clip(m.content.map((c: any) => c.text ?? "").join(""), 200)}\n`);
				}
			}
		} else {
			throw new Error(`unknown command: ${cmd} (try --help)`);
		}
	} finally {
		await close();
	}
}

main().catch((e) => {
	info(`error: ${(e as Error)?.message ?? String(e)}`);
	process.exit(1);
});
