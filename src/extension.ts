/**
 * pi extension: registers the "ampersand" provider (humansand.ai Agents API).
 *
 * Install: symlinked as ~/.pi/agent/extensions/ampersand-provider.ts
 * Use:     pi --model ampersand/ampersand-v0.1 "..."
 * Env:     AMPERSAND_API_KEY (required), AMPERSAND_BASE_URL, AMPERSAND_MODEL,
 *          AMPERSAND_DEBUG=1 prints session routing lines to stderr (no payloads/secrets).
 */
import { homedir } from "node:os";
import { join } from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { AMPERSAND_API, ampersandModelEntry, createAmpersandStream, DEFAULT_BASE_URL, FileSessionStore, resolveApiKey } from "./ampersand.ts";

export default function (pi: ExtensionAPI) {
	const store = new FileSessionStore(process.env.AMPERSAND_SESSION_MAP ?? join(homedir(), ".pi", "agent", "ampersand", "sessions.json"));
	// Capture the key and remove it from process.env: pi's bash tool inherits process.env,
	// so a model-run `env` would otherwise send the key into the transcript.
	// $AMPERSAND_API_KEY wins; otherwise the key saved by `amp-harness login`.
	const key = resolveApiKey();
	delete process.env.AMPERSAND_API_KEY;
	// Literal value in pi's key syntax: `$$` is a literal `$`, a leading `$!` a literal `!`.
	const literal = key.replace(/\$/g, "$$$$").replace(/^!/, "$!");
	pi.registerProvider("ampersand", {
		baseUrl: process.env.AMPERSAND_BASE_URL || DEFAULT_BASE_URL,
		apiKey: key ? literal : "$AMPERSAND_API_KEY",
		api: AMPERSAND_API,
		models: [ampersandModelEntry()],
		streamSimple: createAmpersandStream({
			store,
			debug: process.env.AMPERSAND_DEBUG ? (m) => process.stderr.write(`${m}\n`) : undefined,
		}),
	} as any);
}
