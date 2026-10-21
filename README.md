# amp-harness

**Run [pi](https://github.com/earendil-works/pi) on [Ampersand](https://ampersand.humansand.ai/), the collaborator model from humans&.**

Two things in one repo, sharing one adapter:

- **A pi provider extension**: pick `ampersand/ampersand-v0.1` in pi like any other model. pi's own tools (`bash`, `read`, `edit`, `write`) work.
- **`amp-harness`**: a small durable agent CLI built on [`@earendil-works/pi-durable`](https://www.npmjs.com/package/@earendil-works/pi-durable). You can `kill -9` it mid-turn and `resume` without losing the tool call record.

pi runs the agent loop and every tool on your machine. Ampersand keeps the conversation on its side ([sessions](https://ampersand.humansand.ai/docs/)), so each request sends only what's new, not the whole history.

```
pi CLI / amp-harness ──> pi-durable (SQLite) ──> ampersand adapter ──> Ampersand Agents API
        runs tools locally        durable state       session mapping          keeps the conversation
```

## Quick start

```sh
git clone git@github.com:leovoon/amp-harness.git && cd amp-harness
./scripts/install.sh            # npm install, put amp-harness on PATH, install the pi extension
amp-harness login               # paste your key from https://ampersand.humansand.ai/ (hidden prompt)
```

`login` checks the key against the API before saving it to `~/.config/amp-harness/credentials.json` (mode 600). Both the harness and the pi extension use the saved key. `AMPERSAND_API_KEY` in the environment takes precedence, and `amp-harness logout` removes the saved key. For scripts, pipe the key in: `printf %s "$KEY" | amp-harness login`.

Requires Node ≥ 22.19 (it runs TypeScript directly via `--experimental-strip-types`) and, for the extension, pi ≥ 1.3.

### In pi

```sh
pi --model ampersand/ampersand-v0.1                    # interactive
pi -p --model ampersand/ampersand-v0.1 "say OK"        # one-shot
AMPERSAND_DEBUG=1 pi -p --model ampersand/ampersand-v0.1 "…"   # show session routing on stderr
```

`pi -c` continues the same Ampersand session, even from a new process.

### amp-harness

```sh
amp-harness login | logout          # store / remove your API key
amp-harness balance                 # remaining Ampersand credit
amp-harness send "fix the failing test"
amp-harness new  "start over: …"    # new context (old history stays in storage)
amp-harness resume                  # finish whatever a killed process left
amp-harness status                  # live tasks and submissions
amp-harness history 20              # last 20 transcript entries
```

Options: `--dir <path>` for the state dir (default `./.amp-harness`), `--cwd <path>` for the tools' working directory, and `-v` to print tool output. State is `session.sqlite` (pi-durable) plus `ampersand-sessions.json` (the session map). Every run checks your credit first.

## Configuration

| Variable | Default | |
|---|---|---|
| `AMPERSAND_API_KEY` | key saved by `login` | Overrides the saved key. Get one from the [developer console](https://ampersand.humansand.ai/) |
| `AMP_HARNESS_CREDENTIALS` | `~/.config/amp-harness/credentials.json` | Where `login` saves the key |
| `AMPERSAND_BASE_URL` | `https://ampersand.humansand.ai/v1` | |
| `AMPERSAND_MODEL` | `ampersand-v0.1` | |
| `AMPERSAND_DEBUG` | unset | `1` prints session routing (no payloads, no secrets) |
| `AMPERSAND_SESSION_MAP` | `~/.pi/agent/ampersand/sessions.json` | Session map location for the pi extension |
| `AMP_HARNESS_DIR` | `./.amp-harness` | Harness state dir |

If there is no saved key and no `AMPERSAND_API_KEY`, `bin/amp-harness` loads `AMPERSAND_*` exports from `~/.zshrc` as a fallback, because non-interactive shells skip `.zshrc`.

## How it works

- **Tools run locally.** Every pi tool is declared to Ampersand as a [function tool](https://ampersand.humansand.ai/docs/GUIDES.html#complete-example). When the model calls one, the session emits `agent.session.requires_action`. pi runs the tool and sends `agent.session.input.tool_result`, and the same turn continues.
- **Only what's new is sent.** The adapter hashes the conversation and finds the longest prefix the Ampersand session already holds. It then sends only the new messages or tool results. If pi branches, compacts, changes model or tools, or the map is lost, the adapter starts a new session and replays the history.
- **Crash-safe.** Before each request the adapter records it as in flight, and event POSTs use a repeatable `Idempotency-Key`. After a crash, the same transcript re-attaches to the server turn instead of submitting twice. pi-durable keeps the tool-call record: an interrupted call becomes an `interrupted` error result, and the model carries on.
- **Recovery.** A dropped stream falls back to [saved session state](https://ampersand.humansand.ai/docs/GUIDES.html#recovery). 429/502/503 errors are retried, honoring Retry-After.

## Security

- The API key goes only into the `Authorization` header. Errors are scrubbed and harness output is redacted.
- The key is **removed from `process.env`** after it is read, in both pi and the harness. pi's `bash` tool and pi-durable's `NodeExecutionEnv` pass `process.env` to tool shells, so otherwise a model-run `env` would put the key in the transcript.
- Session map files are written `0600` and hold only hashes and session ids.

## Ampersand specifics (checked against the live API)

| | |
|---|---|
| `environment` | Required when creating a session; this uses `{type: "none"}` |
| Reserved tool names | `bash`, `amp_recurse`, `amp_send`, `amp_wake`; pi's `bash` is renamed `pi_bash` on the wire |
| Streaming follow-ups | Open `GET /agents/sessions/{id}/events` first, then `POST` input (202) |
| Session input | User messages only, so a replay sends history as serialized text |
| Reasoning effort | Only `high`, the default, is accepted, so it is never sent |
| Usage | `session.usage` is cumulative; per-request usage is a delta |
| Rate limit | 10 work requests/min, burst 5; long tool loops slow down and wait |

See the [OpenAI compatibility notes](https://ampersand.humansand.ai/docs/GUIDES.html#openai-compatibility) and the [API reference](https://ampersand.humansand.ai/docs/API_REFERENCE.html).

## Limits

- Text only; no image input.
- Cost shows `$0` in pi because Ampersand bills prepaid credit ([pricing and limits](https://ampersand.humansand.ai/docs/PRICING.html)). Use `amp-harness balance`. Token counts are accurate.
- Not yet: a Cloudflare Durable Object deployment (pi-durable's `storage/sqlite/cloudflare`). The session map is an interface, so a DO-backed store can replace the file store.

## Layout

| Path | |
|---|---|
| `src/ampersand.ts` | Adapter: HTTP/SSE client, session map, pi `streamSimple`, pi-ai `Provider` |
| `src/extension.ts` | pi extension entry |
| `src/cli.ts`, `bin/amp-harness` | Harness CLI |
| `scripts/install.sh` | Installer |
| `probe/` | Small scripts that probe the live API (`./withenv.sh node probe/probe.mjs balance`) |

```sh
npm run typecheck
```

## Links

- [Ampersand](https://ampersand.humansand.ai/): developer console and API keys
- [Ampersand docs](https://ampersand.humansand.ai/docs/): [Guides](https://ampersand.humansand.ai/docs/GUIDES.html), [API reference](https://ampersand.humansand.ai/docs/API_REFERENCE.html), [Pricing](https://ampersand.humansand.ai/docs/PRICING.html)
- [pi custom providers](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/custom-provider.md)
- [pi-durable](https://www.npmjs.com/package/@earendil-works/pi-durable)
