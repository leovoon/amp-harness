// Live probe of the Ampersand Agents API. Prints event types/payloads, never auth headers.
const BASE = process.env.AMPERSAND_BASE_URL ?? "https://ampersand.humansand.ai/v1";
const KEY = process.env.AMPERSAND_API_KEY;
const MODEL = process.env.AMPERSAND_MODEL ?? "ampersand-v0.1";
const H = { Authorization: `Bearer ${KEY}`, "OpenAI-Beta": "agents=v1", "Content-Type": "application/json" };
const mode = process.argv[2] ?? "balance";
const trunc = (s, n = 400) => (s.length > n ? s.slice(0, n) + `…(+${s.length - n})` : s);

async function j(method, path, body, extra = {}) {
  const r = await fetch(BASE + path, { method, headers: { ...H, ...extra }, body: body ? JSON.stringify(body) : undefined });
  const t = await r.text();
  console.log(`${method} ${path} -> ${r.status}`, trunc(t, 1500));
  return t ? JSON.parse(t) : null;
}

async function sse(method, path, body, until = () => false) {
  const r = await fetch(BASE + path, { method, headers: { ...H, Accept: "text/event-stream" }, body: body ? JSON.stringify(body) : undefined });
  console.log(`${method} ${path} -> ${r.status} ${r.headers.get("content-type")}`);
  if (!r.ok) { console.log(trunc(await r.text(), 1500)); return []; }
  const dec = new TextDecoder(); let buf = ""; const out = [];
  for await (const chunk of r.body) {
    buf += dec.decode(chunk, { stream: true });
    let i;
    while ((i = buf.indexOf("\n\n")) >= 0) {
      const raw = buf.slice(0, i); buf = buf.slice(i + 2);
      const data = raw.split("\n").filter((l) => l.startsWith("data:")).map((l) => l.slice(5).trimStart()).join("\n");
      const evName = raw.split("\n").find((l) => l.startsWith("event:"));
      if (!data) { if (raw.trim()) console.log("RAW", trunc(raw)); continue; }
      if (data === "[DONE]") { console.log("[DONE]"); return out; }
      let ev; try { ev = JSON.parse(data); } catch { console.log("NONJSON", trunc(data)); continue; }
      out.push(ev);
      const t = ev.type;
      if (t?.endsWith(".delta")) process.stdout.write(`  ${t} ${JSON.stringify(ev.delta)}\n`);
      else console.log(`EV ${evName ?? ""} ${t}`, trunc(JSON.stringify(ev), 900));
      if (until(ev)) { r.body.cancel?.().catch(() => {}); return out; }
    }
  }
  console.log("stream closed");
  return out;
}

const endTurn = (ev) => ["agent.session.turn.completed", "agent.session.turn.failed", "agent.session.turn.cancelled", "agent.session.requires_action", "agent.session.failed", "error"].includes(ev.type);

const echoTool = {
  type: "function", name: "pi_bash", description: "Run a shell command locally and return stdout.",
  parameters: { type: "object", properties: { command: { type: "string" } }, required: ["command"], additionalProperties: false },
};

if (mode === "balance") {
  await j("GET", "/account/balance");
  await j("GET", "/models");
} else if (mode === "text") {
  await sse("POST", "/agents/sessions", { environment: { type: "none" }, agent: { model: MODEL, instructions: "Be terse." }, input: "say OK", stream: true }, endTurn);
} else if (mode === "tool") {
  const evs = await sse("POST", "/agents/sessions", {
    environment: { type: "none" }, agent: { model: MODEL, instructions: "You are a coding agent. Use the bash tool when asked to run commands.", tools: [echoTool] },
    input: "Run `echo hello` with the bash tool and tell me its output.", stream: true,
  }, endTurn);
  const sid = evs.find((e) => e.session_id)?.session_id ?? evs.find((e) => e.session?.id)?.session?.id;
  console.log("SESSION", sid);
  if (sid) {
    const s = await j("GET", `/agents/sessions/${sid}`);
    const act = s?.required_actions?.[0];
    if (act) {
      // Subscribe first, then send the tool result.
      const p = sse("GET", `/agents/sessions/${sid}/events?stream=true`, undefined, (ev) => ["agent.session.turn.completed", "agent.session.turn.failed", "agent.session.requires_action", "error"].includes(ev.type));
      await new Promise((r) => setTimeout(r, 500));
      await j("POST", `/agents/sessions/${sid}/events`, { events: [{ type: "agent.session.input.tool_result", turn_id: act.turn_id, call_id: act.call_id, success: true, output: "hello\n" }] }, { "Idempotency-Key": crypto.randomUUID() });
      await p;
      await j("GET", `/agents/sessions/${sid}/items?order=asc&limit=100`);
    }
  }
} else if (mode === "replay") {
  // Can a new session be seeded with prior assistant / function_call items?
  await sse("POST", "/agents/sessions", {
    environment: { type: "none" }, agent: { model: MODEL, instructions: "Be terse.", tools: [echoTool] },
    input: [
      { role: "user", content: [{ type: "input_text", text: "My favourite colour is teal. Remember it." }] },
      { role: "assistant", content: [{ type: "output_text", text: "Noted: teal." }] },
      { role: "user", content: [{ type: "input_text", text: "What is my favourite colour? One word." }] },
    ], stream: true,
  }, endTurn);
} else if (mode === "replay-fc") {
  await sse("POST", "/agents/sessions", {
    environment: { type: "none" }, agent: { model: MODEL, instructions: "Be terse.", tools: [echoTool] },
    input: [
      { role: "user", content: [{ type: "input_text", text: "Run echo 42" }] },
      { type: "function_call", call_id: "call_x1", name: "pi_bash", arguments: JSON.stringify({ command: "echo 42" }) },
      { type: "function_call_output", call_id: "call_x1", output: "42\n" },
      { role: "user", content: [{ type: "input_text", text: "What number did the command print? Just the number." }] },
    ], stream: true,
  }, endTurn);
} else if (mode === "session") {
  await j("GET", `/agents/sessions/${process.argv[3]}`);
  await j("GET", `/agents/sessions/${process.argv[3]}/turns`);
}
