// Probe: how to stream follow-up input on an existing session.
const BASE = process.env.AMPERSAND_BASE_URL;
const H = { Authorization: `Bearer ${process.env.AMPERSAND_API_KEY}`, "OpenAI-Beta": "agents=v1", "Content-Type": "application/json" };
const sid = process.argv[2];
const variant = process.argv[3];
const trunc = (s, n = 300) => (s.length > n ? s.slice(0, n) + "…" : s);

async function show(r) {
  console.log("->", r.status, r.headers.get("content-type"));
  if (!r.body) return;
  const dec = new TextDecoder(); let buf = ""; let n = 0;
  for await (const c of r.body) {
    buf += dec.decode(c, { stream: true });
    let i;
    while ((i = buf.indexOf("\n\n")) >= 0) {
      const raw = buf.slice(0, i); buf = buf.slice(i + 2);
      const data = raw.split("\n").filter((l) => l.startsWith("data:")).map((l) => l.slice(5).trim()).join("");
      if (!data) { console.log("RAW", trunc(raw)); continue; }
      try { const ev = JSON.parse(data); console.log("EV", ev.type, ev.delta ? JSON.stringify(ev.delta) : trunc(JSON.stringify(ev), 200)); if (/turn\.(completed|failed)|requires_action/.test(ev.type)) { r.body.cancel().catch(() => {}); return; } } catch { console.log("DATA", trunc(data)); }
      if (++n > 60) return;
    }
  }
  if (buf.trim()) console.log("TAIL", trunc(buf, 800));
}

const msg = (text) => ({ type: "agent.session.input.message", input: [{ role: "user", content: [{ type: "input_text", text }] }] });

if (variant === "post-stream") {
  await show(await fetch(`${BASE}/agents/sessions/${sid}/events`, { method: "POST", headers: { ...H, Accept: "text/event-stream", "Idempotency-Key": crypto.randomUUID() }, body: JSON.stringify({ events: [msg("Now say DONE.")], stream: true }) }));
} else if (variant === "get-plain") {
  const ac = new AbortController(); setTimeout(() => ac.abort(), 8000);
  try { await show(await fetch(`${BASE}/agents/sessions/${sid}/events`, { headers: { ...H, Accept: "text/event-stream" }, signal: ac.signal })); } catch (e) { console.log("aborted/err", e.name); }
} else if (variant === "get-then-post") {
  // Open GET stream (no ?stream), then post message.
  const ac = new AbortController(); setTimeout(() => ac.abort(), 60000);
  const p = fetch(`${BASE}/agents/sessions/${sid}/events`, { headers: { ...H, Accept: "text/event-stream" }, signal: ac.signal }).then(show).catch((e) => console.log("stream err", e.name));
  await new Promise((r) => setTimeout(r, 800));
  const r = await fetch(`${BASE}/agents/sessions/${sid}/events`, { method: "POST", headers: H, body: JSON.stringify({ events: [msg("Say STREAMED.")] }) });
  console.log("post", r.status);
  await p;
} else if (variant === "turns") {
  const r = await fetch(`${BASE}/agents/sessions/${sid}/turns?order=desc&limit=3`, { headers: H });
  console.log(r.status, trunc(await r.text(), 2000));
}
