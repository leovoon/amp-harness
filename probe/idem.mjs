// Does Ampersand dedupe events by Idempotency-Key? Is status updated synchronously on 202?
const B = process.env.AMPERSAND_BASE_URL;
const H = { Authorization: `Bearer ${process.env.AMPERSAND_API_KEY}`, "OpenAI-Beta": "agents=v1", "Content-Type": "application/json" };
const sid = process.argv[2];
const get = async (p) => (await fetch(B + p, { headers: H })).json();
const key = "idem-" + Date.now();
const body = JSON.stringify({ events: [{ type: "agent.session.input.message", input: [{ role: "user", content: [{ type: "input_text", text: "Reply: one" }] }] }] });
const before = (await get(`/agents/sessions/${sid}/turns?limit=100`)).data.length;
const r1 = await fetch(`${B}/agents/sessions/${sid}/events`, { method: "POST", headers: { ...H, "Idempotency-Key": key }, body });
const s1 = await get(`/agents/sessions/${sid}`);
console.log("post1", r1.status, "status right after:", s1.status);
const r2 = await fetch(`${B}/agents/sessions/${sid}/events`, { method: "POST", headers: { ...H, "Idempotency-Key": key }, body });
console.log("post2 same key", r2.status, (await r2.text()).slice(0, 200), [...r2.headers].filter(([k]) => /idem|replay/i.test(k)));
for (let i = 0; i < 30; i++) { const s = await get(`/agents/sessions/${sid}`); if (s.status === "idle") break; await new Promise((r) => setTimeout(r, 1000)); }
const after = (await get(`/agents/sessions/${sid}/turns?limit=100`)).data.length;
console.log("turns before", before, "after", after, "(+1 = deduped, +2 = not)");
const r3 = await fetch(`${B}/agents/sessions/${sid}/events`, { method: "POST", headers: { ...H, "Idempotency-Key": key }, body: body.replace("one", "two") });
console.log("post3 same key, different body", r3.status, (await r3.text()).slice(0, 200));
