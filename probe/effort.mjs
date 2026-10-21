// Which agent.reasoning.effort values does Ampersand accept? (non-streaming create; 400 = rejected)
const H = { Authorization: `Bearer ${process.env.AMPERSAND_API_KEY}`, "OpenAI-Beta": "agents=v1", "Content-Type": "application/json" };
for (const effort of ["none", "minimal", "low", "medium", "high", "xhigh", "max"]) {
  const r = await fetch(`${process.env.AMPERSAND_BASE_URL}/agents/sessions`, {
    method: "POST", headers: H,
    body: JSON.stringify({ agent: { model: process.env.AMPERSAND_MODEL, instructions: "Reply with one word.", reasoning: { effort } }, environment: { type: "none" }, input: "hi" }),
  });
  const t = await r.text();
  let id = ""; try { id = JSON.parse(t).id ?? ""; } catch {}
  console.log(effort.padEnd(8), r.status, r.ok ? id : t.slice(0, 160));
  if (r.ok && id) await fetch(`${process.env.AMPERSAND_BASE_URL}/agents/sessions/${id}/events`, { method: "POST", headers: H, body: JSON.stringify({ events: [{ type: "agent.session.input.cancel" }] }) });
}
