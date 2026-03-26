module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") return res.status(204).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method Not Allowed" });

  const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY;
  if (!DEEPSEEK_API_KEY) {
    return res.status(500).json({ error: "DEEPSEEK_API_KEY not set in Vercel Environment Variables" });
  }

  // ── 手动解析 body（Vercel Node.js 默认不自动解析）──
  let body;
  try {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    body = JSON.parse(Buffer.concat(chunks).toString());
  } catch (e) {
    return res.status(400).json({ error: "Invalid JSON body" });
  }

  const isStream = !!body.stream;

  // Anthropic 格式 → DeepSeek/OpenAI 格式
  const messages = [];
  if (body.system) messages.push({ role: "system", content: body.system });
  (body.messages || []).forEach(m =>
    messages.push({ role: m.role, content: String(m.content) })
  );

  const dsBody = {
    model: "deepseek-chat",
    messages,
    max_tokens: body.max_tokens || 2000,
    stream: isStream,
  };

  let upstream;
  try {
    upstream = await fetch("https://api.deepseek.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${DEEPSEEK_API_KEY}`,
      },
      body: JSON.stringify(dsBody),
    });
  } catch (e) {
    return res.status(502).json({ error: "Failed to reach DeepSeek API", detail: e.message });
  }

  if (!upstream.ok) {
    const errText = await upstream.text();
    return res.status(upstream.status).json({
      error: `DeepSeek error ${upstream.status}`,
      detail: errText.slice(0, 500),
    });
  }

  // ── 非流式 ──
  if (!isStream) {
    const data = await upstream.json();
    const text = data.choices?.[0]?.message?.content || "";
    return res.status(200).json({ content: [{ type: "text", text }] });
  }

  // ── 流式：OpenAI SSE → Anthropic SSE ──
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("X-Accel-Buffering", "no");
  res.flushHeaders();

  const decoder = new TextDecoder();
  let buf = "";

  try {
    for await (const chunk of upstream.body) {
      buf += decoder.decode(chunk, { stream: true });
      const lines = buf.split("\n");
      buf = lines.pop() ?? "";
      for (const line of lines) {
        if (!line.startsWith("data: ")) continue;
        const payload = line.slice(6).trim();
        if (payload === "[DONE]") continue;
        try {
          const parsed = JSON.parse(payload);
          const delta = parsed.choices?.[0]?.delta?.content;
          if (delta != null) {
            const out = JSON.stringify({
              type: "content_block_delta",
              delta: { type: "text_delta", text: delta },
            });
            res.write(`data: ${out}\n\n`);
          }
        } catch { /* skip malformed chunks */ }
      }
    }
  } catch (e) {
    res.write(`data: ${JSON.stringify({ type: "error", message: e.message })}\n\n`);
  } finally {
    res.end();
  }
};
