module.exports = async function handler(req, res) {
  // CORS headers
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    return res.status(204).end();
  }
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY;
  if (!DEEPSEEK_API_KEY) {
    return res.status(500).json({ error: "DEEPSEEK_API_KEY not configured in Vercel Environment Variables" });
  }

  const body = req.body;
  const isStream = !!body.stream;

  // Anthropic 格式 → OpenAI/DeepSeek 格式
  const messages = [];
  if (body.system) messages.push({ role: "system", content: body.system });
  (body.messages || []).forEach(m => messages.push({ role: m.role, content: String(m.content) }));

  const dsBody = {
    model: "deepseek-chat",
    messages,
    max_tokens: body.max_tokens || 2000,
    stream: isStream,
  };

  const upstream = await fetch("https://api.deepseek.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${DEEPSEEK_API_KEY}`,
    },
    body: JSON.stringify(dsBody),
  });

  if (!upstream.ok) {
    const errText = await upstream.text();
    return res.status(upstream.status).json({ error: `DeepSeek error ${upstream.status}`, detail: errText });
  }

  if (!isStream) {
    // 非流式：转换响应格式 OpenAI → Anthropic
    const data = await upstream.json();
    const text = data.choices?.[0]?.message?.content || "";
    return res.status(200).json({ content: [{ type: "text", text }] });
  }

  // 流式：把 OpenAI SSE 转成 Anthropic SSE 格式
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("X-Accel-Buffering", "no");
  res.flushHeaders();

  const decoder = new TextDecoder();
  let buf = "";

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
      } catch { /* skip malformed */ }
    }
  }
  res.end();
}
