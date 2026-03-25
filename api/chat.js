export const config = { runtime: 'edge' };

export default async function handler(req) {
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  };

  if (req.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }
  if (req.method !== 'POST') {
    return new Response('Method Not Allowed', { status: 405, headers: corsHeaders });
  }

  const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY;
  if (!DEEPSEEK_API_KEY) {
    return new Response(
      JSON.stringify({ error: 'DEEPSEEK_API_KEY not set in Vercel environment variables' }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }

  let body;
  try { body = await req.json(); } catch {
    return new Response(JSON.stringify({ error: 'Invalid JSON' }), { status: 400, headers: corsHeaders });
  }

  const isStream = !!body.stream;

  // Anthropic 格式 → OpenAI/DeepSeek 格式
  const messages = [];
  if (body.system) messages.push({ role: 'system', content: body.system });
  (body.messages || []).forEach(m => messages.push({ role: m.role, content: String(m.content) }));

  const dsBody = {
    model: 'deepseek-chat',
    messages,
    max_tokens: body.max_tokens || 2000,
    stream: isStream,
  };

  const upstream = await fetch('https://api.deepseek.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${DEEPSEEK_API_KEY}`,
    },
    body: JSON.stringify(dsBody),
  });

  if (!upstream.ok) {
    const errText = await upstream.text();
    return new Response(
      JSON.stringify({ error: `DeepSeek error ${upstream.status}`, detail: errText }),
      { status: upstream.status, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }

  // ── 非流式：转换格式返回 ──
  if (!isStream) {
    const data = await upstream.json();
    const text = data.choices?.[0]?.message?.content || '';
    return new Response(
      JSON.stringify({ content: [{ type: 'text', text }] }),
      { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }

  // ── 流式：OpenAI SSE → Anthropic SSE 格式转换 ──
  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const enc = new TextEncoder();
  const dec = new TextDecoder();

  (async () => {
    const reader = upstream.body.getReader();
    let buf = '';
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += dec.decode(value, { stream: true });
        const lines = buf.split('\n');
        buf = lines.pop() ?? '';
        for (const line of lines) {
          if (!line.startsWith('data: ')) continue;
          const payload = line.slice(6).trim();
          if (payload === '[DONE]') continue;
          try {
            const chunk = JSON.parse(payload);
            const delta = chunk.choices?.[0]?.delta?.content;
            if (delta != null) {
              const out = JSON.stringify({
                type: 'content_block_delta',
                delta: { type: 'text_delta', text: delta },
              });
              await writer.write(enc.encode(`data: ${out}\n\n`));
            }
          } catch { /* skip malformed chunks */ }
        }
      }
    } finally {
      await writer.close().catch(() => {});
    }
  })();

  return new Response(readable, {
    status: 200,
    headers: {
      ...corsHeaders,
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'X-Accel-Buffering': 'no',
    },
  });
}
