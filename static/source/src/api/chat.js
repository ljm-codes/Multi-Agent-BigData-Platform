import { AUTH_PREFIX, ApiError, getToken, handleUnauthorized, request } from './http'

/**
 * 解析一个 SSE 事件块（形如 "event: token\ndata: {...}"）
 */
function parseEvent(raw) {
  let event = 'message'
  const dataLines = []
  for (const line of raw.split('\n')) {
    if (line.startsWith('event:')) event = line.slice(6).trim()
    else if (line.startsWith('data:')) dataLines.push(line.slice(5).trim())
  }
  if (!dataLines.length) return null
  let data = null
  try {
    data = JSON.parse(dataLines.join('\n'))
  } catch {
    data = null
  }
  return { event, data }
}

function dispatch({ event, data }, handlers) {
  switch (event) {
    case 'meta':
      handlers.onMeta?.(data)
      break
    case 'update':
      handlers.onUpdate?.(data)
      break
    case 'token':
      handlers.onToken?.(data)
      break
    case 'interrupt':
      handlers.onInterrupt?.(data)
      break
    case 'thread_switch':
      handlers.onThreadSwitch?.(data)
      break
    case 'done':
      handlers.onDone?.(data)
      break
    case 'error':
      handlers.onError?.(data)
      break
    default:
      break
  }
}

/**
 * 流式对话：EventSource 只能 GET，而这里是 POST，所以用 fetch + ReadableStream 自己切事件
 */
export async function streamChat({ userId, message, threadId, signal }, handlers = {}) {
  const resp = await fetch('/ai/chat/stream', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: AUTH_PREFIX + getToken(),
    },
    body: JSON.stringify({ user_id: userId, message, thread_id: threadId || null }),
    signal,
  })

  if (!resp.ok || !resp.body) {
    let detail = `请求失败（HTTP ${resp.status}）`
    try {
      const body = await resp.json()
      detail = body?.detail || body?.msg || detail
    } catch {
      /* 忽略非 JSON 响应 */
    }
    if (resp.status === 401) handleUnauthorized()
    throw new ApiError(detail, resp.status)
  }

  const reader = resp.body.getReader()
  const decoder = new TextDecoder('utf-8')
  let buffer = ''

  while (true) {
    const { value, done } = await reader.read()
    if (done) break
    buffer += decoder.decode(value, { stream: true })

    let index
    while ((index = buffer.indexOf('\n\n')) !== -1) {
      const raw = buffer.slice(0, index)
      buffer = buffer.slice(index + 2)
      const evt = parseEvent(raw)
      if (evt) dispatch(evt, handlers)
    }
  }
}

export function resume({ userId, threadId, answer }) {
  return request('/ai/resume', {
    method: 'POST',
    body: { user_id: userId, thread_id: threadId, answer },
  })
}
