import { request } from './http'

/**
 * 后台任务走 WebSocket：
 *   1. POST /ai/background/verify 换一个一次性 ticket（20 秒内有效）
 *   2. ws://host/ai/ws/background?ticket=xxx
 * 每次重连都要重新换 ticket。
 */
export function createTicket() {
  return request('/ai/background/verify', { method: 'POST' })
}

export function openBackgroundSocket(ticket) {
  const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:'
  const url = `${protocol}//${location.host}/ai/ws/background?ticket=${encodeURIComponent(ticket)}`
  return new WebSocket(url)
}

/** 兜底用：HTTP 拉一次列表（WS 连不上时也能看到数据） */
export function listBackground(threadId) {
  const q = new URLSearchParams()
  if (threadId) q.set('thread_id', threadId)
  return request(`/ai/background?${q.toString()}`, { raw: true })
}
