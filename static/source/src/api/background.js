import { request } from './http'

/**
 * 后台任务走 WebSocket：
 *   1. POST /ai/background/verify 换一个一次性 ticket（20 秒内有效）
 *   2. ws://host/ai/ws/background?ticket=xxx
 * 每次重连都要重新换 ticket。
 * source 告诉后端是哪一端在用：ops 端在未开启运维功能时会被拒绝，
 * 否则会在 Redis 里留下一个永远没人消费的 ticket。
 */
export function createTicket(source = 'main') {
  return request('/ai/background/verify', { method: 'POST', body: { source } })
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
