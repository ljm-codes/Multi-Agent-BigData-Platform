import { request } from './http'
import { createTicket } from './background'

export { createTicket }

function buildQuery(params = {}) {
  const q = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === '') continue
    q.set(key, String(value))
  }
  const text = q.toString()
  return text ? `?${text}` : ''
}

/** 错误列表：app_type 支持 ALL / SPARK / FLINK / CLUSTER */
export function listCheckerErrors(params = {}) {
  return request(`/ops/checker/app/error${buildQuery(params)}`)
}

/** 运维 Agent 运行记录（历史归档；status 可传 ALL/RUNNING/DONE/ERROR/ABANDONED） */
export function listAgentRuns(params = {}) {
  return request(`/ops/agent/info${buildQuery(params)}`)
}

/** 某个 session_id 对应的 ops-Agent 对话历史 */
export function getAgentHistory(sessionId) {
  return request(`/ops/agent/history${buildQuery({ session_id: sessionId })}`)
}

/** 感知 WebSocket：先换一次性 ticket，再连 /ops/ws/perception/error */
export function openPerceptionSocket(ticket) {
  const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:'
  return new WebSocket(
    `${protocol}//${location.host}/ops/ws/perception/error?ticket=${encodeURIComponent(ticket)}`,
  )
}
