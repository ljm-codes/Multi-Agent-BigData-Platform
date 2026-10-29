import { request } from './http'

/** 后台任务列表；后端按 token 过滤，不需要传 user_id，返回裸数组 */
export function listBackground(threadId) {
  const q = new URLSearchParams()
  if (threadId) q.set('thread_id', threadId)
  return request(`/ai/background?${q.toString()}`, { raw: true })
}
