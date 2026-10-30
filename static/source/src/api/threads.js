import { request } from './http'

export function listThreads({ limit = 50, offset = 0, isArchive = false } = {}) {
  const q = new URLSearchParams({
    limit: String(limit),
    offset: String(offset),
    is_archive: String(Boolean(isArchive)),
  })
  return request(`/ai/threads?${q.toString()}`)
}

export function getHistory(threadId) {
  return request(`/ai/history?thread_id=${encodeURIComponent(threadId)}`)
}

export function archiveThread(threadId) {
  return request(`/ai/thread/archive?thread_id=${encodeURIComponent(threadId)}`, { method: 'PUT' })
}

export function deleteThread(threadId) {
  return request(`/ai/thread/delete?thread_id=${encodeURIComponent(threadId)}`, { method: 'DELETE' })
}

/** 批量归档：后端收重复的 thread_ids 参数 */
export function archiveThreads(threadIds) {
  const q = new URLSearchParams()
  threadIds.forEach((id) => q.append('thread_ids', id))
  return request(`/ai/thread/archives?${q.toString()}`, { method: 'PUT' })
}

/** 批量删除：只对已归档的会话生效 */
export function deleteThreads(threadIds) {
  const q = new URLSearchParams()
  threadIds.forEach((id) => q.append('thread_ids', id))
  return request(`/ai/thread/deletes?${q.toString()}`, { method: 'DELETE' })
}

/** 重命名会话（后端会把名字截到 10 个字符） */
export function renameThread(threadId, threadName) {
  return request('/ai/thread/change', {
    method: 'PUT',
    body: { thread_id: threadId, thread_name: threadName, is_top: false },
  })
}

/** 置顶 / 取消置顶；后端模型里 thread_name 是必填字段，这里传当前标题 */
export function topThread(threadId, isTop, threadName = '') {
  return request('/ai/thread/top', {
    method: 'PUT',
    body: { thread_id: threadId, thread_name: threadName, is_top: isTop },
  })
}
