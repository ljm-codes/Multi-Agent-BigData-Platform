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
