import { defineStore } from 'pinia'
import * as threadsApi from '@/api/threads'
import { loadTitles, removeTitle, saveTitle, shortId, titleFromText } from '@/utils/text'

// 老版本把置顶存在本地；现在后端 admin_thread.is_top 才是准的，这里只当兜底
const PIN_KEY = 'thread_pins'

function loadPins() {
  try {
    return JSON.parse(localStorage.getItem(PIN_KEY) || '[]')
  } catch {
    return []
  }
}

export const useThreadStore = defineStore('threads', {
  state: () => ({
    titles: loadTitles(),
    pins: loadPins(),
    lists: { active: [], archived: [] },
    loading: false,
    error: '',
  }),
  getters: {
    recordOf: (state) => (threadId) =>
      [...state.lists.active, ...state.lists.archived]
        .find((item) => item.thread_id === threadId),
    titleOf() {
      return (threadId) =>
        this.recordOf(threadId)?.thread_name || this.titles[threadId] || `会话 ${shortId(threadId)}`
    },
    isPinned() {
      return (threadId) => {
        const record = this.recordOf(threadId)
        return Boolean(record?.is_top ?? this.pins.includes(threadId))
      }
    },
    recordsOf() {
      return (tab) =>
        [...(this.lists[tab] || [])]
          // 置顶在前；sort 是稳定的，同组保持后端返回顺序（create_time desc）
          .sort((a, b) => Number(b.is_top || 0) - Number(a.is_top || 0))
    },
  },
  actions: {
    async load(tab = 'active') {
      this.loading = true
      this.error = ''
      try {
        const data = await threadsApi.listThreads({ isArchive: tab === 'archived' })
        this.lists[tab] = data?.records || []
      } catch (e) {
        this.error = e?.message || '加载会话失败'
      } finally {
        this.loading = false
      }
    },
    async loadAll() {
      await Promise.all([this.load('active'), this.load('archived')])
    },
    /** 本地兜底标题：新建会话到列表刷新之间先用它顶着 */
    setTitle(threadId, text) {
      if (!threadId || !text) return
      const title = titleFromText(text)
      this.titles[threadId] = title
      saveTitle(threadId, title)
    },
    async rename(threadId, title) {
      const clean = String(title || '').trim().slice(0, 10)
      if (!clean) return
      await threadsApi.renameThread(threadId, clean)
      this.titles[threadId] = clean
      saveTitle(threadId, clean)
      await this.loadAll()
    },
    async togglePin(threadId) {
      const next = !this.isPinned(threadId)
      await threadsApi.topThread(threadId, next, this.titleOf(threadId))
      await this.loadAll()
    },
    async archive(threadId) {
      await threadsApi.archiveThread(threadId)
      await this.loadAll()
    },
    async remove(threadId) {
      await threadsApi.deleteThread(threadId)
      removeTitle(threadId)
      delete this.titles[threadId]
      await this.loadAll()
    },
  },
})
