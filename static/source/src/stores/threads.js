import { defineStore } from 'pinia'
import * as threadsApi from '@/api/threads'
import { loadTitles, removeTitle, saveTitle, shortId, titleFromText } from '@/utils/text'

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
    titleOf: (state) => (threadId) => {
      // 后端 thread_name 最准（新建时取首条消息前 10 字）
      const record = [...state.lists.active, ...state.lists.archived]
        .find((item) => item.thread_id === threadId)
      return record?.thread_name || state.titles[threadId] || `会话 ${shortId(threadId)}`
    },
    isPinned: (state) => (threadId) => state.pins.includes(threadId),
    recordsOf: (state) => (tab) => {
      const list = state.lists[tab] || []
      // 置顶的排前面，其余按后端返回顺序（create_time desc，最新在前）
      return [...list].sort((a, b) => {
        const pa = state.pins.includes(a.thread_id) ? 0 : 1
        const pb = state.pins.includes(b.thread_id) ? 0 : 1
        return pa - pb
      })
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
    setTitle(threadId, text) {
      if (!threadId || !text) return
      const title = titleFromText(text)
      this.titles[threadId] = title
      saveTitle(threadId, title)
    },
    rename(threadId, title) {
      const clean = String(title || '').trim()
      if (!clean) return
      this.titles[threadId] = clean
      saveTitle(threadId, clean)
    },
    togglePin(threadId) {
      const index = this.pins.indexOf(threadId)
      if (index >= 0) this.pins.splice(index, 1)
      else this.pins.push(threadId)
      localStorage.setItem(PIN_KEY, JSON.stringify(this.pins))
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
