import { defineStore } from 'pinia'

import { streamChat, streamResume } from '@/api/chat'
import { getVerification, listToolTraces } from '@/api/background'
import { resolveProjectDir } from '@/api/openApp'
import { getHistory } from '@/api/threads'
import { useThreadStore } from '@/stores/threads'
import { useUserStore } from '@/stores/user'
import { normalizeAgent } from '@/utils/text'

const ACTIVE_KEY = 'chat_active_turns'
const LAST_KEY = 'last_thread_id'
const DRAFT_PREFIX = 'draft:'
const INQUIRY_KEY_PREFIX = 'chat_inquiry_draft:'
const INQUIRY_TTL_MS = 2 * 60 * 60 * 1000
const POLL_MS = 3000
const ACTIVE_MAX_AGE_MS = 24 * 60 * 60 * 1000
const FINAL_QUIET_MS = 20 * 1000
const STALE_QUIET_MS = 15 * 60 * 1000

let tickTimer = null
let pollTimer = null
let pollBusy = false
let saveTimer = null

function uid() {
  return globalThis.crypto?.randomUUID?.() || `m${Date.now()}${Math.random().toString(16).slice(2)}`
}

function asTime(value) {
  if (!value) return 0
  const ts = Date.parse(value)
  return Number.isFinite(ts) ? ts : 0
}

/** 工具轨迹合并：按 trace_id 去重，incoming 覆盖同 id 的状态，按开始时间倒序 */
function mergeToolTraces(existing, incoming) {
  const merged = new Map()
  for (const trace of existing || []) {
    if (trace?.trace_id) merged.set(trace.trace_id, trace)
  }
  for (const trace of incoming || []) {
    if (trace?.trace_id) merged.set(trace.trace_id, trace)
  }
  return [...merged.values()].sort((a, b) => {
    const left = asTime(b.started_at || b.finished_at)
    const right = asTime(a.started_at || a.finished_at)
    return left - right
  })
}

function loadActiveTurns() {
  try {
    const raw = JSON.parse(localStorage.getItem(ACTIVE_KEY) || '{}')
    if (!raw || typeof raw !== 'object') return {}
    const now = Date.now()
    const out = {}
    for (const [threadId, value] of Object.entries(raw)) {
      if (!threadId || !value || typeof value !== 'object') continue
      const startedAt = Number(value.startedAt) || now
      if (now - startedAt > ACTIVE_MAX_AGE_MS) continue
      out[threadId] = {
        startedAt,
        updatedAt: Number(value.updatedAt) || startedAt,
        projectName: String(value.projectName || ''),
        baselineLastAssistant: String(value.baselineLastAssistant || ''),
        baselineCount: Number(value.baselineCount) || 0,
        mode: String(value.mode || ''),
        resumeSignature: String(value.resumeSignature || ''),
      }
    }
    return out
  } catch {
    return {}
  }
}

function extractInterrupt(payload) {
  const list = Array.isArray(payload) ? payload : payload ? [payload] : []
  const first = list[0]
  if (!first) return null
  const value = first.value ?? first
  if (value == null) return null
  if (typeof value === 'string') return { kind: 'submit', text: value }
  // info_inquiry 的结构化追问：{title, description, info_inquiry:[{question, suggestion, suggestion_index}]}
  if (typeof value === 'object' && Array.isArray(value.info_inquiry)) {
    return {
      kind: 'inquiry',
      title: String(value.title || ''),
      description: String(value.description || ''),
      questions: value.info_inquiry.map((item, index) => ({
        id: `q${index}`,
        question: String(item?.question || ''),
        suggestion: Array.isArray(item?.suggestion) ? item.suggestion.map((s) => String(s)) : [],
        suggestionIndex: Number.isInteger(item?.suggestion_index) ? item.suggestion_index : null,
      })),
    }
  }
  return { kind: 'manual', text: value.msg || value.message || '' }
}

function inquiryKey(threadId) {
  return `${INQUIRY_KEY_PREFIX}${threadId}`
}

function inquirySignature(interrupt) {
  return JSON.stringify(
    (interrupt?.questions || []).map((q) => [q.question, q.suggestion, q.suggestionIndex])
  )
}

/** 任意中断的签名；用于区分“提交前那个旧中断”和 resume 产生的新中断 */
function interruptSignature(interrupt) {
  if (!interrupt) return ''
  if (interrupt.kind === 'inquiry') return `inquiry:${inquirySignature(interrupt)}`
  return `${interrupt.kind}:${interrupt.text || ''}`
}

/** 推荐建议文本；没有合法 suggestion_index 返回空串 */
function recommendedText(question) {
  if (!Number.isInteger(question?.suggestionIndex)) return ''
  const value = question?.suggestion?.[question.suggestionIndex]
  return typeof value === 'string' ? value : ''
}

/** 让面板高亮与当前回答一致：回答恰好等于某条建议时才高亮 */
function syncInquirySuggestion(draft, question) {
  if (!draft) return
  if (!question) {
    draft.suggestion = -1
    return
  }
  const reply = String(draft.replies?.[question.id] ?? '').trim()
  if (!reply) {
    draft.suggestion = -1
    return
  }
  const index = (question.suggestion || []).findIndex((item) => item.trim() === reply)
  draft.suggestion = index
}

/** 新的追问：所有有推荐建议的问题自动填入推荐项；没有推荐的留空，必须人工填 */
function makeInquiryDraft(interrupt) {
  const questions = interrupt?.questions || []
  const draft = { active: 0, suggestion: -1, replies: {}, touched: {} }
  for (const question of questions) {
    draft.replies[question.id] = recommendedText(question)
    draft.touched[question.id] = false
  }
  syncInquirySuggestion(draft, questions[0])
  return draft
}

/** 恢复 sessionStorage 草稿：保留人工清空/修改的 touched 状态，不强行回填推荐 */
function restoreInquiryDraft(interrupt, data) {
  const questions = interrupt?.questions || []
  const draft = { active: 0, suggestion: -1, replies: {}, touched: {} }
  for (const question of questions) {
    let reply = String(data?.replies?.[question.id] ?? '')
    const touched = Boolean(data?.touched?.[question.id])
    if (!reply && !touched) {
      const recommended = recommendedText(question)
      if (recommended) reply = recommended
    }
    draft.replies[question.id] = reply
    draft.touched[question.id] = touched
  }
  const count = questions.length
  draft.active = count ? Math.min(Math.max(Number(data?.active) || 0, 0), count - 1) : 0
  syncInquirySuggestion(draft, questions[draft.active])
  return draft
}

function makeSession(key) {
  return {
    key,
    threadId: key.startsWith(DRAFT_PREFIX) ? '' : key,
    messages: [],
    sending: false,
    seconds: 0,
    startedAt: 0,
    resuming: false,
    interrupt: null,
    paused: false,
    error: '',
    stale: false,
    projectDir: '',
    projectName: '',
    verification: null,
    toolTraces: [],
    lastTool: null,
    lastTraceAt: 0,
    lastSignature: '',
    historyLoaded: false,
    sawDone: false,
    inquiryDraft: null,
    inquirySignature: '',
    revision: 0,
    tracked: false,
    bubbleMap: {},
    pendingBubble: null,
    // 先到的 Agent 用量，等对应气泡创建后再挂上
    pendingUsage: {},
    // 本轮第一个气泡在 messages 里的下标；done 时只给本轮气泡挂用量
    turnStartIndex: 0,
    // 本轮开始时间；工具轨迹只显示这之后的，避免上一轮的工具串到这一轮
    turnStartedAt: 0,
    // 整个 thread 的累计用量（来自 history 或 done 的 usage_total）
    usageTotal: null,
  }
}

function lastAssistantOf(session) {
  if (!session) return null
  for (let i = session.messages.length - 1; i >= 0; i -= 1) {
    const message = session.messages[i]
    if (message.role === 'assistant' && !message.error) return message
  }
  return null
}

/** 把某一轮的按 Agent 用量挂到该 Agent 本轮最后一条气泡上 */
function attachTurnUsage(messages, usage) {
  if (!usage?.by_agent) return
  const lastByAgent = new Map()
  for (const message of messages) {
    if (message.role !== 'assistant' || message.error || !message.agent) continue
    lastByAgent.set(normalizeAgent(message.agent), message)
  }
  for (const [agent, value] of Object.entries(usage.by_agent)) {
    const target = lastByAgent.get(normalizeAgent(agent))
    if (!target) continue
    target.usage = {
      ...value,
      provider: usage.provider || '',
      model: usage.model || '',
    }
  }
}

/** 历史页的用量：后端按用户轮返回 turns，从最新一轮往回对齐 */
function attachHistoryUsage(rendered, usageTurns) {
  if (!Array.isArray(usageTurns) || !usageTurns.length) return
  const groups = []
  let current = null
  for (const message of rendered) {
    if (message.role === 'user') {
      current = []
      groups.push(current)
      continue
    }
    if (current) current.push(message)
  }
  const offset = groups.length - usageTurns.length
  usageTurns.forEach((usage, index) => {
    const group = groups[index + offset]
    if (group) attachTurnUsage(group, usage)
  })
}

/** 历史渲染：父图 messages + output_msg 兜底；Tool 消息挂到上一条气泡 */
function renderHistory(data) {
  const interrupted = Boolean(data?.interrupted ?? data?.manual_intervention)
  const rendered = []
  for (const m of data?.messages || []) {
    const type = String(m.type || '')
    const content = String(m.content ?? '')
    if (type.includes('Tool')) {
      const last = rendered[rendered.length - 1]
      if (last) last.tools.push(content)
      continue
    }
    if (!content.trim()) continue
    rendered.push({
      id: uid(),
      role: type.includes('Human') ? 'user' : 'assistant',
      content,
      agent: type.includes('Human') ? '' : normalizeAgent(m.name),
      tools: [],
    })
  }
  const finalAnswer = String(data?.output_msg || '').trim()
  const lastAssistant = [...rendered].reverse().find((m) => m.role === 'assistant')
  if (finalAnswer && !interrupted && !(lastAssistant && lastAssistant.content.trim() === finalAnswer)) {
    rendered.push({
      id: uid(),
      role: 'assistant',
      content: finalAnswer,
      agent: normalizeAgent(data?.metadata?.from_),
      tools: [],
    })
  }
  attachHistoryUsage(rendered, data?.usage?.turns)
  return { rendered, interrupted, finalAnswer }
}

export const useChatStore = defineStore('chat', {
  state: () => ({
    sessions: {},
    viewKey: '',
    draftSeq: 0,
    activeTurns: loadActiveTurns(),
    lastThreadId: localStorage.getItem(LAST_KEY) || '',
    ownerId: '',
    now: Date.now(),
  }),

  getters: {
    viewSession: (state) => state.sessions[state.viewKey] || null,
    viewThreadId: (state) => state.sessions[state.viewKey]?.threadId || '',
    runningIds: (state) => {
      const ids = new Set(Object.keys(state.activeTurns))
      for (const [key, session] of Object.entries(state.sessions)) {
        if (session.sending && session.threadId) ids.add(session.threadId)
        else if (session.sending && key.startsWith(DRAFT_PREFIX)) ids.add(key)
      }
      return [...ids]
    },
    isRunning: (state) => (threadId) => Boolean(
      threadId && (state.activeTurns[threadId] || state.sessions[threadId]?.sending)
    ),
    isTrackedRunning: (state) => (threadId) => Boolean(
      threadId && state.activeTurns[threadId] && !state.sessions[threadId]?.sending
    ),
    isResuming: (state) => (threadId) => Boolean(
      threadId && state.activeTurns[threadId]?.mode === 'resume'
    ),
    trackedSeconds: (state) => (threadId) => {
      const active = state.activeTurns[threadId]
      if (!active) return 0
      return Math.max(0, Math.floor((state.now - active.startedAt) / 1000))
    },
    projectOf: (state) => (threadId) => (
      state.activeTurns[threadId]?.projectName || state.sessions[threadId]?.projectName || ''
    ),
  },

  actions: {
    ensureSession(key) {
      if (!key) key = `${DRAFT_PREFIX}0`
      if (!this.sessions[key]) this.sessions[key] = makeSession(key)
      return this.sessions[key]
    },

    newChat() {
      this.draftSeq += 1
      const key = `${DRAFT_PREFIX}${this.draftSeq}`
      this.ensureSession(key)
      this.viewKey = key
      // 清掉没有在跑的空草稿，避免 sessions 一直涨
      for (const oldKey of Object.keys(this.sessions)) {
        if (!oldKey.startsWith(DRAFT_PREFIX) || oldKey === key) continue
        const old = this.sessions[oldKey]
        if (!old.sending) delete this.sessions[oldKey]
      }
    },

    rekey(session, threadId) {
      if (!session || !threadId) return
      const oldKey = session.key
      if (this.sessions[oldKey] === session) delete this.sessions[oldKey]
      session.threadId = threadId
      session.key = threadId
      this.sessions[threadId] = session
      if (this.viewKey === oldKey) this.viewKey = threadId
      this.lastThreadId = threadId
      localStorage.setItem(LAST_KEY, threadId)
    },

    async selectThread(threadId) {
      if (!threadId) {
        this.newChat()
        return
      }
      const session = this.ensureSession(threadId)
      this.viewKey = threadId
      this.lastThreadId = threadId
      localStorage.setItem(LAST_KEY, threadId)
      if (!session.historyLoaded && !session.sending) {
        await this.loadHistory(threadId)
      } else {
        this.loadExtras(threadId)
      }
      if (this.activeTurns[threadId] && !session.sending) {
        session.tracked = true
        if (!session.turnStartedAt) session.turnStartedAt = this.activeTurns[threadId].startedAt
        this.startPolling()
        this.startTick()
      }
    },

    async restore() {
      const threadStore = useThreadStore()
      const user = useUserStore()
      const owner = user.adminId == null ? '' : String(user.adminId)
      // 同一浏览器换账号登录：不要把上一个账号的会话/运行态带进来
      if (this.ownerId && owner && this.ownerId !== owner) this.reset()
      this.ownerId = owner
      const activeIds = Object.keys(this.activeTurns)
        .sort((a, b) => (this.activeTurns[b].startedAt || 0) - (this.activeTurns[a].startedAt || 0))
      const exists = (id) => Boolean(id && threadStore.recordOf(id))
      const candidate = activeIds.find(exists) || (exists(this.lastThreadId) ? this.lastThreadId : '')
      if (candidate) await this.selectThread(candidate)
      else this.newChat()

      for (const threadId of activeIds) {
        const session = this.ensureSession(threadId)
        session.tracked = true
        if (!session.historyLoaded) this.loadHistory(threadId)
        else this.loadExtras(threadId)
      }
      if (activeIds.length) {
        this.startPolling()
        this.startTick()
      }
    },

    async loadHistory(threadId, { force = false, allowInterrupt = false } = {}) {
      const session = this.ensureSession(threadId)
      session.error = ''
      try {
        const data = await getHistory(threadId)
        if (this.sessions[threadId] !== session) return
        // 加载期间用户又发了消息：别用旧历史覆盖乐观气泡，交给 SSE
        if (session.sending && !force) return
        this.applyHistory(session, data, { allowInterrupt })
        await this.loadExtras(threadId)
        const threadStore = useThreadStore()
        if (!threadStore.titles[threadId]) {
          const firstUser = session.messages.find((m) => m.role === 'user')
          if (firstUser) threadStore.setTitle(threadId, firstUser.content)
        }
      } catch (e) {
        if (this.sessions[threadId] === session) session.error = e?.message || '加载会话失败'
      }
    },

    historySignature(data) {
      return JSON.stringify([
        (data?.messages || []).length,
        String(data?.output_msg || ''),
        Boolean(data?.interrupted ?? data?.manual_intervention),
      ])
    },

    applyHistory(session, data, { allowInterrupt = false } = {}) {
      const { rendered, interrupted } = renderHistory(data)
      session.messages = rendered
      session.bubbleMap = {}
      session.pendingBubble = null
      // 兼容修复前创建的 active 记录：resume 期间旧记录没有 mode，
      // 刷新后 history 里还是旧追问，先把 mode 补成 resume，避免面板重新挡住界面。
      if (interrupted && session.threadId) {
        const activeTurn = this.activeTurns[session.threadId]
        if (activeTurn && !activeTurn.mode && !session.sending) {
          activeTurn.mode = 'resume'
          this.saveActiveNow()
        }
      }
      // 提交追问后的恢复阶段，history 里可能还是提交前的中断，先不把面板拉回来；
      // 强制加载（resume 返回后）传 allowInterrupt: true 再应用真正的新中断。
      const historyInterrupt = interrupted && data?.interrupt
        ? extractInterrupt(data.interrupt)
        : null
      const historySignature = interruptSignature(historyInterrupt)
      let suppressInterrupt = !allowInterrupt && (
        session.resuming || this.isResuming(session.threadId)
      )
      if (suppressInterrupt && historySignature) {
        const activeTurn = session.threadId ? this.activeTurns[session.threadId] : null
        const resumeSignature = activeTurn?.resumeSignature || ''
        // 刷新后后台 resume 已经推进到新的中断：结束 resume 追踪并展示新面板
        if (resumeSignature && historySignature !== resumeSignature) {
          suppressInterrupt = false
          session.resuming = false
          this.endActiveTurn(session.threadId, 'done')
        }
      }
      if (suppressInterrupt) {
        session.interrupt = null
        session.paused = false
      } else {
        this.setInterrupt(session, historyInterrupt)
        if (interrupted) session.paused = true
      }
      session.projectDir = data?.project_dir || session.projectDir || ''
      if (data?.metadata?.verification) session.verification = data.metadata.verification
      const usageTurns = Array.isArray(data?.usage?.turns) ? data.usage.turns : []
      const lastTurnUsage = usageTurns[usageTurns.length - 1] || null
      session.usageTotal = Number(data?.usage?.total?.total)
        ? {
            ...data.usage.total,
            provider: lastTurnUsage?.provider || '',
            model: lastTurnUsage?.model || '',
          }
        : null
      session.historyLoaded = true
      session.error = ''
      session.stale = false
      session.lastSignature = this.historySignature(data)
      session.revision += 1
      if (session.threadId && this.activeTurns[session.threadId]) {
        this.touchActiveTurn(session.threadId)
        if (interrupted && !suppressInterrupt) this.endActiveTurn(session.threadId, 'paused')
      }
    },

    setInterrupt(session, interrupt) {
      session.interrupt = interrupt || null
      session.paused = Boolean(interrupt)
      if (interrupt?.kind === 'inquiry') {
        const signature = inquirySignature(interrupt)
        if (session.inquirySignature !== signature || !session.inquiryDraft) {
          session.inquirySignature = signature
          session.inquiryDraft = this.loadInquiryDraft(
            session.threadId || session.key, signature, interrupt
          ) || makeInquiryDraft(interrupt)
          this.saveInquiryDraft(session)
        } else {
          this.hydrateInquiryDraft(session, interrupt)
        }
      } else {
        this.clearInquiryDraft(session)
        session.inquirySignature = ''
        session.inquiryDraft = null
      }
    },

    hydrateInquiryDraft(session, interrupt) {
      const draft = session.inquiryDraft
      if (!draft) {
        session.inquiryDraft = makeInquiryDraft(interrupt)
        return
      }
      for (const question of interrupt.questions || []) {
        if (!(question.id in draft.replies)) draft.replies[question.id] = recommendedText(question)
        if (!(question.id in draft.touched)) draft.touched[question.id] = false
      }
      const activeQuestion = interrupt.questions?.[draft.active] || interrupt.questions?.[0]
      syncInquirySuggestion(draft, activeQuestion)
    },

    loadInquiryDraft(threadId, signature, interrupt) {
      if (!threadId) return null
      try {
        const raw = sessionStorage.getItem(inquiryKey(threadId))
        if (!raw) return null
        const data = JSON.parse(raw)
        if (!data || data.signature !== signature) return null
        const savedAt = Number(data.savedAt) || 0
        if (!savedAt || Date.now() - savedAt > INQUIRY_TTL_MS) {
          sessionStorage.removeItem(inquiryKey(threadId))
          return null
        }
        return restoreInquiryDraft(interrupt, data)
      } catch {
        return null
      }
    },

    saveInquiryDraft(session) {
      const draft = session?.inquiryDraft
      const signature = session?.inquirySignature
      const threadId = session?.threadId || session?.key
      if (!draft || !signature || !threadId) return
      try {
        sessionStorage.setItem(inquiryKey(threadId), JSON.stringify({
          signature,
          savedAt: Date.now(),
          active: draft.active,
          suggestion: draft.suggestion,
          replies: draft.replies,
          touched: draft.touched,
        }))
      } catch {
        /* 隐私模式/配额满不影响回答 */
      }
    },

    clearInquiryDraft(session) {
      const threadId = session?.threadId || session?.key
      if (!threadId) return
      try {
        sessionStorage.removeItem(inquiryKey(threadId))
      } catch {
        /* ignore */
      }
    },

    clearAllInquiryDrafts() {
      try {
        const keys = []
        for (let index = 0; index < sessionStorage.length; index += 1) {
          const key = sessionStorage.key(index)
          if (key?.startsWith(INQUIRY_KEY_PREFIX)) keys.push(key)
        }
        for (const key of keys) sessionStorage.removeItem(key)
      } catch {
        /* ignore */
      }
    },

    async loadExtras(threadId) {
      const session = this.ensureSession(threadId)
      try {
        const rows = await listToolTraces(threadId, 500)
        if (this.sessions[threadId] !== session) return
        session.toolTraces = mergeToolTraces(session.toolTraces, Array.isArray(rows) ? rows : [])
        this.applyProjectFromTraces(session)
        session.revision += 1
      } catch {
        /* 工具轨迹不是强依赖 */
      }
      try {
        const latest = await getVerification(threadId)
        if (this.sessions[threadId] === session && latest) {
          session.verification = latest
          session.revision += 1
        }
      } catch {
        /* 验证进度不是强依赖 */
      }
    },

    setProjectDir(threadId, dir) {
      if (!threadId || !dir) return
      const session = this.ensureSession(threadId)
      session.projectDir = dir
      session.revision += 1
    },

    applyProjectFromTraces(session) {
      if (!session?.toolTraces?.length) return
      let latest = 0
      let initProject = ''
      for (const trace of session.toolTraces) {
        const ts = asTime(trace.finished_at || trace.started_at)
        if (ts > latest) latest = ts
        if (!initProject && trace.tool === 'initialize_project' && trace.args?.project_name) {
          initProject = String(trace.args.project_name)
        }
      }
      if (latest) session.lastTraceAt = latest
      if (initProject) session.projectName = initProject
      const newest = session.toolTraces[0]
      if (newest) {
        session.lastTool = {
          name: newest.tool || '',
          at: newest.finished_at || newest.started_at || '',
          status: newest.status || '',
        }
      }
      const threadId = session.threadId
      if (threadId && this.activeTurns[threadId] && session.projectName) {
        this.activeTurns[threadId].projectName = session.projectName
        this.scheduleSaveActive()
      }
    },

    setTraces(threadId, records) {
      const session = this.ensureSession(threadId)
      // WS 的 tool_trace_snapshot 只带最近 200 条；直接替换会把更早的
      // data_develop 轨迹（比如 initialize_project）挤掉，切换会话后就“消失”。
      // 这里按 trace_id 合并：快照覆盖同 id 的最新状态，已有旧轨迹保留。
      session.toolTraces = mergeToolTraces(session.toolTraces, records)
      this.applyProjectFromTraces(session)
      session.revision += 1
    },

    upsertTrace(threadId, record) {
      if (!record?.trace_id) return
      const session = this.ensureSession(threadId)
      const index = session.toolTraces.findIndex((item) => item.trace_id === record.trace_id)
      if (index >= 0) session.toolTraces[index] = record
      else session.toolTraces = [record, ...session.toolTraces]
      if (record.tool === 'initialize_project' && record.args?.project_name) {
        session.projectName = String(record.args.project_name)
      }
      session.lastTool = {
        name: record.tool || '',
        at: record.finished_at || record.started_at || '',
        status: record.status || '',
      }
      session.lastTraceAt = Math.max(session.lastTraceAt || 0, asTime(record.finished_at || record.started_at))
      // 工具轨迹可能早于 Agent 的第一条文本到达：先给该 Agent 建一个占位气泡，
      // 轨迹立刻挂在它下面，而不是等文本到了才“落后显示”。
      const agent = normalizeAgent(record.agent)
      if (agent) {
        const ts = Date.parse(record.started_at || record.finished_at || '')
        const turnStart = Number(session.turnStartedAt) || Number(session.startedAt) || 0
        const current = !turnStart || !Number.isFinite(ts) || ts >= turnStart - 5000
        if (current && !session.messages.some((m) => (
          m.role === 'assistant' && normalizeAgent(m.agent) === agent
        ))) {
          session.messages.push({
            id: uid(), role: 'assistant', content: '', streaming: true, agent, tools: [],
          })
        }
      }
      session.revision += 1
      if (this.activeTurns[threadId]) {
        this.activeTurns[threadId].updatedAt = Date.now()
        if (session.projectName) this.activeTurns[threadId].projectName = session.projectName
        this.scheduleSaveActive()
      }
    },

    setVerification(threadId, record) {
      if (!record) return
      const session = this.ensureSession(threadId)
      session.verification = record
      session.revision += 1
    },

    handleWsMessage(threadId, message) {
      if (!threadId || !message) return
      if (message.type === 'tool_trace_snapshot') this.setTraces(threadId, message.records)
      else if (message.type === 'tool_trace') this.upsertTrace(threadId, message.record)
      else if (message.type === 'verification_snapshot') this.setVerification(threadId, message.record)
      else if (message.type === 'verification') this.setVerification(threadId, message.record)
    },

    ensureBubble(session, agent, key) {
      let bubble = session.bubbleMap[key]
      if (bubble && session.messages.includes(bubble)) return bubble
      const normalized = normalizeAgent(agent)
      if (!bubble && normalized) {
        // 工具轨迹先到时创建的占位气泡：同 Agent 的文本直接复用，避免再开一个气泡
        bubble = session.messages.find((m) => (
          m.role === 'assistant'
          && normalizeAgent(m.agent) === normalized
          && !m.content
          && m.streaming
        ))
      }
      if (!bubble && session.pendingBubble && !session.pendingBubble.content && session.messages.includes(session.pendingBubble)) {
        bubble = session.pendingBubble
        session.pendingBubble = null
      }
      if (!bubble) {
        session.messages.push({ id: uid(), role: 'assistant', content: '', streaming: true, agent: '', tools: [] })
        bubble = session.messages[session.messages.length - 1]
      }
      bubble.agent = normalizeAgent(agent)
      const pending = session.pendingUsage?.[bubble.agent]
      if (pending) {
        bubble.usage = pending
        delete session.pendingUsage[bubble.agent]
        session.revision += 1
      }
      session.bubbleMap[key] = bubble
      return bubble
    },

    stopStreaming(session, removeEmpty = true) {
      for (const message of session.messages) {
        if (message.role === 'assistant') {
          message.streaming = false
          message.status = ''
        }
      }
      if (removeEmpty) {
        session.messages = session.messages.filter(
          (m) => !(m.role === 'assistant' && !m.content && !m.error)
        )
      }
      session.bubbleMap = {}
      session.pendingBubble = null
    },

    stopTypewriter(session) {
      if (session.typewriterTimer) {
        clearInterval(session.typewriterTimer)
        session.typewriterTimer = null
      }
    },

    /** 非流式模型一次给整段文字时，前端按 6 字/24ms 补打字机；小 delta 直接追加 */
    appendToken(session, bubble, content) {
      const text = String(content || '')
      if (!text) return
      if (text.length <= 80) {
        bubble.content += text
        session.revision += 1
        return
      }
      bubble.typeQueue = (bubble.typeQueue || '') + text
      if (bubble.typeTimer) return
      bubble.typeTimer = setInterval(() => {
        if (!session.messages.includes(bubble)) {
          clearInterval(bubble.typeTimer)
          bubble.typeTimer = null
          bubble.typeQueue = ''
          return
        }
        const queue = bubble.typeQueue || ''
        if (!queue) {
          clearInterval(bubble.typeTimer)
          bubble.typeTimer = null
          return
        }
        bubble.content += queue.slice(0, 6)
        bubble.typeQueue = queue.slice(6)
        session.revision += 1
      }, 24)
    },

    /** 新发送前把上一轮未打完的队列补完，避免留下半截 */
    flushTypeTimers(session) {
      for (const message of session.messages) {
        if (!message.typeTimer) continue
        if (message.typeQueue) message.content += message.typeQueue
        message.typeQueue = ''
        clearInterval(message.typeTimer)
        message.typeTimer = null
      }
    },

    /** resume 是非流式接口，历史一次返回；这里只给最后一条汇总答复补打字机效果 */
    animateFinalText(session, text) {
      const full = String(text || '')
      if (!full) return
      let bubble = [...session.messages].reverse().find(
        (m) => m.role === 'assistant' && m.content.trim() === full.trim()
      )
      if (!bubble) {
        bubble = { id: uid(), role: 'assistant', content: '', streaming: true, agent: '', tools: [] }
        session.messages.push(bubble)
      }
      if (bubble.error) return
      this.stopTypewriter(session)
      bubble.content = ''
      bubble.streaming = true
      bubble.status = ''
      let index = 0
      session.typewriterTimer = setInterval(() => {
        if (!session.messages.includes(bubble)) {
          this.stopTypewriter(session)
          return
        }
        bubble.content += full.slice(index, index + 6)
        index += 6
        session.revision += 1
        if (index >= full.length) {
          this.stopTypewriter(session)
          bubble.streaming = false
          session.revision += 1
        }
      }, 30)
    },

    /** 同一段最终答复可能既以 node message 又以 node=final 回放；相邻同文只留后一条 */
    dedupeAssistant(session) {
      const out = []
      for (const message of session.messages) {
        const prev = out[out.length - 1]
        if (
          message.role === 'assistant'
          && prev?.role === 'assistant'
          && message.content.trim()
          && prev.content.trim() === message.content.trim()
        ) {
          out[out.length - 1] = message
          continue
        }
        out.push(message)
      }
      session.messages = out
    },

    startActiveTurn(session, baseline = {}) {
      const threadId = session.threadId
      if (!threadId) return
      const previous = this.activeTurns[threadId]
      this.activeTurns[threadId] = {
        startedAt: session.startedAt || previous?.startedAt || Date.now(),
        updatedAt: Date.now(),
        projectName: session.projectName || previous?.projectName || '',
        baselineLastAssistant: baseline.baselineLastAssistant ?? previous?.baselineLastAssistant ?? '',
        baselineCount: baseline.baselineCount ?? previous?.baselineCount ?? 0,
        mode: baseline.mode || previous?.mode || 'chat',
        resumeSignature: baseline.resumeSignature ?? previous?.resumeSignature ?? '',
      }
      this.saveActiveNow()
      this.startTick()
      this.startPolling()
    },

    touchActiveTurn(threadId) {
      const active = this.activeTurns[threadId]
      if (!active) return
      active.updatedAt = Date.now()
      this.scheduleSaveActive()
    },

    endActiveTurn(threadId, reason = 'done') {
      if (!threadId || !this.activeTurns[threadId]) return
      delete this.activeTurns[threadId]
      this.saveActiveNow()
      const session = this.sessions[threadId]
      if (session) {
        session.tracked = false
        if (reason === 'stale') session.stale = true
        if (reason === 'paused') session.paused = true
      }
      if (!Object.keys(this.activeTurns).length) {
        this.stopPolling()
        this.maybeStopTick()
      }
    },

    stopTracking(threadId) {
      this.endActiveTurn(threadId, 'manual')
      const session = this.sessions[threadId]
      if (session) session.stale = false
    },

    clearStale(threadId) {
      const session = this.sessions[threadId]
      if (session) session.stale = false
    },

    reset() {
      this.stopPolling()
      this.stopTick()
      if (saveTimer) {
        clearTimeout(saveTimer)
        saveTimer = null
      }
      this.sessions = {}
      this.viewKey = ''
      this.draftSeq = 0
      this.activeTurns = {}
      this.lastThreadId = ''
      this.ownerId = ''
      this.clearAllInquiryDrafts()
      try {
        localStorage.removeItem(ACTIVE_KEY)
        localStorage.removeItem(LAST_KEY)
      } catch {
        /* 忽略隐私模式下的存储异常 */
      }
    },

    dropThread(threadId) {
      if (!threadId) return
      this.endActiveTurn(threadId, 'manual')
      this.clearInquiryDraft(this.sessions[threadId] || { threadId })
      delete this.sessions[threadId]
      if (this.viewKey === threadId) this.newChat()
    },

    scheduleSaveActive() {
      if (saveTimer) return
      saveTimer = setTimeout(() => {
        saveTimer = null
        this.saveActiveNow()
      }, 800)
    },

    saveActiveNow() {
      if (saveTimer) {
        clearTimeout(saveTimer)
        saveTimer = null
      }
      try {
        localStorage.setItem(ACTIVE_KEY, JSON.stringify(this.activeTurns))
      } catch {
        /* 隐私模式/配额满不影响主流程 */
      }
    },

    startTick() {
      if (tickTimer) return
      tickTimer = setInterval(() => {
        this.now = Date.now()
        for (const session of Object.values(this.sessions)) {
          if (session.sending) session.seconds += 1
        }
      }, 1000)
    },

    stopTick() {
      if (tickTimer) {
        clearInterval(tickTimer)
        tickTimer = null
      }
    },

    maybeStopTick() {
      const anySending = Object.values(this.sessions).some((session) => session.sending)
      if (!anySending && !Object.keys(this.activeTurns).length) this.stopTick()
    },

    startPolling() {
      if (pollTimer) return
      if (!Object.keys(this.activeTurns).length) return
      pollTimer = setInterval(() => {
        this.pollActive()
      }, POLL_MS)
    },

    stopPolling() {
      if (pollTimer) {
        clearInterval(pollTimer)
        pollTimer = null
      }
    },

    async pollActive() {
      if (pollBusy) return
      if (typeof document !== 'undefined' && document.visibilityState === 'hidden') return
      const ids = Object.keys(this.activeTurns)
      if (!ids.length) {
        this.stopPolling()
        return
      }
      pollBusy = true
      try {
        for (const threadId of ids) {
          await this.pollOne(threadId)
        }
      } finally {
        pollBusy = false
      }
    },

    async pollOne(threadId) {
      const active = this.activeTurns[threadId]
      if (!active) return
      const session = this.ensureSession(threadId)
      session.tracked = true
      if (session.sending) {
        // 隐藏会话的 SSE 仍在跑；用工具轨迹把“当前项目/最近工具”补上
        await this.refreshActivity(threadId)
        if (session.resuming) {
          // resume 是同步 POST，没有 SSE；轮询父图历史，让节点级消息也能出现。
          // 此时 history 里可能还是提交前的中断，applyHistory 默认会抑制它。
          try {
            const data = await getHistory(threadId)
            if (!this.activeTurns[threadId] || !session.resuming) return
            if (this.historySignature(data) !== session.lastSignature) {
              this.applyHistory(session, data, { allowInterrupt: false })
              active.updatedAt = Date.now()
              this.scheduleSaveActive()
            }
          } catch {
            /* 忽略单次轮询失败 */
          }
        }
        return
      }

      try {
        const data = await getHistory(threadId)
        if (!this.activeTurns[threadId]) return
        const signature = this.historySignature(data)
        if (signature !== session.lastSignature) {
          this.applyHistory(session, data)
          active.updatedAt = Date.now()
          this.scheduleSaveActive()
        }
        await this.refreshActivity(threadId)

        const resuming = Boolean(session.resuming || this.isResuming(threadId))
        if (data?.interrupted && !resuming) {
          this.endActiveTurn(threadId, 'paused')
          return
        }

        const last = lastAssistantOf(session)
        const output = String(data?.output_msg || '').trim()
        const lastText = last?.content.trim() || ''
        const baselineLast = String(active.baselineLastAssistant || '').trim()
        const finalSeen = Boolean(output) && lastText === output && lastText !== baselineLast
        const quiet = Date.now() - Math.max(active.updatedAt || 0, session.lastTraceAt || 0)

        if (finalSeen && quiet >= FINAL_QUIET_MS) {
          this.endActiveTurn(threadId, 'done')
          return
        }
        if (quiet >= STALE_QUIET_MS) {
          this.endActiveTurn(threadId, finalSeen ? 'done' : 'stale')
        }
      } catch {
        /* 历史接口暂时失败时继续下一轮 */
      }
    },

    async refreshActivity(threadId) {
      const session = this.sessions[threadId]
      if (!session) return
      try {
        const rows = await listToolTraces(threadId, 20)
        if (!Array.isArray(rows) || !rows.length) return
        let latest = 0
        for (const row of rows) {
          latest = Math.max(latest, asTime(row.finished_at || row.started_at))
        }
        if (latest) session.lastTraceAt = latest
        const newest = rows[0]
        session.lastTool = {
          name: newest.tool || '',
          at: newest.finished_at || newest.started_at || '',
          status: newest.status || '',
        }
        const init = rows.find((row) => row.tool === 'initialize_project' && row.args?.project_name)
        if (init) session.projectName = String(init.args.project_name)
        if (session.projectName && this.activeTurns[threadId]) {
          this.activeTurns[threadId].projectName = session.projectName
          this.scheduleSaveActive()
        }
      } catch {
        /* 轨迹刷新失败不影响轮询 */
      }
    },

    async send(key, text) {
      const message = String(text || '').trim()
      if (!message) return false
      if (!key) {
        this.newChat()
        key = this.viewKey
      }
      const session = this.ensureSession(key)
      if (session.sending || session.interrupt || session.paused || this.isTrackedRunning(session.threadId)) {
        return false
      }
      this.stopTypewriter(session)
      this.flushTypeTimers(session)

      const threadIdBefore = session.threadId
      const baselineLastAssistant = lastAssistantOf(session)?.content || ''
      const baselineCount = session.messages.length
      session.error = ''
      session.stale = false
      this.setInterrupt(session, null)
      session.sending = true
      session.sawDone = false
      session.seconds = 0
      session.startedAt = Date.now()
      session.bubbleMap = {}
      session.pendingBubble = null
      session.pendingUsage = {}
      this.startTick()

      session.turnStartIndex = session.messages.length
      session.turnStartedAt = Date.now()
      session.messages.push({ id: uid(), role: 'user', content: message })
      session.messages.push({ id: uid(), role: 'assistant', content: '', streaming: true, agent: '', tools: [] })
      session.pendingBubble = session.messages[session.messages.length - 1]
      session.revision += 1

      const isNewThread = !threadIdBefore
      if (!isNewThread) {
        if (this.activeTurns[threadIdBefore]) delete this.activeTurns[threadIdBefore]
        this.startActiveTurn(session, { baselineLastAssistant, baselineCount })
      }

      const userId = (() => {
        const user = useUserStore()
        return user.adminId == null ? '' : String(user.adminId)
      })()

      try {
        await streamChat(
          { userId, message, threadId: threadIdBefore || null },
          {
            onMeta: (data) => {
              const threadId = String(data?.thread_id || '')
              if (!threadId) return
              if (session.threadId !== threadId) this.rekey(session, threadId)
              if (isNewThread) {
                this.lastThreadId = threadId
                const threadStore = useThreadStore()
                if (!threadStore.titles[threadId]) threadStore.setTitle(threadId, message)
                // 新会话立刻进左侧列表，运行中标识/项目名才对用户可见
                threadStore.load('active')
              }
              this.startActiveTurn(session, { baselineLastAssistant, baselineCount })
              this.startTick()
            },
            onStatus: (data) => {
              if (!data?.text) return
              const agent = data.agent || data.node || ''
              const bubble = this.ensureBubble(session, agent, agent || 'assistant')
              bubble.status = data.text
              session.revision += 1
              this.touchActiveTurn(session.threadId)
            },
            onVerification: (data) => {
              if (data?.record) {
                session.verification = data.record
                session.revision += 1
              }
            },
            onUsage: (data) => {
              const agent = normalizeAgent(data?.agent)
              const usage = data?.usage
              if (!agent || !usage?.total) return
              const payload = {
                ...usage,
                provider: data.provider || '',
                model: data.model || '',
              }
              if (!session.pendingUsage) session.pendingUsage = {}
              session.pendingUsage[agent] = payload
              // 只在本轮气泡里找该 Agent 的最后一条；找到就立即显示
              const start = Number.isInteger(session.turnStartIndex) ? session.turnStartIndex : 0
              let target = null
              for (const message of session.messages.slice(start)) {
                if (message.role !== 'assistant' || message.error) continue
                if (normalizeAgent(message.agent) === agent) target = message
              }
              if (target) {
                target.usage = payload
                delete session.pendingUsage[agent]
                session.revision += 1
                this.touchActiveTurn(session.threadId)
              }
            },
            onToken: (data) => {
              if (!data?.content) return
              if (!data.agent && data.node === 'handler') return
              const isFinal = data.node === 'final'
              const keyName = isFinal ? 'final' : data.agent || data.node || 'assistant'
              const bubble = this.ensureBubble(session, isFinal ? '' : data.agent || data.node || '', keyName)
              bubble.status = ''
              this.appendToken(session, bubble, data.content)
              this.touchActiveTurn(session.threadId)
            },
            onThreadSwitch: (data) => {
              const newThreadId = String(data?.thread_id || '')
              if (!newThreadId) return
              const oldThreadId = session.threadId
              if (oldThreadId) this.endActiveTurn(oldThreadId, 'continue')
              this.rekey(session, newThreadId)
              const lastUser = [...session.messages].reverse().find((m) => m.role === 'user') || null
              session.messages = lastUser
                ? [{ ...lastUser }]
                : []
              session.messages.push({ id: uid(), role: 'assistant', content: '', streaming: true, agent: '', tools: [] })
              session.pendingBubble = session.messages[session.messages.length - 1]
              session.bubbleMap = {}
              session.pendingUsage = {}
              session.turnStartIndex = 0
              session.turnStartedAt = Date.now()
              session.usageTotal = null
              session.revision += 1
              this.startActiveTurn(session, { baselineLastAssistant: '', baselineCount: session.messages.length })
              if (data.thread_title) {
                const threadStore = useThreadStore()
                threadStore.rename(newThreadId, data.thread_title).catch(() => {})
              }
            },
            onInterrupt: (data) => {
              this.setInterrupt(session, extractInterrupt(data?.interrupt))
              session.paused = true
              session.sawDone = true
              session.historyLoaded = true
              this.stopStreaming(session, true)
              this.endActiveTurn(session.threadId, 'paused')
              session.revision += 1
            },
            onDone: (data) => {
              this.applyDone(session, data)
            },
            onError: (data) => {
              this.applyStreamError(session, data?.detail || '对话失败')
            },
          }
        )
      } catch (e) {
        this.applyStreamError(session, e?.message || '对话失败')
      } finally {
        session.sending = false
        this.stopStreaming(session, true)
        this.dedupeAssistant(session)
        const threadId = session.threadId
        const failed = Boolean(session.error)
        const interrupted = Boolean(session.interrupt)
        // SSE 正常收尾时保留流式消息（CVA 子图对话只在流里，父图 history 只有汇总）；
        // 只有“流断了但没有 done”才用 history 兜底
        if (threadId && !failed && !interrupted && !session.sawDone) {
          await this.loadHistory(threadId)
        }
        this.endActiveTurn(threadId, interrupted ? 'paused' : failed ? 'error' : 'done')
        this.maybeStopTick()
        const threadStore = useThreadStore()
        await threadStore.loadAll()
        session.revision += 1
      }
      return true
    },

    applyDone(session, data) {
      this.flushTypeTimers(session)
      session.sawDone = true
      // 保留流式消息（含 CVA 子图对话）；切走再切回也不要用父图 history 覆盖掉它
      session.historyLoaded = true
      if (data?.metadata?.verification) session.verification = data.metadata.verification
      if (data?.project_dir) session.projectDir = data.project_dir
      const finalText = String(data?.output_msg || '')
      if (finalText.trim()) {
        const finalBubble = session.bubbleMap['final']
        if (finalBubble && session.messages.includes(finalBubble)) {
          // 以 done 的完整正文为准，避免分帧/断流留下半截
          finalBubble.content = finalText
          finalBubble.streaming = false
          finalBubble.status = ''
          finalBubble.agent = normalizeAgent(data?.metadata?.from_)
        } else {
          // 最后一条 assistant 已经是同一段文本时复用它，避免新建气泡导致打字机被整段覆盖
          const last = [...session.messages].reverse().find((m) => m.role === 'assistant')
          if (last && last.content.trim() === finalText.trim()) {
            last.streaming = false
            last.status = ''
            last.agent = normalizeAgent(data?.metadata?.from_)
          } else {
            session.messages.push({
              id: uid(),
              role: 'assistant',
              content: finalText,
              streaming: false,
              agent: normalizeAgent(data?.metadata?.from_),
              tools: [],
            })
          }
        }
      }
      // 只在 done 时展示：把本轮各 Agent 的用量挂到对应气泡，底部更新线程累计
      const usage = data?.usage
      if (usage?.by_agent) {
        const start = Number.isInteger(session.turnStartIndex) ? session.turnStartIndex : 0
        attachTurnUsage(session.messages.slice(start), usage)
      }
      if (usage?.total || data?.usage_total) {
        const total = data?.usage_total || usage?.total
        session.usageTotal = {
          ...total,
          provider: usage?.provider || session.usageTotal?.provider || '',
          model: usage?.model || session.usageTotal?.model || '',
        }
      }
      session.pendingUsage = {}
      if (data?.thread_title && session.threadId) {
        const threadStore = useThreadStore()
        threadStore.rename(session.threadId, data.thread_title).catch(() => {})
      }
      // done 的 project_dir 可能仍然是 null（work_end 没带 app_type/date/project_name）；
      // 用线程 state/工具轨迹再解析一次，保证完成后“打开项目”可点。
      if (session.threadId && !session.projectDir) {
        const threadId = session.threadId
        resolveProjectDir(threadId)
          .then((resolved) => {
            if (resolved?.project_dir) this.setProjectDir(threadId, resolved.project_dir)
          })
          .catch(() => {})
      }
      this.setInterrupt(session, null)
      session.revision += 1
    },

    applyStreamError(session, detail) {
      session.sending = false
      session.error = detail || '对话失败'
      let bubble = [...session.messages].reverse().find((m) => m.role === 'assistant')
      if (!bubble) {
        session.messages.push({ id: uid(), role: 'assistant', content: '', streaming: false, agent: '', tools: [] })
        bubble = session.messages[session.messages.length - 1]
      }
      bubble.streaming = false
      bubble.error = true
      bubble.content = session.error
      this.stopStreaming(session, false)
      this.endActiveTurn(session.threadId, 'error')
      session.revision += 1
    },

    async submitAnswer(key, answer) {
      const session = this.ensureSession(key)
      if (!session.threadId || session.sending) return
      this.stopTypewriter(session)
      this.flushTypeTimers(session)
      const threadId = session.threadId
      const baselineLastAssistant = lastAssistantOf(session)?.content || ''
      const baselineCount = session.messages.length
      const savedInterrupt = session.interrupt
      const savedDraft = session.inquiryDraft
      const savedSignature = session.inquirySignature
      session.error = ''
      session.stale = false
      // 先把追问/人工确认面板收起来，恢复执行本身是同步 POST，可能要跑很久；
      // 真正的新的 interrupt 在 resume 返回后用 allowInterrupt 的应用。
      session.resuming = true
      session.interrupt = null
      session.paused = false
      session.sending = true
      session.seconds = 0
      // 恢复的是同一轮：工具轨迹起点沿用本轮原开始时间，别把暂停前的轨迹切掉
      session.turnStartedAt = session.turnStartedAt
        || this.activeTurns[threadId]?.startedAt
        || Date.now()
      session.startedAt = Date.now()
      this.startTick()
      this.startActiveTurn(session, {
        baselineLastAssistant,
        baselineCount,
        mode: 'resume',
        resumeSignature: interruptSignature(savedInterrupt),
      })

      try {
        const userId = (() => {
          const user = useUserStore()
          return user.adminId == null ? '' : String(user.adminId)
        })()
        await streamResume({ userId, threadId, answer }, {
          onMeta: (data) => {
            const newThreadId = String(data?.thread_id || '')
            if (newThreadId && session.threadId !== newThreadId) this.rekey(session, newThreadId)
          },
          onStatus: (data) => {
            if (!data?.text) return
            const agent = data.agent || data.node || ''
            const bubble = this.ensureBubble(session, agent, agent || 'assistant')
            bubble.status = data.text
            session.revision += 1
            this.touchActiveTurn(session.threadId)
          },
          onVerification: (data) => {
            if (data?.record) {
              session.verification = data.record
              session.revision += 1
            }
          },
          onUsage: (data) => {
            const agent = normalizeAgent(data?.agent)
            const usage = data?.usage
            if (!agent || !usage?.total) return
            const payload = {
              ...usage,
              provider: data.provider || '',
              model: data.model || '',
            }
            if (!session.pendingUsage) session.pendingUsage = {}
            session.pendingUsage[agent] = payload
            const start = Number.isInteger(session.turnStartIndex) ? session.turnStartIndex : 0
            let target = null
            for (const message of session.messages.slice(start)) {
              if (message.role !== 'assistant' || message.error) continue
              if (normalizeAgent(message.agent) === agent) target = message
            }
            if (target) {
              target.usage = payload
              delete session.pendingUsage[agent]
              session.revision += 1
              this.touchActiveTurn(session.threadId)
            }
          },
          onToken: (data) => {
            if (!data?.content) return
            if (!data.agent && data.node === 'handler') return
            const isFinal = data.node === 'final'
            const keyName = isFinal ? 'final' : data.agent || data.node || 'assistant'
            const bubble = this.ensureBubble(session, isFinal ? '' : data.agent || data.node || '', keyName)
            bubble.status = ''
            this.appendToken(session, bubble, data.content)
            this.touchActiveTurn(session.threadId)
          },
          onInterrupt: (data) => {
            this.setInterrupt(session, extractInterrupt(data?.interrupt))
            session.paused = true
            session.sawDone = true
            session.historyLoaded = true
            this.stopStreaming(session, true)
            this.endActiveTurn(session.threadId, 'paused')
            session.revision += 1
          },
          onDone: (data) => {
            this.applyDone(session, data)
          },
          onError: (data) => {
            this.applyStreamError(session, data?.detail || '对话失败')
          },
        })
        session.resuming = false
        if (!session.error && !session.paused) this.setInterrupt(session, null)
      } catch (e) {
        session.resuming = false
        session.error = e?.message || '提交失败'
        // 提交失败：把刚才被收起来的面板和草稿还回去，用户可以直接重试
        if (savedInterrupt) {
          session.interrupt = savedInterrupt
          session.paused = true
          session.inquiryDraft = savedDraft
          session.inquirySignature = savedSignature
          this.saveInquiryDraft(session)
        }
      } finally {
        session.sending = false
        session.resuming = false
        this.dedupeAssistant(session)
        this.endActiveTurn(session.threadId, session.paused ? 'paused' : 'done')
        this.maybeStopTick()
        const threadStore = useThreadStore()
        await threadStore.loadAll()
        session.revision += 1
      }
    },
  },
})