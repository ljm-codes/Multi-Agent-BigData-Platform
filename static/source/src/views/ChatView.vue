<script setup>
import {computed, nextTick, onBeforeUnmount, onMounted, ref, watch} from 'vue'
import {useRouter} from 'vue-router'
import {useUserStore} from '@/stores/user'
import {useThreadStore} from '@/stores/threads'
import {useChatStore} from '@/stores/chat'
import {archiveThreads, deleteThreads} from '@/api/threads'
import {getIdes, openFile, resolveProjectDir} from '@/api/openApp'
import BackgroundPanel from '@/components/BackgroundPanel.vue'
import MessageList from '@/components/MessageList.vue'
import ThreadItem from '@/components/ThreadItem.vue'
import UsageChip from '@/components/UsageChip.vue'
import VerificationCard from '@/components/VerificationCard.vue'
import InquiryPanel from '@/components/InquiryPanel.vue'
import ToolTracePanel from '@/components/ToolTracePanel.vue'
import {createTicket, openBackgroundSocket} from '@/api/background'

const router = useRouter()
const user = useUserStore()
const threadStore = useThreadStore()
const chat = useChatStore()

const collapsed = ref(false)
const tab = ref('active')
const sidebarWidth = ref(260)
const resizing = ref(false)

const session = computed(() => chat.viewSession)
const currentThreadId = computed(() => chat.viewThreadId)
const messages = computed(() => session.value?.messages || [])
const sending = computed(() => Boolean(session.value?.sending))
const sendingSeconds = computed(() => session.value?.seconds || 0)
const interrupt = computed(() => session.value?.interrupt || null)
const paused = computed(() => Boolean(session.value?.paused))
const projectDir = computed(() => session.value?.projectDir || '')
const verification = computed(() => session.value?.verification || null)
const usageTotal = computed(() => session.value?.usageTotal || null)
const toolTraces = computed(() => session.value?.toolTraces || [])
const stale = computed(() => Boolean(session.value?.stale))
const inquiry = computed(() => (
    session.value?.interrupt?.kind === 'inquiry' ? session.value.interrupt : null
))
const inquiryDraft = computed(() => session.value?.inquiryDraft || null)
const inquiryActiveQuestion = computed(() => (
    inquiry.value?.questions?.[inquiryDraft.value?.active ?? 0] || null
))
const inquiryAllAnswered = computed(() => {
  if (!inquiry.value) return false
  const questions = inquiry.value.questions || []
  return questions.length > 0 && questions.every((q) => Boolean(String(draftReply(q.id)).trim()))
})
const inquiryMissingCount = computed(() => {
  if (!inquiry.value) return 0
  return (inquiry.value.questions || []).filter((q) => !String(draftReply(q.id)).trim()).length
})
const trackedRunning = computed(() => chat.isTrackedRunning(currentThreadId.value))
const trackedSeconds = computed(() => (
    currentThreadId.value ? chat.trackedSeconds(currentThreadId.value) : 0
))
const resuming = computed(() => Boolean(
    session.value?.resuming
    || (currentThreadId.value && chat.isResuming(currentThreadId.value))
))
const runSeconds = computed(() => (
    session.value?.sending ? (session.value.seconds || 0) : trackedSeconds.value
))
const showTraces = ref(false)
const runningTraces = computed(() => {
  const current = session.value
  if (!current) return []
  const agentsWithMessages = new Set(
      current.messages.filter((message) => message.agent).map((message) => message.agent)
  )
  const startedAt = current.startedAt
      || chat.activeTurns[currentThreadId.value]?.startedAt
      || 0
  return (current.toolTraces || []).filter((trace) => {
    if (trace?.status !== 'running') return false
    if (!trace?.agent || agentsWithMessages.has(trace.agent)) return false
    const ts = Date.parse(trace.finished_at || trace.started_at || '')
    if (!startedAt || !Number.isFinite(ts)) return true
    return ts >= startedAt - 5000
  })
})
const otherRunning = computed(() => chat.runningIds
    .filter((id) => !id.startsWith('draft:') && id !== currentThreadId.value)
    .map((id) => ({
      id,
      title: threadStore.titleOf(id),
      project: chat.projectOf(id),
    })))
const projectName = computed(() => (
    session.value?.projectName || (currentThreadId.value ? chat.projectOf(currentThreadId.value) : '')
))
const lastToolLabel = computed(() => {
  const item = session.value?.lastTool
  if (!item?.name) return ''
  const date = item.at ? new Date(item.at) : null
  const time = date && !Number.isNaN(date.getTime())
      ? `${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}:${String(date.getSeconds()).padStart(2, '0')}`
      : ''
  return `${item.name}${time ? ` · ${time}` : ''}`
})

/** 当前追问草稿中某个问题的回答 */
function draftReply(questionId) {
  return inquiryDraft.value?.replies?.[questionId] || ''
}

const input = ref('')
const inputRef = ref(null)
const inquiryPanelRef = ref(null)
const MAX_INPUT_HEIGHT = 300
const listRef = ref(null)
const composerText = computed({
  get: () => (
      inquiry.value && inquiryActiveQuestion.value
          ? draftReply(inquiryActiveQuestion.value.id)
          : input.value
  ),
  set: (value) => {
    if (inquiry.value && inquiryActiveQuestion.value && inquiryDraft.value) {
      const questionId = inquiryActiveQuestion.value.id
      inquiryDraft.value.replies[questionId] = value
      if (!inquiryDraft.value.touched) inquiryDraft.value.touched = {}
      inquiryDraft.value.touched[questionId] = true
      const index = inquiryActiveQuestion.value.suggestion.findIndex(
          (item) => item.trim() === String(value).trim()
      )
      inquiryDraft.value.suggestion = index
      chat.saveInquiryDraft(session.value)
    } else {
      input.value = value
    }
  },
})
const uiError = ref('')
const error = computed(() => session.value?.error || uiError.value)
const readOnly = computed(() => tab.value === 'archived')
const threadList = computed(() => threadStore.recordsOf(tab.value))
const title = computed(() => (
    currentThreadId.value ? threadStore.titleOf(currentThreadId.value) : '新对话'
))
const canSend = computed(() => {
  if (sending.value || trackedRunning.value || readOnly.value) return false
  if (inquiry.value) return inquiryAllAnswered.value
  return !paused.value && !interrupt.value && Boolean(input.value.trim())
})
const composerDisabled = computed(() => (
    sending.value
    || trackedRunning.value
    || (!inquiry.value && (paused.value || Boolean(interrupt.value)))
))

const multiSelect = ref(false)
const selectedIds = ref([])
const allSelected = computed(() => (
    threadList.value.length > 0 && selectedIds.value.length === threadList.value.length
))

const ides = ref({})
const currentIde = ref(localStorage.getItem('ide') || '')
const theme = ref(localStorage.getItem('theme') || 'dark')

const showBackground = ref(false)

let taskSocket = null
let taskRetryTimer = null
let taskRetries = 0
let taskClosedByUs = false

watch(
    theme,
    (value) => {
      document.documentElement.dataset.theme = value
      localStorage.setItem('theme', value)
    },
    {immediate: true}
)

watch(tab, () => {
  multiSelect.value = false
  selectedIds.value = []
  threadStore.load(tab.value)
})

watch(currentIde, (value) => value && localStorage.setItem('ide', value))

watch(currentThreadId, () => {
  showTraces.value = false
})
watch(
    () => sending.value || resuming.value || trackedRunning.value,
    (active) => {
      if (!active) showTraces.value = false
    }
)

// 只给“当前可见会话”开 WS；隐藏的 running 会话由 store 轮询工具轨迹/历史兜底
watch(
    () => chat.viewThreadId,
    (threadId) => {
      taskRetries = 0
      connectTaskSocket(threadId)
    },
    {immediate: true}
)

// 流式 token / 轮询历史更新时，只有原本就在底部才自动跟随
watch(
    () => session.value?.revision,
    async () => {
      if (!session.value) return
      if (session.value.key !== chat.viewKey) return
      await nextTick()
      if (session.value.sending || listRef.value?.isNearBottom?.() !== false) {
        listRef.value?.scrollToBottom?.()
      }
    }
)

watch(composerText, () => nextTick(autoGrow))
watch(inquiry, (value) => {
  // 追问面板出现时把焦点交给面板：←→ / ↑↓ 可直接选，推荐项已自动填入输入框
  if (value && !readOnly.value) nextTick(() => inquiryPanelRef.value?.focus?.())
})
watch(composerDisabled, (disabled) => {
  // 中断事件到达时 sending 还是 true，等它收尾后再把焦点交给面板
  if (!disabled && inquiry.value && !readOnly.value) {
    nextTick(() => inquiryPanelRef.value?.focus?.())
  }
})

onBeforeUnmount(() => {
  closeTaskSocket()
  chat.stopPolling()
  chat.stopTick()
})


onMounted(async () => {
  await threadStore.loadAll()
  await loadIdes()
  await chat.restore()
  syncTabForThread(chat.viewThreadId)
  await nextTick()
  scrollToBottom()
})

function startResize(event) {
  event.preventDefault()
  resizing.value = true
  if (collapsed.value) collapsed.value = false
  const startX = event.clientX
  const startWidth = sidebarWidth.value

  const onMove = (e) => {
    sidebarWidth.value = Math.min(460, Math.max(180, startWidth + e.clientX - startX))
  }
  const onUp = () => {
    resizing.value = false
    window.removeEventListener('mousemove', onMove)
    window.removeEventListener('mouseup', onUp)
  }
  window.addEventListener('mousemove', onMove)
  window.addEventListener('mouseup', onUp)
}

async function loadIdes() {
  try {
    ides.value = (await getIdes()) || {}
    const keys = Object.keys(ides.value)
    if (!keys.length) {
      currentIde.value = ''
      return
    }
    if (!keys.includes(currentIde.value)) {
      currentIde.value = keys.find((key) => key !== 'notepad') || keys[0]
    }
  } catch (e) {
    uiError.value = e?.message || '获取 IDE 列表失败'
  }
}

function newChat() {
  uiError.value = ''
  tab.value = 'active'
  chat.newChat()
  input.value = ''
  nextTick(autoGrow)
}

function syncTabForThread(threadId) {
  if (!threadId) return
  const record = threadStore.recordOf(threadId)
  if (!record) return
  const next = record.is_archive ? 'archived' : 'active'
  if (tab.value !== next) tab.value = next
}

async function selectThread(threadId) {
  if (!threadId) return
  uiError.value = ''
  syncTabForThread(threadId)
  await chat.selectThread(threadId)
  await nextTick()
  scrollToBottom()
}

async function scrollToBottom() {
  await nextTick()
  listRef.value?.scrollToBottom?.()
}

function closeTaskSocket() {
  if (taskRetryTimer) {
    clearTimeout(taskRetryTimer)
    taskRetryTimer = null
  }
  if (!taskSocket) return
  taskClosedByUs = true
  taskSocket.onclose = null
  taskSocket.onmessage = null
  taskSocket.close()
  taskSocket = null
}

function scheduleTaskReconnect(threadId) {
  if (taskClosedByUs) return
  if (taskRetryTimer) clearTimeout(taskRetryTimer)
  const delay = Math.min(1000 * 2 ** taskRetries, 10000)
  taskRetries += 1
  taskRetryTimer = setTimeout(() => connectTaskSocket(threadId), delay)
}

async function connectTaskSocket(threadId) {
  closeTaskSocket()
  taskClosedByUs = false
  if (!threadId) return

  let ticket = ''
  try {
    ticket = (await createTicket())?.ticket || ''
  } catch {
    scheduleTaskReconnect(threadId)
    return
  }
  if (!ticket) {
    scheduleTaskReconnect(threadId)
    return
  }

  taskSocket = openBackgroundSocket(ticket, threadId)
  taskSocket.onmessage = (event) => {
    let message
    try {
      message = JSON.parse(event.data)
    } catch {
      return
    }
    chat.handleWsMessage(threadId, message)
  }
  taskSocket.onclose = () => {
    if (!taskClosedByUs) scheduleTaskReconnect(threadId)
  }
  taskSocket.onerror = () => {
    // 交给 onclose 统一重连
  }
}

async function send() {
  if (inquiry.value) {
    await submitInquiry()
    return
  }

  const text = input.value.trim()
  if (!text || !canSend.value) return

  if (otherRunning.value.length) {
    const labels = otherRunning.value
        .map((item) => `${item.title}${item.project ? `（${item.project}）` : ''}`)
        .join('、')
    const ok = window.confirm(
        `以下会话仍在后端运行：\n${labels}\n\n当前会话会新开一轮，确定继续吗？`
    )
    if (!ok) return
  }

  uiError.value = ''
  input.value = ''
  await nextTick(autoGrow)
  try {
    const ok = await chat.send(chat.viewKey, text)
    if (!ok) {
      input.value = text
      await nextTick(autoGrow)
    }
  } catch (e) {
    uiError.value = e?.message || '发送失败'
    input.value = text
    await nextTick(autoGrow)
  }
}

async function submitAnswer(answer) {
  if (!currentThreadId.value || sending.value) return
  uiError.value = ''
  try {
    await chat.submitAnswer(chat.viewKey, answer)
  } catch (e) {
    uiError.value = e?.message || '提交失败'
  }
}

function selectInquiryQuestion(index) {
  const current = inquiry.value
  const draft = inquiryDraft.value
  if (!current || !draft) return
  const count = current.questions.length
  if (!count) return
  const next = ((Number(index) % count) + count) % count
  draft.active = next
  const question = current.questions[next]
  const reply = String(draft.replies?.[question.id] ?? '').trim()
  draft.suggestion = (question.suggestion || []).findIndex((item) => item.trim() === reply)
  chat.saveInquiryDraft(session.value)
  nextTick(autoGrow)
}

function pickInquirySuggestion(payload) {
  const {index, focus} = payload || {}
  const question = inquiryActiveQuestion.value
  const draft = inquiryDraft.value
  if (!question || !draft || !Number.isInteger(index)) return
  const text = question.suggestion[index]
  if (typeof text !== 'string') return
  draft.suggestion = index
  draft.replies[question.id] = text
  if (!draft.touched) draft.touched = {}
  draft.touched[question.id] = true
  chat.saveInquiryDraft(session.value)
  nextTick(() => {
    autoGrow()
    if (focus) focusComposer()
  })
}

function focusComposer() {
  const el = inputRef.value
  // 窗口不在前台时不要 focus，否则切走/最小化后会“回弹”
  if (!el || document.visibilityState !== 'visible' || !document.hasFocus()) return
  el.focus()
  const len = el.value.length
  try {
    el.setSelectionRange(len, len)
  } catch {
    /* 某些浏览器/输入法下 setSelectionRange 不可用 */
  }
}

async function submitInquiry() {
  const current = inquiry.value
  if (!current || sending.value) return
  const questions = current.questions || []
  const missing = questions.filter((q) => !String(draftReply(q.id)).trim()).length
  if (missing > 0) {
    uiError.value = `还有 ${missing} 个问题未填写，全部填写后才能提交`
    return
  }
  const answers = questions.map((q) => ({
    question: q.question,
    reply: String(draftReply(q.id)).trim(),
  }))
  uiError.value = ''
  try {
    await chat.submitAnswer(chat.viewKey, answers)
  } catch (e) {
    uiError.value = e?.message || '提交回答失败'
  }
}

function stopTracking() {
  if (currentThreadId.value) chat.stopTracking(currentThreadId.value)
}

function clearStale() {
  if (currentThreadId.value) chat.clearStale(currentThreadId.value)
}

async function openIdeLink(href) {
  const raw = String(href || '').replace(/^IDE:\/\//i, '')
  const [rawPath, hash] = raw.split('#')
  let filePath = rawPath
  try {
    filePath = decodeURIComponent(rawPath)
  } catch {
    filePath = rawPath
  }
  const start = /^L(\d+)/.exec(hash || '')?.[1] || 1
  if (!filePath) return

  const isWindowsAbs = /^[a-zA-Z]:[\\/]/.test(filePath) || filePath.startsWith('\\\\')
  const isPosixAbs = filePath.startsWith('/') && projectDir.value.startsWith('/')
  const fullPath = isWindowsAbs || isPosixAbs || !projectDir.value
      ? filePath
      : `${projectDir.value.replace(/[\\/]+$/, '')}/${filePath.replace(/^[\\/]+/, '')}`
  console.info('[IDE] open', {filePath: fullPath, ide: currentIde.value, line: start})
  try {
    await openFile({filePath: fullPath, ideName: currentIde.value, startIdx: Number(start)})
  } catch (e) {
    console.error('[IDE] open failed', e)
    uiError.value = e?.message || '打开文件失败'
  }
}

function cleanProjectPath(value) {
  return String(value || '').trim().replace(/[，。；;：:、,.)）\]】*]+$/g, '')
}

/** 最终回复里经常直接写了项目绝对路径；后端还没重启/接口不可用时先从这里兜底 */
function projectDirFromMessages() {
  const name = projectName.value
  if (!name) return ''
  const pattern = /(?:[A-Za-z]:[\\/]|\/)[^`\n<>"|]*tmp_code[^`\n<>"|]*/g
  for (const message of messages.value) {
    const text = String(message.content || '')
    if (!text) continue
    const matches = text.match(pattern) || []
    const hit = matches.find((item) => item.includes(name))
    if (hit) {
      const end = hit.indexOf(name) + name.length
      return cleanProjectPath(hit.slice(0, end))
    }
  }
  return ''
}

async function openProject() {
  if (!currentThreadId.value) return
  let dir = projectDir.value
  if (!dir) dir = projectDirFromMessages()
  if (dir) chat.setProjectDir(currentThreadId.value, dir)
  if (!dir && projectName.value) {
    try {
      const resolved = await resolveProjectDir(currentThreadId.value)
      dir = resolved?.project_dir || ''
      if (dir) chat.setProjectDir(currentThreadId.value, dir)
    } catch (e) {
      uiError.value = e?.message || '定位项目目录失败'
      return
    }
  }
  if (!dir) {
    uiError.value = '项目目录还不存在（可能还没初始化完成）'
    return
  }
  try {
    await openFile({filePath: dir, ideName: currentIde.value})
  } catch (e) {
    uiError.value = e?.message || '打开项目失败'
  }
}

async function onRename(threadId, nextTitle) {
  if (!nextTitle) return
  try {
    await threadStore.rename(threadId, nextTitle)
  } catch (e) {
    uiError.value = e?.message || '重命名失败'
  }
}

async function onPin(threadId) {
  try {
    await threadStore.togglePin(threadId)
  } catch (e) {
    uiError.value = e?.message || '置顶失败'
  }
}

async function onArchive(threadId) {
  try {
    await threadStore.archive(threadId)
    if (currentThreadId.value === threadId) newChat()
  } catch (e) {
    uiError.value = e?.message || '归档失败'
  }
}

async function onRemove(threadId) {
  if (!window.confirm('删除后会话记录不可恢复，确定删除？')) return
  try {
    const wasCurrent = currentThreadId.value === threadId
    await threadStore.remove(threadId)
    if (wasCurrent) tab.value = 'active'
    chat.dropThread(threadId)
  } catch (e) {
    uiError.value = e?.message || '删除失败'
  }
}

function toggleMultiSelect() {
  multiSelect.value = !multiSelect.value
  selectedIds.value = []
}

function toggleSelect(threadId) {
  const index = selectedIds.value.indexOf(threadId)
  if (index >= 0) selectedIds.value.splice(index, 1)
  else selectedIds.value.push(threadId)
}

function toggleSelectAll() {
  selectedIds.value = allSelected.value ? [] : threadList.value.map((item) => item.thread_id)
}

async function batchArchive() {
  if (!selectedIds.value.length) return
  try {
    await archiveThreads([...selectedIds.value])
    selectedIds.value = []
    multiSelect.value = false
    await threadStore.loadAll()
  } catch (e) {
    uiError.value = e?.message || '批量归档失败'
  }
}

async function batchDelete() {
  if (!selectedIds.value.length) return
  if (!window.confirm(`确定删除选中的 ${selectedIds.value.length} 个会话？不可恢复。`)) return
  try {
    const ids = [...selectedIds.value]
    const hadCurrent = ids.includes(currentThreadId.value)
    await deleteThreads(ids)
    for (const threadId of ids) chat.dropThread(threadId)
    if (hadCurrent) tab.value = 'active'
    selectedIds.value = []
    multiSelect.value = false
    await threadStore.loadAll()
  } catch (e) {
    uiError.value = e?.message || '批量删除失败'
  }
}

function onKeydown(event) {
  // 追问面板下，Enter 允许换行；方向键/建议选择在面板里处理
  if (inquiry.value) return
  if (event.key === 'Enter' && !event.shiftKey) {
    event.preventDefault()
    send()
  }
}

/** 输入框随内容长高，到上限后改为内部滚动 */
function autoGrow() {
  const el = inputRef.value
  if (!el) return
  el.style.height = 'auto'
  el.style.height = `${Math.min(el.scrollHeight, MAX_INPUT_HEIGHT)}px`
}

function logout() {
  closeTaskSocket()
  chat.reset()
  user.logout()
  router.replace('/login')
}
</script>

<template>
  <div class="shell">
    <aside
        class="sidebar"
        :class="{ collapsed }"
        :style="{ width: collapsed ? '56px' : sidebarWidth + 'px' }"
    >
      <div class="sidebar-head">
        <button class="icon-btn" title="折叠 / 展开" @click="collapsed = !collapsed">☰</button>
        <span v-show="!collapsed" class="brand">Multi-Agent</span>
        <button
            v-show="!collapsed"
            class="icon-btn multi-btn"
            :class="{ on: multiSelect }"
            title="多选"
            @click="toggleMultiSelect"
        >
          ☑
        </button>
      </div>

      <button v-show="!collapsed && !multiSelect" class="primary new-chat" @click="newChat">
        ＋ 新增对话
      </button>

      <div v-show="!collapsed" class="tabs">
        <button :class="['tab', { active: tab === 'active' }]" @click="tab = 'active'">未归档</button>
        <button :class="['tab', { active: tab === 'archived' }]" @click="tab = 'archived'">已归档</button>
      </div>

      <div class="thread-list">
        <p v-if="!collapsed && !threadList.length" class="empty-hint">
          {{ threadStore.loading ? '加载中…' : '暂无对话' }}
        </p>
        <template v-if="!collapsed">
          <ThreadItem
              v-for="record in threadList"
              :key="record.thread_id"
              :thread="record"
              :title="threadStore.titleOf(record.thread_id)"
              :active="record.thread_id === currentThreadId"
              :pinned="threadStore.isPinned(record.thread_id)"
              :archived="tab === 'archived'"
              :selectable="multiSelect"
              :selected="selectedIds.includes(record.thread_id)"
              :running="chat.isRunning(record.thread_id)"
              :project-name="chat.projectOf(record.thread_id)"
              @select="selectThread"
              @toggle="toggleSelect"
              @rename="onRename"
              @pin="onPin"
              @archive="onArchive"
              @remove="onRemove"
          />
        </template>
      </div>

      <div v-if="multiSelect" class="multi-bar">
        <span class="muted">已选 {{ selectedIds.length }}</span>
        <div class="multi-actions">
          <button class="ghost small" @click="toggleSelectAll">{{ allSelected ? '取消全选' : '全选' }}</button>
          <button class="ghost small" :disabled="!selectedIds.length" @click="batchArchive">归档</button>
          <button
              v-if="tab === 'archived'"
              class="ghost small danger"
              :disabled="!selectedIds.length"
              @click="batchDelete"
          >
            删除
          </button>
          <button class="ghost small" @click="toggleMultiSelect">退出</button>
        </div>
      </div>

      <div v-else class="sidebar-foot">
        <button
            v-if="Number(user.permission ?? 0) >= 1"
            class="ghost"
            :title="collapsed ? '运维看板' : ''"
            @click="router.push('/ops')"
        >
          {{ collapsed ? '🛠' : '运维看板' }}
        </button>
        <button class="ghost" :title="collapsed ? '用户管理' : ''" @click="router.push('/admin')">
          {{ collapsed ? '⚙' : '用户管理' }}
        </button>
        <button class="ghost" :title="collapsed ? '退出登录' : ''" @click="logout">
          {{ collapsed ? '⏻' : '退出登录' }}
        </button>
      </div>

      <div v-show="!collapsed" class="resizer" :class="{ active: resizing }" @mousedown="startResize"></div>
    </aside>

    <main class="main">
      <header class="main-head">
        <span class="title">{{ title }}</span>
        <span v-if="projectName" class="project-chip" :title="`当前会话项目：${projectName}`">
          {{ projectName }}
        </span>

        <button
            class="ghost small"
            :disabled="!projectDir && !projectName"
            :title="projectDir || (projectName ? `按项目名定位：${projectName}` : '当前会话没有关联项目')"
            @click="openProject"
        >
          打开项目
        </button>

        <select v-if="Object.keys(ides).length" v-model="currentIde" class="select" title="打开方式">
          <option v-for="(info, key) in ides" :key="key" :value="key">{{ info.display }}</option>
        </select>
        <span v-else class="muted small">未检测到 IDE</span>

        <button
            class="icon-btn"
            :class="{ on: showBackground }"
            :disabled="!currentThreadId"
            title="后台任务"
            @click="showBackground = !showBackground"
        >
          ⚑
        </button>

        <button
            class="icon-btn"
            :title="theme === 'dark' ? '切换浅色' : '切换深色'"
            @click="theme = theme === 'dark' ? 'light' : 'dark'"
        >
          {{ theme === 'dark' ? '☾' : '☀' }}
        </button>
      </header>

      <div class="body">
        <div class="chat">
          <div v-if="otherRunning.length" class="run-banner">
            <span class="run-banner-label">其他会话仍在后端运行：</span>
            <button
                v-for="item in otherRunning"
                :key="item.id"
                class="run-link"
                :title="item.project ? `项目 ${item.project}` : '查看运行中的会话'"
                @click="selectThread(item.id)"
            >
              {{ item.title }}
              <template v-if="item.project"> · {{ item.project }}</template>
            </button>
          </div>

          <div v-if="sending || resuming || trackedRunning" class="run-strip">
            <span class="run-dot"></span>
            <span class="run-strip-main">
              {{ resuming ? '已提交，Agent 继续运行' : sending ? 'Agent 正在运行' : '后端仍在运行' }}
              {{ projectName ? ` · 项目 ${projectName}` : '' }} · 已 {{ runSeconds }}s
              <span v-if="lastToolLabel" class="run-strip-sub">最近：{{ lastToolLabel }}</span>
            </span>
            <button
                v-if="trackedRunning && !sending && !resuming"
                class="ghost tiny"
                title="只隐藏进度，不会终止后端执行"
                @click="stopTracking"
            >
              停止跟踪
            </button>
            <button
                class="ghost tiny"
                :disabled="!runningTraces.length"
                :title="runningTraces.length ? '查看当前运行的工具调用' : '暂时没有工具轨迹'"
                @click="showTraces = !showTraces"
            >
              {{ showTraces ? '收起工具' : (runningTraces.length ? `工具调用 · ${runningTraces.length}` : '工具调用') }}
            </button>
          </div>

          <div
              v-if="(sending || resuming || trackedRunning) && showTraces && runningTraces.length"
              class="task-dock trace-dock"
          >
            <ToolTracePanel :traces="runningTraces" :sticky-header="true"/>
          </div>

          <div v-if="stale" class="run-strip stale">
            <span class="run-strip-main">
              上次后台执行已停止，未收到最终结果；可查看历史消息或工具轨迹。
            </span>
            <button class="ghost tiny" @click="clearStale">知道了</button>
          </div>

          <MessageList
              ref="listRef"
              :messages="messages"
              :tool-traces="toolTraces"
              :read-only="readOnly"
              :interrupt="readOnly ? null : (inquiry ? null : interrupt)"
              :elapsed="sendingSeconds"
              :turn-started-at="session?.turnStartedAt || 0"
              @open-ide="openIdeLink"
              @submit="submitAnswer"
          />

          <div v-if="currentThreadId && verification" class="task-dock">
            <VerificationCard :record="verification"/>
          </div>

          <p v-if="error" class="error bar">{{ error }}</p>
          <p v-if="paused && !interrupt" class="error bar">
            该会话停在人工确认，但当前无法恢复确认面板；请新建对话或回到原窗口完成确认。</p>

          <InquiryPanel
              v-if="inquiry && !readOnly"
              ref="inquiryPanelRef"
              :interrupt="inquiry"
              :active="inquiryDraft?.active ?? 0"
              :suggestion="inquiryDraft?.suggestion ?? -1"
              :replies="inquiryDraft?.replies || {}"
              :disabled="sending"
              @update:active="selectInquiryQuestion"
              @pick="pickInquirySuggestion"
          />

          <div v-if="usageTotal && Number(usageTotal.total) && !readOnly" class="usage-bar">
            <UsageChip :usage="usageTotal" variant="total" label="Token 用量" />
          </div>

          <footer v-if="!readOnly" class="composer">
            <textarea
                ref="inputRef"
                v-model="composerText"
                rows="1"
                :disabled="composerDisabled"
                :placeholder="inquiry ? (inquiryMissingCount ? `还差 ${inquiryMissingCount} 个问题必填；选择建议或直接输入…` : '可继续补充当前回答；全部填写后点右下角提交回答') : resuming ? '已提交，Agent 继续运行…' : trackedRunning ? '该会话正在后台运行，完成后可继续输入' : '输入消息，Enter 发送，Shift + Enter 换行'"
                @keydown="onKeydown"
            ></textarea>
            <button class="primary" :disabled="!canSend" @click="send">
              {{
                sending ? (resuming ? '运行中…' : inquiry ? '提交中…' : '处理中…') : inquiry ? '提交回答' : trackedRunning ? '运行中…' : '发送'
              }}
            </button>
          </footer>
        </div>

        <BackgroundPanel v-if="showBackground" :thread-id="currentThreadId"/>
      </div>
    </main>
  </div>
</template>

<style scoped>
.shell {
  display: flex;
  height: 100%;
  background: var(--bg);
}

.sidebar {
  position: relative;
  display: flex;
  flex-direction: column;
  flex-shrink: 0;
  background: var(--bg-soft);
  border-right: 1px solid var(--border);
  transition: width 0.15s ease;
}

.sidebar-head {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 12px;
}

.brand {
  flex: 1;
  font-size: 13px;
  font-weight: 600;
  letter-spacing: 0.02em;
}

.icon-btn {
  width: 32px;
  height: 32px;
  padding: 0;
  color: var(--text);
  background: transparent;
  border: 1px solid var(--border);
  border-radius: 8px;
}

.icon-btn.on {
  color: #fff;
  background: var(--primary);
  border-color: var(--primary);
}

.icon-btn:disabled {
  cursor: not-allowed;
  opacity: 0.45;
}

.multi-btn {
  margin-left: auto;
}

.new-chat {
  margin: 4px 12px 10px;
}

.tabs {
  display: flex;
  gap: 4px;
  padding: 0 12px 10px;
}

.tab {
  flex: 1;
  padding: 6px 0;
  font-size: 13px;
  color: var(--text-muted);
  background: transparent;
  border: 0;
  border-bottom: 1px solid transparent;
}

.tab.active {
  color: var(--text);
  border-bottom-color: var(--primary);
}

.thread-list {
  flex: 1;
  overflow-y: auto;
  padding: 4px 8px;
}

.sidebar-foot {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 10px 12px;
  border-top: 1px solid var(--border);
}

.multi-bar {
  padding: 8px 12px;
  border-top: 1px solid var(--border);
}

.multi-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-top: 8px;
}

.ghost.small {
  padding: 4px 9px;
  font-size: 12px;
}

.ghost.small:disabled {
  cursor: not-allowed;
  opacity: 0.45;
}

.ghost.danger {
  color: var(--danger);
  border-color: var(--danger);
}

.resizer {
  position: absolute;
  top: 0;
  right: -3px;
  width: 6px;
  height: 100%;
  cursor: col-resize;
}

.resizer:hover,
.resizer.active {
  background: var(--primary);
  opacity: 0.35;
}

.main {
  display: flex;
  flex-direction: column;
  flex: 1;
  min-width: 0;
}

.main-head {
  display: flex;
  align-items: center;
  gap: 10px;
  height: 52px;
  padding: 0 16px;
  border-bottom: 1px solid var(--border);
}

.title {
  flex: 1;
  overflow: hidden;
  font-size: 14px;
  font-weight: 600;
  white-space: nowrap;
  text-overflow: ellipsis;
}

.project-chip {
  flex-shrink: 1;
  max-width: 220px;
  padding: 2px 8px;
  overflow: hidden;
  font-size: 11px;
  color: var(--text-muted);
  white-space: nowrap;
  text-overflow: ellipsis;
  background: var(--bg-soft);
  border: 1px solid var(--border);
  border-radius: 999px;
}

.select {
  padding: 5px 8px;
  font-size: 12px;
  color: var(--text);
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-radius: 8px;
  outline: none;
}

.small {
  font-size: 12px;
}

.body {
  display: flex;
  flex: 1;
  min-height: 0;
}

.chat {
  display: flex;
  flex-direction: column;
  flex: 1;
  min-width: 0;
}

.run-banner {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
  flex-shrink: 0;
  padding: 8px 20px;
  font-size: 12px;
  color: var(--text-muted);
  background: rgba(59, 130, 246, 0.08);
  border-bottom: 1px solid var(--border);
}

.run-banner-label {
  flex-shrink: 0;
}

.run-link {
  max-width: 260px;
  overflow: hidden;
  padding: 2px 9px;
  font-size: 12px;
  color: var(--primary);
  white-space: nowrap;
  text-overflow: ellipsis;
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-radius: 999px;
}

.run-link:hover {
  border-color: var(--primary);
}

.run-strip {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-shrink: 0;
  padding: 8px 20px;
  font-size: 12px;
  color: var(--text);
  background: rgba(59, 130, 246, 0.1);
  border-bottom: 1px solid var(--border);
}

.run-strip.stale {
  color: var(--text-muted);
  background: rgba(245, 158, 11, 0.12);
}

.run-strip-main {
  flex: 1;
  min-width: 0;
}

.run-strip-sub {
  margin-left: 8px;
  color: var(--text-muted);
}

.run-dot {
  flex-shrink: 0;
  width: 7px;
  height: 7px;
  background: var(--primary);
  border-radius: 50%;
  animation: run-pulse 1.2s ease-in-out infinite;
}

.ghost.tiny {
  flex-shrink: 0;
  padding: 3px 9px;
  font-size: 11px;
}

@keyframes run-pulse {
  0%,
  100% {
    opacity: 1;
  }
  50% {
    opacity: 0.25;
  }
}

.task-dock {
  flex-shrink: 0;
  max-height: 42vh;
  overflow-y: auto;
  border-top: 1px solid var(--border);
  background: var(--bg);
}

.trace-dock {
  border-top: 0;
  border-bottom: 1px solid var(--border);
}

.bar {
  margin: 0;
  padding: 8px 24px;
  font-size: 12px;
  border-top: 1px solid var(--border);
}

.usage-bar {
  display: flex;
  justify-content: flex-end;
  flex-shrink: 0;
  padding: 4px 20px 0;
}

.composer {
  display: flex;
  gap: 10px;
  align-items: flex-end;
  padding: 12px 20px 18px;
  border-top: 1px solid var(--border);
}

.composer textarea {
  flex: 1;
  min-height: 40px;
  max-height: 300px;
  padding: 10px 12px;
  overflow-y: auto;
  color: var(--text);
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-radius: 10px;
  resize: none;
  outline: none;
}

.composer textarea:focus {
  border-color: var(--primary);
}

.empty-hint {
  margin: 8px 0;
  font-size: 13px;
  color: var(--text-muted);
  text-align: center;
}
</style>