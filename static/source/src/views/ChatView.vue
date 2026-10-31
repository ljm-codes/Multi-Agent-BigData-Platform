<script setup>
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { useUserStore } from '@/stores/user'
import { useThreadStore } from '@/stores/threads'
import { normalizeAgent } from '@/utils/text'
import { resume as resumeApi, streamChat } from '@/api/chat'
import { archiveThreads, deleteThreads, getHistory } from '@/api/threads'
import { getIdes, openFile } from '@/api/openApp'
import BackgroundPanel from '@/components/BackgroundPanel.vue'
import MessageList from '@/components/MessageList.vue'
import ThreadItem from '@/components/ThreadItem.vue'

/** 稳定的消息 id：不要用 index 当 key，列表增删时 Vue 会复用错 DOM */
function uid() {
  return globalThis.crypto?.randomUUID?.() || `m${Date.now()}${Math.random().toString(16).slice(2)}`
}

const LAST_THREAD_KEY = 'last_thread_id'

const router = useRouter()
const user = useUserStore()
const threadStore = useThreadStore()

const collapsed = ref(false)
const tab = ref('active')
const sidebarWidth = ref(260)
const resizing = ref(false)

const currentThreadId = ref('')
const messages = ref([])
const interrupt = ref(null)
const sending = ref(false)
const sendingSeconds = ref(0)
let sendingTimer = null
const paused = ref(false)
const error = ref('')
const input = ref('')
const inputRef = ref(null)
const MAX_INPUT_HEIGHT = 300
const listRef = ref(null)

const multiSelect = ref(false)
const selectedIds = ref([])

const projectDir = ref('')
const showBackground = ref(false)

const ides = ref({})
const currentIde = ref(localStorage.getItem('ide') || '')
const theme = ref(localStorage.getItem('theme') || 'dark')

const userId = computed(() => (user.adminId == null ? '' : String(user.adminId)))
const readOnly = computed(() => tab.value === 'archived')
const threadList = computed(() => threadStore.recordsOf(tab.value))
const title = computed(() =>
  currentThreadId.value ? threadStore.titleOf(currentThreadId.value) : '新对话'
)
const canSend = computed(
  () => !sending.value && !paused.value && !readOnly.value && !interrupt.value && input.value.trim()
)
const allSelected = computed(
  () => threadList.value.length > 0 && selectedIds.value.length === threadList.value.length
)

watch(
  theme,
  (value) => {
    document.documentElement.dataset.theme = value
    localStorage.setItem('theme', value)
  },
  { immediate: true }
)

watch(tab, () => {
  multiSelect.value = false
  selectedIds.value = []
  threadStore.load(tab.value)
})
watch(currentIde, (value) => value && localStorage.setItem('ide', value))
watch(currentThreadId, (id) => {
  if (id) localStorage.setItem(LAST_THREAD_KEY, id)
  else localStorage.removeItem(LAST_THREAD_KEY)
})
watch(sending, (active) => {
  if (sendingTimer) {
    clearInterval(sendingTimer)
    sendingTimer = null
  }
  sendingSeconds.value = 0
  if (active) {
    sendingTimer = setInterval(() => {
      sendingSeconds.value += 1
    }, 1000)
  }
})
onBeforeUnmount(() => {
  if (sendingTimer) clearInterval(sendingTimer)
  clearTyping()
})

onMounted(async () => {
  await threadStore.loadAll()
  await loadIdes()
  // 刷新后回到上次打开的会话；找不到（被删/换用户）就留在新对话
  const saved = localStorage.getItem(LAST_THREAD_KEY)
  if (!saved) return
  const inActive = threadStore.recordsOf('active').some((r) => r.thread_id === saved)
  const inArchived = threadStore.recordsOf('archived').some((r) => r.thread_id === saved)
  if (!inActive && !inArchived) {
    localStorage.removeItem(LAST_THREAD_KEY)
    return
  }
  if (!inActive && inArchived) tab.value = 'archived'
  await selectThread(saved)
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
      // 有真实 IDE 就别默认掉到记事本；只有全是轻量编辑器时才选第一个
      currentIde.value = keys.find((key) => key !== 'notepad') || keys[0]
    }
  } catch (e) {
    error.value = e?.message || '获取 IDE 列表失败'
  }
}

function newChat() {
  clearTyping()
  tab.value = 'active'
  currentThreadId.value = ''
  messages.value = []
  interrupt.value = null
  paused.value = false
  projectDir.value = ''
  error.value = ''
  multiSelect.value = false
  selectedIds.value = []
}

async function loadHistory(threadId) {
  const data = await getHistory(threadId)
  const interrupted = Boolean(data?.interrupted ?? data?.manual_intervention)
  paused.value = interrupted
  projectDir.value = data?.project_dir || projectDir.value || ''
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
  // 旧会话兜底：最后一条 output_msg 还没进 messages 时补一条；interrupt 时它是旧值，不能用
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
  messages.value = rendered
  interrupt.value = interrupted && data?.interrupt ? extractInterrupt(data.interrupt) : null
  if (!threadStore.titles[threadId]) {
    const firstUser = rendered.find((m) => m.role === 'user')
    if (firstUser) threadStore.setTitle(threadId, firstUser.content)
  }
  await scrollToBottom()
  return data
}

async function selectThread(threadId) {
  if (sending.value) return
  clearTyping()
  currentThreadId.value = threadId
  interrupt.value = null
  paused.value = false
  projectDir.value = ''
  error.value = ''
  messages.value = []
  try {
    await loadHistory(threadId)
  } catch (e) {
    error.value = e?.message || '加载会话失败'
  }
}

function extractInterrupt(payload) {
  const list = Array.isArray(payload) ? payload : payload ? [payload] : []
  const first = list[0]
  if (!first) return null
  const value = first.value ?? first
  if (value == null) return null
  if (typeof value === 'string') return { kind: 'submit', text: value }
  return { kind: 'manual', text: value.msg || value.message || '' }
}

async function scrollToBottom() {
  await nextTick()
  listRef.value?.scrollToBottom?.()
}

// 未启用：前端本地打字机（当前由后端 _final_frames 分帧，前端收到即追加）
const TYPE_CHARS_PER_TICK = 2
const TYPE_TICK_MS = 16
let typeTimer = null

function ensureTypeTimer() {
  if (typeTimer) return
  typeTimer = setInterval(() => {
    let active = false
    for (const m of messages.value) {
      if (m.role !== 'assistant' || !m.pendingText) continue
      active = true
      const take = Math.min(TYPE_CHARS_PER_TICK, m.pendingText.length)
      m.content += m.pendingText.slice(0, take)
      m.pendingText = m.pendingText.slice(take)
      if (!m.pendingText) {
        m.streaming = false
        m.status = ''
      }
    }
    if (active) scrollToBottom()
    else {
      clearInterval(typeTimer)
      typeTimer = null
    }
  }, TYPE_TICK_MS)
}

function startFinalTyping(data) {
  // 本轮分析/开发 Agent 还没吐完的文字先收尾，再单独给最终答案做打字机
  flushTyping()
  for (const m of messages.value) {
    if (m.role === 'assistant') {
      m.streaming = false
      m.status = ''
    }
  }
  let bubble = [...messages.value].reverse().find(
    (m) => m.role === 'assistant' && !m.content && !m.error
  )
  if (!bubble) {
    bubble = { id: uid(), role: 'assistant', content: '', streaming: true, agent: '', tools: [] }
    messages.value.push(bubble)
  }
  messages.value = messages.value.filter(
    (m) => m === bubble || !(m.role === 'assistant' && !m.content && !m.error)
  )
  bubble.agent = normalizeAgent(data?.metadata?.from_)
  bubble.pendingText = String(data?.output_msg || '（无输出）')
  bubble.streaming = true
  ensureTypeTimer()
}

function flushTyping() {
  for (const m of messages.value) {
    if (m.role === 'assistant' && m.pendingText) {
      m.content += m.pendingText
      m.pendingText = ''
      m.streaming = false
    }
  }
  if (typeTimer) {
    clearInterval(typeTimer)
    typeTimer = null
  }
}

function clearTyping() {
  if (typeTimer) {
    clearInterval(typeTimer)
    typeTimer = null
  }
  for (const m of messages.value) {
    if (m.pendingText) m.pendingText = ''
  }
}

function enqueueTyping(bubble, text) {
  if (!bubble || !text) return
  bubble.pendingText = (bubble.pendingText || '') + text
  bubble.streaming = true
  ensureTypeTimer()
}

async function send() {
  const text = input.value.trim()
  if (!text || sending.value || readOnly.value || interrupt.value) return

  flushTyping()
  input.value = ''
  error.value = ''
  messages.value.push({ id: uid(), role: 'user', content: text })
  const userBubble = messages.value[messages.value.length - 1]

  const bubbleMap = new Map()
  let pendingBubble = { id: uid(), role: 'assistant', content: '', streaming: true, agent: '', tools: [] }
  messages.value.push(pendingBubble)
  // 必须拿数组里的响应式代理再改，直接改 push 进去的原始对象不会触发渲染
  pendingBubble = messages.value[messages.value.length - 1]
  const isNewThread = !currentThreadId.value
  sending.value = true
  await scrollToBottom()

  function ensureBubble(agent, key) {
    let bubble = bubbleMap.get(key)
    if (bubble) return bubble
    if (pendingBubble && !pendingBubble.content) {
      bubble = pendingBubble
      pendingBubble = null
    } else {
      bubble = { id: uid(), role: 'assistant', content: '', streaming: true, agent: '', tools: [] }
      messages.value.push(bubble)
      bubble = messages.value[messages.value.length - 1]
    }
    bubble.agent = normalizeAgent(agent)
    bubbleMap.set(key, bubble)
    return bubble
  }

  function removeEmptyBubbles() {
    messages.value = messages.value.filter(
      (m) => !(m.role === 'assistant' && !m.content && !m.error)
    )
  }

  function stopStreaming(removeEmpty = true) {
    for (const m of messages.value) {
      if (m.role === 'assistant') {
        m.streaming = false
        m.status = ''
      }
    }
    if (removeEmpty) removeEmptyBubbles()
  }

  try {
    await streamChat(
      { userId: userId.value, message: text, threadId: currentThreadId.value || null },
      {
        onMeta: (data) => {
          if (!data?.thread_id) return
          currentThreadId.value = data.thread_id
          if (isNewThread) threadStore.setTitle(data.thread_id, text)
        },
        onStatus: (data) => {
          if (!data?.text) return
          const key = data.agent || data.node || 'assistant'
          const bubble = ensureBubble(data.agent || data.node || '', key)
          bubble.status = data.text
          scrollToBottom()
        },
        onToken: (data) => {
          if (!data?.content) return
          // 旧后端会把 handler 回传的 HumanMessage 当成 token 发出来
          if (!data.agent && data.node === 'handler') return
          // 最终答案由后端 node=final 分帧，token 到达即追加
          const isFinal = data.node === 'final'
          const key = isFinal ? 'final' : data.agent || data.node || 'assistant'
          const bubble = ensureBubble(isFinal ? '' : data.agent || data.node || '', key)
          bubble.status = ''
          bubble.content += data.content
          scrollToBottom()
        },
        onThreadSwitch: (data) => {
          if (!data?.thread_id) return
          currentThreadId.value = data.thread_id
          clearTyping()
          // 后端换了新 thread 重跑，旧会话的乐观消息不该留在新窗口
          for (const m of messages.value) {
            if (m.role === 'assistant') m.streaming = false
          }
          messages.value = [userBubble]
          bubbleMap.clear()
          pendingBubble = { id: uid(), role: 'assistant', content: '', streaming: true, agent: '', tools: [] }
          messages.value.push(pendingBubble)
          pendingBubble = messages.value[messages.value.length - 1]
          if (data.thread_title) threadStore.rename(data.thread_id, data.thread_title)
        },
        onInterrupt: (data) => {
          flushTyping()
          stopStreaming(true)
          interrupt.value = extractInterrupt(data?.interrupt)
        },
        onDone: (data) => {
          // 最终答案已由 node=final 的 token 追加，这里只收尾
          const finalBubble = bubbleMap.get('final')
          if (finalBubble) finalBubble.agent = normalizeAgent(data?.metadata?.from_)
          else if (String(data?.output_msg || '').trim()) {
            messages.value.push({
              id: uid(),
              role: 'assistant',
              content: String(data.output_msg),
              agent: normalizeAgent(data?.metadata?.from_),
              tools: [],
            })
          }
          stopStreaming(true)
          if (data?.project_dir) projectDir.value = data.project_dir
          if (data?.thread_title && currentThreadId.value) {
            threadStore.rename(currentThreadId.value, data.thread_title)
          }
        },
        onError: (data) => {
          flushTyping()
          stopStreaming(false)
          let bubble = [...messages.value].reverse().find((m) => m.role === 'assistant')
          if (!bubble) {
            bubble = { id: uid(), role: 'assistant', content: '', streaming: false, agent: '', tools: [] }
            messages.value.push(bubble)
            bubble = messages.value[messages.value.length - 1]
          }
          bubble.streaming = false
          bubble.error = true
          bubble.content = data?.detail || '对话失败'
        },
      }
    )
  } catch (e) {
    flushTyping()
    stopStreaming(false)
    let bubble = [...messages.value].reverse().find((m) => m.role === 'assistant')
    if (!bubble) {
      bubble = { id: uid(), role: 'assistant', content: '', streaming: false, agent: '', tools: [] }
      messages.value.push(bubble)
      bubble = messages.value[messages.value.length - 1]
    }
    bubble.streaming = false
    bubble.error = true
    bubble.content = e?.message || '对话失败'
  } finally {
    // 打字机还在跑时不要收尾，等它逐字显示完自己把 streaming 置 false
    if (!typeTimer) stopStreaming(true)
    sending.value = false
    await threadStore.load(tab.value)
    await scrollToBottom()
  }
}

async function submitAnswer(answer) {
  if (!currentThreadId.value || sending.value) return
  clearTyping()
  interrupt.value = null
  sending.value = true
  error.value = ''
  try {
    const data = await resumeApi({
      userId: userId.value,
      threadId: currentThreadId.value,
      answer,
    })
    if (data?.project_dir) projectDir.value = data.project_dir
    if (data?.thread_id) currentThreadId.value = data.thread_id
    // 不再用 output_msg 造气泡：停在 interrupt 时它还是上一轮的值。
    // 统一从 checkpoint 重读，本轮已写入 messages 的最终回复/确认结果直接显示。
    await loadHistory(currentThreadId.value)
    if (data?.interrupted) {
      interrupt.value = extractInterrupt(data.interrupt) || interrupt.value
    }
  } catch (e) {
    error.value = e?.message || '提交失败'
  } finally {
    sending.value = false
    await threadStore.load(tab.value)
    await scrollToBottom()
  }
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

  // 新后端给的是绝对路径；旧消息/mock 仍是项目相对路径，这里兜底拼 projectDir
  const isWindowsAbs = /^[a-zA-Z]:[\\/]/.test(filePath) || filePath.startsWith('\\\\')
  const isPosixAbs = filePath.startsWith('/') && projectDir.value.startsWith('/')
  const fullPath = isWindowsAbs || isPosixAbs || !projectDir.value
    ? filePath
    : `${projectDir.value.replace(/[\\/]+$/, '')}/${filePath.replace(/^[\\/]+/, '')}`
  console.info('[IDE] open', { filePath: fullPath, ide: currentIde.value, line: start })
  try {
    await openFile({ filePath: fullPath, ideName: currentIde.value, startIdx: Number(start) })
  } catch (e) {
    console.error('[IDE] open failed', e)
    error.value = e?.message || '打开文件失败'
  }
}

async function openProject() {
  if (!projectDir.value) return
  try {
    await openFile({ filePath: projectDir.value, ideName: currentIde.value })
  } catch (e) {
    error.value = e?.message || '打开项目失败'
  }
}

async function onRename(threadId, nextTitle) {
  if (!nextTitle) return
  try {
    await threadStore.rename(threadId, nextTitle)
  } catch (e) {
    error.value = e?.message || '重命名失败'
  }
}

async function onPin(threadId) {
  try {
    await threadStore.togglePin(threadId)
  } catch (e) {
    error.value = e?.message || '置顶失败'
  }
}

async function onArchive(threadId) {
  try {
    await threadStore.archive(threadId)
    if (currentThreadId.value === threadId) newChat()
  } catch (e) {
    error.value = e?.message || '归档失败'
  }
}

async function onRemove(threadId) {
  if (!window.confirm('删除后会话记录不可恢复，确定删除？')) return
  try {
    await threadStore.remove(threadId)
    if (currentThreadId.value === threadId) newChat()
  } catch (e) {
    error.value = e?.message || '删除失败'
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
    error.value = e?.message || '批量归档失败'
  }
}

async function batchDelete() {
  if (!selectedIds.value.length) return
  if (!window.confirm(`确定删除选中的 ${selectedIds.value.length} 个会话？不可恢复。`)) return
  try {
    await deleteThreads([...selectedIds.value])
    selectedIds.value = []
    multiSelect.value = false
    await threadStore.loadAll()
  } catch (e) {
    error.value = e?.message || '批量删除失败'
  }
}

function onKeydown(event) {
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

watch(input, () => nextTick(autoGrow))

function logout() {
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

        <button
          class="ghost small"
          :disabled="!projectDir"
          :title="projectDir || '当前会话没有关联项目'"
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
          <MessageList
            ref="listRef"
            :messages="messages"
            :read-only="readOnly"
            :interrupt="readOnly ? null : interrupt"
            :elapsed="sendingSeconds"
            @open-ide="openIdeLink"
            @submit="submitAnswer"
          />

          <p v-if="error" class="error bar">{{ error }}</p>
          <p v-if="paused && !interrupt" class="error bar">该会话停在人工确认，但当前无法恢复确认面板；请新建对话或回到原窗口完成确认。</p>

          <footer v-if="!readOnly" class="composer">
            <textarea
              ref="inputRef"
              v-model="input"
              rows="1"
              :disabled="sending || !!interrupt || paused"
              placeholder="输入消息，Enter 发送，Shift + Enter 换行"
              @keydown="onKeydown"
            ></textarea>
            <button class="primary" :disabled="!canSend" @click="send">
              {{ sending ? '处理中…' : '发送' }}
            </button>
          </footer>
        </div>

        <BackgroundPanel v-if="showBackground" :thread-id="currentThreadId" />
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

.bar {
  margin: 0;
  padding: 8px 24px;
  font-size: 12px;
  border-top: 1px solid var(--border);
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
