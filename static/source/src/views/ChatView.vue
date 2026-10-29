<script setup>
import { computed, nextTick, onMounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { useUserStore } from '@/stores/user'
import { useThreadStore } from '@/stores/threads'
import { resume as resumeApi, streamChat } from '@/api/chat'
import { archiveThreads, deleteThreads, getHistory } from '@/api/threads'
import { getIdes, openFile } from '@/api/openApp'
import BackgroundPanel from '@/components/BackgroundPanel.vue'
import InterruptPanel from '@/components/InterruptPanel.vue'
import MessageList from '@/components/MessageList.vue'
import ThreadItem from '@/components/ThreadItem.vue'

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

onMounted(async () => {
  await threadStore.loadAll()
  await loadIdes()
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
      currentIde.value = keys.includes('notepad') ? 'notepad' : keys[0]
    }
  } catch (e) {
    error.value = e?.message || '获取 IDE 列表失败'
  }
}

function newChat() {
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

async function selectThread(threadId) {
  if (sending.value) return
  currentThreadId.value = threadId
  interrupt.value = null
  paused.value = false
  projectDir.value = ''
  error.value = ''
  messages.value = []
  try {
    const data = await getHistory(threadId)
    paused.value = Boolean(data?.manual_intervention)
    projectDir.value = data?.project_dir || ''
    const rendered = []
    for (const m of data?.messages || []) {
      const type = String(m.type || '')
      const content = String(m.content ?? '')
      if (type.includes('Tool')) {
        const last = rendered[rendered.length - 1]
        if (last) {
          last.tools.push(content)
          continue
        }
      }
      rendered.push({
        role: type.includes('Human') ? 'user' : 'assistant',
        content,
        tools: [],
      })
    }
    messages.value = rendered
    if (!threadStore.titles[threadId]) {
      const firstUser = rendered.find((m) => m.role === 'user')
      if (firstUser) threadStore.setTitle(threadId, firstUser.content)
    }
  } catch (e) {
    error.value = e?.message || '加载会话失败'
  }
  await scrollToBottom()
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

async function send() {
  const text = input.value.trim()
  if (!text || sending.value || readOnly.value || interrupt.value) return

  input.value = ''
  error.value = ''
  messages.value.push({ role: 'user', content: text })
  const userBubble = messages.value[messages.value.length - 1]
  messages.value.push({ role: 'assistant', content: '', streaming: true })
  const bubble = messages.value[messages.value.length - 1]
  const isNewThread = !currentThreadId.value
  sending.value = true
  await scrollToBottom()

  try {
    await streamChat(
      { userId: userId.value, message: text, threadId: currentThreadId.value || null },
      {
        onMeta: (data) => {
          if (!data?.thread_id) return
          currentThreadId.value = data.thread_id
          if (isNewThread) threadStore.setTitle(data.thread_id, text)
        },
        onToken: (data) => {
          if (!data?.content) return
          bubble.content += data.content
          scrollToBottom()
        },
        onThreadSwitch: (data) => {
          if (!data?.thread_id) return
          currentThreadId.value = data.thread_id
          // 后端换了新 thread 重跑，旧会话的乐观消息不该留在新窗口
          messages.value = [userBubble, bubble]
          if (data.thread_title) threadStore.rename(data.thread_id, data.thread_title)
        },
        onInterrupt: (data) => {
          interrupt.value = extractInterrupt(data?.interrupt)
        },
        onDone: (data) => {
          bubble.streaming = false
          bubble.content = data?.output_msg || bubble.content || '（无输出）'
          if (data?.project_dir) projectDir.value = data.project_dir
          if (data?.thread_title && currentThreadId.value) {
            threadStore.rename(currentThreadId.value, data.thread_title)
          }
        },
        onError: (data) => {
          bubble.streaming = false
          bubble.error = true
          bubble.content = data?.detail || '对话失败'
        },
      }
    )
  } catch (e) {
    bubble.streaming = false
    bubble.error = true
    bubble.content = e?.message || '对话失败'
  } finally {
    sending.value = false
    await threadStore.load(tab.value)
    await scrollToBottom()
  }
}

async function submitAnswer(answer) {
  if (!currentThreadId.value) return
  interrupt.value = null
  sending.value = true
  error.value = ''
  messages.value.push({ role: 'assistant', content: '', streaming: true })
  const bubble = messages.value[messages.value.length - 1]
  try {
    const data = await resumeApi({
      userId: userId.value,
      threadId: currentThreadId.value,
      answer,
    })
    bubble.streaming = false
    bubble.content = data?.output_msg || '（无输出）'
    if (data?.interrupted) interrupt.value = extractInterrupt(data.interrupt)
    if (data?.project_dir) projectDir.value = data.project_dir
    if (data?.thread_id) currentThreadId.value = data.thread_id
  } catch (e) {
    bubble.streaming = false
    bubble.error = true
    bubble.content = e?.message || '提交失败'
  } finally {
    sending.value = false
    await threadStore.load(tab.value)
    await scrollToBottom()
  }
}

async function openIdeLink(href) {
  const raw = String(href || '').replace(/^IDE:\/\//, '')
  const [filePath, hash] = raw.split('#')
  const start = /^L(\d+)/.exec(hash || '')?.[1] || 1
  if (!filePath) return
  try {
    await openFile({ filePath: decodeURIComponent(filePath), ideName: currentIde.value, startIdx: Number(start) })
  } catch (e) {
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
  if (nextTitle) threadStore.rename(threadId, nextTitle)
}

function onPin(threadId) {
  threadStore.togglePin(threadId)
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
          <MessageList ref="listRef" :messages="messages" :read-only="readOnly" @open-ide="openIdeLink" />

          <p v-if="error" class="error bar">{{ error }}</p>
          <p v-if="paused" class="error bar">该会话停在人工确认，无法直接继续；请从原窗口完成确认，或新建对话。</p>

          <InterruptPanel v-if="interrupt" :interrupt="interrupt" @submit="submitAnswer" />

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
