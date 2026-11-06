<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { useUserStore } from '@/stores/user'
import JsonView from '@/components/JsonView.vue'
import JobRestartReview from '@/components/JobRestartReview.vue'
import {
  createTicket,
  getAgentHistory,
  listAgentRuns,
  listCheckerErrors,
  openPerceptionSocket,
  resumeAgent,
} from '@/api/ops'

const router = useRouter()
const user = useUserStore()

const canView = computed(() => Number(user.permission ?? 0) >= 1)
const tab = ref('errors')
const limit = 20
const TOAST_STORAGE_KEY = 'ops-perception-toasts'
const TOAST_LIFETIME = 60000

const errors = ref([])
const errorTotal = ref(0)
const errorOffset = ref(0)
const errorLoading = ref(false)
const errorMsg = ref('')
const filters = ref({
  app_type: 'ALL',
  status: 'PENDING',
  is_checker_error: false,
  is_solved: false,
  app_id: '',
})
const selectedError = ref(null)

const runs = ref([])
const runTotal = ref(0)
const runOffset = ref(0)
const runLoading = ref(false)
const runMsg = ref('')
const runStatus = ref('ALL')

const detailWidth = ref(380)
const toasts = ref([])
const toastTimers = new Map()
const wsState = ref('connecting')
let socket = null
let retryTimer = null
let reloadTimer = null
let filterTimer = null
let disposed = false

const historyOpen = ref(false)
const historyLoading = ref(false)
const historyError = ref('')
const historyData = ref(null)
const review = ref(null)
const reviewBusy = ref(false)
const reviewError = ref('')
const reviewNotice = ref('')

const ERROR_COLUMNS = [
  { key: 'id', label: 'ID', width: 56 },
  { key: 'app_id', label: '应用', width: 230 },
  { key: 'app_type', label: '类型', width: 78 },
  { key: 'check_type', label: '检查项', width: 100 },
  { key: 'error_type', label: '级别', width: 76 },
  { key: 'check_describe', label: '描述', width: 300 },
  { key: 'repeat_count', label: '重复', width: 56 },
  { key: 'status', label: '状态', width: 90 },
  { key: 'create_time', label: '时间', width: 168 },
  { key: 'actions', label: '操作', width: 72 },
]
const RUN_COLUMNS = [
  { key: 'app_id', label: '应用', width: 230 },
  { key: 'session_id', label: 'Session', width: 270 },
  { key: 'status', label: '状态', width: 96 },
  { key: 'app_type', label: '类型', width: 80 },
  { key: 'error_count', label: '错误数', width: 76 },
  { key: 'started_at', label: '开始时间', width: 170 },
  { key: 'finished_at', label: '结束时间', width: 170 },
  { key: 'actions', label: '操作', width: 90 },
]
const errorWidths = ref(Object.fromEntries(ERROR_COLUMNS.map((column) => [column.key, column.width])))
const runWidths = ref(Object.fromEntries(RUN_COLUMNS.map((column) => [column.key, column.width])))
const errorMinWidth = computed(() => ERROR_COLUMNS.reduce(
  (sum, column) => sum + (errorWidths.value[column.key] || column.width), 0,
))
const runMinWidth = computed(() => RUN_COLUMNS.reduce(
  (sum, column) => sum + (runWidths.value[column.key] || column.width), 0,
))

const loading = computed(() => errorLoading.value || runLoading.value)
const wsText = computed(() => {
  if (wsState.value === 'open') return '实时已连接'
  if (wsState.value === 'connecting') return '实时连接中'
  return '实时已断开'
})
const errorPage = computed(() => Math.floor(errorOffset.value / limit) + 1)
const errorPages = computed(() => Math.max(1, Math.ceil(errorTotal.value / limit)))
const runPage = computed(() => Math.floor(runOffset.value / limit) + 1)
const runPages = computed(() => Math.max(1, Math.ceil(runTotal.value / limit)))

function fmt(value) {
  if (!value) return '-'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString()
}

function appType(value) {
  return value || 'CLUSTER'
}

function runFor(appId) {
  return runs.value.find((item) => item.app_id === appId) || null
}

function hasCheckData(row) {
  const value = row?.check_data
  if (value === null || value === undefined) return false
  if (typeof value === 'object') return Object.keys(value).length > 0
  return String(value).trim() !== ''
}

function liveDesc(item) {
  const meta = item?.metadata || {}
  let text = ''
  if (meta.check_describe) text = meta.check_describe
  else if (meta.error) text = meta.error
  else if (meta.check_data) text = JSON.stringify(meta.check_data)
  if (text.length > 180) text = `${text.slice(0, 180)}…`
  return text || '收到一条新的错误通知'
}

function parseJson(value) {
  if (typeof value !== 'string') return null
  const text = value.trim()
  if (!text.startsWith('{') && !text.startsWith('[')) return null
  try {
    return JSON.parse(text)
  } catch {
    return null
  }
}

async function loadErrors() {
  errorLoading.value = true
  errorMsg.value = ''
  try {
    const data = await listCheckerErrors({
      app_type: filters.value.app_type,
      status: filters.value.status,
      is_checker_error: filters.value.is_checker_error,
      is_solved: filters.value.is_solved,
      app_id: filters.value.app_id,
      limit,
      offset: errorOffset.value,
      order_by: 'create_time',
      desc_flag: true,
    })
    errors.value = data?.records || []
    errorTotal.value = data?.total_count ?? data?.total ?? 0
  } catch (e) {
    errorMsg.value = e?.message || '加载错误列表失败'
    errors.value = []
    errorTotal.value = 0
  } finally {
    errorLoading.value = false
  }
}

async function loadRuns() {
  runLoading.value = true
  runMsg.value = ''
  try {
    const data = await listAgentRuns({ status: runStatus.value, limit, offset: runOffset.value })
    runs.value = data?.records || []
    runTotal.value = data?.total_count ?? data?.total ?? 0
  } catch (e) {
    runMsg.value = e?.message || '加载 Agent 列表失败'
    runs.value = []
    runTotal.value = 0
  } finally {
    runLoading.value = false
  }
}

function loadAll() {
  loadErrors()
  loadRuns()
}

function scheduleReload() {
  clearTimeout(reloadTimer)
  reloadTimer = setTimeout(loadAll, 800)
}

function saveToasts() {
  try {
    sessionStorage.setItem(TOAST_STORAGE_KEY, JSON.stringify(toasts.value))
  } catch {
    // 隐私模式/存储满：忽略
  }
}

function restoreToasts() {
  try {
    const raw = sessionStorage.getItem(TOAST_STORAGE_KEY)
    if (!raw) return
    const now = Date.now()
    const seen = new Set()
    toasts.value = JSON.parse(raw)
      .filter((toast) => toast && toast.expiresAt > now)
      .filter((toast) => {
        const key = toast.key || toast.id
        if (seen.has(key)) return false
        seen.add(key)
        return true
      })
      .map((toast) => ({
        ...toast,
        createdAt: toast.createdAt || toast.expiresAt - TOAST_LIFETIME,
      }))
    for (const toast of toasts.value) {
      const remaining = Math.max(0, toast.expiresAt - now)
      toastTimers.set(toast.id, setTimeout(() => removeToast(toast.id), remaining))
    }
  } catch {
    toasts.value = []
  }
}

function removeToast(id) {
  const timer = toastTimers.get(id)
  if (timer) clearTimeout(timer)
  toastTimers.delete(id)
  toasts.value = toasts.value.filter((toast) => toast.id !== id)
  saveToasts()
}

function pushToast(record) {
  const meta = record?.metadata || {}
  const key = meta.dedup_key
    || `${meta.app_id || 'cluster'}:${meta.check_type || ''}:${meta.check_describe || ''}`
  const now = Date.now()
  const existingIndex = toasts.value.findIndex((toast) => toast.key === key)
  if (existingIndex >= 0) {
    // 同一条错误重复推送：只刷新已有弹窗，不叠新的
    const old = toasts.value[existingIndex]
    const oldTimer = toastTimers.get(old.id)
    if (oldTimer) clearTimeout(oldTimer)
    toastTimers.delete(old.id)
    const id = `${now}-${Math.random().toString(16).slice(2)}`
    toasts.value.splice(existingIndex, 1, {
      ...old,
      id,
      desc: liveDesc(record),
      createdAt: now,
      expiresAt: now + TOAST_LIFETIME,
    })
    toastTimers.set(id, setTimeout(() => removeToast(id), TOAST_LIFETIME))
    saveToasts()
    return
  }
  const id = `${now}-${Math.random().toString(16).slice(2)}`
  toasts.value.push({
    id,
    key,
    title: meta.check_type || '新错误',
    app: meta.app_id || '集群级',
    desc: liveDesc(record),
    createdAt: now,
    expiresAt: now + TOAST_LIFETIME,
  })
  while (toasts.value.length > 4) {
    const dropped = toasts.value.shift()
    const timer = toastTimers.get(dropped.id)
    if (timer) clearTimeout(timer)
    toastTimers.delete(dropped.id)
  }
  toastTimers.set(id, setTimeout(() => removeToast(id), TOAST_LIFETIME))
  saveToasts()
}

async function connectWs() {
  if (disposed) return
  wsState.value = 'connecting'
  try {
    const data = await createTicket()
    if (!data?.ticket) throw new Error('生成 WebSocket ticket 失败')
    const ws = openPerceptionSocket(data.ticket)
    socket = ws
    ws.onopen = () => {
      wsState.value = 'open'
    }
    ws.onmessage = (event) => {
      try {
        const record = JSON.parse(event.data)
        pushToast(record)
        scheduleReload()
      } catch {
        // 非 JSON 消息直接忽略
      }
    }
    ws.onclose = () => {
      wsState.value = 'closed'
      if (!disposed) retryTimer = setTimeout(connectWs, 5000)
    }
    ws.onerror = () => {
      wsState.value = 'closed'
    }
  } catch {
    wsState.value = 'closed'
    if (!disposed) retryTimer = setTimeout(connectWs, 5000)
  }
}

function extractReview(payload) {
  const list = Array.isArray(payload) ? payload : payload ? [payload] : []
  const first = list[0]
  const value = first?.value ?? first
  if (!value || typeof value !== 'object') return null
  if (value.kind !== 'job_restart' || typeof value.command !== 'string') return null
  return value
}

async function openHistory(sessionId) {
  if (!sessionId) return
  historyOpen.value = true
  historyLoading.value = true
  historyError.value = ''
  historyData.value = null
  review.value = null
  reviewError.value = ''
  reviewNotice.value = ''
  try {
    const data = await getAgentHistory(sessionId)
    historyData.value = data
    review.value = extractReview(data?.interrupt)
  } catch (e) {
    historyError.value = e?.message || '加载历史失败'
  } finally {
    historyLoading.value = false
  }
}

async function submitReview(answer) {
  const sid = historyData.value?.thread_id
  if (!sid || reviewBusy.value) return
  reviewBusy.value = true
  reviewError.value = ''
  try {
    const data = await resumeAgent({ sessionId: sid, answer })
    const ignored = Array.isArray(data?.ignored_values) ? data.ignored_values : []
    await openHistory(sid)
    await loadRuns()
    if (ignored.length) {
      reviewNotice.value = '以下值不合法，已保留原值：' + ignored
        .map((item) => `${item.flag || item.key || ''} ${item.value || ''}（${item.reason || '不合法'}）`)
        .join('；')
    }
  } catch (e) {
    reviewError.value = e?.message || '提交审阅结果失败'
  } finally {
    reviewBusy.value = false
  }
}

function startDetailResize(event) {
  const startX = event.clientX
  const startWidth = detailWidth.value
  const onMove = (moveEvent) => {
    const max = Math.max(280, Math.min(900, window.innerWidth - 420))
    detailWidth.value = Math.min(max, Math.max(260, startWidth - (moveEvent.clientX - startX)))
  }
  const stop = () => {
    window.removeEventListener('mousemove', onMove)
    window.removeEventListener('mouseup', stop)
    document.body.style.cursor = ''
    document.body.style.userSelect = ''
  }
  window.addEventListener('mousemove', onMove)
  window.addEventListener('mouseup', stop)
  document.body.style.cursor = 'col-resize'
  document.body.style.userSelect = 'none'
}

function startColResize(widths, key, event) {
  event.preventDefault()
  // 模板里 ref 会被自动解包；这里兼容直接传 ref 的情况
  const target = widths?.value ?? widths
  const startX = event.clientX
  const startWidth = target[key] || 60
  const onMove = (moveEvent) => {
    target[key] = Math.max(48, startWidth + moveEvent.clientX - startX)
  }
  const stop = () => {
    window.removeEventListener('mousemove', onMove)
    window.removeEventListener('mouseup', stop)
    document.body.style.cursor = ''
    document.body.style.userSelect = ''
  }
  window.addEventListener('mousemove', onMove)
  window.addEventListener('mouseup', stop)
  document.body.style.cursor = 'col-resize'
  document.body.style.userSelect = 'none'
}

function prevErrors() {
  if (errorOffset.value <= 0) return
  errorOffset.value = Math.max(0, errorOffset.value - limit)
  loadErrors()
}
function nextErrors() {
  if (errorOffset.value + limit >= errorTotal.value) return
  errorOffset.value += limit
  loadErrors()
}
function prevRuns() {
  if (runOffset.value <= 0) return
  runOffset.value = Math.max(0, runOffset.value - limit)
  loadRuns()
}
function nextRuns() {
  if (runOffset.value + limit >= runTotal.value) return
  runOffset.value += limit
  loadRuns()
}

watch(
  filters,
  () => {
    clearTimeout(filterTimer)
    filterTimer = setTimeout(() => {
      errorOffset.value = 0
      loadErrors()
    }, 300)
  },
  { deep: true },
)

watch(runStatus, () => {
  runOffset.value = 0
  loadRuns()
})

onMounted(() => {
  restoreToasts()
  loadAll()
  connectWs()
})

onBeforeUnmount(() => {
  disposed = true
  clearTimeout(retryTimer)
  clearTimeout(reloadTimer)
  clearTimeout(filterTimer)
  for (const timer of toastTimers.values()) clearTimeout(timer)
  toastTimers.clear()
  socket?.close()
})
</script>

<template>
  <div class="ops">
    <header class="ops-head">
      <div class="head-left">
        <button class="ghost" @click="router.push('/')">← 返回对话</button>
        <h1>运维看板</h1>
        <span class="muted">{{ user.username }}</span>
      </div>
      <div class="head-right">
        <span class="ws-state" :class="wsState">{{ wsText }}</span>
        <button class="ghost" :disabled="loading" @click="loadAll">刷新</button>
        <button class="ghost" @click="router.push('/admin')">用户管理</button>
      </div>
    </header>

    <main v-if="canView" class="ops-body">
      <div class="main-column">
        <nav class="tabs">
          <button :class="['tab', { active: tab === 'errors' }]" @click="tab = 'errors'">
            错误列表
            <span class="count">{{ errorTotal }}</span>
          </button>
          <button :class="['tab', { active: tab === 'runs' }]" @click="tab = 'runs'">
            Agent 记录
            <span class="count">{{ runTotal }}</span>
          </button>
        </nav>

        <section v-if="tab === 'errors'" class="panel">
          <div class="filters">
            <label>
              类型
              <select v-model="filters.app_type">
                <option value="ALL">全部</option>
                <option value="SPARK">SPARK</option>
                <option value="FLINK">FLINK</option>
                <option value="CLUSTER">集群级</option>
              </select>
            </label>
            <label>
              状态
              <select v-model="filters.status">
                <option value="PENDING">PENDING</option>
                <option value="PROCESSING">PROCESSING</option>
                <option value="DONE">DONE</option>
                <option value="ESCALATED">ESCALATED</option>
                <option value="FAILED">FAILED</option>
              </select>
            </label>
            <label>
              解决
              <select v-model="filters.is_solved">
                <option :value="false">未解决</option>
                <option :value="true">已解决</option>
              </select>
            </label>
            <label class="check">
              <input v-model="filters.is_checker_error" type="checkbox" />
              只看检查器自身错误
            </label>
            <label class="grow">
              应用 ID
              <input v-model.trim="filters.app_id" placeholder="application_...（留空为全部）" />
            </label>
          </div>

          <p v-if="errorMsg" class="error bar">{{ errorMsg }}</p>
          <div class="table-wrap">
            <table class="data-table" :style="{ minWidth: `${errorMinWidth}px` }">
              <colgroup>
                <col
                  v-for="column in ERROR_COLUMNS"
                  :key="column.key"
                  :style="{ width: `${errorWidths[column.key]}px` }"
                />
              </colgroup>
              <thead>
                <tr>
                  <th v-for="column in ERROR_COLUMNS" :key="column.key">
                    {{ column.label }}
                    <span
                      class="col-resizer"
                      @mousedown.stop.prevent="startColResize(errorWidths, column.key, $event)"
                    ></span>
                  </th>
                </tr>
              </thead>
              <tbody>
                <tr
                  v-for="row in errors"
                  :key="row.id"
                  class="clickable"
                  :class="{ selected: selectedError?.id === row.id }"
                  @click="selectedError = row"
                >
                  <td>{{ row.id }}</td>
                  <td class="mono">{{ row.app_id || '—' }}</td>
                  <td>{{ appType(row.app_type) }}</td>
                  <td>{{ row.check_type || '-' }}</td>
                  <td>
                    <span :class="['level', row.error_type]">{{ row.error_type || '-' }}</span>
                  </td>
                  <td class="desc" :title="row.check_describe || ''">
                    {{ row.checker_error || row.check_describe || '-' }}
                  </td>
                  <td>{{ row.repeat_count ?? '-' }}</td>
                  <td>{{ row.status }}</td>
                  <td class="nowrap">{{ fmt(row.create_time) }}</td>
                  <td>
                    <button
                      v-if="runFor(row.app_id)"
                      class="ghost tiny"
                      @click.stop="openHistory(runFor(row.app_id).session_id)"
                    >
                      历史
                    </button>
                    <span v-else class="muted">—</span>
                  </td>
                </tr>
                <tr v-if="!errorLoading && !errors.length">
                  <td :colspan="ERROR_COLUMNS.length" class="empty">没有记录</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div class="pager">
            <span class="muted">第 {{ errorPage }} / {{ errorPages }} 页 · 共 {{ errorTotal }} 条</span>
            <div>
              <button class="ghost tiny" :disabled="errorOffset <= 0" @click="prevErrors">上一页</button>
              <button
                class="ghost tiny"
                :disabled="errorOffset + limit >= errorTotal"
                @click="nextErrors"
              >
                下一页
              </button>
            </div>
          </div>
        </section>

        <section v-else class="panel">
          <div class="filters">
            <label>
              状态
              <select v-model="runStatus">
                <option value="ALL">全部</option>
                <option value="RUNNING">RUNNING</option>
                <option value="PAUSED">PAUSED</option>
                <option value="DONE">DONE</option>
                <option value="ERROR">ERROR</option>
                <option value="ABANDONED">ABANDONED</option>
              </select>
            </label>
          </div>
          <p v-if="runMsg" class="error bar">{{ runMsg }}</p>
          <div class="table-wrap">
            <table class="data-table" :style="{ minWidth: `${runMinWidth}px` }">
              <colgroup>
                <col
                  v-for="column in RUN_COLUMNS"
                  :key="column.key"
                  :style="{ width: `${runWidths[column.key]}px` }"
                />
              </colgroup>
              <thead>
                <tr>
                  <th v-for="column in RUN_COLUMNS" :key="column.key">
                    {{ column.label }}
                    <span
                      class="col-resizer"
                      @mousedown.stop.prevent="startColResize(runWidths, column.key, $event)"
                    ></span>
                  </th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="row in runs" :key="row.session_id">
                  <td class="mono">{{ row.app_id || '（集群级）' }}</td>
                  <td class="mono">{{ row.session_id }}</td>
                  <td>
                    <span :class="['run-status', row.status]">{{ row.status }}</span>
                  </td>
                  <td>{{ row.app_type || '—' }}</td>
                  <td>{{ row.error_count ?? '-' }}</td>
                  <td class="nowrap">{{ fmt(row.started_at) }}</td>
                  <td class="nowrap">{{ fmt(row.finished_at) }}</td>
                  <td>
                    <button class="ghost tiny" @click="openHistory(row.session_id)">查看历史</button>
                  </td>
                </tr>
                <tr v-if="!runLoading && !runs.length">
                  <td :colspan="RUN_COLUMNS.length" class="empty">暂无 Agent 运行记录</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div class="pager">
            <span class="muted">第 {{ runPage }} / {{ runPages }} 页 · 共 {{ runTotal }} 条</span>
            <div>
              <button class="ghost tiny" :disabled="runOffset <= 0" @click="prevRuns">上一页</button>
              <button
                class="ghost tiny"
                :disabled="runOffset + limit >= runTotal"
                @click="nextRuns"
              >
                下一页
              </button>
            </div>
          </div>
        </section>
      </div>

      <div
        class="detail-splitter"
        role="separator"
        aria-orientation="vertical"
        title="拖动调整宽度"
        @mousedown.prevent="startDetailResize"
      ></div>

      <aside class="detail-panel" :style="{ width: `${detailWidth}px` }">
        <div class="detail-head">
          <h2>检查数据</h2>
          <button v-if="selectedError" class="ghost tiny" @click="selectedError = null">清空</button>
        </div>
        <template v-if="selectedError">
          <div class="detail-meta">
            <span class="mono">{{ selectedError.app_id || '集群级' }}</span>
            <span>{{ selectedError.check_type || '-' }}</span>
            <span :class="['level', selectedError.error_type]">{{ selectedError.error_type || '-' }}</span>
          </div>
          <p v-if="selectedError.checker_error" class="error detail-desc">
            {{ selectedError.checker_error }}
          </p>
          <p v-else-if="selectedError.check_describe" class="muted detail-desc">
            {{ selectedError.check_describe }}
          </p>
          <div class="json-box">
            <JsonView v-if="hasCheckData(selectedError)" :data="selectedError.check_data" />
            <p v-else class="muted">这条错误没有 check_data（可能来自检查器自身错误）。</p>
          </div>
          <button
            v-if="runFor(selectedError.app_id)"
            class="ghost"
            @click="openHistory(runFor(selectedError.app_id).session_id)"
          >
            查看 Agent 历史
          </button>
        </template>
        <p v-else class="muted empty-detail">点击左侧错误行查看 check_data</p>
      </aside>
    </main>

    <main v-else class="ops-body">
      <p class="error">权限不足，无法查看运维看板。</p>
    </main>

    <div class="toast-stack">
      <div v-for="toast in toasts" :key="toast.id" class="toast">
        <button class="toast-close" title="关闭" @click="removeToast(toast.id)">×</button>
        <div class="toast-title">{{ toast.title }}</div>
        <div class="mono small">{{ toast.app }}</div>
        <p>{{ toast.desc }}</p>
        <div
          class="toast-progress"
          :style="{ animationDelay: `-${Math.min(60000, Math.max(0, Date.now() - toast.createdAt))}ms` }"
        ></div>
      </div>
    </div>

    <div v-if="historyOpen" class="drawer-mask" @click.self="historyOpen = false">
      <aside class="drawer">
        <header class="drawer-head">
          <div class="drawer-title">
            <h2>Agent 历史</h2>
            <span v-if="historyData" class="muted small">
              session: {{ historyData.thread_id }} · app: {{ historyData.app_id || '—' }} ·
              {{ appType(historyData.app_type) }}
            </span>
          </div>
          <button class="ghost close-btn" @click="historyOpen = false">关闭</button>
        </header>
        <p v-if="historyLoading" class="muted">加载中…</p>
        <p v-else-if="historyError" class="error">{{ historyError }}</p>
        <template v-else-if="historyData">
          <p v-if="historyData.out_msg" class="out-msg">{{ historyData.out_msg }}</p>
          <p v-if="historyData.error_desc_text" class="error">{{ historyData.error_desc_text }}</p>
          <p v-if="reviewNotice" class="bar notice">{{ reviewNotice }}</p>
          <JobRestartReview
            v-if="review"
            :review="review"
            :busy="reviewBusy"
            :error="reviewError"
            @submit="submitReview"
          />
          <p v-else-if="historyData.interrupted" class="error bar">
            有待确认的中断，但当前看板不支持该类型；请回到发起该任务的会话处理。
          </p>
          <div v-if="!historyData.messages.length" class="muted">没有消息</div>
          <div class="messages">
            <article
              v-for="(message, index) in historyData.messages"
              :key="index"
              :class="['msg', message.type]"
            >
              <div class="msg-head">
                <strong>{{ message.type }}</strong>
                <span v-if="message.name" class="muted">{{ message.name }}</span>
              </div>
              <JsonView
                v-if="parseJson(message.content)"
                :data="parseJson(message.content)"
              />
              <pre v-else-if="message.content">{{ typeof message.content === 'string' ? message.content : JSON.stringify(message.content, null, 2) }}</pre>
              <div v-if="message.tool_calls?.length" class="tool-calls">
                <div v-for="(call, callIndex) in message.tool_calls" :key="callIndex">
                  <code>{{ call.name }}</code>
                  <JsonView v-if="call.args" :data="call.args" />
                </div>
              </div>
            </article>
          </div>
        </template>
        <p v-else class="muted">暂无数据</p>
      </aside>
    </div>
  </div>
</template>

<style scoped>
.ops {
  min-height: 100%;
  background: var(--bg-soft);
}

.ops-head {
  position: sticky;
  top: 0;
  z-index: 5;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 12px 20px;
  background: var(--bg);
  border-bottom: 1px solid var(--border);
}

.head-left,
.head-right {
  display: flex;
  align-items: center;
  gap: 12px;
}

h1 {
  margin: 0;
  font-size: 18px;
}

.ws-state {
  font-size: 12px;
  white-space: nowrap;
}

.ws-state.open {
  color: #16a34a;
}

.ws-state.connecting {
  color: #d97706;
}

.ws-state.closed {
  color: var(--danger);
}

.ops-body {
  display: flex;
  align-items: flex-start;
  gap: 0;
  padding: 16px;
  max-width: 1600px;
  margin: 0 auto;
}

.main-column {
  flex: 1;
  min-width: 0;
}

.tabs {
  display: flex;
  gap: 8px;
  margin-bottom: 12px;
}

.tab {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 8px 14px;
  color: var(--text);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 8px;
}

.tab.active {
  color: var(--primary);
  border-color: var(--primary);
}

.count {
  min-width: 20px;
  padding: 0 6px;
  font-size: 12px;
  background: var(--bg-soft);
  border-radius: 999px;
}

.panel {
  padding: 14px;
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-radius: var(--radius);
}

.filters {
  display: flex;
  flex-wrap: wrap;
  align-items: flex-end;
  gap: 10px 16px;
  margin-bottom: 12px;
}

.filters label {
  display: flex;
  flex-direction: column;
  gap: 4px;
  color: var(--text-muted);
  font-size: 12px;
}

.filters label.check {
  flex-direction: row;
  align-items: center;
  gap: 6px;
  padding-bottom: 8px;
  color: var(--text);
}

.filters label.grow {
  flex: 1;
  min-width: 180px;
}

.filters select,
.filters input {
  padding: 7px 9px;
  color: var(--text);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 7px;
}

.table-wrap {
  overflow-x: auto;
}

.data-table {
  width: 100%;
  table-layout: fixed;
  border-collapse: collapse;
  font-size: 13px;
}

.data-table th,
.data-table td {
  padding: 8px 10px;
  border-bottom: 1px solid var(--border);
  text-align: left;
  vertical-align: top;
  word-break: break-word;
}

.data-table th {
  position: relative;
  color: var(--text-muted);
  font-weight: 600;
  white-space: nowrap;
}

.data-table tbody tr:hover {
  background: var(--bg-soft);
}

.data-table .clickable {
  cursor: pointer;
}

.data-table .clickable.selected {
  background: rgba(37, 99, 235, 0.09);
}

.data-table .empty {
  color: var(--text-muted);
  text-align: center;
  padding: 24px;
}

.col-resizer {
  position: absolute;
  top: 0;
  right: -3px;
  z-index: 1;
  width: 6px;
  height: 100%;
  cursor: col-resize;
}

.col-resizer:hover {
  background: var(--primary);
  opacity: 0.4;
}

.mono {
  font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
  font-size: 12px;
  word-break: break-all;
}

.desc {
  max-width: 320px;
}

.nowrap {
  white-space: nowrap;
}

.level {
  padding: 1px 7px;
  border-radius: 999px;
  background: var(--bg-soft);
  font-size: 12px;
}

.level.error {
  color: var(--danger);
}

.level.warning {
  color: #b45309;
}

.run-status {
  padding: 1px 7px;
  border-radius: 999px;
  background: var(--bg-soft);
  font-size: 12px;
}

.run-status.RUNNING {
  color: #d97706;
}

.run-status.PAUSED {
  color: #7c3aed;
}

.run-status.DONE {
  color: #16a34a;
}

.run-status.ERROR {
  color: var(--danger);
}

.run-status.ABANDONED {
  color: var(--text-muted);
}

.ghost.tiny {
  padding: 3px 8px;
  font-size: 12px;
}

.bar {
  margin: 0 0 10px;
  padding: 8px 10px;
  background: var(--bg-soft);
  border-radius: 7px;
}

.bar.notice {
  color: #b45309;
}

.pager {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-top: 12px;
}

.pager > div {
  display: flex;
  gap: 8px;
}

.detail-splitter {
  flex: 0 0 8px;
  align-self: stretch;
  min-height: 240px;
  cursor: col-resize;
  border-radius: 4px;
}

.detail-splitter:hover {
  background: var(--primary);
  opacity: 0.25;
}

.detail-panel {
  flex: 0 0 auto;
  position: sticky;
  top: 72px;
  max-height: calc(100vh - 88px);
  overflow-y: auto;
  padding: 14px;
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-radius: var(--radius);
}

.detail-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  margin-bottom: 10px;
}

.detail-head h2 {
  margin: 0;
  font-size: 15px;
}

.detail-meta {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;
  font-size: 12px;
}

.detail-desc {
  margin: 0 0 10px;
  font-size: 12px;
  word-break: break-word;
}

.json-box {
  padding: 8px;
  max-height: calc(100vh - 300px);
  overflow: auto;
  background: var(--bg-soft);
  border: 1px solid var(--border);
  border-radius: 8px;
}

.empty-detail {
  padding: 24px 4px;
  text-align: center;
}

.toast-stack {
  position: fixed;
  right: 18px;
  bottom: 18px;
  z-index: 60;
  display: flex;
  flex-direction: column;
  gap: 10px;
  width: min(360px, calc(100vw - 36px));
}

.toast {
  position: relative;
  padding: 12px 34px 16px 14px;
  overflow: hidden;
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-radius: 10px;
  box-shadow: 0 8px 24px rgba(0, 0, 0, 0.18);
}

.toast-close {
  position: absolute;
  top: 5px;
  right: 6px;
  width: 24px;
  height: 24px;
  padding: 0;
  color: var(--text-muted);
  font-size: 18px;
  line-height: 1;
  background: transparent;
  border: 0;
}

.toast-close:hover {
  color: var(--text);
}

.toast-title {
  margin-bottom: 2px;
  padding-right: 8px;
  font-weight: 600;
}

.toast p {
  margin: 6px 0 0;
  max-height: 60px;
  overflow: hidden;
  color: var(--text-muted);
  font-size: 12px;
  word-break: break-word;
}

.toast-progress {
  position: absolute;
  left: 0;
  bottom: 0;
  width: 100%;
  height: 3px;
  background: var(--primary);
  animation: toast-progress 60s linear forwards;
}

@keyframes toast-progress {
  from {
    width: 100%;
  }
  to {
    width: 0;
  }
}

.drawer-mask {
  position: fixed;
  inset: 0;
  z-index: 30;
  background: rgba(0, 0, 0, 0.35);
}

.drawer {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  width: min(620px, 94vw);
  padding: 16px;
  overflow-y: auto;
  background: var(--bg-elevated);
  border-left: 1px solid var(--border);
}

.drawer-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 12px;
}

.drawer-title {
  min-width: 0;
}

.drawer-title .muted {
  display: block;
  margin-top: 4px;
  overflow-wrap: anywhere;
}

.drawer-head h2 {
  margin: 0;
  font-size: 17px;
}

.drawer-head .close-btn {
  flex: 0 0 auto;
  white-space: nowrap;
}

.out-msg {
  padding: 10px;
  background: var(--bg-soft);
  border-radius: 8px;
  white-space: pre-wrap;
}

.messages {
  display: flex;
  flex-direction: column;
  gap: 10px;
  margin-top: 12px;
}

.msg {
  padding: 10px;
  background: var(--bg-soft);
  border: 1px solid var(--border);
  border-radius: 8px;
}

.msg-head {
  display: flex;
  gap: 8px;
  margin-bottom: 6px;
  font-size: 12px;
}

.msg pre {
  margin: 0;
  padding: 8px;
  overflow-x: auto;
  font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
  font-size: 12px;
  white-space: pre-wrap;
  word-break: break-word;
  background: var(--bg);
  border-radius: 6px;
}

.tool-calls {
  display: flex;
  flex-direction: column;
  gap: 6px;
  margin-top: 8px;
}

.tool-calls code {
  display: inline-block;
  margin-bottom: 4px;
  color: var(--primary);
  font-size: 12px;
}

@media (max-width: 1000px) {
  .ops-body {
    flex-direction: column;
  }

  .detail-splitter {
    display: none;
  }

  .detail-panel {
    position: static;
    width: 100% !important;
    max-height: none;
    margin-top: 12px;
  }
}
</style>
