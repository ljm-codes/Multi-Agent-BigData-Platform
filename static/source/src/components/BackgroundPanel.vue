<script setup>
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { createTicket, openBackgroundSocket } from '@/api/background'

const props = defineProps({
  threadId: { type: String, default: '' },
})

const records = ref([])
const status = ref('idle') // idle | connecting | open | retry
const error = ref('')

let socket = null
let retryTimer = null
let retries = 0
let closedByUs = false

function keepThread(record) {
  return !props.threadId || record.main_thread_id === props.threadId
}

function upsert(record) {
  if (!record || !keepThread(record)) return
  const index = records.value.findIndex((item) => item.task_id === record.task_id)
  if (index >= 0) records.value[index] = record
  else records.value = [record, ...records.value]
}

function scheduleReconnect() {
  if (closedByUs) return
  status.value = 'retry'
  const delay = Math.min(1000 * 2 ** retries, 10000)
  retries += 1
  retryTimer = setTimeout(connect, delay)
}

function closeSocket() {
  if (!socket) return
  closedByUs = true
  socket.onclose = null
  socket.onmessage = null
  socket.close()
  socket = null
}

function cleanup() {
  if (retryTimer) {
    clearTimeout(retryTimer)
    retryTimer = null
  }
  closeSocket()
}

async function connect() {
  cleanup()
  closedByUs = false

  if (!props.threadId) {
    status.value = 'idle'
    records.value = []
    return
  }

  status.value = 'connecting'
  error.value = ''

  let ticket = ''
  try {
    const data = await createTicket()
    ticket = data?.ticket || ''
  } catch (e) {
    error.value = e?.message || '获取连接凭证失败'
    scheduleReconnect()
    return
  }
  if (!ticket) {
    error.value = '未拿到连接凭证'
    scheduleReconnect()
    return
  }

  socket = openBackgroundSocket(ticket)
  socket.onopen = () => {
    status.value = 'open'
    retries = 0
    error.value = ''
  }
  socket.onmessage = (event) => {
    let message
    try {
      message = JSON.parse(event.data)
    } catch {
      return
    }
    if (message.type === 'snapshot') {
      records.value = (message.records || []).filter(keepThread)
    } else if (message.type === 'update') {
      upsert(message.record)
    }
  }
  socket.onclose = () => {
    if (!closedByUs) scheduleReconnect()
  }
  socket.onerror = () => {
    // 交给 onclose 触发重连
  }
}

watch(
  () => props.threadId,
  () => {
    retries = 0
    records.value = []
    connect()
  }
)

onMounted(connect)
onBeforeUnmount(() => {
  closedByUs = true
  cleanup()
})
</script>

<template>
  <aside class="panel">
    <header class="head">
      <span>后台任务</span>
      <span class="state" :class="status">
        {{ { idle: '未连接', connecting: '连接中…', open: '已连接', retry: '重连中…' }[status] }}
      </span>
    </header>

    <p v-if="!threadId" class="hint">先选择一个会话</p>
    <p v-else-if="error" class="hint error">{{ error }}</p>
    <p v-else-if="!records.length" class="hint">暂无后台任务</p>

    <div v-else class="list">
      <article v-for="item in records" :key="item.task_id" class="card">
        <div class="row">
          <span class="badge" :class="item.status">{{ item.status }}</span>
          <span class="time">{{ item.updated_at }}</span>
        </div>
        <p class="summary">{{ item.summary || '（无摘要）' }}</p>
        <p v-if="item.result" class="result">{{ item.result }}</p>
        <p v-if="item.interrupt" class="interrupt">该任务停在人工确认，请回到会话处理</p>
      </article>
    </div>
  </aside>
</template>

<style scoped>
.panel {
  display: flex;
  flex-direction: column;
  width: 320px;
  flex-shrink: 0;
  border-left: 1px solid var(--border);
  background: var(--bg-soft);
}
.head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  height: 52px;
  padding: 0 14px;
  font-size: 13px;
  font-weight: 600;
  border-bottom: 1px solid var(--border);
}
.state {
  font-size: 11px;
  font-weight: 400;
  color: var(--text-muted);
}
.state.open {
  color: var(--primary);
}
.state.retry {
  color: var(--danger);
}
.hint {
  margin: 16px 14px;
  font-size: 13px;
  color: var(--text-muted);
}
.hint.error {
  color: var(--danger);
}
.list {
  flex: 1;
  overflow-y: auto;
  padding: 10px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}
.card {
  padding: 10px 12px;
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-radius: 10px;
}
.row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}
.badge {
  padding: 2px 8px;
  font-size: 11px;
  color: var(--text-muted);
  background: var(--bg);
  border-radius: 999px;
}
.badge.running,
.badge.registered {
  color: var(--primary);
}
.badge.failed {
  color: var(--danger);
}
.time {
  font-size: 11px;
  color: var(--text-muted);
}
.summary {
  margin: 8px 0 0;
  font-size: 12px;
  line-height: 1.5;
}
.result {
  margin: 6px 0 0;
  max-height: 120px;
  overflow: auto;
  font-size: 12px;
  color: var(--text-muted);
  white-space: pre-wrap;
  word-break: break-word;
}
.interrupt {
  margin: 6px 0 0;
  font-size: 12px;
  color: var(--primary);
}
</style>
