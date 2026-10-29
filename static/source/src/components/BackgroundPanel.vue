<script setup>
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { listBackground } from '@/api/background'

const props = defineProps({
  threadId: { type: String, default: '' },
})

const records = ref([])
const loading = ref(false)
const error = ref('')
let timer = null

const STATUS_TEXT = {
  registered: '已登记',
  running: '运行中',
  completed: '已完成',
  interrupted: '已中断',
  failed: '失败',
}

async function load() {
  if (!props.threadId) {
    records.value = []
    return
  }
  loading.value = true
  error.value = ''
  try {
    records.value = (await listBackground(props.threadId)) || []
  } catch (e) {
    error.value = e?.message || '加载后台任务失败'
  } finally {
    loading.value = false
  }
}

function start() {
  stop()
  load()
  // 深测动辄几分钟，5 秒一轮足够，也顺带每次请求都过一遍鉴权
  timer = setInterval(load, 5000)
}

function stop() {
  if (timer) {
    clearInterval(timer)
    timer = null
  }
}

watch(() => props.threadId, start)
onMounted(start)
onBeforeUnmount(stop)
</script>

<template>
  <aside class="panel">
    <header class="head">
      <span>后台任务</span>
      <button class="ghost small" :disabled="loading" @click="load">刷新</button>
    </header>

    <p v-if="!threadId" class="hint">先选择一个会话</p>
    <p v-else-if="error" class="hint error">{{ error }}</p>
    <p v-else-if="!records.length" class="hint">{{ loading ? '加载中…' : '暂无后台任务' }}</p>

    <div v-else class="list">
      <article v-for="item in records" :key="item.task_id" class="card">
        <div class="row">
          <span class="status" :class="item.status">{{ STATUS_TEXT[item.status] || item.status }}</span>
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
.ghost.small {
  padding: 4px 9px;
  font-size: 12px;
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
.status {
  padding: 2px 8px;
  font-size: 11px;
  color: var(--text-muted);
  background: var(--bg);
  border-radius: 999px;
}
.status.running,
.status.registered {
  color: var(--primary);
}
.status.failed {
  color: var(--danger);
}
.time {
  font-size: 11px;
  color: var(--text-muted);
}
.summary {
  margin: 8px 0 0;
  font-size: 12px;
  color: var(--text);
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
