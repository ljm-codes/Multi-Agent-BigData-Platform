<script setup>
import { computed, ref } from 'vue'
import JsonView from '@/components/JsonView.vue'

const props = defineProps({
  traces: { type: Array, default: () => [] },
  defaultOpen: { type: Boolean, default: true },
  stickyHeader: { type: Boolean, default: false },
})

const open = ref(props.defaultOpen)
const rawMode = ref({})
const copied = ref('')

const runningCount = computed(() => props.traces.filter((t) => t.status === 'running').length)
const successCount = computed(() => props.traces.filter((t) => t.status === 'success').length)
const failedCount = computed(() => props.traces.filter((t) => t.status === 'error').length)
const agentLabel = computed(() => {
  const agents = [...new Set(props.traces.map((t) => t.agent).filter(Boolean))]
  return agents.length === 1 ? agents[0] : ''
})

function isRaw(id) {
  return Boolean(rawMode.value[id])
}

function toggleRaw(id) {
  rawMode.value[id] = !rawMode.value[id]
}

function pretty(value) {
  try {
    return JSON.stringify(value ?? {}, null, 2)
  } catch {
    return String(value)
  }
}

function duration(trace) {
  if (trace.duration_ms == null) return '…'
  return `${trace.duration_ms}ms`
}

async function copyTrace(trace) {
  const text = `[${trace.tool}]\n${pretty(trace.args)}\n\n${trace.output || ''}`
  try {
    await navigator.clipboard.writeText(text)
    copied.value = trace.trace_id
    setTimeout(() => {
      if (copied.value === trace.trace_id) copied.value = ''
    }, 1200)
  } catch {
    window.prompt('复制工具调用内容', text)
  }
}
</script>

<template>
  <section v-if="traces.length" class="trace-panel">
    <header :class="['panel-head', { sticky: stickyHeader }]" @click="open = !open">
      <div class="head-left">
        <span class="title">工具调用<template v-if="agentLabel"> · {{ agentLabel }}</template></span>
        <span class="count">{{ traces.length }}</span>
        <span v-if="runningCount" class="pill running">运行中 {{ runningCount }}</span>
        <span v-if="successCount" class="pill success">成功 {{ successCount }}</span>
        <span v-if="failedCount" class="pill error">失败 {{ failedCount }}</span>
      </div>
      <button class="ghost tiny" type="button" @click.stop="open = !open">
        {{ open ? '收起' : '展开' }}
      </button>
    </header>

    <div v-if="open" class="trace-list">
      <article v-for="trace in traces" :key="trace.trace_id" class="trace-item">
        <div class="trace-row">
          <div class="trace-name">
            <span class="tool">{{ trace.tool || '(unknown)' }}</span>
            <span class="badge" :class="trace.status">{{ trace.status }}</span>
            <span class="muted">{{ trace.agent }} · {{ duration(trace) }}</span>
          </div>
          <div class="trace-actions">
            <button class="ghost tiny" type="button" @click="toggleRaw(trace.trace_id)">
              {{ isRaw(trace.trace_id) ? '表格' : '原始' }}
            </button>
            <button class="ghost tiny" type="button" @click="copyTrace(trace)">
              {{ copied === trace.trace_id ? '已复制' : '复制' }}
            </button>
          </div>
        </div>

        <div class="trace-section">
          <div class="section-label">输入参数</div>
          <JsonView v-if="!isRaw(trace.trace_id)" :data="trace.args || {}" />
          <pre v-else class="raw">{{ pretty(trace.args) }}</pre>
        </div>

        <div class="trace-section">
          <div class="section-label">输出</div>
          <pre class="output">{{ trace.output || (trace.status === 'running' ? '运行中…' : '（无输出）') }}</pre>
          <p v-if="trace.output_truncated" class="muted">输出已截断</p>
          <p v-if="trace.error" class="error-text">{{ trace.error }}</p>
        </div>
      </article>
    </div>
  </section>
</template>

<style scoped>
.trace-panel {
  margin: 6px 0;
  background: rgba(127, 127, 127, 0.08);
  border: 1px solid var(--border);
  border-radius: 10px;
  font-size: 12px;
}

.panel-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  padding: 8px 10px;
  cursor: pointer;
  user-select: none;
  background: var(--bg-soft);
  border-radius: 10px 10px 0 0;
}

.panel-head.sticky {
  position: sticky;
  top: 0;
  z-index: 2;
}

.head-left {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.title {
  font-weight: 600;
  font-size: 13px;
}

.count {
  min-width: 20px;
  padding: 1px 6px;
  text-align: center;
  border-radius: 999px;
  background: var(--bg);
  color: var(--text-muted);
}

.pill,
.badge {
  padding: 1px 7px;
  border-radius: 999px;
  font-size: 11px;
  background: var(--bg);
  color: var(--text-muted);
}

.pill.running,
.badge.running {
  color: var(--primary);
}

.pill.success,
.badge.success {
  color: #16a34a;
}

.pill.error,
.badge.error {
  color: var(--danger);
}

.trace-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding: 0 10px 10px;
  max-height: 380px;
  overflow-y: auto;
}

.trace-item {
  padding: 8px;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: var(--bg-soft);
}

.trace-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.trace-name {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.tool {
  font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
  color: var(--primary);
  font-weight: 600;
}

.trace-actions {
  display: flex;
  gap: 6px;
  flex-shrink: 0;
}

.trace-section {
  margin-top: 8px;
}

.section-label {
  margin-bottom: 4px;
  font-size: 11px;
  color: var(--text-muted);
}

.raw,
.output {
  margin: 0;
  padding: 8px;
  max-height: 260px;
  overflow: auto;
  white-space: pre-wrap;
  word-break: break-word;
  font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
  font-size: 12px;
  background: var(--bg);
  border-radius: 6px;
}

.error-text {
  margin: 6px 0 0;
  color: var(--danger);
}

.muted {
  color: var(--text-muted);
}

.ghost {
  border: 1px solid var(--border);
  background: transparent;
  color: var(--text);
  border-radius: 6px;
  cursor: pointer;
}

.ghost.tiny {
  padding: 2px 8px;
  font-size: 11px;
}

.ghost:hover {
  border-color: var(--primary);
  color: var(--primary);
}
</style>
