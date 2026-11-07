<script setup>
import { computed, ref } from 'vue'

const props = defineProps({
  record: { type: Object, default: null },
  defaultOpen: { type: Boolean, default: true },
})

const open = ref(props.defaultOpen)

const PHASE_LABEL = {
  started: '准备中',
  test_done: '运行中',
  heartbeat: '运行中',
  handed_off: '已转后台',
  completed: '已完成',
  timeout: '已超时',
  failed: '失败',
  running: '运行中',
}

const stage = computed(() => {
  if (!props.record) return ''
  return props.record.stage || (props.record.deep ? 'deep' : props.record.shallow ? 'shallow' : '')
})

const stageLabel = computed(() => (stage.value === 'deep' ? '深层验证' : stage.value === 'shallow' ? '浅层验证' : '验证'))

const metrics = computed(() => {
  if (!props.record) return {}
  return props.record.deep || props.record.shallow || props.record.report || props.record
})

const phase = computed(() => props.record?.phase || props.record?.status || 'running')
const phaseLabel = computed(() => PHASE_LABEL[phase.value] || phase.value)
const phaseClass = computed(() => {
  if (phase.value === 'completed') return 'success'
  if (phase.value === 'timeout' || phase.value === 'failed') return 'error'
  if (phase.value === 'handed_off') return 'handed'
  return 'running'
})

const total = computed(() => Number(metrics.value.total || 0))
const passed = computed(() => Number(metrics.value.passed || 0))
const failed = computed(() => Number(metrics.value.failed || 0))
const completed = computed(() => {
  const counted = passed.value + failed.value
  if (['completed', 'timeout', 'failed'].includes(phase.value)) {
    return total.value || counted
  }
  if (props.record?.completed != null) {
    return Math.max(Number(props.record.completed), counted)
  }
  return counted
})
const elapsed = computed(() => props.record?.elapsed_s)
const current = computed(() => props.record?.current || null)
const lastOutput = computed(() => props.record?.last_output || '')
const failures = computed(() => {
  const list = props.record?.failures || metrics.value.failures || []
  return Array.isArray(list) ? list : []
})
const pct = computed(() => {
  if (phase.value === 'completed') return 100
  if (!total.value) return 0
  return Math.max(0, Math.min(100, Math.round((completed.value / total.value) * 100)))
})
const message = computed(() => props.record?.message || '')
</script>

<template>
  <section v-if="record" class="verify-card">
    <header class="panel-head" @click="open = !open">
      <div class="head-left">
        <span class="title">验证进度 · {{ stageLabel }}</span>
        <span class="badge" :class="phaseClass">{{ phaseLabel }}</span>
        <span class="muted">
          {{ completed }}/{{ total }} · 通过 {{ passed }} · 失败 {{ failed }}
          <template v-if="elapsed != null"> · {{ elapsed }}s</template>
        </span>
      </div>
      <button class="ghost tiny" type="button" @click.stop="open = !open">
        {{ open ? '收起' : '展开' }}
      </button>
    </header>

    <div v-if="open" class="body">
      <div class="bar">
        <div class="fill" :class="phaseClass" :style="{ width: pct + '%' }"></div>
      </div>

      <p v-if="current" class="line">
        当前：
        <code v-if="current.test_file">{{ current.test_file }}</code>
        <span v-if="current.name && current.name !== current.test_file">{{ current.name }}</span>
      </p>
      <p v-if="message" class="line">{{ message }}</p>

      <pre v-if="lastOutput" class="last-output">{{ lastOutput }}</pre>

      <div v-if="failures.length" class="failures">
        <p class="section-label">失败用例</p>
        <div v-for="(item, index) in failures" :key="index" class="failure">
          <code v-if="item.test_file">{{ item.test_file }}</code>
          <span>{{ item.name }}</span>
          <span v-if="item.message" class="muted">{{ item.message }}</span>
        </div>
      </div>
    </div>
  </section>
</template>

<style scoped>
.verify-card {
  margin: 8px 16px;
  background: var(--bg-soft);
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
  position: sticky;
  top: 0;
  z-index: 2;
  background: var(--bg-soft);
  border-radius: 10px 10px 0 0;
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

.badge {
  padding: 1px 7px;
  border-radius: 999px;
  font-size: 11px;
  background: var(--bg);
}

.badge.running {
  color: var(--primary);
}

.badge.success {
  color: #16a34a;
}

.badge.error {
  color: var(--danger);
}

.badge.handed {
  color: #d97706;
}

.body {
  padding: 0 10px 10px;
}

.bar {
  height: 6px;
  overflow: hidden;
  background: var(--bg);
  border-radius: 999px;
}

.fill {
  height: 100%;
  background: var(--primary);
  transition: width 0.25s ease;
}

.fill.success {
  background: #16a34a;
}

.fill.error {
  background: var(--danger);
}

.fill.handed {
  background: #d97706;
}

.line {
  margin: 8px 0 0;
}

.last-output {
  margin: 8px 0 0;
  padding: 8px;
  max-height: 160px;
  overflow: auto;
  white-space: pre-wrap;
  word-break: break-word;
  font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
  font-size: 11px;
  color: var(--text-muted);
  background: var(--bg);
  border-radius: 6px;
}

.section-label {
  margin: 10px 0 4px;
  font-size: 11px;
  color: var(--text-muted);
}

.failure {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
  padding: 4px 0;
  border-top: 1px dashed var(--border);
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
