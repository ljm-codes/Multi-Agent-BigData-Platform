<script setup>
import { computed, onBeforeUnmount, ref, nextTick } from 'vue'

const props = defineProps({
  // { uncached_input, cache_read, cache_write, output, total, hit_rate, provider, model }
  usage: { type: Object, default: null },
  label: { type: String, default: '用量' },
  // bubble: 单条气泡（含提供方/模型、缓存写入）；total: 会话累计
  variant: { type: String, default: 'bubble' },
})

const open = ref(false)
const anchor = ref(null)
const style = ref({})

function stripZero(value) {
  return String(value).replace(/\.0+$/, '')
}

/** 胶囊上的紧凑写法：12.3k / 2.9M / 117M */
function compactTokens(value) {
  const n = Number(value) || 0
  if (n >= 1e9) return `${stripZero((n / 1e9).toFixed(n >= 1e10 ? 0 : 1))}B tok`
  if (n >= 1e6) return `${stripZero((n / 1e6).toFixed(n >= 1e8 ? 0 : 1))}M tok`
  if (n >= 1e3) return `${stripZero((n / 1e3).toFixed(n >= 1e5 ? 0 : 1))}k tok`
  return `${n} tok`
}

/** 明细里用完整数字，和截图一致 */
function fullTokens(value) {
  return `${Math.round(Number(value) || 0).toLocaleString('en-US')} tok`
}

const total = computed(() => Number(props.usage?.total) || 0)
const hitRate = computed(() => {
  const rate = Number(props.usage?.hit_rate) || 0
  if (props.variant === 'total') return `${Math.round(rate)}%`
  return `${stripZero(rate.toFixed(1))}%`
})
const modelText = computed(() => {
  const provider = String(props.usage?.provider || '')
  const model = String(props.usage?.model || '')
  if (provider && model) return `${provider}/${model}`
  return model || provider
})
const showWrite = computed(() => (
  props.variant === 'bubble' || Number(props.usage?.cache_write) > 0
))

function place() {
  const el = anchor.value
  if (!el) return
  const rect = el.getBoundingClientRect()
  const width = 252
  const left = Math.max(8, Math.min(rect.left, window.innerWidth - width - 8))
  // 下方空间不够就贴着胶囊往下展开，否则向上展开
  if (rect.bottom + 220 > window.innerHeight) {
    style.value = { left: `${left}px`, bottom: `${window.innerHeight - rect.top + 8}px` }
  } else {
    style.value = { left: `${left}px`, top: `${rect.bottom + 8}px` }
  }
}

async function toggle() {
  open.value = !open.value
  if (!open.value) return
  await nextTick()
  place()
  document.addEventListener('click', onDocumentClick, true)
  window.addEventListener('scroll', close, true)
  window.addEventListener('resize', close)
}

function close() {
  if (!open.value) return
  open.value = false
  document.removeEventListener('click', onDocumentClick, true)
  window.removeEventListener('scroll', close, true)
  window.removeEventListener('resize', close)
}

function onDocumentClick(event) {
  const target = event.target
  if (anchor.value?.contains(target) || target?.closest?.('.usage-pop')) return
  close()
}

function onKeydown(event) {
  if (event.key === 'Escape') close()
}

document.addEventListener('keydown', onKeydown)

onBeforeUnmount(() => {
  close()
  document.removeEventListener('keydown', onKeydown)
})
</script>

<template>
  <span v-if="usage && total" ref="anchor" class="usage-anchor">
    <button
      type="button"
      class="usage-chip"
      :class="variant"
      title="查看详细用量"
      @click.stop="toggle"
    >
      <svg viewBox="0 0 24 24" width="13" height="13" aria-hidden="true">
        <ellipse cx="12" cy="5.5" rx="7" ry="3" fill="none" stroke="currentColor" stroke-width="1.7" />
        <path d="M5 5.5v13c0 1.66 3.13 3 7 3s7-1.34 7-3v-13" fill="none"
              stroke="currentColor" stroke-width="1.7" />
        <path d="M5 12c0 1.66 3.13 3 7 3s7-1.34 7-3" fill="none"
              stroke="currentColor" stroke-width="1.7" />
      </svg>
      <span>
        {{ label }} {{ compactTokens(total) }}
        <template v-if="variant === 'total'"> · 缓存命中 {{ hitRate }}</template>
      </span>
    </button>

    <Teleport to="body">
      <div v-if="open" class="usage-pop" :style="style" @click.stop>
        <div class="usage-head">
          <span>{{ variant === 'total' ? 'Token 用量' : '本轮用量' }}</span>
          <strong>{{ fullTokens(total) }}</strong>
        </div>
        <div v-if="modelText" class="usage-row">
          <span>提供方 / 模型</span>
          <span class="usage-value">{{ modelText }}</span>
        </div>
        <div class="usage-row">
          <span>缓存命中</span>
          <span class="usage-value">{{ hitRate }}</span>
        </div>
        <div class="usage-row">
          <span>未缓存输入</span>
          <span class="usage-value">{{ fullTokens(usage.uncached_input) }}</span>
        </div>
        <div class="usage-row">
          <span>缓存读取</span>
          <span class="usage-value">{{ fullTokens(usage.cache_read) }}</span>
        </div>
        <div v-if="showWrite" class="usage-row">
          <span>缓存写入</span>
          <span class="usage-value">{{ fullTokens(usage.cache_write) }}</span>
        </div>
        <div class="usage-row">
          <span>输出</span>
          <span class="usage-value">{{ fullTokens(usage.output) }}</span>
        </div>
      </div>
    </Teleport>
  </span>
</template>

<style scoped>
.usage-anchor {
  display: inline-flex;
}

.usage-chip {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 2px 8px;
  font-size: 11.5px;
  line-height: 18px;
  color: var(--text-muted);
  background: transparent;
  border: 1px solid transparent;
  border-radius: 999px;
  cursor: pointer;
  transition: color 0.15s ease, background 0.15s ease, border-color 0.15s ease;
}

.usage-chip:hover,
.usage-chip:focus-visible {
  color: var(--text);
  background: var(--bg-soft);
  border-color: var(--border);
  outline: none;
}

.usage-chip.total {
  color: var(--text-muted);
  background: var(--bg-elevated);
  border-color: var(--border);
}

.usage-chip.total:hover {
  color: var(--text);
  border-color: var(--primary);
}

.usage-pop {
  position: fixed;
  z-index: 120;
  width: 252px;
  padding: 10px 12px;
  font-size: 12px;
  color: var(--text);
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-radius: 10px;
  box-shadow: 0 10px 30px rgba(0, 0, 0, 0.28);
}

.usage-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding-bottom: 8px;
  margin-bottom: 6px;
  font-weight: 600;
  border-bottom: 1px solid var(--border);
}

.usage-head strong {
  font-variant-numeric: tabular-nums;
}

.usage-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 3px 0;
  color: var(--text-muted);
}

.usage-value {
  color: var(--text);
  font-variant-numeric: tabular-nums;
}
</style>
