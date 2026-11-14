<script setup>
import { computed, nextTick, ref, watch } from 'vue'

const props = defineProps({
  interrupt: { type: Object, required: true },
  active: { type: Number, default: 0 },
  suggestion: { type: Number, default: -1 },
  replies: { type: Object, default: () => ({}) },
  disabled: { type: Boolean, default: false },
})

const emit = defineEmits(['update:active', 'pick'])

const root = ref(null)
const tabsRef = ref(null)

const questions = computed(() => props.interrupt?.questions || [])
const activeIndex = computed(() => {
  const count = questions.value.length
  if (!count) return 0
  const index = Number.isInteger(props.active) ? props.active : 0
  return Math.min(Math.max(index, 0), count - 1)
})
const question = computed(() => questions.value[activeIndex.value] || null)
const suggestions = computed(() => question.value?.suggestion || [])
const answeredCount = computed(() => questions.value.filter((item) => replyOf(item.id)).length)
const missingCount = computed(() => Math.max(0, questions.value.length - answeredCount.value))

function replyOf(questionId) {
  return String(props.replies?.[questionId] ?? '').trim()
}

function focus() {
  // 不要跨窗口抢焦点：用户切到别的软件/最小化浏览器时，focus() 可能把浏览器拉回来
  if (document.visibilityState !== 'visible' || !document.hasFocus()) return
  root.value?.focus()
}

/** 供外部输入框（红框）转发方向键；键盘焦点不在面板时也能切换 */
function move(key) {
  if (props.disabled || !questions.value.length) return
  if (key === 'ArrowLeft' || key === 'ArrowRight') {
    selectQuestion(activeIndex.value + (key === 'ArrowRight' ? 1 : -1))
    return
  }
  if (key === 'ArrowUp' || key === 'ArrowDown') {
    if (!suggestions.value.length) return
    let next
    if (!Number.isInteger(props.suggestion) || props.suggestion < 0) {
      next = key === 'ArrowDown' ? 0 : suggestions.value.length - 1
    } else {
      const delta = key === 'ArrowDown' ? 1 : -1
      next = (props.suggestion + delta + suggestions.value.length) % suggestions.value.length
    }
    pick(next, false)
  }
}

defineExpose({ focus, move })

function scrollActiveTab() {
  nextTick(() => {
    const el = tabsRef.value?.querySelector('.q-tab.active')
    el?.scrollIntoView?.({ block: 'nearest', inline: 'nearest' })
  })
}

function selectQuestion(index) {
  if (props.disabled) return
  const count = questions.value.length
  if (!count) return
  const next = ((index % count) + count) % count
  emit('update:active', next)
  nextTick(() => {
    focus()
    scrollActiveTab()
  })
}

function pick(index, withFocus) {
  if (props.disabled) return
  if (!Number.isInteger(index) || index < 0 || index >= suggestions.value.length) return
  emit('pick', { index, focus: Boolean(withFocus) })
  if (!withFocus) nextTick(focus)
}

function onKeydown(event) {
  if (props.disabled) return
  if (event.ctrlKey || event.altKey || event.metaKey) return
  if (!questions.value.length) return
  const key = event.key
  if (!['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown'].includes(key)) return
  event.preventDefault()
  move(key)
}

watch(activeIndex, scrollActiveTab)
watch(
  () => props.interrupt,
  () => {
    nextTick(scrollActiveTab)
  }
)
</script>

<template>
  <section
    ref="root"
    class="inquiry-panel"
    :class="{ disabled }"
    tabindex="0"
    @keydown="onKeydown"
  >
    <header class="inquiry-head">
      <span class="tag">补充信息</span>
      <span class="title">{{ interrupt.title || '需要你确认几个问题' }}</span>
      <span class="answered">已填 {{ answeredCount }}/{{ questions.length }}</span>
      <span class="count">{{ activeIndex + 1 }}/{{ questions.length }}</span>
    </header>

    <p v-if="interrupt.description" class="desc">{{ interrupt.description }}</p>

    <!-- 问题横向排列：一次只展示当前这个问题 -->
    <div class="question-bar">
      <button
        class="nav-btn"
        type="button"
        title="上一个问题"
        :disabled="disabled"
        @click="selectQuestion(activeIndex - 1)"
      >
        ‹
      </button>
      <div ref="tabsRef" class="question-tabs">
        <button
          v-for="(item, index) in questions"
          :key="item.id"
          type="button"
          class="q-tab"
          :class="{ active: index === activeIndex, done: replyOf(item.id) }"
          :title="item.question"
          :disabled="disabled"
          @click="selectQuestion(index)"
        >
          <span class="q-tab-no">{{ index + 1 }}</span>
          <span class="q-tab-state">{{ replyOf(item.id) ? '已填' : '未填' }}</span>
        </button>
      </div>
      <button
        class="nav-btn"
        type="button"
        title="下一个问题"
        :disabled="disabled"
        @click="selectQuestion(activeIndex + 1)"
      >
        ›
      </button>
    </div>

    <div v-if="question" class="question-body">
      <p class="question-text">
        <span class="q-no">{{ activeIndex + 1 }}</span>
        <span class="q-text">{{ question.question }}</span>
      </p>

      <!-- 建议纵向排列 -->
      <div v-if="suggestions.length" class="suggestion-list">
        <button
          v-for="(value, sIndex) in suggestions"
          :key="sIndex"
          type="button"
          class="suggestion-row"
          :class="{
            active: suggestion === sIndex,
            recommended: question.suggestionIndex === sIndex,
          }"
          :disabled="disabled"
          @click="pick(sIndex, true)"
        >
          <span class="suggestion-text">{{ value }}</span>
          <span class="suggestion-meta">
            <span v-if="question.suggestionIndex === sIndex" class="recommended-label">（推荐）</span>
            <span v-if="suggestion === sIndex" class="checked">✓</span>
          </span>
        </button>
      </div>
      <p v-else class="no-suggestion">该问题没有建议，请在下方输入框填写你的回答。</p>
    </div>
    <p v-else class="no-suggestion">当前没有可展示的问题。</p>

    <footer class="inquiry-foot">
      <span v-if="missingCount" class="missing">还有 {{ missingCount }} 个问题必填 · </span>
      所有问题填写后才能提交 · ←→ 切换问题 · ↑↓ 选择建议 · 点击建议或点下方输入框可补充
    </footer>
  </section>
</template>

<style scoped>
.inquiry-panel {
  flex-shrink: 0;
  margin: 0 20px 8px;
  padding: 10px 12px 8px;
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-radius: 12px;
  outline: none;
}
.inquiry-panel:focus {
  border-color: var(--primary);
  box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.14);
}
.inquiry-panel.disabled {
  opacity: 0.75;
}

.inquiry-head {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  margin-bottom: 6px;
}
.tag {
  padding: 2px 8px;
  font-size: 11px;
  color: #fff;
  background: var(--primary);
  border-radius: 999px;
}
.title {
  font-size: 13px;
  font-weight: 600;
}
.answered,
.count {
  font-size: 11px;
  color: var(--text-muted);
}
.answered {
  margin-left: auto;
}

.desc {
  margin: 0 0 8px;
  font-size: 12px;
  line-height: 1.6;
  color: var(--text-muted);
  white-space: pre-wrap;
}

/* 问题导航：横向 */
.question-bar {
  display: flex;
  align-items: center;
  gap: 6px;
  margin: 4px 0 8px;
}
.nav-btn {
  flex-shrink: 0;
  width: 26px;
  height: 30px;
  padding: 0;
  font-size: 18px;
  line-height: 1;
  color: var(--text-muted);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 8px;
}
.nav-btn:hover:not(:disabled) {
  color: var(--primary);
  border-color: var(--primary);
}
.nav-btn:disabled {
  cursor: not-allowed;
  opacity: 0.45;
}
.question-tabs {
  display: flex;
  flex: 1;
  gap: 6px;
  min-width: 0;
  padding-bottom: 2px;
  overflow-x: auto;
}
.q-tab {
  display: flex;
  flex-shrink: 0;
  flex-direction: column;
  align-items: center;
  gap: 1px;
  min-width: 48px;
  padding: 4px 8px;
  font-size: 11px;
  color: var(--text-muted);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 8px;
}
.q-tab:hover:not(:disabled) {
  border-color: var(--primary);
}
.q-tab.active {
  color: #fff;
  background: var(--primary);
  border-color: var(--primary);
}
.q-tab.done:not(.active) {
  border-color: #16a34a;
}
.q-tab-no {
  font-size: 12px;
  font-weight: 600;
}
.q-tab-state {
  font-size: 10px;
}
.q-tab.done:not(.active) .q-tab-state {
  color: #16a34a;
}

.question-body {
  margin-top: 2px;
}
.question-text {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  margin: 0 0 8px;
  font-size: 13px;
  line-height: 1.6;
  white-space: pre-wrap;
}
.q-no {
  flex-shrink: 0;
  width: 20px;
  height: 20px;
  font-size: 11px;
  line-height: 20px;
  color: var(--text-muted);
  text-align: center;
  background: var(--bg);
  border-radius: 50%;
}
.q-text {
  flex: 1;
  min-width: 0;
  word-break: break-word;
}

/* 建议导航：纵向 */
.suggestion-list {
  display: flex;
  flex-direction: column;
  gap: 6px;
  max-height: 28vh;
  padding-right: 2px;
  overflow-y: auto;
}
.suggestion-row {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 10px;
  width: 100%;
  padding: 8px 10px;
  font-size: 12.5px;
  color: var(--text);
  text-align: left;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 8px;
}
.suggestion-row:hover:not(:disabled) {
  border-color: var(--primary);
}
.suggestion-row.recommended:not(.active) {
  border-color: var(--primary);
}
.suggestion-row.active {
  color: #fff;
  background: var(--primary);
  border-color: var(--primary);
}
.suggestion-row:disabled {
  cursor: not-allowed;
}
.suggestion-text {
  flex: 1;
  min-width: 0;
  white-space: pre-wrap;
  word-break: break-word;
}
.suggestion-meta {
  display: flex;
  flex-shrink: 0;
  align-items: center;
  gap: 6px;
}
.recommended-label {
  font-size: 11px;
  color: var(--primary);
}
.suggestion-row.active .recommended-label {
  color: #fff;
}
.checked {
  font-size: 12px;
  font-weight: 700;
}

.no-suggestion {
  margin: 0;
  font-size: 11px;
  color: var(--text-muted);
}

.inquiry-foot {
  margin-top: 8px;
  font-size: 11px;
  line-height: 1.5;
  color: var(--text-muted);
}
.missing {
  color: var(--danger);
}
</style>
