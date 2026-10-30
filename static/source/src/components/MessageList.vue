<script setup>
import { computed, nextTick, onMounted, ref, watch } from 'vue'
import { renderMarkdownLite } from '@/utils/text'
import { renderMermaid } from '@/utils/mermaid'
import InterruptPanel from '@/components/InterruptPanel.vue'

const props = defineProps({
  messages: { type: Array, default: () => [] },
  readOnly: { type: Boolean, default: false },
  interrupt: { type: Object, default: null },
  elapsed: { type: Number, default: 0 },
})

const emit = defineEmits(['open-ide', 'submit'])
const root = ref(null)
const hovered = ref(null)

const hasStreaming = computed(() => props.messages.some((m) => m.streaming))

/** 流式期间先显示 mermaid 源码，等所有消息都结束后一次渲染，避免反复重建 SVG */
async function drawMermaid() {
  if (hasStreaming.value) return
  await nextTick()
  await renderMermaid(root.value)
}

onMounted(drawMermaid)
// 只在“流式从 true 变 false”或消息数组被整体替换（换会话/加载历史）时重绘
watch(hasStreaming, (streaming) => { if (!streaming) drawMermaid() })
watch(() => props.messages, drawMermaid)
watch(() => props.interrupt, drawMermaid, { deep: true })

const guides = computed(() =>
  props.messages
    .map((message, index) => ({ index, message }))
    .filter((item) => item.message.role === 'user')
)

function scrollToBottom() {
  const el = root.value
  if (el) el.scrollTop = el.scrollHeight
}

function jumpTo(index) {
  const el = root.value?.querySelector(`[data-index="${index}"]`)
  if (el) el.scrollIntoView({ behavior: 'smooth', block: 'center' })
}

/** 悬停提示挂在 body 上：轨道本身要能滚动，留在里面会被 overflow 裁掉 */
function showTip(index, text, event) {
  const rect = event.currentTarget.getBoundingClientRect()
  hovered.value = { index, text, top: rect.top + rect.height / 2 }
}

function hideTip() {
  hovered.value = null
}

defineExpose({ scrollToBottom, jumpTo })

function onClick(event) {
  const target = event.target
  if (!(target instanceof Element)) return
  const link = target.closest('a')
  if (!link) return
  const href = link.getAttribute('href') || ''
  if (!href.toLowerCase().startsWith('ide://')) return
  event.preventDefault()
  emit('open-ide', href)
}

function assistantHtml(message) {
  return renderMarkdownLite(message.content)
}

function preview(text, size = 10) {
  const clean = String(text || '').replace(/\s+/g, ' ').trim()
  return clean.length > size ? `${clean.slice(0, size)}…` : clean
}
</script>

<template>
  <div class="list-wrap">
    <div ref="root" class="messages" @click="onClick">
      <p v-if="!messages.length" class="empty-hint">
        {{ readOnly ? '已归档会话暂无可显示内容' : '还没有消息，输入内容开始对话' }}
      </p>

      <div
        v-for="(message, index) in messages"
        :key="message.id || index"
        :data-index="index"
        class="row"
        :class="message.role"
      >
        <div v-if="message.role === 'assistant'" class="stack">
          <span v-if="message.agent" class="agent-name">{{ message.agent }}</span>
          <div
            v-if="message.content || message.streaming"
            class="bubble"
            :class="{ streaming: message.streaming, error: message.error }"
          >
            <div v-if="message.content" class="md" v-html="assistantHtml(message)"></div>
            <span v-else class="pending-hint" title="Agent 工具执行期间可能较久">
              正在生成…
              <span v-if="elapsed" class="pending-elapsed">{{ elapsed }}s</span>
            </span>
            <span v-if="message.status" class="tool-status">{{ message.status }}</span>
            <span v-if="message.streaming && message.content" class="caret"></span>
          </div>

          <details v-if="message.tools && message.tools.length" class="tools">
            <summary>工具调用结果 · {{ message.tools.length }} 条</summary>
            <pre v-for="(tool, toolIndex) in message.tools" :key="toolIndex">{{ tool }}</pre>
          </details>
        </div>

        <div v-else class="bubble" :class="{ error: message.error }">
          {{ message.content }}
        </div>
      </div>

      <InterruptPanel
        v-if="interrupt"
        :interrupt="interrupt"
        @submit="emit('submit', $event)"
        @open-ide="emit('open-ide', $event)"
      />
    </div>

    <div v-if="guides.length" class="rail">
      <button
        v-for="guide in guides"
        :key="guide.index"
        class="dot"
        :title="preview(guide.message.content)"
        @mouseenter="showTip(guide.index, guide.message.content, $event)"
        @mouseleave="hideTip"
        @click="jumpTo(guide.index)"
      ></button>

      <Teleport to="body">
        <div v-if="hovered" class="rail-tip" :style="{ top: hovered.top + 'px' }">
          {{ preview(hovered.text) }}
        </div>
      </Teleport>
    </div>
  </div>
</template>

<style scoped>
.list-wrap {
  position: relative;
  display: flex;
  flex: 1;
  min-height: 0;
}
.messages {
  flex: 1;
  overflow-y: auto;
  padding: 22px 56px 22px 24px;
  display: flex;
  flex-direction: column;
  gap: 14px;
}
.row {
  display: flex;
}
.row.user {
  justify-content: flex-end;
}
.row.assistant {
  justify-content: flex-start;
}
.bubble {
  max-width: min(760px, 78%);
  padding: 10px 14px;
  line-height: 1.65;
  word-break: break-word;
  border-radius: 12px;
}
.row.user .bubble {
  color: #fff;
  background: var(--primary);
  border-bottom-right-radius: 4px;
  white-space: pre-wrap;
}
.row.assistant .bubble {
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-bottom-left-radius: 4px;
}

.stack {
  display: flex;
  flex-direction: column;
  gap: 6px;
  max-width: min(760px, 78%);
}
.stack .bubble {
  max-width: none;
}
.agent-name {
  padding: 0 2px;
  font-size: 12px;
  font-weight: 600;
  color: var(--text-muted);
  letter-spacing: 0.02em;
}

.tools {
  background: var(--bg);
  border: 1px dashed var(--border);
  border-radius: 8px;
}
.tools summary {
  padding: 6px 10px;
  font-size: 12px;
  color: var(--text-muted);
  cursor: pointer;
  user-select: none;
}
.tools pre {
  margin: 0;
  padding: 0 10px 8px;
  max-height: 220px;
  overflow: auto;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 12px;
  color: var(--text-muted);
  white-space: pre-wrap;
  word-break: break-word;
  opacity: 0.85;
}
.bubble.error {
  border-color: var(--danger);
}
.bubble.streaming {
  opacity: 0.92;
}
.pending-hint {
  font-size: 12px;
  color: var(--text-muted);
}
.pending-elapsed {
  margin-left: 6px;
  font-variant-numeric: tabular-nums;
}
.tool-status {
  display: block;
  margin-top: 4px;
  font-size: 12px;
  color: var(--text-muted);
}
.caret {
  display: inline-block;
  width: 6px;
  height: 14px;
  margin-left: 2px;
  vertical-align: text-bottom;
  background: var(--primary);
  animation: blink 1s steps(2, start) infinite;
}
@keyframes blink {
  to {
    visibility: hidden;
  }
}
.md :deep(p) {
  margin: 0 0 8px;
}
.md :deep(p:last-child) {
  margin-bottom: 0;
}
.md :deep(pre) {
  margin: 8px 0;
  padding: 10px 12px;
  overflow-x: auto;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 8px;
}
.md :deep(pre code) {
  padding: 0;
  background: transparent;
  border-radius: 0;
}
.md :deep(code) {
  padding: 1px 4px;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 12.5px;
  background: var(--bg);
  border-radius: 4px;
}
.md :deep(a) {
  color: var(--primary);
}
.md :deep(h1),
.md :deep(h2),
.md :deep(h3),
.md :deep(h4),
.md :deep(h5),
.md :deep(h6) {
  margin: 10px 0 6px;
  font-size: 14px;
}
.md :deep(blockquote) {
  margin: 6px 0;
  padding-left: 10px;
  color: var(--text-muted);
  border-left: 2px solid var(--border);
}
.md :deep(.md-table-wrap) {
  margin: 8px 0;
  overflow-x: auto;
}
.md :deep(table) {
  width: 100%;
  font-size: 12.5px;
  border-collapse: collapse;
}
.md :deep(table th),
.md :deep(table td) {
  padding: 6px 10px;
  text-align: left;
  vertical-align: top;
  white-space: pre-wrap;
  border: 1px solid var(--border);
}
.md :deep(table th) {
  font-weight: 600;
  background: var(--bg);
}
.md :deep(pre.mermaid) {
  padding: 0;
  background: transparent;
  border: 0;
}
.md :deep(pre.mermaid-error) {
  padding: 10px 12px;
  white-space: pre-wrap;
  background: var(--bg);
  border: 1px solid var(--danger);
}
.md :deep(pre.mermaid svg),
.md :deep(pre.mermaid + svg) {
  max-width: 100%;
  height: auto;
}
.md :deep(ul),
.md :deep(ol) {
  margin: 6px 0;
  padding-left: 20px;
}
.md :deep(li) {
  margin: 2px 0;
}
.empty-hint {
  margin: auto;
  font-size: 13px;
  color: var(--text-muted);
}

.rail {
  position: absolute;
  top: 50%;
  right: 14px;
  display: flex;
  flex-direction: column;
  gap: 8px;
  max-height: 70%;
  padding: 8px 4px;
  overflow-y: auto;
  transform: translateY(-50%);
}
.dot {
  position: relative;
  flex-shrink: 0;
  width: 12px;
  height: 7px;
  padding: 0;
  background: var(--border);
  border: 0;
  border-radius: 999px;
}
.dot:hover {
  background: var(--primary);
}

/* 挂在 body 上（Teleport），不受轨道 overflow 影响 */
.rail-tip {
  position: fixed;
  right: 40px;
  z-index: 100;
  max-width: 220px;
  padding: 5px 9px;
  overflow: hidden;
  font-size: 12px;
  color: var(--text);
  white-space: nowrap;
  text-overflow: ellipsis;
  pointer-events: none;
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-radius: 6px;
  box-shadow: 0 4px 14px rgba(0, 0, 0, 0.18);
  transform: translateY(-50%);
}
</style>
