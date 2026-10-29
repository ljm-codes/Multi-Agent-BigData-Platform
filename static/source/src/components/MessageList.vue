<script setup>
import { computed, ref } from 'vue'
import { renderMarkdownLite } from '@/utils/text'

const props = defineProps({
  messages: { type: Array, default: () => [] },
  readOnly: { type: Boolean, default: false },
})

const emit = defineEmits(['open-ide'])
const root = ref(null)
const hovered = ref(null)

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
  const link = target.closest('a[href^="IDE://"]')
  if (!link) return
  event.preventDefault()
  emit('open-ide', link.getAttribute('href'))
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
        :key="index"
        :data-index="index"
        class="row"
        :class="message.role"
      >
        <div v-if="message.role === 'assistant'" class="stack">
          <div
            v-if="message.content || message.streaming"
            class="bubble"
            :class="{ streaming: message.streaming, error: message.error }"
          >
            <div class="md" v-html="assistantHtml(message)"></div>
            <span v-if="message.streaming" class="caret"></span>
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
.md :deep(pre.code) {
  margin: 8px 0;
  padding: 10px 12px;
  overflow-x: auto;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 8px;
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
.md :deep(h3),
.md :deep(h4) {
  margin: 10px 0 6px;
  font-size: 14px;
}
.md :deep(blockquote) {
  margin: 6px 0;
  padding-left: 10px;
  color: var(--text-muted);
  border-left: 2px solid var(--border);
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
