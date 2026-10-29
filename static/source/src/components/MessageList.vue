<script setup>
import { renderMarkdownLite } from '@/utils/text'

defineProps({
  messages: { type: Array, default: () => [] },
  readOnly: { type: Boolean, default: false },
})

const emit = defineEmits(['open-ide'])

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
</script>

<template>
  <div class="messages" @click="onClick">
    <p v-if="!messages.length" class="empty-hint">
      {{ readOnly ? '已归档会话暂无可显示内容' : '还没有消息，输入内容开始对话' }}
    </p>

    <div v-for="(message, index) in messages" :key="index" class="row" :class="message.role">
      <div class="bubble" :class="{ streaming: message.streaming, error: message.error }">
        <template v-if="message.role === 'assistant'">
          <div class="md" v-html="assistantHtml(message)"></div>
          <span v-if="message.streaming" class="caret"></span>
        </template>
        <template v-else>
          {{ message.content }}
        </template>
      </div>
    </div>
  </div>
</template>

<style scoped>
.messages {
  flex: 1;
  overflow-y: auto;
  padding: 22px 24px;
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
</style>
