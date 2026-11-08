<script setup>
import { computed } from 'vue'
import { renderMarkdownLite } from '@/utils/text'

const props = defineProps({
  text: { type: String, default: '' },
})

// html:false 在 utils/text 里已强制，v-html 只接收 markdown-it 生成的标签
const html = computed(() => renderMarkdownLite(props.text || ''))
</script>

<template>
  <div class="md" v-html="html"></div>
</template>

<style scoped>
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
</style>
