<script setup>
import { computed, ref } from 'vue'
import { renderMarkdownLite } from '@/utils/text'

const props = defineProps({
  interrupt: { type: Object, required: true },
})

const emit = defineEmits(['submit'])

const reason = ref('')

const isManual = computed(() => props.interrupt.kind === 'manual')
const html = computed(() => renderMarkdownLite(props.interrupt.text || ''))

function allow() {
  emit('submit', { pass: true, msg: '', force_terminate: false })
}

function reject() {
  const msg = reason.value.trim()
  if (!msg) return
  emit('submit', { pass: false, msg, force_terminate: false })
}

function terminate() {
  emit('submit', { pass: false, msg: reason.value.trim() || '人工强制终止', force_terminate: true })
}

function autoSubmit() {
  emit('submit', 'yes')
}

function manualSubmit() {
  emit('submit', 'no')
}
</script>

<template>
  <div class="interrupt">
    <div class="head">
      <span class="tag">{{ isManual ? '人工确认' : '提交确认' }}</span>
      <span class="hint">
        {{ isManual ? '请核对结果后选择：允许继续 / 拒绝并给出建议 / 强制终止' : '请选择提交方式' }}
      </span>
    </div>

    <div class="body md" v-html="html"></div>

    <div v-if="isManual" class="reason">
      <label for="interrupt-reason">拒绝原因（选择“拒绝”时必填）</label>
      <textarea id="interrupt-reason" v-model="reason" rows="2" placeholder="说明哪里不符合预期"></textarea>
    </div>

    <div class="actions">
      <template v-if="isManual">
        <button class="primary" @click="allow">允许</button>
        <button class="ghost" :disabled="!reason.trim()" @click="reject">拒绝</button>
        <button class="ghost danger" @click="terminate">强制终止</button>
      </template>
      <template v-else>
        <button class="primary" @click="autoSubmit">自动提交</button>
        <button class="ghost" @click="manualSubmit">手动提交</button>
      </template>
    </div>
  </div>
</template>

<style scoped>
.interrupt {
  margin: 0 24px 12px;
  padding: 14px 16px;
  background: var(--bg-elevated);
  border: 1px solid var(--primary);
  border-radius: 12px;
}
.head {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 10px;
}
.tag {
  padding: 2px 8px;
  font-size: 12px;
  color: #fff;
  background: var(--primary);
  border-radius: 999px;
}
.hint {
  font-size: 12px;
  color: var(--text-muted);
}
.body {
  max-height: 260px;
  overflow-y: auto;
  padding: 10px 12px;
  font-size: 13px;
  line-height: 1.7;
  background: var(--bg);
  border-radius: 8px;
}
.body :deep(pre.code) {
  margin: 8px 0;
  padding: 10px;
  overflow-x: auto;
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-radius: 6px;
}
.body :deep(code) {
  padding: 1px 4px;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 12px;
  background: var(--bg-elevated);
  border-radius: 4px;
}
.body :deep(a) {
  color: var(--primary);
}
.body :deep(p) {
  margin: 0 0 8px;
}
.body :deep(h3),
.body :deep(h4) {
  margin: 10px 0 6px;
  font-size: 13px;
}
.reason {
  margin-top: 10px;
}
.reason label {
  display: block;
  margin-bottom: 4px;
  font-size: 12px;
  color: var(--text-muted);
}
.reason textarea {
  width: 100%;
  padding: 8px 10px;
  color: var(--text);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 8px;
  resize: vertical;
  outline: none;
}
.actions {
  display: flex;
  gap: 10px;
  margin-top: 12px;
}
.ghost.danger {
  color: var(--danger);
  border-color: var(--danger);
}
.ghost:disabled {
  cursor: not-allowed;
  opacity: 0.5;
}
</style>
