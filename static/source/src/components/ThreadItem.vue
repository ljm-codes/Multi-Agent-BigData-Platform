<script setup>
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'

const props = defineProps({
  thread: { type: Object, required: true },
  title: { type: String, required: true },
  active: { type: Boolean, default: false },
  pinned: { type: Boolean, default: false },
  archived: { type: Boolean, default: false },
})

const emit = defineEmits(['select', 'rename', 'pin', 'archive', 'remove'])

const open = ref(false)
const root = ref(null)

const threadId = computed(() => props.thread.thread_id)

function onDocumentClick(event) {
  if (root.value && !root.value.contains(event.target)) open.value = false
}

function act(name) {
  open.value = false
  emit(name, threadId.value)
}

onMounted(() => document.addEventListener('click', onDocumentClick))
onBeforeUnmount(() => document.removeEventListener('click', onDocumentClick))
</script>

<template>
  <div ref="root" class="thread" :class="{ active }" @click="emit('select', threadId)">
    <span class="title">
      <span v-if="pinned" class="pin">★</span>{{ title }}
    </span>
    <button class="more" title="更多" @click.stop="open = !open">…</button>

    <div v-if="open" class="menu" @click.stop>
      <button @click="act('rename')">重命名</button>
      <button @click="act('pin')">{{ pinned ? '取消置顶' : '置顶' }}</button>
      <button v-if="!archived" @click="act('archive')">归档</button>
      <button v-else class="danger" @click="act('remove')">删除</button>
    </div>
  </div>
</template>

<style scoped>
.thread {
  position: relative;
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 8px 10px;
  margin-bottom: 2px;
  font-size: 13px;
  color: var(--text);
  border-radius: 8px;
  cursor: pointer;
}
.thread:hover {
  background: var(--bg-elevated);
}
.thread.active {
  background: var(--bg-elevated);
  box-shadow: inset 2px 0 0 var(--primary);
}
.title {
  flex: 1;
  overflow: hidden;
  white-space: nowrap;
  text-overflow: ellipsis;
}
.pin {
  margin-right: 4px;
  color: var(--primary);
}
.more {
  width: 22px;
  height: 22px;
  padding: 0;
  color: var(--text-muted);
  background: transparent;
  border: 0;
  border-radius: 6px;
  opacity: 0;
}
.thread:hover .more,
.thread.active .more {
  opacity: 1;
}
.more:hover {
  color: var(--text);
  background: var(--bg);
}
.menu {
  position: absolute;
  top: 32px;
  right: 6px;
  z-index: 10;
  min-width: 108px;
  padding: 4px;
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-radius: 8px;
  box-shadow: 0 6px 18px rgba(0, 0, 0, 0.18);
}
.menu button {
  display: block;
  width: 100%;
  padding: 7px 10px;
  font-size: 13px;
  color: var(--text);
  text-align: left;
  background: transparent;
  border: 0;
  border-radius: 6px;
}
.menu button:hover {
  background: var(--bg);
}
.menu button.danger {
  color: var(--danger);
}
</style>
