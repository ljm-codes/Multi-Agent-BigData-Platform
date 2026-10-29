<script setup>
import { computed, nextTick, onBeforeUnmount, onMounted, ref } from 'vue'

const props = defineProps({
  thread: { type: Object, required: true },
  title: { type: String, required: true },
  active: { type: Boolean, default: false },
  pinned: { type: Boolean, default: false },
  archived: { type: Boolean, default: false },
  selectable: { type: Boolean, default: false },
  selected: { type: Boolean, default: false },
})

const emit = defineEmits(['select', 'rename', 'pin', 'archive', 'remove', 'toggle'])

const open = ref(false)
const root = ref(null)
const editing = ref(false)
const draft = ref('')
const inputRef = ref(null)

const threadId = computed(() => props.thread.thread_id)

function onDocumentClick(event) {
  if (root.value && !root.value.contains(event.target)) open.value = false
}

function act(name) {
  open.value = false
  emit(name, threadId.value)
}

function onClick() {
  if (editing.value) return
  if (props.selectable) emit('toggle', threadId.value)
  else emit('select', threadId.value)
}

/** 就地重命名：把标题换成输入框，Enter/失焦提交，Esc 取消 */
function startRename() {
  open.value = false
  draft.value = props.title
  editing.value = true
  nextTick(() => inputRef.value?.select())
}

function commitRename() {
  if (!editing.value) return
  editing.value = false
  const next = draft.value.trim()
  if (next && next !== props.title) emit('rename', threadId.value, next)
}

function cancelRename() {
  editing.value = false
}

onMounted(() => document.addEventListener('click', onDocumentClick))
onBeforeUnmount(() => document.removeEventListener('click', onDocumentClick))
</script>

<template>
  <div
    ref="root"
    class="thread"
    :class="{ active, selected: selectable && selected }"
    @click="onClick"
  >
    <span v-if="selectable" class="check" :class="{ on: selected }"></span>

    <input
      v-if="editing"
      ref="inputRef"
      v-model="draft"
      class="rename-input"
      maxlength="30"
      @click.stop
      @keydown.enter.prevent="commitRename"
      @keydown.esc.prevent="cancelRename"
      @blur="commitRename"
    />
    <span v-else class="title">
      <span v-if="pinned" class="pin">★</span>{{ title }}
    </span>

    <button v-if="!selectable && !editing" class="more" title="更多" @click.stop="open = !open">
      …
    </button>

    <div v-if="open && !selectable" class="menu" @click.stop>
      <button @click="startRename">重命名</button>
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
  gap: 8px;
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
.thread.active,
.thread.selected {
  background: var(--bg-elevated);
  box-shadow: inset 2px 0 0 var(--primary);
}

.check {
  flex-shrink: 0;
  width: 14px;
  height: 14px;
  border: 1px solid var(--border);
  border-radius: 4px;
}
.check.on {
  background: var(--primary);
  border-color: var(--primary);
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

.rename-input {
  flex: 1;
  min-width: 0;
  padding: 3px 6px;
  font-size: 13px;
  color: var(--text);
  background: var(--bg);
  border: 1px solid var(--primary);
  border-radius: 6px;
  outline: none;
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
