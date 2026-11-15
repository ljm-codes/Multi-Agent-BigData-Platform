<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'

const props = defineProps({
  modelValue: { type: String, default: '00:00' },
})
const emit = defineEmits(['update:modelValue'])

const root = ref(null)
const open = ref(false)
const text = ref(props.modelValue || '00:00')

const options = computed(() => {
  const values = []
  for (let hour = 0; hour < 24; hour += 1) {
    for (const minute of ['00', '30']) {
      values.push(`${String(hour).padStart(2, '0')}:${minute}`)
    }
  }
  return values
})

function normalize(value) {
  const matched = /^([01]\d|2[0-3]):([0-5]\d)$/.exec(String(value || '').trim())
  return matched ? `${matched[1]}:${matched[2]}` : ''
}

function commit() {
  const value = normalize(text.value)
  if (!value) {
    text.value = props.modelValue || '00:00'
    return
  }
  text.value = value
  emit('update:modelValue', value)
}

function pick(value) {
  text.value = value
  emit('update:modelValue', value)
  open.value = false
}

function toggle() {
  open.value = !open.value
}

function onDocumentClick(event) {
  if (root.value && !root.value.contains(event.target)) open.value = false
}

watch(() => props.modelValue, (value) => {
  text.value = value || '00:00'
})

onMounted(() => document.addEventListener('click', onDocumentClick))
onBeforeUnmount(() => document.removeEventListener('click', onDocumentClick))
</script>

<template>
  <div ref="root" class="time-picker">
    <div class="trigger">
      <input v-model="text" placeholder="HH:mm" @change="commit" @blur="commit" />
      <button type="button" class="icon" @click.stop="toggle">🕒</button>
    </div>
    <div v-if="open" class="pop" @click.stop>
      <p class="hint">可滚动选择，也可以直接改上面的时间</p>
      <button
        v-for="value in options"
        :key="value"
        :class="['time', { active: value === modelValue }]"
        @click="pick(value)"
      >
        {{ value }}
      </button>
    </div>
  </div>
</template>

<style scoped>
.time-picker {
  position: relative;
  display: inline-flex;
  flex-direction: column;
}

.trigger {
  display: flex;
  align-items: center;
  gap: 4px;
}

input {
  width: 110px;
  padding: 8px 10px;
  color: var(--text);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 7px;
  font-size: 14px;
}

.icon {
  width: 32px;
  height: 34px;
  padding: 0;
  color: var(--text);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 7px;
}

.pop {
  position: absolute;
  z-index: 100;
  top: calc(100% + 6px);
  left: 0;
  width: 170px;
  max-height: 250px;
  overflow: auto;
  padding: 8px;
  background: var(--bg-soft);
  border: 1px solid var(--border);
  border-radius: 10px;
  box-shadow: 0 18px 50px rgba(0, 0, 0, 0.35);
}

.hint {
  margin: 2px 4px 6px;
  color: var(--muted);
  font-size: 12px;
}

.time {
  width: 100%;
  padding: 7px 9px;
  color: var(--text);
  font-size: 14px;
  text-align: left;
  background: transparent;
  border: 1px solid transparent;
  border-radius: 6px;
}

.time:hover,
.time.active {
  background: var(--bg);
  border-color: var(--border);
}
</style>
