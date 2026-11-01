<script setup>
import { computed } from 'vue'

defineOptions({ name: 'JsonView' })

const props = defineProps({
  data: { type: null, default: null },
  name: { type: String, default: '' },
  depth: { type: Number, default: 0 },
})

const isObject = computed(() => props.data !== null && typeof props.data === 'object')
const isArray = computed(() => Array.isArray(props.data))
const entries = computed(() => {
  if (!isObject.value) return []
  return isArray.value
    ? props.data.map((value, index) => [String(index), value])
    : Object.entries(props.data)
})
const summary = computed(() => (isArray.value ? `[${props.data.length}]` : `{${entries.value.length}}`))
const valueType = computed(() => (props.data === null ? 'null' : typeof props.data))
const displayValue = computed(() => {
  if (props.data === null) return 'null'
  if (typeof props.data === 'string') return props.data
  return String(props.data)
})
const longString = computed(() => typeof props.data === 'string' && props.data.length > 240)
</script>

<template>
  <div class="json-node">
    <template v-if="isObject">
      <details :open="depth === 0">
        <summary>
          <span v-if="name" class="json-key">{{ name }}</span>
          <span class="json-summary">{{ summary }}</span>
        </summary>
        <div class="json-children">
          <JsonView
            v-for="[key, value] in entries"
            :key="key"
            :data="value"
            :name="key"
            :depth="depth + 1"
          />
        </div>
      </details>
    </template>
    <template v-else>
      <div class="json-leaf">
        <span v-if="name" class="json-key">{{ name }}:</span>
        <template v-if="longString">
          <details>
            <summary class="json-string-preview">{{ data.slice(0, 180) }}… <em>展开</em></summary>
            <pre class="json-long">{{ data }}</pre>
          </details>
        </template>
        <span v-else :class="['json-value', valueType]">{{ displayValue }}</span>
      </div>
    </template>
  </div>
</template>

<style scoped>
.json-node {
  font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
  font-size: 12px;
  line-height: 1.6;
}

details > summary {
  list-style: none;
  cursor: pointer;
  padding: 1px 0;
}

details > summary::-webkit-details-marker {
  display: none;
}

details > summary::before {
  content: '▸';
  display: inline-block;
  width: 13px;
  color: var(--text-muted);
}

details[open] > summary::before {
  content: '▾';
}

.json-key {
  margin-right: 4px;
  color: var(--primary);
}

.json-summary {
  color: var(--text-muted);
}

.json-children {
  margin-left: 6px;
  padding-left: 12px;
  border-left: 1px dashed var(--border);
}

.json-leaf {
  padding: 1px 0;
}

.json-value.string {
  color: #16a34a;
}

.json-value.number {
  color: #d97706;
}

.json-value.boolean {
  color: #7c3aed;
}

.json-value.null {
  color: var(--text-muted);
}

.json-string-preview {
  color: var(--text);
  word-break: break-word;
}

.json-long {
  margin: 4px 0 4px 12px;
  padding: 8px;
  max-height: 320px;
  overflow: auto;
  white-space: pre-wrap;
  word-break: break-word;
  background: var(--bg-soft);
  border-radius: 6px;
}
</style>
