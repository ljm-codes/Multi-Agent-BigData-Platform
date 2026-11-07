<script setup>
import { computed, ref, watch } from 'vue'

const props = defineProps({
  review: { type: Object, required: true },
  busy: { type: Boolean, default: false },
  error: { type: String, default: '' },
})
const emit = defineEmits(['submit'])

const CONF_FLAGS = new Set(['-D', '--conf', '--config'])
const COUNT_KEYS = new Set([
  'p', 'parallelism', 'ys', 'yn', 'num-executors', 'executor-cores',
  'driver-cores', 'parallelism.default', 'taskmanager.numberoftaskslots',
])
const MEMORY_KEYS = new Set([
  'yjm', 'ytm', 'driver-memory', 'executor-memory',
  'jobmanager.memory.process.size', 'taskmanager.memory.process.size',
  'spark.driver.memory', 'spark.executor.memory',
])

const mode = ref('table')
const stage = ref('review')
const rows = ref([])
const initialRows = ref([])
const text = ref('')
const textError = ref('')
const reason = ref('')

function cloneRows(value) {
  return (value || []).map((row) => ({
    flag: String(row.flag || ''),
    value: String(row.value || ''),
    program: Boolean(row.program),
  }))
}

function tokensFromRows(value) {
  const tokens = []
  for (const row of value || []) {
    const flag = String(row.flag || '').trim()
    const val = String(row.value || '')
    if (row.program) {
      if (flag) tokens.push(flag)
    } else if (flag && val !== '') {
      tokens.push(flag, val)
    } else if (flag) {
      tokens.push(flag)
    } else if (val) {
      tokens.push(val)
    }
  }
  return tokens
}

function rowsFromTokens(tokens) {
  if (!tokens.length) return []
  const out = [{ flag: tokens[0], value: '', program: true }]
  let i = 1
  while (i < tokens.length) {
    const tok = tokens[i]
    if (CONF_FLAGS.has(tok) && i + 1 < tokens.length) {
      out.push({ flag: tok, value: tokens[i + 1], program: false })
      i += 2
      continue
    }
    if (tok.startsWith('-') && tok.includes('=')) {
      const idx = tok.indexOf('=')
      const flag = tok.slice(0, idx)
      if (CONF_FLAGS.has(flag) || flag.startsWith('--') || flag.startsWith('-D')) {
        out.push({ flag, value: tok.slice(idx + 1), program: false })
        i += 1
        continue
      }
    }
    if (tok.startsWith('-') && i + 1 < tokens.length && !tokens[i + 1].startsWith('-')) {
      out.push({ flag: tok, value: tokens[i + 1], program: false })
      i += 2
      continue
    }
    out.push({ flag: '', value: tok, program: false })
    i += 1
  }
  return out
}

function tokenizeCommand(raw) {
  const tokens = []
  let cur = ''
  let quote = ''
  let escaped = false
  let has = false
  for (const ch of String(raw || '')) {
    if (escaped) {
      cur += ch
      escaped = false
      has = true
      continue
    }
    if (ch === '\\' && quote !== "'") {
      escaped = true
      continue
    }
    if (quote) {
      if (ch === quote) quote = ''
      else cur += ch
      has = true
      continue
    }
    if (ch === '"' || ch === "'") {
      quote = ch
      has = true
      continue
    }
    if (/\s/.test(ch)) {
      if (has) {
        tokens.push(cur)
        cur = ''
        has = false
      }
      continue
    }
    cur += ch
    has = true
  }
  if (escaped) cur += '\\'
  if (quote) throw new Error('引号没有闭合')
  if (has) tokens.push(cur)
  return tokens
}

function quoteToken(token) {
  if (token === '') return "''"
  if (/^[A-Za-z0-9_@%+=:,./-]+$/.test(token)) return token
  return `'${String(token).replace(/'/g, "'\\''")}'`
}

function serializeRows(value) {
  return tokensFromRows(value).map(quoteToken).join(' ')
}

function pairKey(row) {
  return `${row.flag || ''}\u0000${row.value || ''}`
}

const SHELL_OPERATORS = new Set([
  '|', '||', '&', '&&', ';', '>', '>>', '<', '<<', '2>', '2>&1', '&>', '1>', '1>>',
])

function structuralError(tokens) {
  if (!tokens.length) return '命令不能为空'
  const program = String(tokens[0]).split('/').pop()
  if (!['spark-submit', 'flink'].includes(program)) {
    return `程序名必须是 spark-submit 或 flink（收到 ${tokens[0]}）`
  }
  for (const token of tokens) {
    if (SHELL_OPERATORS.has(token) || token.includes('`') || token.includes('$(')) {
      return `不允许 shell 控制符：${token}`
    }
  }
  return ''
}

function normalizeKey(key) {
  let out = String(key || '').trim()
  for (const prefix of ['--config ', '--conf ', 'config ', 'conf ', '-D ', 'D ']) {
    if (out.startsWith(prefix)) {
      out = out.slice(prefix.length).trim()
      break
    }
  }
  return out
}

function isCount(key) {
  const k = key.toLowerCase()
  return COUNT_KEYS.has(k) || k.endsWith('.numberoftaskslots') || k.endsWith('.partitions')
}

function isMemory(key) {
  const k = key.toLowerCase()
  if (k.includes('fraction')) return false
  return MEMORY_KEYS.has(k) || k.includes('.memory') || k.endsWith('.process.size')
    || k.endsWith('memoryoverhead')
}

function isBool(key) {
  const k = key.toLowerCase()
  return k.endsWith('.enabled') || k.endsWith('waitappcompletion')
}

function validateValue(key, value) {
  const raw = String(value ?? '')
  if (raw.includes('\n') || raw.includes('\r')) return '值里不能有换行'
  if (raw.trim() === '') return '值不能为空'
  const flag = normalizeKey(key).replace(/^-+/, '').trim()
  if (isCount(normalizeKey(key)) || isCount(flag)) {
    return /^[1-9]\d*$/.test(raw.trim()) ? '' : '必须是正整数'
  }
  if (isMemory(normalizeKey(key)) || isMemory(flag)) {
    return /^\d+(\.\d+)?[kKmMgGtT]$/.test(raw.trim()) ? '' : '内存必须带单位（如 2g / 1024m）'
  }
  if (isBool(normalizeKey(key)) || isBool(flag)) {
    return /^(true|false)$/i.test(raw.trim()) ? '' : '只能是 true/false'
  }
  return ''
}

function rowValueInfo(row) {
  const flag = String(row.flag || '')
  const value = String(row.value || '')
  if (!flag) return null
  if (CONF_FLAGS.has(flag) && value.includes('=')) {
    const idx = value.indexOf('=')
    return { key: value.slice(0, idx).trim(), value: value.slice(idx + 1) }
  }
  if (value !== '' && flag.startsWith('-')) {
    return { key: flag.replace(/^-+/, '').trim(), value }
  }
  return null
}

function validateRows(current) {
  const errors = []
  if (!current.length || !String(current[0]?.flag || '').trim()) {
    errors.push({ index: 0, message: '程序名不能为空' })
    return errors
  }
  const shellError = structuralError(tokensFromRows(current))
  if (shellError) {
    errors.push({ index: -1, message: shellError })
    return errors
  }
  const counts = new Map()
  for (const row of initialRows.value.slice(1)) {
    const key = pairKey(row)
    counts.set(key, (counts.get(key) || 0) + 1)
  }
  current.slice(1).forEach((row, index) => {
    const key = pairKey(row)
    if ((counts.get(key) || 0) > 0) {
      counts.set(key, counts.get(key) - 1)
      return
    }
    const info = rowValueInfo(row)
    if (!info) return
    const message = validateValue(info.key, info.value)
    if (message) errors.push({ index: index + 1, message: `${info.key}: ${message}` })
  })
  return errors
}

const validationErrors = computed(() => {
  if (mode.value === 'text') {
    if (textError.value) return [{ index: -1, message: textError.value }]
    try {
      return validateRows(rowsFromTokens(tokenizeCommand(text.value)))
    } catch (e) {
      return [{ index: -1, message: e?.message || '命令解析失败' }]
    }
  }
  return validateRows(rows.value)
})

const canSubmitEdit = computed(() => stage.value === 'edit' && !validationErrors.value.length)

function init() {
  const original = props.review?.command_rows?.length
    ? cloneRows(props.review.command_rows)
    : rowsFromTokens(props.review?.command_tokens || [])
  if (!original.length && props.review?.command) {
    try {
      original.push(...rowsFromTokens(tokenizeCommand(props.review.command)))
    } catch {
      // 交给文本模式展示
    }
  }
  if (original.length) original[0].program = true
  rows.value = cloneRows(original)
  initialRows.value = cloneRows(original)
  text.value = String(props.review?.command || serializeRows(original))
  textError.value = ''
  reason.value = ''
  mode.value = 'table'
  stage.value = 'review'
}

watch(() => props.review, init, { immediate: true, deep: true })

function toggleMode() {
  if (mode.value === 'table') {
    const dirty = tokensFromRows(rows.value).join('\u0000')
      !== tokensFromRows(initialRows.value).join('\u0000')
    // 没改过就保留原始命令文本，避免只切个视图就被重新 quote
    text.value = dirty
      ? serializeRows(rows.value)
      : String(props.review?.command || serializeRows(rows.value))
    textError.value = ''
    mode.value = 'text'
    return
  }
  try {
    rows.value = rowsFromTokens(tokenizeCommand(text.value))
    textError.value = ''
    mode.value = 'table'
  } catch (e) {
    textError.value = e?.message || '命令解析失败'
  }
}

function chooseNo() {
  stage.value = 'choice'
}

function chooseEdit() {
  stage.value = 'edit'
}

function chooseObject() {
  stage.value = 'object'
}

function addRow() {
  rows.value.push({ flag: '', value: '', program: false })
}

function removeRow(index) {
  if (index === 0) return
  rows.value.splice(index, 1)
}

function submitYes() {
  if (props.busy) return
  emit('submit', { type: 'yes' })
}

function submitEdit() {
  if (props.busy || !canSubmitEdit.value) return
  if (mode.value === 'text') {
    emit('submit', { type: 'no', new_command: text.value.trim() })
    return
  }
  emit('submit', {
    type: 'no',
    new_command_rows: rows.value.map((row) => ({
      flag: row.flag,
      value: row.value,
      program: Boolean(row.program),
    })),
  })
}

function submitObjection() {
  const msg = reason.value.trim()
  if (props.busy || !msg) return
  emit('submit', { type: 'no', msg })
}
</script>

<template>
  <section class="review">
    <header class="head">
      <span class="tag">命令审核</span>
      <span class="hint">请确认是否提交，或直接修改/提出异议</span>
    </header>

    <dl class="meta">
      <div><dt>应用ID</dt><dd class="mono">{{ review.app_id || '—' }}</dd></div>
      <div><dt>名称</dt><dd>{{ review.app_name || '—' }}</dd></div>
      <div><dt>类型</dt><dd>{{ String(review.app_type || '—').toUpperCase() }}</dd></div>
    </dl>

    <p class="prompt">请审阅一下命令，来确认是否提交或者提出你的想法或直接修改：</p>

    <div class="command-head">
      <span class="muted">命令参数</span>
      <button class="ghost tiny" type="button" @click="toggleMode">
        {{ mode === 'table' ? '文本模式' : '表格模式' }}
      </button>
    </div>

    <div v-if="mode === 'table'" class="table-wrap">
      <table class="cmd-table">
        <thead>
          <tr>
            <th class="idx">#</th>
            <th>参数</th>
            <th>值</th>
            <th v-if="stage === 'edit'" class="op"></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(row, index) in rows" :key="index">
            <td class="idx">{{ index + 1 }}</td>
            <td>
              <code v-if="row.program">{{ row.flag }}</code>
              <span v-else-if="!row.flag" class="muted">(位置参数)</span>
              <input
                v-else
                v-model="row.flag"
                :disabled="stage !== 'edit' || busy"
                spellcheck="false"
              >
            </td>
            <td>
              <input
                v-model="row.value"
                :disabled="stage !== 'edit' || busy || row.program"
                spellcheck="false"
              >
            </td>
            <td v-if="stage === 'edit'" class="op">
              <button
                v-if="!row.program"
                class="ghost tiny danger"
                type="button"
                :disabled="busy"
                @click="removeRow(index)"
              >删除</button>
            </td>
          </tr>
        </tbody>
      </table>
      <button
        v-if="stage === 'edit'"
        class="ghost tiny add"
        type="button"
        :disabled="busy"
        @click="addRow"
      >+ 添加行</button>
    </div>

    <textarea
      v-else
      v-model="text"
      class="cmd-text"
      rows="5"
      spellcheck="false"
      :readonly="stage !== 'edit' || busy"
    ></textarea>

    <p v-if="validationErrors.length" class="val-errors">
      <span v-for="(item, i) in validationErrors" :key="i">
        {{ item.index >= 0 ? `第 ${item.index + 1} 行` : '命令' }}：{{ item.message }}
      </span>
    </p>
    <p v-if="error" class="error">{{ error }}</p>

    <div v-if="stage === 'review'" class="actions">
      <button class="primary" type="button" :disabled="busy" @click="submitYes">
        {{ busy ? '提交中…' : '是 (y)，提交' }}
      </button>
      <button class="ghost" type="button" :disabled="busy" @click="chooseNo">否 (n)，修改/异议</button>
    </div>

    <div v-else-if="stage === 'choice'" class="actions">
      <button class="primary" type="button" :disabled="busy" @click="chooseEdit">直接修改命令</button>
      <button class="ghost" type="button" :disabled="busy" @click="chooseObject">提交异议</button>
    </div>

    <div v-else-if="stage === 'edit'" class="actions">
      <button class="primary" type="button" :disabled="busy || !canSubmitEdit" @click="submitEdit">
        {{ busy ? '提交中…' : '提交修改后的命令' }}
      </button>
      <button class="ghost" type="button" :disabled="busy" @click="stage = 'choice'">返回</button>
    </div>

    <div v-else class="objection">
      <label for="job-restart-reason">异议原因</label>
      <textarea
        id="job-restart-reason"
        v-model="reason"
        rows="3"
        placeholder="说明哪里不符合预期，工具会把这条意见交回给运维 Agent"
        :disabled="busy"
      ></textarea>
      <div class="actions">
        <button class="primary" type="button" :disabled="busy || !reason.trim()" @click="submitObjection">
          {{ busy ? '提交中…' : '提交异议' }}
        </button>
        <button class="ghost" type="button" :disabled="busy" @click="stage = 'choice'">返回</button>
      </div>
    </div>
  </section>
</template>

<style scoped>
.review {
  margin: 10px 0 14px;
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
.hint,
.muted {
  color: var(--text-muted);
}
.hint {
  font-size: 12px;
}
.meta {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 6px 14px;
  margin: 0 0 10px;
  font-size: 13px;
}
.meta div {
  display: flex;
  gap: 6px;
}
.meta dt {
  color: var(--text-muted);
}
.meta dd {
  margin: 0;
  word-break: break-all;
}
.mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 12px;
}
.prompt {
  margin: 0 0 10px;
  font-size: 13px;
}
.command-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 6px;
  font-size: 12px;
}
.table-wrap {
  max-width: 100%;
  overflow-x: auto;
}
.cmd-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}
.cmd-table th,
.cmd-table td {
  padding: 5px 6px;
  text-align: left;
  border: 1px solid var(--border);
  vertical-align: middle;
}
.cmd-table th {
  color: var(--text-muted);
  font-weight: 500;
  background: var(--bg-soft);
}
.cmd-table .idx {
  width: 34px;
  text-align: right;
  color: var(--text-muted);
}
.cmd-table .op {
  width: 58px;
}
.cmd-table input {
  width: 100%;
  min-width: 110px;
  padding: 4px 6px;
  color: var(--text);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 5px;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 12px;
  outline: none;
}
.cmd-table input:disabled {
  color: var(--text-muted);
  background: transparent;
  border-color: transparent;
}
.cmd-table input:focus {
  border-color: var(--primary);
}
.cmd-table code {
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
}
.add {
  margin-top: 6px;
}
.review .tiny {
  padding: 2px 7px;
  font-size: 12px;
}
.cmd-text {
  width: 100%;
  padding: 8px 10px;
  color: var(--text);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 8px;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 12px;
  line-height: 1.6;
  resize: vertical;
  outline: none;
}
.cmd-text:focus {
  border-color: var(--primary);
}
.val-errors {
  display: flex;
  flex-direction: column;
  gap: 2px;
  margin: 8px 0 0;
  font-size: 12px;
  color: var(--danger);
}
.actions {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin-top: 12px;
}
.objection label {
  display: block;
  margin-bottom: 4px;
  font-size: 12px;
  color: var(--text-muted);
}
.objection textarea {
  width: 100%;
  padding: 8px 10px;
  color: var(--text);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 8px;
  resize: vertical;
  outline: none;
}
.ghost.danger {
  color: var(--danger);
  border-color: var(--danger);
}
</style>
