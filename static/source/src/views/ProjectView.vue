<script setup>
import { computed, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { useUserStore } from '@/stores/user'
import {
  deleteProject,
  editProjectCommand,
  getProjectCommand,
  listProjects,
  submitProjectCommand,
} from '@/api/project'

const router = useRouter()
const user = useUserStore()

const limit = 20
const rows = ref([])
const total = ref(0)
const offset = ref(0)
const loading = ref(false)
const error = ref('')
const notice = ref('')
const filters = ref({ app_type: '', status: '', verify_status: '' })

const commandOpen = ref(false)
const commandLoading = ref(false)
const commandBusy = ref(false)
const commandError = ref('')
const commandNotice = ref('')
const commandMeta = ref(null)
const commandText = ref('')
const submitOutput = ref('')
const selected = ref(null)

const page = computed(() => Math.floor(offset.value / limit) + 1)
const pages = computed(() => Math.max(1, Math.ceil(total.value / limit)))

function flash(message) {
  notice.value = message
  setTimeout(() => {
    notice.value = ''
  }, 2400)
}

function fmtTime(value) {
  if (!value) return '-'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString()
}

async function load() {
  loading.value = true
  error.value = ''
  try {
    const data = await listProjects({ ...filters.value, limit, offset: offset.value })
    rows.value = data?.log_data || []
    total.value = Number(data?.total_count || 0)
  } catch (e) {
    error.value = e?.message || '加载项目失败'
  } finally {
    loading.value = false
  }
}

function search() {
  offset.value = 0
  load()
}

function turnPage(delta) {
  const next = offset.value + delta * limit
  if (next < 0 || next >= total.value) return
  offset.value = next
  load()
}

async function openCommand(row) {
  selected.value = row
  commandOpen.value = true
  commandLoading.value = true
  commandError.value = ''
  commandNotice.value = ''
  commandMeta.value = null
  commandText.value = ''
  submitOutput.value = ''
  try {
    const data = await getProjectCommand(row.project_id)
    commandMeta.value = data || null
    commandText.value = data?.command || ''
  } catch (e) {
    commandError.value = e?.message || '获取项目命令失败'
  } finally {
    commandLoading.value = false
  }
}

function closeCommand() {
  if (commandBusy.value) return
  commandOpen.value = false
  selected.value = null
}

async function saveCommand() {
  if (!selected.value || !commandMeta.value) return
  commandBusy.value = true
  commandError.value = ''
  commandNotice.value = ''
  try {
    const data = await editProjectCommand(selected.value.project_id, commandText.value)
    commandText.value = data?.command || commandText.value
    commandMeta.value = { ...(commandMeta.value || {}), ...(data || {}) }
    commandNotice.value = '命令已保存'
    await load()
  } catch (e) {
    commandError.value = e?.message || '保存命令失败'
  } finally {
    commandBusy.value = false
  }
}

async function submitCommand() {
  if (!selected.value || !commandMeta.value) return
  if (!window.confirm('提交后会直接在 RM 客户端执行该命令，确定提交？')) return
  commandBusy.value = true
  commandError.value = ''
  commandNotice.value = ''
  submitOutput.value = ''
  try {
    const data = await submitProjectCommand(selected.value.project_id)
    submitOutput.value = data?.output || ''
    commandNotice.value = `已提交，退出码 ${data?.exit_code ?? '-'}`
    commandMeta.value = { ...(commandMeta.value || {}), idx_type: data?.idx_type || 'brief' }
    await load()
  } catch (e) {
    submitOutput.value = e?.data?.output || ''
    commandMeta.value = {
      ...(commandMeta.value || {}),
      idx_type: e?.data?.idx_type || commandMeta.value?.idx_type,
    }
    commandError.value = e?.message || '提交命令失败'
  } finally {
    commandBusy.value = false
  }
}

async function removeProject(row) {
  if (!window.confirm(`删除项目「${row.title}」及其命令存档？相关会话必须先删除。`)) return
  error.value = ''
  try {
    await deleteProject(row.project_id)
    if (rows.value.length === 1 && offset.value > 0) offset.value -= limit
    flash('项目已删除')
    await load()
  } catch (e) {
    error.value = e?.message || '删除项目失败'
  }
}

onMounted(load)
</script>

<template>
  <div class="project">
    <header class="head">
      <button class="ghost" @click="router.push('/')">← 返回</button>
      <h1>项目</h1>
      <span class="muted">{{ user.username }}</span>
    </header>

    <div class="content">
      <section class="card">
        <div class="filters">
          <label>
            应用类型
            <select v-model="filters.app_type" @change="search">
              <option value="">全部</option>
              <option value="spark">spark</option>
              <option value="flink">flink</option>
            </select>
          </label>
          <label>
            项目状态
            <select v-model="filters.status" @change="search">
              <option value="">全部</option>
              <option value="active">active</option>
              <option value="completed">completed</option>
              <option value="failed">failed</option>
              <option value="missing">missing</option>
              <option value="archived">archived</option>
            </select>
          </label>
          <label>
            验证状态
            <select v-model="filters.verify_status" @change="search">
              <option value="">全部</option>
              <option value="unknown">unknown</option>
              <option value="passed">passed</option>
              <option value="failed">failed</option>
              <option value="error">error</option>
            </select>
          </label>
          <button class="ghost" :disabled="loading" @click="load">
            {{ loading ? '刷新中…' : '刷新' }}
          </button>
        </div>

        <p v-if="error" class="error">{{ error }}</p>
        <p v-if="notice" class="notice">{{ notice }}</p>
        <p v-if="loading" class="muted">加载中…</p>
        <p v-else-if="!rows.length" class="muted">暂无项目</p>

        <div v-else class="table-wrap">
          <table class="table">
            <thead>
              <tr>
                <th>标题</th>
                <th>类型</th>
                <th>项目名</th>
                <th>日期</th>
                <th>状态</th>
                <th>验证</th>
                <th>更新时间</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="row in rows" :key="row.project_id">
                <td class="title-cell" :title="row.description || row.title">{{ row.title }}</td>
                <td>{{ row.app_type }}</td>
                <td class="muted">{{ row.project_name || '-' }}</td>
                <td class="muted">{{ row.project_date || '-' }}</td>
                <td>{{ row.status }}</td>
                <td>{{ row.verify_status }}</td>
                <td class="muted">{{ fmtTime(row.updated_at) }}</td>
                <td class="ops">
                  <button class="ghost small" @click="openCommand(row)">命令</button>
                  <button class="ghost small danger" @click="removeProject(row)">删除</button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <div class="pager">
          <button class="ghost small" :disabled="offset <= 0 || loading" @click="turnPage(-1)">上一页</button>
          <span class="muted">第 {{ page }} / {{ pages }} 页，共 {{ total }} 条</span>
          <button class="ghost small" :disabled="page >= pages || loading" @click="turnPage(1)">下一页</button>
        </div>
      </section>
    </div>

    <div v-if="commandOpen" class="mask" @click.self="closeCommand">
      <div class="dialog">
        <h3>项目命令 <span class="muted small">{{ selected?.title }}</span></h3>

        <p v-if="commandLoading" class="muted">加载中…</p>
        <template v-else>
          <dl v-if="commandMeta" class="meta">
            <div><dt>应用名</dt><dd>{{ commandMeta.app_name || '-' }}</dd></div>
            <div><dt>类型</dt><dd>{{ commandMeta.app_type || selected?.app_type }}</dd></div>
            <div>
              <dt>命令类型</dt>
              <dd>{{ commandMeta.idx_type === 'brief' ? '已提交命令' : '参考命令' }}</dd>
            </div>
            <div><dt>更新时间</dt><dd>{{ fmtTime(commandMeta.updated_at) }}</dd></div>
          </dl>

          <textarea v-model="commandText" class="command" rows="10" spellcheck="false"></textarea>

          <pre v-if="submitOutput" class="output">{{ submitOutput }}</pre>
          <p v-if="commandError" class="error">{{ commandError }}</p>
          <p v-if="commandNotice" class="notice">{{ commandNotice }}</p>

          <div class="dialog-actions">
            <button class="ghost" :disabled="commandBusy" @click="closeCommand">关闭</button>
            <button
                class="ghost"
                :disabled="commandBusy || !commandMeta"
                @click="saveCommand"
            >保存修改</button>
            <button
                class="primary"
                :disabled="commandBusy || !commandMeta"
                @click="submitCommand"
            >提交命令</button>
          </div>
        </template>
      </div>
    </div>
  </div>
</template>

<style scoped>
.project {
  display: flex;
  flex-direction: column;
  height: 100%;
  background: var(--bg);
}
.head {
  display: flex;
  align-items: center;
  gap: 14px;
  height: 52px;
  padding: 0 20px;
  border-bottom: 1px solid var(--border);
}
.head h1 {
  margin: 0;
  font-size: 15px;
  font-weight: 600;
}
.content {
  flex: 1;
  overflow-y: auto;
  padding: 20px;
}
.card {
  padding: 16px 18px;
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-radius: var(--radius);
}
.filters {
  display: flex;
  gap: 10px;
  align-items: flex-end;
  flex-wrap: wrap;
  margin-bottom: 12px;
}
.filters label {
  display: flex;
  flex-direction: column;
  gap: 5px;
  font-size: 12px;
  color: var(--text-muted);
}
.filters select {
  padding: 7px 9px;
  color: var(--text);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 8px;
  outline: none;
}
.table-wrap {
  overflow-x: auto;
}
.table {
  width: 100%;
  margin-top: 4px;
  font-size: 13px;
  border-collapse: collapse;
}
.table th,
.table td {
  padding: 9px 8px;
  text-align: left;
  white-space: nowrap;
  border-bottom: 1px solid var(--border);
}
.table th {
  font-size: 12px;
  font-weight: 500;
  color: var(--text-muted);
}
.title-cell {
  max-width: 280px;
  overflow: hidden;
  text-overflow: ellipsis;
}
.ops {
  display: flex;
  gap: 6px;
}
.ghost.small {
  padding: 4px 9px;
  font-size: 12px;
}
.ghost.danger {
  color: var(--danger);
  border-color: var(--danger);
}
.pager {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-top: 14px;
}
.notice {
  color: var(--primary);
}
.mask {
  position: fixed;
  inset: 0;
  z-index: 50;
  display: flex;
  align-items: center;
  justify-content: center;
  background: rgba(0, 0, 0, 0.45);
}
.dialog {
  width: min(920px, 94vw);
  max-height: 88vh;
  overflow-y: auto;
  padding: 22px;
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-radius: var(--radius);
}
.dialog h3 {
  margin: 0 0 14px;
  font-size: 15px;
  font-weight: 600;
}
.small {
  font-size: 12px;
}
.meta {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 8px 16px;
  margin: 0 0 12px;
  font-size: 12px;
}
.meta div {
  min-width: 0;
}
.meta dt {
  color: var(--text-muted);
}
.meta dd {
  margin: 3px 0 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.command {
  width: 100%;
  padding: 10px 12px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 12px;
  line-height: 1.6;
  color: var(--text);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 8px;
  outline: none;
  resize: vertical;
}
.output {
  max-height: 200px;
  overflow: auto;
  padding: 10px 12px;
  font-size: 12px;
  white-space: pre-wrap;
  word-break: break-all;
  background: var(--bg-soft);
  border: 1px solid var(--border);
  border-radius: 8px;
}
.dialog-actions {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  margin-top: 14px;
}
</style>
