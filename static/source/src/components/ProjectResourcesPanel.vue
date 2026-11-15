<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import {
  getProjectDrift,
  getProjectResources,
  materializeProject,
  replaceProjectResource,
  resolveProjectDrift,
} from '@/api/clusterEnv'

const props = defineProps({
  projectId: { type: String, default: '' },
  projectName: { type: String, default: '' },
  projectDir: { type: String, default: '' },
})
const emit = defineEmits(['close', 'open-project'])

const loading = ref(false)
const busy = ref(false)
const error = ref('')
const notice = ref('')
const info = ref(null)
const drift = ref(null)
const selected = ref('')
const draft = ref('')
const newPath = ref('')
const uploadInput = ref(null)

const files = computed(() => info.value?.files || [])
const omitted = computed(() => new Set(info.value?.content_omitted || []))
const contents = computed(() => info.value?.contents || {})
const selectedIsText = computed(() => Boolean(selected.value && selected.value in contents.value))
const selectedIsOmitted = computed(() => Boolean(selected.value && omitted.value.has(selected.value)))
const archived = computed(() => info.value?.status === 'archived')
const driftOpen = computed(() => Boolean(drift.value && drift.value.clean === false))
const publicDrift = computed(() => drift.value?.public_drift || null)
const projectDriftId = computed(() => drift.value?.drift_id || null)

function baseName(path) {
  const parts = String(path || '').split('/')
  return parts[parts.length - 1] || 'file'
}

async function load() {
  if (!props.projectId) return
  loading.value = true
  error.value = ''
  try {
    const [resourceData, driftData] = await Promise.all([
      getProjectResources(props.projectId),
      getProjectDrift(props.projectId).catch((e) => ({ clean: true, error: e?.message || '' })),
    ])
    info.value = resourceData
    drift.value = driftData
    if (!selected.value && files.value.length) selectFile(files.value[0])
    if (selected.value && !files.value.includes(selected.value)) {
      selected.value = ''
      draft.value = ''
    }
  } catch (e) {
    error.value = e?.message || '读取项目配置失败'
  } finally {
    loading.value = false
  }
}

function selectFile(path) {
  selected.value = path
  draft.value = contents.value[path] || ''
}

async function saveText() {
  if (!selected.value || !props.projectId) return
  busy.value = true
  error.value = ''
  try {
    const file = new File([draft.value], baseName(selected.value),
      { type: 'text/plain;charset=utf-8' })
    await replaceProjectResource(props.projectId, {
      path: selected.value, file, reason: '前端编辑项目配置',
    })
    notice.value = '已保存，项目验证状态已标记为 stale'
    await load()
  } catch (e) {
    error.value = e?.message || '保存失败'
  } finally {
    busy.value = false
  }
}

async function uploadFile() {
  const file = uploadInput.value?.files?.[0]
  if (!file || !props.projectId) return
  busy.value = true
  error.value = ''
  try {
    const path = (newPath.value || file.name).trim()
    await replaceProjectResource(props.projectId, {
      path, file, reason: '前端新增/替换项目资源',
    })
    notice.value = `已写入 ${path}`
    newPath.value = ''
    if (uploadInput.value) uploadInput.value.value = ''
    await load()
    if (files.value.includes(path)) selectFile(path)
  } catch (e) {
    error.value = e?.message || '上传失败'
  } finally {
    busy.value = false
  }
}

async function removeSelected() {
  if (!selected.value || !props.projectId) return
  if (!window.confirm(`删除 ${selected.value}？删除后需要重新验证。`)) return
  busy.value = true
  error.value = ''
  try {
    await replaceProjectResource(props.projectId, {
      path: selected.value, remove: true, reason: '前端删除项目资源',
    })
    notice.value = `已删除 ${selected.value}`
    selected.value = ''
    draft.value = ''
    await load()
  } catch (e) {
    error.value = e?.message || '删除失败'
  } finally {
    busy.value = false
  }
}

async function resolveDrift(decision) {
  if (!projectDriftId.value || !props.projectId) return
  const text = decision === 'rollback' ? '回滚到 materialized snapshot' : '保留当前修改'
  if (!window.confirm(`确认${text}？`)) return
  busy.value = true
  error.value = ''
  try {
    await resolveProjectDrift(props.projectId, projectDriftId.value, decision)
    notice.value = decision === 'rollback' ? '已回滚' : '已保留，验证状态为 stale'
    await load()
  } catch (e) {
    error.value = e?.message || '处理 drift 失败'
  } finally {
    busy.value = false
  }
}

async function rematerialize() {
  if (!props.projectId) return
  busy.value = true
  error.value = ''
  try {
    await materializeProject(props.projectId, { reason: '前端重新 materialize' })
    notice.value = '已重新 materialize'
    await load()
  } catch (e) {
    error.value = e?.message || 'materialize 失败'
  } finally {
    busy.value = false
  }
}

async function copyPath() {
  try {
    await navigator.clipboard.writeText(info.value?.project_dir || props.projectDir || '')
    notice.value = '路径已复制'
  } catch {
    notice.value = '复制失败，请手动选择路径'
  }
  setTimeout(() => (notice.value = ''), 1600)
}

onMounted(load)
watch(() => props.projectId, (value) => {
  if (value) load()
})
</script>

<template>
  <div class="mask" @click.self="emit('close')">
    <section class="panel">
      <header class="head">
        <div>
          <h2>项目配置</h2>
          <p class="sub">{{ projectName || info?.project_id || projectId }}</p>
        </div>
        <button class="ghost small" @click="emit('close')">关闭</button>
      </header>

      <p v-if="error" class="error">{{ error }}</p>
      <p v-if="notice" class="notice">{{ notice }}</p>
      <p v-if="loading" class="muted">加载中…</p>

      <template v-else-if="info">
        <div class="pathbar">
          <span class="path" :title="info.project_dir || projectDir">
            {{ info.project_dir || projectDir || '尚未生成项目目录' }}
          </span>
          <button class="ghost small" @click="copyPath">复制路径</button>
          <button class="ghost small" @click="emit('open-project')">打开项目</button>
        </div>

        <div v-if="archived" class="banner">
          项目已归档。目录仍保留，你可以自行保留、删除或移动；删除会话不会自动删目录。
        </div>
        <div v-if="driftOpen" class="banner warn">
          <template v-if="publicDrift && !projectDriftId">
            cluster_resources 与项目记录的公有快照不一致。请管理员先在“集群环境”处理 public drift。
          </template>
          <template v-else>
            项目 src/main/resources 与基线不一致。
            <button class="ghost small" :disabled="busy" @click="resolveDrift('keep')">保留修改</button>
            <button class="ghost small danger" :disabled="busy" @click="resolveDrift('rollback')">回滚</button>
          </template>
        </div>
        <div class="meta">
          <span>验证状态：<b>{{ info.verify_status }}</b></span>
          <span>物化快照：{{ info.snapshots?.materialized_snapshot_id || '-' }}</span>
          <span>公有快照：{{ info.snapshots?.public_snapshot_id || '-' }}</span>
          <button class="ghost small" :disabled="busy" @click="rematerialize">重新 materialize</button>
        </div>

        <div class="body">
          <aside class="files">
            <p class="files-title">resources（{{ files.length }}）</p>
            <button
              v-for="path in files"
              :key="path"
              :class="['file', { active: path === selected }]"
              @click="selectFile(path)"
            >
              <span class="file-name">{{ path }}</span>
              <span v-if="omitted.has(path)" class="tag">仅哈希</span>
            </button>
            <p v-if="!files.length" class="muted small">还没有资源文件</p>
          </aside>

          <div class="editor">
            <template v-if="selected">
              <div class="editor-head">
                <span class="file-name">{{ selected }}</span>
                <button class="ghost small danger" :disabled="busy" @click="removeSelected">删除</button>
              </div>
              <template v-if="selectedIsText">
                <textarea v-model="draft" spellcheck="false"></textarea>
                <div class="editor-actions">
                  <button class="primary" :disabled="busy" @click="saveText">
                    {{ busy ? '保存中…' : '保存' }}
                  </button>
                  <span class="muted small">保存后验证状态变为 stale</span>
                </div>
              </template>
              <p v-else class="muted">
                {{ selectedIsOmitted ? '大文件只保存哈希，不能在页面编辑；请用下方上传替换。' : '二进制或无法解码的文件，请用上传替换。' }}
              </p>
            </template>
            <p v-else class="muted">从左侧选择文件查看/编辑</p>
          </div>
        </div>

        <footer class="upload">
          <input v-model="newPath" placeholder="目标相对路径，例如 conf/app.properties（留空用文件名）" />
          <input ref="uploadInput" type="file" />
          <button class="primary" :disabled="busy" @click="uploadFile">上传/替换</button>
        </footer>
      </template>
    </section>
  </div>
</template>

<style scoped>
.mask {
  position: fixed;
  inset: 0;
  z-index: 80;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 24px;
  background: rgba(0, 0, 0, 0.55);
}

.panel {
  width: min(1100px, 96vw);
  max-height: 90vh;
  display: flex;
  flex-direction: column;
  gap: 10px;
  padding: 18px;
  overflow: auto;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 14px;
  box-shadow: 0 24px 80px rgba(0, 0, 0, 0.35);
}

.head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
}

h2 {
  margin: 0;
  font-size: 18px;
}

.sub {
  margin: 4px 0 0;
  color: var(--muted);
  font-size: 12px;
}

.pathbar,
.meta,
.upload,
.editor-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.pathbar,
.meta {
  padding: 8px 10px;
  background: var(--bg-soft);
  border: 1px solid var(--border);
  border-radius: 8px;
  font-size: 12px;
}

.path {
  flex: 1;
  min-width: 240px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: var(--muted);
}

.banner {
  padding: 9px 10px;
  color: var(--text);
  background: var(--bg-soft);
  border: 1px solid var(--border);
  border-left: 3px solid var(--primary);
  border-radius: 8px;
  font-size: 12px;
}

.banner.warn {
  border-left-color: #d97706;
}

.body {
  display: grid;
  grid-template-columns: 260px minmax(0, 1fr);
  gap: 12px;
  min-height: 380px;
}

.files {
  max-height: 520px;
  overflow: auto;
  padding: 8px;
  background: var(--bg-soft);
  border: 1px solid var(--border);
  border-radius: 8px;
}

.files-title {
  margin: 2px 4px 8px;
  color: var(--muted);
  font-size: 12px;
}

.file {
  width: 100%;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 7px 8px;
  color: var(--text);
  text-align: left;
  background: transparent;
  border: 1px solid transparent;
  border-radius: 6px;
}

.file:hover,
.file.active {
  background: var(--bg);
  border-color: var(--border);
}

.file-name {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 12px;
}

.tag {
  flex: none;
  padding: 1px 5px;
  color: #92400e;
  background: #fef3c7;
  border-radius: 4px;
  font-size: 10px;
}

.editor {
  display: flex;
  flex-direction: column;
  gap: 8px;
  min-width: 0;
}

.editor-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

textarea {
  min-height: 300px;
  flex: 1;
  padding: 10px;
  color: var(--text);
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 12px;
  line-height: 1.5;
  background: var(--bg-soft);
  border: 1px solid var(--border);
  border-radius: 8px;
  resize: vertical;
}

.upload {
  padding-top: 4px;
  border-top: 1px solid var(--border);
}

.upload input[type='text'],
.upload input:not([type]) {
  flex: 1;
  min-width: 220px;
  padding: 7px 9px;
  color: var(--text);
  background: var(--bg-soft);
  border: 1px solid var(--border);
  border-radius: 7px;
}

.notice {
  margin: 0;
  color: #047857;
  font-size: 12px;
}

.error {
  margin: 0;
  color: #dc2626;
  font-size: 12px;
}

.danger {
  color: #dc2626;
}

@media (max-width: 760px) {
  .body {
    grid-template-columns: 1fr;
  }
  .files {
    max-height: 180px;
  }
}
</style>
