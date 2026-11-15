<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { useUserStore } from '@/stores/user'
import CalendarPicker from '@/components/CalendarPicker.vue'
import TimePicker from '@/components/TimePicker.vue'
import {
  createProfile,
  deleteArchive,
  deleteProfile,
  deletePublicSnapshot,
  downloadArchive,
  editProfileFile,
  editPublicFile,
  getPublicDrift,
  getPublicResources,
  listArchives,
  listProfileFiles,
  listProfiles,
  listPublicSnapshots,
  pruneArchives,
  readProfileFile,
  resolvePublicDrift,
  restoreArchive,
  rollbackPublic,
  runArchiveGc,
  scanPublic,
  uploadProfileFiles,
  uploadPublicFiles,
} from '@/api/clusterEnv'

const router = useRouter()
const user = useUserStore()
const isAdmin2 = computed(() => Number(user.permission ?? 0) === 2)

const tab = ref('profiles')
const error = ref('')
const notice = ref('')

// ---------------- profiles ----------------
const profiles = ref([])
const selectedProfile = ref(null)
const profileFiles = ref([])
const selectedFile = ref(null)
const fileContent = ref('')
const fileSha = ref('')
const profileReason = ref('')
const profileBusy = ref(false)
const profileLoading = ref(false)
const uploadInput = ref(null)
const createFilesInput = ref(null)
const createForm = ref({ name: '', description: '' })

// ---------------- public resources ----------------
const publicData = ref(null)
const publicSnapshots = ref([])
const publicDrift = ref(null)
const publicLoading = ref(false)
const publicReason = ref('')
const selectedPublicFile = ref('')
const publicDraft = ref('')
const publicUploadInput = ref(null)
const publicSaving = ref(false)

// ---------------- archives ----------------
const archives = ref([])
const archivesLoading = ref(false)
const archiveResult = ref(null)
const pruneDate = ref(defaultPruneDate())
const pruneTime = ref('00:00')
const pruneDryRun = ref(true)
const gcDryRun = ref(true)

const publicContents = computed(() => publicData.value?.contents || {})
const publicFiles = computed(() => (publicData.value?.files || []).map((item) => item.filename))

function flash(message) {
  notice.value = message
  setTimeout(() => (notice.value = ''), 2400)
}

function defaultPruneDate() {
  const date = new Date()
  date.setDate(date.getDate() - 30)
  return date.toISOString().slice(0, 10)
}

function setPruneDays(days) {
  const date = new Date()
  date.setDate(date.getDate() - days)
  pruneDate.value = date.toISOString().slice(0, 10)
}

function fmtTime(value) {
  if (!value) return '-'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString()
}

// ---------------- profiles ----------------
async function loadProfiles() {
  profileLoading.value = true
  error.value = ''
  try {
    profiles.value = (await listProfiles()) || []
  } catch (e) {
    error.value = e?.message || '加载 profile 失败'
  } finally {
    profileLoading.value = false
  }
}

async function createNewProfile() {
  if (!createForm.value.name.trim()) {
    error.value = 'profile 名称不能为空'
    return
  }
  profileBusy.value = true
  error.value = ''
  try {
    const files = [...(createFilesInput.value?.files || [])]
    const existing = profiles.value.find(
      (item) => item.name === createForm.value.name.trim(),
    )
    if (existing) {
      if (files.length) {
        await uploadProfileFiles(existing.id, files, existing.draft_revision,
          '选择已有 profile 时上传')
      }
      flash(files.length
        ? `profile 已存在，已上传 ${files.length} 个文件`
        : 'profile 已存在，已选中')
      createForm.value = { name: '', description: '' }
      if (createFilesInput.value) createFilesInput.value.value = ''
      await loadProfiles()
      const refreshed = profiles.value.find((p) => p.id === existing.id) || existing
      await pickProfile(refreshed)
      return
    }
    const created = await createProfile(createForm.value)
    if (files.length) {
      await uploadProfileFiles(created.id, files, created.draft_revision,
        '创建 profile 时上传')
    }
    flash(files.length ? `profile 已创建并上传 ${files.length} 个文件` : 'profile 已创建')
    createForm.value = { name: '', description: '' }
    if (createFilesInput.value) createFilesInput.value.value = ''
    await loadProfiles()
    const refreshed = profiles.value.find((p) => p.id === created.id) || created
    await pickProfile(refreshed)
  } catch (e) {
    error.value = e?.message || '创建 profile 或上传文件失败'
  } finally {
    profileBusy.value = false
  }
}

async function pickProfile(profile) {
  selectedProfile.value = profile
  selectedFile.value = null
  fileContent.value = ''
  fileSha.value = ''
  await loadProfileFiles()
}

async function loadProfileFiles() {
  if (!selectedProfile.value) return
  profileBusy.value = true
  error.value = ''
  try {
    profileFiles.value = (await listProfileFiles(
      selectedProfile.value.id, selectedProfile.value.draft_revision,
    )) || []
  } catch (e) {
    error.value = e?.message || '加载 profile 文件失败'
  } finally {
    profileBusy.value = false
  }
}

async function openProfileFile(row) {
  if (!selectedProfile.value) return
  profileBusy.value = true
  error.value = ''
  try {
    const data = await readProfileFile(
      selectedProfile.value.id, row.filename, selectedProfile.value.draft_revision,
    )
    selectedFile.value = data
    fileContent.value = data.content || ''
    fileSha.value = data.sha256 || ''
  } catch (e) {
    error.value = e?.message || '读取文件失败'
  } finally {
    profileBusy.value = false
  }
}

async function saveProfileFile() {
  if (!selectedProfile.value || !selectedFile.value) return
  profileBusy.value = true
  error.value = ''
  try {
    await editProfileFile(
      selectedProfile.value.id, selectedFile.value.filename, fileContent.value,
      fileSha.value, profileReason.value,
    )
    flash('文件已保存')
    await loadProfiles()
    const refreshed = profiles.value.find((p) => p.id === selectedProfile.value.id)
    if (refreshed) selectedProfile.value = refreshed
    await loadProfileFiles()
    selectedFile.value = null
  } catch (e) {
    error.value = e?.message || '保存文件失败'
  } finally {
    profileBusy.value = false
  }
}

async function uploadFiles() {
  if (!selectedProfile.value) return
  const files = [...(uploadInput.value?.files || [])]
  if (!files.length) return
  profileBusy.value = true
  error.value = ''
  try {
    await uploadProfileFiles(
      selectedProfile.value.id, files, selectedProfile.value.draft_revision,
      profileReason.value,
    )
    flash(`已上传 ${files.length} 个文件`)
    if (uploadInput.value) uploadInput.value.value = ''
    await loadProfiles()
    const refreshed = profiles.value.find((p) => p.id === selectedProfile.value.id)
    if (refreshed) selectedProfile.value = refreshed
    await loadProfileFiles()
  } catch (e) {
    error.value = e?.message || '上传失败'
  } finally {
    profileBusy.value = false
  }
}

function triggerProfileUpload() {
  uploadInput.value?.click()
}

async function removeProfile() {
  if (!selectedProfile.value) return
  if (!window.confirm(`删除 profile「${selectedProfile.value.name}」？被项目引用时后端会拒绝。`)) return
  profileBusy.value = true
  error.value = ''
  try {
    await deleteProfile(selectedProfile.value.id, profileReason.value)
    flash('profile 已删除')
    selectedProfile.value = null
    profileFiles.value = []
    selectedFile.value = null
    await loadProfiles()
  } catch (e) {
    error.value = e?.message || '删除 profile 失败'
  } finally {
    profileBusy.value = false
  }
}

// ---------------- public ----------------
async function loadPublic() {
  publicLoading.value = true
  error.value = ''
  try {
    publicData.value = await getPublicResources()
    if (isAdmin2.value) {
      publicSnapshots.value = (await listPublicSnapshots()) || []
      publicDrift.value = await getPublicDrift()
    }
    const names = publicFiles.value
    if (!selectedPublicFile.value || !names.includes(selectedPublicFile.value)) {
      selectedPublicFile.value = names[0] || ''
    }
    publicDraft.value = publicContents.value[selectedPublicFile.value] || ''
  } catch (e) {
    error.value = e?.message || '加载公有资源失败'
  } finally {
    publicLoading.value = false
  }
}

function selectPublicFile(name) {
  selectedPublicFile.value = name
  publicDraft.value = publicContents.value[name] || ''
}

async function savePublicFile() {
  if (!isAdmin2.value || !selectedPublicFile.value) return
  publicSaving.value = true
  error.value = ''
  try {
    await editPublicFile(selectedPublicFile.value, publicDraft.value, publicReason.value)
    flash('已保存并生成新的公有快照')
    await loadPublic()
  } catch (e) {
    error.value = e?.message || '保存公有配置失败'
  } finally {
    publicSaving.value = false
  }
}

async function uploadPublicFilesFromInput() {
  if (!isAdmin2.value) return
  const files = [...(publicUploadInput.value?.files || [])]
  if (!files.length) return
  publicSaving.value = true
  error.value = ''
  try {
    await uploadPublicFiles(files, publicReason.value)
    flash(`已上传 ${files.length} 个公有配置文件`)
    if (publicUploadInput.value) publicUploadInput.value.value = ''
    await loadPublic()
  } catch (e) {
    error.value = e?.message || '上传公有配置失败'
  } finally {
    publicSaving.value = false
  }
}

async function onScanPublic() {
  if (!window.confirm('扫描 cluster_resources/ 生成新的公有快照？')) return
  publicLoading.value = true
  error.value = ''
  try {
    await scanPublic(publicReason.value)
    flash('公有资源已扫描')
    await loadPublic()
  } catch (e) {
    error.value = e?.message || '扫描失败'
  } finally {
    publicLoading.value = false
  }
}

async function onRollbackPublic(snapshot) {
  if (!window.confirm(`回滚 cluster_resources/ 到版本 ${snapshot.version}？`)) return
  publicLoading.value = true
  error.value = ''
  try {
    await rollbackPublic(snapshot.snapshot_id, publicReason.value)
    flash('已回滚公有资源')
    await loadPublic()
  } catch (e) {
    error.value = e?.message || '回滚失败'
  } finally {
    publicLoading.value = false
  }
}

async function onDeletePublicSnapshot(row) {
  if (row.snapshot_id === publicData.value?.snapshot_id) {
    error.value = '当前公有快照不能删除'
    return
  }
  if (!window.confirm(`删除公有快照 v${row.version}？至少要保留当前快照。`)) return
  publicLoading.value = true
  error.value = ''
  try {
    await deletePublicSnapshot(row.snapshot_id, '前端删除公有快照')
    flash('公有快照已删除')
    await loadPublic()
  } catch (e) {
    error.value = e?.message || '删除公有快照失败'
  } finally {
    publicLoading.value = false
  }
}

async function onResolvePublic(decision) {
  if (!publicDrift.value?.drift_id) return
  if (!window.confirm(decision === 'rollback' ? '回滚公有资源？' : '保留当前 cluster_resources/ 修改？')) return
  publicLoading.value = true
  error.value = ''
  try {
    await resolvePublicDrift(publicDrift.value.drift_id, decision, publicReason.value)
    flash('公有 drift 已处理')
    await loadPublic()
  } catch (e) {
    error.value = e?.message || '处理公有 drift 失败'
  } finally {
    publicLoading.value = false
  }
}

// ---------------- archives ----------------
async function loadArchives() {
  if (!isAdmin2.value) return
  archivesLoading.value = true
  error.value = ''
  try {
    archives.value = ((await listArchives()) || []).filter(
      (row) => row.status !== 'deleted',
    )
  } catch (e) {
    error.value = e?.message || '加载归档失败'
  } finally {
    archivesLoading.value = false
  }
}

async function onDownloadArchive(row) {
  try {
    const { blob, filename } = await downloadArchive(row.archive_id)
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = filename || `archive-${row.archive_id}.tar.gz`
    link.click()
    URL.revokeObjectURL(url)
  } catch (e) {
    error.value = e?.message || '下载失败'
  }
}

async function onDeleteArchive(row) {
  if (!window.confirm(`删除归档 #${row.archive_id}？该操作会删磁盘上的 tar.gz。`)) return
  try {
    await deleteArchive(row.archive_id, '前端删除归档')
    flash('归档已删除')
    await loadArchives()
  } catch (e) {
    error.value = e?.message || '删除归档失败'
  }
}

async function onRestoreArchive(row) {
  if (!window.confirm(`从归档 #${row.archive_id} 恢复快照？`)) return
  try {
    await restoreArchive(row.archive_id, '前端恢复归档')
    flash('已恢复归档')
    await loadArchives()
    await loadPublic()
  } catch (e) {
    error.value = e?.message || '恢复归档失败'
  }
}

async function onPrune() {
  if (!pruneDate.value) {
    error.value = '请选择截止日期'
    return
  }
  const iso = new Date(`${pruneDate.value}T${pruneTime.value || '00:00'}:00`).toISOString()
  try {
    archiveResult.value = await pruneArchives(iso, pruneDryRun.value)
    flash(pruneDryRun.value ? '已 dry-run' : '已执行 prune')
    if (!pruneDryRun.value) await loadArchives()
  } catch (e) {
    error.value = e?.message || 'prune 失败'
  }
}

async function onGc() {
  try {
    archiveResult.value = await runArchiveGc(gcDryRun.value, '前端 GC')
    flash(gcDryRun.value ? '已 dry-run GC' : '已执行 GC')
    if (!gcDryRun.value) await loadArchives()
  } catch (e) {
    error.value = e?.message || 'GC 失败'
  }
}

function switchTab(next) {
  tab.value = next
  error.value = ''
  if (next === 'public') loadPublic()
  if (next === 'archives') loadArchives()
}

onMounted(async () => {
  await loadProfiles()
})

watch(isAdmin2, (value) => {
  if (value && tab.value === 'archives') loadArchives()
})
</script>

<template>
  <div class="env">
    <header class="head">
      <button class="ghost" @click="router.push('/')">← 返回</button>
      <h1>集群环境</h1>
      <span class="muted">{{ user.username }} · 权限 {{ user.permission }}</span>
    </header>

    <div class="tabs">
      <button :class="['tab', { active: tab === 'profiles' }]" @click="switchTab('profiles')">环境配置</button>
      <button :class="['tab', { active: tab === 'public' }]" @click="switchTab('public')">cluster_resources</button>
      <button v-if="isAdmin2" :class="['tab', { active: tab === 'archives' }]" @click="switchTab('archives')">归档</button>
    </div>

    <p v-if="error" class="error">{{ error }}</p>
    <p v-if="notice" class="notice">{{ notice }}</p>

    <section v-if="tab === 'profiles'" class="layout">
      <aside class="card list">
        <div class="card-head">
          <h2>环境 profile</h2>
          <button class="ghost small" :disabled="profileLoading" @click="loadProfiles">刷新</button>
        </div>
        <p v-if="profileLoading" class="muted">加载中…</p>
        <p v-else-if="!profiles.length" class="muted">还没有 profile</p>
        <button
          v-for="item in profiles"
          :key="item.id"
          :class="['row', { active: selectedProfile?.id === item.id }]"
          @click="pickProfile(item)"
        >
          <span>{{ item.name }}</span>
          <span class="muted small">r{{ item.draft_revision }}</span>
        </button>
      </aside>

      <div class="main-col">
        <section class="card">
          <h2>新建 profile</h2>
          <p class="muted small">
            profile 就是一组环境配置文件。先建一个名字，然后上传 core-site.xml / hive-site.xml /
            flink-conf.yaml 等；参数从文件里读，不需要手填 key。
            允许的文件：core-site.xml、hdfs-site.xml、yarn-site.xml、mapred-site.xml、
            hive-site.xml、hbase-site.xml、flink-conf.yaml、server.properties。
            如果名称已存在，会直接选择已有 profile 并上传，不会重复创建。
          </p>
          <div class="form-grid">
            <label>名称<input v-model="createForm.name" placeholder="例如 prod-cluster" /></label>
            <label class="wide">描述<input v-model="createForm.description" placeholder="可选" /></label>
            <label class="wide">
              配置文件（可多选，和创建一起上传）
              <input ref="createFilesInput" type="file" multiple />
            </label>
          </div>
          <button class="primary" :disabled="profileBusy" @click="createNewProfile">
            {{ profileBusy ? '创建/上传中…' : '创建/选择并上传' }}
          </button>
          <p v-if="!selectedProfile" class="muted small">
            也可以先创建空 profile，再在左侧选中后点“上传配置文件”。
          </p>
        </section>

        <section v-if="selectedProfile" class="card">
          <div class="card-head">
            <h2>{{ selectedProfile.name }}</h2>
            <div class="ops">
              <button class="primary small" :disabled="profileBusy" @click="triggerProfileUpload">上传配置文件</button>
              <button class="ghost small danger" :disabled="profileBusy" @click="removeProfile">删除 profile</button>
            </div>
          </div>
          <p class="muted small">
            draft revision {{ selectedProfile.draft_revision }} ·
            current revision {{ selectedProfile.current_revision }} ·
            {{ selectedProfile.status }}
          </p>

          <div class="profile-body">
            <aside class="files">
              <p class="muted small">文件</p>
              <button
                v-for="row in profileFiles"
                :key="row.filename"
                :class="['row', { active: selectedFile?.filename === row.filename }]"
                @click="openProfileFile(row)"
              >
                {{ row.filename }}
              </button>
              <p v-if="!profileFiles.length" class="muted small">还没有文件，先上传</p>
            </aside>

            <div class="editor">
              <template v-if="selectedFile">
                <div class="editor-head">
                  <b>{{ selectedFile.filename }}</b>
                  <span class="muted small">{{ selectedFile.sha256 }}</span>
                </div>
                <textarea v-model="fileContent" spellcheck="false"></textarea>
                <div class="editor-actions">
                  <input v-model="profileReason" placeholder="修改原因（可选）" />
                  <button class="primary" :disabled="profileBusy" @click="saveProfileFile">保存文件</button>
                </div>
              </template>
              <p v-else class="muted">选择左侧文件编辑，或在下方上传。</p>

              <div class="upload">
                <input ref="uploadInput" type="file" multiple />
                <button class="ghost" :disabled="profileBusy" @click="uploadFiles">上传到当前 revision</button>
              </div>
            </div>
          </div>
        </section>
      </div>
    </section>

    <section v-else-if="tab === 'public'" class="main-col">
      <section class="card">
        <div class="card-head">
          <h2>cluster_resources</h2>
          <button class="ghost small" :disabled="publicLoading" @click="loadPublic">刷新</button>
        </div>
        <p class="muted small">
          只允许配置 allowlist 文件；这里改的是所有项目共享的公有资源目录。
        </p>
        <p v-if="publicLoading" class="muted">加载中…</p>
        <template v-else-if="publicData">
          <p>
            当前版本 <b>v{{ publicData.version }}</b> ·
            snapshot {{ publicData.snapshot_id }} ·
            <span class="muted">{{ publicData.bundle_hash }}</span>
          </p>
          <div class="file-tabs">
            <button
              v-for="name in publicFiles"
              :key="name"
              :class="['row', { active: selectedPublicFile === name }]"
              @click="selectPublicFile(name)"
            >
              {{ name }}
            </button>
          </div>
          <template v-if="selectedPublicFile">
            <textarea
              v-model="publicDraft"
              class="public-editor"
              :readonly="!isAdmin2"
              spellcheck="false"
            ></textarea>
            <p v-if="!isAdmin2" class="muted small">只有权限 2 的管理员可以修改公有配置。</p>
          </template>
          <p v-else class="muted">选择文件查看内容</p>
        </template>
        <p v-else class="muted">还没有公有快照</p>

        <div v-if="isAdmin2" class="actions">
          <input v-model="publicReason" placeholder="操作原因（可选）" />
          <button class="primary" :disabled="publicSaving || !selectedPublicFile" @click="savePublicFile">
            保存当前文件
          </button>
          <input ref="publicUploadInput" type="file" multiple class="hidden-file"
                 @change="uploadPublicFilesFromInput" />
          <button class="ghost" :disabled="publicSaving" @click="publicUploadInput?.click()">
            上传配置文件
          </button>
          <button class="ghost" :disabled="publicLoading" @click="onScanPublic">重新扫描 cluster_resources</button>
        </div>

        <div v-if="publicDrift && publicDrift.clean === false" class="banner warn">
          cluster_resources 与当前公有快照不一致。
          <button class="ghost small" @click="onResolvePublic('keep')">保留修改</button>
          <button class="ghost small danger" @click="onResolvePublic('rollback')">回滚</button>
        </div>
      </section>

      <section v-if="isAdmin2" class="card">
        <h2>公有快照历史</h2>
        <table class="table">
          <thead><tr><th>版本</th><th>snapshot</th><th>来源</th><th>时间</th><th>操作</th></tr></thead>
          <tbody>
            <tr v-for="row in publicSnapshots" :key="row.snapshot_id">
              <td>v{{ row.version }}</td>
              <td>{{ row.snapshot_id }}</td>
              <td>{{ row.source }}</td>
              <td class="muted">{{ fmtTime(row.created_at) }}</td>
              <td class="ops">
                <button class="ghost small" @click="onRollbackPublic(row)">回滚到此版本</button>
                <button
                  v-if="row.snapshot_id !== publicData?.snapshot_id"
                  class="ghost small danger"
                  @click="onDeletePublicSnapshot(row)"
                >
                  删除
                </button>
                <button
                  v-else
                  class="ghost small"
                  disabled
                  title="至少要保留一个公有快照，当前版本不能删除"
                >
                  删除
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </section>
    </section>

    <section v-else-if="tab === 'archives' && isAdmin2" class="main-col">
      <section class="card">
        <div class="card-head">
          <h2>快照归档</h2>
          <button class="ghost small" :disabled="archivesLoading" @click="loadArchives">刷新</button>
        </div>
        <p v-if="archivesLoading" class="muted">加载中…</p>
        <table v-else class="table">
          <thead><tr><th>archive</th><th>scope</th><th>大小</th><th>状态</th><th>时间</th><th>操作</th></tr></thead>
          <tbody>
            <tr v-for="row in archives" :key="row.archive_id">
              <td>#{{ row.archive_id }} <span class="muted small">{{ row.path }}</span></td>
              <td>{{ row.scope }}</td>
              <td>{{ row.size_bytes }}</td>
              <td>{{ row.status }}</td>
              <td class="muted">{{ fmtTime(row.created_at) }}</td>
              <td class="ops">
                <button class="ghost small" :disabled="row.status !== 'archived'" @click="onDownloadArchive(row)">下载</button>
                <button class="ghost small" :disabled="row.status !== 'archived'" @click="onRestoreArchive(row)">恢复</button>
                <button class="ghost small danger" @click="onDeleteArchive(row)">删除</button>
              </td>
            </tr>
          </tbody>
        </table>
      </section>

      <section class="card">
        <h2>清理 / GC</h2>
        <div class="form-grid">
          <label class="picker-label">
            截止日期
            <CalendarPicker v-model="pruneDate" />
          </label>
          <label class="picker-label">
            时间
            <TimePicker v-model="pruneTime" />
          </label>
          <label class="check"><input v-model="pruneDryRun" type="checkbox" /> dry-run</label>
          <button class="ghost" @click="onPrune">执行 prune</button>
          <label class="check"><input v-model="gcDryRun" type="checkbox" /> dry-run</label>
          <button class="ghost" @click="onGc">执行 GC</button>
        </div>
        <pre v-if="archiveResult" class="result">{{ JSON.stringify(archiveResult, null, 2) }}</pre>
      </section>
    </section>
  </div>
</template>

<style scoped>
.env {
  height: 100%;
  overflow: auto;
  padding: 18px 22px 40px;
  background: var(--bg);
  font-size: 14px;
  line-height: 1.45;
}

.head {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 14px;
}

h1 {
  margin: 0;
  font-size: 20px;
}

.tabs {
  display: flex;
  gap: 6px;
  margin-bottom: 14px;
  border-bottom: 1px solid var(--border);
}

.tab {
  padding: 9px 14px;
  color: var(--muted);
  background: transparent;
  border: 0;
  border-bottom: 2px solid transparent;
  font-size: 14px;
}

.tab.active {
  color: var(--text);
  border-bottom-color: var(--primary);
}

.layout {
  display: grid;
  grid-template-columns: 280px minmax(0, 1fr);
  gap: 14px;
}

.main-col {
  display: flex;
  flex-direction: column;
  gap: 14px;
  min-width: 0;
}

.card {
  padding: 14px;
  background: var(--bg-soft);
  border: 1px solid var(--border);
  border-radius: 10px;
}

.card-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  margin-bottom: 8px;
}

h2 {
  margin: 0 0 8px;
  font-size: 17px;
}

.card-head h2 {
  margin: 0;
}

.list {
  align-self: start;
  max-height: 70vh;
  overflow: auto;
}

.row {
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
  font-size: 13.5px;
}

.row:hover,
.row.active {
  background: var(--bg);
  border-color: var(--border);
}

.form-grid {
  display: flex;
  flex-wrap: wrap;
  align-items: end;
  gap: 10px;
  margin-bottom: 10px;
}

.form-grid label {
  display: flex;
  flex-direction: column;
  gap: 4px;
  color: var(--muted);
  font-size: 13px;
}

.form-grid .wide {
  flex: 1;
  min-width: 220px;
}

.form-grid input {
  min-width: 190px;
  padding: 7px 9px;
  color: var(--text);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 7px;
  font-size: 14px;
}

.form-grid .check {
  flex-direction: row;
  align-items: center;
  gap: 7px;
  font-size: 14px;
}

.form-grid .check input[type='checkbox'] {
  width: 18px;
  height: 18px;
  accent-color: var(--primary);
}

.picker-label {
  align-self: flex-start;
}

.form-grid input[type='file'] {
  color: var(--muted);
  font-size: 13px;
}

.profile-body {
  display: grid;
  grid-template-columns: 240px minmax(0, 1fr);
  gap: 12px;
}

.files {
  max-height: 460px;
  overflow: auto;
  padding-right: 4px;
}

.editor {
  display: flex;
  flex-direction: column;
  gap: 8px;
  min-width: 0;
}

.editor-head,
.editor-actions,
.upload,
.actions {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.editor-head b {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 12px;
}

textarea {
  min-height: 280px;
  padding: 10px;
  color: var(--text);
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 13.5px;
  line-height: 1.55;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 8px;
  resize: vertical;
}

textarea.public-editor {
  width: 100%;
  min-height: 240px;
  margin-top: 8px;
}

.editor-actions input,
.actions input {
  flex: 1;
  min-width: 180px;
  padding: 7px 9px;
  color: var(--text);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 7px;
}

.file-tabs {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin: 8px 0;
}

.file-tabs .row {
  width: auto;
}

.table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13.5px;
}

.table th,
.table td {
  padding: 8px 9px;
  font-size: 13.5px;
  text-align: left;
  border-bottom: 1px solid var(--border);
  vertical-align: top;
}

.table th {
  font-weight: 500;
  color: var(--muted);
  font-size: 13.5px;
}

.ops {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
}

.hidden-file {
  display: none;
}

.quick-picks {
  display: flex;
  align-items: center;
  gap: 4px;
}

.banner {
  margin-top: 10px;
  padding: 9px 10px;
  background: var(--bg);
  border: 1px solid var(--border);
  border-left: 3px solid var(--primary);
  border-radius: 8px;
  font-size: 12px;
}

.banner.warn {
  border-left-color: #d97706;
}

.result {
  max-height: 240px;
  overflow: auto;
  padding: 10px;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 8px;
  font-size: 11px;
}

.notice {
  margin: 0 0 8px;
  color: #047857;
  font-size: 12px;
}

.error {
  margin: 0 0 8px;
  color: #dc2626;
  font-size: 12px;
}

.danger {
  color: #dc2626;
}

.muted {
  color: var(--muted);
}

.small {
  font-size: 12px;
}

@media (max-width: 860px) {
  .layout,
  .profile-body {
    grid-template-columns: 1fr;
  }
  .list {
    max-height: 220px;
  }
}
</style>
