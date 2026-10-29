<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { useUserStore } from '@/stores/user'
import { buildRecommendLink, listUsers, setAccountStatus } from '@/api/admin'

const router = useRouter()
const user = useUserStore()

const tab = ref('active')
const users = ref([])
const loading = ref(false)
const error = ref('')
const notice = ref('')

const level = ref(0)
const reason = ref('')
const link = ref('')
const generating = ref(false)

const canManage = computed(() => user.permission === 2)
const canInvite = computed(() => Number(user.permission ?? 0) > 0)
const levelOptions = computed(() => {
  const max = Math.min(2, Number(user.permission ?? 0))
  return [0, 1, 2].filter((value) => value <= max)
})

async function load() {
  loading.value = true
  error.value = ''
  try {
    const data = await listUsers({ disabled: tab.value === 'disabled' })
    users.value = data?.admin_data || []
  } catch (e) {
    error.value = e?.message || '加载用户列表失败'
  } finally {
    loading.value = false
  }
}

async function invite() {
  if (!user.adminId || !user.username) {
    error.value = '登录信息不完整，请重新登录'
    return
  }
  generating.value = true
  error.value = ''
  link.value = ''
  try {
    const data = await buildRecommendLink({
      recommendId: Number(user.adminId),
      recommendName: user.username,
      reason: reason.value,
      level: Number(level.value),
    })
    const path = data?.link || `/recommend/register?code=${data?.code}`
    link.value = `${location.origin}${path}`
  } catch (e) {
    error.value = e?.message || '生成推荐链接失败'
  } finally {
    generating.value = false
  }
}

async function copyLink() {
  try {
    await navigator.clipboard.writeText(link.value)
    notice.value = '已复制'
  } catch {
    notice.value = '复制失败，请手动选择'
  }
  setTimeout(() => (notice.value = ''), 1600)
}

async function toggleStatus(row) {
  const nextDisabled = !row.disabled
  const text = window.prompt(nextDisabled ? '封禁原因（可留空）' : '启用原因（可留空）', '') ?? ''
  try {
    await setAccountStatus({ id: row.id, disabled: nextDisabled, reason: text })
    await load()
  } catch (e) {
    error.value = e?.message || '操作失败'
  }
}

function formatTime(value) {
  if (!value) return '-'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString()
}

onMounted(load)
watch(tab, load)
</script>

<template>
  <div class="admin">
    <header class="head">
      <button class="ghost" @click="router.push('/')">← 返回</button>
      <h1>用户管理</h1>
      <span class="muted">{{ user.username }}</span>
    </header>

    <div class="content">
      <section v-if="canInvite" class="card">
        <h2>邀请用户</h2>
        <p class="muted">生成一个 20 分钟内有效、只能使用一次的注册链接。</p>

        <div class="form">
          <label>
            权限等级
            <select v-model.number="level">
              <option v-for="option in levelOptions" :key="option" :value="option">{{ option }}</option>
            </select>
          </label>

          <label class="reason">
            推荐原因
            <input v-model="reason" placeholder="可选" />
          </label>

          <button class="primary" :disabled="generating" @click="invite">
            {{ generating ? '生成中…' : '生成链接' }}
          </button>
        </div>

        <div v-if="link" class="link-box">
          <input :value="link" readonly />
          <button class="ghost" @click="copyLink">复制</button>
          <span v-if="notice" class="muted">{{ notice }}</span>
        </div>
      </section>

      <section class="card">
        <div class="card-head">
          <h2>账号列表</h2>
          <div class="tabs">
            <button :class="['tab', { active: tab === 'active' }]" @click="tab = 'active'">启用中</button>
            <button :class="['tab', { active: tab === 'disabled' }]" @click="tab = 'disabled'">已封禁</button>
          </div>
        </div>

        <p v-if="error" class="error">{{ error }}</p>
        <p v-if="loading" class="muted">加载中…</p>
        <p v-else-if="!users.length" class="muted">暂无数据</p>

        <table v-else class="table">
          <thead>
            <tr>
              <th>ID</th>
              <th>用户名</th>
              <th>邮箱</th>
              <th>权限</th>
              <th>创建时间</th>
              <th v-if="canManage">操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in users" :key="row.id">
              <td>{{ row.id }}</td>
              <td>{{ row.username }}</td>
              <td class="muted">{{ row.email }}</td>
              <td>{{ row.permission }}</td>
              <td class="muted">{{ formatTime(row.create_time) }}</td>
              <td v-if="canManage">
                <button class="ghost small" :class="{ danger: !row.disabled }" @click="toggleStatus(row)">
                  {{ row.disabled ? '启用' : '封禁' }}
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </section>
    </div>
  </div>
</template>

<style scoped>
.admin {
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
  display: flex;
  flex-direction: column;
  gap: 16px;
}
.card {
  padding: 16px 18px;
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-radius: var(--radius);
}
.card h2 {
  margin: 0 0 6px;
  font-size: 14px;
  font-weight: 600;
}
.card-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 10px;
}
.form {
  display: flex;
  gap: 10px;
  align-items: flex-end;
  margin-top: 12px;
  flex-wrap: wrap;
}
.form label {
  display: flex;
  flex-direction: column;
  gap: 5px;
  font-size: 12px;
  color: var(--text-muted);
}
.form select,
.form input {
  padding: 7px 9px;
  color: var(--text);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 8px;
  outline: none;
}
.reason input {
  width: 240px;
}
.link-box {
  display: flex;
  gap: 8px;
  align-items: center;
  margin-top: 12px;
}
.link-box input {
  flex: 1;
  padding: 8px 10px;
  font-size: 12px;
  color: var(--text);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 8px;
}
.tabs {
  display: flex;
  gap: 6px;
}
.tab {
  padding: 5px 10px;
  font-size: 12px;
  color: var(--text-muted);
  background: transparent;
  border: 1px solid transparent;
  border-radius: 999px;
}
.tab.active {
  color: var(--text);
  background: var(--bg);
  border-color: var(--border);
}
.table {
  width: 100%;
  margin-top: 8px;
  font-size: 13px;
  border-collapse: collapse;
}
.table th,
.table td {
  padding: 9px 8px;
  text-align: left;
  border-bottom: 1px solid var(--border);
}
.table th {
  font-size: 12px;
  font-weight: 500;
  color: var(--text-muted);
}
.ghost.small {
  padding: 4px 9px;
  font-size: 12px;
}
.ghost.danger {
  color: var(--danger);
  border-color: var(--danger);
}
</style>
