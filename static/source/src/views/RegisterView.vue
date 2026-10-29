<script setup>
import { computed, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import { request } from '@/api/http'

const route = useRoute()

const code = computed(() => String(route.query.code || ''))
const info = ref(null)
const loading = ref(false)
const submitting = ref(false)
const done = ref(false)
const error = ref('')

const form = ref({ username: '', email: '', password: '' })

async function check() {
  if (!code.value) {
    error.value = '链接不完整，缺少推荐码'
    return
  }
  loading.value = true
  error.value = ''
  try {
    info.value = await request(
      `/auth/admin/recommend/register?code=${encodeURIComponent(code.value)}`,
      { auth: false }
    )
  } catch (e) {
    error.value = e?.message || '链接无效或已过期'
  } finally {
    loading.value = false
  }
}

async function submit() {
  if (!form.value.username.trim() || !form.value.email.trim() || !form.value.password) {
    error.value = '请填写完整'
    return
  }
  submitting.value = true
  error.value = ''
  try {
    await request('/auth/admin/recommend/register', {
      method: 'POST',
      auth: false,
      body: {
        username: form.value.username.trim(),
        email: form.value.email.trim(),
        password: form.value.password,
        code: code.value,
      },
    })
    done.value = true
  } catch (e) {
    error.value = e?.message || '注册失败'
  } finally {
    submitting.value = false
  }
}

onMounted(check)
</script>

<template>
  <div class="page">
    <div class="card">
      <h1>通过邀请注册</h1>

      <p v-if="loading" class="muted">正在校验邀请码…</p>

      <template v-else-if="done">
        <p class="ok">注册成功，请返回登录页使用该账号登录。</p>
        <router-link class="primary link" to="/login">去登录</router-link>
      </template>

      <template v-else-if="info">
        <p class="muted">
          邀请有效，注册后的权限等级为 <b>{{ info.recommended_level }}</b>
          <span v-if="info.expire_in">，剩余 {{ Math.floor(info.expire_in / 60) }} 分钟</span>
        </p>

        <label>用户名</label>
        <input v-model="form.username" maxlength="255" autocomplete="username" />

        <label>邮箱</label>
        <input v-model="form.email" type="email" maxlength="255" autocomplete="email" />

        <label>密码</label>
        <input v-model="form.password" type="password" autocomplete="new-password" />

        <p v-if="error" class="error">{{ error }}</p>

        <button class="primary submit" :disabled="submitting" @click="submit">
          {{ submitting ? '提交中…' : '注册' }}
        </button>
      </template>

      <template v-else>
        <p class="error">{{ error || '链接无效或已过期' }}</p>
        <router-link class="primary link" to="/login">去登录</router-link>
      </template>
    </div>
  </div>
</template>

<style scoped>
.page {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  background: var(--bg-soft);
}
.card {
  width: 380px;
  padding: 28px;
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-radius: var(--radius);
}
h1 {
  margin: 0 0 14px;
  font-size: 18px;
  font-weight: 600;
}
label {
  display: block;
  margin: 14px 0 6px;
  font-size: 13px;
  color: var(--text-muted);
}
input {
  width: 100%;
  padding: 9px 11px;
  color: var(--text);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 8px;
  outline: none;
}
input:focus {
  border-color: var(--primary);
}
.submit {
  width: 100%;
  margin-top: 18px;
}
.ok {
  color: var(--text);
}
.link {
  display: inline-block;
  margin-top: 12px;
  padding: 9px 14px;
  color: #fff;
  text-decoration: none;
  background: var(--primary);
  border-radius: 8px;
}
.error {
  margin: 12px 0 0;
  color: var(--danger);
}
</style>
