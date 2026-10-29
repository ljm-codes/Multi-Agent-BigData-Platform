<script setup>
import { ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useUserStore } from '@/stores/user'

const router = useRouter()
const route = useRoute()
const user = useUserStore()

const email = ref('')
const password = ref('')
const loading = ref(false)
const error = ref('')

async function submit() {
  if (!email.value.trim() || !password.value) {
    error.value = '请输入邮箱和密码'
    return
  }
  loading.value = true
  error.value = ''
  try {
    await user.login(email.value.trim(), password.value)
    router.replace(route.query.redirect || '/')
  } catch (e) {
    error.value = e?.message || '登录失败'
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div class="auth-page">
    <form class="auth-card" @submit.prevent="submit">
      <h1>Multi-Agent 大数据平台</h1>
      <p class="muted">使用管理员账号登录</p>

      <label for="email">邮箱</label>
      <input id="email" v-model="email" type="email" autocomplete="username" placeholder="name@example.com" />

      <label for="password">密码</label>
      <input id="password" v-model="password" type="password" autocomplete="current-password" placeholder="请输入密码" />

      <p v-if="error" class="error">{{ error }}</p>

      <button class="primary submit" type="submit" :disabled="loading">
        {{ loading ? '登录中…' : '登录' }}
      </button>
    </form>
  </div>
</template>

<style scoped>
.auth-page {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  background: var(--bg-soft);
}
.auth-card {
  width: 360px;
  padding: 28px;
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-radius: var(--radius);
}
h1 { margin: 0 0 6px; font-size: 18px; font-weight: 600; }
.muted { margin: 0 0 20px; font-size: 13px; color: var(--text-muted); }
label { display: block; margin: 14px 0 6px; font-size: 13px; color: var(--text-muted); }
input {
  width: 100%;
  padding: 9px 11px;
  color: var(--text);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 8px;
  outline: none;
}
input:focus { border-color: var(--primary); }
.error { margin: 12px 0 0; font-size: 13px; color: var(--danger); }
.submit { width: 100%; margin-top: 20px; }
</style>
