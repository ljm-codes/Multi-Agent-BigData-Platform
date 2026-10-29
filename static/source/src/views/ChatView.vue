<script setup>
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { useUserStore } from '@/stores/user'

const user = useUserStore()
const router = useRouter()

const collapsed = ref(false)
const tab = ref('active')
const sidebarWidth = ref(260)
const resizing = ref(false)

function startResize(e) {
  e.preventDefault()
  resizing.value = true
  if (collapsed.value) collapsed.value = false
  const startX = e.clientX
  const startWidth = sidebarWidth.value

  const onMove = (ev) => {
    sidebarWidth.value = Math.min(460, Math.max(180, startWidth + ev.clientX - startX))
  }
  const onUp = () => {
    resizing.value = false
    window.removeEventListener('mousemove', onMove)
    window.removeEventListener('mouseup', onUp)
  }
  window.addEventListener('mousemove', onMove)
  window.addEventListener('mouseup', onUp)
}

function logout() {
  user.logout()
  router.replace('/login')
}
</script>

<template>
  <div class="shell">
    <aside
      class="sidebar"
      :class="{ collapsed }"
      :style="{ width: collapsed ? '56px' : sidebarWidth + 'px' }"
    >
      <div class="sidebar-head">
        <button class="icon-btn" title="折叠 / 展开" @click="collapsed = !collapsed">☰</button>
        <span v-show="!collapsed" class="brand">Multi-Agent</span>
      </div>

      <button v-show="!collapsed" class="primary new-chat">＋ 新增对话</button>

      <div v-show="!collapsed" class="tabs">
        <button :class="['tab', { active: tab === 'active' }]" @click="tab = 'active'">未归档</button>
        <button :class="['tab', { active: tab === 'archived' }]" @click="tab = 'archived'">已归档</button>
      </div>

      <div class="thread-list">
        <p v-show="!collapsed" class="empty-hint">暂无对话</p>
      </div>

      <div class="sidebar-foot">
        <button class="ghost" :title="collapsed ? '退出登录' : ''" @click="logout">
          {{ collapsed ? '⏻' : '退出登录' }}
        </button>
      </div>

      <div v-show="!collapsed" class="resizer" :class="{ active: resizing }" @mousedown="startResize"></div>
    </aside>

    <main class="main">
      <header class="main-head">
        <span class="title">新对话</span>
      </header>

      <section class="messages">
        <p class="empty-hint">还没有消息，输入内容开始对话</p>
      </section>

      <footer class="composer">
        <textarea rows="1" placeholder="输入消息，Enter 发送，Shift + Enter 换行" disabled></textarea>
        <button class="primary" disabled>发送</button>
      </footer>
    </main>
  </div>
</template>

<style scoped>
.shell {
  display: flex;
  height: 100%;
  background: var(--bg);
}

.sidebar {
  position: relative;
  display: flex;
  flex-direction: column;
  flex-shrink: 0;
  background: var(--bg-soft);
  border-right: 1px solid var(--border);
  transition: width 0.15s ease;
}
.sidebar.collapsed { transition: width 0.15s ease; }

.sidebar-head {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 12px;
}
.brand { font-size: 13px; font-weight: 600; letter-spacing: 0.02em; }

.icon-btn {
  width: 32px;
  height: 32px;
  color: var(--text);
  background: transparent;
  border: 1px solid var(--border);
  border-radius: 8px;
}

.new-chat { margin: 4px 12px 10px; }

.tabs {
  display: flex;
  gap: 4px;
  padding: 0 12px 10px;
}
.tab {
  flex: 1;
  padding: 6px 0;
  font-size: 13px;
  color: var(--text-muted);
  background: transparent;
  border: 0;
  border-bottom: 1px solid transparent;
}
.tab.active {
  color: var(--text);
  border-bottom-color: var(--primary);
}

.thread-list {
  flex: 1;
  overflow-y: auto;
  padding: 4px 8px;
}

.sidebar-foot {
  padding: 10px 12px;
  border-top: 1px solid var(--border);
}

.resizer {
  position: absolute;
  top: 0;
  right: -3px;
  width: 6px;
  height: 100%;
  cursor: col-resize;
}
.resizer:hover, .resizer.active { background: var(--primary); opacity: 0.35; }

.main {
  display: flex;
  flex-direction: column;
  flex: 1;
  min-width: 0;
}

.main-head {
  display: flex;
  align-items: center;
  gap: 12px;
  height: 52px;
  padding: 0 20px;
  border-bottom: 1px solid var(--border);
}
.title { font-size: 14px; font-weight: 600; }

.messages {
  flex: 1;
  overflow-y: auto;
  padding: 20px;
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
}

.composer {
  display: flex;
  gap: 10px;
  align-items: flex-end;
  padding: 12px 20px 18px;
  border-top: 1px solid var(--border);
}
.composer textarea {
  flex: 1;
  min-height: 40px;
  max-height: 160px;
  padding: 10px 12px;
  color: var(--text);
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  border-radius: 10px;
  resize: none;
  outline: none;
}

.empty-hint {
  margin: 0;
  font-size: 13px;
  color: var(--text-muted);
  text-align: center;
}
</style>
