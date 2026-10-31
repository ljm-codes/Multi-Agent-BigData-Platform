import MarkdownIt from 'markdown-it'

const TITLE_KEY = 'thread_titles'

/** 标题规则按需求：首个用户消息的前 10 个字符 */
export function titleFromText(text, size = 10) {
  const clean = String(text || '').replace(/\s+/g, ' ').trim()
  if (!clean) return '新对话'
  return clean.length > size ? clean.slice(0, size) : clean
}

export function loadTitles() {
  try {
    return JSON.parse(localStorage.getItem(TITLE_KEY) || '{}')
  } catch {
    return {}
  }
}

export function saveTitle(threadId, title) {
  if (!threadId || !title) return
  const all = loadTitles()
  all[threadId] = title
  localStorage.setItem(TITLE_KEY, JSON.stringify(all))
}

export function removeTitle(threadId) {
  const all = loadTitles()
  delete all[threadId]
  localStorage.setItem(TITLE_KEY, JSON.stringify(all))
}

export function shortId(threadId) {
  const tail = String(threadId || '').split(':').pop() || ''
  return tail.slice(0, 6)
}

/** 把历史消息的类名映射成角色：HumanMessage -> user，其余按 assistant 处理 */
export function roleOf(type) {
  return String(type || '').includes('Human') ? 'user' : 'assistant'
}

/** 把后端的 from_/name 归一成节点名，用于气泡上的 Agent 标签 */
export function normalizeAgent(name) {
  const raw = String(name || '').trim()
  if (!raw) return ''
  if (raw === 'code_verify' || raw === 'verify_agent' || raw.startsWith('ver_')) return 'code_verify'
  if (raw === 'data_analysis' || raw === 'data_analysis_agent') return 'data_analysis'
  if (raw === 'data_develop' || raw === 'data_develop_agent') return 'data_develop'
  if (['work_end', 'dis_chat', 'handler', 'distribute_task_to_agent'].includes(raw)) return 'dis_chat'
  return raw
}

export function escapeHtml(text) {
  return String(text ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
}

const md = new MarkdownIt({
  html: false,
  linkify: true,
  breaks: true,
})

// 模型偶尔把 "## 完成情况" 接在句子里；markdown-it 只认行首标题，这里先拆行
function normalizeInlineHeadings(text) {
  return String(text || '').replace(/([^\n`])\s+(#{2,6})\s+/g, '$1\n\n$2 ')
}

const defaultFence = md.renderer.rules.fence
md.renderer.rules.fence = (tokens, idx, options, env, self) => {
  const token = tokens[idx]
  const info = (token.info || '').trim().split(/\s+/)[0].toLowerCase()
  // 空代码块不渲染，避免出现空灰框
  if (!token.content.trim()) return ''
  // mermaid 交给前端 mermaid.run 处理
  if (info === 'mermaid') {
    return `<pre class="mermaid">${escapeHtml(token.content)}</pre>`
  }
  return defaultFence
    ? defaultFence(tokens, idx, options, env, self)
    : `<pre><code>${escapeHtml(token.content)}</code></pre>`
}

const defaultLinkOpen = md.renderer.rules.link_open
  || ((tokens, idx, options, env, self) => self.renderToken(tokens, idx, options))
md.renderer.rules.link_open = (tokens, idx, options, env, self) => {
  tokens[idx].attrSet('target', '_blank')
  tokens[idx].attrSet('rel', 'noreferrer')
  return defaultLinkOpen(tokens, idx, options, env, self)
}

// 宽表格放在可横向滚动的容器里
const defaultTableOpen = md.renderer.rules.table_open
  || ((tokens, idx, options, env, self) => self.renderToken(tokens, idx, options))
const defaultTableClose = md.renderer.rules.table_close
  || ((tokens, idx, options, env, self) => self.renderToken(tokens, idx, options))
md.renderer.rules.table_open = (tokens, idx, options, env, self) =>
  `<div class="md-table-wrap">${defaultTableOpen(tokens, idx, options, env, self)}`
md.renderer.rules.table_close = (tokens, idx, options, env, self) =>
  `${defaultTableClose(tokens, idx, options, env, self)}</div>`

/**
 * 用 markdown-it 渲染；代码块中的 mermaid 由 MarkdownContent 之后调用 mermaid.run。
 * 所有原始 HTML 关闭（html:false），链接走 markdown-it 的协议校验。
 */
export function renderMarkdownLite(text) {
  return md.render(normalizeInlineHeadings(text))
}
