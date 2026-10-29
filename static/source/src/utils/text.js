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

export function escapeHtml(text) {
  return String(text ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
}

/**
 * 极简 markdown：代码块、行内代码、标题、粗体、链接（含 IDE:// 链接）、换行。
 * 只用 v-html 渲染这里生成的内容，所有文本都先 escape。
 */
export function renderMarkdownLite(text) {
  const escaped = escapeHtml(text)
  const blocks = []

  // 先抽出代码块，避免内部内容被后续规则破坏
  let rest = escaped.replace(/```([a-zA-Z0-9_-]*)\n([\s\S]*?)```/g, (_, lang, code) => {
    const index = blocks.push(`<pre class="code"><code>${code}</code></pre>`) - 1
    return `\u0000BLOCK${index}\u0000`
  })

  rest = rest
    .replace(/^### (.*)$/gm, '<h4>$1</h4>')
    .replace(/^## (.*)$/gm, '<h3>$1</h3>')
    .replace(/^# (.*)$/gm, '<h3>$1</h3>')
    .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
    .replace(/`([^`\n]+)`/g, '<code>$1</code>')
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank" rel="noreferrer">$1</a>')
    .replace(/^&gt; (.*)$/gm, '<blockquote>$1</blockquote>')
    .replace(/\n{2,}/g, '</p><p>')
    .replace(/\n/g, '<br>')

  rest = `<p>${rest}</p>`
  rest = rest.replace(/\u0000BLOCK(\d+)\u0000/g, (_, i) => blocks[Number(i)] || '')
  return rest
}
