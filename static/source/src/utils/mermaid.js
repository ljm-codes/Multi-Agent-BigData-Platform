let mermaidPromise = null

function loadMermaid() {
  if (!mermaidPromise) {
    // Mermaid 很大，按需加载，避免阻塞首屏
    mermaidPromise = import('mermaid').then((mod) => mod.default || mod)
  }
  return mermaidPromise
}

/**
 * 模型常见的 Mermaid 语法问题：节点标签里有括号但没加引号、多条语句挤在一行。
 * 例：A[transformDataFrame S + lit(P)]E --> F
 * 修成：A["transformDataFrame S + lit(P)"]
 *       E --> F
 */
function repairMermaid(text) {
  let s = String(text || '').trim()
  s = s.replace(/^```(?:mermaid)?\s*/i, '').replace(/```$/, '').trim()

  // 方括号标签里出现括号但没加引号 -> 加双引号
  s = s.replace(/\[([^\[\]\n]*\([^\[\]\n]*\)[^\[\]\n]*)\]/g, (match, label) => {
    const value = label.trim()
    if (value.startsWith('"') || value.startsWith('(')) return match
    return `["${label.replace(/"/g, '\\"')}"]`
  })

  // 多条语句挤在一行：]E --> F / )F --> G 等，在节点结束符后补换行
  s = s.replace(/([\]\)\}])(?=[A-Za-z_][A-Za-z0-9_]*\s*(?:-->|---|==>|\[|\(|\{))/g, '$1\n')

  // 箭头两侧统一空格
  s = s.replace(/\s*-->\s*/g, ' --> ')
  return s.split('\n').map((line) => line.trim()).filter(Boolean).join('\n')
}

/**
 * 把容器里的 <pre class="mermaid"> 就地渲染成 SVG。
 * 只处理还没渲染过的节点；流式消息应等 streaming 结束后再调用。
 */
export async function renderMermaid(root) {
  if (!root) return
  const nodes = Array.from(root.querySelectorAll('pre.mermaid:not([data-processed])'))
  if (!nodes.length) return

  const mermaid = await loadMermaid()
  const light = document.documentElement.dataset.theme === 'light'
  mermaid.initialize({
    startOnLoad: false,
    securityLevel: 'strict',
    theme: light ? 'default' : 'dark',
  })

  // 先尝试修复常见的模型语法错误；修不好的交给 run 抛错并标红
  for (const node of nodes) {
    const source = node.textContent || ''
    try {
      await mermaid.parse(source)
    } catch {
      const repaired = repairMermaid(source)
      if (repaired === source) continue
      try {
        await mermaid.parse(repaired)
        node.textContent = repaired
        console.info('[mermaid] syntax repaired')
      } catch {
        // 保留原始文本，由下面的 run 统一报错
      }
    }
  }

  try {
    await mermaid.run({ nodes })
  } catch (e) {
    // 语法错误时保留原始文本并标红，不阻塞其余消息
    console.error('[mermaid] render failed', e)
    nodes.filter((node) => !node.querySelector('svg')).forEach((node) => {
      node.classList.add('mermaid-error')
    })
  }
}
