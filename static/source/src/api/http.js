// 后端 main.py 的校验前缀是 "Bearer token "（后面还带一个 token 单词）
// 如果后端改成 "Bearer "，只改这一个常量即可
export const AUTH_PREFIX = 'Bearer token '

export class ApiError extends Error {
  constructor(message, code = -1) {
    super(message)
    this.name = 'ApiError'
    this.code = code
  }
}

export function getToken() {
  return localStorage.getItem('token') || ''
}

export function setToken(token) {
  localStorage.setItem('token', token)
}

export function clearToken() {
  localStorage.removeItem('token')
}

async function toJson(resp) {
  const text = await resp.text()
  if (!text) return null
  try {
    return JSON.parse(text)
  } catch {
    return null
  }
}

/**
 * 统一请求：自动带 token、解 {code, msg, data} 信封、401 时清掉本地 token
 */
export async function request(path, { method = 'GET', body, auth = true } = {}) {
  const headers = {}
  if (body !== undefined) headers['Content-Type'] = 'application/json'
  if (auth && getToken()) headers.Authorization = AUTH_PREFIX + getToken()

  const resp = await fetch(path, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  })
  const payload = await toJson(resp)

  if (resp.status === 401) {
    clearToken()
    throw new ApiError(payload?.msg || '登录已失效，请重新登录', 401)
  }
  if (!resp.ok) {
    throw new ApiError(payload?.msg || `请求失败（HTTP ${resp.status}）`, payload?.code ?? resp.status)
  }
  // 业务信封：code 为 0 才算成功
  if (payload && typeof payload === 'object' && 'code' in payload && payload.code !== 0) {
    throw new ApiError(payload.msg || '请求失败', payload.code)
  }
  return payload?.data ?? null
}
