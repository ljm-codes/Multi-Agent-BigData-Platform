import { request } from './http'

/** 管理员列表：只能看到权限不高于自己的账号 */
export function listUsers({ limit = 20, offset = 0, disabled = false, aId = null } = {}) {
  const q = new URLSearchParams({
    limit: String(limit),
    offset: String(offset),
    disabled: String(Boolean(disabled)),
  })
  if (aId) q.set('a_id', String(aId))
  return request(`/auth/admin/admin_info?${q.toString()}`)
}

/** 生成推荐链接：recommend_id / recommend_name 必须与 token 一致 */
export function buildRecommendLink({ recommendId, recommendName, reason = '', level = 0 }) {
  return request('/auth/admin/build/link', {
    method: 'POST',
    body: {
      recommend_id: recommendId,
      recommend_name: recommendName,
      recommend_reason: reason,
      recommended_person_level: level,
    },
  })
}

/** 启用 / 封禁（逻辑删除） */
export function setAccountStatus({ id, disabled, reason = '' }) {
  return request('/auth/admin/account/status', {
    method: 'POST',
    body: { id, disabled, reason },
  })
}
