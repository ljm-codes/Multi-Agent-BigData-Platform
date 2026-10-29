import { request } from './http'

/**
 * 登录：POST /auth/admin/login
 * 后端用的是 AdminInputInfo，登录只读 email + password
 */
export function login(email, password) {
  return request('/auth/admin/login', {
    method: 'POST',
    body: { email, password },
    auth: false,
  })
}
