import { defineStore } from 'pinia'
import { clearToken, getToken, setToken } from '@/api/http'
import * as authApi from '@/api/auth'

/** 从 JWT 里读出 payload（只做展示用，不做校验） */
function decodePayload(token) {
  try {
    const part = String(token).split('.')[1]
    if (!part) return null
    const json = atob(part.replace(/-/g, '+').replace(/_/g, '/'))
    const bytes = Uint8Array.from(json, (c) => c.charCodeAt(0))
    return JSON.parse(new TextDecoder().decode(bytes))
  } catch {
    return null
  }
}

const initialToken = getToken()

export const useUserStore = defineStore('user', {
  state: () => ({
    token: initialToken,
    profile: initialToken ? decodePayload(initialToken) : null,
  }),
  getters: {
    isLoggedIn: (state) => Boolean(state.token),
    adminId: (state) => state.profile?.id ?? null,
    username: (state) => state.profile?.username ?? '',
    permission: (state) => state.profile?.permission ?? null,
  },
  actions: {
    async login(email, password) {
      const data = await authApi.login(email, password)
      this.token = data.token
      this.profile = data.payload || decodePayload(data.token)
      setToken(data.token)
      return data
    },
    logout() {
      this.token = ''
      this.profile = null
      clearToken()
    },
  },
})
