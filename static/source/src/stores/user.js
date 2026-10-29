import { defineStore } from 'pinia'
import { clearToken, getToken, setToken } from '@/api/http'
import * as authApi from '@/api/auth'

export const useUserStore = defineStore('user', {
  state: () => ({
    token: getToken(),
    profile: null,
  }),
  getters: {
    isLoggedIn: (state) => Boolean(state.token),
    adminId: (state) => state.profile?.id ?? null,
    permission: (state) => state.profile?.permission ?? null,
  },
  actions: {
    async login(email, password) {
      const data = await authApi.login(email, password)
      this.token = data.token
      this.profile = data.payload || null
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
