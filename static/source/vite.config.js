import { fileURLToPath, URL } from 'node:url'
import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// 开发时走 vite 代理到后端 8000，生产由后端静态托管，所以 base 用 /
const backend = 'http://127.0.0.1:8000'

export default defineConfig({
  plugins: [vue()],
  resolve: {
    alias: { '@': fileURLToPath(new URL('./src', import.meta.url)) },
  },
  build: {
    // 构建产物直接落到 static/dist，后端从这里托管
    outDir: fileURLToPath(new URL('../dist', import.meta.url)),
    emptyOutDir: true,
  },
  server: {
    port: 5173,
    proxy: {
      '/ai': backend,
      '/auth/admin': backend,
      '/open/app': backend,
    },
  },
})
