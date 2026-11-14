import { createApp } from 'vue'
import { createPinia } from 'pinia'
import App from './App.vue'
import router from './router'
import { installFocusDiagnostics } from './utils/focusDiag'
import './styles/main.css'

installFocusDiagnostics()

createApp(App).use(createPinia()).use(router).mount('#app')
