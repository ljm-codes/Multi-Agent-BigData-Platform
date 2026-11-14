/**
 * 焦点诊断：URL 带 ?focusdiag=1 时启用。
 * 用来确认「切走/最小化后回弹」到底是不是页面里的 focus() 造成的。
 */
export function installFocusDiagnostics() {
  if (typeof window === 'undefined') return
  if (!new URLSearchParams(window.location.search).has('focusdiag')) return

  const original = HTMLElement.prototype.focus
  HTMLElement.prototype.focus = function patchedFocus(...args) {
    console.warn('[focusdiag] HTMLElement.focus', this, new Error('focus call stack').stack)
    return original.apply(this, args)
  }
  const originalWindowFocus = window.focus
  window.focus = function patchedWindowFocus(...args) {
    console.warn('[focusdiag] window.focus', new Error('window.focus call stack').stack)
    return originalWindowFocus.apply(window, args)
  }
  window.addEventListener('focus', () => console.warn('[focusdiag] window focus', Date.now()))
  window.addEventListener('blur', () => console.warn('[focusdiag] window blur', Date.now()))
  document.addEventListener('visibilitychange', () => {
    console.warn('[focusdiag] visibility', document.visibilityState, Date.now())
  })
}
