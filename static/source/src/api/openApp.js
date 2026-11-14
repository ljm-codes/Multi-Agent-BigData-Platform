import { request } from './http'

/** 系统里探测到的 IDE 列表：{ exe: { path, display } } */
export function getIdes() {
  return request('/open/app/get_ides')
}

/** 当前会话对应的项目目录；后端会从 state / 工具轨迹 / tmp_code 扫描解析 */
export function resolveProjectDir(threadId) {
  return request(`/ai/project/path?thread_id=${encodeURIComponent(threadId)}`, { expectJson: true })
}

/** 打开文件并跳到指定行；ide_name 用 detect_ides 返回的 key */
export function openFile({ filePath, ideName, idePath = '', startIdx = 1 }) {
  return request('/open/app/open_file', {
    method: 'POST',
    body: {
      file_path: filePath,
      ide_name: ideName,
      ide_path: idePath,
      start_idx: startIdx,
    },
  })
}
