import { request } from './http'

/** 系统里探测到的 IDE 列表：{ exe: { path, display } } */
export function getIdes() {
  return request('/open/app/get_ides')
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
