import { request } from './http'

function buildQuery(params = {}) {
  const q = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === '') continue
    q.set(key, String(value))
  }
  const text = q.toString()
  return text ? `?${text}` : ''
}

export function listProjects(params = {}) {
  return request(`/project/info${buildQuery(params)}`)
}

export function getProjectCommand(projectId) {
  return request(`/project/command/${encodeURIComponent(projectId)}`)
}

export function editProjectCommand(projectId, command) {
  return request(`/project/command/${encodeURIComponent(projectId)}/edit`, {
    method: 'PUT',
    body: { command },
  })
}

export function submitProjectCommand(projectId) {
  return request(`/project/command/${encodeURIComponent(projectId)}/submit`, {
    method: 'POST',
  })
}

export function deleteProject(projectId) {
  return request(`/project/${encodeURIComponent(projectId)}`, { method: 'DELETE' })
}
