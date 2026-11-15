import { downloadBlob, postForm, request } from './http'

const BASE = '/cluster'

// ---------------- profiles ----------------
export function listProfiles() {
  return request(`${BASE}/profiles`, { expectJson: true })
}

export function createProfile(payload) {
  return request(`${BASE}/profiles`, { method: 'POST', body: payload })
}

export function getProfile(profileId) {
  return request(`${BASE}/profiles/${profileId}`, { expectJson: true })
}

export function deleteProfile(profileId, reason = '') {
  return request(`${BASE}/profiles/${profileId}?reason=${encodeURIComponent(reason)}`,
    { method: 'DELETE' })
}

export function listProfileFiles(profileId, revision = null) {
  const query = revision ? `?revision=${revision}` : ''
  return request(`${BASE}/profiles/${profileId}/files${query}`, { expectJson: true })
}

export function readProfileFile(profileId, filename, revision = null) {
  const query = revision ? `?revision=${revision}` : ''
  return request(`${BASE}/profiles/${profileId}/files/${encodeURIComponent(filename)}${query}`,
    { expectJson: true })
}

export function uploadProfileFiles(profileId, files, expectedRevision = null, reason = '') {
  const form = new FormData()
  for (const file of files) form.append('files', file)
  if (expectedRevision != null) form.append('expected_revision', String(expectedRevision))
  form.append('reason', reason)
  return postForm(`${BASE}/profiles/${profileId}/files/upload`, form)
}

export function editProfileFile(profileId, filename, content, expectedSha256 = '', reason = '') {
  return request(`${BASE}/profiles/${profileId}/files/${encodeURIComponent(filename)}`, {
    method: 'PUT',
    body: { content, expected_sha256: expectedSha256, reason },
  })
}

// ---------------- public cluster_resources ----------------
export function getPublicResources() {
  return request(`${BASE}/public`, { expectJson: true })
}

export function listPublicSnapshots() {
  return request(`${BASE}/public/snapshots`, { expectJson: true })
}

export function scanPublic(reason = '') {
  return request(`${BASE}/public/scan`, { method: 'POST', body: { reason } })
}

export function editPublicFile(filename, content, reason = '') {
  return request(`${BASE}/public/files/${encodeURIComponent(filename)}`, {
    method: 'PUT',
    body: { content, reason },
  })
}

export function uploadPublicFiles(files, reason = '') {
  const form = new FormData()
  for (const file of files) form.append('files', file)
  form.append('reason', reason)
  return postForm(`${BASE}/public/files/upload`, form)
}

export function rollbackPublic(snapshotId, reason = '') {
  return request(`${BASE}/public/rollback`, {
    method: 'POST',
    body: { snapshot_id: snapshotId, reason },
  })
}

export function deletePublicSnapshot(snapshotId, reason = '') {
  return request(`${BASE}/public/snapshots/${snapshotId}?reason=${encodeURIComponent(reason)}`,
    { method: 'DELETE' })
}

export function getPublicDrift() {
  return request(`${BASE}/public/config-drift`, { expectJson: true })
}

export function resolvePublicDrift(driftId, decision, note = '') {
  return request(`${BASE}/public/config-drift/resolve`, {
    method: 'POST',
    body: { drift_id: driftId, decision, note },
  })
}

// ---------------- archives ----------------
export function listArchives() {
  return request(`${BASE}/archives`, { expectJson: true })
}

export function downloadArchive(archiveId) {
  return downloadBlob(`${BASE}/archives/${archiveId}/download`)
}

export function deleteArchive(archiveId, note = '') {
  return request(`${BASE}/archives/${archiveId}?note=${encodeURIComponent(note)}`,
    { method: 'DELETE' })
}

export function pruneArchives(before, dryRun = true) {
  return request(`${BASE}/archives/prune`, {
    method: 'POST',
    body: { before, dry_run: dryRun },
  })
}

export function restoreArchive(archiveId, reason = '') {
  return request(`${BASE}/archives/${archiveId}/restore`, {
    method: 'POST',
    body: { reason },
  })
}

export function runArchiveGc(dryRun = true, reason = '') {
  return request(`${BASE}/archives/gc`, {
    method: 'POST',
    body: { dry_run: dryRun, reason },
  })
}

// ---------------- project resources ----------------
export function getProjectResources(projectId) {
  return request(`${BASE}/projects/${projectId}/resources`, { expectJson: true })
}

export function replaceProjectResource(projectId, { path, file = null, remove = false, reason = '' }) {
  const form = new FormData()
  form.append('path', path)
  form.append('delete', remove ? 'true' : 'false')
  form.append('reason', reason)
  if (file) form.append('file', file)
  return postForm(`${BASE}/projects/${projectId}/resources/replace`, form)
}

export function getProjectDrift(projectId) {
  return request(`${BASE}/projects/${projectId}/config-drift`, { expectJson: true })
}

export function resolveProjectDrift(projectId, driftId, decision, note = '') {
  return request(`${BASE}/projects/${projectId}/config-drift/resolve`, {
    method: 'POST',
    body: { drift_id: driftId, decision, note },
  })
}

export function materializeProject(projectId, payload = {}) {
  return request(`${BASE}/projects/${projectId}/materialize`, {
    method: 'POST',
    body: { reason: '', ...payload },
  })
}
