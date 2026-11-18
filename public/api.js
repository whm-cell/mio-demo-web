const UUID_V4_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export class ApiError extends Error {
  constructor(status, code, message, requestId = null) {
    super(message || code || `HTTP_${status}`);
    this.name = 'ApiError';
    this.status = status;
    this.code = code || `HTTP_${status}`;
    this.requestId = requestId;
  }
}

async function requestJson(url, options = {}) {
  const headers = new Headers(options.headers || {});
  if (options.json !== undefined && !headers.has('content-type')) {
    headers.set('content-type', 'application/json');
  }

  const res = await fetch(url, {
    credentials: 'include',
    method: options.method ?? (options.json !== undefined ? 'POST' : 'GET'),
    headers,
    body: options.json !== undefined ? JSON.stringify(options.json) : options.body,
    signal: options.signal,
  });

  const text = await res.text();
  let data = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = null;
    }
  }

  if (!res.ok) {
    const nested = data && typeof data === 'object' ? data.error : null;
    const code =
      (nested && typeof nested.code === 'string' && nested.code) ||
      (data && typeof data.code === 'string' && data.code) ||
      `HTTP_${res.status}`;
    const message =
      (nested && typeof nested.message === 'string' && nested.message) ||
      (data && typeof data.message === 'string' && data.message) ||
      res.statusText ||
      code;
    const requestId =
      (nested && typeof nested.requestId === 'string' && nested.requestId) ||
      res.headers.get('x-request-id');
    throw new ApiError(res.status, code, message, requestId);
  }

  return data;
}

export function normalizeImageAssetIds(assetIds) {
  if (!Array.isArray(assetIds)) return [];
  const seen = new Set();
  const out = [];
  for (const raw of assetIds) {
    if (typeof raw !== 'string') continue;
    const trimmed = raw.trim().toLowerCase();
    if (!UUID_V4_RE.test(trimmed) || seen.has(trimmed)) continue;
    seen.add(trimmed);
    out.push(trimmed);
    if (out.length >= 4) break;
  }
  return out;
}

export function getDemoConfig() {
  return requestJson('/demo-api/config');
}

export function setDemoMode(mode) {
  return requestJson('/demo-api/config', { method: 'POST', json: { mode } });
}

export function getMe() {
  return requestJson('/api/v1/me');
}

export function getSession() {
  return requestJson('/api/auth/get-session');
}

export function signInEmail(email, password) {
  return requestJson('/api/auth/sign-in/email', {
    method: 'POST',
    json: { email, password },
  });
}

export function signUpEmail(name, email, password) {
  return requestJson('/api/auth/sign-up/email', {
    method: 'POST',
    json: { name, email, password },
  });
}

export function signOut() {
  return requestJson('/api/auth/sign-out', {
    method: 'POST',
    json: {},
  });
}

export function getOnelinkProvider() {
  return requestJson('/api/v1/providers/onelink');
}

export function putOnelinkCredential(apiKey, expectedRevision) {
  return requestJson('/api/v1/providers/onelink/credential', {
    method: 'PUT',
    json: { apiKey, expectedRevision },
  });
}

export function testOnelinkCatalog() {
  return requestJson('/api/v1/providers/onelink/test', {
    method: 'POST',
    json: {},
  });
}

export function listOnelinkModels() {
  return requestJson('/api/v1/providers/onelink/models');
}

export function listConversations(limit = 50) {
  return requestJson(`/api/v1/conversations?limit=${limit}`);
}

export function createConversation(modelRef) {
  return requestJson('/api/v1/conversations', {
    method: 'POST',
    json: modelRef ? { modelRef } : {},
  });
}

export function getConversationHistory(conversationId, afterSeq = -1, limit = 100) {
  const id = encodeURIComponent(conversationId);
  return requestJson(
    `/api/v1/conversations/${id}/history?afterSeq=${encodeURIComponent(afterSeq)}&limit=${encodeURIComponent(limit)}`,
  );
}

export function sendConversationMessage(conversationId, { requestId, text, imageAssetIds }) {
  const id = encodeURIComponent(conversationId);
  const cleanedIds = normalizeImageAssetIds(imageAssetIds);
  const payload = {
    requestId,
    text,
    ...(cleanedIds.length > 0 ? { imageAssetIds: cleanedIds } : {}),
  };
  return requestJson(`/api/v1/conversations/${id}/messages`, {
    method: 'POST',
    json: payload,
  });
}

export function cancelConversationTurn(conversationId) {
  const id = encodeURIComponent(conversationId);
  return requestJson(`/api/v1/conversations/${id}/cancel`, {
    method: 'POST',
    json: {},
  });
}

export function openConversationEvents(conversationId) {
  return new EventSource(`/api/v1/conversations/${encodeURIComponent(conversationId)}/events`);
}

function createAssetUpload({ filename, contentType, size }) {
  return requestJson('/api/v1/assets/uploads', {
    method: 'POST',
    json: { filename, contentType, size },
  });
}

async function putAssetBinaryViaRelay(uploadUrl, fileOrBytes, contentType) {
  const target = encodeURIComponent(uploadUrl);
  const res = await fetch(`/demo-api/cos-upload?target=${target}`, {
    method: 'PUT',
    credentials: 'include',
    headers: {
      'content-type': contentType || 'application/octet-stream',
    },
    body: fileOrBytes,
  });
  if (!res.ok) {
    const errBody = await res.json().catch(() => null);
    throw new ApiError(
      res.status,
      errBody?.error?.code || `UPLOAD_HTTP_${res.status}`,
      errBody?.error?.message || `COS relay failed (${res.status})`,
    );
  }
}

function completeAssetUpload(assetId) {
  return requestJson(`/api/v1/assets/${encodeURIComponent(assetId)}/complete`, {
    method: 'POST',
    json: {},
  });
}

export function getAsset(assetId) {
  return requestJson(`/api/v1/assets/${encodeURIComponent(assetId)}`);
}

export async function uploadImageAsset(file, onStage) {
  onStage?.('正在申请上传地址...');
  const init = await createAssetUpload({
    filename: file.name || 'image.png',
    contentType: file.type || 'image/png',
    size: file.size,
  });
  onStage?.('正在上传图片字节到对象存储...');
  await putAssetBinaryViaRelay(init.uploadUrl, file, file.type || 'image/png');
  onStage?.('正在确认资产登记...');
  const completed = await completeAssetUpload(init.assetId);
  return completed;
}

export function getImageJob(jobId) {
  return requestJson(`/api/v1/compute/images/${encodeURIComponent(jobId)}`);
}

export function recoverImageJob(jobId) {
  return requestJson(`/api/v1/compute/images/${encodeURIComponent(jobId)}/recover`, {
    method: 'POST',
    json: {},
  });
}
