import {
  ApiError,
  cancelConversationTurn,
  createConversation,
  createWechatQrSession,
  getAsset,
  getConversationHistory,
  getDemoConfig,
  getImageJob,
  getMe,
  getOnelinkProvider,
  getSession,
  listConversations,
  listOnelinkModels,
  normalizeImageAssetIds,
  openConversationEvents,
  pollWechatQrSession,
  putOnelinkCredential,
  recoverImageJob,
  sendConversationMessage,
  setDemoMode,
  signInEmail,
  signOut,
  signUpEmail,
  testOnelinkCatalog,
  uploadImageAsset,
} from './api.js';

const NON_TERMINAL_JOB_STATUSES = new Set(['accepted', 'submitted', 'processing', 'materializing']);
const NON_TERMINAL_STATUS_ORDER = {
  accepted: 0,
  submitted: 1,
  processing: 2,
  materializing: 3,
};
const TERMINAL_TURN_STATUSES = new Set(['completed', 'cancelled', 'failed', 'unknown']);
const ALLOWED_IMAGE_TYPES = new Set(['image/png', 'image/jpeg', 'image/webp']);
const MAX_VISION_BYTES = 10 * 1024 * 1024;
const ASSET_CACHE_DEFAULT_TTL_MS = 240_000;
const JOB_POLL_INTERVAL_MS = 1500;
const WECHAT_QR_POLL_INTERVAL_MS = 1000;
const WECHAT_QR_MAX_POLLS_PAST_EXPIRY = 10;
const WECHAT_QR_MAX_CONSECUTIVE_401 = 5;
const WECHAT_OPEN_ORIGIN = 'https://open.weixin.qq.com';
const WECHAT_WXLOGIN_SCRIPT_URL =
  'https://res.wx.qq.com/connect/zh_CN/htmledition/js/wxLogin.js';
const WECHAT_QR_IFRAME_ORIGINAL_WIDTH = 300;
const WECHAT_QR_IFRAME_ORIGINAL_HEIGHT = 400;
const WECHAT_QR_DEFAULT_VIEWPORT_SIZE = 248;
const WECHAT_QR_CALLBACK_MESSAGE_TYPE = 'mio-wechat-qr-callback-landed';
const WECHAT_QR_WAKE_DELAY_MS = 20;
const WECHAT_QR_CUSTOM_CSS = [
  '.impowerBox .qrcode { margin: 0 auto; }',
  '.impowerBox .title { display: none; }',
  '.impowerBox .info { display: none; }',
  '.impowerBox .status { display: none; }',
  '.impowerBox .wrap { overflow: hidden; }',
].join('');
const WECHAT_QR_CUSTOM_HREF = `data:text/css;charset=utf-8,${encodeURIComponent(WECHAT_QR_CUSTOM_CSS)}`;
const WECHAT_QR_REFRESHABLE_STATUSES = new Set([
  'expired',
  'failed',
  'error',
  'stopped',
  'consumed',
]);

const JOB_STATUS_LABELS = {
  accepted: '已受理',
  submitted: '已提交上游',
  processing: '生成处理中',
  materializing: '落库物化中',
  completed: '已完成',
  partial: '部分完成',
  materialization_failed: '物化失败',
  failed: '失败',
  rejected: '已拒绝',
  unknown: '状态未知',
};

const state = {
  config: null,
  me: null,
  sessionUser: null,
  authTab: 'signin',
  provider: null,
  models: [],
  selectedChatModelRef: null,
  conversations: [],
  activeConversationId: null,
  historyItems: [],
  nextAfterSeq: -1,
  reconcilePromise: null,
  reconcileQueued: false,
  activeTurn: null,
  completedTurnIds: new Set(),
  eventSource: null,
  jobPollTimers: new Map(),
  jobs: new Map(),
  assetCache: new Map(),
  assetInflight: new Map(),
  attachments: [],
  wechatQr: {
    qrcodeId: null,
    authorizationUrl: null,
    expireSeconds: 300,
    expiresAt: 0,
    status: 'idle',
    loading: false,
    polling: false,
    pollInFlight: false,
    wakePending: false,
    pollCount: 0,
    consecutive401: 0,
    recorded401InTrail: false,
    trail: [],
    matchedUserId: null,
    message: '',
    timerId: null,
    runId: 0,
    manualPolling: false,
    pollIntervalMs: WECHAT_QR_POLL_INTERVAL_MS,
  },
};

let eventsBound = false;
let wxLoginScriptPromise = null;
let wechatQrObserver = null;
let wechatQrWxInstance = null;

function collectDomElements() {
  if (typeof document === 'undefined') return {};
  return {
    modeSandboxBtn: document.getElementById('modeSandboxBtn'),
    modeDevBtn: document.getElementById('modeDevBtn'),
    refreshHealthBtn: document.getElementById('refreshHealthBtn'),
    healthDot: document.getElementById('healthDot'),
    healthText: document.getElementById('healthText'),
    upstreamOriginText: document.getElementById('upstreamOriginText'),

    authBadge: document.getElementById('authBadge'),
    authSignedInBox: document.getElementById('authSignedInBox'),
    authSignedOutBox: document.getElementById('authSignedOutBox'),
    authUserLabel: document.getElementById('authUserLabel'),
    authSessionLabel: document.getElementById('authSessionLabel'),
    signOutBtn: document.getElementById('signOutBtn'),
    sandboxQuickLoginBtn: document.getElementById('sandboxQuickLoginBtn'),
    tabWechatQrBtn: document.getElementById('tabWechatQrBtn'),
    wechatQrPanel: document.getElementById('wechatQrPanel'),
    wechatQrViewport: document.getElementById('wechatQrViewport'),
    wechatQrEmbedContainer: document.getElementById('wechatQrEmbedContainer'),
    wechatQrLoadingOverlay: document.getElementById('wechatQrLoadingOverlay'),
    wechatQrRefreshBtn: document.getElementById('wechatQrRefreshBtn'),
    wechatQrRefreshLabel: document.getElementById('wechatQrRefreshLabel'),
    wechatQrStatusBadge: document.getElementById('wechatQrStatusBadge'),
    wechatQrCountdown: document.getElementById('wechatQrCountdown'),
    wechatQrHintText: document.getElementById('wechatQrHintText'),
    wechatQrCancelBtn: document.getElementById('wechatQrCancelBtn'),
    wechatQrDiagBox: document.getElementById('wechatQrDiagBox'),
    wechatQrDiagStatusBadge: document.getElementById('wechatQrDiagStatusBadge'),
    wechatQrDiagId: document.getElementById('wechatQrDiagId'),
    wechatQrDiagPolls: document.getElementById('wechatQrDiagPolls'),
    wechatQrDiagUserId: document.getElementById('wechatQrDiagUserId'),
    wechatQrDiagTrail: document.getElementById('wechatQrDiagTrail'),
    wechatQrManualPollBtn: document.getElementById('wechatQrManualPollBtn'),
    tabSignInBtn: document.getElementById('tabSignInBtn'),
    tabSignUpBtn: document.getElementById('tabSignUpBtn'),
    authForm: document.getElementById('authForm'),
    authNameField: document.getElementById('authNameField'),
    authNameInput: document.getElementById('authNameInput'),
    authEmailInput: document.getElementById('authEmailInput'),
    authPasswordInput: document.getElementById('authPasswordInput'),
    authSubmitBtn: document.getElementById('authSubmitBtn'),
    authErrorText: document.getElementById('authErrorText'),

    openProviderModalBtn: document.getElementById('openProviderModalBtn'),
    providerEmptyHint: document.getElementById('providerEmptyHint'),
    providerLoadedBox: document.getElementById('providerLoadedBox'),
    chatModelSelect: document.getElementById('chatModelSelect'),
    chatModelBadges: document.getElementById('chatModelBadges'),
    imageModelsCount: document.getElementById('imageModelsCount'),
    imageModelsList: document.getElementById('imageModelsList'),

    newConversationBtn: document.getElementById('newConversationBtn'),
    conversationList: document.getElementById('conversationList'),
    conversationEmptyText: document.getElementById('conversationEmptyText'),

    activeConvTitle: document.getElementById('activeConvTitle'),
    activeModelBadge: document.getElementById('activeModelBadge'),
    activeVisionBadge: document.getElementById('activeVisionBadge'),
    activeConvSubtitle: document.getElementById('activeConvSubtitle'),
    sseStatusBadge: document.getElementById('sseStatusBadge'),
    cancelTurnBtn: document.getElementById('cancelTurnBtn'),

    globalNotice: document.getElementById('globalNotice'),
    timeline: document.getElementById('timeline'),
    timelineWelcome: document.getElementById('timelineWelcome'),

    presetTurn1Btn: document.getElementById('presetTurn1Btn'),
    presetTurn2Btn: document.getElementById('presetTurn2Btn'),
    presetChatBtn: document.getElementById('presetChatBtn'),
    attachmentBar: document.getElementById('attachmentBar'),
    visionWarningBanner: document.getElementById('visionWarningBanner'),
    composerForm: document.getElementById('composerForm'),
    imageFileInput: document.getElementById('imageFileInput'),
    uploadProgressText: document.getElementById('uploadProgressText'),
    composerInput: document.getElementById('composerInput'),
    sendMessageBtn: document.getElementById('sendMessageBtn'),

    providerModal: document.getElementById('providerModal'),
    closeProviderModalBtn: document.getElementById('closeProviderModalBtn'),
    providerStatusSummary: document.getElementById('providerStatusSummary'),
    providerKeyForm: document.getElementById('providerKeyForm'),
    providerApiKeyInput: document.getElementById('providerApiKeyInput'),
    saveProviderKeyBtn: document.getElementById('saveProviderKeyBtn'),
    testProviderCatalogBtn: document.getElementById('testProviderCatalogBtn'),
    providerModalFeedback: document.getElementById('providerModalFeedback'),
  };
}

const el = collectDomElements();

function safeHref(rawUrl) {
  if (typeof rawUrl !== 'string' || !rawUrl) return null;
  try {
    const parsed = new URL(rawUrl, window.location.origin);
    if (parsed.protocol === 'http:' || parsed.protocol === 'https:' || parsed.protocol === 'blob:') {
      return parsed.href;
    }
  } catch {
    return null;
  }
  return null;
}

function safeHttpHref(rawUrl) {
  const href = safeHref(rawUrl);
  if (!href) return null;
  return href.startsWith('https://') || href.startsWith('http://') ? href : null;
}

function makeRequestId() {
  const rand = globalThis.crypto?.randomUUID?.().replace(/-/g, '').slice(0, 12) ?? Math.random().toString(36).slice(2, 10);
  return `web-${Date.now().toString(36)}-${rand}`.slice(0, 80);
}

function showNotice(message, level = 'info') {
  if (!message) {
    el.globalNotice.className = 'notice hidden';
    el.globalNotice.textContent = '';
    return;
  }
  el.globalNotice.textContent = message;
  el.globalNotice.className = `notice notice-${level}`;
}

function formatApiError(err, fallback) {
  if (err instanceof ApiError) {
    return err.message && err.message !== err.code ? `${err.code}: ${err.message}` : err.code;
  }
  return err instanceof Error ? err.message : fallback;
}

function setSseStatus(text, tone = 'muted') {
  el.sseStatusBadge.textContent = text;
  el.sseStatusBadge.className = `pill pill-${tone}`;
}

function closeConversationResources() {
  if (state.eventSource) {
    state.eventSource.close();
    state.eventSource = null;
  }
  for (const timerId of state.jobPollTimers.values()) {
    clearTimeout(timerId);
  }
  state.jobPollTimers.clear();
  state.activeTurn = null;
  state.completedTurnIds.clear();
  state.historyItems = [];
  state.nextAfterSeq = -1;
  state.reconcilePromise = null;
  state.reconcileQueued = false;
  state.jobs.clear();
  setSseStatus('未连接实时流', 'muted');
}

function clearAttachmentsAndAssetCache() {
  for (const att of state.attachments) {
    if (att.objectUrl) URL.revokeObjectURL(att.objectUrl);
  }
  state.attachments = [];
  state.assetCache.clear();
  state.assetInflight.clear();
  renderAttachments();
}

function findModelByRef(modelRef) {
  if (!modelRef) return null;
  return state.models.find((m) => m.modelRef === modelRef) ?? null;
}

function supportsVision(modelItem) {
  const mods = modelItem?.adaptation?.metadata?.inputModalities;
  return Array.isArray(mods) && mods.includes('image');
}

function getActiveConversation() {
  if (!state.activeConversationId) return null;
  return state.conversations.find((c) => c.id === state.activeConversationId) ?? null;
}

function getActiveConversationModelItem() {
  const conv = getActiveConversation();
  if (conv?.modelRef) return findModelByRef(conv.modelRef);
  return findModelByRef(state.selectedChatModelRef);
}

function mergeJobView(prev, next) {
  if (!next || typeof next !== 'object') return prev;
  if (!prev) return { ...next };
  const prevTerminal = !NON_TERMINAL_JOB_STATUSES.has(prev.status);
  const nextTerminal = !NON_TERMINAL_JOB_STATUSES.has(next.status);
  if (prevTerminal && !nextTerminal) {
    return prev;
  }
  const keepPrevStatus =
    !prevTerminal &&
    !nextTerminal &&
    (NON_TERMINAL_STATUS_ORDER[prev.status] ?? -1) >
      (NON_TERMINAL_STATUS_ORDER[next.status] ?? -1);
  const status = keepPrevStatus ? prev.status : next.status;
  const errorCode =
    next.errorCode !== undefined
      ? next.errorCode
      : status === 'completed'
        ? null
        : prev.errorCode;
  return {
    ...prev,
    ...next,
    status,
    errorCode,
    assetIds: next.assetIds?.length ? next.assetIds : prev.assetIds || [],
    outputs: next.outputs?.length ? next.outputs : prev.outputs || [],
  };
}

function resolveMergedToolName(prevTool, nextTool) {
  if (prevTool === 'submit_image_job' || nextTool === 'submit_image_job') {
    return 'submit_image_job';
  }
  return prevTool || nextTool || 'tool';
}

function extractJobAssetIds(job) {
  const assetIds = [];
  if (Array.isArray(job?.outputs)) {
    for (const out of job.outputs) {
      if (out?.assetId && !assetIds.includes(out.assetId)) assetIds.push(out.assetId);
    }
  }
  if (Array.isArray(job?.assetIds)) {
    for (const id of job.assetIds) {
      if (id && !assetIds.includes(id)) assetIds.push(id);
    }
  }
  return assetIds;
}

function jobStatusTone(status) {
  if (status === 'completed') return 'success';
  if (status === 'partial' || status === 'materializing' || status === 'processing') return 'primary';
  if (status === 'accepted' || status === 'submitted') return 'warn';
  if (status === 'materialization_failed' || status === 'failed' || status === 'rejected') return 'danger';
  return 'muted';
}

function jobStatusLabel(status) {
  const zh = JOB_STATUS_LABELS[status] || status;
  return `${zh} (${status})`;
}

async function resolveAssetCached(assetId, forceRefresh = false) {
  if (!assetId) return null;
  const cached = state.assetCache.get(assetId);
  if (!forceRefresh && cached && cached.asset?.status === 'ready' && cached.asset?.downloadUrl && Date.now() < cached.expiresAt) {
    return cached.asset;
  }
  if (!forceRefresh && state.assetInflight.has(assetId)) {
    return state.assetInflight.get(assetId);
  }

  const p = getAsset(assetId)
    .then((asset) => {
      let expiresAt = Date.now() + ASSET_CACHE_DEFAULT_TTL_MS;
      if (asset?.downloadUrlExpiresAt) {
        const parsed = Date.parse(asset.downloadUrlExpiresAt);
        if (Number.isFinite(parsed)) {
          expiresAt = Math.min(expiresAt, parsed - 30_000);
        }
      }
      state.assetCache.set(assetId, { asset, fetchedAt: Date.now(), expiresAt });
      return asset;
    })
    .catch(() => null)
    .finally(() => {
      state.assetInflight.delete(assetId);
    });

  state.assetInflight.set(assetId, p);
  return p;
}

function ensureJobAssetsResolved(job) {
  for (const id of extractJobAssetIds(job)) {
    const cached = state.assetCache.get(id);
    if (!cached || cached.asset?.status !== 'ready' || Date.now() >= cached.expiresAt) {
      void resolveAssetCached(id).then((asset) => {
        if (asset) renderTimeline();
      });
    }
  }
}

function scheduleJobPoll(jobId, immediate = false) {
  if (!jobId || !state.activeConversationId) return;
  const current = state.jobs.get(jobId);
  if (current && !NON_TERMINAL_JOB_STATUSES.has(current.status)) {
    return;
  }
  if (state.jobPollTimers.has(jobId)) return;

  const expectedConvId = state.activeConversationId;
  const runPoll = async () => {
    state.jobPollTimers.delete(jobId);
    if (state.activeConversationId !== expectedConvId || !state.me) return;

    try {
      const fresh = await getImageJob(jobId);
      if (state.activeConversationId !== expectedConvId) return;
      const merged = mergeJobView(state.jobs.get(jobId), fresh);
      state.jobs.set(jobId, merged);
      ensureJobAssetsResolved(merged);
      renderTimeline();
      if (NON_TERMINAL_JOB_STATUSES.has(merged.status)) {
        scheduleJobPoll(jobId, false);
      }
    } catch (err) {
      if (!state.me || state.activeConversationId !== expectedConvId) return;
      if (err instanceof ApiError && [400, 401, 403, 404].includes(err.status)) {
        return;
      }
      scheduleJobPoll(jobId, false);
    }
  };

  const timerId = setTimeout(runPoll, immediate ? 50 : JOB_POLL_INTERVAL_MS);
  state.jobPollTimers.set(jobId, timerId);
}

function registerToolJob(job) {
  if (!job?.jobId) return;
  const merged = mergeJobView(state.jobs.get(job.jobId), job);
  state.jobs.set(job.jobId, merged);
  ensureJobAssetsResolved(merged);
  if (NON_TERMINAL_JOB_STATUSES.has(merged.status)) {
    scheduleJobPoll(job.jobId, true);
  } else {
    const existingTimer = state.jobPollTimers.get(job.jobId);
    if (existingTimer) {
      clearTimeout(existingTimer);
      state.jobPollTimers.delete(job.jobId);
    }
  }
}

function renderConfig() {
  const cfg = state.config;
  if (!cfg) return;
  el.modeSandboxBtn.classList.toggle('active', cfg.mode === 'sandbox');
  el.modeDevBtn.classList.toggle('active', cfg.mode === 'dev');
  el.upstreamOriginText.textContent = `上游：${cfg.upstreamOrigin}`;

  if (cfg.health?.ready) {
    el.healthDot.className = 'dot dot-ok';
    el.healthText.textContent = `就绪 (${cfg.health.instanceId?.slice(0, 8) || 'ready'})`;
  } else {
    el.healthDot.className = 'dot dot-err';
    el.healthText.textContent = `未就绪 (${cfg.health?.error || 'unreachable'})`;
  }

  const showQuickSandbox = !state.me && cfg.mode === 'sandbox' && Boolean(cfg.sandboxAccount);
  el.sandboxQuickLoginBtn.classList.toggle('hidden', !showQuickSandbox);
}

function parseWechatAuthorizationUrl(rawUrl) {
  if (typeof rawUrl !== 'string' || !rawUrl.trim()) {
    throw new Error('微信授权链接无效');
  }
  let url;
  try {
    url = new URL(rawUrl);
  } catch {
    throw new Error('微信授权链接解析失败');
  }
  if (url.origin !== WECHAT_OPEN_ORIGIN) {
    throw new Error(`不允许的微信授权源：${url.origin}`);
  }
  const appid = (url.searchParams.get('appid') || '').trim();
  const redirect_uri = (url.searchParams.get('redirect_uri') || '').trim();
  const stateParam = (url.searchParams.get('state') || '').trim();
  const scope = (url.searchParams.get('scope') || '').trim() || 'snsapi_login';
  if (!appid || !redirect_uri || !stateParam) {
    throw new Error('微信授权链接缺少必要参数（appid / redirect_uri / state）');
  }
  let parsedRedirect;
  try {
    parsedRedirect = new URL(redirect_uri);
  } catch {
    throw new Error('微信授权链接的 redirect_uri 必须为绝对 HTTP(S) 地址');
  }
  if (parsedRedirect.protocol !== 'https:' && parsedRedirect.protocol !== 'http:') {
    throw new Error('微信授权链接的 redirect_uri 协议无效');
  }
  return {
    appid,
    redirect_uri,
    state: stateParam,
    scope,
  };
}

function getWxLoginConstructor() {
  const ctor = (typeof window !== 'undefined' && window?.WxLogin) || globalThis.WxLogin;
  return typeof ctor === 'function' ? ctor : null;
}

function loadWxLoginScript() {
  const existing = getWxLoginConstructor();
  if (existing) {
    return Promise.resolve(existing);
  }
  if (wxLoginScriptPromise) {
    return wxLoginScriptPromise;
  }

  wxLoginScriptPromise = new Promise((resolve, reject) => {
    if (typeof document === 'undefined' || !document.head) {
      reject(new Error('当前环境不支持加载微信官方扫码脚本'));
      return;
    }

    const existingScript =
      typeof document.querySelector === 'function'
        ? document.querySelector(`script[src="${WECHAT_WXLOGIN_SCRIPT_URL}"]`)
        : null;
    const script = existingScript || document.createElement('script');

    const cleanup = () => {
      script.removeEventListener?.('load', onLoad);
      script.removeEventListener?.('error', onError);
    };

    const onLoad = () => {
      cleanup();
      const ctor = getWxLoginConstructor();
      if (ctor) {
        resolve(ctor);
      } else {
        script.remove?.();
        reject(new Error('微信官方脚本已加载，但未暴露 WxLogin'));
      }
    };

    const onError = () => {
      cleanup();
      script.remove?.();
      reject(new Error('微信官方脚本加载失败，请检查网络连接后重试'));
    };

    if (typeof script.addEventListener === 'function') {
      script.addEventListener('load', onLoad);
      script.addEventListener('error', onError);
    } else {
      script.onload = onLoad;
      script.onerror = onError;
    }

    if (!existingScript) {
      script.src = WECHAT_WXLOGIN_SCRIPT_URL;
      script.async = true;
      document.head.appendChild(script);
    }
  }).finally(() => {
    wxLoginScriptPromise = null;
  });

  return wxLoginScriptPromise;
}

function findWechatQrIframe(container = el.wechatQrEmbedContainer) {
  if (!container) return null;
  if (typeof container.querySelector === 'function') {
    const found = container.querySelector('iframe');
    if (found) return found;
  }
  const stack = [...(container.children || [])];
  while (stack.length > 0) {
    const node = stack.shift();
    if (node?.tagName === 'IFRAME') return node;
    if (Array.isArray(node?.children)) {
      stack.push(...node.children);
    }
  }
  return null;
}

function fitWechatQrIframe(container = el.wechatQrEmbedContainer) {
  const iframe = findWechatQrIframe(container);
  if (!iframe) return false;

  const containerWidth = container.offsetWidth || WECHAT_QR_DEFAULT_VIEWPORT_SIZE;
  const containerHeight = container.offsetHeight || WECHAT_QR_DEFAULT_VIEWPORT_SIZE;
  const scale = Math.min(
    containerWidth / WECHAT_QR_IFRAME_ORIGINAL_WIDTH,
    containerHeight / WECHAT_QR_IFRAME_ORIGINAL_HEIGHT,
  );

  iframe.style.width = `${WECHAT_QR_IFRAME_ORIGINAL_WIDTH}px`;
  iframe.style.height = `${WECHAT_QR_IFRAME_ORIGINAL_HEIGHT}px`;
  iframe.style.border = 'none';
  iframe.style.display = 'block';
  iframe.style.position = 'absolute';
  iframe.style.left = '50%';
  iframe.style.top = '50%';
  iframe.style.transform = `translate(-50%, -50%) scale(${scale})`;
  iframe.style.transformOrigin = 'center center';
  return true;
}

function clearWechatQrEmbedContainer() {
  if (wechatQrObserver) {
    wechatQrObserver.disconnect();
    wechatQrObserver = null;
  }
  wechatQrWxInstance = null;
  el.wechatQrEmbedContainer?.replaceChildren?.();
}

function isWechatQrRefreshable() {
  const qr = state.wechatQr;
  if (qr.loading || qr.polling) return false;
  if (WECHAT_QR_REFRESHABLE_STATUSES.has(qr.status)) return true;
  return !state.me && (qr.status === 'idle' || qr.status === 'confirmed');
}

function isWechatQrSessionActive() {
  const qr = state.wechatQr;
  if (qr.loading) return true;
  return Boolean(qr.polling && qr.qrcodeId && (qr.expiresAt === 0 || Date.now() < qr.expiresAt));
}

function wechatQrRefreshLabel(status) {
  switch (status) {
    case 'expired':
      return '二维码已过期，点击刷新二维码';
    case 'failed':
      return '授权未通过，点击刷新二维码';
    case 'error':
      return '加载或轮询失败，点击刷新二维码';
    case 'stopped':
      return '轮询已停止，点击刷新二维码';
    case 'consumed':
      return '二维码已使用，点击刷新二维码';
    default:
      return '点击刷新二维码';
  }
}

function appendWechatQrTrail(entry) {
  if (!entry) return;
  const trail = state.wechatQr.trail;
  if (trail[trail.length - 1] !== entry) {
    trail.push(entry);
  }
}

function wechatQrStatusMeta(status) {
  switch (status) {
    case 'loading':
      return { label: '加载中 (loading)', tone: 'primary' };
    case 'pending':
      return { label: '等待扫码 (pending)', tone: 'primary' };
    case 'confirmed':
      return { label: '已确认 (confirmed)', tone: 'success' };
    case 'consumed':
      return { label: '已消费 (consumed)', tone: 'accent' };
    case 'failed':
      return { label: '授权失败 (failed)', tone: 'danger' };
    case 'expired':
      return { label: '已过期 (expired)', tone: 'warn' };
    case 'stopped':
      return { label: '已停止 (stopped)', tone: 'muted' };
    case 'error':
      return { label: '轮询异常 (error)', tone: 'danger' };
    default:
      return { label: status || 'idle', tone: 'muted' };
  }
}

function renderWechatQr() {
  const qr = state.wechatQr;
  const refreshable = isWechatQrRefreshable();

  if (el.wechatQrLoadingOverlay) {
    el.wechatQrLoadingOverlay.classList.toggle('hidden', !qr.loading);
  }

  if (el.wechatQrRefreshBtn) {
    el.wechatQrRefreshBtn.classList.toggle('hidden', !refreshable);
  }

  if (el.wechatQrRefreshLabel) {
    el.wechatQrRefreshLabel.textContent = wechatQrRefreshLabel(qr.status);
  }

  if (el.wechatQrViewport) {
    el.wechatQrViewport.classList.toggle('qr-viewport-clickable', refreshable);
  }

  if (el.wechatQrStatusBadge) {
    if (qr.polling && qr.consecutive401 > 0) {
      el.wechatQrStatusBadge.textContent = `等待 Cookie 写入 (401 重试 ${qr.consecutive401}/${WECHAT_QR_MAX_CONSECUTIVE_401})`;
      el.wechatQrStatusBadge.className = 'pill pill-warn';
    } else {
      const meta = wechatQrStatusMeta(qr.status);
      el.wechatQrStatusBadge.textContent = meta.label;
      el.wechatQrStatusBadge.className = `pill pill-${meta.tone}`;
    }
  }

  if (el.wechatQrCountdown) {
    if (qr.status === 'expired') {
      el.wechatQrCountdown.textContent = '0s';
    } else if (qr.polling && qr.expiresAt > 0) {
      const remaining = Math.max(0, Math.ceil((qr.expiresAt - Date.now()) / 1000));
      el.wechatQrCountdown.textContent = remaining > 0 ? `${remaining}s` : '0s (等待 expired)';
    } else if (qr.loading) {
      el.wechatQrCountdown.textContent = '加载中...';
    } else {
      el.wechatQrCountdown.textContent = `${qr.expireSeconds}s`;
    }
  }

  if (el.wechatQrHintText) {
    el.wechatQrHintText.textContent =
      qr.message || '请使用手机微信扫描上方二维码并确认登录。';
  }

  if (el.wechatQrCancelBtn) {
    const canStop = qr.loading || qr.polling;
    el.wechatQrCancelBtn.textContent = canStop ? '停止轮询' : '刷新二维码';
    el.wechatQrCancelBtn.className = canStop
      ? 'btn btn-danger btn-xs btn-full'
      : 'btn btn-secondary btn-xs btn-full';
  }

  if (el.wechatQrDiagBox) {
    el.wechatQrDiagBox.classList.toggle('hidden', !qr.qrcodeId);
  }

  if (el.wechatQrDiagStatusBadge) {
    const meta = wechatQrStatusMeta(qr.status);
    el.wechatQrDiagStatusBadge.textContent = qr.status || 'idle';
    el.wechatQrDiagStatusBadge.className = `pill pill-${meta.tone}`;
  }

  if (el.wechatQrDiagId) {
    el.wechatQrDiagId.textContent = qr.qrcodeId || '-';
    el.wechatQrDiagId.title = qr.qrcodeId || '';
  }

  if (el.wechatQrDiagPolls) {
    el.wechatQrDiagPolls.textContent = String(qr.pollCount);
  }

  if (el.wechatQrDiagUserId) {
    el.wechatQrDiagUserId.textContent = qr.matchedUserId || '-';
    el.wechatQrDiagUserId.title = qr.matchedUserId || '';
  }

  if (el.wechatQrDiagTrail) {
    el.wechatQrDiagTrail.textContent = qr.trail.length > 0 ? qr.trail.join(' -> ') : '-';
  }

  if (el.wechatQrManualPollBtn) {
    el.wechatQrManualPollBtn.disabled =
      !qr.qrcodeId || qr.loading || qr.polling || qr.manualPolling;
  }
}

function stopWechatQrPolling(options = {}) {
  const { resetDiag = false, status = null, message = null } = options;
  const qr = state.wechatQr;
  if (qr.timerId !== null) {
    clearTimeout(qr.timerId);
    qr.timerId = null;
  }
  qr.runId += 1;
  qr.loading = false;
  qr.polling = false;
  qr.pollInFlight = false;
  qr.wakePending = false;
  qr.manualPolling = false;
  qr.consecutive401 = 0;

  clearWechatQrEmbedContainer();

  if (resetDiag) {
    qr.qrcodeId = null;
    qr.authorizationUrl = null;
    qr.expireSeconds = 300;
    qr.expiresAt = 0;
    qr.status = 'idle';
    qr.pollCount = 0;
    qr.recorded401InTrail = false;
    qr.trail = [];
    qr.matchedUserId = null;
    qr.message = '';
  } else {
    if (status !== null) {
      qr.status = status;
    } else if (qr.status === 'loading' || qr.status === 'pending') {
      qr.status = 'stopped';
      if (message === null) {
        qr.message = '微信扫码轮询已停止，点击上方二维码区域可重新加载。';
      }
    }
    if (message !== null) qr.message = message;
  }
  renderWechatQr();
}

function scheduleWechatQrPoll(runId, overrideDelayMs = null) {
  const qr = state.wechatQr;
  if (!qr.polling || qr.runId !== runId || !qr.qrcodeId || qr.pollInFlight) return;
  if (qr.timerId !== null) {
    clearTimeout(qr.timerId);
    qr.timerId = null;
  }
  const defaultDelayMs =
    typeof qr.pollIntervalMs === 'number' && qr.pollIntervalMs >= 0
      ? qr.pollIntervalMs
      : WECHAT_QR_POLL_INTERVAL_MS;
  const delayMs =
    typeof overrideDelayMs === 'number' && overrideDelayMs >= 0
      ? overrideDelayMs
      : defaultDelayMs;
  qr.timerId = setTimeout(() => {
    void runWechatQrPollTick(runId);
  }, delayMs);
}

function handleWechatQrWindowMessage(event) {
  const qr = state.wechatQr;
  if (!qr.polling || !qr.qrcodeId) return;

  const expectedOrigin = typeof window !== 'undefined' ? window.location?.origin : null;
  if (!expectedOrigin || event?.origin !== expectedOrigin) return;

  const data = event?.data;
  if (!data || typeof data !== 'object' || data.type !== WECHAT_QR_CALLBACK_MESSAGE_TYPE) {
    return;
  }

  const iframe = findWechatQrIframe(el.wechatQrEmbedContainer);
  if (!iframe) return;
  const expectedSource = iframe.contentWindow ?? null;
  if (expectedSource && event.source !== expectedSource) {
    return;
  }

  const rawErr = data.error
    ? String(data.error).trim().replace(/[\r\n\t]+/g, ' ').slice(0, 32)
    : '';
  appendWechatQrTrail(rawErr ? `iframe_cb(${rawErr})` : 'iframe_cb(ok)');
  renderWechatQr();

  if (qr.pollInFlight) {
    qr.wakePending = true;
    return;
  }
  scheduleWechatQrPoll(qr.runId, WECHAT_QR_WAKE_DELAY_MS);
}

async function runWechatQrPollTick(runId) {
  const qr = state.wechatQr;
  qr.timerId = null;
  if (!qr.polling || qr.runId !== runId || !qr.qrcodeId || qr.pollInFlight) return;

  const qrcodeId = qr.qrcodeId;
  qr.pollInFlight = true;
  try {
    const res = await pollWechatQrSession(qrcodeId);
    if (qr.runId === runId) qr.pollInFlight = false;
    if (!qr.polling || qr.runId !== runId) return;

    qr.pollCount += 1;
    qr.consecutive401 = 0;
    const pollStatus = res?.status;

    if (pollStatus === 'pending') {
      qr.status = 'pending';
      appendWechatQrTrail('pending');
      const maxPolls = Math.max(
        (qr.expireSeconds || 300) + WECHAT_QR_MAX_POLLS_PAST_EXPIRY,
        310,
      );
      if (qr.pollCount >= maxPolls) {
        const msg = '自动轮询已达上限次数仍未收敛，请点击手动轮询或刷新二维码重试。';
        stopWechatQrPolling({ status: 'stopped', message: msg });
        if (el.authErrorText) {
          el.authErrorText.textContent = msg;
          el.authErrorText.classList.remove('hidden');
        }
        return;
      }
      const nextDelay = qr.wakePending ? WECHAT_QR_WAKE_DELAY_MS : null;
      qr.wakePending = false;
      renderWechatQr();
      scheduleWechatQrPoll(runId, nextDelay);
      return;
    }

    if (pollStatus === 'confirmed') {
      appendWechatQrTrail('confirmed');
      stopWechatQrPolling({
        status: 'confirmed',
        message: '微信扫码已确认，正在拉取 /api/v1/me 会话...',
      });
      await refreshAuthAndData();
      if (state.wechatQr.qrcodeId === qrcodeId) {
        if (state.me?.userId) {
          state.wechatQr.matchedUserId = state.me.userId;
          state.wechatQr.message = '微信扫码登录成功，已建立会话。';
          el.authErrorText?.classList.add('hidden');
          showNotice('微信扫码登录成功，已自动加载账号会话。', 'info');
        } else {
          const msg =
            '轮询已返回 confirmed，但未能从 /api/v1/me 读取有效会话，请检查同源 Cookie 后重试。';
          state.wechatQr.matchedUserId = null;
          state.wechatQr.message = msg;
          if (el.authErrorText) {
            el.authErrorText.textContent = msg;
            el.authErrorText.classList.remove('hidden');
          }
        }
        renderWechatQr();
      }
      return;
    }

    if (pollStatus === 'failed' || pollStatus === 'expired' || pollStatus === 'consumed') {
      appendWechatQrTrail(pollStatus);
      const messageByStatus = {
        failed: '微信授权未完成（failed），请点击刷新二维码重试。',
        expired: '微信扫码会话已过期（expired），请点击刷新二维码重试。',
        consumed: '该扫码结果已被消费（consumed），如需重新登录请刷新二维码。',
      };
      const msg = messageByStatus[pollStatus];
      stopWechatQrPolling({ status: pollStatus, message: msg });
      if (el.authErrorText) {
        el.authErrorText.textContent = msg;
        el.authErrorText.classList.remove('hidden');
      }
      return;
    }

    const unknownLabel = String(pollStatus || 'unknown');
    appendWechatQrTrail(unknownLabel);
    const unknownMsg = `微信扫码轮询返回未知状态：${unknownLabel}`;
    stopWechatQrPolling({ status: 'error', message: unknownMsg });
    if (el.authErrorText) {
      el.authErrorText.textContent = unknownMsg;
      el.authErrorText.classList.remove('hidden');
    }
  } catch (err) {
    if (qr.runId === runId) qr.pollInFlight = false;
    if (!qr.polling || qr.runId !== runId) return;
    qr.pollCount += 1;

    if (err instanceof ApiError && err.status === 401) {
      qr.consecutive401 += 1;
      if (!qr.recorded401InTrail) {
        qr.recorded401InTrail = true;
        appendWechatQrTrail('401');
      }
      if (qr.consecutive401 <= WECHAT_QR_MAX_CONSECUTIVE_401) {
        qr.message = `微信可能已确认授权，正在等待当前浏览器写入 Session Cookie（401 重试 ${qr.consecutive401}/${WECHAT_QR_MAX_CONSECUTIVE_401}）...`;
        const nextDelay = qr.wakePending ? WECHAT_QR_WAKE_DELAY_MS : null;
        qr.wakePending = false;
        renderWechatQr();
        scheduleWechatQrPoll(runId, nextDelay);
        return;
      }
      const msg =
        '微信已确认该二维码（401 UNAUTHORIZED），但当前浏览器缺少匹配的 Session Cookie；请确认当前页面通过与回调域一致的 HTTPS 域名访问，且浏览器未拦截内嵌 iframe 的同源 Cookie 后重试。';
      stopWechatQrPolling({ status: 'error', message: msg });
      if (el.authErrorText) {
        el.authErrorText.textContent = msg;
        el.authErrorText.classList.remove('hidden');
      }
      return;
    }

    const trailCode = err instanceof ApiError ? `${err.status}(${err.code})` : 'network_error';
    appendWechatQrTrail(trailCode);
    const errMsg = `微信扫码轮询失败：${formatApiError(err, '未知错误')}`;
    stopWechatQrPolling({ status: 'error', message: errMsg });
    if (el.authErrorText) {
      el.authErrorText.textContent = errMsg;
      el.authErrorText.classList.remove('hidden');
    }
  }
}

async function startWechatQrLogin() {
  stopWechatQrPolling();
  el.authErrorText?.classList.add('hidden');

  const qr = state.wechatQr;
  const createRunId = ++qr.runId;
  qr.loading = true;
  qr.status = 'loading';
  qr.message = '正在加载微信二维码...';
  renderWechatQr();

  try {
    const res = await createWechatQrSession('/auth/wechat/qr-complete');
    if (qr.runId !== createRunId || state.me) return;

    const authUrl = safeHttpHref(res?.authorization_url);
    if (!res?.qrcode_id || !authUrl) {
      throw new Error('服务端返回的二维码会话或授权地址无效');
    }

    const wxParams = parseWechatAuthorizationUrl(authUrl);
    const WxLoginCtor = await loadWxLoginScript();
    if (qr.runId !== createRunId || state.me) return;

    const container = el.wechatQrEmbedContainer;
    if (!container) {
      throw new Error('缺少微信二维码挂载容器');
    }

    clearWechatQrEmbedContainer();

    wechatQrWxInstance = new WxLoginCtor({
      self_redirect: true,
      id: container.id || 'wechatQrEmbedContainer',
      appid: wxParams.appid,
      redirect_uri: wxParams.redirect_uri,
      state: wxParams.state,
      scope: wxParams.scope,
      style: 'black',
      href: WECHAT_QR_CUSTOM_HREF,
    });

    if (!fitWechatQrIframe(container) && typeof MutationObserver === 'function') {
      const observer = new MutationObserver(() => {
        if (qr.runId !== createRunId) {
          observer.disconnect();
          if (wechatQrObserver === observer) wechatQrObserver = null;
          return;
        }
        if (fitWechatQrIframe(container)) {
          observer.disconnect();
          if (wechatQrObserver === observer) wechatQrObserver = null;
        }
      });
      wechatQrObserver = observer;
      observer.observe(container, {
        childList: true,
        subtree: true,
      });
    }

    const expireSeconds =
      typeof res.expire_seconds === 'number' && res.expire_seconds > 0
        ? res.expire_seconds
        : 300;
    qr.loading = false;
    qr.qrcodeId = res.qrcode_id;
    qr.authorizationUrl = authUrl;
    qr.expireSeconds = expireSeconds;
    qr.expiresAt = Date.now() + expireSeconds * 1000;
    qr.status = 'pending';
    qr.polling = true;
    qr.pollInFlight = false;
    qr.wakePending = false;
    qr.pollCount = 0;
    qr.consecutive401 = 0;
    qr.recorded401InTrail = false;
    qr.trail = ['created(201)'];
    qr.matchedUserId = null;
    qr.manualPolling = false;
    qr.message = '请使用手机微信扫描上方二维码并确认；本页每秒自动轮询状态。';

    renderWechatQr();
    scheduleWechatQrPoll(createRunId);
  } catch (err) {
    if (qr.runId !== createRunId) return;
    clearWechatQrEmbedContainer();
    qr.loading = false;
    qr.polling = false;
    qr.pollInFlight = false;
    qr.wakePending = false;
    qr.status = 'error';
    const errMsg = formatApiError(err, '创建微信扫码会话失败');
    qr.message = `${errMsg}，点击上方二维码区域可重试。`;
    if (el.authErrorText) {
      el.authErrorText.textContent = errMsg;
      el.authErrorText.classList.remove('hidden');
    }
    renderWechatQr();
  }
}

async function manualPollWechatQr() {
  const qr = state.wechatQr;
  if (!qr.qrcodeId || qr.loading || qr.polling || qr.manualPolling) return;
  const targetRunId = qr.runId;
  const targetQrcodeId = qr.qrcodeId;
  qr.manualPolling = true;
  renderWechatQr();
  try {
    const res = await pollWechatQrSession(targetQrcodeId);
    if (qr.runId !== targetRunId || qr.qrcodeId !== targetQrcodeId) return;
    qr.pollCount += 1;
    const nextStatus = res?.status || 'unknown';
    qr.status = nextStatus;
    appendWechatQrTrail(nextStatus);
    showNotice(`手动轮询结果：${nextStatus}`, 'info');
  } catch (err) {
    if (qr.runId !== targetRunId || qr.qrcodeId !== targetQrcodeId) return;
    qr.pollCount += 1;
    const label = err instanceof ApiError ? `${err.status}(${err.code})` : 'network_error';
    appendWechatQrTrail(label);
    showNotice(`手动轮询返回异常：${formatApiError(err, '未知错误')}`, 'warn');
  } finally {
    if (qr.runId === targetRunId && qr.qrcodeId === targetQrcodeId) {
      qr.manualPolling = false;
      renderWechatQr();
    }
  }
}

function switchAuthTab(nextTab) {
  const prevTab = state.authTab;
  state.authTab = nextTab;
  el.authErrorText?.classList.add('hidden');

  if (nextTab !== 'wechat') {
    const wasActive = state.wechatQr.polling || state.wechatQr.loading;
    if (prevTab === 'wechat' || wasActive) {
      stopWechatQrPolling(
        wasActive
          ? { status: 'stopped', message: '已切换登录方式，停止微信扫码轮询。' }
          : {},
      );
    }
    renderAuth();
    return undefined;
  }

  renderAuth();
  if (!state.me && !isWechatQrSessionActive()) {
    return startWechatQrLogin();
  }
  return undefined;
}

function renderAuth() {
  const signedIn = Boolean(state.me);
  el.authBadge.textContent = signedIn ? '已登录' : '未登录';
  el.authBadge.className = `pill ${signedIn ? 'pill-success' : 'pill-muted'}`;
  el.authSignedInBox.classList.toggle('hidden', !signedIn);
  el.authSignedOutBox.classList.toggle('hidden', signedIn);

  if (signedIn) {
    el.authUserLabel.textContent = state.sessionUser?.email || state.me.userId;
    el.authSessionLabel.textContent = state.me.sessionId;
  }

  const isSignIn = state.authTab === 'signin';
  const isSignUp = state.authTab === 'signup';
  const isWechat = state.authTab === 'wechat';
  el.tabSignInBtn.classList.toggle('active', isSignIn);
  el.tabSignUpBtn.classList.toggle('active', isSignUp);
  el.tabWechatQrBtn?.classList.toggle('active', isWechat);
  el.tabSignInBtn.setAttribute('aria-selected', String(isSignIn));
  el.tabSignUpBtn.setAttribute('aria-selected', String(isSignUp));
  el.tabWechatQrBtn?.setAttribute('aria-selected', String(isWechat));
  el.authForm.classList.toggle('hidden', isWechat);
  el.wechatQrPanel?.classList.toggle('hidden', !isWechat);
  el.authNameField.classList.toggle('hidden', !isSignUp);
  el.authPasswordInput.setAttribute('autocomplete', isSignUp ? 'new-password' : 'current-password');
  el.authSubmitBtn.textContent = isSignUp ? '注册并登录' : '登录';

  renderConfig();
  renderWechatQr();
}

function renderModels() {
  if (!state.me) {
    el.providerEmptyHint.classList.remove('hidden');
    el.providerEmptyHint.textContent = '登录后加载当前用户的动态模型目录。';
    el.providerLoadedBox.classList.add('hidden');
    return;
  }

  const runnableChat = state.models.filter((item) => item.runnable);
  const runnableImage = state.models.filter((item) => item.image?.runnable);

  el.providerEmptyHint.classList.add('hidden');
  el.providerLoadedBox.classList.remove('hidden');

  el.chatModelSelect.replaceChildren();
  if (runnableChat.length === 0) {
    const opt = document.createElement('option');
    opt.value = '';
    opt.textContent = '暂无可用对话模型（请配置并测试 OneLink Key）';
    el.chatModelSelect.append(opt);
    state.selectedChatModelRef = null;
  } else {
    const preferredName = state.config?.recommendedModels?.chat || 'deepseek-v4.1-flash';
    if (!runnableChat.some((m) => m.modelRef === state.selectedChatModelRef)) {
      const preferred =
        runnableChat.find((m) => m.modelId === preferredName) ||
        runnableChat.find((m) => supportsVision(m)) ||
        runnableChat[0];
      state.selectedChatModelRef = preferred.modelRef;
    }
    for (const item of runnableChat) {
      const opt = document.createElement('option');
      opt.value = item.modelRef;
      const visionTag = supportsVision(item) ? ' [视觉]' : '';
      opt.textContent = `${item.name || item.modelId}${visionTag}`;
      if (item.modelRef === state.selectedChatModelRef) opt.selected = true;
      el.chatModelSelect.append(opt);
    }
  }

  el.chatModelBadges.replaceChildren();
  const chosenChat = findModelByRef(state.selectedChatModelRef);
  if (chosenChat) {
    const visionPill = document.createElement('span');
    const canSee = supportsVision(chosenChat);
    visionPill.className = `pill ${canSee ? 'pill-accent' : 'pill-muted'}`;
    visionPill.textContent = canSee ? '支持视觉看图' : '纯文本对话';
    el.chatModelBadges.append(visionPill);

    if (chosenChat.protocol) {
      const protoPill = document.createElement('span');
      protoPill.className = 'pill pill-muted mono';
      protoPill.textContent = chosenChat.protocol;
      el.chatModelBadges.append(protoPill);
    }
  }

  el.imageModelsCount.textContent = `${runnableImage.length} 个可用`;
  el.imageModelsList.replaceChildren();
  if (runnableImage.length === 0) {
    const li = document.createElement('li');
    li.className = 'muted tiny';
    li.textContent = '暂无可运行生图模型';
    el.imageModelsList.append(li);
  } else {
    for (const item of runnableImage) {
      const li = document.createElement('li');
      li.className = 'mini-item';

      const left = document.createElement('span');
      left.className = 'mono truncate';
      left.textContent = item.modelId;

      const right = document.createElement('span');
      const maxRefs = item.image?.adaptation?.metadata?.image?.references?.maxImages ?? 0;
      right.className = `pill ${maxRefs > 0 ? 'pill-primary' : 'pill-muted'}`;
      right.textContent = maxRefs > 0 ? `参考图≤${maxRefs}` : '纯文生图';

      li.append(left, right);
      el.imageModelsList.append(li);
    }
  }

  renderWorkspaceHeader();
  renderAttachments();
}

function renderConversations() {
  el.conversationList.replaceChildren();
  const hasItems = state.conversations.length > 0;
  el.conversationEmptyText.classList.toggle('hidden', hasItems || !state.me);
  if (!state.me) {
    el.conversationEmptyText.classList.add('hidden');
    return;
  }

  const ordered = [...state.conversations].sort((a, b) => Date.parse(b.createdAt) - Date.parse(a.createdAt));
  for (const conv of ordered) {
    const li = document.createElement('li');
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = `conv-item-btn ${conv.id === state.activeConversationId ? 'active' : ''}`;
    btn.addEventListener('click', () => {
      void selectConversation(conv.id);
    });

    const title = document.createElement('span');
    title.className = 'small truncate';
    const modelItem = findModelByRef(conv.modelRef);
    title.textContent = modelItem ? `${modelItem.modelId} · ${conv.id.slice(0, 8)}` : `会话 ${conv.id.slice(0, 8)}`;

    const sub = document.createElement('span');
    sub.className = 'tiny muted';
    sub.textContent = new Date(conv.createdAt).toLocaleString('zh-CN');

    btn.append(title, sub);
    li.append(btn);
    el.conversationList.append(li);
  }
}

function renderWorkspaceHeader() {
  const conv = getActiveConversation();
  if (!conv) {
    el.activeConvTitle.textContent = '请选择或新建会话';
    el.activeModelBadge.classList.add('hidden');
    el.activeVisionBadge.classList.add('hidden');
    el.activeConvSubtitle.textContent = '';
    el.cancelTurnBtn.classList.add('hidden');
    return;
  }

  const modelItem = findModelByRef(conv.modelRef);
  el.activeConvTitle.textContent = `会话 ${conv.id.slice(0, 8)}`;
  el.activeConvSubtitle.textContent = `ID: ${conv.id}`;

  if (modelItem || conv.modelRef) {
    el.activeModelBadge.textContent = modelItem?.modelId || conv.modelRef;
    el.activeModelBadge.classList.remove('hidden');
  } else {
    el.activeModelBadge.classList.add('hidden');
  }

  const canSee = supportsVision(modelItem);
  el.activeVisionBadge.classList.toggle('hidden', !canSee);

  const isRunning =
    Boolean(state.activeTurn) && !TERMINAL_TURN_STATUSES.has(state.activeTurn.status);
  el.cancelTurnBtn.classList.toggle('hidden', !isRunning);
}

function createAssetThumbnailEl(assetId, compact = false) {
  const cached = state.assetCache.get(assetId)?.asset;
  const href = safeHref(cached?.downloadUrl);

  if (compact) {
    const img = document.createElement('img');
    img.className = 'user-thumb';
    img.alt = cached?.filename || `资产 ${assetId.slice(0, 8)}`;
    if (href) {
      img.src = href;
      img.addEventListener('error', () => {
        void resolveAssetCached(assetId, true).then((fresh) => {
          const freshUrl = safeHref(fresh?.downloadUrl);
          if (freshUrl && img.src !== freshUrl) img.src = freshUrl;
        });
      });
    } else {
      void resolveAssetCached(assetId).then((asset) => {
        if (asset) renderTimeline();
      });
    }
    return img;
  }

  const box = document.createElement('div');
  box.className = 'output-card';

  if (href) {
    const link = document.createElement('a');
    link.href = href;
    link.target = '_blank';
    link.rel = 'noopener noreferrer';
    link.title = '点击打开原图';

    const img = document.createElement('img');
    img.className = 'output-img';
    img.src = href;
    img.alt = `生成结果 ${assetId.slice(0, 8)}`;
    img.addEventListener('error', () => {
      void resolveAssetCached(assetId, true).then((fresh) => {
        const freshUrl = safeHref(fresh?.downloadUrl);
        if (freshUrl && img.src !== freshUrl) {
          img.src = freshUrl;
          link.href = freshUrl;
        }
      });
    });

    link.append(img);
    box.append(link);
  } else {
    const placeholder = document.createElement('div');
    placeholder.className = 'output-img muted small';
    placeholder.style.display = 'grid';
    placeholder.style.placeItems = 'center';
    placeholder.textContent = '加载图片中...';
    box.append(placeholder);
    void resolveAssetCached(assetId).then((asset) => {
      if (asset) renderTimeline();
    });
  }

  const actions = document.createElement('div');
  actions.className = 'output-actions';

  const idText = document.createElement('span');
  idText.className = 'mono tiny muted truncate';
  idText.textContent = assetId;
  actions.append(idText);

  if (href) {
    const origLink = document.createElement('a');
    origLink.className = 'btn btn-secondary btn-xs';
    origLink.style.textAlign = 'center';
    origLink.style.textDecoration = 'none';
    origLink.href = href;
    origLink.target = '_blank';
    origLink.rel = 'noopener noreferrer';
    origLink.textContent = '打开原图';
    actions.append(origLink);
  }

  return { box, actions };
}

function buildToolCardElement(toolName, rawJob, isLive = false) {
  const job = mergeJobView(rawJob, state.jobs.get(rawJob?.jobId)) || rawJob;
  const row = document.createElement('div');
  row.className = 'msg-row msg-tool';

  const card = document.createElement('div');
  card.className = 'tool-card';
  if (card.dataset && job?.jobId) {
    card.dataset.jobId = job.jobId;
  }

  const head = document.createElement('div');
  head.className = 'tool-card-head';

  const leftBadges = document.createElement('div');
  leftBadges.className = 'badge-row';

  const toolBadge = document.createElement('span');
  toolBadge.className = 'pill pill-primary';
  toolBadge.textContent =
    toolName === 'submit_image_job'
      ? '提交生图任务'
      : toolName === 'get_image_job'
        ? '查询生图任务'
        : toolName;

  const statusPill = document.createElement('span');
  statusPill.className = `pill pill-${jobStatusTone(job.status)}`;
  statusPill.textContent = jobStatusLabel(job.status);

  leftBadges.append(toolBadge, statusPill);

  if (isLive) {
    const liveBadge = document.createElement('span');
    liveBadge.className = 'pill pill-accent';
    liveBadge.textContent = '实时事件';
    leftBadges.append(liveBadge);
  }

  const jobIdSpan = document.createElement('span');
  jobIdSpan.className = 'mono tiny muted';
  jobIdSpan.textContent = `jobId: ${job.jobId}`;

  head.append(leftBadges, jobIdSpan);
  card.append(head);

  const meta = document.createElement('div');
  meta.className = 'tool-card-meta';

  const reqCount = document.createElement('span');
  reqCount.textContent = `请求数量: ${job.requestedCount ?? 1}`;
  meta.append(reqCount);

  if (job.errorCode) {
    const errSpan = document.createElement('span');
    errSpan.className = 'error-text';
    errSpan.textContent = `错误码: ${job.errorCode}`;
    meta.append(errSpan);
  }

  if (NON_TERMINAL_JOB_STATUSES.has(job.status)) {
    const pollSpan = document.createElement('span');
    pollSpan.textContent = '自动轮询进度中 (1.5s)...';
    meta.append(pollSpan);
  }

  card.append(meta);

  const assetIds = extractJobAssetIds(job);

  if (assetIds.length > 0) {
    const outputsWrap = document.createElement('div');
    outputsWrap.className = 'tool-outputs';
    for (const assetId of assetIds) {
      const { box, actions } = createAssetThumbnailEl(assetId, false);
      const attachBtn = document.createElement('button');
      attachBtn.type = 'button';
      attachBtn.className = 'btn btn-accent btn-xs';
      attachBtn.textContent = '带此图追问 / 二次生图';
      attachBtn.addEventListener('click', () => {
        attachDerivedAssetForFollowUp(job.jobId, assetId);
      });
      actions.append(attachBtn);
      box.append(actions);
      outputsWrap.append(box);
    }
    card.append(outputsWrap);
  }

  if (job.status === 'materialization_failed') {
    const recoverBtn = document.createElement('button');
    recoverBtn.type = 'button';
    recoverBtn.className = 'btn btn-secondary btn-xs';
    recoverBtn.style.alignSelf = 'flex-start';
    recoverBtn.textContent = '重试物化恢复';
    recoverBtn.addEventListener('click', async () => {
      recoverBtn.disabled = true;
      try {
        const nextJob = await recoverImageJob(job.jobId);
        registerToolJob(nextJob);
        renderTimeline();
      } catch (err) {
        showNotice(`恢复生图任务失败：${formatApiError(err, '未知错误')}`, 'error');
      } finally {
        recoverBtn.disabled = false;
      }
    });
    card.append(recoverBtn);
  }

  row.append(card);
  return row;
}

function collectMergedToolMap() {
  const byJobId = new Map();

  const record = (toolName, rawJob, isLive) => {
    const jobId = rawJob?.jobId;
    if (!jobId) return;
    const prev = byJobId.get(jobId);
    if (prev) {
      prev.tool = resolveMergedToolName(prev.tool, toolName);
      prev.job = mergeJobView(prev.job, rawJob);
    } else {
      byJobId.set(jobId, {
        tool: toolName,
        job: mergeJobView(null, rawJob),
        isLive,
      });
    }
  };

  for (const item of state.historyItems) {
    if (item.role === 'tool' && item.job) {
      record(item.tool, item.job, false);
    }
  }

  if (state.activeTurn?.liveTools) {
    for (const liveTool of state.activeTurn.liveTools) {
      if (liveTool?.job) {
        record(liveTool.tool, liveTool.job, true);
      }
    }
  }

  for (const [jobId, entry] of byJobId.entries()) {
    const storeJob = state.jobs.get(jobId);
    if (storeJob) {
      entry.job = mergeJobView(entry.job, storeJob);
    }
  }

  return byJobId;
}

function renderTimeline() {
  const prevScrollBottom = el.timeline.scrollHeight - el.timeline.scrollTop - el.timeline.clientHeight;
  const nearBottom = prevScrollBottom < 80;

  if (!state.activeConversationId) {
    el.timeline.replaceChildren(el.timelineWelcome);
    el.timelineWelcome.classList.remove('hidden');
    return;
  }

  const fragment = document.createDocumentFragment();
  if (state.historyItems.length === 0 && !state.activeTurn) {
    const empty = document.createElement('div');
    empty.className = 'empty-state muted small';
    empty.textContent = '当前会话尚无消息。发送问题或点击下方快捷预设开始第 1 轮生图演示。';
    fragment.append(empty);
  }

  const mergedToolsByJobId = collectMergedToolMap();
  const renderedJobIds = new Set();
  const appendToolCard = (toolName, rawJob, isLive) => {
    const jobId = rawJob?.jobId;
    if (jobId) {
      if (renderedJobIds.has(jobId)) return;
      renderedJobIds.add(jobId);
      const mergedEntry = mergedToolsByJobId.get(jobId);
      if (mergedEntry) {
        fragment.append(
          buildToolCardElement(mergedEntry.tool, mergedEntry.job, mergedEntry.isLive),
        );
        return;
      }
    }
    fragment.append(buildToolCardElement(toolName, rawJob, isLive));
  };

  for (const item of state.historyItems) {
    if (item.role === 'user') {
      const row = document.createElement('div');
      row.className = 'msg-row msg-user';
      const meta = document.createElement('div');
      meta.className = 'msg-meta';
      meta.textContent = '你';
      const bubble = document.createElement('div');
      bubble.className = 'msg-bubble';
      bubble.textContent = item.text;

      if (Array.isArray(item.imageAssetIds) && item.imageAssetIds.length > 0) {
        const thumbs = document.createElement('div');
        thumbs.className = 'user-attachments';
        for (const assetId of item.imageAssetIds) {
          thumbs.append(createAssetThumbnailEl(assetId, true));
        }
        bubble.append(thumbs);
      }

      row.append(meta, bubble);
      fragment.append(row);
    } else if (item.role === 'assistant') {
      const row = document.createElement('div');
      row.className = 'msg-row msg-assistant';
      const meta = document.createElement('div');
      meta.className = 'msg-meta';
      meta.textContent = item.interrupted ? 'Mio 助手（已中断）' : 'Mio 助手';
      const bubble = document.createElement('div');
      bubble.className = 'msg-bubble';
      bubble.textContent = item.text;
      row.append(meta, bubble);
      fragment.append(row);
    } else if (item.role === 'tool') {
      appendToolCard(item.tool, item.job, false);
    } else if (item.role === 'status') {
      const row = document.createElement('div');
      row.className = 'msg-row msg-status';
      const pill = document.createElement('span');
      pill.className = 'turn-status-pill';
      pill.textContent = `轮次状态：${item.status}`;
      row.append(pill);
      fragment.append(row);
    }
  }

  if (state.activeTurn) {
    const turn = state.activeTurn;
    if (turn.pendingUserText) {
      const row = document.createElement('div');
      row.className = 'msg-row msg-user';
      const meta = document.createElement('div');
      meta.className = 'msg-meta';
      meta.textContent = '你（发送中）';
      const bubble = document.createElement('div');
      bubble.className = 'msg-bubble';
      bubble.textContent = turn.pendingUserText;
      if (turn.pendingUserImageAssetIds?.length) {
        const thumbs = document.createElement('div');
        thumbs.className = 'user-attachments';
        for (const assetId of turn.pendingUserImageAssetIds) {
          thumbs.append(createAssetThumbnailEl(assetId, true));
        }
        bubble.append(thumbs);
      }
      row.append(meta, bubble);
      fragment.append(row);
    }

    for (const liveTool of turn.liveTools) {
      appendToolCard(liveTool.tool, liveTool.job, true);
    }

    if (turn.streamText) {
      const row = document.createElement('div');
      row.className = 'msg-row msg-assistant';
      const meta = document.createElement('div');
      meta.className = 'msg-meta';
      meta.textContent = 'Mio 助手（实时生成中...）';
      const bubble = document.createElement('div');
      bubble.className = 'msg-bubble';
      bubble.textContent = turn.streamText;
      row.append(meta, bubble);
      fragment.append(row);
    } else if (!TERMINAL_TURN_STATUSES.has(turn.status)) {
      const row = document.createElement('div');
      row.className = 'msg-row msg-status';
      const pill = document.createElement('span');
      pill.className = 'turn-status-pill';
      pill.textContent = `执行中 (${turn.status})...`;
      row.append(pill);
      fragment.append(row);
    }
  }

  el.timeline.replaceChildren(fragment);
  if (nearBottom) {
    el.timeline.scrollTop = el.timeline.scrollHeight;
  }
}

function renderAttachments() {
  el.attachmentBar.replaceChildren();
  el.attachmentBar.classList.toggle('hidden', state.attachments.length === 0);

  for (const att of state.attachments) {
    const chip = document.createElement('div');
    chip.className = 'attach-chip';

    const cachedHref = safeHref(
      att.previewUrl || state.assetCache.get(att.assetId)?.asset?.downloadUrl,
    );
    if (cachedHref) {
      const img = document.createElement('img');
      img.className = 'attach-thumb';
      img.src = cachedHref;
      img.alt = att.filename || att.assetId;
      chip.append(img);
    }

    const label = document.createElement('span');
    label.className = 'mono tiny';
    label.textContent = `${att.source === 'derived' ? '生成图' : att.filename || '本地图'} (${att.assetId.slice(0, 8)})`;
    chip.append(label);

    const removeBtn = document.createElement('button');
    removeBtn.type = 'button';
    removeBtn.className = 'btn-icon';
    removeBtn.textContent = '✕';
    removeBtn.title = '移除附件';
    removeBtn.addEventListener('click', () => {
      if (att.objectUrl) URL.revokeObjectURL(att.objectUrl);
      state.attachments = state.attachments.filter((item) => item.assetId !== att.assetId);
      renderAttachments();
    });

    chip.append(removeBtn);
    el.attachmentBar.append(chip);
  }

  const activeModel = getActiveConversationModelItem();
  const hasAttachments = state.attachments.length > 0;
  const modelUnsupported = hasAttachments && activeModel && !supportsVision(activeModel);
  el.visionWarningBanner.classList.toggle('hidden', !modelUnsupported);
  if (modelUnsupported) {
    el.visionWarningBanner.textContent = `注意：当前模型 ${activeModel.modelId} 未声明视觉输入能力（inputModalities 不含 image），发送附图可能会返回 MODEL_MODALITY_UNSUPPORTED。`;
  }
}

function attachAssetToComposer({ assetId, filename, previewUrl, objectUrl, source }) {
  const normalized = normalizeImageAssetIds([assetId])[0];
  if (!normalized) return false;
  if (state.attachments.some((a) => a.assetId === normalized)) {
    return true;
  }
  if (state.attachments.length >= 4) {
    showNotice('每条消息最多附加 4 张图片（imageAssetIds 上限 4）。', 'warn');
    return false;
  }
  state.attachments.push({
    assetId: normalized,
    filename: filename || 'image',
    previewUrl: previewUrl || null,
    objectUrl: objectUrl || null,
    source,
  });
  renderAttachments();
  return true;
}

function buildFollowUpPrompt(jobId, assetId, canSee) {
  if (canSee) {
    return `请先调用 get_image_job 查询上一轮生图任务 ${jobId} 的状态，检查资产 ${assetId}，再调用 submit_image_job 引用资产 ${assetId}（doubao-seedream-4.5）生成 1 张暖色调变体图并总结结果。`;
  }
  return `请先调用 get_image_job 查询上一轮生图任务 ${jobId} 的状态，再调用 list_image_models 与 submit_image_job 引用资产 ${assetId}（使用 doubao-seedream-4.5）生成 1 张暖色调变体图并总结结果。`;
}

function attachDerivedAssetForFollowUp(jobId, assetId, options = {}) {
  const { forcePrefill = false } = options;
  const canSee = supportsVision(getActiveConversationModelItem());

  if (canSee) {
    const cached = state.assetCache.get(assetId)?.asset;
    const attached = attachAssetToComposer({
      assetId,
      filename: cached?.filename || 'generated-image',
      previewUrl: cached?.downloadUrl || null,
      source: 'derived',
    });
    if (!attached) return;
  } else {
    showNotice(
      '当前会话模型不支持直接输入视觉像素，已自动切换为「通过资产 ID 调用工具引用改图」；如需模型直接看图，可新建会话并选择带 [视觉] 标记的模型（如 qwen3.5-plus）。',
      'info',
    );
  }

  if (forcePrefill || !el.composerInput.value.trim()) {
    el.composerInput.value = buildFollowUpPrompt(jobId, assetId, canSee);
  }
  el.composerInput.focus();
}

function findLatestCompletedImageInConversation() {
  const entries = [...collectMergedToolMap().values()].reverse();
  for (const entry of entries) {
    const job = entry.job;
    const assetId = extractJobAssetIds(job)[0];
    if (assetId && (job.status === 'completed' || job.status === 'partial')) {
      return { jobId: job.jobId, assetId };
    }
  }
  for (const job of [...state.jobs.values()].reverse()) {
    const assetId = extractJobAssetIds(job)[0];
    if (assetId && (job.status === 'completed' || job.status === 'partial')) {
      return { jobId: job.jobId, assetId };
    }
  }
  return null;
}

async function reconcileIncrementalHistory(conversationId) {
  if (state.activeConversationId !== conversationId) return;
  if (state.reconcilePromise) {
    state.reconcileQueued = true;
    return state.reconcilePromise;
  }

  state.reconcilePromise = (async () => {
    try {
      do {
        state.reconcileQueued = false;
        let hasMore = true;
        let loops = 0;
        while (hasMore && loops < 20 && state.activeConversationId === conversationId) {
          loops++;
          const page = await getConversationHistory(conversationId, state.nextAfterSeq, 100);
          if (state.activeConversationId !== conversationId) return;

          const existingSeqs = new Set(state.historyItems.map((i) => i.seq));
          for (const item of page.items || []) {
            if (!existingSeqs.has(item.seq)) {
              state.historyItems.push(item);
              existingSeqs.add(item.seq);
            }
            if (item.role === 'user' && item.imageAssetIds?.length) {
              for (const assetId of item.imageAssetIds) {
                void resolveAssetCached(assetId).then((asset) => {
                  if (asset) renderTimeline();
                });
              }
            }
            if (item.role === 'tool' && item.job) {
              registerToolJob(item.job);
            }
            if (item.role === 'user' && state.activeTurn?.pendingUserText === item.text) {
              state.activeTurn.pendingUserText = null;
              state.activeTurn.pendingUserImageAssetIds = null;
            }
          }
          if (typeof page.nextAfterSeq === 'number') {
            state.nextAfterSeq = page.nextAfterSeq;
          }
          hasMore = Boolean(page.hasMore);
        }
      } while (state.reconcileQueued && state.activeConversationId === conversationId);
    } catch (err) {
      showNotice(`同步历史记录失败：${formatApiError(err, '未知错误')}`, 'error');
    } finally {
      state.reconcilePromise = null;
      state.reconcileQueued = false;
    }
  })();

  return state.reconcilePromise;
}

function connectConversationSse(conversationId) {
  if (state.eventSource) {
    state.eventSource.close();
    state.eventSource = null;
  }

  const es = openConversationEvents(conversationId);
  state.eventSource = es;
  setSseStatus('连接实时流中...', 'warn');

  const markCompletedTurn = (requestId) => {
    if (!requestId) return;
    state.completedTurnIds.add(requestId);
    if (state.completedTurnIds.size > 30) {
      const oldest = state.completedTurnIds.values().next().value;
      if (oldest) state.completedTurnIds.delete(oldest);
    }
  };

  const ensureTurn = (requestId) => {
    if (requestId && state.completedTurnIds.has(requestId)) {
      return null;
    }
    if (!state.activeTurn || (requestId && state.activeTurn.requestId !== requestId)) {
      state.activeTurn = {
        requestId: requestId || 'active',
        status: 'accepted',
        pendingUserText: null,
        pendingUserImageAssetIds: null,
        streamText: '',
        liveTools: [],
      };
    }
    return state.activeTurn;
  };

  const handleTurnTerminal = (receipt) => {
    if (state.activeConversationId !== conversationId) return;
    markCompletedTurn(receipt?.requestId);
    if (receipt?.error) {
      showNotice(`本轮执行结束状态：${receipt.status} (${receipt.error})`, 'warn');
    }
    void reconcileIncrementalHistory(conversationId).then(() => {
      if (state.activeConversationId !== conversationId) return;
      if (!receipt?.requestId || state.activeTurn?.requestId === receipt.requestId) {
        state.activeTurn = null;
      }
      renderWorkspaceHeader();
      renderTimeline();
    });
  };

  es.onopen = () => {
    if (state.eventSource === es) {
      setSseStatus('实时流已连接', 'success');
    }
  };

  es.addEventListener('snapshot', (ev) => {
    if (state.activeConversationId !== conversationId) return;
    setSseStatus('实时流已连接', 'success');
    try {
      const data = JSON.parse(ev.data);
      if (data.activeRequestId) {
        const turn = ensureTurn(data.activeRequestId);
        if (turn) {
          turn.status = data.receipt?.status || turn.status || 'accepted';
          renderWorkspaceHeader();
          renderTimeline();
        }
      } else if (data.receipt && TERMINAL_TURN_STATUSES.has(data.receipt.status)) {
        handleTurnTerminal(data.receipt);
      }
    } catch {}
  });

  const onTextChunk = (ev) => {
    if (state.activeConversationId !== conversationId) return;
    try {
      const data = JSON.parse(ev.data);
      const turn = ensureTurn(data.requestId);
      if (turn && typeof data.text === 'string') {
        turn.streamText += data.text;
        renderTimeline();
      }
    } catch {}
  };
  es.addEventListener('text', onTextChunk);
  es.addEventListener('delta', onTextChunk);

  es.addEventListener('tool', (ev) => {
    if (state.activeConversationId !== conversationId) return;
    try {
      const data = JSON.parse(ev.data);
      const turn = ensureTurn(data.requestId);
      if (turn && data.job) {
        registerToolJob(data.job);
        const jobId = data.job.jobId;
        const existingIdx = jobId
          ? turn.liveTools.findIndex((t) => t.job?.jobId === jobId)
          : -1;
        if (existingIdx >= 0) {
          const existing = turn.liveTools[existingIdx];
          turn.liveTools[existingIdx] = {
            ...existing,
            tool: resolveMergedToolName(existing.tool, data.tool),
            job: mergeJobView(existing.job, data.job),
          };
        } else {
          turn.liveTools.push({
            tool: data.tool,
            job: mergeJobView(null, data.job),
          });
        }
        renderTimeline();
      }
    } catch {}
  });

  const onStateEvent = (ev) => {
    if (state.activeConversationId !== conversationId) return;
    try {
      const data = JSON.parse(ev.data);
      const receipt = data.receipt || data;
      if (!receipt?.status) return;
      if (TERMINAL_TURN_STATUSES.has(receipt.status)) {
        handleTurnTerminal(receipt);
      } else {
        const turn = ensureTurn(receipt.requestId);
        if (turn) {
          turn.status = receipt.status;
          if (receipt.status === 'input-persisted') {
            void reconcileIncrementalHistory(conversationId).then(() => renderTimeline());
          }
          renderWorkspaceHeader();
          renderTimeline();
        }
      }
    } catch {}
  };
  es.addEventListener('state', onStateEvent);
  es.addEventListener('end', onStateEvent);

  es.addEventListener('error', (ev) => {
    if (ev?.data) {
      try {
        const data = JSON.parse(ev.data);
        showNotice(`SSE 服务事件错误：${data.code || 'STREAM_ERROR'}`, 'error');
        if (data.code === 'UNAUTHORIZED') {
          es.close();
          state.eventSource = null;
          setSseStatus('登录已失效', 'danger');
          void refreshAuthAndData();
        }
      } catch {}
    }
  });

  es.onerror = () => {
    if (state.eventSource !== es) return;
    if (es.readyState === EventSource.CLOSED) {
      setSseStatus('实时流已关闭', 'danger');
    } else {
      setSseStatus('实时流重连中...', 'warn');
    }
  };
}

async function selectConversation(conversationId) {
  if (!conversationId) {
    closeConversationResources();
    state.activeConversationId = null;
    renderConversations();
    renderWorkspaceHeader();
    renderTimeline();
    renderAttachments();
    return;
  }

  closeConversationResources();
  state.activeConversationId = conversationId;
  renderConversations();
  renderWorkspaceHeader();
  renderAttachments();
  showNotice('');

  await reconcileIncrementalHistory(conversationId);
  if (state.activeConversationId !== conversationId) return;
  renderTimeline();
  connectConversationSse(conversationId);
}

async function refreshConfig() {
  state.config = await getDemoConfig();
  renderConfig();
}

async function refreshProviderAndModels() {
  if (!state.me) {
    state.provider = null;
    state.models = [];
    renderModels();
    return;
  }
  try {
    const [provider, catalog] = await Promise.all([
      getOnelinkProvider().catch(() => null),
      listOnelinkModels().catch(() => ({ provider: null, items: [] })),
    ]);
    state.provider = catalog?.provider ?? provider;
    state.models = Array.isArray(catalog?.items) ? catalog.items : [];
  } catch {
    state.provider = null;
    state.models = [];
  }
  renderModels();
}

async function refreshConversations(autoSelectFirst = true) {
  if (!state.me) {
    state.conversations = [];
    await selectConversation(null);
    return;
  }
  try {
    const res = await listConversations(50);
    state.conversations = Array.isArray(res?.items) ? res.items : [];
  } catch (err) {
    state.conversations = [];
    showNotice(`加载会话列表失败：${formatApiError(err, '未知错误')}`, 'error');
  }
  renderConversations();

  if (state.activeConversationId && state.conversations.some((c) => c.id === state.activeConversationId)) {
    renderWorkspaceHeader();
  } else if (autoSelectFirst && state.conversations.length > 0) {
    const newest = [...state.conversations].sort(
      (a, b) => Date.parse(b.createdAt) - Date.parse(a.createdAt),
    )[0];
    await selectConversation(newest.id);
  } else {
    await selectConversation(null);
  }
}

async function refreshAuthAndData() {
  try {
    state.me = await getMe();
    const sess = await getSession().catch(() => null);
    state.sessionUser = sess?.user ?? null;
  } catch {
    state.me = null;
    state.sessionUser = null;
  }
  if (state.me && (state.wechatQr.polling || state.wechatQr.loading)) {
    stopWechatQrPolling();
  }
  renderAuth();
  await refreshProviderAndModels();
  await refreshConversations(true);
}

function renderProviderModalSummary() {
  const p = state.provider;
  el.providerStatusSummary.replaceChildren();
  const rows = [
    ['Key 状态', p?.hasKey ? `已配置 (${p.masked})` : '未配置'],
    ['凭据校验', p?.credentialAuthState || 'unchecked'],
    ['当前 Revision', String(p?.revision ?? 0)],
    ['目录快照', p?.catalog?.current ? `有效 (v${p.catalog.version}, 模型 ${state.models.length} 个)` : '未发现或待刷新'],
  ];
  if (p?.lastAttempt?.error) {
    rows.push(['最近失败', `${p.lastAttempt.status}: ${p.lastAttempt.error}`]);
  }
  for (const [k, v] of rows) {
    const line = document.createElement('div');
    line.className = 'kv-line';
    const keyEl = document.createElement('span');
    keyEl.className = 'muted';
    keyEl.textContent = k;
    const valEl = document.createElement('span');
    valEl.className = 'mono';
    valEl.textContent = v;
    line.append(keyEl, valEl);
    el.providerStatusSummary.append(line);
  }
}

function bindEvents() {
  if (eventsBound) return;
  eventsBound = true;

  const switchMode = async (mode, message) => {
    try {
      stopWechatQrPolling({ resetDiag: true });
      state.config = await setDemoMode(mode);
      closeConversationResources();
      state.activeConversationId = null;
      clearAttachmentsAndAssetCache();
      await refreshAuthAndData();
      showNotice(message, 'info');
    } catch (err) {
      showNotice(`切换环境失败：${formatApiError(err, '未知错误')}`, 'error');
    }
  };

  el.modeSandboxBtn.addEventListener('click', () => {
    void switchMode('sandbox', '已切换至 Mio Sandbox 零扣费模拟环境。');
  });

  el.modeDevBtn.addEventListener('click', () => {
    void switchMode('dev', '已切换至 Mio Dev 常驻环境，请使用该环境账号登录。');
  });

  el.refreshHealthBtn.addEventListener('click', () => {
    void refreshConfig();
  });

  if (typeof window !== 'undefined' && typeof window.addEventListener === 'function') {
    window.addEventListener('message', handleWechatQrWindowMessage);
  }

  el.tabSignInBtn.addEventListener('click', () => switchAuthTab('signin'));

  el.tabSignUpBtn.addEventListener('click', () => switchAuthTab('signup'));

  el.tabWechatQrBtn?.addEventListener('click', () => switchAuthTab('wechat'));

  el.sandboxQuickLoginBtn.addEventListener('click', async () => {
    const acc = state.config?.sandboxAccount;
    if (!acc) return;
    stopWechatQrPolling();
    el.authErrorText.classList.add('hidden');
    el.sandboxQuickLoginBtn.disabled = true;
    try {
      await signInEmail(acc.email, acc.password);
      await refreshAuthAndData();
      showNotice('沙盒测试账号登录成功。', 'info');
    } catch (err) {
      el.authErrorText.textContent = formatApiError(err, '一键登录失败');
      el.authErrorText.classList.remove('hidden');
    } finally {
      el.sandboxQuickLoginBtn.disabled = false;
    }
  });

  el.wechatQrRefreshBtn?.addEventListener('click', (e) => {
    e?.stopPropagation?.();
    return startWechatQrLogin();
  });

  el.wechatQrViewport?.addEventListener('click', (e) => {
    if (e?.target === el.wechatQrRefreshBtn) return undefined;
    if (isWechatQrRefreshable()) {
      return startWechatQrLogin();
    }
    return undefined;
  });

  el.wechatQrCancelBtn?.addEventListener('click', () => {
    if (state.wechatQr.loading || state.wechatQr.polling) {
      appendWechatQrTrail('stopped');
      stopWechatQrPolling({
        status: 'stopped',
        message: '已手动停止轮询，点击上方二维码区域可重新加载。',
      });
      return undefined;
    }
    return startWechatQrLogin();
  });

  el.wechatQrManualPollBtn?.addEventListener('click', () => manualPollWechatQr());

  el.authForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    stopWechatQrPolling();
    el.authErrorText.classList.add('hidden');
    const email = el.authEmailInput.value.trim();
    const password = el.authPasswordInput.value;
    const name = el.authNameInput.value.trim() || email.split('@')[0] || 'Mio User';

    el.authSubmitBtn.disabled = true;
    try {
      if (state.authTab === 'signup') {
        await signUpEmail(name, email, password);
      } else {
        await signInEmail(email, password);
      }
      el.authPasswordInput.value = '';
      await refreshAuthAndData();
    } catch (err) {
      el.authErrorText.textContent = formatApiError(err, '认证请求失败');
      el.authErrorText.classList.remove('hidden');
    } finally {
      el.authSubmitBtn.disabled = false;
    }
  });

  el.signOutBtn.addEventListener('click', async () => {
    stopWechatQrPolling();
    try {
      await signOut();
    } catch {}
    closeConversationResources();
    state.activeConversationId = null;
    clearAttachmentsAndAssetCache();
    await refreshAuthAndData();
  });

  el.chatModelSelect.addEventListener('change', () => {
    state.selectedChatModelRef = el.chatModelSelect.value || null;
    renderModels();
  });

  el.newConversationBtn.addEventListener('click', async () => {
    if (!state.me) {
      showNotice('请先登录账号，再创建新会话。', 'warn');
      return;
    }
    el.newConversationBtn.disabled = true;
    try {
      const created = await createConversation(state.selectedChatModelRef || undefined);
      state.conversations.unshift(created);
      renderConversations();
      await selectConversation(created.id);
    } catch (err) {
      showNotice(`创建会话失败：${formatApiError(err, '未知错误')}`, 'error');
    } finally {
      el.newConversationBtn.disabled = false;
    }
  });

  el.cancelTurnBtn.addEventListener('click', async () => {
    if (!state.activeConversationId) return;
    el.cancelTurnBtn.disabled = true;
    try {
      await cancelConversationTurn(state.activeConversationId);
      showNotice('已发送停止生成请求，等待服务端确认收尾。', 'info');
    } catch (err) {
      showNotice(`停止生成失败：${formatApiError(err, '未知错误')}`, 'error');
    } finally {
      el.cancelTurnBtn.disabled = false;
    }
  });

  el.imageFileInput.addEventListener('change', async () => {
    const files = [...(el.imageFileInput.files || [])];
    el.imageFileInput.value = '';
    if (files.length === 0) return;

    if (!state.me) {
      showNotice('请先登录再上传图片附件。', 'warn');
      return;
    }

    for (const file of files) {
      if (state.attachments.length >= 4) {
        showNotice('最多只能附加 4 张图片。', 'warn');
        break;
      }
      if (!ALLOWED_IMAGE_TYPES.has(file.type)) {
        showNotice(`不支持的图片格式 ${file.type || file.name}，仅支持 PNG/JPEG/WebP。`, 'warn');
        continue;
      }
      if (file.size < 1 || file.size > MAX_VISION_BYTES) {
        showNotice(`图片 ${file.name} 超出 10 MiB 上限。`, 'warn');
        continue;
      }

      el.uploadProgressText.classList.remove('hidden');
      try {
        const asset = await uploadImageAsset(file, (stage) => {
          el.uploadProgressText.textContent = `${file.name}: ${stage}`;
        });
        const objUrl = URL.createObjectURL(file);
        state.assetCache.set(asset.assetId, {
          asset,
          fetchedAt: Date.now(),
          expiresAt: Date.now() + ASSET_CACHE_DEFAULT_TTL_MS,
        });
        attachAssetToComposer({
          assetId: asset.assetId,
          filename: file.name,
          previewUrl: asset.downloadUrl || objUrl,
          objectUrl: objUrl,
          source: 'upload',
        });
      } catch (err) {
        showNotice(`上传失败 (${file.name})：${formatApiError(err, '未知错误')}`, 'error');
      } finally {
        el.uploadProgressText.classList.add('hidden');
        el.uploadProgressText.textContent = '';
      }
    }
  });

  el.presetTurn1Btn.addEventListener('click', () => {
    el.composerInput.value =
      '请先调用 list_image_models 查看可用生图模型，然后调用 submit_image_job 使用 doubao-seedream-4.0 生成 1 张极简蓝色几何方块白色背景测试图，并告诉我任务 ID。';
    el.composerInput.focus();
  });

  el.presetTurn2Btn.addEventListener('click', () => {
    const latest = findLatestCompletedImageInConversation();
    if (!latest) {
      showNotice('当前会话还没有已完成的生图资产，请先执行「第 1 轮：文生图」并等待生成完成。', 'warn');
      return;
    }
    attachDerivedAssetForFollowUp(latest.jobId, latest.assetId, { forcePrefill: true });
  });

  el.presetChatBtn.addEventListener('click', () => {
    el.composerInput.value = '请用一句话回复：Mio 对话链路联通正常。';
    el.composerInput.focus();
  });

  el.composerInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      el.composerForm.requestSubmit();
    }
  });

  el.composerForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    if (!state.me) {
      showNotice('请先登录账号。', 'warn');
      return;
    }
    const text = el.composerInput.value.trim();
    if (!text) return;
    if (state.activeTurn && !TERMINAL_TURN_STATUSES.has(state.activeTurn.status)) {
      showNotice('当前会话上一轮仍在生成中，请等待完成或点击「停止生成」。', 'warn');
      return;
    }

    let seededRequestId = null;
    el.sendMessageBtn.disabled = true;
    showNotice('');
    try {
      let convId = state.activeConversationId;
      if (!convId) {
        const created = await createConversation(state.selectedChatModelRef || undefined);
        state.conversations.unshift(created);
        renderConversations();
        await selectConversation(created.id);
        convId = created.id;
      }

      const imageAssetIds = normalizeImageAssetIds(state.attachments.map((a) => a.assetId));
      const requestId = makeRequestId();
      seededRequestId = requestId;

      // Seed activeTurn BEFORE sendConversationMessage so fast SSE events arriving
      // before the 202 HTTP response returns are merged rather than wiped or dropped.
      state.activeTurn = {
        requestId,
        status: 'accepted',
        pendingUserText: text,
        pendingUserImageAssetIds: imageAssetIds,
        streamText: '',
        liveTools: [],
      };
      renderWorkspaceHeader();
      renderTimeline();

      const receipt = await sendConversationMessage(convId, {
        requestId,
        text,
        imageAssetIds,
      });

      const actualRequestId = receipt.requestId || requestId;
      if (state.completedTurnIds.has(actualRequestId)) {
        if (state.activeTurn?.requestId === actualRequestId) {
          state.activeTurn = null;
        }
      } else if (state.activeTurn?.requestId === actualRequestId) {
        if (state.activeTurn.status === 'accepted' && receipt.status) {
          state.activeTurn.status = receipt.status;
        }
      } else {
        state.activeTurn = {
          requestId: actualRequestId,
          status: receipt.status || 'accepted',
          pendingUserText: text,
          pendingUserImageAssetIds: imageAssetIds,
          streamText: '',
          liveTools: [],
        };
      }

      el.composerInput.value = '';
      for (const att of state.attachments) {
        if (att.objectUrl) URL.revokeObjectURL(att.objectUrl);
      }
      state.attachments = [];
      renderAttachments();
      renderWorkspaceHeader();
      renderTimeline();
    } catch (err) {
      if (seededRequestId && state.activeTurn?.requestId === seededRequestId) {
        state.activeTurn = null;
        renderWorkspaceHeader();
        renderTimeline();
      }
      showNotice(`发送消息失败：${formatApiError(err, '未知错误')}`, 'error');
    } finally {
      el.sendMessageBtn.disabled = false;
    }
  });

  el.openProviderModalBtn.addEventListener('click', async () => {
    if (!state.me) {
      showNotice('请先登录再配置或测试 OneLink Key。', 'warn');
      return;
    }
    el.providerModalFeedback.classList.add('hidden');
    state.provider = await getOnelinkProvider().catch(() => state.provider);
    renderProviderModalSummary();
    el.providerModal.showModal();
  });

  el.closeProviderModalBtn.addEventListener('click', () => {
    el.providerModal.close();
  });

  el.providerKeyForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const apiKey = el.providerApiKeyInput.value.trim();
    if (!apiKey) return;
    el.saveProviderKeyBtn.disabled = true;
    el.providerModalFeedback.classList.add('hidden');
    try {
      const expectedRevision = state.provider?.revision ?? 0;
      state.provider = await putOnelinkCredential(apiKey, expectedRevision);
      el.providerApiKeyInput.value = '';
      renderProviderModalSummary();
      el.providerModalFeedback.textContent = 'Key 已加密保存；请点击「测试并发现目录」刷新可用模型。';
      el.providerModalFeedback.className = 'small';
    } catch (err) {
      el.providerModalFeedback.textContent = `保存失败：${formatApiError(err, '未知错误')}`;
      el.providerModalFeedback.className = 'error-text';
    } finally {
      el.saveProviderKeyBtn.disabled = false;
    }
  });

  el.testProviderCatalogBtn.addEventListener('click', async () => {
    el.testProviderCatalogBtn.disabled = true;
    el.providerModalFeedback.classList.add('hidden');
    try {
      const disc = await testOnelinkCatalog();
      await refreshProviderAndModels();
      renderProviderModalSummary();
      if (disc.status === 'connected') {
        el.providerModalFeedback.textContent = `目录连接成功，共发现 ${disc.ids?.length ?? 0} 个模型。`;
        el.providerModalFeedback.className = 'small';
      } else {
        el.providerModalFeedback.textContent = `测试失败：${disc.error || disc.status}`;
        el.providerModalFeedback.className = 'error-text';
      }
    } catch (err) {
      el.providerModalFeedback.textContent = `测试请求失败：${formatApiError(err, '未知错误')}`;
      el.providerModalFeedback.className = 'error-text';
    } finally {
      el.testProviderCatalogBtn.disabled = false;
    }
  });
}

async function boot() {
  bindEvents();
  await refreshConfig().catch((err) => {
    showNotice(`读取配置失败：${formatApiError(err, '未知错误')}`, 'error');
  });
  await refreshAuthAndData();
}

if (typeof document !== 'undefined' && !globalThis.__MIO_DEMO_SKIP_BOOT__) {
  void boot();
}

export {
  attachDerivedAssetForFollowUp,
  bindEvents,
  buildFollowUpPrompt,
  buildToolCardElement,
  clearAttachmentsAndAssetCache,
  clearWechatQrEmbedContainer,
  closeConversationResources,
  collectMergedToolMap,
  connectConversationSse,
  el,
  extractJobAssetIds,
  findLatestCompletedImageInConversation,
  fitWechatQrIframe,
  handleWechatQrWindowMessage,
  loadWxLoginScript,
  manualPollWechatQr,
  mergeJobView,
  parseWechatAuthorizationUrl,
  registerToolJob,
  renderAuth,
  renderTimeline,
  renderWechatQr,
  resolveMergedToolName,
  startWechatQrLogin,
  state,
  stopWechatQrPolling,
  switchAuthTab,
};
