#!/usr/bin/env node
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { existsSync, readFileSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import http from 'node:http';
import https from 'node:https';
import { dirname, join, resolve, sep } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = resolve(__dirname, 'public');
const APIFOX_DIR = resolve(__dirname, '../mio-apifox');
const SANDBOX_SCRIPT = join(APIFOX_DIR, 'scripts/start-mock-sandbox.mjs');
const SANDBOX_VARS_PATH = join(APIFOX_DIR, 'sandbox-vars.example.json');

const HOST = '127.0.0.1';
const SANDBOX_PORT = Number(process.env.MIO_SANDBOX_PORT ?? 4319);
const SANDBOX_ORIGIN = `http://127.0.0.1:${SANDBOX_PORT}`;
const DEV_ORIGIN = normalizeHttpOrigin(process.env.MIO_DEV_ORIGIN ?? 'http://localhost:4310');

const MAX_JSON_BODY_BYTES = 64 * 1024;
const MAX_UPLOAD_BYTES = 25 * 1024 * 1024;

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
};

const HOP_BY_HOP_HEADERS = new Set([
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailers',
  'transfer-encoding',
  'upgrade',
]);

function normalizeHttpOrigin(raw) {
  const url = new URL(raw);
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error(`Invalid upstream origin protocol: ${raw}`);
  }
  return url.origin;
}

function parsePort(raw, fallback) {
  if (raw === undefined || raw === '') return fallback;
  const num = Number(raw);
  if (!Number.isInteger(num) || num < 0 || num > 65535) {
    throw new Error(`Invalid DEMO_PORT: ${raw}`);
  }
  return num;
}

function loadSandboxMeta() {
  if (!existsSync(SANDBOX_VARS_PATH)) {
    return {
      sandboxAccount: null,
      recommendedModels: {
        chat: 'deepseek-v4.1-flash',
        image: 'doubao-seedream-4.0',
        refImage: 'doubao-seedream-4.5',
      },
    };
  }
  try {
    const parsed = JSON.parse(readFileSync(SANDBOX_VARS_PATH, 'utf8'));
    const vars = parsed?.environmentVariables ?? {};
    const sandboxAccount =
      typeof vars.OWNER_EMAIL === 'string' && typeof vars.OWNER_PASSWORD === 'string'
        ? { email: vars.OWNER_EMAIL, password: vars.OWNER_PASSWORD }
        : null;
    return {
      sandboxAccount,
      recommendedModels: {
        chat: vars.CHAT_MODEL_ID || 'deepseek-v4.1-flash',
        image: vars.IMAGE_MODEL_ID || 'doubao-seedream-4.0',
        refImage: vars.REF_IMAGE_MODEL_ID || 'doubao-seedream-4.5',
      },
    };
  } catch {
    return {
      sandboxAccount: null,
      recommendedModels: {
        chat: 'deepseek-v4.1-flash',
        image: 'doubao-seedream-4.0',
        refImage: 'doubao-seedream-4.5',
      },
    };
  }
}

function writeJson(res, status, payload) {
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'x-content-type-options': 'nosniff',
  });
  res.end(JSON.stringify(payload));
}

function writeApiError(res, status, code, message) {
  writeJson(res, status, { error: { code, message } });
}

async function readBoundedBody(req, maxBytes) {
  const declaredLength = req.headers['content-length'];
  if (declaredLength !== undefined) {
    const len = Number(declaredLength);
    if (!Number.isSafeInteger(len) || len < 0) {
      throw Object.assign(new Error('Invalid Content-Length'), { status: 400, code: 'BODY_INVALID' });
    }
    if (len > maxBytes) {
      throw Object.assign(new Error('Body exceeds limit'), { status: 413, code: 'BODY_TOO_LARGE' });
    }
  }

  const chunks = [];
  let total = 0;
  for await (const chunk of req) {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    total += buf.length;
    if (total > maxBytes) {
      throw Object.assign(new Error('Body exceeds limit'), { status: 413, code: 'BODY_TOO_LARGE' });
    }
    chunks.push(buf);
  }
  const body = Buffer.concat(chunks);
  if (declaredLength !== undefined && Number(declaredLength) !== body.length) {
    throw Object.assign(new Error('Content-Length mismatch'), { status: 400, code: 'BODY_INVALID' });
  }
  return body;
}

/**
 * Validates Step-2 COS PUT targets strictly:
 * - Sandbox mock COS: `http://127.0.0.1:<port>/object/...` (not demo server's own port)
 * - Real Tencent COS: `https://<bucket>.myqcloud.com/...`
 */
export function validateUploadTarget(rawTarget, ownPort) {
  if (typeof rawTarget !== 'string' || !rawTarget) return null;
  let url;
  try {
    url = new URL(rawTarget);
  } catch {
    return null;
  }
  if (url.username || url.password || url.hash) return null;

  let decodedPath;
  try {
    decodedPath = decodeURIComponent(url.pathname);
  } catch {
    return null;
  }
  if (/(?:^|\/)\.\.(?:\/|$)/.test(decodedPath)) return null;

  if (url.protocol === 'http:') {
    if (url.hostname !== '127.0.0.1') return null;
    if (!/^\d+$/.test(url.port)) return null;
    const port = Number(url.port);
    if (port < 1 || port > 65535 || port === ownPort) return null;
    if (!url.pathname.startsWith('/object/') || url.pathname.length <= '/object/'.length) {
      return null;
    }
    return url;
  }

  if (url.protocol === 'https:') {
    const host = url.hostname.toLowerCase();
    const myqcloudPattern =
      /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*\.myqcloud\.com$/;
    if (!myqcloudPattern.test(host)) return null;
    if (url.port !== '' && url.port !== '443') return null;
    if (!url.pathname.startsWith('/') || url.pathname.length <= 1) return null;
    return url;
  }

  return null;
}

const LOOPBACK_HOSTNAMES = new Set(['127.0.0.1', 'localhost', '[::1]', '::1']);
const SECURE_PREFIX_COOKIE_RE = /^\s*__(?:Secure|Host)-/i;

function getHeaderIgnoreCase(headers, lowerName) {
  if (!headers || typeof headers !== 'object') return undefined;
  if (Object.prototype.hasOwnProperty.call(headers, lowerName)) {
    return headers[lowerName];
  }
  for (const [k, v] of Object.entries(headers)) {
    if (k.toLowerCase() === lowerName) return v;
  }
  return undefined;
}

function getFirstHeaderToken(value) {
  if (Array.isArray(value)) {
    return String(value[0] ?? '').split(',')[0].trim();
  }
  return String(value ?? '').split(',')[0].trim();
}

export function rewriteSetCookie(cookies, { isSecureRequest = false } = {}) {
  if (!cookies) return undefined;
  const list = Array.isArray(cookies) ? cookies : [cookies];
  return list.map((cookie) => {
    const withoutDomain = cookie.replace(/;\s*domain=[^;]*/gi, '');
    if (isSecureRequest || SECURE_PREFIX_COOKIE_RE.test(withoutDomain)) {
      return withoutDomain;
    }
    return withoutDomain.replace(/;\s*secure\b/gi, '');
  });
}

export function resolveUpstreamOriginHeader(
  reqHeaders,
  method,
  upstreamOrigin,
  publicOriginEnv = process.env.MIO_PUBLIC_ORIGIN,
) {
  const normalizedMethod = String(method ?? 'GET').toUpperCase();
  const rawOrigin = getHeaderIgnoreCase(reqHeaders, 'origin');
  if (rawOrigin === undefined && (normalizedMethod === 'GET' || normalizedMethod === 'HEAD')) {
    return undefined;
  }

  const trimmedPublic = typeof publicOriginEnv === 'string' ? publicOriginEnv.trim() : '';
  if (trimmedPublic) {
    return normalizeHttpOrigin(trimmedPublic);
  }

  if (typeof rawOrigin === 'string') {
    try {
      const parsed = new URL(rawOrigin);
      if (
        (parsed.protocol === 'http:' || parsed.protocol === 'https:') &&
        (parsed.protocol === 'https:' || !LOOPBACK_HOSTNAMES.has(parsed.hostname.toLowerCase()))
      ) {
        return parsed.origin;
      }
    } catch {}
  } else if (rawOrigin === undefined) {
    const forwardedProto = getFirstHeaderToken(
      getHeaderIgnoreCase(reqHeaders, 'x-forwarded-proto'),
    ).toLowerCase();
    if (forwardedProto === 'https') {
      const rawHost =
        getFirstHeaderToken(getHeaderIgnoreCase(reqHeaders, 'x-forwarded-host')) ||
        getFirstHeaderToken(getHeaderIgnoreCase(reqHeaders, 'host'));
      if (rawHost && !/[\s/@\\?#]/.test(rawHost)) {
        try {
          const parsed = new URL(`https://${rawHost}`);
          if (
            parsed.protocol === 'https:' &&
            !parsed.username &&
            !parsed.password &&
            parsed.pathname === '/' &&
            !parsed.search &&
            !parsed.hash &&
            parsed.hostname &&
            !LOOPBACK_HOSTNAMES.has(parsed.hostname.toLowerCase())
          ) {
            return parsed.origin;
          }
        } catch {}
      }
    }
  }

  return upstreamOrigin;
}

async function probeReadyz(origin) {
  try {
    const res = await fetch(`${origin}/readyz`, { signal: AbortSignal.timeout(1500) });
    const body = await res.json().catch(() => null);
    return {
      ready: res.status === 200 && body?.status === 'ready',
      status: res.status,
      instanceId: typeof body?.instanceId === 'string' ? body.instanceId : null,
      error: res.status === 200 ? null : `HTTP_${res.status}`,
    };
  } catch (err) {
    return {
      ready: false,
      status: null,
      instanceId: null,
      error: err instanceof Error ? err.message : 'PROBE_FAILED',
    };
  }
}

async function ensureSandboxStarted() {
  const existing = await probeReadyz(SANDBOX_ORIGIN);
  if (existing.ready) {
    return { child: null, reused: true };
  }
  if (!existsSync(SANDBOX_SCRIPT)) {
    throw new Error(`Sandbox launcher not found: ${SANDBOX_SCRIPT}`);
  }

  const child = spawn(process.execPath, [SANDBOX_SCRIPT], {
    cwd: APIFOX_DIR,
    env: { ...process.env, MIO_SANDBOX_PORT: String(SANDBOX_PORT) },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let stdoutTail = '';
  let stderrTail = '';
  let seenReadyBanner = false;

  const appendTail = (prev, chunk) => {
    const next = prev + chunk.toString('utf8');
    return next.length > 16384 ? next.slice(-16384) : next;
  };

  child.stdout?.on('data', (chunk) => {
    stdoutTail = appendTail(stdoutTail, chunk);
    if (stdoutTail.includes('"sandbox_ready"')) {
      seenReadyBanner = true;
    }
  });
  child.stderr?.on('data', (chunk) => {
    stderrTail = appendTail(stderrTail, chunk);
  });

  try {
    const deadline = Date.now() + 60_000;
    while (Date.now() < deadline) {
      if (child.exitCode !== null || child.signalCode !== null) {
        throw new Error(
          `Sandbox exited early (code=${child.exitCode}, signal=${child.signalCode}): ${stderrTail || stdoutTail}`,
        );
      }
      if (seenReadyBanner) {
        const probe = await probeReadyz(SANDBOX_ORIGIN);
        if (probe.ready) return { child, reused: false };
      }
      await delay(250);
    }
    throw new Error(`Timed out waiting for Mio Sandbox readiness: ${stderrTail || stdoutTail}`);
  } catch (error) {
    await terminateChild(child);
    throw error;
  }
}

async function terminateChild(child) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, 'exit');
  child.kill('SIGTERM');
  const timerAc = new AbortController();
  try {
    await Promise.race([
      exited,
      delay(15_000, undefined, { signal: timerAc.signal }).catch(() => {}),
    ]);
  } finally {
    timerAc.abort();
  }
  if (child.exitCode === null && child.signalCode === null) {
    child.kill('SIGKILL');
    await exited;
  }
}

async function serveStatic(pathname, res) {
  if (pathname === '/favicon.ico') {
    res.writeHead(204, { 'cache-control': 'no-store' });
    res.end();
    return;
  }
  const relPath =
    pathname === '/'
      ? 'index.html'
      : pathname === '/auth/wechat/qr-complete'
        ? 'qr-complete.html'
        : pathname.replace(/^\/+/, '');
  const fullPath = resolve(PUBLIC_DIR, relPath);
  if (fullPath !== PUBLIC_DIR && !fullPath.startsWith(PUBLIC_DIR + sep)) {
    writeApiError(res, 404, 'NOT_FOUND', 'File not found');
    return;
  }
  try {
    const content = await readFile(fullPath);
    const ext = fullPath.slice(fullPath.lastIndexOf('.')).toLowerCase();
    res.writeHead(200, {
      'content-type': MIME_TYPES[ext] ?? 'application/octet-stream',
      'cache-control': 'no-store',
      'x-content-type-options': 'nosniff',
    });
    res.end(content);
  } catch {
    writeApiError(res, 404, 'NOT_FOUND', 'File not found');
  }
}

function forwardProxy(req, res, upstreamOrigin) {
  const rawUrl = req.url ?? '/';
  if (!rawUrl.startsWith('/') || rawUrl.startsWith('//')) {
    writeApiError(res, 400, 'INVALID_PATH', 'Invalid request path');
    return;
  }
  const targetUrl = new URL(rawUrl, upstreamOrigin);
  if (targetUrl.origin !== upstreamOrigin) {
    writeApiError(res, 400, 'INVALID_PATH', 'Invalid proxy target');
    return;
  }

  const headers = {};
  for (const [key, value] of Object.entries(req.headers)) {
    const lower = key.toLowerCase();
    if (value === undefined || HOP_BY_HOP_HEADERS.has(lower)) continue;
    if (lower === 'host' || lower === 'referer') continue;
    headers[lower] = value;
  }

  const method = (req.method ?? 'GET').toUpperCase();
  const isSecureRequest =
    getFirstHeaderToken(req.headers['x-forwarded-proto']).toLowerCase() === 'https' ||
    (typeof req.headers.origin === 'string' &&
      req.headers.origin.toLowerCase().startsWith('https://'));

  const resolvedOrigin = resolveUpstreamOriginHeader(req.headers, method, upstreamOrigin);
  if (resolvedOrigin !== undefined) {
    headers.origin = resolvedOrigin;
  } else {
    delete headers.origin;
  }

  const transport = targetUrl.protocol === 'https:' ? https : http;
  const isEventStreamPath = /^\/api\/v1\/conversations\/[^/]+\/events$/.test(targetUrl.pathname);

  let activeUpstreamRes = null;
  const upstreamReq = transport.request(
    targetUrl,
    {
      method,
      headers,
    },
    (upstreamRes) => {
      activeUpstreamRes = upstreamRes;
      const statusCode = upstreamRes.statusCode ?? 502;
      const contentType = String(upstreamRes.headers['content-type'] ?? '');
      const isSse =
        contentType.toLowerCase().includes('text/event-stream') ||
        (isEventStreamPath && statusCode < 400);

      const respHeaders = {};
      for (const [key, value] of Object.entries(upstreamRes.headers)) {
        const lower = key.toLowerCase();
        if (value === undefined || HOP_BY_HOP_HEADERS.has(lower)) continue;
        if (lower === 'set-cookie') {
          const rewritten = rewriteSetCookie(value, { isSecureRequest });
          if (rewritten?.length) respHeaders['set-cookie'] = rewritten;
          continue;
        }
        if (isSse && (lower === 'content-length' || lower === 'cache-control')) continue;
        respHeaders[lower] = value;
      }

      if (targetUrl.pathname === '/api/auth/callback/wechat') {
        console.log(
          JSON.stringify({
            event: 'wechat_callback_proxy',
            statusCode,
            hasCookieHeader: Boolean(req.headers.cookie),
            hasStateCookieName: /(?:^|;\s*)(?:__Secure-)?mio\.state=/.test(
              String(req.headers.cookie ?? ''),
            ),
            secFetchDest: req.headers['sec-fetch-dest'] ?? null,
            secFetchSite: req.headers['sec-fetch-site'] ?? null,
            setSessionCookie:
              Array.isArray(respHeaders['set-cookie']) &&
              respHeaders['set-cookie'].some((c) => c.includes('session_token=')),
          }),
        );
      }

      if (isSse) {
        respHeaders['cache-control'] = 'no-cache, no-transform';
        respHeaders['x-accel-buffering'] = 'no';
        req.socket?.setTimeout(0);
        res.setTimeout?.(0);
        res.socket?.setNoDelay(true);
        res.writeHead(statusCode, respHeaders);
        res.flushHeaders();

        upstreamRes.on('data', (chunk) => {
          if (res.destroyed || res.writableEnded) return;
          if (!res.write(chunk)) {
            upstreamRes.pause();
          }
        });
        res.on('drain', () => {
          upstreamRes.resume();
        });
        upstreamRes.on('end', () => {
          if (!res.writableEnded) res.end();
        });
        upstreamRes.on('error', () => {
          if (!res.writableEnded) res.end();
        });
        return;
      }

      res.writeHead(statusCode, respHeaders);
      upstreamRes.pipe(res);
    },
  );

  if (isEventStreamPath) {
    upstreamReq.setTimeout(0);
  }

  upstreamReq.on('error', (err) => {
    if (!res.headersSent) {
      writeApiError(
        res,
        502,
        'UPSTREAM_UNAVAILABLE',
        err instanceof Error ? err.message : 'Upstream connection failed',
      );
    } else if (!res.destroyed) {
      res.destroy();
    }
  });

  req.on('error', () => {
    upstreamReq.destroy();
  });

  res.on('close', () => {
    if (!res.writableFinished) {
      upstreamReq.destroy();
      if (activeUpstreamRes && !activeUpstreamRes.destroyed) {
        activeUpstreamRes.destroy();
      }
    }
  });

  if (method === 'GET' || method === 'HEAD') {
    upstreamReq.end();
  } else {
    req.pipe(upstreamReq);
  }
}

async function handleCosUpload(req, res, requestUrl, ownPort) {
  const method = (req.method ?? 'GET').toUpperCase();
  if (method !== 'PUT') {
    res.setHeader('allow', 'PUT');
    writeApiError(res, 405, 'METHOD_NOT_ALLOWED', 'Only PUT is supported');
    return;
  }

  const targetRaw = requestUrl.searchParams.get('target');
  const targetUrl = validateUploadTarget(targetRaw, ownPort);
  if (!targetUrl) {
    writeApiError(res, 400, 'UPLOAD_TARGET_DENIED', 'Upload target URL is not allowed');
    return;
  }

  let body;
  try {
    body = await readBoundedBody(req, MAX_UPLOAD_BYTES);
  } catch (err) {
    writeApiError(
      res,
      err?.status ?? 400,
      err?.code ?? 'BODY_INVALID',
      err instanceof Error ? err.message : 'Invalid upload body',
    );
    return;
  }

  const forwardHeaders = {
    'content-type': req.headers['content-type'] || 'application/octet-stream',
    'content-length': String(body.length),
  };
  if (typeof req.headers['content-md5'] === 'string') {
    forwardHeaders['content-md5'] = req.headers['content-md5'];
  }
  for (const [key, value] of Object.entries(req.headers)) {
    if (/^x-cos-[a-z0-9-]+$/i.test(key) && typeof value === 'string') {
      forwardHeaders[key.toLowerCase()] = value;
    }
  }

  const transport = targetUrl.protocol === 'https:' ? https : http;
  await new Promise((resolvePromise) => {
    const upstreamReq = transport.request(
      targetUrl,
      {
        method: 'PUT',
        headers: forwardHeaders,
        timeout: 60_000,
      },
      (upstreamRes) => {
        const chunks = [];
        upstreamRes.on('data', (chunk) => {
          chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        });
        upstreamRes.on('end', () => {
          const respBuf = Buffer.concat(chunks);
          const respHeaders = {
            'cache-control': 'no-store',
            'x-content-type-options': 'nosniff',
          };
          if (typeof upstreamRes.headers['content-type'] === 'string') {
            respHeaders['content-type'] = upstreamRes.headers['content-type'];
          }
          if (typeof upstreamRes.headers.etag === 'string') {
            respHeaders.etag = upstreamRes.headers.etag;
          }
          if (!res.headersSent) {
            res.writeHead(upstreamRes.statusCode ?? 502, respHeaders);
            res.end(respBuf);
          }
          resolvePromise();
        });
        upstreamRes.on('error', (err) => {
          if (!res.headersSent) {
            writeApiError(
              res,
              502,
              'COS_UPLOAD_FAILED',
              err instanceof Error ? err.message : 'COS relay stream failed',
            );
          }
          resolvePromise();
        });
      },
    );

    upstreamReq.on('timeout', () => {
      upstreamReq.destroy(new Error('COS_UPLOAD_TIMEOUT'));
    });

    upstreamReq.on('close', () => {
      resolvePromise();
    });

    upstreamReq.on('error', (err) => {
      if (!res.headersSent) {
        writeApiError(
          res,
          502,
          'COS_UPLOAD_FAILED',
          err instanceof Error ? err.message : 'COS relay failed',
        );
      }
      resolvePromise();
    });

    res.on('close', () => {
      if (!res.writableFinished) {
        upstreamReq.destroy();
      }
    });

    upstreamReq.end(body);
  });
}

async function main() {
  const args = new Set(process.argv.slice(2));
  const withSandbox = args.has('--with-sandbox');
  const modeIdx = process.argv.indexOf('--mode');
  const initialMode =
    modeIdx >= 0 ? process.argv[modeIdx + 1] : (process.env.DEMO_MODE ?? 'sandbox');

  if (initialMode !== 'sandbox' && initialMode !== 'dev') {
    throw new Error(`Unsupported initial mode: ${initialMode}`);
  }

  const requestedPort = parsePort(process.env.DEMO_PORT, 4320);
  const sandboxMeta = loadSandboxMeta();
  let activeMode = withSandbox ? 'sandbox' : initialMode;
  let sandboxChild = null;
  let shuttingDown = false;

  const targets = {
    sandbox: { label: 'Mio Sandbox 零扣费模拟', origin: SANDBOX_ORIGIN },
    dev: { label: 'Mio Dev 常驻环境', origin: DEV_ORIGIN },
  };

  if (withSandbox) {
    const started = await ensureSandboxStarted();
    sandboxChild = started.child;
  }

  let ownPort = requestedPort;
  const server = http.createServer((req, res) => {
    void (async () => {
      let requestUrl;
      try {
        requestUrl = new URL(req.url ?? '/', `http://${HOST}:${ownPort || 4320}`);
      } catch {
        writeApiError(res, 400, 'INVALID_URL', 'Malformed request URL');
        return;
      }
      const { pathname } = requestUrl;

      if (pathname === '/demo-api/config') {
        const method = (req.method ?? 'GET').toUpperCase();
        if (method === 'POST') {
          let raw;
          try {
            raw = await readBoundedBody(req, MAX_JSON_BODY_BYTES);
          } catch (err) {
            writeApiError(res, err?.status ?? 400, err?.code ?? 'BODY_INVALID', 'Invalid body');
            return;
          }
          let parsed;
          try {
            parsed = JSON.parse(raw.toString('utf8'));
          } catch {
            writeApiError(res, 400, 'BODY_INVALID', 'Invalid JSON');
            return;
          }
          if (parsed?.mode !== 'sandbox' && parsed?.mode !== 'dev') {
            writeApiError(res, 400, 'INVALID_CONFIG', 'mode must be sandbox or dev');
            return;
          }
          activeMode = parsed.mode;
        } else if (method !== 'GET') {
          res.setHeader('allow', 'GET, POST');
          writeApiError(res, 405, 'METHOD_NOT_ALLOWED', 'Method not allowed');
          return;
        }

        const upstreamOrigin = targets[activeMode].origin;
        const health = await probeReadyz(upstreamOrigin);
        writeJson(res, 200, {
          mode: activeMode,
          upstreamOrigin,
          withSandbox,
          managedSandbox: sandboxChild !== null,
          targets,
          health,
          sandboxAccount: activeMode === 'sandbox' ? sandboxMeta.sandboxAccount : null,
          recommendedModels: sandboxMeta.recommendedModels,
        });
        return;
      }

      if (pathname === '/demo-api/cos-upload') {
        await handleCosUpload(req, res, requestUrl, ownPort);
        return;
      }

      if (
        pathname === '/healthz' ||
        pathname === '/readyz' ||
        pathname === '/api' ||
        pathname.startsWith('/api/')
      ) {
        forwardProxy(req, res, targets[activeMode].origin);
        return;
      }

      await serveStatic(pathname, res);
    })().catch((err) => {
      if (!res.headersSent) {
        writeApiError(
          res,
          500,
          'INTERNAL_ERROR',
          err instanceof Error ? err.message : 'Unexpected server error',
        );
      } else if (!res.destroyed) {
        res.destroy();
      }
    });
  });

  const shutdown = async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    server.closeAllConnections?.();
    await new Promise((resolveClose) => server.close(() => resolveClose()));
    await terminateChild(sandboxChild);
  };

  process.once('SIGINT', () => {
    void shutdown().then(() => process.exit(0));
  });
  process.once('SIGTERM', () => {
    void shutdown().then(() => process.exit(0));
  });

  try {
    server.listen(requestedPort, HOST);
    await once(server, 'listening');
    const addr = server.address();
    ownPort = typeof addr === 'object' && addr ? addr.port : requestedPort;
    console.log(
      JSON.stringify({
        status: 'demo_ready',
        url: `http://${HOST}:${ownPort}`,
        mode: activeMode,
        upstreamOrigin: targets[activeMode].origin,
        managedSandbox: sandboxChild !== null,
      }),
    );
  } catch (err) {
    await shutdown();
    throw err;
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  await main();
}
