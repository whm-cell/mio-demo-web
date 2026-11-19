import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { existsSync, readFileSync } from 'node:fs';
import http from 'node:http';
import { createRequire } from 'node:module';
import { dirname, join, resolve } from 'node:path';
import test, { describe } from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import {
  resolveUpstreamOriginHeader,
  rewriteSetCookie,
  validateUploadTarget,
} from '../server.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const demoRoot = resolve(__dirname, '..');
const apifoxRoot = resolve(demoRoot, '../mio-apifox');
const mioRoot = resolve(demoRoot, '../mio');
const samplePngPath = join(apifoxRoot, 'fixtures/sample.png');

const requireFromMio = createRequire(join(mioRoot, 'package.json'));
const { Client } = requireFromMio('pg');

function readRegressionDbUrl() {
  if (process.env.MIO_TEST_DATABASE_URL) return process.env.MIO_TEST_DATABASE_URL;
  const envPath = join(mioRoot, '.env');
  if (existsSync(envPath)) {
    for (const line of readFileSync(envPath, 'utf8').split(/\r?\n/)) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) continue;
      const eq = trimmed.indexOf('=');
      if (eq < 0) continue;
      const key = trimmed.slice(0, eq).trim();
      let val = trimmed.slice(eq + 1).trim();
      if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
        val = val.slice(1, -1);
      }
      if (key === 'MIO_TEST_DATABASE_URL' && val) return val;
    }
  }
  throw new Error('MIO_TEST_DATABASE_URL not found');
}

async function getFreePort() {
  const srv = http.createServer();
  srv.listen(0, '127.0.0.1');
  await once(srv, 'listening');
  const { port } = srv.address();
  await new Promise((r) => srv.close(() => r()));
  return port;
}

class CookieJar {
  #cookies = new Map();

  ingest(res) {
    const setCookies = typeof res.headers.getSetCookie === 'function' ? res.headers.getSetCookie() : [];
    for (const raw of setCookies) {
      assert.ok(!/;\s*domain=/i.test(raw), `Set-Cookie must be host-only (no Domain=): ${raw}`);
      const pair = raw.split(';')[0];
      const eq = pair.indexOf('=');
      if (eq > 0) {
        this.#cookies.set(pair.slice(0, eq).trim(), pair.slice(eq + 1).trim());
      }
    }
  }

  headerValue() {
    return [...this.#cookies.entries()].map(([k, v]) => `${k}=${v}`).join('; ');
  }
}

function openSseReader(url, cookieHeader) {
  const controller = new AbortController();
  const events = [];
  const waiters = [];

  const notify = () => {
    while (waiters.length > 0) {
      const { predicate, resolve: res, reject: rej } = waiters[0];
      const found = events.find(predicate);
      if (found) {
        waiters.shift();
        res(found);
      } else {
        break;
      }
    }
  };

  const started = (async () => {
    const res = await fetch(url, {
      headers: { cookie: cookieHeader, accept: 'text/event-stream' },
      signal: controller.signal,
    });
    assert.equal(res.status, 200);
    assert.match(res.headers.get('content-type') || '', /^text\/event-stream/i);
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    void (async () => {
      try {
        while (true) {
          const { value, done } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          let boundary;
          while ((boundary = buffer.indexOf('\n\n')) >= 0) {
            const rawFrame = buffer.slice(0, boundary);
            buffer = buffer.slice(boundary + 2);
            let eventType = 'message';
            const dataLines = [];
            for (const line of rawFrame.split(/\r?\n/)) {
              if (line.startsWith('event:')) eventType = line.slice(6).trim();
              else if (line.startsWith('data:')) dataLines.push(line.slice(5).trim());
            }
            if (dataLines.length > 0) {
              const rawData = dataLines.join('\n');
              let data = rawData;
              try {
                data = JSON.parse(rawData);
              } catch {}
              events.push({ event: eventType, data });
              notify();
            }
          }
        }
      } catch (err) {
        if (!controller.signal.aborted) {
          for (const w of waiters.splice(0)) w.rej(err);
        }
      }
    })();
  })();

  return {
    events,
    async waitFor(predicate, timeoutMs = 20_000, label = 'SSE event') {
      await started;
      const existing = events.find(predicate);
      if (existing) return existing;
      const timerAc = new AbortController();
      try {
        return await Promise.race([
          new Promise((res, rej) => {
            waiters.push({ predicate, resolve: res, reject: rej });
          }),
          delay(timeoutMs, undefined, { signal: timerAc.signal }).then(() => {
            throw new Error(`Timed out waiting for ${label}; seen=${JSON.stringify(events)}`);
          }),
        ]);
      } finally {
        timerAc.abort();
      }
    },
    close() {
      controller.abort();
    },
  };
}

test('validateUploadTarget strictly enforces COS and loopback sandbox allowlist', () => {
  assert.ok(validateUploadTarget('http://127.0.0.1:54321/object/mio%2Fstaging%2Fabc', 4320));
  assert.ok(
    validateUploadTarget(
      'https://mio-bucket-1250000000.cos.ap-beijing.myqcloud.com/mio/staging/abc?q=1',
      4320,
    ),
  );

  const rejected = [
    null,
    '',
    'ftp://127.0.0.1:54321/object/abc',
    'http://127.0.0.1:4320/object/abc',
    'http://127.0.0.1:4319/api/v1/me',
    'http://127.0.0.1:54321/object/../api/v1/me',
    'http://localhost:54321/object/abc',
    'http://169.254.169.254/latest/meta-data/',
    'https://evil.com/object/abc',
    'https://myqcloud.com/mio/staging/abc',
    'https://myqcloud.com.evil.com/mio/staging/abc',
    'https://user:pass@bucket.cos.ap-beijing.myqcloud.com/mio/staging/abc',
    'http://bucket.cos.ap-beijing.myqcloud.com/mio/staging/abc',
  ];

  for (const target of rejected) {
    assert.equal(validateUploadTarget(target, 4320), null, `Expected rejection for: ${target}`);
  }
});

test('rewriteSetCookie strips Domain and preserves Secure on HTTPS or __Secure-/__Host- cookies', () => {
  assert.equal(rewriteSetCookie(undefined), undefined);

  const plainAndPrefixed = [
    'mio.session_token=abc; Path=/; HttpOnly; SameSite=Lax; Domain=.example.com; Secure',
    '__Secure-mio.wechat_qr=qr1; Path=/api/auth/wechat/qr-session/id/poll; Max-Age=900; HttpOnly; SameSite=Lax; Domain=mio.example.com; Secure',
    '__Host-mio.state=st1; Path=/; HttpOnly; SameSite=Lax; Secure',
  ];

  // Plain HTTP request: strip Secure only from non-prefixed cookies; always strip Domain.
  const httpRewritten = rewriteSetCookie(plainAndPrefixed, { isSecureRequest: false });
  assert.equal(httpRewritten.length, 3);
  assert.doesNotMatch(httpRewritten[0], /;\s*domain=/i);
  assert.doesNotMatch(httpRewritten[0], /;\s*secure\b/i);
  assert.doesNotMatch(httpRewritten[1], /;\s*domain=/i);
  assert.match(httpRewritten[1], /;\s*secure\b/i);
  assert.match(httpRewritten[2], /;\s*secure\b/i);

  // HTTPS request: preserve Secure on all cookies, still strip Domain.
  const httpsRewritten = rewriteSetCookie(plainAndPrefixed, { isSecureRequest: true });
  for (const c of httpsRewritten) {
    assert.doesNotMatch(c, /;\s*domain=/i);
    assert.match(c, /;\s*secure\b/i);
  }

  // Single string input with case-insensitive __secure- prefix
  const single = rewriteSetCookie('__secure-mio.token=v1; Path=/; Secure');
  assert.deepEqual(single, ['__secure-mio.token=v1; Path=/; Secure']);
});

test('resolveUpstreamOriginHeader handles GET/HEAD, MIO_PUBLIC_ORIGIN, HTTPS/non-loopback passthrough, and loopback rewrite', () => {
  const upstream = 'http://localhost:4310';

  // GET / HEAD without Origin: never synthesize Origin
  assert.equal(resolveUpstreamOriginHeader({}, 'GET', upstream, ''), undefined);
  assert.equal(resolveUpstreamOriginHeader({}, 'HEAD', upstream, 'https://pub.example.com'), undefined);
  assert.equal(
    resolveUpstreamOriginHeader({ 'x-forwarded-proto': 'https', host: 'mio-qr.example.com' }, 'GET', upstream, ''),
    undefined,
  );

  // Explicit MIO_PUBLIC_ORIGIN takes precedence when Origin is present or method is mutating
  assert.equal(
    resolveUpstreamOriginHeader(
      { origin: 'http://127.0.0.1:4320' },
      'POST',
      upstream,
      'https://mio-public.example.com/ignored-path',
    ),
    'https://mio-public.example.com',
  );
  assert.equal(
    resolveUpstreamOriginHeader(
      { origin: 'https://other.example.com' },
      'GET',
      upstream,
      'https://mio-public.example.com',
    ),
    'https://mio-public.example.com',
  );

  // HTTPS or non-loopback HTTP Origin passes through verbatim
  assert.equal(
    resolveUpstreamOriginHeader({ origin: 'https://mio-qr.example.com' }, 'POST', upstream, ''),
    'https://mio-qr.example.com',
  );
  assert.equal(
    resolveUpstreamOriginHeader({ origin: 'https://mio-qr.example.com' }, 'GET', upstream, ''),
    'https://mio-qr.example.com',
  );
  assert.equal(
    resolveUpstreamOriginHeader({ origin: 'http://mio-qr.example.com:8080' }, 'POST', upstream, ''),
    'http://mio-qr.example.com:8080',
  );

  // Loopback HTTP Origin rewrites to upstreamOrigin (Sandbox & local Dev compatibility)
  for (const loopback of [
    'http://127.0.0.1:4320',
    'http://localhost:4320',
    'http://[::1]:4320',
  ]) {
    assert.equal(
      resolveUpstreamOriginHeader({ origin: loopback }, 'POST', upstream, ''),
      upstream,
      `Expected loopback origin ${loopback} to rewrite to ${upstream}`,
    );
  }

  // POST without Origin behind HTTPS reverse proxy synthesizes https://<host> for non-loopback host
  assert.equal(
    resolveUpstreamOriginHeader(
      { 'x-forwarded-proto': 'https, http', 'x-forwarded-host': 'mio-qr.example.com' },
      'POST',
      upstream,
      '',
    ),
    'https://mio-qr.example.com',
  );
  assert.equal(
    resolveUpstreamOriginHeader(
      { 'x-forwarded-proto': 'https', host: 'mio-qr.example.com:8443' },
      'POST',
      upstream,
      '',
    ),
    'https://mio-qr.example.com:8443',
  );
  // Loopback host under x-forwarded-proto falls back to upstreamOrigin
  assert.equal(
    resolveUpstreamOriginHeader(
      { 'x-forwarded-proto': 'https', host: '127.0.0.1:4320' },
      'POST',
      upstream,
      '',
    ),
    upstream,
  );
  // Local POST without Origin or forwarded proto falls back to upstreamOrigin
  assert.equal(resolveUpstreamOriginHeader({}, 'POST', upstream, ''), upstream);
});

test(
  'server.mjs dev proxy serves /auth/wechat/qr-complete and preserves HTTPS Origin and __Secure- cookies',
  { timeout: 30_000 },
  async () => {
    const observedRequests = [];
    const mockUpstream = http.createServer(async (req, res) => {
      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      const bodyText = Buffer.concat(chunks).toString('utf8');
      observedRequests.push({
        method: req.method,
        url: req.url,
        origin: req.headers.origin,
        xForwardedProto: req.headers['x-forwarded-proto'],
        body: bodyText,
      });

      if (req.url === '/readyz') {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ status: 'ready', instanceId: 'mock-dev-1' }));
        return;
      }

      if (req.method === 'POST' && req.url === '/api/auth/wechat/qr-session') {
        res.writeHead(201, {
          'content-type': 'application/json',
          'cache-control': 'no-store',
          'set-cookie': [
            '__Secure-mio.wechat_qr=qr-cookie-val; Path=/api/auth/wechat/qr-session/qr-1/poll; Max-Age=900; HttpOnly; SameSite=Lax; Domain=.mio-qr.example.com; Secure',
            '__Secure-mio.session_token=sess-val; Path=/; HttpOnly; SameSite=Lax; Secure',
            'mio.plain_cookie=plain-val; Path=/; HttpOnly; SameSite=Lax; Domain=.mio-qr.example.com; Secure',
          ],
        });
        res.end(
          JSON.stringify({
            qrcode_id: 'qr-1',
            authorization_url: 'https://open.weixin.qq.com/connect/qrconnect?appid=wx-demo',
            expire_seconds: 300,
          }),
        );
        return;
      }

      if (req.method === 'GET' && req.url?.startsWith('/api/auth/callback/wechat')) {
        if (!req.headers.cookie) {
          res.writeHead(400, {
            'content-type': 'application/json',
            'cache-control': 'no-store',
          });
          res.end(JSON.stringify({ code: 'STATE_MISMATCH', message: 'State cookie missing' }));
          return;
        }
        res.writeHead(302, {
          location: 'https://mio-qr.example.com/auth/wechat/qr-complete',
          'cache-control': 'no-store',
          'set-cookie': [
            '__Secure-mio.session_token=cb-sess-val; Path=/; HttpOnly; SameSite=Lax; Secure',
          ],
        });
        res.end();
        return;
      }

      res.writeHead(404, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: { code: 'NOT_FOUND', message: 'Not found' } }));
    });

    mockUpstream.listen(0, '127.0.0.1');
    await once(mockUpstream, 'listening');
    const upstreamPort = mockUpstream.address().port;
    const mockUpstreamOrigin = `http://127.0.0.1:${upstreamPort}`;

    const child = spawn(process.execPath, ['./server.mjs'], {
      cwd: demoRoot,
      env: {
        ...process.env,
        DEMO_MODE: 'dev',
        DEMO_PORT: '0',
        MIO_DEV_ORIGIN: mockUpstreamOrigin,
        MIO_PUBLIC_ORIGIN: '',
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    let stdout = '';
    let stderr = '';
    let demoReady = null;
    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString('utf8');
      for (const line of stdout.split(/\r?\n/)) {
        if (!line.trim().startsWith('{')) continue;
        try {
          const parsed = JSON.parse(line);
          if (parsed.status === 'demo_ready' && parsed.url) demoReady = parsed;
        } catch {}
      }
    });
    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString('utf8');
    });

    const stopChild = async () => {
      if (child.exitCode === null && child.signalCode === null) {
        const exited = once(child, 'exit');
        child.kill('SIGTERM');
        const timerAc = new AbortController();
        let result;
        try {
          result = await Promise.race([
            exited,
            delay(10_000, undefined, { signal: timerAc.signal }).then(() => 'timeout'),
          ]);
        } finally {
          timerAc.abort();
        }
        if (result === 'timeout') {
          child.kill('SIGKILL');
          return await exited;
        }
        return result;
      }
      return [child.exitCode, child.signalCode];
    };

    try {
      const deadline = Date.now() + 15_000;
      while (!demoReady && Date.now() < deadline) {
        if (child.exitCode !== null || child.signalCode !== null) {
          throw new Error(`server.mjs exited early: code=${child.exitCode}\n${stdout}\n${stderr}`);
        }
        await delay(100);
      }
      assert.ok(demoReady?.url, `demo proxy failed to start: ${stderr || stdout}`);
      const baseUrl = demoReady.url;

      // 1. /auth/wechat/qr-complete serves completion HTML with no-store & nosniff (wake-only same-origin postMessage, zero innerHTML/opener)
      const completeRes = await fetch(`${baseUrl}/auth/wechat/qr-complete`);
      assert.equal(completeRes.status, 200);
      assert.equal(completeRes.headers.get('content-type'), 'text/html; charset=utf-8');
      assert.equal(completeRes.headers.get('cache-control'), 'no-store');
      assert.equal(completeRes.headers.get('x-content-type-options'), 'nosniff');
      const completeHtml = await completeRes.text();
      assert.match(completeHtml, /微信授权已完成/);
      assert.match(completeHtml, /window\.self !== window\.top/);
      assert.match(completeHtml, /mio-wechat-qr-callback-landed/);
      assert.match(completeHtml, /window\.parent\.postMessage/);
      assert.match(completeHtml, /window\.location\.origin/);
      assert.doesNotMatch(completeHtml, /window\.opener|innerHTML|postMessage\([^)]*['"]\*['"]/);

      const completeErrRes = await fetch(
        `${baseUrl}/auth/wechat/qr-complete?error=WECHAT_AUTH_FAILED`,
      );
      assert.equal(completeErrRes.status, 200);
      assert.equal(completeErrRes.headers.get('content-type'), 'text/html; charset=utf-8');
      assert.equal(completeErrRes.headers.get('cache-control'), 'no-store');

      // 2. HTTPS tunnel POST /api/auth/wechat/qr-session preserves Origin and ; Secure cookies
      const qrRes = await fetch(`${baseUrl}/api/auth/wechat/qr-session`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          origin: 'https://mio-qr.example.com',
          'x-forwarded-proto': 'https',
        },
        body: JSON.stringify({ callbackURL: '/auth/wechat/qr-complete' }),
      });
      assert.equal(qrRes.status, 201);
      const qrSetCookies = qrRes.headers.getSetCookie();
      const qrCookie = qrSetCookies.find((c) => c.startsWith('__Secure-mio.wechat_qr='));
      const sessCookie = qrSetCookies.find((c) => c.startsWith('__Secure-mio.session_token='));
      const plainCookieHttps = qrSetCookies.find((c) => c.startsWith('mio.plain_cookie='));
      assert.ok(qrCookie, 'Expected __Secure-mio.wechat_qr Set-Cookie');
      assert.ok(sessCookie, 'Expected __Secure-mio.session_token Set-Cookie');
      assert.ok(plainCookieHttps, 'Expected mio.plain_cookie Set-Cookie');
      for (const c of [qrCookie, sessCookie, plainCookieHttps]) {
        assert.match(c, /;\s*secure\b/i, `HTTPS proxy must preserve ; Secure on ${c}`);
        assert.doesNotMatch(c, /;\s*domain=/i, `Proxy must strip Domain on ${c}`);
      }
      const upstreamQrReq = observedRequests.find(
        (r) => r.method === 'POST' && r.url === '/api/auth/wechat/qr-session',
      );
      assert.equal(upstreamQrReq?.origin, 'https://mio-qr.example.com');

      // 3. HTTPS callback GET without Origin does not synthesize Origin, preserves ; Secure, and logs sanitized telemetry
      const cbRes = await fetch(
        `${baseUrl}/api/auth/callback/wechat?code=secret-code-123&state=secret-state-456`,
        {
          redirect: 'manual',
          headers: {
            'x-forwarded-proto': 'https',
            cookie: '__Secure-mio.state=secret-state-cookie-789',
            'sec-fetch-dest': 'iframe',
            'sec-fetch-site': 'cross-site',
          },
        },
      );
      assert.equal(cbRes.status, 302);
      assert.equal(
        cbRes.headers.get('location'),
        'https://mio-qr.example.com/auth/wechat/qr-complete',
      );
      const cbCookie = cbRes.headers
        .getSetCookie()
        .find((c) => c.startsWith('__Secure-mio.session_token='));
      assert.ok(cbCookie);
      assert.match(cbCookie, /;\s*secure\b/i);
      const upstreamCbReq = observedRequests.find((r) =>
        r.url?.startsWith('/api/auth/callback/wechat'),
      );
      assert.equal(upstreamCbReq?.origin, undefined);

      // 3b. Callback GET missing state cookie logs hasCookieHeader:false / hasStateCookieName:false / statusCode:400
      const cbMissingCookieRes = await fetch(
        `${baseUrl}/api/auth/callback/wechat?code=secret-code-999&state=secret-state-999`,
        {
          redirect: 'manual',
          headers: {
            'x-forwarded-proto': 'https',
            'sec-fetch-dest': 'iframe',
            'sec-fetch-site': 'cross-site',
          },
        },
      );
      assert.equal(cbMissingCookieRes.status, 400);

      // 4. Plain local HTTP POST rewrites loopback Origin -> mockUpstreamOrigin; strips Secure only on plain cookies
      observedRequests.length = 0;
      const localRes = await fetch(`${baseUrl}/api/auth/wechat/qr-session`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          origin: 'http://127.0.0.1:4320',
        },
        body: JSON.stringify({ callbackURL: '/auth/wechat/qr-complete' }),
      });
      assert.equal(localRes.status, 201);
      assert.equal(observedRequests[0]?.origin, mockUpstreamOrigin);
      const localCookies = localRes.headers.getSetCookie();
      assert.match(
        localCookies.find((c) => c.startsWith('__Secure-mio.wechat_qr=')),
        /;\s*secure\b/i,
      );
      assert.doesNotMatch(
        localCookies.find((c) => c.startsWith('mio.plain_cookie=')),
        /;\s*secure\b/i,
      );

      const [exitCode] = await stopChild();
      assert.equal(exitCode, 0);

      const cbLogs = stdout
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter((line) => line.startsWith('{'))
        .map((line) => {
          try {
            return JSON.parse(line);
          } catch {
            return null;
          }
        })
        .filter((entry) => entry?.event === 'wechat_callback_proxy');

      assert.equal(cbLogs.length, 2, `Expected 2 wechat_callback_proxy log lines, got: ${stdout}`);
      assert.deepEqual(cbLogs[0], {
        event: 'wechat_callback_proxy',
        statusCode: 302,
        hasCookieHeader: true,
        hasStateCookieName: true,
        secFetchDest: 'iframe',
        secFetchSite: 'cross-site',
        setSessionCookie: true,
      });
      assert.deepEqual(cbLogs[1], {
        event: 'wechat_callback_proxy',
        statusCode: 400,
        hasCookieHeader: false,
        hasStateCookieName: false,
        secFetchDest: 'iframe',
        secFetchSite: 'cross-site',
        setSessionCookie: false,
      });

      // Sanitized telemetry must NEVER leak query code/state or raw cookie values
      const combinedLogs = `${stdout}\n${stderr}`;
      for (const secret of [
        'secret-code-123',
        'secret-state-456',
        'secret-state-cookie-789',
        'cb-sess-val',
        'secret-code-999',
        'secret-state-999',
      ]) {
        assert.ok(
          !combinedLogs.includes(secret),
          `Proxy logs leaked sensitive value ${secret}: ${combinedLogs}`,
        );
      }
    } finally {
      await stopChild();
      mockUpstream.closeAllConnections?.();
      await new Promise((r) => mockUpstream.close(() => r()));
    }
  },
);

async function listSandboxSchemas() {
  const dbClient = new Client({ connectionString: readRegressionDbUrl() });
  await dbClient.connect();
  try {
    const schemaRes = await dbClient.query(
      "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'apifox_sbx_%' ORDER BY schema_name",
    );
    return schemaRes.rows.map((r) => r.schema_name);
  } finally {
    await dbClient.end();
  }
}

test('server.mjs --with-sandbox full E2E flow and clean schema teardown', { timeout: 90_000 }, async () => {
  const schemasBefore = new Set(await listSandboxSchemas());
  const sandboxPort = await getFreePort();
  const child = spawn(process.execPath, ['./server.mjs', '--with-sandbox'], {
    cwd: demoRoot,
    env: {
      ...process.env,
      DEMO_PORT: '0',
      MIO_SANDBOX_PORT: String(sandboxPort),
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let stdout = '';
  let stderr = '';
  let demoReady = null;

  child.stdout.on('data', (chunk) => {
    stdout += chunk.toString('utf8');
    for (const line of stdout.split(/\r?\n/)) {
      if (!line.trim().startsWith('{')) continue;
      try {
        const parsed = JSON.parse(line);
        if (parsed.status === 'demo_ready' && parsed.url) {
          demoReady = parsed;
        }
      } catch {}
    }
  });
  child.stderr.on('data', (chunk) => {
    stderr += chunk.toString('utf8');
  });

  const stopChild = async () => {
    if (child.exitCode === null && child.signalCode === null) {
      const exited = once(child, 'exit');
      child.kill('SIGTERM');
      const timerAc = new AbortController();
      let result;
      try {
        result = await Promise.race([
          exited,
          delay(20_000, undefined, { signal: timerAc.signal }).then(() => 'timeout'),
        ]);
      } finally {
        timerAc.abort();
      }
      if (result === 'timeout') {
        child.kill('SIGKILL');
        return await exited;
      }
      return result;
    }
    return [child.exitCode, child.signalCode];
  };

  let sse = null;
  try {
    const startDeadline = Date.now() + 60_000;
    while (!demoReady && Date.now() < startDeadline) {
      if (child.exitCode !== null || child.signalCode !== null) {
        throw new Error(`server.mjs exited early: code=${child.exitCode}\n${stdout}\n${stderr}`);
      }
      await delay(200);
    }
    assert.ok(demoReady?.url, `demo server failed to report ready: ${stderr || stdout}`);
    const baseUrl = demoReady.url;
    const schemasDuring = await listSandboxSchemas();
    const createdByRun = schemasDuring.filter((s) => !schemasBefore.has(s));
    assert.equal(
      createdByRun.length,
      1,
      `Expected this test run to create 1 isolated apifox_sbx_% schema, found: ${JSON.stringify(createdByRun)}`,
    );

    // 1. Static UI assets served (200)
    for (const [path, ct] of [
      ['/', 'text/html'],
      ['/styles.css', 'text/css'],
      ['/api.js', 'text/javascript'],
      ['/app.js', 'text/javascript'],
    ]) {
      const res = await fetch(`${baseUrl}${path}`);
      assert.equal(res.status, 200, `GET ${path} should be 200`);
      assert.match(res.headers.get('content-type') || '', new RegExp(ct));
      const body = await res.text();
      assert.ok(body.length > 50);
    }

    // 2. Demo config & /demo-api/cos-upload SSRF rejection
    const cfgRes = await fetch(`${baseUrl}/demo-api/config`);
    assert.equal(cfgRes.status, 200);
    const cfg = await cfgRes.json();
    assert.equal(cfg.mode, 'sandbox');
    assert.equal(cfg.health.ready, true);
    assert.ok(cfg.sandboxAccount?.email && cfg.sandboxAccount?.password);

    const ssrfRes1 = await fetch(
      `${baseUrl}/demo-api/cos-upload?target=${encodeURIComponent('https://evil.example.com/upload')}`,
      { method: 'PUT', body: Buffer.from('abc') },
    );
    assert.equal(ssrfRes1.status, 400);
    assert.equal((await ssrfRes1.json()).error.code, 'UPLOAD_TARGET_DENIED');

    const ssrfRes2 = await fetch(
      `${baseUrl}/demo-api/cos-upload?target=${encodeURIComponent(`http://127.0.0.1:${sandboxPort}/api/v1/me`)}`,
      { method: 'PUT', body: Buffer.from('abc') },
    );
    assert.equal(ssrfRes2.status, 400);
    assert.equal((await ssrfRes2.json()).error.code, 'UPLOAD_TARGET_DENIED');

    // 2b. Unauthenticated /events returns 401 JSON without SSE header mutation
    const unauthSseRes = await fetch(
      `${baseUrl}/api/v1/conversations/00000000-0000-4000-8000-000000000001/events`,
    );
    assert.equal(unauthSseRes.status, 401);
    assert.match(unauthSseRes.headers.get('content-type') || '', /^application\/json/);
    assert.equal(unauthSseRes.headers.get('cache-control'), 'no-store');
    assert.equal(unauthSseRes.headers.get('x-accel-buffering'), null);
    assert.equal((await unauthSseRes.json()).error.code, 'UNAUTHORIZED');

    // 3. Sandbox sign-in cookie flow (testing Origin rewrite & Set-Cookie Domain stripping)
    const jar = new CookieJar();
    const loginRes = await fetch(`${baseUrl}/api/auth/sign-in/email`, {
      method: 'POST',
      headers: { origin: baseUrl, 'content-type': 'application/json' },
      body: JSON.stringify({
        email: cfg.sandboxAccount.email,
        password: cfg.sandboxAccount.password,
      }),
    });
    assert.equal(loginRes.status, 200);
    jar.ingest(loginRes);
    assert.ok(jar.headerValue().length > 0, 'Session cookie must be set');

    const meRes = await fetch(`${baseUrl}/api/v1/me`, {
      headers: { cookie: jar.headerValue() },
    });
    assert.equal(meRes.status, 200);
    const me = await meRes.json();
    assert.ok(me.userId && me.sessionId);

    // 4. Catalog read WITHOUT ?category=
    const modelsRes = await fetch(`${baseUrl}/api/v1/providers/onelink/models`, {
      headers: { cookie: jar.headerValue() },
    });
    assert.equal(modelsRes.status, 200);
    const catalog = await modelsRes.json();
    const chatModel = catalog.items.find(
      (i) => i.runnable && i.adaptation?.metadata?.inputModalities?.includes('image'),
    );
    const img40 = catalog.items.find((i) => i.modelId === 'doubao-seedream-4.0' && i.image?.runnable);
    const img45 = catalog.items.find((i) => i.modelId === 'doubao-seedream-4.5' && i.image?.runnable);
    assert.ok(chatModel, 'Expected runnable vision chat model');
    assert.ok(img40, 'Expected runnable doubao-seedream-4.0');
    assert.ok(img45, 'Expected runnable doubao-seedream-4.5');

    // 5. 3-step PNG upload via /demo-api/cos-upload
    const samplePngBytes = readFileSync(samplePngPath);
    const uploadInitRes = await fetch(`${baseUrl}/api/v1/assets/uploads`, {
      method: 'POST',
      headers: { origin: baseUrl, cookie: jar.headerValue(), 'content-type': 'application/json' },
      body: JSON.stringify({
        filename: 'sample.png',
        contentType: 'image/png',
        size: samplePngBytes.length,
      }),
    });
    assert.equal(uploadInitRes.status, 201);
    const uploadInit = await uploadInitRes.json();
    assert.ok(uploadInit.assetId && uploadInit.uploadUrl);

    const relayPutRes = await fetch(
      `${baseUrl}/demo-api/cos-upload?target=${encodeURIComponent(uploadInit.uploadUrl)}`,
      {
        method: 'PUT',
        headers: { 'content-type': 'image/png' },
        body: samplePngBytes,
      },
    );
    assert.equal(relayPutRes.status, 200);

    const completeRes = await fetch(`${baseUrl}/api/v1/assets/${uploadInit.assetId}/complete`, {
      method: 'POST',
      headers: { origin: baseUrl, cookie: jar.headerValue(), 'content-type': 'application/json' },
      body: '{}',
    });
    assert.equal(completeRes.status, 200);
    const uploadedAsset = await completeRes.json();
    assert.equal(uploadedAsset.status, 'ready');
    assert.ok(uploadedAsset.downloadUrl);

    // 6. Conversation create + live SSE stream during Turn 1 image generation
    const convRes = await fetch(`${baseUrl}/api/v1/conversations`, {
      method: 'POST',
      headers: { origin: baseUrl, cookie: jar.headerValue(), 'content-type': 'application/json' },
      body: JSON.stringify({ modelRef: chatModel.modelRef }),
    });
    assert.equal(convRes.status, 201);
    const conv = await convRes.json();
    assert.ok(conv.id);

    sse = openSseReader(`${baseUrl}/api/v1/conversations/${conv.id}/events`, jar.headerValue());
    const snap = await sse.waitFor((e) => e.event === 'snapshot', 10_000, 'initial snapshot');
    assert.equal(snap.data.conversationId, conv.id);

    const turn1ReqId = `e2e-turn1-${Date.now()}`;
    const turn1SendRes = await fetch(`${baseUrl}/api/v1/conversations/${conv.id}/messages`, {
      method: 'POST',
      headers: { origin: baseUrl, cookie: jar.headerValue(), 'content-type': 'application/json' },
      body: JSON.stringify({
        requestId: turn1ReqId,
        text: '请先调用 list_image_models 查看可用生图模型，然后调用 submit_image_job 使用 doubao-seedream-4.0 生成 1 张极简蓝色几何方块白色背景测试图，并告诉我任务 ID。',
      }),
    });
    assert.equal(turn1SendRes.status, 202);

    const turn1Tool = await sse.waitFor(
      (e) => e.event === 'tool' && e.data?.requestId === turn1ReqId && e.data?.tool === 'submit_image_job',
      20_000,
      'Turn 1 submit_image_job tool event',
    );
    const turn1JobId = turn1Tool.data.job.jobId;
    assert.ok(turn1JobId);

    await sse.waitFor(
      (e) => e.event === 'text' && e.data?.requestId === turn1ReqId && typeof e.data?.text === 'string',
      20_000,
      'Turn 1 text stream event',
    );

    const turn1Done = await sse.waitFor(
      (e) =>
        e.event === 'state' &&
        e.data?.receipt?.requestId === turn1ReqId &&
        e.data?.receipt?.status === 'completed',
      20_000,
      'Turn 1 state completed',
    );
    assert.equal(turn1Done.data.receipt.status, 'completed');

    const hist1Res = await fetch(
      `${baseUrl}/api/v1/conversations/${conv.id}/history?afterSeq=-1&limit=100`,
      { headers: { cookie: jar.headerValue() } },
    );
    assert.equal(hist1Res.status, 200);
    const hist1 = await hist1Res.json();
    const afterTurn1Seq = hist1.nextAfterSeq;
    assert.ok(
      hist1.items.some((i) => i.role === 'tool' && i.tool === 'submit_image_job' && i.job?.jobId === turn1JobId),
    );

    // 7. Poll Turn 1 image job until completed + verify asset1Id ready
    let job1 = null;
    for (let i = 0; i < 30; i++) {
      const res = await fetch(`${baseUrl}/api/v1/compute/images/${turn1JobId}`, {
        headers: { cookie: jar.headerValue() },
      });
      assert.equal(res.status, 200);
      job1 = await res.json();
      if (job1.status === 'completed') break;
      await delay(500);
    }
    assert.equal(job1?.status, 'completed');
    const asset1Id = job1.assetIds?.[0];
    assert.ok(asset1Id, 'Expected asset1Id from Turn 1 image job');

    const asset1Res = await fetch(`${baseUrl}/api/v1/assets/${asset1Id}`, {
      headers: { cookie: jar.headerValue() },
    });
    assert.equal(asset1Res.status, 200);
    const asset1 = await asset1Res.json();
    assert.equal(asset1.status, 'ready');
    assert.ok(asset1.downloadUrl);
    const img1BytesRes = await fetch(asset1.downloadUrl);
    assert.equal(img1BytesRes.status, 200);
    assert.ok((await img1BytesRes.arrayBuffer()).byteLength > 100);

    // 8. Turn 2 with imageAssetIds: [asset1Id] -> get_image_job + inspect_image_asset + submit_image_job -> asset2Id ready
    const turn2ReqId = `e2e-turn2-${Date.now()}`;
    const turn2SendRes = await fetch(`${baseUrl}/api/v1/conversations/${conv.id}/messages`, {
      method: 'POST',
      headers: { origin: baseUrl, cookie: jar.headerValue(), 'content-type': 'application/json' },
      body: JSON.stringify({
        requestId: turn2ReqId,
        text: `请先调用 get_image_job 查询上一轮生图任务 ${turn1JobId} 的状态，检查资产 ${asset1Id}，再调用 submit_image_job 引用资产 ${asset1Id}（doubao-seedream-4.5）生成 1 张暖色调变体图并总结结果。`,
        imageAssetIds: [asset1Id],
      }),
    });
    assert.equal(turn2SendRes.status, 202);

    await sse.waitFor(
      (e) =>
        e.event === 'tool' &&
        e.data?.requestId === turn2ReqId &&
        e.data?.tool === 'get_image_job' &&
        e.data?.job?.jobId === turn1JobId,
      20_000,
      'Turn 2 get_image_job tool event',
    );

    const turn2Submit = await sse.waitFor(
      (e) =>
        e.event === 'tool' &&
        e.data?.requestId === turn2ReqId &&
        e.data?.tool === 'submit_image_job' &&
        e.data?.job?.jobId !== turn1JobId,
      20_000,
      'Turn 2 submit_image_job tool event',
    );
    const turn2JobId = turn2Submit.data.job.jobId;
    assert.ok(turn2JobId && turn2JobId !== turn1JobId);

    await sse.waitFor(
      (e) =>
        e.event === 'state' &&
        e.data?.receipt?.requestId === turn2ReqId &&
        e.data?.receipt?.status === 'completed',
      20_000,
      'Turn 2 state completed',
    );

    const hist2Res = await fetch(
      `${baseUrl}/api/v1/conversations/${conv.id}/history?afterSeq=${afterTurn1Seq}&limit=100`,
      { headers: { cookie: jar.headerValue() } },
    );
    assert.equal(hist2Res.status, 200);
    const hist2 = await hist2Res.json();
    const turn2User = hist2.items.find((i) => i.role === 'user');
    assert.deepEqual(turn2User?.imageAssetIds, [asset1Id]);
    const turn2Assistant = hist2.items.find((i) => i.role === 'assistant');
    assert.match(turn2Assistant?.text || '', /附图=true/);

    let job2 = null;
    for (let i = 0; i < 30; i++) {
      const res = await fetch(`${baseUrl}/api/v1/compute/images/${turn2JobId}`, {
        headers: { cookie: jar.headerValue() },
      });
      assert.equal(res.status, 200);
      job2 = await res.json();
      if (job2.status === 'completed') break;
      await delay(500);
    }
    assert.equal(job2?.status, 'completed');
    const asset2Id = job2.assetIds?.[0];
    assert.ok(asset2Id && asset2Id !== asset1Id, 'Expected distinct asset2Id from Turn 2');

    const asset2Res = await fetch(`${baseUrl}/api/v1/assets/${asset2Id}`, {
      headers: { cookie: jar.headerValue() },
    });
    assert.equal(asset2Res.status, 200);
    const asset2 = await asset2Res.json();
    assert.equal(asset2.status, 'ready');
    assert.ok(asset2.downloadUrl);

    sse.close();
    sse = null;

    // 9. Clean shutdown leaving zero newly created apifox_sbx_% schemas
    const [exitCode] = await stopChild();
    assert.equal(exitCode, 0, `server.mjs should exit cleanly with 0; stderr=${stderr}`);

    const schemasAfter = await listSandboxSchemas();
    const leaked = schemasAfter.filter((s) => !schemasBefore.has(s));
    assert.equal(
      leaked.length,
      0,
      `Expected zero leftover apifox_sbx_% schemas from this test run, found: ${JSON.stringify(leaked)}`,
    );
  } finally {
    sse?.close();
    await stopChild();
  }
});

function createMockDomEnv() {
  const byId = new Map();

  class MockElement {
    constructor(tagName, id = null) {
      this.tagName = String(tagName).toUpperCase();
      this.nodeType = this.tagName === '#DOCUMENT-FRAGMENT' ? 11 : 1;
      this.id = id;
      this._classSet = new Set();
      this._className = '';
      this.textContent = '';
      this.value = '';
      this.disabled = false;
      this.type = '';
      this.href = '';
      this.target = '';
      this.rel = '';
      this.title = '';
      this.src = '';
      this.async = false;
      this.alt = '';
      this.dataset = {};
      this.style = {};
      this.attributes = new Map();
      this.listeners = new Map();
      this.children = [];
      this.parentNode = null;
      this.contentWindow = null;
      this.offsetWidth = 0;
      this.offsetHeight = 0;
      this.scrollHeight = 0;
      this.scrollTop = 0;
      this.clientHeight = 400;
      this.focused = false;
      this.classList = {
        add: (...names) => {
          for (const n of names) {
            if (n) this._classSet.add(n);
          }
          this._className = [...this._classSet].join(' ');
        },
        remove: (...names) => {
          for (const n of names) {
            this._classSet.delete(n);
          }
          this._className = [...this._classSet].join(' ');
        },
        toggle: (name, force) => {
          const shouldAdd = force === undefined ? !this._classSet.has(name) : Boolean(force);
          if (shouldAdd) this._classSet.add(name);
          else this._classSet.delete(name);
          this._className = [...this._classSet].join(' ');
          return shouldAdd;
        },
        contains: (name) => this._classSet.has(name),
      };
    }

    get className() {
      return this._className;
    }

    set className(val) {
      this._className = String(val || '');
      this._classSet = new Set(this._className.split(/\s+/).filter(Boolean));
    }

    setAttribute(k, v) {
      this.attributes.set(k, String(v));
    }

    getAttribute(k) {
      return this.attributes.get(k) ?? null;
    }

    addEventListener(type, handler) {
      if (!this.listeners.has(type)) this.listeners.set(type, []);
      this.listeners.get(type).push(handler);
    }

    removeEventListener(type, handler) {
      const list = this.listeners.get(type);
      if (!list) return;
      const idx = list.indexOf(handler);
      if (idx >= 0) list.splice(idx, 1);
    }

    async dispatch(type, event = {}) {
      const handlers = [...(this.listeners.get(type) || [])];
      for (const h of handlers) {
        await h({ type, target: this, stopPropagation() {}, ...event });
      }
    }

    async click() {
      await this.dispatch('click');
    }

    focus() {
      this.focused = true;
    }

    append(...nodes) {
      for (const n of nodes) {
        if (!n) continue;
        if (n.nodeType === 11) {
          for (const child of n.children) {
            child.parentNode = this;
            this.children.push(child);
          }
          n.children = [];
        } else {
          n.parentNode = this;
          this.children.push(n);
        }
      }
    }

    appendChild(node) {
      this.append(node);
      return node;
    }

    remove() {
      if (!this.parentNode) return;
      const idx = this.parentNode.children.indexOf(this);
      if (idx >= 0) this.parentNode.children.splice(idx, 1);
      this.parentNode = null;
    }

    replaceChildren(...nodes) {
      for (const c of this.children) {
        c.parentNode = null;
      }
      this.children = [];
      this.append(...nodes);
    }

    querySelector(selector) {
      const tagMatch = /^[a-z]+$/i.exec(selector);
      if (tagMatch) {
        const expectedTag = selector.toUpperCase();
        return findAll(this, (n) => n !== this && n.tagName === expectedTag)[0] ?? null;
      }
      const scriptMatch = /^script\[src="([^"]+)"\]$/i.exec(selector);
      if (scriptMatch) {
        const expectedSrc = scriptMatch[1];
        return (
          findAll(this, (n) => n !== this && n.tagName === 'SCRIPT' && n.src === expectedSrc)[0] ??
          null
        );
      }
      return null;
    }
  }

  function findAll(root, predicate, out = []) {
    if (!root) return out;
    if (predicate(root)) out.push(root);
    for (const child of root.children || []) {
      findAll(child, predicate, out);
    }
    return out;
  }

  function collectText(node) {
    if (!node) return '';
    const parts = [];
    if (node.textContent) parts.push(node.textContent);
    for (const child of node.children || []) {
      const t = collectText(child);
      if (t) parts.push(t);
    }
    return parts.join(' ');
  }

  const head = new MockElement('head');

  const documentMock = {
    head,
    getElementById(id) {
      if (!byId.has(id)) {
        byId.set(id, new MockElement('div', id));
      }
      return byId.get(id);
    },
    createElement(tag) {
      return new MockElement(tag);
    },
    createDocumentFragment() {
      return new MockElement('#DOCUMENT-FRAGMENT');
    },
    querySelector(selector) {
      const inHead = head.querySelector(selector);
      if (inHead) return inHead;
      for (const node of byId.values()) {
        const found = node.querySelector(selector);
        if (found) return found;
      }
      return null;
    },
  };

  const windowListeners = new Map();
  const windowMock = {
    location: { origin: 'http://127.0.0.1:4320' },
    addEventListener(type, fn) {
      if (!windowListeners.has(type)) windowListeners.set(type, []);
      windowListeners.get(type).push(fn);
    },
    removeEventListener(type, fn) {
      const list = windowListeners.get(type);
      if (!list) return;
      const idx = list.indexOf(fn);
      if (idx >= 0) list.splice(idx, 1);
    },
    dispatchMessage(event) {
      const handlers = [...(windowListeners.get('message') || [])];
      for (const fn of handlers) {
        fn(event);
      }
    },
  };

  class MockMutationObserver {
    static instances = [];

    constructor(callback) {
      this.callback = callback;
      this.target = null;
      this.options = null;
      this.disconnected = false;
      MockMutationObserver.instances.push(this);
    }

    observe(target, options) {
      this.target = target;
      this.options = options;
      this.disconnected = false;
    }

    disconnect() {
      this.disconnected = true;
    }

    trigger(records = []) {
      if (!this.disconnected) {
        this.callback(records, this);
      }
    }
  }

  class MockEventSource {
    static CLOSED = 2;
    static instances = [];

    constructor(url) {
      this.url = url;
      this.readyState = 1;
      this.closed = false;
      this.listeners = new Map();
      this.onopen = null;
      this.onerror = null;
      MockEventSource.instances.push(this);
    }

    addEventListener(type, fn) {
      if (!this.listeners.has(type)) this.listeners.set(type, []);
      this.listeners.get(type).push(fn);
    }

    emit(type, payload) {
      const handlers = this.listeners.get(type) || [];
      const ev = { type, data: typeof payload === 'string' ? payload : JSON.stringify(payload) };
      for (const fn of handlers) fn(ev);
    }

    close() {
      this.closed = true;
      this.readyState = MockEventSource.CLOSED;
    }
  }

  return {
    documentMock,
    windowMock,
    MockEventSource,
    MockMutationObserver,
    findAll,
    collectText,
  };
}

describe('public/app.js tool card deduplication and follow-up helpers', () => {
  let app = null;
  let env = null;
  let realFetch = null;
  let prevGlobals = null;

  const ASSET_1 = '11111111-1111-4111-8111-111111111111';
  const ASSET_2 = '22222222-2222-4222-8222-222222222222';
  const ASSET_RECOVERED = '33333333-3333-4333-8333-333333333333';

  function resetAppForTest() {
    app.stopWechatQrPolling({ resetDiag: true });
    app.state.authTab = 'signin';
    app.state.wechatQr.pollIntervalMs = 10;
    app.closeConversationResources();
    app.clearAttachmentsAndAssetCache();
    app.el.authErrorText.classList.add('hidden');
    app.el.authErrorText.textContent = '';
    app.state.me = { userId: 'user-1', sessionId: 'sess-1' };
    app.state.activeConversationId = 'conv-1';
    app.state.conversations = [
      { id: 'conv-1', modelRef: 'onelink:qwen3.5-plus', createdAt: new Date().toISOString() },
    ];
    app.state.models = [
      {
        modelRef: 'onelink:qwen3.5-plus',
        modelId: 'qwen3.5-plus',
        runnable: true,
        adaptation: { metadata: { inputModalities: ['text', 'image'] } },
      },
    ];
    app.state.selectedChatModelRef = 'onelink:qwen3.5-plus';
    app.el.composerInput.value = '';
    app.el.timeline.replaceChildren();
    env.documentMock.head.replaceChildren();
    env.MockEventSource.instances = [];
    env.MockMutationObserver.instances = [];
    delete globalThis.WxLogin;
    delete globalThis.window.WxLogin;

    for (const id of [ASSET_1, ASSET_2, ASSET_RECOVERED]) {
      app.state.assetCache.set(id, {
        asset: {
          assetId: id,
          status: 'ready',
          filename: `${id.slice(0, 8)}.png`,
          downloadUrl: `https://example.test/assets/${id}.png`,
        },
        fetchedAt: Date.now(),
        expiresAt: Date.now() + 120_000,
      });
    }
  }

  test.before(async () => {
    env = createMockDomEnv();
    realFetch = globalThis.fetch;
    prevGlobals = {
      document: globalThis.document,
      window: globalThis.window,
      EventSource: globalThis.EventSource,
      MutationObserver: globalThis.MutationObserver,
      WxLogin: globalThis.WxLogin,
      skipBoot: globalThis.__MIO_DEMO_SKIP_BOOT__,
    };
    globalThis.__MIO_DEMO_SKIP_BOOT__ = true;
    globalThis.document = env.documentMock;
    globalThis.window = env.windowMock;
    globalThis.EventSource = env.MockEventSource;
    globalThis.MutationObserver = env.MockMutationObserver;
    app = await import('../public/app.js');
  });

  test.after(() => {
    app?.stopWechatQrPolling({ resetDiag: true });
    app?.closeConversationResources();
    globalThis.fetch = realFetch;
    if (prevGlobals.document === undefined) delete globalThis.document;
    else globalThis.document = prevGlobals.document;
    if (prevGlobals.window === undefined) delete globalThis.window;
    else globalThis.window = prevGlobals.window;
    if (prevGlobals.EventSource === undefined) delete globalThis.EventSource;
    else globalThis.EventSource = prevGlobals.EventSource;
    if (prevGlobals.MutationObserver === undefined) delete globalThis.MutationObserver;
    else globalThis.MutationObserver = prevGlobals.MutationObserver;
    if (prevGlobals.WxLogin === undefined) delete globalThis.WxLogin;
    else globalThis.WxLogin = prevGlobals.WxLogin;
    if (prevGlobals.skipBoot === undefined) delete globalThis.__MIO_DEMO_SKIP_BOOT__;
    else globalThis.__MIO_DEMO_SKIP_BOOT__ = prevGlobals.skipBoot;
  });

  test('same-turn SSE tool events with same jobId merge in-place into 1 card', () => {
    resetAppForTest();
    globalThis.fetch = async () => new Response('{}', { status: 200 });
    try {
      app.connectConversationSse('conv-1');
      const es = env.MockEventSource.instances.at(-1);
      assert.ok(es, 'MockEventSource should be created');

      es.emit('tool', {
        requestId: 'req-sse-1',
        tool: 'submit_image_job',
        job: { jobId: 'job-sse-1', status: 'accepted', requestedCount: 1 },
      });

      let cards = env.findAll(app.el.timeline, (n) => n.classList?.contains('tool-card'));
      assert.equal(cards.length, 1);
      assert.equal(cards[0].dataset.jobId, 'job-sse-1');
      assert.match(env.collectText(cards[0]), /提交生图任务/);
      assert.match(env.collectText(cards[0]), /已受理 \(accepted\)/);

      // Second distinct job arrives before get_image_job updates job-sse-1
      es.emit('tool', {
        requestId: 'req-sse-1',
        tool: 'submit_image_job',
        job: { jobId: 'job-sse-2', status: 'submitted', requestedCount: 1 },
      });

      // Same-turn get_image_job updates job-sse-1 in-place at index 0
      es.emit('tool', {
        requestId: 'req-sse-1',
        tool: 'get_image_job',
        job: {
          jobId: 'job-sse-1',
          status: 'completed',
          requestedCount: 1,
          assetIds: [ASSET_1],
          outputs: [{ assetId: ASSET_1 }],
        },
      });

      assert.equal(app.state.activeTurn.liveTools.length, 2);
      assert.equal(app.state.activeTurn.liveTools[0].job.jobId, 'job-sse-1');
      assert.equal(app.state.activeTurn.liveTools[0].tool, 'submit_image_job');
      assert.equal(app.state.activeTurn.liveTools[1].job.jobId, 'job-sse-2');

      cards = env.findAll(app.el.timeline, (n) => n.classList?.contains('tool-card'));
      assert.equal(cards.length, 2, 'Should render exactly 1 card per unique jobId');
      assert.equal(cards[0].dataset.jobId, 'job-sse-1');
      assert.equal(cards[1].dataset.jobId, 'job-sse-2');
      const card1Text = env.collectText(cards[0]);
      assert.match(card1Text, /提交生图任务/);
      assert.match(card1Text, /已完成 \(completed\)/);
      assert.doesNotMatch(card1Text, /自动轮询进度中/);
      assert.match(card1Text, new RegExp(ASSET_1));

      // Also verify same-turn SSE submit_image_job -> get_image_job rejected leaves 1 red card
      es.emit('tool', {
        requestId: 'req-sse-1',
        tool: 'get_image_job',
        job: { jobId: 'job-sse-2', status: 'rejected', errorCode: 'SAFETY_BLOCK' },
      });
      cards = env.findAll(app.el.timeline, (n) => n.classList?.contains('tool-card'));
      assert.equal(cards.length, 2);
      const card2Text = env.collectText(cards[1]);
      assert.match(card2Text, /提交生图任务/);
      assert.match(card2Text, /已拒绝 \(rejected\)/);
      assert.match(card2Text, /错误码: SAFETY_BLOCK/);
    } finally {
      app.closeConversationResources();
      globalThis.fetch = realFetch;
    }
  });

  test('historyItems merges submit_image_job and get_image_job for same jobId (completed & rejected)', () => {
    resetAppForTest();
    globalThis.fetch = async () => new Response('{}', { status: 200 });
    try {
      app.state.historyItems = [
        { seq: 1, role: 'user', text: 'draw 1' },
        {
          seq: 2,
          role: 'tool',
          tool: 'submit_image_job',
          job: { jobId: 'job-hist-ok', status: 'accepted', requestedCount: 1 },
        },
        {
          seq: 3,
          role: 'tool',
          tool: 'get_image_job',
          job: {
            jobId: 'job-hist-ok',
            status: 'completed',
            requestedCount: 1,
            assetIds: [ASSET_1],
            outputs: [{ assetId: ASSET_1 }],
          },
        },
        { seq: 4, role: 'assistant', text: 'done 1' },
        { seq: 5, role: 'user', text: 'draw 2' },
        {
          seq: 6,
          role: 'tool',
          tool: 'submit_image_job',
          job: { jobId: 'job-hist-rej', status: 'accepted', requestedCount: 1 },
        },
        {
          seq: 7,
          role: 'tool',
          tool: 'get_image_job',
          job: {
            jobId: 'job-hist-rej',
            status: 'rejected',
            requestedCount: 1,
            errorCode: 'POLICY_VIOLATION',
          },
        },
        { seq: 8, role: 'assistant', text: 'failed 2' },
      ];

      app.renderTimeline();

      const cards = env.findAll(app.el.timeline, (n) => n.classList?.contains('tool-card'));
      assert.equal(cards.length, 2, 'Expected 1 merged card for each distinct jobId');

      assert.equal(cards[0].dataset.jobId, 'job-hist-ok');
      const okText = env.collectText(cards[0]);
      assert.match(okText, /提交生图任务/);
      assert.match(okText, /已完成 \(completed\)/);
      assert.match(okText, new RegExp(ASSET_1));

      assert.equal(cards[1].dataset.jobId, 'job-hist-rej');
      const rejText = env.collectText(cards[1]);
      assert.match(rejText, /提交生图任务/);
      assert.match(rejText, /已拒绝 \(rejected\)/);
      assert.match(rejText, /错误码: POLICY_VIOLATION/);

      // Order in timeline: user(1), tool(job-hist-ok), assistant(1), user(2), tool(job-hist-rej), assistant(2)
      assert.equal(app.el.timeline.children.length, 6);
      assert.ok(app.el.timeline.children[1].children.some((c) => c.dataset?.jobId === 'job-hist-ok'));
      assert.ok(app.el.timeline.children[4].children.some((c) => c.dataset?.jobId === 'job-hist-rej'));
    } finally {
      app.closeConversationResources();
      globalThis.fetch = realFetch;
    }
  });

  test('cross-turn historyItems + activeTurn.liveTools deduplicate by jobId and follow-up helpers work', async () => {
    resetAppForTest();
    globalThis.fetch = async (url, init) => {
      if (String(url).endsWith('/api/v1/compute/images/job-mat-fail/recover') && init?.method === 'POST') {
        return new Response(
          JSON.stringify({
            jobId: 'job-mat-fail',
            status: 'completed',
            requestedCount: 1,
            assetIds: [ASSET_RECOVERED],
            outputs: [{ assetId: ASSET_RECOVERED }],
          }),
          { status: 200, headers: { 'content-type': 'application/json' } },
        );
      }
      return new Response('{}', { status: 200 });
    };

    try {
      app.state.historyItems = [
        { seq: 1, role: 'user', text: 'turn 1' },
        {
          seq: 2,
          role: 'tool',
          tool: 'submit_image_job',
          job: { jobId: 'job-turn1', status: 'accepted', requestedCount: 1 },
        },
        { seq: 3, role: 'assistant', text: 'turn 1 done' },
        { seq: 4, role: 'user', text: 'turn 2' },
        {
          seq: 5,
          role: 'tool',
          tool: 'submit_image_job',
          job: { jobId: 'job-turn2', status: 'completed', assetIds: [ASSET_2], outputs: [{ assetId: ASSET_2 }] },
        },
      ];
      app.state.jobs.set('job-turn1', {
        jobId: 'job-turn1',
        status: 'completed',
        requestedCount: 1,
        assetIds: [ASSET_1],
        outputs: [{ assetId: ASSET_1 }],
      });
      app.state.activeTurn = {
        requestId: 'req-turn3',
        status: 'processing',
        pendingUserText: 'turn 3',
        pendingUserImageAssetIds: [],
        streamText: '',
        liveTools: [
          {
            tool: 'get_image_job',
            job: {
              jobId: 'job-turn1',
              status: 'completed',
              assetIds: [ASSET_1],
              outputs: [{ assetId: ASSET_1 }],
            },
          },
          {
            tool: 'submit_image_job',
            job: {
              jobId: 'job-mat-fail',
              status: 'accepted',
              requestedCount: 1,
            },
          },
          {
            tool: 'get_image_job',
            job: {
              jobId: 'job-mat-fail',
              status: 'materialization_failed',
              errorCode: 'MAT_TIMEOUT',
            },
          },
        ],
      };

      app.renderTimeline();

      let cards = env.findAll(app.el.timeline, (n) => n.classList?.contains('tool-card'));
      assert.equal(cards.length, 3, 'job-turn1 must not duplicate across historyItems and liveTools');
      assert.deepEqual(
        cards.map((c) => c.dataset.jobId),
        ['job-turn1', 'job-turn2', 'job-mat-fail'],
      );

      // findLatestCompletedImageInConversation should pick job-turn2 (newest created completed job),
      // even though Turn 3 called get_image_job on older job-turn1.
      const latest = app.findLatestCompletedImageInConversation();
      assert.deepEqual(latest, { jobId: 'job-turn2', assetId: ASSET_2 });

      // buildFollowUpPrompt must target doubao-seedream-4.5 without output_format
      const visionPrompt = app.buildFollowUpPrompt(latest.jobId, latest.assetId, true);
      const nonVisionPrompt = app.buildFollowUpPrompt(latest.jobId, latest.assetId, false);
      assert.match(visionPrompt, /doubao-seedream-4\.5/);
      assert.match(nonVisionPrompt, /doubao-seedream-4\.5/);
      assert.doesNotMatch(visionPrompt, /output_format/i);
      assert.doesNotMatch(nonVisionPrompt, /output_format/i);

      // Follow-up button on job-turn2 card attaches ASSET_2 and fills prompt
      const card2Buttons = env.findAll(cards[1], (n) => n.tagName === 'BUTTON');
      const followUpBtn = card2Buttons.find((b) => b.textContent.includes('带此图追问'));
      assert.ok(followUpBtn, 'Completed card should expose follow-up button');
      await followUpBtn.click();
      assert.equal(app.state.attachments.length, 1);
      assert.equal(app.state.attachments[0].assetId, ASSET_2);
      assert.match(app.el.composerInput.value, new RegExp(ASSET_2));
      assert.doesNotMatch(app.el.composerInput.value, /output_format/i);

      // Recovery button on merged materialization_failed card updates card in-place and clears errorCode
      const card3Buttons = env.findAll(cards[2], (n) => n.tagName === 'BUTTON');
      const recoverBtn = card3Buttons.find((b) => b.textContent === '重试物化恢复');
      assert.ok(recoverBtn, 'materialization_failed card should expose recover button');
      await recoverBtn.click();

      cards = env.findAll(app.el.timeline, (n) => n.classList?.contains('tool-card'));
      assert.equal(cards.length, 3);
      const recoveredText = env.collectText(cards[2]);
      assert.match(recoveredText, /已完成 \(completed\)/);
      assert.doesNotMatch(recoveredText, /MAT_TIMEOUT/);
      assert.match(recoveredText, new RegExp(ASSET_RECOVERED));

      // Cross-boundary merge without pre-populated state.jobs: historyItems(accepted) + liveTools(completed)
      resetAppForTest();
      app.state.historyItems = [
        { seq: 1, role: 'user', text: 'turn 1' },
        {
          seq: 2,
          role: 'tool',
          tool: 'submit_image_job',
          job: { jobId: 'job-boundary', status: 'accepted', requestedCount: 1 },
        },
      ];
      app.state.activeTurn = {
        requestId: 'req-boundary',
        status: 'processing',
        pendingUserText: null,
        pendingUserImageAssetIds: null,
        streamText: '',
        liveTools: [
          {
            tool: 'get_image_job',
            job: {
              jobId: 'job-boundary',
              status: 'completed',
              assetIds: [ASSET_1],
              outputs: [{ assetId: ASSET_1 }],
            },
          },
        ],
      };
      app.renderTimeline();
      cards = env.findAll(app.el.timeline, (n) => n.classList?.contains('tool-card'));
      assert.equal(cards.length, 1);
      assert.equal(cards[0].dataset.jobId, 'job-boundary');
      assert.match(env.collectText(cards[0]), /提交生图任务/);
      assert.match(env.collectText(cards[0]), /已完成 \(completed\)/);
      assert.match(env.collectText(cards[0]), new RegExp(ASSET_1));

      // Also when historyItems ends with a get_image_job re-querying an older job,
      // findLatestCompletedImageInConversation still returns the newest created job (job-turn2).
      resetAppForTest();
      app.state.historyItems = [
        {
          seq: 1,
          role: 'tool',
          tool: 'submit_image_job',
          job: { jobId: 'job-turn1', status: 'completed', assetIds: [ASSET_1], outputs: [{ assetId: ASSET_1 }] },
        },
        {
          seq: 2,
          role: 'tool',
          tool: 'submit_image_job',
          job: { jobId: 'job-turn2', status: 'completed', assetIds: [ASSET_2], outputs: [{ assetId: ASSET_2 }] },
        },
        {
          seq: 3,
          role: 'tool',
          tool: 'get_image_job',
          job: { jobId: 'job-turn1', status: 'completed', assetIds: [ASSET_1], outputs: [{ assetId: ASSET_1 }] },
        },
      ];
      assert.deepEqual(app.findLatestCompletedImageInConversation(), {
        jobId: 'job-turn2',
        assetId: ASSET_2,
      });
    } finally {
      app.closeConversationResources();
      globalThis.fetch = realFetch;
    }
  });

  test('WeChat QR inline flow: WxLogin({ self_redirect: true }) with zero window.open -> scales iframe -> pending -> transient 401 -> confirmed -> /api/v1/me -> manual re-poll consumed', async () => {
    resetAppForTest();
    app.bindEvents();
    app.state.me = null;
    app.state.sessionUser = null;
    app.state.activeConversationId = null;
    app.state.conversations = [];

    let windowOpenCalls = 0;
    const prevOpen = globalThis.window.open;
    globalThis.window.open = () => {
      windowOpenCalls += 1;
      throw new Error('window.open must never be called for embedded WeChat QR login');
    };

    const wxLoginCalls = [];
    const iframeWindow = { label: 'wechat-iframe-win' };
    globalThis.WxLogin = function MockWxLogin(opts) {
      wxLoginCalls.push(opts);
      const container = document.getElementById(opts.id);
      const iframe = document.createElement('iframe');
      iframe.src = `https://open.weixin.qq.com/connect/qrconnect?appid=${opts.appid}`;
      iframe.contentWindow = iframeWindow;
      container.replaceChildren(iframe);
    };

    let pollStep = 0;
    const qrId = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
    const redirectUri = 'https://mio-qr.example.com/api/auth/callback/wechat';
    const authUrl = `https://open.weixin.qq.com/connect/qrconnect?appid=wx-real-test&redirect_uri=${encodeURIComponent(redirectUri)}&response_type=code&scope=snsapi_login&state=st1#wechat_redirect`;

    globalThis.fetch = async (url, init = {}) => {
      const u = String(url);
      if (u === '/api/auth/wechat/qr-session' && init.method === 'POST') {
        const parsed = JSON.parse(init.body);
        assert.deepEqual(parsed, { callbackURL: '/auth/wechat/qr-complete' });
        return new Response(
          JSON.stringify({
            qrcode_id: qrId,
            authorization_url: authUrl,
            expire_seconds: 300,
          }),
          { status: 201, headers: { 'content-type': 'application/json' } },
        );
      }
      if (u === `/api/auth/wechat/qr-session/${qrId}/poll`) {
        pollStep += 1;
        if (pollStep === 1) {
          return new Response(JSON.stringify({ status: 'pending' }), {
            status: 200,
            headers: { 'content-type': 'application/json' },
          });
        }
        if (pollStep === 2) {
          return new Response(
            JSON.stringify({ error: { code: 'UNAUTHORIZED', message: 'Session required' } }),
            { status: 401, headers: { 'content-type': 'application/json' } },
          );
        }
        if (pollStep === 3) {
          return new Response(JSON.stringify({ status: 'confirmed' }), {
            status: 200,
            headers: { 'content-type': 'application/json' },
          });
        }
        return new Response(JSON.stringify({ status: 'consumed' }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        });
      }
      if (u === '/api/v1/me') {
        return new Response(
          JSON.stringify({ userId: 'wx-user-101', sessionId: 'sess-wx-101' }),
          { status: 200, headers: { 'content-type': 'application/json' } },
        );
      }
      if (u === '/api/auth/get-session') {
        return new Response(
          JSON.stringify({ user: { id: 'wx-user-101', email: 'wx-101@auth.mio.invalid' } }),
          { status: 200, headers: { 'content-type': 'application/json' } },
        );
      }
      if (u === '/api/v1/providers/onelink') {
        return new Response(JSON.stringify({ hasKey: false, revision: 0 }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        });
      }
      if (u === '/api/v1/providers/onelink/models') {
        return new Response(JSON.stringify({ provider: null, items: [] }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        });
      }
      if (u.startsWith('/api/v1/conversations')) {
        return new Response(JSON.stringify({ items: [] }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        });
      }
      return new Response('{}', { status: 200, headers: { 'content-type': 'application/json' } });
    };

    try {
      await app.el.tabWechatQrBtn.click();

      // Zero window.open calls; auth form hidden and QR panel shown
      assert.equal(windowOpenCalls, 0);
      assert.equal(app.state.authTab, 'wechat');
      assert.equal(app.el.authForm.classList.contains('hidden'), true);
      assert.equal(app.el.wechatQrPanel.classList.contains('hidden'), false);
      assert.equal(app.el.tabWechatQrBtn.getAttribute('aria-selected'), 'true');

      // WxLogin invoked with parsed params and self_redirect: true
      assert.equal(wxLoginCalls.length, 1);
      assert.equal(wxLoginCalls[0].self_redirect, true);
      assert.equal(wxLoginCalls[0].id, 'wechatQrEmbedContainer');
      assert.equal(wxLoginCalls[0].appid, 'wx-real-test');
      assert.equal(wxLoginCalls[0].redirect_uri, redirectUri);
      assert.equal(wxLoginCalls[0].state, 'st1');
      assert.equal(wxLoginCalls[0].scope, 'snsapi_login');
      assert.equal(wxLoginCalls[0].style, 'black');
      assert.match(wxLoginCalls[0].href, /^data:text\/css;charset=utf-8,/);
      const decodedCss = decodeURIComponent(
        wxLoginCalls[0].href.replace(/^data:text\/css;charset=utf-8,/, ''),
      );
      assert.match(decodedCss, /\.impowerBox \.title \{ display: none; \}/);

      // Inserted iframe is scaled from 300x400 to default 248x248 (scale=0.62 > 0)
      const iframe = app.el.wechatQrEmbedContainer.querySelector('iframe');
      assert.ok(iframe, 'WxLogin should mount an iframe inside #wechatQrEmbedContainer');
      assert.equal(iframe.style.width, '300px');
      assert.equal(iframe.style.height, '400px');
      assert.equal(iframe.style.transform, 'translate(-50%, -50%) scale(0.62)');
      assert.equal(app.el.wechatQrLoadingOverlay.classList.contains('hidden'), true);
      assert.equal(app.el.wechatQrRefreshBtn.classList.contains('hidden'), true);

      // Re-clicking active 微信扫码 tab while polling an unexpired session does NOT recreate WxLogin
      await app.el.tabWechatQrBtn.click();
      assert.equal(wxLoginCalls.length, 1);

      // Wait for pending -> transient 401 -> confirmed -> /api/v1/me
      const deadline = Date.now() + 3000;
      while ((app.state.wechatQr.polling || !app.state.me) && Date.now() < deadline) {
        await delay(15);
      }

      assert.equal(windowOpenCalls, 0);
      assert.equal(app.state.wechatQr.polling, false);
      assert.equal(app.state.me?.userId, 'wx-user-101');
      assert.equal(app.el.authBadge.textContent, '已登录');
      assert.equal(app.el.authSignedInBox.classList.contains('hidden'), false);
      assert.equal(app.el.wechatQrEmbedContainer.children.length, 0);
      assert.equal(app.el.wechatQrDiagBox.classList.contains('hidden'), false);
      assert.equal(app.el.wechatQrDiagId.textContent, qrId);
      assert.equal(app.el.wechatQrDiagUserId.textContent, 'wx-user-101');
      assert.equal(app.el.wechatQrDiagPolls.textContent, '3');
      assert.equal(
        app.el.wechatQrDiagTrail.textContent,
        'created(201) -> pending -> 401 -> confirmed',
      );
      assert.equal(app.el.wechatQrManualPollBtn.disabled, false);

      // Manual re-poll after login observes `consumed` without resetting login state
      await app.el.wechatQrManualPollBtn.click();
      assert.equal(app.state.me?.userId, 'wx-user-101');
      assert.equal(app.el.authBadge.textContent, '已登录');
      assert.equal(app.el.wechatQrDiagPolls.textContent, '4');
      assert.equal(
        app.el.wechatQrDiagTrail.textContent,
        'created(201) -> pending -> 401 -> confirmed -> consumed',
      );
    } finally {
      app.stopWechatQrPolling({ resetDiag: true });
      app.closeConversationResources();
      globalThis.window.open = prevOpen;
      globalThis.fetch = realFetch;
    }
  });

  test('WeChat QR inline flow: same-origin iframe callback message wakes /poll without bypassing /poll + /api/v1/me; foreign/invalid messages ignored', async () => {
    resetAppForTest();
    app.bindEvents();
    app.state.me = null;
    app.state.sessionUser = null;
    // Use long 5s interval so wake-up via postMessage (~20ms) is unambiguously proven
    app.state.wechatQr.pollIntervalMs = 5000;

    const iframeWin = { name: 'qr-iframe' };
    const otherWin = { name: 'other-win' };
    globalThis.WxLogin = function MockWxLogin(opts) {
      const container = document.getElementById(opts.id);
      const iframe = document.createElement('iframe');
      iframe.contentWindow = iframeWin;
      container.replaceChildren(iframe);
    };

    const qrId = 'dddddddd-dddd-4ddd-8ddd-dddddddddddd';
    const authUrl = `https://open.weixin.qq.com/connect/qrconnect?appid=wx-wake&redirect_uri=${encodeURIComponent('https://mio-qr.example.com/api/auth/callback/wechat')}&state=st-wake`;
    let pollCalls = 0;

    globalThis.fetch = async (url) => {
      const u = String(url);
      if (u === '/api/auth/wechat/qr-session') {
        return new Response(
          JSON.stringify({ qrcode_id: qrId, authorization_url: authUrl, expire_seconds: 300 }),
          { status: 201, headers: { 'content-type': 'application/json' } },
        );
      }
      if (u === `/api/auth/wechat/qr-session/${qrId}/poll`) {
        pollCalls += 1;
        return new Response(JSON.stringify({ status: 'confirmed' }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        });
      }
      if (u === '/api/v1/me') {
        return new Response(
          JSON.stringify({ userId: 'wx-wake-user', sessionId: 'sess-wake-1' }),
          { status: 200, headers: { 'content-type': 'application/json' } },
        );
      }
      if (u === '/api/auth/get-session') {
        return new Response(JSON.stringify({ user: { id: 'wx-wake-user' } }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        });
      }
      if (u === '/api/v1/providers/onelink') {
        return new Response(JSON.stringify({ hasKey: false, revision: 0 }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        });
      }
      if (u === '/api/v1/providers/onelink/models') {
        return new Response(JSON.stringify({ provider: null, items: [] }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        });
      }
      if (u.startsWith('/api/v1/conversations')) {
        return new Response(JSON.stringify({ items: [] }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        });
      }
      return new Response('{}', { status: 200, headers: { 'content-type': 'application/json' } });
    };

    try {
      await app.el.tabWechatQrBtn.click();
      assert.equal(app.state.wechatQr.polling, true);
      assert.equal(pollCalls, 0);

      // 1. Cross-origin message must be ignored
      env.windowMock.dispatchMessage({
        origin: 'https://evil.example.com',
        source: iframeWin,
        data: { type: 'mio-wechat-qr-callback-landed', error: null },
      });
      // 2. Wrong type message must be ignored
      env.windowMock.dispatchMessage({
        origin: 'http://127.0.0.1:4320',
        source: iframeWin,
        data: { type: 'wrong-type', error: null },
      });
      // 3. Wrong source window must be ignored
      env.windowMock.dispatchMessage({
        origin: 'http://127.0.0.1:4320',
        source: otherWin,
        data: { type: 'mio-wechat-qr-callback-landed', error: null },
      });

      await delay(60);
      assert.equal(pollCalls, 0, 'Ignored messages must not wake /poll');
      assert.equal(app.state.me, null);
      assert.deepEqual(app.state.wechatQr.trail, ['created(201)']);

      // 4. Valid same-origin message from embedded iframe records iframe_cb(ok), does NOT mark logged in directly, and wakes /poll quickly
      const wakeStart = Date.now();
      env.windowMock.dispatchMessage({
        origin: 'http://127.0.0.1:4320',
        source: iframeWin,
        data: { type: 'mio-wechat-qr-callback-landed', error: null },
      });

      assert.equal(app.state.me, null, 'postMessage must NEVER establish login directly');
      assert.equal(app.state.wechatQr.polling, true);
      assert.deepEqual(app.state.wechatQr.trail, ['created(201)', 'iframe_cb(ok)']);

      const deadline = Date.now() + 1000;
      while ((app.state.wechatQr.polling || !app.state.me) && Date.now() < deadline) {
        await delay(15);
      }
      const elapsed = Date.now() - wakeStart;

      assert.ok(elapsed < 1000, `Expected postMessage wake to trigger /poll well before 5s (took ${elapsed}ms)`);
      assert.equal(pollCalls, 1);
      assert.equal(app.state.me?.userId, 'wx-wake-user');
      assert.equal(
        app.el.wechatQrDiagTrail.textContent,
        'created(201) -> iframe_cb(ok) -> confirmed',
      );
    } finally {
      app.stopWechatQrPolling({ resetDiag: true });
      app.closeConversationResources();
      globalThis.fetch = realFetch;
    }
  });

  test('WeChat QR inline flow: 503 WECHAT_UNAVAILABLE & invalid authorization_url show refresh overlay and retry succeeds', async () => {
    resetAppForTest();
    app.bindEvents();
    app.state.me = null;

    let windowOpenCalls = 0;
    const prevOpen = globalThis.window.open;
    globalThis.window.open = () => {
      windowOpenCalls += 1;
      return null;
    };

    const wxLoginCalls = [];
    globalThis.WxLogin = function MockWxLogin(opts) {
      wxLoginCalls.push(opts);
      const container = document.getElementById(opts.id);
      const iframe = document.createElement('iframe');
      container.replaceChildren(iframe);
    };

    let attempt = 0;
    const qrId = 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee';
    const validAuthUrl = `https://open.weixin.qq.com/connect/qrconnect?appid=wx-retry&redirect_uri=${encodeURIComponent('https://mio-qr.example.com/api/auth/callback/wechat')}&state=st-retry`;

    globalThis.fetch = async (url) => {
      const u = String(url);
      if (u === '/api/auth/wechat/qr-session') {
        attempt += 1;
        if (attempt === 1) {
          return new Response(
            JSON.stringify({
              error: { code: 'WECHAT_UNAVAILABLE', message: 'WeChat login is disabled' },
            }),
            { status: 503, headers: { 'content-type': 'application/json' } },
          );
        }
        if (attempt === 2) {
          return new Response(
            JSON.stringify({
              qrcode_id: qrId,
              authorization_url:
                'https://evil.example.com/connect/qrconnect?appid=wx-bad&redirect_uri=https://mio-qr.example.com/cb&state=s1',
              expire_seconds: 300,
            }),
            { status: 201, headers: { 'content-type': 'application/json' } },
          );
        }
        if (attempt === 3) {
          return new Response(
            JSON.stringify({
              qrcode_id: qrId,
              authorization_url:
                'https://open.weixin.qq.com/connect/qrconnect?appid=wx-bad&redirect_uri=/api/auth/callback/wechat&state=s1',
              expire_seconds: 300,
            }),
            { status: 201, headers: { 'content-type': 'application/json' } },
          );
        }
        return new Response(
          JSON.stringify({
            qrcode_id: qrId,
            authorization_url: validAuthUrl,
            expire_seconds: 300,
          }),
          { status: 201, headers: { 'content-type': 'application/json' } },
        );
      }
      if (u === `/api/auth/wechat/qr-session/${qrId}/poll`) {
        return new Response(JSON.stringify({ status: 'pending' }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        });
      }
      return new Response('{}', { status: 200, headers: { 'content-type': 'application/json' } });
    };

    try {
      // Attempt 1: 503 WECHAT_UNAVAILABLE -> error state + clickable refresh overlay
      await app.el.tabWechatQrBtn.click();
      assert.equal(windowOpenCalls, 0);
      assert.equal(wxLoginCalls.length, 0);
      assert.equal(app.state.wechatQr.polling, false);
      assert.equal(app.state.wechatQr.status, 'error');
      assert.equal(app.el.wechatQrRefreshBtn.classList.contains('hidden'), false);
      assert.equal(app.el.wechatQrDiagBox.classList.contains('hidden'), true);
      assert.equal(app.el.authErrorText.classList.contains('hidden'), false);
      assert.match(app.el.authErrorText.textContent, /WECHAT_UNAVAILABLE/);

      // Attempt 2: click #wechatQrRefreshBtn -> non-WeChat origin rejected BEFORE WxLogin
      await app.el.wechatQrRefreshBtn.click();
      assert.equal(wxLoginCalls.length, 0);
      assert.equal(app.state.wechatQr.status, 'error');
      assert.equal(app.el.wechatQrRefreshBtn.classList.contains('hidden'), false);
      assert.match(app.el.authErrorText.textContent, /不允许的微信授权源/);

      // Attempt 3: relative redirect_uri rejected BEFORE WxLogin
      await app.el.wechatQrRefreshBtn.click();
      assert.equal(wxLoginCalls.length, 0);
      assert.equal(app.state.wechatQr.status, 'error');
      assert.equal(app.el.wechatQrRefreshBtn.classList.contains('hidden'), false);
      assert.match(app.el.authErrorText.textContent, /绝对 HTTP\(S\) 地址/);

      // Attempt 4: click #wechatQrViewport in error state -> refreshes and renders WxLogin cleanly
      await app.el.wechatQrViewport.click();
      assert.equal(wxLoginCalls.length, 1);
      assert.equal(app.state.wechatQr.status, 'pending');
      assert.equal(app.state.wechatQr.polling, true);
      assert.equal(app.el.wechatQrRefreshBtn.classList.contains('hidden'), true);
      assert.equal(app.el.authErrorText.classList.contains('hidden'), true);
    } finally {
      app.stopWechatQrPolling({ resetDiag: true });
      globalThis.window.open = prevOpen;
      globalThis.fetch = realFetch;
    }
  });

  test('WeChat QR inline flow: >5 consecutive 401 responses stop polling with guidance and reveal refresh overlay', async () => {
    resetAppForTest();
    app.bindEvents();
    app.state.me = null;

    globalThis.WxLogin = function MockWxLogin(opts) {
      const container = document.getElementById(opts.id);
      const iframe = document.createElement('iframe');
      container.replaceChildren(iframe);
    };

    const qrId = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
    const authUrl = `https://open.weixin.qq.com/connect/qrconnect?appid=wx-401-test&redirect_uri=${encodeURIComponent('https://mio-qr.example.com/api/auth/callback/wechat')}&state=st401`;
    let pollCalls = 0;
    globalThis.fetch = async (url) => {
      const u = String(url);
      if (u === '/api/auth/wechat/qr-session') {
        return new Response(
          JSON.stringify({ qrcode_id: qrId, authorization_url: authUrl, expire_seconds: 300 }),
          { status: 201, headers: { 'content-type': 'application/json' } },
        );
      }
      if (u === `/api/auth/wechat/qr-session/${qrId}/poll`) {
        pollCalls += 1;
        return new Response(
          JSON.stringify({ error: { code: 'UNAUTHORIZED', message: 'Missing session cookie' } }),
          { status: 401, headers: { 'content-type': 'application/json' } },
        );
      }
      return new Response('{}', { status: 200 });
    };

    try {
      await app.el.tabWechatQrBtn.click();

      // Wait until >5 consecutive 401 responses stop polling (5 retries + 6th stops)
      const deadline = Date.now() + 3000;
      while (app.state.wechatQr.polling && Date.now() < deadline) {
        await delay(15);
      }

      assert.equal(app.state.wechatQr.polling, false);
      assert.equal(app.state.wechatQr.status, 'error');
      assert.equal(pollCalls, 6);
      assert.equal(app.el.wechatQrRefreshBtn.classList.contains('hidden'), false);
      assert.equal(app.el.wechatQrEmbedContainer.children.length, 0);
      assert.equal(app.el.wechatQrDiagTrail.textContent, 'created(201) -> 401');
      assert.equal(app.el.authErrorText.classList.contains('hidden'), false);
      assert.match(app.el.authErrorText.textContent, /401 UNAUTHORIZED/);
      assert.match(app.el.authErrorText.textContent, /Session Cookie/);
    } finally {
      app.stopWechatQrPolling({ resetDiag: true });
      globalThis.fetch = realFetch;
    }
  });

  test('WeChat QR inline flow: async MutationObserver scaling, tab-switch cleanup/in-flight cancellation, and wxLogin.js script deduplication', async () => {
    resetAppForTest();
    app.bindEvents();
    app.state.me = null;

    // 1. Deduplicated script loader appends only 1 script tag for concurrent loads
    const load1 = app.loadWxLoginScript();
    const load2 = app.loadWxLoginScript();
    const scripts = env.findAll(env.documentMock.head, (n) => n.tagName === 'SCRIPT');
    assert.equal(scripts.length, 1);
    assert.equal(
      scripts[0].src,
      'https://res.wx.qq.com/connect/zh_CN/htmledition/js/wxLogin.js',
    );

    const AsyncMockWxLogin = function AsyncMockWxLogin() {
      // Intentionally do NOT insert iframe synchronously so MutationObserver is engaged
    };
    globalThis.window.WxLogin = AsyncMockWxLogin;
    await scripts[0].dispatch('load');
    const [ctor1, ctor2] = await Promise.all([load1, load2]);
    assert.equal(ctor1, AsyncMockWxLogin);
    assert.equal(ctor2, AsyncMockWxLogin);

    // 2. Async iframe insertion scales via MutationObserver and disconnects on fit
    const qrId = 'ffffffff-ffff-4fff-8fff-ffffffffffff';
    const authUrl = `https://open.weixin.qq.com/connect/qrconnect?appid=wx-obs&redirect_uri=${encodeURIComponent('https://mio-qr.example.com/api/auth/callback/wechat')}&state=st-obs`;
    let resolveDeferredCreate = null;
    let createCalls = 0;

    globalThis.fetch = (url) => {
      const u = String(url);
      if (u === '/api/auth/wechat/qr-session') {
        createCalls += 1;
        if (createCalls === 1) {
          return Promise.resolve(
            new Response(
              JSON.stringify({ qrcode_id: qrId, authorization_url: authUrl, expire_seconds: 300 }),
              { status: 201, headers: { 'content-type': 'application/json' } },
            ),
          );
        }
        return new Promise((resolve) => {
          resolveDeferredCreate = resolve;
        });
      }
      return Promise.resolve(
        new Response(JSON.stringify({ status: 'pending' }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        }),
      );
    };

    try {
      await app.el.tabWechatQrBtn.click();
      assert.equal(env.MockMutationObserver.instances.length, 1);
      const obs1 = env.MockMutationObserver.instances[0];
      assert.equal(obs1.disconnected, false);
      assert.equal(obs1.target, app.el.wechatQrEmbedContainer);

      const asyncIframe = document.createElement('iframe');
      app.el.wechatQrEmbedContainer.appendChild(asyncIframe);
      obs1.trigger();
      assert.equal(asyncIframe.style.transform, 'translate(-50%, -50%) scale(0.62)');
      assert.equal(obs1.disconnected, true);

      // Switching away from 微信扫码 tab stops polling and clears container
      await app.el.tabSignInBtn.click();
      assert.equal(app.state.authTab, 'signin');
      assert.equal(app.state.wechatQr.polling, false);
      assert.equal(app.el.wechatQrEmbedContainer.children.length, 0);
      assert.equal(app.el.authForm.classList.contains('hidden'), false);
      assert.equal(app.el.wechatQrPanel.classList.contains('hidden'), true);

      // 3. Switching away while createWechatQrSession is in flight cancels stale completion
      const pendingCreate = app.el.tabWechatQrBtn.click();
      assert.equal(app.state.wechatQr.loading, true);
      await app.el.tabSignUpBtn.click();
      assert.equal(app.state.wechatQr.loading, false);
      assert.equal(app.state.wechatQr.polling, false);

      resolveDeferredCreate(
        new Response(
          JSON.stringify({ qrcode_id: 'stale-qr', authorization_url: authUrl, expire_seconds: 300 }),
          { status: 201, headers: { 'content-type': 'application/json' } },
        ),
      );
      await pendingCreate;

      assert.equal(app.state.wechatQr.polling, false);
      assert.notEqual(app.state.wechatQr.qrcodeId, 'stale-qr');
      assert.equal(app.el.wechatQrEmbedContainer.children.length, 0);

      // 4. Clicking refresh while wxLogin.js is still loading deduplicates <script> and mounts only the latest session
      delete globalThis.window.WxLogin;
      delete globalThis.WxLogin;
      env.documentMock.head.replaceChildren();
      const mountedStates = [];
      let scriptRaceReq = 0;
      globalThis.fetch = (url) => {
        const u = String(url);
        if (u === '/api/auth/wechat/qr-session') {
          scriptRaceReq += 1;
          const reqNum = scriptRaceReq;
          const raceUrl = `https://open.weixin.qq.com/connect/qrconnect?appid=wx-race&redirect_uri=${encodeURIComponent('https://mio-qr.example.com/api/auth/callback/wechat')}&state=st-race-${reqNum}`;
          return Promise.resolve(
            new Response(
              JSON.stringify({
                qrcode_id: `race-qr-${reqNum}`,
                authorization_url: raceUrl,
                expire_seconds: 300,
              }),
              { status: 201, headers: { 'content-type': 'application/json' } },
            ),
          );
        }
        return Promise.resolve(
          new Response(JSON.stringify({ status: 'pending' }), {
            status: 200,
            headers: { 'content-type': 'application/json' },
          }),
        );
      };

      const run1 = app.el.tabWechatQrBtn.click();
      await delay(10);
      const run2 = app.el.wechatQrRefreshBtn.click();
      await delay(10);

      const raceScripts = env.findAll(env.documentMock.head, (n) => n.tagName === 'SCRIPT');
      assert.equal(raceScripts.length, 1, 'Concurrent refresh while wxLogin.js loads must append only 1 script');

      globalThis.window.WxLogin = function RaceWxLogin(opts) {
        mountedStates.push(opts.state);
        const container = document.getElementById(opts.id);
        const iframe = document.createElement('iframe');
        iframe.contentWindow = { id: opts.state };
        container.replaceChildren(iframe);
      };
      await raceScripts[0].dispatch('load');
      await Promise.all([run1, run2]);

      assert.deepEqual(mountedStates, ['st-race-2'], 'Only the latest refresh run may instantiate WxLogin');
      assert.equal(app.state.wechatQr.qrcodeId, 'race-qr-2');
      assert.equal(app.state.wechatQr.polling, true);

      // 5. postMessage arriving while a /poll fetch is already in flight does NOT start a concurrent /poll,
      // records error code in trail, and wakes the follow-up poll right after the in-flight poll completes
      app.stopWechatQrPolling({ resetDiag: true });
      delete globalThis.window.WxLogin;
      app.state.wechatQr.pollIntervalMs = 5000;
      const inFlightIframeWin = { id: 'in-flight-win' };
      globalThis.WxLogin = function InFlightWxLogin(opts) {
        const container = document.getElementById(opts.id);
        const iframe = document.createElement('iframe');
        iframe.contentWindow = inFlightIframeWin;
        container.replaceChildren(iframe);
      };
      let resolveFirstPoll = null;
      let activePolls = 0;
      let maxConcurrentPolls = 0;
      let totalInFlightTestPolls = 0;
      globalThis.fetch = (url) => {
        const u = String(url);
        if (u === '/api/auth/wechat/qr-session') {
          return Promise.resolve(
            new Response(
              JSON.stringify({
                qrcode_id: 'qr-inflight',
                authorization_url: authUrl,
                expire_seconds: 300,
              }),
              { status: 201, headers: { 'content-type': 'application/json' } },
            ),
          );
        }
        if (u === '/api/auth/wechat/qr-session/qr-inflight/poll') {
          totalInFlightTestPolls += 1;
          activePolls += 1;
          maxConcurrentPolls = Math.max(maxConcurrentPolls, activePolls);
          if (totalInFlightTestPolls === 1) {
            return new Promise((resolve) => {
              resolveFirstPoll = () => {
                activePolls -= 1;
                resolve(
                  new Response(JSON.stringify({ status: 'pending' }), {
                    status: 200,
                    headers: { 'content-type': 'application/json' },
                  }),
                );
              };
            });
          }
          activePolls -= 1;
          return Promise.resolve(
            new Response(JSON.stringify({ status: 'failed' }), {
              status: 200,
              headers: { 'content-type': 'application/json' },
            }),
          );
        }
        return Promise.resolve(new Response('{}', { status: 200 }));
      };

      app.state.wechatQr.pollIntervalMs = 5;
      await app.el.wechatQrRefreshBtn.click();
      // Wait until poll #1 is actively in flight
      const pollWaitDeadline = Date.now() + 500;
      while (!resolveFirstPoll && Date.now() < pollWaitDeadline) {
        await delay(5);
      }
      assert.ok(resolveFirstPoll, 'Expected first /poll request to be in flight');
      // Switch default interval to 5000ms so wake-pending behavior is clearly verified
      app.state.wechatQr.pollIntervalMs = 5000;

      env.windowMock.dispatchMessage({
        origin: 'http://127.0.0.1:4320',
        source: inFlightIframeWin,
        data: { type: 'mio-wechat-qr-callback-landed', error: 'WECHAT_AUTH_FAILED' },
      });
      assert.equal(totalInFlightTestPolls, 1, 'Wake during in-flight poll must not fire concurrent poll');
      assert.equal(maxConcurrentPolls, 1);

      resolveFirstPoll();
      const wakeFinishDeadline = Date.now() + 1000;
      while (app.state.wechatQr.polling && Date.now() < wakeFinishDeadline) {
        await delay(10);
      }
      assert.equal(maxConcurrentPolls, 1);
      assert.equal(totalInFlightTestPolls, 2);
      assert.equal(app.state.wechatQr.status, 'failed');
      assert.equal(
        app.el.wechatQrDiagTrail.textContent,
        'created(201) -> iframe_cb(WECHAT_AUTH_FAILED) -> pending -> failed',
      );

      // 6. Retry succeeds after wxLogin.js fires `load` without defining WxLogin (dead <script> removed)
      app.stopWechatQrPolling({ resetDiag: true });
      delete globalThis.window.WxLogin;
      delete globalThis.WxLogin;
      env.documentMock.head.replaceChildren();

      const deadLoadPromise = app.loadWxLoginScript();
      const firstScripts = env.findAll(env.documentMock.head, (n) => n.tagName === 'SCRIPT');
      assert.equal(firstScripts.length, 1);
      await firstScripts[0].dispatch('load');
      await assert.rejects(deadLoadPromise, /未暴露 WxLogin/);
      assert.equal(
        env.findAll(env.documentMock.head, (n) => n.tagName === 'SCRIPT').length,
        0,
        'Dead <script> without WxLogin must be removed so retry does not hang',
      );

      const retryLoadPromise = app.loadWxLoginScript();
      const retryScripts = env.findAll(env.documentMock.head, (n) => n.tagName === 'SCRIPT');
      assert.equal(retryScripts.length, 1);
      assert.notEqual(retryScripts[0], firstScripts[0]);
      const RetryWxLogin = function RetryWxLogin(opts) {
        const container = document.getElementById(opts.id);
        container.replaceChildren(document.createElement('iframe'));
      };
      globalThis.window.WxLogin = RetryWxLogin;
      await retryScripts[0].dispatch('load');
      assert.equal(await retryLoadPromise, RetryWxLogin);

      // 7. Stopping an active pending QR session via stopWechatQrPolling() without status transitions to stopped and shows refresh overlay
      globalThis.fetch = (url) => {
        if (String(url) === '/api/auth/wechat/qr-session') {
          return Promise.resolve(
            new Response(
              JSON.stringify({
                qrcode_id: 'qr-stop-check',
                authorization_url: authUrl,
                expire_seconds: 300,
              }),
              { status: 201, headers: { 'content-type': 'application/json' } },
            ),
          );
        }
        return Promise.resolve(
          new Response(JSON.stringify({ status: 'pending' }), {
            status: 200,
            headers: { 'content-type': 'application/json' },
          }),
        );
      };
      app.state.wechatQr.pollIntervalMs = 5000;
      await app.el.wechatQrRefreshBtn.click();
      assert.equal(app.state.wechatQr.status, 'pending');
      assert.equal(app.state.wechatQr.polling, true);
      assert.equal(app.el.wechatQrRefreshBtn.classList.contains('hidden'), true);

      app.stopWechatQrPolling();
      assert.equal(app.state.wechatQr.status, 'stopped');
      assert.equal(app.state.wechatQr.polling, false);
      assert.equal(app.el.wechatQrEmbedContainer.children.length, 0);
      assert.equal(app.el.wechatQrRefreshBtn.classList.contains('hidden'), false);
    } finally {
      app.stopWechatQrPolling({ resetDiag: true });
      globalThis.fetch = realFetch;
    }
  });

  test('WeChat QR flow: stale in-flight manual poll does not mutate state after resetDiag or stopWechatQrPolling', async () => {
    resetAppForTest();
    app.bindEvents();

    const qrId = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc';
    app.state.wechatQr.qrcodeId = qrId;
    app.state.wechatQr.status = 'confirmed';
    app.state.wechatQr.pollCount = 2;
    app.state.wechatQr.trail = ['created(201)', 'confirmed'];
    app.renderWechatQr();

    let resolveManualPoll = null;
    let rejectManualPoll = null;
    let callIdx = 0;
    globalThis.fetch = (url) => {
      assert.equal(String(url), `/api/auth/wechat/qr-session/${qrId}/poll`);
      callIdx += 1;
      if (callIdx === 1) {
        return new Promise((resolve) => {
          resolveManualPoll = resolve;
        });
      }
      return new Promise((_, reject) => {
        rejectManualPoll = reject;
      });
    };

    try {
      // 1. Stale resolved manual poll after stopWechatQrPolling({ resetDiag: true })
      app.el.globalNotice.textContent = 'switched-mode';
      app.el.globalNotice.className = 'notice notice-info';
      const inFlightResolve = app.el.wechatQrManualPollBtn.click();
      assert.equal(app.state.wechatQr.manualPolling, true);

      app.stopWechatQrPolling({ resetDiag: true });
      assert.equal(app.state.wechatQr.manualPolling, false);
      assert.equal(app.state.wechatQr.qrcodeId, null);

      resolveManualPoll(
        new Response(JSON.stringify({ status: 'consumed' }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        }),
      );
      await inFlightResolve;

      assert.equal(app.state.wechatQr.qrcodeId, null);
      assert.equal(app.state.wechatQr.status, 'idle');
      assert.equal(app.state.wechatQr.pollCount, 0);
      assert.deepEqual(app.state.wechatQr.trail, []);
      assert.equal(app.el.wechatQrDiagBox.classList.contains('hidden'), true);
      assert.equal(app.el.globalNotice.textContent, 'switched-mode');

      // 2. Stale rejected manual poll after stopWechatQrPolling() (e.g., sign-out / new QR session)
      app.state.wechatQr.qrcodeId = qrId;
      app.state.wechatQr.status = 'confirmed';
      app.state.wechatQr.pollCount = 1;
      app.state.wechatQr.trail = ['created(201)', 'confirmed'];
      app.renderWechatQr();
      app.el.globalNotice.textContent = 'signed-out';

      const inFlightReject = app.el.wechatQrManualPollBtn.click();
      assert.equal(app.state.wechatQr.manualPolling, true);

      app.stopWechatQrPolling();
      assert.equal(app.state.wechatQr.manualPolling, false);

      rejectManualPoll(new Error('stale network failure'));
      await inFlightReject;

      assert.equal(app.state.wechatQr.status, 'confirmed');
      assert.equal(app.state.wechatQr.pollCount, 1);
      assert.deepEqual(app.state.wechatQr.trail, ['created(201)', 'confirmed']);
      assert.equal(app.el.globalNotice.textContent, 'signed-out');
    } finally {
      app.stopWechatQrPolling({ resetDiag: true });
      globalThis.fetch = realFetch;
    }
  });
});
