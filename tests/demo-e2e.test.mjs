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
import { validateUploadTarget } from '../server.mjs';

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
      this.alt = '';
      this.dataset = {};
      this.style = {};
      this.attributes = new Map();
      this.listeners = new Map();
      this.children = [];
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

    addEventListener(type, handler) {
      if (!this.listeners.has(type)) this.listeners.set(type, []);
      this.listeners.get(type).push(handler);
    }

    async click() {
      const handlers = this.listeners.get('click') || [];
      for (const h of handlers) {
        await h({ type: 'click', target: this });
      }
    }

    focus() {
      this.focused = true;
    }

    append(...nodes) {
      for (const n of nodes) {
        if (!n) continue;
        if (n.nodeType === 11) {
          this.children.push(...n.children);
          n.children = [];
        } else {
          this.children.push(n);
        }
      }
    }

    replaceChildren(...nodes) {
      this.children = [];
      this.append(...nodes);
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

  const documentMock = {
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
  };

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

  return { documentMock, MockEventSource, findAll, collectText };
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
    app.closeConversationResources();
    app.clearAttachmentsAndAssetCache();
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
    env.MockEventSource.instances = [];

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
      skipBoot: globalThis.__MIO_DEMO_SKIP_BOOT__,
    };
    globalThis.__MIO_DEMO_SKIP_BOOT__ = true;
    globalThis.document = env.documentMock;
    globalThis.window = { location: { origin: 'http://127.0.0.1:4320' } };
    globalThis.EventSource = env.MockEventSource;
    app = await import('../public/app.js');
  });

  test.after(() => {
    app?.closeConversationResources();
    globalThis.fetch = realFetch;
    if (prevGlobals.document === undefined) delete globalThis.document;
    else globalThis.document = prevGlobals.document;
    if (prevGlobals.window === undefined) delete globalThis.window;
    else globalThis.window = prevGlobals.window;
    if (prevGlobals.EventSource === undefined) delete globalThis.EventSource;
    else globalThis.EventSource = prevGlobals.EventSource;
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
});
