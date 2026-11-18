import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { existsSync, readFileSync } from 'node:fs';
import http from 'node:http';
import { createRequire } from 'node:module';
import { dirname, join, resolve } from 'node:path';
import test from 'node:test';
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
