# Mio Web Demo（多轮对话 · 视觉理解 · AI 生图）

零外部依赖的本地 Mio 交互式 Web 工作台（Node 24 内置模块 + 原生浏览器 ES Modules，无需 `npm install`），支持一键拉起零扣费 **Mio Sandbox** 或连接常驻 **Mio Dev** 环境。

## 目录结构

- `package.json`：脚本入口（`start` / `start:proxy` / `test`）
- `server.mjs`：绑定 `127.0.0.1`（默认 `4320`），提供静态页面（含 `/auth/wechat/qr-complete` 微信授权完成页）、`/demo-api/config` 环境切换、`/api/*` 同源反向代理（支持 HTTPS/公网 `Origin` 透传与回环 `Origin` 重写、`Set-Cookie` 剥离 `Domain` 并保留 HTTPS/`__Secure-`/`__Host-` 的 `Secure` 属性、`/api/auth/callback/wechat` 脱敏边界日志 `wechat_callback_proxy`、无缓冲 SSE 实时流转发）、白名单受控的 `PUT /demo-api/cos-upload` 二进制中继，以及 `--with-sandbox` 子进程生命周期管理
- `public/index.html`、`public/qr-complete.html`、`public/styles.css`、`public/api.js`、`public/app.js`：纯前端响应式中文交互界面、内嵌 `WxLogin({ self_redirect: true })` 自定义 `<div>` 微信扫码标签页（零二次弹窗）与微信 QR Polling 诊断面板
- `tests/demo-e2e.test.mjs`：基于 `node:test` 的真实端到端测试套件

## 快速启动

### 方式一：一键拉起零扣费 Sandbox + Demo UI（推荐）

```bash
cd /Volumes/ssd/gs_projects/p-mio/mio-demo-web
npm start
```

若 `http://127.0.0.1:4319/readyz` 尚未就绪，`server.mjs --with-sandbox` 会自动拉起 `../mio-apifox/scripts/start-mock-sandbox.mjs`（使用 `mio_regression_test` 临时 Schema 与全本地拦截 Mock，零外网调用、零扣费），随后在浏览器打开：

- **http://127.0.0.1:4320**

点击左侧 **「一键登录沙盒测试账号」** 即可开始体验多轮对话生图、看图追问与引用二次生图。按 `Ctrl+C`（`SIGINT`）或发送 `SIGTERM` 时会自动优雅关闭沙盒并清理临时 Schema。

### 方式二：仅启动代理并连接已有 Sandbox 或 Mio Dev

```bash
cd /Volumes/ssd/gs_projects/p-mio/mio-demo-web
npm run start:proxy
```

在页面左上角可实时切换：
- **Mio Sandbox 零扣费模拟**（`http://127.0.0.1:4319`）
- **Mio Dev 常驻环境**（`http://localhost:4310`，需使用你在 Dev 环境注册的账号并在「配置 / 测试 Key」弹窗中保存已授权的 OneLink Key）

## 环境变量

- `DEMO_PORT`：Demo 服务端口（默认 `4320`，`0` 表示随机可用端口）
- `MIO_SANDBOX_PORT`：Sandbox 端口（默认 `4319`）
- `MIO_DEV_ORIGIN`：Dev 上游地址（默认 `http://localhost:4310`）
- `MIO_PUBLIC_ORIGIN`：可选，强制覆盖转发给上游的对外 `Origin`（如 `https://<callback-domain>`）；未设置时自动透传公网 / HTTPS 请求 `Origin`（仅本地回环 HTTP 重写为上游地址）
- `DEMO_MODE`：初始目标模式 `sandbox` 或 `dev`（默认 `sandbox`）

## 真实微信扫码登录（QR Polling）端到端测试

用于针对 `codex/auth-qr-polling` 分支开展真实微信网站应用 + 手机微信扫码验收。

### 1. 启动 `auth-qr-polling` 后端（`:4310`）

在 `/Users/coolm/.codex/worktrees/auth-qr-polling/mio` 的私有 `.env` 中配置微信网站应用凭据与公网 HTTPS 回调域：

- `MIO_AUTH_BASE_URL=https://<callback-domain>`（与微信开放平台登记的授权回调域一致，不含路径和末尾 `/`）
- `MIO_AUTH_WECHAT_ENABLED=true`
- `MIO_AUTH_WECHAT_APP_ID=...` 与 `MIO_AUTH_WECHAT_APP_SECRET=...`

执行显式迁移并启动常驻 Dev 服务（监听 `:4310`）：

```bash
cd /Users/coolm/.codex/worktrees/auth-qr-polling/mio
pnpm migrate:dev --apply
pnpm build
pnpm start:dev
```

### 2. 启动 `mio-demo-web` Dev 代理（`:4320`）

```bash
cd /Volumes/ssd/gs_projects/p-mio/mio-demo-web
DEMO_MODE=dev npm run start:proxy
```

> 若上游非默认 `http://localhost:4310`，可附加 `MIO_DEV_ORIGIN=...`；若外层反代不传递标准 `Origin` / `X-Forwarded-Proto`，可显式设置 `MIO_PUBLIC_ORIGIN=https://<callback-domain>`。

### 3. 将公网 HTTPS 隧道 / 反向代理指向 `http://127.0.0.1:4320`

将 `https://<callback-domain>` 的全部流量（包含 `/`、`/auth/wechat/qr-complete`、`/api/auth/*`、`/api/v1/*`）反代或隧道转发至 `http://127.0.0.1:4320`（保持同源并建议透传 `X-Forwarded-Proto: https` 与原始 `Origin`）。

随后在桌面浏览器访问 **`https://<callback-domain>/`**，确认左上角环境为 **Mio Dev 常驻环境** 且 `/readyz` 就绪。

### 4. 内嵌 `WxLogin({ self_redirect: true })` 扫码验收与边界日志核对

本 Demo 在左侧「账号会话」卡片提供三标签切换：**邮箱登录 / 注册账号 / 微信扫码**。切换到 **「微信扫码」** 时不会调用 `window.open` 弹出二级窗口，而是：
- 调用 `POST /api/auth/wechat/qr-session`，严格校验 `authorization_url` 来自 `https://open.weixin.qq.com` 并解析 `appid`、`redirect_uri`、`state`、`scope`；
- 动态加载微信官方 `wxLogin.js` 并以 `new WxLogin({ self_redirect: true, id: 'wechatQrEmbedContainer', ... })` 在自定义 `248×248` `<div>` 容器内直接渲染缩放适配后的微信二维码；
- 扫码授权后，微信在同内嵌 iframe 内重定向至 `/api/auth/callback/wechat` 并落地 `/auth/wechat/qr-complete`（自动启用紧凑 iframe 视图并隐藏关闭按钮，仅向同源父页发送无凭据唤醒消息 `mio-wechat-qr-callback-landed`），主页面始终通过真实 `GET /api/auth/wechat/qr-session/{id}/poll` + `GET /api/v1/me` 建立登录态。

#### 五项 `real-wechat` 验收检查清单

1. **内嵌二维码渲染（零二次弹窗）**：在左侧账号卡片切换到 **「微信扫码」** 标签，确认 `POST /api/auth/wechat/qr-session` 返回 `201`（`qrcode_id`、`authorization_url`、`expire_seconds: 300`），写入 `__Secure-mio.state` 与限定 poll 路径的 `__Secure-mio.wechat_qr`，二维码直接显示在自定义 `248×248` 容器内，不弹出新窗口，页面持续 `pending` 轮询。
2. **手机扫码确认、UI 与 `wechat_callback_proxy` 边界日志核对**：使用手机微信扫码并确认授权，观察：
   - **正常收敛路径**：`server.mjs` stdout 输出单行脱敏 JSON 日志 `wechat_callback_proxy`（绝不打印 `code`/`state` 或 Cookie 明文），其中 `statusCode: 302`、`hasCookieHeader: true`、`hasStateCookieName: true`、`secFetchDest: "iframe"`、`setSessionCookie: true`；内嵌 iframe 落地 `/auth/wechat/qr-complete` 显示紧凑版「扫码授权已完成，正在确认登录...」并唤醒 `/poll`；左侧诊断面板状态流转依次出现 `created(201) -> pending -> iframe_cb(ok) -> confirmed`（容忍隧道瞬态 `401` 重试）并自动拉取 `/api/v1/me` 完成登录（新用户建号、已有微信用户复登不重复建号）。
   - **若浏览器拦截跨站触发的 iframe `SameSite=Lax` Cookie**：会清晰暴露为 `wechat_callback_proxy` 中 `statusCode: 400` 且 `hasStateCookieName: false`（`hasCookieHeader: false` 或未携带 `(__Secure-)mio.state`）、`setSessionCookie: false`，后端返回 `400 STATE_MISMATCH` 且不会错误标记 QR 为登录成功，便于第一时间确认是否触发浏览器 `SameSite=Lax` + `self_redirect` iframe 边界。
3. **单次消费校验（`confirmed -> consumed`）**：登录成功后在左侧「QR Polling 诊断」卡片点击「再轮询一次（验证 consumed / expired）」，观察状态流转追加 `consumed`（`200 { status: "consumed" }`），且当前登录态保持不变。
4. **取消 / 拒绝授权校验（`failed`）**：退出登录并重新加载微信二维码，在手机端取消/拒绝授权，确认内嵌完成页安全显示紧凑脱敏错误（如 `WECHAT_AUTH_FAILED`），主页面轮询收到 `failed` 后自动停止、遮罩提示「点击刷新二维码」且保持未登录。
5. **过期与跨浏览器隔离校验（`expired` / `400` / `401`）**：等待超过 5 分钟（300 秒）后观察轮询返回 `expired`、自动停止并显示刷新遮罩（或在登录后满 5 分钟手动再轮询观察 `expired` 覆盖 `consumed`）；确认跨浏览器/无匹配 Cookie 请求无法消费或窃取结果（缺/错 QR Cookie 返回 `400 STATE_MISMATCH`，缺/错匹配 Session Cookie 返回 `401 UNAUTHORIZED`）。

## 运行端到端测试

```bash
cd /Volumes/ssd/gs_projects/p-mio/mio-demo-web
npm test
```
