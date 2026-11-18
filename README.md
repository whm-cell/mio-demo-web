# Mio Web Demo（多轮对话 · 视觉理解 · AI 生图）

零外部依赖的本地 Mio 交互式 Web 工作台（Node 24 内置模块 + 原生浏览器 ES Modules，无需 `npm install`），支持一键拉起零扣费 **Mio Sandbox** 或连接常驻 **Mio Dev** 环境。

## 目录结构

- `package.json`：脚本入口（`start` / `start:proxy` / `test`）
- `server.mjs`：绑定 `127.0.0.1`（默认 `4320`），提供静态页面、`/demo-api/config` 环境切换、`/api/*` 同源反向代理（自动重写 `Origin`、处理 `Set-Cookie`、无缓冲 SSE 实时流转发）、白名单受控的 `PUT /demo-api/cos-upload` 二进制中继，以及 `--with-sandbox` 子进程生命周期管理
- `public/index.html`、`public/styles.css`、`public/api.js`、`public/app.js`：纯前端响应式中文交互界面
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
- `DEMO_MODE`：初始目标模式 `sandbox` 或 `dev`（默认 `sandbox`）

## 运行端到端测试

```bash
cd /Volumes/ssd/gs_projects/p-mio/mio-demo-web
npm test
```
