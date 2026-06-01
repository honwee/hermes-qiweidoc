# hermes-qiweidoc

Hermes Agent platform plugin for **qiweidoc** — brings WeCom (企业微信) groups
into the [Hermes Agent](https://github.com/NousResearch/hermes-agent) gateway as
a first-class channel.

- **Inbound**  WebSocket subscription to `wss://<base>/lumist/ws?token=<PAT>`
  (text + 图片/表情/文件 media, with long-message re-fetch)
- **Outbound** `POST /api/ai/rpa/dispatch` (server-side WorkTool RPA pipeline)

The server side enforces hosting on/off, per-hour rate limit, silent-window, and
`(employee_id, replied_to_msg_id)` dedup — the adapter is intentionally thin.

This repo is packaged as an **installable hermes plugin**: its root is the plugin
(`plugin.yaml`, `__init__.py`, `adapter.py`), so `hermes plugins install` clones
it straight into `~/.hermes/plugins/qiweidoc/`. No image edits, no manual copying.

---

## 在一台新机器上部署（原生，无 Docker 镜像）

```bash
# 1. 原生安装 hermes（装 uv / Python 3.11 / Node / ripgrep / ffmpeg 到 ~/.hermes）
curl -fsSL https://raw.githubusercontent.com/NousResearch/hermes-agent/main/scripts/install.sh | bash

# 2. 安装企微渠道（会交互提示输入 QIWEIDOC_PAT，自动写入 ~/.hermes/.env）
hermes plugins install honwee/hermes-qiweidoc

# 3. 配模型 + key（见安装后渲染的 after-install.md / 下面的 config.yaml.example）
#    ~/.hermes/config.yaml ← model(azure-foundry gpt-5.4, supports_vision:true)
#                            + plugins.enabled:[qiweidoc]
#    ~/.hermes/.env        ← AZURE_FOUNDRY_API_KEY=...

# 4. ⚠️ 安全：起本地 gateway 前先停掉生产 bot（同 PAT 会重复回复客户群）
#    在服务器上：docker stop hermes-qiweidoc

# 5. 起服务
hermes gateway
```

`hermes plugins list` 应列出 `qiweidoc`；在绑定的测试群发消息验证。

> **PAT 与重复回复**：本渠道用 PAT 订阅服务端 WS。同一 PAT 同时跑两个 gateway 会对
> 同一条消息各回一遍。复用生产 PAT 时务必先停生产实例；要长期并行，给一个独立测试
> 员工签发单独 PAT，并用独立测试群。

### 配置模板

- [`config.yaml.example`](./config.yaml.example) — 合并 `model` + `plugins` 块进 `~/.hermes/config.yaml`
- [`.env.example`](./.env.example) — 需要的 env（key 手动填，仓库不含真实密钥）
- [`after-install.md`](./after-install.md) — `hermes plugins install` 后自动渲染的同款指引

### 插件声明的环境变量（`plugin.yaml`）

| 变量 | 必填 | 说明 |
|---|---|---|
| `QIWEIDOC_PAT` | ✅ | `lumist auth login` 签发的 PAT（安装时提示录入） |
| `QIWEIDOC_BASE_URL` | | qiweidoc 部署地址，默认 `https://x.lumiclass.com` |
| `QIWEIDOC_WS_PATH` | | WS 路径，默认 `/lumist/ws` |
| `QIWEIDOC_ALLOWED_GROUPS` | | 逗号分隔的群 chat_id；空 = PAT 员工绑定的全部群 |
| `QIWEIDOC_HOME_CHAT_ID` | | cron / 通知的默认投递目标 |
| `QIWEIDOC_REPLY_ON_ADDRESS_ONLY` | | true=仅 @ 时响应（默认 false） |

模型 key（`AZURE_FOUNDRY_API_KEY`）由 hermes 模型层读取，填在 `~/.hermes/.env`。

---

## Architecture

```
WeCom群 ──▶ WorkTool ──▶ qiweidoc API ──▶ PG NOTIFY ──▶ /lumist/ws ──▶ adapter
                                                                         │
                                                                         ▼
                                                                  MessageEvent
                                                                         │
                                                                         ▼
                                                                  Hermes agent
                                                                         │
                                                              SendResult │
WeCom群 ◀── WorkTool ◀── rpa_tasks ◀── /api/ai/rpa/dispatch ◀────────────┘
```

- `chat_id` = WeCom `roomid` (group id)
- `message_id` = inbound `msg_id`, carried on `reply_to_message_id` so the
  outbound dispatch passes it as `reply_to_msg_id` for the server-side dedup index
- Cron / standalone delivery routes through `_standalone_send` — same REST call,
  no gateway-runner required
- The adapter self-registers via `register(ctx)` → `ctx.register_platform(name="qiweidoc", …)`

### 媒体支持

- **图片 / 表情** → 经 `GET /api/ai/media` 下载并落 core 缓存，作为 `PHOTO`/`STICKER`
  事件带像素给模型（需 `model.supports_vision: true`）
- **文件** → 作为 `DOCUMENT`，带文件名；文本类内联、二进制类由模型用工具读取
- **语音 / 视频** → 占位 `[语音]` / `[视频]`，本期不转写
- 长文本（NOTIFY 受 pg_notify 8KB 上限截断）按 `msg_id` 回查完整正文
- 出站拦截 hermes 内部 meta 消息（`No home channel` / `Self-improvement review` /
  `Still working`），不下发到客户群

## Server prerequisites (in qiweidoc)

插件假设服务端已就绪（见 qiweidoc 仓库的相关提交）：

- `ai_hosting.rpa_tasks` 含 `replied_to_msg_id` 列 + 唯一索引
- `AiAggregateController::rpaDispatch` 强制托管窗口 + 每小时上限 + 去重；
  接受 `reply_to_msg_id`，admin scope 下接受 `force`
- `GET /api/ai/media`、`GET /api/ai/message` 端点已部署
- `/lumist/ws` 已部署且可达；`notify_lumist_msg` 触发器带 `msg_type`/`msg`
- `lumist auth login` 流程签发 `scope=self` 的 PAT
