# QiweiDoc 渠道已安装 ✅

插件已克隆到 `~/.hermes/plugins/qiweidoc/`。

> **给 agent 的说明**：`hermes plugins install` 在非交互（agent）环境下会**跳过**
> `QIWEIDOC_PAT` 录入和「enable now?」询问。所以 PAT 还**没**写入，插件也**还没**启用。
> 按下面 4 步执行即可（命令均为非交互，可直接 run）。把 `<PAT>` / `<AZURE_KEY>`
> 换成真实值——这两个密钥需要由人提供。

## 1. 写密钥到 `~/.hermes/.env`

```bash
mkdir -p ~/.hermes
{
  echo "QIWEIDOC_PAT=<PAT>"
  echo "QIWEIDOC_BASE_URL=https://x.lumiclass.com"
  echo "AZURE_FOUNDRY_API_KEY=<AZURE_KEY>"
  echo "GATEWAY_ALLOW_ALL_USERS=true"
} >> ~/.hermes/.env
```

可选项（不需要就略过）：`QIWEIDOC_ALLOWED_GROUPS=`（逗号分隔群 chat_id，空=PAT 员工绑定的全部群）、
`QIWEIDOC_HOME_CHAT_ID=`（cron/通知默认投递目标）、`QIWEIDOC_REPLY_ON_ADDRESS_ONLY=true`（仅 @ 时响应）。

> ⚠️ 别用 `hermes config set QIWEIDOC_PAT ...`：它只把 `*_API_KEY`/`*_TOKEN` 路由进 `.env`，
> `QIWEIDOC_PAT` 会被错误写进 `config.yaml`。直接追加到 `.env` 最稳妥。

## 2. 启用插件（platform 插件默认 opt-in）

```bash
hermes plugins enable qiweidoc
```

## 3. 合并 config.yaml（模型 + 平台块）

把以下内容合并进 `~/.hermes/config.yaml`（已有 `model` 就只补 `platforms`；
机器上已有支持视觉的模型也可保留原模型，跳过 `model` 块）：

```yaml
model:
  provider: azure-foundry
  base_url: https://lumistai.openai.azure.com/openai/v1
  api_mode: chat_completions
  default: gpt-5.4
  model: gpt-5.4
  context_length: 400000
  supports_vision: true          # 图片/表情走原生视觉，别漏

platforms:
  qiweidoc:
    enabled: true                # 老版本 hermes 无 env 自动启用时的兜底
```

## 4. ⚠️ 起服务前：先停掉生产 bot

本渠道用 PAT 订阅服务端 WS。**同一个 PAT 同时跑两个 bot，会对同一条客户消息各回一遍。**
复用生产 PAT 前，先到服务器停掉生产实例（迁移场景下通常已停）：

```bash
# 在服务器上：
docker stop hermes-qiweidoc       # 要回滚生产时 docker start hermes-qiweidoc
```

长期并行请给一个独立测试员工签发单独 PAT + 独立测试群。

## 起服务

```bash
hermes gateway          # 或已在跑：hermes gateway restart
```

验证：`hermes plugins list` 应看到 `qiweidoc`（enabled）；在绑定的测试群发文字/图片/文件，
看 `~/.hermes/logs/gateway.log` 与群里回复。环境问题用 `hermes doctor` 排查。
