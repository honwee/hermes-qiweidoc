# QiweiDoc 渠道安装完成 ✅

插件已装到 `~/.hermes/plugins/qiweidoc/`，`QIWEIDOC_PAT` 已写入 `~/.hermes/.env`。
再做 3 步就能起服务。

## 1. 配模型（Azure OpenAI / azure-foundry）

把 `config.yaml.example` 的 `model` + `plugins` 块合并进 `~/.hermes/config.yaml`：

```yaml
model:
  provider: azure-foundry
  base_url: https://lumistai.openai.azure.com/openai/v1
  api_mode: chat_completions
  default: gpt-5.4
  model: gpt-5.4
  context_length: 400000
  supports_vision: true          # 图片/表情走原生视觉，别漏

plugins:
  enabled:
    - qiweidoc                    # 平台插件 opt-in，不列就不加载
```

## 2. 填 key

编辑 `~/.hermes/.env`，把 Azure key 填上（`QIWEIDOC_PAT` 安装时已写好）：

```
AZURE_FOUNDRY_API_KEY=<你的 Azure OpenAI key>
# 可选：QIWEIDOC_BASE_URL（默认 https://x.lumiclass.com）、
#       QIWEIDOC_ALLOWED_GROUPS、QIWEIDOC_HOME_CHAT_ID、GATEWAY_ALLOW_ALL_USERS=true
```

## 3. ⚠️ 起服务前：先停掉生产 bot

本渠道用 PAT 订阅服务端 WS。**同一个 PAT 同时跑两个 bot，会对同一条客户消息各回一遍**。
用生产 PAT 在本机起 gateway 前，先到服务器停掉生产实例：

```
docker stop hermes-qiweidoc
```

（要换回生产时 `docker start hermes-qiweidoc`；长期并行请改用一个独立测试员工的 PAT。）

## 起服务

```
hermes gateway
```

验证：在绑定的测试群发图片/文件/语音，看 `~/.hermes/logs/gateway.log` 与群里回复。
`hermes plugins list` 应能看到 `qiweidoc`；`hermes doctor` 可排查环境问题。
