# hermes-qiweidoc

Hermes Agent platform plugin for [qiweidoc](https://github.com/lumiclass/qiweidoc) —
brings WeCom (企业微信) groups into the Hermes gateway as a first-class channel.

- **Inbound**  WebSocket subscription to `wss://<base>/lumist/ws?token=<PAT>`
- **Outbound** `POST /api/ai/rpa/dispatch` (server-side WorkTool RPA pipeline)

The server side enforces hosting on/off, per-hour rate limit, silent-window,
and `(employee_id, replied_to_msg_id)` dedup — the adapter is intentionally
thin.

## Install

```bash
# 1. Copy the plugin into your hermes-agent checkout
cp -r plugins/platforms/qiweidoc <hermes-agent>/plugins/platforms/

# 2. Install runtime deps (already pulled by most hermes setups)
pip install websockets httpx

# 3. Configure env (e.g. in ~/.hermes/.env)
export QIWEIDOC_PAT=lmt_xxxxxxxxxxxxxxxxxxxx          # from `lumist auth login`
export QIWEIDOC_BASE_URL=https://x.lumiclass.com      # default
# optional:
# export QIWEIDOC_ALLOWED_GROUPS=R:1234,R:5678        # csv; empty = all bound
# export QIWEIDOC_HOME_CHAT_ID=R:1234                 # cron target
# export QIWEIDOC_REPLY_ON_ADDRESS_ONLY=true          # @mention gate

# 4. Restart the gateway
hermes gateway restart
```

`hermes gateway status` should now list `qiweidoc` as connected.

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
- `message_id` = inbound `msg_id` (carried on `reply_to_message_id` so the
  outbound dispatch can pass it as `reply_to_msg_id` for the server-side
  dedup index)
- Cron / standalone delivery routes through `_standalone_send` — same REST
  call, no gateway-runner required.

## Server prerequisites (in qiweidoc)

The plugin assumes these are already in place — see commits in qiweidoc:

- `ai_hosting.rpa_tasks` with `replied_to_msg_id` column + unique index
- `AiAggregateController::rpaDispatch` enforces hosting window + hourly cap +
  dedup; accepts `reply_to_msg_id` and (for admin scope) `force` body fields
- `/lumist/ws` deployed and reachable
- `lumist auth login` flow issues PATs with `scope=self`

## Status

MVP. Tested against `lumist watch` event schema (`type=ready|msg|error`,
`data.{from, from_name, roomid, room_name, msg, msg_time, msg_id}`). No
attachment support yet — text only.
