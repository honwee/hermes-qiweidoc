"""
QiweiDoc Platform Adapter for Hermes Agent.

Connects to a qiweidoc deployment over WebSocket for inbound WeCom group
messages, and dispatches outbound replies via the server-side WorkTool RPA
pipeline.

  - Inbound:  wss://<base>/lumist/ws?token=<PAT>
              event types: ready / msg / error
  - Outbound: POST <base>/api/ai/rpa/dispatch (Bearer PAT)

Configuration via env vars (preferred) or config.yaml platform extras:
    QIWEIDOC_PAT          - personal access token (required)
    QIWEIDOC_BASE_URL     - https://x.lumiclass.com (default)
    QIWEIDOC_WS_PATH      - /lumist/ws (default)
    QIWEIDOC_ALLOWED_GROUPS  - csv of chat_ids (empty = all bound groups)
    QIWEIDOC_HOME_CHAT_ID    - default chat for cron delivery
    QIWEIDOC_REPLY_ON_ADDRESS_ONLY - "true" to only react when @mentioned
"""

import asyncio
import datetime as _dt
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_document_from_bytes,
    cache_image_from_bytes,
)
from gateway.config import Platform


_DEFAULT_BASE_URL = "https://x.lumiclass.com"
_DEFAULT_WS_PATH = "/lumist/ws"

# 企微媒体类型（与服务端 ChatSessionService::ValidMediaType 对齐）。
# 图片/表情/文件走真正理解（下载 + 缓存 + 视觉/文档管线）；
# 语音/视频本期只占位不转写（hermes 运行时对 voice 走 STT、无 video 分支）。
_DOWNLOAD_MEDIA = {"image", "emotion", "file"}
_PLACEHOLDER_MEDIA = {
    "voice": "[语音消息]",
    "meeting_voice_call": "[语音通话]",
    "video": "[视频]",
}
_DEFAULT_EXT = {"image": "png", "emotion": "png", "file": "bin"}

# hermes 运行时会把内部状态/系统消息也走 channel.send（home channel 未配、
# 自我改进回顾、长任务进行中提示等）。这些不该出现在客户企微群里，出站侧拦掉。
_INTERNAL_PREFIXES = (
    "📬 No home channel is set",
    "💾 Self-improvement review",
    "⏳ Still working",
)


def _is_internal_meta(content: str) -> bool:
    s = (content or "").strip()
    return any(s.startswith(p) for p in _INTERNAL_PREFIXES)


def _bool_env(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "on"}


def _csv_env(name: str) -> List[str]:
    raw = os.getenv(name, "") or ""
    return [p.strip() for p in raw.split(",") if p.strip()]


def _resolve_config(extra: dict) -> Dict[str, Any]:
    """Merge env vars with PlatformConfig.extra; env wins."""
    base_url = (os.getenv("QIWEIDOC_BASE_URL") or extra.get("base_url") or _DEFAULT_BASE_URL).rstrip("/")
    ws_path = os.getenv("QIWEIDOC_WS_PATH") or extra.get("ws_path") or _DEFAULT_WS_PATH
    pat = os.getenv("QIWEIDOC_PAT") or extra.get("pat") or ""
    allowed = _csv_env("QIWEIDOC_ALLOWED_GROUPS") or list(extra.get("allowed_groups") or [])
    home = os.getenv("QIWEIDOC_HOME_CHAT_ID") or extra.get("home_chat_id") or ""
    address_only = (
        _bool_env("QIWEIDOC_REPLY_ON_ADDRESS_ONLY", bool(extra.get("reply_on_address_only", False)))
        if os.getenv("QIWEIDOC_REPLY_ON_ADDRESS_ONLY") is not None
        else bool(extra.get("reply_on_address_only", False))
    )
    return {
        "base_url": base_url,
        "ws_path": ws_path,
        "pat": pat,
        "allowed_groups": set(allowed),
        "home_chat_id": home,
        "address_only": address_only,
    }


class QiweidocAdapter(BasePlatformAdapter):
    """Hermes adapter for qiweidoc / WeCom."""

    def __init__(self, config, **kwargs):
        platform = Platform("qiweidoc")
        super().__init__(config=config, platform=platform)

        extra = getattr(config, "extra", {}) or {}
        c = _resolve_config(extra)
        self.base_url: str = c["base_url"]
        self.ws_path: str = c["ws_path"]
        self.pat: str = c["pat"]
        self.allowed_groups: set = c["allowed_groups"]
        self.home_chat_id: str = c["home_chat_id"]
        self.address_only: bool = c["address_only"]

        self._ws = None
        self._recv_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._self_userid: str = ""  # filled from ready event

    @property
    def name(self) -> str:
        return "QiweiDoc"

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def connect(self) -> bool:
        if not self.pat:
            logger.error("qiweidoc: QIWEIDOC_PAT not configured")
            self._set_fatal_error("config_missing", "QIWEIDOC_PAT must be set", retryable=False)
            return False

        self._stop.clear()
        self._recv_task = asyncio.create_task(self._run_with_reconnect())
        self._mark_connected()
        logger.info("qiweidoc: adapter started base=%s ws=%s", self.base_url, self.ws_path)
        return True

    async def disconnect(self) -> None:
        self._stop.set()
        self._mark_disconnected()
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
            try:
                await self._recv_task
            except (asyncio.CancelledError, Exception):
                pass

    # ── Outbound ─────────────────────────────────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        try:
            import httpx
        except ImportError:
            return SendResult(success=False, error="httpx not installed; pip install httpx")

        meta = metadata or {}

        if _is_internal_meta(content):
            logger.info("qiweidoc: 拦截内部 meta 消息，不下发到群: %s", content[:60])
            return SendResult(success=True, message_id="")

        body: Dict[str, Any] = {
            "group_id": chat_id,
            "content": content,
        }
        # Prefer explicit reply_to_msg_id from metadata, fall back to reply_to.
        # This is the inbound msg_id we are replying to — used by the server
        # for B-3 dedup so retries don't double-send.
        reply_msg_id = meta.get("reply_to_msg_id") or reply_to
        if reply_msg_id:
            body["reply_to_msg_id"] = reply_msg_id
        if meta.get("at_list"):
            body["atList"] = list(meta["at_list"])
        if meta.get("analysis_id"):
            body["analysis_id"] = int(meta["analysis_id"])

        url = f"{self.base_url}/api/ai/rpa/dispatch"
        headers = {
            "Authorization": f"Bearer {self.pat}",
            "Content-Type": "application/json",
        }
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.post(url, json=body, headers=headers)
        except Exception as e:
            logger.warning("qiweidoc: dispatch transport error: %s", e)
            return SendResult(success=False, error=f"transport: {e}")

        if resp.status_code >= 400:
            text = resp.text[:500] if resp.text else ""
            logger.warning("qiweidoc: dispatch http %s: %s", resp.status_code, text)
            return SendResult(success=False, error=f"http {resp.status_code}: {text}")

        try:
            data = resp.json() or {}
        except Exception:
            return SendResult(success=False, error="dispatch returned non-JSON body")

        if data.get("error"):
            return SendResult(success=False, error=str(data.get("error")))

        msg_id = str(data.get("worktool_message_id") or data.get("task_id") or "")
        return SendResult(success=True, message_id=msg_id)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        # WeCom has no typing indicator.
        return None

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": chat_id, "type": "group"}

    # ── Inbound ──────────────────────────────────────────────────────────

    async def _run_with_reconnect(self) -> None:
        backoff = 2.0
        while not self._stop.is_set():
            try:
                await self._run_once()
                backoff = 2.0  # reset after clean exit
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("qiweidoc: ws loop error: %s — reconnecting in %.1fs", e, backoff)
            if self._stop.is_set():
                break
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2.0, 60.0)

    async def _run_once(self) -> None:
        try:
            import websockets
        except ImportError:
            logger.error("qiweidoc: 'websockets' package not installed; pip install websockets")
            self._set_fatal_error(
                "missing_dep",
                "'websockets' package required for qiweidoc adapter",
                retryable=False,
            )
            await asyncio.sleep(30)
            return

        scheme = "wss" if self.base_url.startswith("https://") else "ws"
        host = self.base_url.split("://", 1)[1].rstrip("/")
        from urllib.parse import quote
        ws_url = f"{scheme}://{host}{self.ws_path}?token={quote(self.pat, safe='')}"
        logger.info("qiweidoc: dialing %s%s", host, self.ws_path)

        async with websockets.connect(
            ws_url,
            ping_interval=30,
            ping_timeout=20,
            close_timeout=5,
            max_size=2 * 1024 * 1024,
        ) as ws:
            self._ws = ws
            try:
                async for raw in ws:
                    if self._stop.is_set():
                        break
                    await self._handle_raw(raw)
            finally:
                self._ws = None

    async def _handle_raw(self, raw) -> None:
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except Exception:
                return
        try:
            payload = json.loads(raw)
        except Exception as e:
            logger.debug("qiweidoc: bad json frame: %s raw=%r", e, raw[:200])
            return

        t = payload.get("type")
        if t == "ready":
            self._self_userid = str(payload.get("userid") or "")
            logger.info(
                "qiweidoc: ws ready mode=%s userid=%s groups=%s",
                payload.get("mode"), self._self_userid, payload.get("groups"),
            )
            return
        if t == "error":
            logger.warning("qiweidoc: ws server error: %s", payload.get("msg"))
            return
        if t != "msg":
            logger.debug("qiweidoc: unknown ws frame type=%r", t)
            return

        data = payload.get("data") or {}
        await self._dispatch_msg(data)

    async def _dispatch_msg(self, data: Dict[str, Any]) -> None:
        if not self._message_handler:
            return

        chat_id = str(data.get("roomid") or "")
        if not chat_id:
            return  # DMs (direct) not supported in this MVP
        if self.allowed_groups and chat_id not in self.allowed_groups:
            return

        from_id = str(data.get("from") or "")
        from_name = str(data.get("from_name") or from_id or "")
        room_name = str(data.get("room_name") or chat_id)
        text = str(data.get("msg") or "")
        msg_id = str(data.get("msg_id") or "")
        msg_type = str(data.get("msg_type") or "")

        # Ignore our own sends (echoes from WorkTool may surface here)
        if self._self_userid and from_id == self._self_userid:
            return

        # 长文本回查：NOTIFY 载荷受 pg_notify 8KB 字节上限约束，触发器把正文截到
        # ~6000 字节。若内联 msg 已接近该上限，说明被截断，按 msg_id 回查完整正文，
        # 保证模型看到的是完整消息（短消息不触发，零额外开销）。
        if text and len(text.encode("utf-8")) >= 5800:
            full = await self._fetch_text(msg_id)
            if full:
                text = full

        media_urls: List[str] = []
        media_types: List[str] = []
        message_type = MessageType.TEXT

        # 媒体消息：msg 文本为空或是存储 hash，按类型拉取并落 core 缓存。
        if not text.strip():
            if msg_type in _PLACEHOLDER_MEDIA:
                # 语音/视频：占位不转写，让 bot 知道收到但不处理音视频内容。
                text = _PLACEHOLDER_MEDIA[msg_type]
            elif msg_type in _DOWNLOAD_MEDIA:
                media = await self._fetch_media(msg_id)
                if not media or not media.get("data"):
                    logger.info(
                        "qiweidoc: media %s msg_id=%s unavailable, skipping", msg_type, msg_id
                    )
                    return
                data_bytes = media["data"]
                ext = "." + (str(media.get("ext") or "").lstrip(".") or _DEFAULT_EXT[msg_type])
                mime = str(media.get("mime") or "")
                filename = str(media.get("filename") or "")
                try:
                    if msg_type in ("image", "emotion"):
                        path = cache_image_from_bytes(data_bytes, ext)
                        media_urls = [path]
                        media_types = [mime or "image/png"]
                        if msg_type == "emotion":
                            message_type = MessageType.STICKER
                            text = "[表情]"
                        else:
                            message_type = MessageType.PHOTO
                            text = "[图片]"
                    else:  # file
                        path = cache_document_from_bytes(data_bytes, filename or f"file{ext}")
                        media_urls = [path]
                        media_types = [mime or "application/octet-stream"]
                        message_type = MessageType.DOCUMENT
                        text = (f"[文件] {filename}").strip()
                except Exception as e:
                    logger.warning("qiweidoc: cache media failed (%s): %s", msg_type, e)
                    return
            else:
                # 空内容且非媒体（mixed/link/weapp 等）— 维持现状跳过。
                return

        if self.address_only:
            # WeCom @mention format isn't standardised in the WS payload, so we
            # do a best-effort substring check against our user_id and a few
            # common @ markers. Operators can disable this gate by leaving
            # QIWEIDOC_REPLY_ON_ADDRESS_ONLY unset.
            tag_candidates = [self._self_userid] if self._self_userid else []
            tag_candidates += ["@机器人", "@bot", "@agent"]
            if not any(tag and tag in text for tag in tag_candidates):
                return

        source = self.build_source(
            chat_id=chat_id,
            chat_name=room_name,
            chat_type="group",
            user_id=from_id,
            user_name=from_name,
            message_id=msg_id,
        )

        # Carry msg_id forward via reply_to_message_id so the agent's reply
        # path can pass it back as reply_to_msg_id (dedup key on the server).
        event = MessageEvent(
            text=text,
            message_type=message_type,
            source=source,
            raw_message=data,
            message_id=msg_id or str(int(time.time() * 1000)),
            media_urls=media_urls,
            media_types=media_types,
            reply_to_message_id=msg_id or None,
            timestamp=_parse_ts(data.get("msg_time")),
        )
        await self.handle_message(event)

    async def _fetch_media(self, msg_id: str) -> Optional[Dict[str, Any]]:
        """GET /api/ai/media for metadata, then download the file bytes.

        Returns the metadata dict with a ``data`` (bytes) key added, or None on
        failure. The server returns a relative ``/storage/...`` url for local
        storage and an absolute presigned url for cloud — both resolved here.
        """
        if not msg_id:
            return None
        try:
            import httpx
        except ImportError:
            logger.warning("qiweidoc: httpx not installed; cannot fetch media")
            return None

        headers = {"Authorization": f"Bearer {self.pat}"}
        meta_url = f"{self.base_url}/api/ai/media"
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.get(meta_url, params={"msg_id": msg_id}, headers=headers)
                if resp.status_code >= 400:
                    logger.warning(
                        "qiweidoc: media meta http %s: %s", resp.status_code, resp.text[:300]
                    )
                    return None
                body = resp.json() or {}
                # qiweidoc 标准响应 {status, data: {...}}；兼容直接返回。
                meta = body.get("data") if isinstance(body.get("data"), dict) else body
                url = str(meta.get("url") or "")
                if not url:
                    return None
                file_url = urljoin(self.base_url + "/", url)
                dl = await client.get(file_url, headers=headers)
                if dl.status_code >= 400:
                    logger.warning(
                        "qiweidoc: media download http %s for msg_id=%s", dl.status_code, msg_id
                    )
                    return None
                meta["data"] = dl.content
                return meta
        except Exception as e:
            logger.warning("qiweidoc: media fetch error for msg_id=%s: %s", msg_id, e)
            return None

    async def _fetch_text(self, msg_id: str) -> str:
        """GET /api/ai/message for the complete text body of a truncated message.

        NOTIFY payloads are capped at pg_notify's 8 KB limit, so long messages
        arrive trimmed; this re-fetches the full ``msg_content`` by id. Returns
        "" on any failure so the caller falls back to the truncated inline text.
        """
        if not msg_id:
            return ""
        try:
            import httpx
        except ImportError:
            logger.warning("qiweidoc: httpx not installed; cannot fetch full text")
            return ""

        headers = {"Authorization": f"Bearer {self.pat}"}
        url = f"{self.base_url}/api/ai/message"
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(url, params={"msg_id": msg_id}, headers=headers)
                if resp.status_code >= 400:
                    logger.warning(
                        "qiweidoc: message http %s for msg_id=%s", resp.status_code, msg_id
                    )
                    return ""
                body = resp.json() or {}
                meta = body.get("data") if isinstance(body.get("data"), dict) else body
                return str(meta.get("msg") or "")
        except Exception as e:
            logger.warning("qiweidoc: text fetch error for msg_id=%s: %s", msg_id, e)
            return ""


def _parse_ts(v) -> _dt.datetime:
    if v is None or v == "":
        return _dt.datetime.now()
    try:
        # msg_time arrives as string seconds-since-epoch
        return _dt.datetime.fromtimestamp(float(v))
    except (TypeError, ValueError):
        return _dt.datetime.now()


# ── Plugin hooks ─────────────────────────────────────────────────────────


def check_requirements() -> bool:
    return bool(os.getenv("QIWEIDOC_PAT"))


def validate_config(config) -> bool:
    extra = getattr(config, "extra", {}) or {}
    return bool(os.getenv("QIWEIDOC_PAT") or extra.get("pat"))


def is_connected(config) -> bool:
    return validate_config(config)


def _env_enablement() -> Optional[dict]:
    if not os.getenv("QIWEIDOC_PAT"):
        return None
    seed: Dict[str, Any] = {
        "base_url": os.getenv("QIWEIDOC_BASE_URL") or _DEFAULT_BASE_URL,
        "ws_path": os.getenv("QIWEIDOC_WS_PATH") or _DEFAULT_WS_PATH,
    }
    home = os.getenv("QIWEIDOC_HOME_CHAT_ID", "").strip()
    if home:
        seed["home_chat_id"] = home
        seed["home_channel"] = {"chat_id": home, "name": home}
    allowed = _csv_env("QIWEIDOC_ALLOWED_GROUPS")
    if allowed:
        seed["allowed_groups"] = allowed
    if os.getenv("QIWEIDOC_REPLY_ON_ADDRESS_ONLY"):
        seed["reply_on_address_only"] = _bool_env("QIWEIDOC_REPLY_ON_ADDRESS_ONLY", False)
    return seed


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Ephemeral REST POST for out-of-process cron delivery."""
    try:
        import httpx
    except ImportError:
        return {"error": "qiweidoc standalone send: httpx not installed"}

    extra = getattr(pconfig, "extra", {}) or {}
    cfg = _resolve_config(extra)
    if not cfg["pat"]:
        return {"error": "qiweidoc standalone send: QIWEIDOC_PAT must be configured"}
    if _is_internal_meta(message):
        return {"ok": True, "skipped": "internal meta"}
    target = chat_id or cfg["home_chat_id"]
    if not target:
        return {"error": "qiweidoc standalone send: chat_id missing and no QIWEIDOC_HOME_CHAT_ID set"}

    body = {"group_id": target, "content": message}
    headers = {
        "Authorization": f"Bearer {cfg['pat']}",
        "Content-Type": "application/json",
    }
    url = f"{cfg['base_url']}/api/ai/rpa/dispatch"
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(url, json=body, headers=headers)
    except Exception as e:
        return {"error": f"qiweidoc standalone send transport: {e}"}
    if resp.status_code >= 400:
        return {"error": f"qiweidoc standalone send http {resp.status_code}: {resp.text[:300]}"}
    try:
        data = resp.json() or {}
    except Exception:
        return {"error": "qiweidoc standalone send: non-JSON body"}
    if data.get("error"):
        return {"error": str(data["error"])}
    return {
        "success": True,
        "message_id": str(data.get("worktool_message_id") or data.get("task_id") or ""),
    }


def register(ctx):
    """Plugin entry point.

    Defensive against hermes-agent version drift: ``register_platform``
    forwards extra kwargs straight into the ``PlatformEntry`` dataclass, and
    older/newer hermes builds may not define every field this adapter sets
    (``env_enablement_fn`` / ``cron_deliver_env_var`` / ``standalone_sender_fn``
    are relatively new). Passing an unknown kwarg raises ``TypeError`` and the
    whole plugin fails to load. So we filter the optional kwargs down to the
    fields the *installed* ``PlatformEntry`` actually accepts, degrading
    gracefully (e.g. on a build without ``env_enablement_fn`` the channel is
    enabled via ``platforms.qiweidoc.enabled: true`` in config.yaml instead of
    auto-enabling from env). The core kwargs are always supported.
    """
    kwargs = dict(
        name="qiweidoc",
        label="QiweiDoc (WeCom)",
        adapter_factory=lambda cfg: QiweidocAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["QIWEIDOC_PAT"],
        install_hint="pip install websockets httpx",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="QIWEIDOC_HOME_CHAT_ID",
        standalone_sender_fn=_standalone_send,
        max_message_length=4000,
        emoji="🐝",
        pii_safe=False,
        allow_update_command=True,
        platform_hint=(
            "You are chatting via WeCom (企业微信) groups. Reply in the same "
            "language as the user (default 中文). Plain text only — markdown "
            "is not rendered. Keep replies concise; long answers are split "
            "by the RPA pipeline. Outbound msgs go through a per-hour rate "
            "limit and are deduped by reply_to_msg_id."
        ),
    )

    # Drop kwargs this hermes build's PlatformEntry doesn't understand, so a
    # version mismatch degrades instead of TypeError-ing the plugin out.
    # name/label/adapter_factory/check_fn/validate_config/required_env/
    # install_hint are explicit params of register_platform AND PlatformEntry
    # fields, so filtering by PlatformEntry fields keeps them.
    try:
        import dataclasses

        from gateway.platform_registry import PlatformEntry

        supported = {f.name for f in dataclasses.fields(PlatformEntry)}
        dropped = [k for k in list(kwargs) if k not in supported]
        for k in dropped:
            kwargs.pop(k, None)
        if dropped:
            logger.info(
                "qiweidoc: this hermes-agent build's PlatformEntry lacks %s; "
                "registering without them (version drift, non-fatal)",
                dropped,
            )
    except Exception as exc:  # pragma: no cover - introspection best-effort
        logger.debug("qiweidoc: PlatformEntry field introspection failed: %s", exc)

    ctx.register_platform(**kwargs)
