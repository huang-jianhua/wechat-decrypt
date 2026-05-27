# -*- coding: utf-8 -*-
"""Adapter from external WeChat ingress events to internal StandardMessage JSON."""

from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Optional

from message_bus.ingress_intent import default_bot_mention_names
from message_bus.media_store import LocalMediaStore, MediaIngressError


def is_external_wechat_ingress_payload(data: Dict[str, Any]) -> bool:
    """Return True when payload uses the public WeChat ingress schema."""

    message = data.get("message")
    chat = data.get("chat")
    return isinstance(message, dict) and isinstance(chat, dict) and (
        "id" in message or "text" in message or "raw_text" in message or "images" in message
    )


def external_wechat_ingress_to_standard_message(
    data: Dict[str, Any],
    *,
    media_store: Optional[LocalMediaStore] = None,
) -> Dict[str, Any]:
    """Convert public WeChat ingress schema into StandardMessage JSON.

    This keeps the external ingress project decoupled from running-bot's
    internal message-bus field names.
    """

    chat = _dict(data.get("chat"))
    sender = _dict(data.get("sender"))
    message = _dict(data.get("message"))
    delivery = _dict(data.get("delivery"))
    quote = message.get("quote") if isinstance(message.get("quote"), dict) else None

    event_id = _clean(data.get("event_id"))
    trace_id = _clean(data.get("trace_id"))
    message_id = _clean(message.get("id")) or event_id or _fallback_message_id(data)
    dedupe_key = _clean(delivery.get("dedupe_key")) or event_id or message_id

    chat_id = _clean(chat.get("id")) or _clean(chat.get("name"))
    chat_name = _clean(chat.get("name")) or chat_id
    sender_name = _clean(sender.get("display_name")) or _clean(sender.get("name")) or _clean(sender.get("id"))

    raw_type = (_clean(message.get("type")) or "text").lower()
    raw_images = [item for item in (message.get("images") or []) if isinstance(item, dict)]
    if len(raw_images) > 1 and any(_uses_inline_base64(item) for item in raw_images):
        raise MediaIngressError("unsupported_multi_image", 400)
    images = [
        _image_to_media_ref(
            item,
            media_store=media_store,
            source_adapter=_clean(data.get("adapter")),
            source_message_id=message_id,
        )
        for item in raw_images
    ]
    message_type = _standard_message_type(raw_type, images)

    text_content = _clean(message.get("text"))
    raw_text = _clean(message.get("raw_text"))
    if not text_content:
        text_content = raw_text

    mentions = _normalize_mentions(message.get("mentions"))
    has_at_bot = bool(message.get("is_at_bot")) or _mentions_include_bot(mentions)

    return {
        "event_id": event_id,
        "message_id": message_id,
        "channel": _clean(data.get("source")) or "wechat",
        "chat": {
            "chat_id": chat_id,
            "chat_name": chat_name,
            "chat_type": _clean(chat.get("type")) or "group",
            "channel_meta": _compact(
                {
                    "source_chat_id": chat.get("id"),
                    "source_chat_name": chat.get("name"),
                }
            ),
        },
        "sender": {
            "sender_id": _clean(sender.get("id")),
            "sender_name": sender_name,
            "sender_role": "admin" if bool(sender.get("is_admin")) else "member",
            "is_self": bool(sender.get("is_self")),
            "channel_meta": _compact(
                {
                    "source_sender_name": sender.get("name"),
                    "source_display_name": sender.get("display_name"),
                    "is_admin": sender.get("is_admin"),
                }
            ),
        },
        "message_type": message_type,
        "text": {
            "content": text_content,
            "mentions": mentions,
            "has_at_bot": has_at_bot,
        },
        "media_refs": images,
        "quote": _quote_to_standard_quote(
            quote,
            media_store=media_store,
            source_adapter=_clean(data.get("adapter")),
            source_message_id=message_id,
        ),
        "timestamps": {
            "raw_timestamp": data.get("timestamp"),
            "received_at": _clean(delivery.get("received_at")) or _clean(data.get("timestamp")),
        },
        "idempotency": {
            "message_id": message_id,
            "raw_message_ref": event_id or dedupe_key,
            "fingerprint": dedupe_key or message_id,
        },
        "raw_payload_ref": "",
        "metadata": _compact(
            {
                "adapter": data.get("adapter"),
                "trace_id": trace_id,
                "event_id": event_id,
                "delivery_retry_count": delivery.get("retry_count"),
                "external_schema": "wechat_ingress_v1",
            }
        ),
    }


def _standard_message_type(raw_type: str, media_refs: List[Dict[str, Any]]) -> str:
    if raw_type == "mixed":
        return "image" if media_refs else "text"
    if raw_type in {"text", "image", "system"}:
        return raw_type
    return "text"


def _image_to_media_ref(
    item: Dict[str, Any],
    *,
    media_store: Optional[LocalMediaStore] = None,
    source_adapter: str = "",
    source_message_id: str = "",
) -> Dict[str, Any]:
    media = item.get("media") if isinstance(item.get("media"), dict) else {}
    stored = None
    transport = _clean(media.get("transport")).lower()
    if transport == "inline_base64":
        if media_store is None:
            media_store = LocalMediaStore()
        stored = media_store.put_inline_base64(
            media,
            source_adapter=source_adapter,
            source_message_id=source_message_id,
        )

    mime_type = (stored or {}).get("mime_type") or item.get("mime_type") or media.get("mime_type")
    size_bytes = (stored or {}).get("size_bytes") or item.get("size_bytes") or media.get("size_bytes")
    sha256 = (stored or {}).get("sha256") or item.get("sha256") or media.get("sha256")
    path = (stored or {}).get("storage_path") or item.get("local_path")
    download_status = "stored" if stored else ("available" if item.get("local_path") or item.get("url") else "missing")
    return {
        "media_type": "image",
        "media_id": _clean(item.get("image_id")),
        "media_ref": (stored or {}).get("media_ref"),
        "path": path,
        "url": item.get("url"),
        "download_status": download_status,
        "mime_type": mime_type,
        "size_bytes": size_bytes,
        "channel_meta": _compact(
            {
                "sha256": sha256,
                "width": item.get("width"),
                "height": item.get("height"),
                "transport": transport or None,
                "source_image_id": item.get("image_id"),
                "storage_backend": (stored or {}).get("storage_backend"),
            }
        ),
    }


def _quote_to_standard_quote(
    quote: Optional[Dict[str, Any]],
    *,
    media_store: Optional[LocalMediaStore] = None,
    source_adapter: str = "",
    source_message_id: str = "",
) -> Dict[str, Any]:
    if not quote:
        return {"exists": False, "media_refs": [], "channel_meta": {}}
    quote_media = []
    image_id = _clean(quote.get("image_id"))
    quote_media_obj = quote.get("media") if isinstance(quote.get("media"), dict) else {}
    transport = _clean(quote_media_obj.get("transport")).lower()
    if transport == "inline_base64":
        if media_store is None:
            media_store = LocalMediaStore()
        stored = media_store.put_inline_base64(
            quote_media_obj,
            source_adapter=source_adapter,
            source_message_id=source_message_id or image_id,
        )
        quote_media.append(
            {
                "media_type": "image",
                "media_id": image_id or _clean(stored.get("media_ref")),
                "media_ref": stored.get("media_ref"),
                "path": stored.get("storage_path"),
                "download_status": "stored",
                "mime_type": stored.get("mime_type"),
                "size_bytes": stored.get("size_bytes"),
                "channel_meta": _compact({"sha256": stored.get("sha256"), "transport": "inline_base64"}),
            }
        )
    elif image_id:
        quote_media.append(
            {
                "media_type": "image",
                "media_id": image_id,
                "path": quote.get("local_path"),
                "url": quote.get("url"),
                "download_status": "available" if quote.get("local_path") or quote.get("url") else "missing",
                "mime_type": quote.get("mime_type"),
                "channel_meta": {},
            }
        )
    quote_text = (
        _clean(quote.get("text"))
        or _clean(quote.get("content"))
        or _clean(quote.get("body"))
        or _clean(quote.get("preview"))
    )
    return {
        "exists": True,
        "message_type": quote.get("type"),
        "sender_name": quote.get("sender_name"),
        "text_preview": quote_text,
        "media_refs": quote_media,
        "channel_meta": _compact({"message_id": quote.get("message_id")}),
    }


def _mentions_include_bot(mentions: List[str]) -> bool:
    bot_names = default_bot_mention_names()
    for item in mentions or []:
        candidate = str(item or "").strip().lstrip("@")
        if not candidate:
            continue
        if any(name in candidate or candidate in name for name in bot_names):
            return True
    return False


def _normalize_mentions(value: Any) -> List[str]:
    mentions = []
    for item in value or []:
        if isinstance(item, dict):
            candidate = _clean(item.get("name")) or _clean(item.get("id"))
        else:
            candidate = _clean(item)
        if candidate:
            mentions.append(candidate)
    return mentions


def _uses_inline_base64(item: Dict[str, Any]) -> bool:
    media = item.get("media") if isinstance(item.get("media"), dict) else {}
    return _clean(media.get("transport")).lower() == "inline_base64"


def _fallback_message_id(data: Dict[str, Any]) -> str:
    chat = _dict(data.get("chat"))
    sender = _dict(data.get("sender"))
    message = _dict(data.get("message"))
    seed = "|".join(
        [
            _clean(chat.get("id")) or _clean(chat.get("name")),
            _clean(data.get("timestamp")),
            _clean(sender.get("id")) or _clean(sender.get("name")),
            _clean(message.get("raw_text")) or _clean(message.get("text")),
        ]
    )
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]
    return f"wechat:{digest}"


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _compact(data: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in data.items() if value not in (None, "")}
