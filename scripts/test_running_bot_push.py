#!/usr/bin/env python3
"""向 running-bot ingress 发送一条测试消息（无需微信在线）。"""
import json
import os
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import load_config

DEFAULT_PAYLOAD = {
    "source": "wechat",
    "adapter": "wechat-decryptor-local",
    "event_id": "wechat:test-chatroom:999:test-msg",
    "trace_id": "test-trace-001",
    "timestamp": "2026-05-22T18:30:00+08:00",
    "chat": {"id": "跑团机器人测试", "name": "跑团机器人测试", "type": "group"},
    "sender": {
        "id": "wxid_test",
        "name": "淇淇",
        "display_name": "淇淇",
        "is_self": False,
        "is_admin": False,
    },
    "message": {
        "id": "test@chatroom:999:test-msg",
        "type": "text",
        "text": "本月排行榜",
        "raw_text": "本月排行榜",
        "mentions": [],
        "is_at_bot": False,
        "quote": None,
        "images": [],
    },
    "delivery": {
        "retry_count": 0,
        "dedupe_key": "wechat:test-chatroom:999:test-msg",
        "received_at": "2026-05-22T18:30:01+08:00",
    },
}


def main():
    cfg = load_config()
    url = cfg.get(
        "running_bot_ingress_url",
        "http://127.0.0.1:18765/api/ingress/wechat/message",
    )
    body = json.dumps(DEFAULT_PAYLOAD, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}, method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            print(f"OK status={resp.status}")
            print(resp.read().decode()[:500])
    except Exception as e:
        print(f"FAIL: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
