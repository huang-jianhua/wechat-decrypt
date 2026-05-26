#!/usr/bin/env python3
"""running-bot 推送测试。

默认行为：向 running-bot ingress 发送一条测试消息（无需微信在线）。
运行单元测试：python -m unittest scripts.test_running_bot_push
"""
import json
import hashlib
import base64
import os
import sys
import tempfile
import time
import unittest
import urllib.request
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import load_config
from running_bot_push import (
    RunningBotOutbox,
    RunningBotPusher,
    RunningBotPushConfig,
    build_ingress_payload,
    build_msg_data_from_db_row,
    init_pusher,
    schedule_push,
    stable_message_id,
)

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


class RunningBotReliableTests(unittest.TestCase):
    def test_same_second_rows_have_distinct_dedupe_keys(self):
        cfg = RunningBotPushConfig(
            group_whitelist=["跑团机器人测试"],
            decoded_image_dir=tempfile.gettempdir(),
        )
        contact_names = {"test@chatroom": "跑团机器人测试", "wxid_user": "淇淇"}
        keys = []
        for local_id in range(1, 9):
            msg_data = build_msg_data_from_db_row(
                username="test@chatroom",
                chat_display="跑团机器人测试",
                db_key="message/message_0.db",
                local_id=local_id,
                local_type=1,
                create_time=999,
                real_sender_id=1,
                message_content=f"第{local_id}条",
                ct_flag=0,
                name2id={1: "wxid_user"},
                contact_names=contact_names,
            )
            payload = build_ingress_payload(msg_data, cfg, contact_names)
            keys.append(payload["delivery"]["dedupe_key"])
        self.assertEqual(len(keys), 8)
        self.assertEqual(len(set(keys)), 8)

    def test_outbox_dedupes_repeated_enqueue(self):
        with tempfile.TemporaryDirectory() as td:
            cfg = RunningBotPushConfig(outbox_db=os.path.join(td, "outbox.db"))
            outbox = RunningBotOutbox(cfg.outbox_db, cfg)
            payload = dict(DEFAULT_PAYLOAD)
            payload["delivery"] = dict(DEFAULT_PAYLOAD["delivery"])
            self.assertEqual(outbox.enqueue(payload), "pending")
            self.assertEqual(outbox.enqueue(payload), "pending")
            conn = outbox._connect()
            try:
                count = conn.execute("SELECT COUNT(*) FROM running_bot_outbox").fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(count, 1)

    def test_outbox_failed_delivery_stays_retryable(self):
        with tempfile.TemporaryDirectory() as td:
            cfg = RunningBotPushConfig(outbox_db=os.path.join(td, "outbox.db"))
            outbox = RunningBotOutbox(cfg.outbox_db, cfg)
            payload = dict(DEFAULT_PAYLOAD)
            payload["delivery"] = dict(DEFAULT_PAYLOAD["delivery"])
            outbox.enqueue(payload)
            outbox._mark_failed(payload["delivery"]["dedupe_key"], "boom")
            conn = outbox._connect()
            try:
                status, attempts, last_error = conn.execute(
                    "SELECT status, attempts, last_error FROM running_bot_outbox WHERE dedupe_key=?",
                    (payload["delivery"]["dedupe_key"],),
                ).fetchone()
            finally:
                conn.close()
            self.assertEqual(status, "failed")
            self.assertEqual(attempts, 1)
            self.assertEqual(last_error, "boom")

    def test_outbox_delivered_skips_future_enqueue(self):
        with tempfile.TemporaryDirectory() as td:
            cfg = RunningBotPushConfig(outbox_db=os.path.join(td, "outbox.db"))
            outbox = RunningBotOutbox(cfg.outbox_db, cfg)
            payload = dict(DEFAULT_PAYLOAD)
            payload["delivery"] = dict(DEFAULT_PAYLOAD["delivery"])
            outbox.enqueue(payload)
            outbox._mark_delivered(payload["delivery"]["dedupe_key"])
            self.assertEqual(outbox.enqueue(payload), "delivered")

    def test_image_payload_uses_inline_base64_media(self):
        with tempfile.TemporaryDirectory() as td:
            raw = b"\xff\xd8\xff\xe0fake-jpeg-bytes"
            img_name = "run.jpg"
            with open(os.path.join(td, img_name), "wb") as f:
                f.write(raw)
            cfg = RunningBotPushConfig(
                group_whitelist=["跑团机器人测试"],
                decoded_image_dir=td,
            )
            contact_names = {"test@chatroom": "跑团机器人测试", "wxid_user": "淇淇"}
            msg_data = build_msg_data_from_db_row(
                username="test@chatroom",
                chat_display="跑团机器人测试",
                db_key="message/message_0.db",
                local_id=99,
                local_type=3,
                create_time=999,
                real_sender_id=1,
                message_content="",
                ct_flag=0,
                name2id={1: "wxid_user"},
                contact_names=contact_names,
            )
            msg_data["image_local_name"] = img_name
            payload = build_ingress_payload(msg_data, cfg, contact_names)
            self.assertEqual(payload["message"]["type"], "image")
            image = payload["message"]["images"][0]
            media = image["media"]
            self.assertEqual(media["transport"], "inline_base64")
            self.assertEqual(media["mime_type"], "image/jpeg")
            self.assertEqual(media["file_name"], img_name)
            self.assertEqual(media["size_bytes"], len(raw))
            self.assertEqual(media["sha256"], hashlib.sha256(raw).hexdigest())
            self.assertEqual(base64.b64decode(media["content_base64"]), raw)
            self.assertFalse(media["content_base64"].startswith("data:"))

    def test_message_id_normalizes_db_key_separators(self):
        id_backslash = stable_message_id(
            "u@chatroom", 100, 5, "sender", "text", "message\\message_0.db",
        )
        id_slash = stable_message_id(
            "u@chatroom", 200, 5, "sender", "text", "message/message_0.db",
        )
        self.assertEqual(id_backslash, id_slash)
        self.assertEqual(id_backslash, "u@chatroom:message/message_0.db:5")

    def test_message_id_stable_across_timestamps(self):
        cfg = RunningBotPushConfig(
            group_whitelist=["跑团机器人测试"],
            decoded_image_dir=tempfile.gettempdir(),
        )
        contact_names = {"test@chatroom": "跑团机器人测试", "wxid_user": "淇淇"}
        keys = []
        for create_time in (100, 999):
            msg_data = build_msg_data_from_db_row(
                username="test@chatroom",
                chat_display="跑团机器人测试",
                db_key="message/message_0.db",
                local_id=42,
                local_type=1,
                create_time=create_time,
                real_sender_id=1,
                message_content="同一条",
                ct_flag=0,
                name2id={1: "wxid_user"},
                contact_names=contact_names,
            )
            payload = build_ingress_payload(msg_data, cfg, contact_names)
            keys.append(payload["delivery"]["dedupe_key"])
        self.assertEqual(len(set(keys)), 1)

    @patch("running_bot_push._push_executor")
    def test_schedule_push_gate_blocks_non_scanner_in_reliable_mode(self, mock_executor):
        init_pusher({"running_bot_reliable_mode": True, "running_bot_outbox_db": ""}, {})
        schedule_push(MagicMock(), {"username": "x@chatroom"}, from_reliable_scanner=False)
        mock_executor.submit.assert_not_called()

    @patch("running_bot_push._push_executor")
    def test_schedule_push_gate_allows_scanner_in_reliable_mode(self, mock_executor):
        init_pusher({"running_bot_reliable_mode": True, "running_bot_outbox_db": ""}, {})
        schedule_push(MagicMock(), {"username": "x@chatroom"}, from_reliable_scanner=True)
        mock_executor.submit.assert_called_once()

    @patch("running_bot_push.push_with_retry")
    def test_try_push_skips_empty_image_in_reliable_mode(self, mock_push):
        with tempfile.TemporaryDirectory() as td:
            cfg = RunningBotPushConfig(
                group_whitelist=["跑团机器人测试"],
                decoded_image_dir=td,
                reliable_mode=True,
                outbox_db="",
            )
            contact_names = {"test@chatroom": "跑团机器人测试", "wxid_user": "淇淇"}
            pusher = RunningBotPusher(cfg, contact_names)
            msg_data = build_msg_data_from_db_row(
                username="test@chatroom",
                chat_display="跑团机器人测试",
                db_key="message/message_0.db",
                local_id=99,
                local_type=3,
                create_time=999,
                real_sender_id=1,
                message_content="",
                ct_flag=0,
                name2id={1: "wxid_user"},
                contact_names=contact_names,
            )
            msg_data["image_local_name"] = "missing.jpg"
            pusher.try_push(MagicMock(), msg_data)
            mock_push.assert_not_called()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--send":
        main()
    else:
        unittest.main()
