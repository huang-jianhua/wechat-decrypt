#!/usr/bin/env python3
"""running-bot 推送测试。

默认行为：向 running-bot ingress 发送一条测试消息（无需微信在线）。
运行单元测试：python -m unittest scripts.test_running_bot_push
"""
import json
import sqlite3
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
    _build_quote,
    build_ingress_payload,
    build_msg_data_from_db_row,
    detect_at_bot,
    evaluate_ingress_response,
    init_pusher,
    parse_mentions,
    parse_appmsg_rich,
    schedule_push,
    stable_event_id,
    stable_message_id,
    stable_trace_id,
)

DEFAULT_PAYLOAD = {
    "source": "wechat",
    "adapter": "wechat-decryptor",
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

    def test_mentions_and_is_at_bot(self):
        cfg = RunningBotPushConfig(bot_name='跑团小助手', group_whitelist=['跑团机器人测试'])
        text = '@跑团小助手 /撤销'
        mentions = parse_mentions(text, cfg.bot_name, cfg.bot_aliases)
        self.assertEqual(len(mentions), 1)
        self.assertTrue(mentions[0]['is_bot'])
        self.assertTrue(detect_at_bot(text, cfg.bot_name, cfg.bot_aliases, mentions))

    def test_quote_text_payload(self):
        rich = {
            'type': 'quote',
            'title': '@跑团小助手 /撤销',
            'ref_name': '跑团小助手',
            'ref_content': '已记录 张三：1.50 公里，本周累计 1.50 公里。打卡ID：123',
            'ref_type': '1',
            'ref_msgid': 'quoted_msg_001',
        }
        quote = _build_quote(rich)
        self.assertEqual(quote['type'], 'text')
        self.assertEqual(quote['message_id'], 'quoted_msg_001')
        self.assertIn('已记录', quote['text'])

    def test_quote_image_payload(self):
        rich = {
            'type': 'quote',
            'title': '@跑团小助手 /补卡 1.6公里',
            'ref_name': '成员昵称',
            'ref_content': '',
            'ref_type': '3',
            'ref_msgid': 'quoted_img_001',
        }
        quote = _build_quote(rich)
        self.assertEqual(quote['type'], 'image')
        self.assertEqual(quote['sender_name'], '成员昵称')
        self.assertEqual(quote['image_id'], 'wx_img_quoted_img_001')

    def test_quote_reply_preserves_at_in_raw_text(self):
        cfg = RunningBotPushConfig(bot_name='跑团小助手', group_whitelist=['跑团机器人测试'])
        contact_names = {'test@chatroom': '跑团机器人测试', 'wxid_user': '淇淇'}
        msg_data = build_msg_data_from_db_row(
            username='test@chatroom',
            chat_display='跑团机器人测试',
            db_key='message/message_0.db',
            local_id=7,
            local_type=49,
            create_time=999,
            real_sender_id=1,
            message_content='<xml/>',
            ct_flag=0,
            name2id={1: 'wxid_user'},
            contact_names=contact_names,
        )
        msg_data['rich'] = {
            'type': 'quote',
            'title': '@跑团小助手 /撤销',
            'ref_name': '跑团小助手',
            'ref_content': '已记录 张三：1.50 公里。打卡ID：123',
            'ref_type': '1',
            'ref_msgid': 'q1',
        }
        payload = build_ingress_payload(msg_data, cfg, contact_names)
        self.assertIn('@跑团小助手', payload['message']['raw_text'])
        self.assertTrue(payload['message']['is_at_bot'])
        self.assertTrue(any(m.get('is_bot') for m in payload['message']['mentions']))

    def test_quote_image_includes_inline_base64_media(self):
        with tempfile.TemporaryDirectory() as td:
            md5 = 'a' * 32
            raw = b'\xff\xd8\xff\xe0fake-jpeg'
            img_name = f'{md5}.jpg'
            with open(os.path.join(td, img_name), 'wb') as f:
                f.write(raw)
            cfg = RunningBotPushConfig(decoded_image_dir=td)
            rich = {
                'type': 'quote',
                'title': '@跑团小助手 /补卡',
                'ref_name': '筋膜球',
                'ref_content': f'<msg><img md5="{md5}" /></msg>',
                'ref_type': '3',
                'ref_image_md5': md5,
                'ref_image_id': f'wx_img_{md5}',
            }
            quote = _build_quote(rich, username='test@chatroom', config=cfg)
            self.assertEqual(quote['type'], 'image')
            media = quote.get('media') or {}
            self.assertEqual(media.get('transport'), 'inline_base64')
            self.assertEqual(media.get('size_bytes'), len(raw))
            self.assertEqual(media.get('sha256'), hashlib.sha256(raw).hexdigest())
            self.assertEqual(base64.b64decode(media['content_base64']), raw)

    def test_parse_appmsg_rich_from_group_prefixed_xml(self):
        xml = '''hjhua_java:
<?xml version="1.0"?>
<msg>
\t<appmsg appid="" sdkver="0">
\t\t<title>@跑团小助手 /撤销</title>
\t\t<type>57</type>
\t\t<refermsg>
\t\t\t<displayname>跑团小助手</displayname>
\t\t\t<svrid>1631115889291477229</svrid>
\t\t\t<type>1</type>
\t\t\t<content>已记录 筋膜球：1.60 公里，本周累计 47.28 公里。打卡ID：367</content>
\t\t</refermsg>
\t</appmsg>
</msg>'''
        rich = parse_appmsg_rich(xml)
        self.assertIsNotNone(rich)
        self.assertEqual(rich['type'], 'quote')
        self.assertIn('/撤销', rich['title'])
        self.assertIn('打卡ID', rich['ref_content'])

    def test_build_payload_from_appmsg_xml_not_raw_xml(self):
        cfg = RunningBotPushConfig(bot_name='跑团小助手', group_whitelist=['跑团机器人测试'])
        contact_names = {'test@chatroom': '跑团机器人测试', 'wxid_admin': '筋膜球'}
        xml = '''hjhua_java:
<?xml version="1.0"?>
<msg><appmsg><title>@跑团小助手 /撤销</title><type>57</type>
<refermsg><displayname>跑团小助手</displayname><svrid>1631115889291477229</svrid>
<type>1</type><content>已记录 筋膜球：1.60 公里，本周累计 47.28 公里。打卡ID：367</content>
</refermsg></appmsg></msg>'''
        msg_data = build_msg_data_from_db_row(
            username='test@chatroom',
            chat_display='跑团机器人测试',
            db_key='message/message_0.db',
            local_id=1288,
            local_type=1,
            create_time=999,
            real_sender_id=1,
            message_content=xml,
            ct_flag=0,
            name2id={1: 'wxid_admin'},
            contact_names=contact_names,
        )
        payload = build_ingress_payload(msg_data, cfg, contact_names)
        msg = payload['message']
        self.assertNotIn('<?xml', msg['text'])
        self.assertIn('/撤销', msg['text'])
        self.assertIsNotNone(msg['quote'])
        self.assertIn('打卡ID', msg['quote']['text'])
        self.assertEqual(len(msg['mentions']), 1)
        self.assertTrue(msg['mentions'][0]['is_bot'])

    def test_quote_empty_text_ref_type_1_is_not_image(self):
        rich = {
            'type': 'quote',
            'title': '@跑团小助手 /撤销',
            'ref_name': '跑团小助手',
            'ref_content': '',
            'ref_type': '1',
        }
        quote = _build_quote(rich)
        self.assertEqual(quote['type'], 'text')
        self.assertEqual(quote['text'], '')

    def test_quote_text_strips_group_prefix_and_keeps_checkin_id(self):
        rich = {
            'type': 'quote',
            'title': '@跑团小助手 /撤销',
            'ref_name': '跑团小助手',
            'ref_content': '跑团小助手:\n已记录 筋膜球：1.60 公里，本周累计 47.28 公里。打卡ID：367',
            'ref_type': '1',
            'ref_msgid': 'q367',
        }
        quote = _build_quote(rich)
        self.assertEqual(quote['type'], 'text')
        self.assertIn('打卡ID', quote['text'])
        self.assertIn('已记录', quote['text'])

    def test_admin_revoke_ingress_payload_has_quote_and_at_bot(self):
        cfg = RunningBotPushConfig(bot_name='跑团小助手', group_whitelist=['跑团机器人测试'])
        contact_names = {'test@chatroom': '华龙大K战队', 'wxid_admin': '筋膜球'}
        msg_data = build_msg_data_from_db_row(
            username='test@chatroom',
            chat_display='华龙大K战队',
            db_key='message/message_0.db',
            local_id=88,
            local_type=49,
            create_time=999,
            real_sender_id=1,
            message_content='',
            ct_flag=0,
            name2id={1: 'wxid_admin'},
            contact_names=contact_names,
        )
        msg_data['rich'] = {
            'type': 'quote',
            'title': '@跑团小助手 /撤销',
            'ref_name': '跑团小助手',
            'ref_content': '已记录 筋膜球：1.60 公里，本周累计 47.28 公里。打卡ID：367',
            'ref_type': '1',
            'ref_msgid': 'quoted_367',
        }
        payload = build_ingress_payload(msg_data, cfg, contact_names)
        msg = payload['message']
        self.assertIn('/撤销', msg['text'])
        self.assertTrue(msg['is_at_bot'])
        self.assertTrue(any(m.get('is_bot') for m in msg['mentions']))
        self.assertIsNotNone(msg['quote'])
        self.assertIn('打卡ID', msg['quote']['text'])
        self.assertEqual(payload['trace_id'], payload['event_id'])

    def test_trace_id_stable_and_equals_event_id(self):
        cfg = RunningBotPushConfig(bot_name='跑团小助手', group_whitelist=['跑团机器人测试'])
        contact_names = {'test@chatroom': '跑团机器人测试', 'wxid_user': '淇淇'}
        msg_data = build_msg_data_from_db_row(
            username='test@chatroom',
            chat_display='跑团机器人测试',
            db_key='message/message_0.db',
            local_id=11,
            local_type=1,
            create_time=999,
            real_sender_id=1,
            message_content='hello',
            ct_flag=0,
            name2id={1: 'wxid_user'},
            contact_names=contact_names,
        )
        payload = build_ingress_payload(msg_data, cfg, contact_names)
        self.assertEqual(payload['trace_id'], payload['event_id'])
        self.assertEqual(payload['trace_id'], stable_trace_id(payload['event_id']))

    def test_evaluate_ingress_duplicate_is_success(self):
        ok, retryable, err = evaluate_ingress_response(200, {'duplicate': True, 'accepted': True})
        self.assertTrue(ok)
        self.assertFalse(retryable)
        self.assertIsNone(err)

    def test_evaluate_ingress_400_not_retryable(self):
        ok, retryable, err = evaluate_ingress_response(400, {'accepted': False, 'error_code': 'invalid_base64'})
        self.assertFalse(ok)
        self.assertFalse(retryable)

    def test_evaluate_ingress_ai_success(self):
        ok, retryable, err = evaluate_ingress_response(200, {
            'accepted': True,
            'running_response': {
                'handler_name': 'enqueue_ai_mention_chat',
                'outbox_ids': ['ob-1'],
            },
        })
        self.assertTrue(ok)
        self.assertFalse(retryable)

    def test_outbox_abandons_after_max_attempts(self):
        with tempfile.TemporaryDirectory() as td:
            cfg = RunningBotPushConfig(outbox_db=os.path.join(td, 'outbox.db'), outbox_max_attempts=3)
            outbox = RunningBotOutbox(cfg.outbox_db, cfg)
            payload = dict(DEFAULT_PAYLOAD)
            payload['delivery'] = dict(DEFAULT_PAYLOAD['delivery'])
            outbox.enqueue(payload)
            dk = payload['delivery']['dedupe_key']
            for _ in range(3):
                outbox._mark_failed(dk, 'boom')
            conn = outbox._connect()
            try:
                status, attempts = conn.execute(
                    'SELECT status, attempts FROM running_bot_outbox WHERE dedupe_key=?',
                    (dk,),
                ).fetchone()
            finally:
                conn.close()
            self.assertEqual(status, 'abandoned')
            self.assertEqual(attempts, 3)

    @patch("running_bot_push.push_with_retry")
    def test_try_push_skips_empty_image_in_reliable_mode(self, mock_push):
        mock_push.return_value = ('success', 200, None, 0, {})
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

    def test_decode_wcdb_text_plain_utf8_bytes(self):
        from running_bot_push import decode_wcdb_text
        text = 'hjhua_java:\n@跑团小助手 /补卡'
        self.assertEqual(decode_wcdb_text(text.encode('utf-8'), 0), text)

    def test_decode_wcdb_text_respects_ct_flag(self):
        from running_bot_push import decode_wcdb_text, _WCDB_ZSTD_MAGIC
        plain = b'not zstd payload'
        self.assertEqual(decode_wcdb_text(plain, 0), 'not zstd payload')
        self.assertEqual(decode_wcdb_text(plain, 4), 'not zstd payload')

    def test_config_keeps_ascii_image_aes_key(self):
        cfg = RunningBotPushConfig.from_cfg({'image_aes_key': '5668554677fa978e'})
        self.assertEqual(cfg.image_aes_key, '5668554677fa978e')

    def test_resolve_quoted_image_via_svrid_fallback(self):
        with tempfile.TemporaryDirectory() as td:
            quote_md5 = 'a939367c92ed' + '0' * 20
            file_md5 = '20f6cdc1a12f7868df6f43ff2608b110'
            raw = b'\xff\xd8\xff\xe0fake-jpeg'
            with open(os.path.join(td, f'{file_md5}.jpg'), 'wb') as f:
                f.write(raw)
            res_db = os.path.join(td, 'message_resource.db')
            conn = sqlite3.connect(res_db)
            conn.execute('''CREATE TABLE MessageResourceInfo (
                message_local_id INTEGER, message_create_time INTEGER,
                message_svr_id INTEGER, message_local_type INTEGER, packed_info BLOB
            )''')
            packed = b'\x12\x22\x0a\x20' + file_md5.encode('ascii')
            conn.execute(
                'INSERT INTO MessageResourceInfo VALUES (?,?,?,?,?)',
                (1305, 1779841763, 6325253997822665284, 3, packed),
            )
            conn.commit()
            conn.close()
            cfg = RunningBotPushConfig(decoded_image_dir=td)
            rich = {
                'type': 'quote',
                'title': '@跑团小助手 /补卡',
                'ref_type': '3',
                'ref_svrid': '6325253997822665284',
                'ref_image_md5': quote_md5,
            }
            from running_bot_push import resolve_quoted_image_local_name
            img = resolve_quoted_image_local_name(
                rich, '53006947615@chatroom', cfg, resource_db_path=res_db,
            )
            self.assertEqual(img, f'{file_md5}.jpg')

    def test_defer_quote_image_without_inline_media(self):
        cfg = RunningBotPushConfig(
            bot_name='跑团小助手',
            group_whitelist=['跑团机器人测试'],
            reliable_mode=True,
            decoded_image_dir=tempfile.mkdtemp(),
        )
        contact_names = {'test@chatroom': '跑团机器人测试', 'wxid_user': '筋膜球'}
        msg_data = build_msg_data_from_db_row(
            username='test@chatroom',
            chat_display='跑团机器人测试',
            db_key='message/message_0.db',
            local_id=1302,
            local_type=244813135921,
            create_time=999,
            real_sender_id=1,
            message_content='',
            ct_flag=0,
            name2id={1: 'wxid_user'},
            contact_names=contact_names,
        )
        msg_data['rich'] = {
            'type': 'quote',
            'title': '@跑团小助手 /补卡',
            'ref_name': '筋膜球',
            'ref_content': '<msg><img md5="81483e0d0c4482407dc80a06a9949378" /></msg>',
            'ref_type': '3',
            'ref_image_md5': '81483e0d0c4482407dc80a06a9949378',
            'ref_image_id': 'wx_img_81483e0d0c4482407dc80a06a9949378',
        }
        pusher = RunningBotPusher(cfg, contact_names)
        with patch('running_bot_push.push_with_retry') as mock_push:
            pusher.try_push(MagicMock(), msg_data)
            mock_push.assert_not_called()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--send":
        main()
    else:
        unittest.main()
