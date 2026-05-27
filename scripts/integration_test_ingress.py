#!/usr/bin/env python3
"""Running Service ingress 联调自测（4 条真群用例口径）。

用法:
  python scripts/integration_test_ingress.py
  python scripts/integration_test_ingress.py --include-ai
  python scripts/integration_test_ingress.py --host 192.168.1.10 --bot-name 跑团小助手
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import load_config
from running_bot_push import RunningBotPushConfig, build_ingress_payload, push_with_retry


def _base_msg_data(**overrides) -> dict:
    data = {
        'username': '53006947615@chatroom',
        'is_group': True,
        'chat': '跑团机器人测试',
        'sender_wxid': 'wxid_test_user',
        'sender_display_name': '联调测试员',
        'sender': '联调测试员',
        'timestamp': 1748246400,
        'local_id': 900001,
        'db_key': 'message/message_0.db',
        'msg_type_raw': 1,
        'content': '',
        'raw_content': '',
    }
    data.update(overrides)
    return data


def _cfg(args) -> RunningBotPushConfig:
    loaded = load_config()
    return RunningBotPushConfig(
        ingress_url=args.url or loaded.get(
            'running_bot_ingress_url',
            'http://127.0.0.1:18765/api/ingress/wechat/message',
        ),
        bot_name=args.bot_name or loaded.get('running_bot_name') or '跑团小助手',
        bot_aliases=list(loaded.get('running_bot_aliases') or []),
        timeout_seconds=float(loaded.get('running_bot_push_timeout_seconds', 10)),
        retry_count=0,
    )


def _print_result(case: str, payload: dict, status: str, http_status: int | None, err: str | None, resp: dict):
    running = resp.get('running_response') or {}
    print('=' * 72)
    print(f'CASE: {case}')
    print(f'trace_id={payload.get("trace_id")} event_id={payload.get("event_id")}')
    print(f'HTTP={http_status} push_status={status} duplicate={resp.get("duplicate")}')
    print(
        f'handler_name={running.get("handler_name") or resp.get("handler_name")} '
        f'execution_status={running.get("execution_status") or resp.get("execution_status")}'
    )
    if resp.get('error_code'):
        print(f'error_code={resp.get("error_code")}')
    if err:
        print(f'error_message={err}')
    reply = running.get('reply')
    if reply:
        print(f'reply={reply[:200]}')
    outbox = running.get('outbox_ids') or resp.get('outbox_ids')
    if outbox:
        print(f'outbox_ids={outbox}')
    if resp.get('image_job_id'):
        print(f'image_job_id={resp.get("image_job_id")} media_ref={resp.get("media_ref")}')


def _run_case(name: str, payload: dict, config: RunningBotPushConfig) -> bool:
    status, http_status, err, _, resp = push_with_retry(payload, config)
    _print_result(name, payload, status, http_status, err, resp)
    return status == 'success'


def build_cases(config: RunningBotPushConfig, contact_names: dict, include_ai: bool) -> list[tuple[str, dict]]:
    bot = config.bot_name or '跑团小助手'
    at_bot = f'@{bot}'

    case1 = build_ingress_payload(_base_msg_data(
        local_id=900101,
        content='联调测试员:\n打卡 1.5公里',
        raw_content='联调测试员:\n打卡 1.5公里',
        text='打卡 1.5公里',
        raw_text='打卡 1.5公里',
    ), config, contact_names)

    case2 = build_ingress_payload(_base_msg_data(
        local_id=900102,
        msg_type_raw=49,
        rich={
            'type': 'quote',
            'title': f'{at_bot} /撤销',
            'ref_name': bot,
            'ref_content': '已记录 联调测试员：1.50 公里，本周累计 1.50 公里。打卡ID：123456',
            'ref_type': '1',
            'ref_msgid': 'quoted_checkin_123456',
        },
        text=f'{at_bot} /撤销',
        raw_text=f'{at_bot} /撤销',
    ), config, contact_names)

    case3 = build_ingress_payload(_base_msg_data(
        local_id=900103,
        content=f'联调测试员:\n{at_bot} 今天天气真好，你觉得呢',
        raw_content=f'联调测试员:\n{at_bot} 今天天气真好，你觉得呢',
        text=f'{at_bot} 今天天气真好，你觉得呢',
        raw_text=f'{at_bot} 今天天气真好，你觉得呢',
    ), config, contact_names)

    case4 = build_ingress_payload(_base_msg_data(
        local_id=900104,
        msg_type_raw=49,
        rich={
            'type': 'quote',
            'title': f'{at_bot} /补卡 1.6公里',
            'ref_name': '联调测试员',
            'ref_content': '',
            'ref_type': '3',
            'ref_msgid': 'quoted_img_789',
            'ref_image_id': 'wx_img_quoted_img_789',
        },
        text=f'{at_bot} /补卡 1.6公里',
        raw_text=f'{at_bot} /补卡 1.6公里',
    ), config, contact_names)

    cases = [
        ('1.text_checkin', case1),
        ('2.quote_revoke', case2),
        ('4.quote_image_makeup', case4),
    ]
    if include_ai:
        cases.insert(2, ('3.ai_mention_chat', case3))
    return cases


def main():
    parser = argparse.ArgumentParser(description='Running Service ingress 联调自测')
    parser.add_argument('--url', help='ingress URL')
    parser.add_argument('--host', default='127.0.0.1', help='running-bot host')
    parser.add_argument('--port', type=int, default=18765)
    parser.add_argument('--bot-name', help='机器人昵称（@ 识别）')
    parser.add_argument('--group-name', default='跑团机器人测试', help='chat.name')
    parser.add_argument(
        '--include-ai',
        action='store_true',
        help='包含 @ 机器人 AI 闲聊用例（默认跳过，避免真群刷屏）',
    )
    args = parser.parse_args()
    if not args.url:
        args.url = f'http://{args.host}:{args.port}/api/ingress/wechat/message'

    config = _cfg(args)
    contact_names = {'53006947615@chatroom': args.group_name}
    cases = build_cases(config, contact_names, include_ai=args.include_ai)
    for _, payload in cases:
        payload['chat']['name'] = args.group_name

    ok = 0
    for name, payload in cases:
        if _run_case(name, payload, config):
            ok += 1
    print('=' * 72)
    print(f'PASS {ok}/{len(cases)}')
    sys.exit(0 if ok == len(cases) else 1)


if __name__ == '__main__':
    main()
