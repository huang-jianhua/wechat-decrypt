#!/usr/bin/env python3
"""导出指定 trace_id / local_id 的 ingress POST JSON，供 Running Service 对账。

用法:
  python scripts/export_ingress_evidence.py
  python scripts/export_ingress_evidence.py --trace-id 723ba51f-9489-45eb-bfdc-871af3791b32
  python scripts/export_ingress_evidence.py --local-id 1236
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DEFAULT_TRACE_IDS = [
    'integration-case3-bc4618c4-15c9-4e53-94c1-9d4a2e2e06b8',
    '723ba51f-9489-45eb-bfdc-871af3791b32',
    '487a3656-0d44-413e-9fc5-8d9f7f08976f',
    '83ba7a5e-c9ba-48d7-b559-86293843b0ae',
]

DEFAULT_LOCAL_IDS = ['1229', '1235', '1236']


def _redact_payload(payload: dict) -> dict:
    redacted = json.loads(json.dumps(payload, ensure_ascii=False))
    for img in (redacted.get('message') or {}).get('images') or []:
        media = img.get('media') or {}
        b64 = media.get('content_base64') or ''
        if b64:
            media['content_base64'] = f'<redacted len={len(b64)}>'
    return redacted


def _match(row_payload: dict, trace_id: str | None, local_id: str | None) -> bool:
    tid = row_payload.get('trace_id') or ''
    eid = row_payload.get('event_id') or ''
    mid = (row_payload.get('message') or {}).get('id') or ''
    if trace_id and (tid == trace_id or trace_id in eid or trace_id in tid):
        return True
    if local_id and f':{local_id}' in mid:
        return True
    return False


def main():
    parser = argparse.ArgumentParser(description='导出 ingress POST JSON 证据')
    parser.add_argument('--db', default=str(ROOT / 'decrypted' / '_monitor_cache' / 'running_bot_outbox.db'))
    parser.add_argument('--trace-id', action='append', dest='trace_ids')
    parser.add_argument('--local-id', action='append', dest='local_ids')
    parser.add_argument('--out-dir', default=str(ROOT / 'reports' / 'ingress_evidence'))
    args = parser.parse_args()

    trace_ids = args.trace_ids or DEFAULT_TRACE_IDS
    local_ids = args.local_ids or DEFAULT_LOCAL_IDS

    if not os.path.isfile(args.db):
        print(f'outbox not found: {args.db}', file=sys.stderr)
        print('note: integration script payloads are not stored in outbox.')
        sys.exit(1)

    os.makedirs(args.out_dir, exist_ok=True)
    conn = sqlite3.connect(args.db)
    rows = conn.execute(
        'SELECT dedupe_key, status, attempts, last_error, payload_json FROM running_bot_outbox ORDER BY created_at'
    ).fetchall()
    conn.close()

    exported = 0
    for dedupe_key, status, attempts, last_error, payload_json in rows:
        payload = json.loads(payload_json)
        hit = any(_match(payload, tid, None) for tid in trace_ids)
        hit = hit or any(_match(payload, None, lid) for lid in local_ids)
        if not hit:
            continue
        redacted = _redact_payload(payload)
        name = payload.get('trace_id') or dedupe_key.replace(':', '_')
        out_path = os.path.join(args.out_dir, f'{name}.json')
        record = {
            'outbox_status': status,
            'outbox_attempts': attempts,
            'outbox_last_error': last_error,
            'post_payload': redacted,
        }
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        print(f'exported: {out_path}')
        exported += 1

    print(f'done: {exported} file(s) -> {args.out_dir}')
    if exported == 0:
        sys.exit(1)


if __name__ == '__main__':
    main()
