#!/usr/bin/env python3
"""Inspect running_bot_outbox.db for duplicate image pushes."""
import json
import re
import sqlite3
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "decrypted" / "_monitor_cache" / "running_bot_outbox.db"


def main():
    if not DB.exists():
        print(f"not found: {DB}")
        sys.exit(1)

    conn = sqlite3.connect(DB)
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )]
    print("tables:", tables)

    rows = conn.execute(
        "SELECT status, COUNT(*) FROM running_bot_outbox GROUP BY status"
    ).fetchall()
    print("outbox by status:", rows)

    all_rows = conn.execute("""
        SELECT dedupe_key, message_id, status, attempts, created_at, payload_json
        FROM running_bot_outbox
        ORDER BY created_at DESC
    """).fetchall()

    delivered_valid = []
    sha_counts = Counter()
    db_key_in_id = Counter()

    for dk, mid, st, att, ca, pj in all_rows:
        try:
            p = json.loads(pj)
        except json.JSONDecodeError:
            continue
        if p.get("message", {}).get("type") != "image":
            continue
        imgs = p.get("message", {}).get("images") or []
        media = (imgs[0].get("media") if imgs else {}) or {}
        b64 = media.get("content_base64") or ""
        if st != "delivered" or len(b64) < 100:
            continue
        sha = media.get("sha256") or ""
        sha_counts[sha] += 1
        msg_id = p.get("message", {}).get("id") or mid
        m = re.search(r"message/message_\d+\.db", msg_id + " " + dk)
        if m:
            db_key_in_id[m.group(0)] += 1
        delivered_valid.append({
            "time": datetime.fromtimestamp(ca).strftime("%Y-%m-%d %H:%M:%S"),
            "dedupe_key": dk,
            "message_id": msg_id,
            "sha256": sha[:16],
            "attempts": att,
        })

    print(f"\n=== delivered images with inline base64: {len(delivered_valid)} ===")
    for i, x in enumerate(delivered_valid, 1):
        print(f"{i:2}. {x['time']}  attempts={x['attempts']}  sha={x['sha256']}...")
        print(f"    id={x['message_id']}")

    print(f"\nunique sha256 (actual image content): {len(sha_counts)}")
    for sha, cnt in sha_counts.most_common():
        if cnt > 1:
            print(f"  DUPLICATE content x{cnt}: {sha[:16]}...")

    print("db_key mentions in message_id:", dict(db_key_in_id))

    if "running_bot_scan_state" in tables:
        ss = conn.execute(
            "SELECT username, db_key, last_create_time, last_local_id "
            "FROM running_bot_scan_state"
        ).fetchall()
        print(f"\nscan_state rows: {len(ss)}")
        for r in ss:
            print(" ", r)

    conn.close()


if __name__ == "__main__":
    main()
