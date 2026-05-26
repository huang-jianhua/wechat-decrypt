#!/usr/bin/env python3
import hashlib
import json
import sqlite3
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MSG_DB = ROOT / "decrypted" / "_monitor_cache" / "message_message_0.db"
OUTBOX = ROOT / "decrypted" / "_monitor_cache" / "running_bot_outbox.db"
CHAT = "53006947615@chatroom"
TABLE = f"Msg_{hashlib.md5(CHAT.encode()).hexdigest()}"


def main():
    print("=== message DB image rows (local_id >= 1188) ===")
    conn = sqlite3.connect(f"file:{MSG_DB}?mode=ro", uri=True)
    rows = conn.execute(f"""
        SELECT local_id, create_time, local_type
        FROM "{TABLE}"
        WHERE local_id >= 1188 AND (local_type = 3 OR local_type % 4294967296 = 3)
        ORDER BY local_id
    """).fetchall()
    print(f"count: {len(rows)}")
    for r in rows:
        print(r)
    conn.close()

    print("\n=== outbox deliveries 2026-05-26 08:57+ ===")
    conn = sqlite3.connect(OUTBOX)
    rows = conn.execute(
        "SELECT dedupe_key, message_id, payload_json, created_at "
        "FROM running_bot_outbox WHERE status='delivered' ORDER BY created_at"
    ).fetchall()
    batch = []
    for dk, mid, pj, ca in rows:
        t = datetime.fromtimestamp(ca)
        if not (t.year == 2026 and t.month == 5 and t.day == 26 and t.hour == 8 and t.minute >= 57):
            continue
        p = json.loads(pj)
        if p.get("message", {}).get("type") != "image":
            continue
        imgs = p.get("message", {}).get("images") or []
        sha = (imgs[0].get("media") or {}).get("sha256", "") if imgs else ""
        msg_id = p.get("message", {}).get("id", mid)
        batch.append((t.strftime("%H:%M:%S"), msg_id, sha[:16], dk))
    print(f"deliveries: {len(batch)}")
    for b in batch:
        print(f"  {b[0]}  id={b[1]}  sha={b[2]}")
    conn.close()


if __name__ == "__main__":
    main()
