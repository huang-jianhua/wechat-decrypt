#!/usr/bin/env python3
import hashlib
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RES_DB = ROOT / "decrypted" / "_monitor_cache" / "message_message_resource.db"
CHAT = "53006947615@chatroom"
MSG_DB = ROOT / "decrypted" / "_monitor_cache" / "message_message_0.db"
TABLE = f"Msg_{hashlib.md5(CHAT.encode()).hexdigest()}"


def extract_md5_hex(packed):
    if not packed:
        return None
    if isinstance(packed, bytes):
        # try simple hex scan
        import re
        m = re.search(rb"[0-9a-fA-F]{32}", packed)
        return m.group(0).decode() if m else packed[:32].hex()
    return str(packed)[:32]


def main():
    msg_conn = sqlite3.connect(f"file:{MSG_DB}?mode=ro", uri=True)
    rows = msg_conn.execute(f"""
        SELECT local_id, create_time FROM "{TABLE}"
        WHERE local_id >= 1188 AND (local_type = 3 OR local_type % 4294967296 = 3)
        ORDER BY local_id
    """).fetchall()
    msg_conn.close()

    res_conn = sqlite3.connect(f"file:{RES_DB}?mode=ro", uri=True)
    print("local_id | create_time | packed_info preview / md5")
    for lid, ts in rows:
        row = res_conn.execute(
            "SELECT packed_info FROM MessageResourceInfo "
            "WHERE message_local_id=? AND message_create_time=?",
            (lid, ts),
        ).fetchone()
        info = row[0] if row else None
        preview = ""
        if isinstance(info, bytes):
            preview = info[:40].hex()
        print(lid, ts, preview[:64] if preview else "MISSING")
    res_conn.close()


if __name__ == "__main__":
    main()
