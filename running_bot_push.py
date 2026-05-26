"""
将监听到的微信消息标准化后 POST 到本机 running-bot ingress。

仅调用: POST /api/ingress/wechat/message
不调用 Outbox，不实现发送或跑团业务逻辑。
"""
from __future__ import annotations

import hashlib
import base64
import json
import os
import re
import sqlite3
import threading
import time
import uuid
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta

try:
    import zstandard as zstd  # type: ignore[reportMissingImports]
except ImportError:
    zstd = None

_zstd_dctx = zstd.ZstdDecompressor() if zstd else None
_TZ_CN = timezone(timedelta(hours=8))

_SKIP_BASE_TYPES = {10000, 10002}


def _split_msg_type(t):
    try:
        t = int(t)
    except (TypeError, ValueError):
        return 0, 0
    if t > 0xFFFFFFFF:
        return t & 0xFFFFFFFF, t >> 32
    return t, 0


def _base_msg_type(t) -> int:
    return _split_msg_type(t)[0]


def _collapse_text(text: str) -> str:
    if not text:
        return ''
    return re.sub(r'\s+', ' ', text).strip()


def _iso_cn(ts: int | float | None = None) -> str:
    dt = datetime.fromtimestamp(ts or time.time(), tz=_TZ_CN)
    return dt.isoformat(timespec='seconds')


@dataclass
class RunningBotPushConfig:
    enabled: bool = True
    ingress_url: str = 'http://127.0.0.1:18765/api/ingress/wechat/message'
    timeout_seconds: float = 5.0
    retry_count: int = 2
    group_whitelist: list[str] = field(default_factory=lambda: ['跑团机器人测试'])
    user_whitelist: list[str] = field(default_factory=list)
    bot_name: str = ''
    bot_aliases: list[str] = field(default_factory=list)
    wechat_base_dir: str = ''
    decoded_image_dir: str = ''
    reliable_mode: bool = True
    outbox_db: str = ''
    outbox_batch_size: int = 20

    @classmethod
    def from_cfg(cls, cfg: dict) -> 'RunningBotPushConfig':
        return cls(
            enabled=bool(cfg.get('enable_running_bot_push', True)),
            ingress_url=str(cfg.get(
                'running_bot_ingress_url',
                'http://127.0.0.1:18765/api/ingress/wechat/message',
            )),
            timeout_seconds=float(cfg.get('running_bot_push_timeout_seconds', 5)),
            retry_count=int(cfg.get('running_bot_push_retry_count', 2)),
            group_whitelist=list(cfg.get('running_bot_group_whitelist') or ['跑团机器人测试']),
            user_whitelist=list(cfg.get('running_bot_user_whitelist') or []),
            bot_name=str(cfg.get('running_bot_name') or ''),
            bot_aliases=list(cfg.get('running_bot_aliases') or []),
            wechat_base_dir=str(cfg.get('wechat_base_dir') or ''),
            decoded_image_dir=str(cfg.get('decoded_image_dir') or ''),
            reliable_mode=bool(cfg.get('running_bot_reliable_mode', True)),
            outbox_db=str(cfg.get('running_bot_outbox_db') or ''),
            outbox_batch_size=int(cfg.get('running_bot_outbox_batch_size', 20)),
        )


def _whitelist_match(entry: str, chat_display: str, username: str) -> bool:
    needle = (entry or '').strip().lower()
    if not needle:
        return False
    chat_l = (chat_display or '').lower()
    user_l = (username or '').lower()
    return needle in chat_l or needle in user_l or chat_l == needle or user_l == needle


def should_push(config: RunningBotPushConfig, username: str, chat_display: str, is_group: bool) -> bool:
    if not config.enabled:
        return False
    if is_group:
        wl = config.group_whitelist
        if not wl:
            return False
        return any(_whitelist_match(e, chat_display, username) for e in wl)
    wl = config.user_whitelist
    if not wl:
        return False
    return any(_whitelist_match(e, chat_display, username) for e in wl)


def _normalize_db_key(db_key: str) -> str:
    if not db_key:
        return ''
    return db_key.replace('\\', '/').strip()


def stable_message_id(
    username: str,
    timestamp: int,
    local_id: int | None,
    sender_key: str,
    raw_text: str,
    source_key: str = '',
) -> str:
    if local_id is not None:
        norm_key = _normalize_db_key(source_key)
        if norm_key:
            return f'{username}:{norm_key}:{local_id}'
        return f'{username}:{timestamp}:{local_id}'
    digest = hashlib.sha256((raw_text or '').encode('utf-8', errors='replace')).hexdigest()[:16]
    sender_part = sender_key or 'unknown'
    return f'{username}:{timestamp}:{sender_part}:{digest}'


def stable_event_id(chat_id: str, message_id: str) -> str:
    return f'wechat:{chat_id}:{message_id}'


def detect_at_bot(raw_text: str, bot_name: str, aliases: list[str]) -> bool:
    text = raw_text or ''
    names = []
    if bot_name:
        names.append(bot_name)
    names.extend(aliases or [])
    for name in names:
        n = (name or '').strip()
        if not n:
            continue
        if re.search(r'@' + re.escape(n) + r'(?:\s|$|[\u200b\u2005])', text, re.IGNORECASE):
            return True
        if f'@{n}' in text:
            return True
    return False


def _get_self_username(wechat_base_dir: str, contact_names: dict) -> str:
    if not wechat_base_dir:
        return ''
    account_dir = os.path.basename(wechat_base_dir)
    candidates = [account_dir]
    m = re.fullmatch(r'(.+)_([0-9a-fA-F]{4,})', account_dir)
    if m:
        candidates.insert(0, m.group(1))
    for c in candidates:
        if c and c in contact_names:
            return c
    return ''


def _ingress_message_type(base_type: int, rich: dict | None) -> str:
    if base_type == 3:
        return 'image'
    return 'text'


def _clean_group_text(content: str, is_group: bool) -> tuple[str, str]:
    raw = content or ''
    text = raw
    if is_group and ':\n' in text:
        text = text.split(':\n', 1)[1]
    cleaned = _collapse_text(text)
    return cleaned, raw


def _build_quote(rich: dict | None) -> dict | None:
    if not rich or rich.get('type') != 'quote':
        return None
    return {
        'message_id': None,
        'sender_name': rich.get('ref_name') or '',
        'text': rich.get('ref_content') or '',
        'type': 'text',
        'image_id': None,
    }


def _build_images(msg_data: dict, decoded_image_dir: str) -> list[dict]:
    image_url = msg_data.get('image_url') or ''
    img_name = ''
    if image_url.startswith('/img/'):
        img_name = image_url[5:]
    elif image_url:
        img_name = os.path.basename(image_url)
    if not img_name and msg_data.get('image_local_name'):
        img_name = msg_data['image_local_name']
    if not img_name:
        return []
    local_path = os.path.join(decoded_image_dir, img_name)
    if not os.path.isabs(local_path):
        local_path = os.path.abspath(local_path)
    if not os.path.isfile(local_path):
        return []
    image_id = os.path.splitext(img_name)[0]
    ext = os.path.splitext(img_name)[1].lower()
    mime = 'image/jpeg'
    if ext == '.png':
        mime = 'image/png'
    elif ext == '.webp':
        mime = 'image/webp'
    elif ext not in ('.jpg', '.jpeg'):
        return []
    with open(local_path, 'rb') as f:
        raw_bytes = f.read()
    digest = hashlib.sha256(raw_bytes).hexdigest()
    return [{
        'image_id': image_id,
        'media': {
            'transport': 'inline_base64',
            'content_base64': base64.b64encode(raw_bytes).decode('ascii'),
            'mime_type': mime,
            'file_name': os.path.basename(local_path),
            'size_bytes': len(raw_bytes),
            'sha256': digest,
        },
    }]


def build_ingress_payload(
    msg_data: dict,
    config: RunningBotPushConfig,
    contact_names: dict,
) -> dict:
    username = msg_data.get('username') or ''
    is_group = bool(msg_data.get('is_group'))
    chat_display = msg_data.get('chat') or contact_names.get(username, username)

    sender_wxid = msg_data.get('sender_wxid') or ''
    sender_display = msg_data.get('sender_display_name') or msg_data.get('sender') or ''
    if is_group:
        sender_id = sender_wxid or sender_display or 'unknown'
        sender_name = contact_names.get(sender_wxid, sender_display) if sender_wxid else sender_display
    else:
        sender_id = username
        sender_name = chat_display
        sender_display = chat_display

    self_wxid = _get_self_username(config.wechat_base_dir, contact_names)
    is_self = bool(self_wxid and (
        (is_group and sender_wxid == self_wxid) or
        (not is_group and username == self_wxid)
    ))

    content = msg_data.get('raw_content') or msg_data.get('content') or ''
    text, raw_text = _clean_group_text(content, is_group)
    if msg_data.get('raw_text'):
        raw_text = msg_data['raw_text']
    if msg_data.get('text'):
        text = msg_data['text']

    rich = msg_data.get('rich') or msg_data.get('rich_content')
    if rich and rich.get('type') == 'quote':
        reply_text = rich.get('title') or text
        text = _collapse_text(reply_text)
        if not raw_text:
            raw_text = text

    local_id = msg_data.get('local_id')
    message_id = stable_message_id(
        username, int(msg_data.get('timestamp', 0)),
        local_id, sender_id, raw_text or text,
        msg_data.get('db_key') or msg_data.get('source_db_key') or '',
    )
    chat_id = username
    event_id = stable_event_id(chat_id, message_id)
    trace_id = msg_data.get('_push_trace_id') or str(uuid.uuid4())

    base_type = _base_msg_type(msg_data.get('msg_type_raw', 0))
    ingress_type = _ingress_message_type(base_type, rich)
    quote = _build_quote(rich)
    images = _build_images(msg_data, config.decoded_image_dir) if ingress_type == 'image' else []

    return {
        'source': 'wechat',
        'adapter': 'wechat-decryptor-local',
        'event_id': event_id,
        'trace_id': trace_id,
        'timestamp': _iso_cn(msg_data.get('timestamp')),
        'chat': {
            'id': chat_id,
            'name': chat_display,
            'type': 'group' if is_group else 'private',
        },
        'sender': {
            'id': sender_id,
            'name': sender_name or sender_display,
            'display_name': sender_display or sender_name,
            'is_self': is_self,
            'is_admin': False,
        },
        'message': {
            'id': message_id,
            'type': ingress_type,
            'text': text,
            'raw_text': raw_text or text,
            'mentions': [],
            'is_at_bot': detect_at_bot(raw_text or text, config.bot_name, config.bot_aliases),
            'quote': quote,
            'images': images,
        },
        'delivery': {
            'retry_count': 0,
            'dedupe_key': event_id,
            'received_at': _iso_cn(),
        },
    }


def decode_message_content(message_content, ct_flag) -> str:
    if isinstance(message_content, bytes) and ct_flag == 4 and _zstd_dctx:
        try:
            return _zstd_dctx.decompress(message_content).decode('utf-8', errors='replace')
        except Exception:
            return message_content.decode('utf-8', errors='replace')
    if isinstance(message_content, bytes):
        return message_content.decode('utf-8', errors='replace')
    return message_content or ''


def build_msg_data_from_db_row(
    *,
    username: str,
    chat_display: str,
    db_key: str,
    local_id: int,
    local_type: int,
    create_time: int,
    real_sender_id,
    message_content,
    ct_flag,
    name2id: dict | None = None,
    contact_names: dict | None = None,
) -> dict:
    """Build monitor-style msg_data directly from a message DB row."""
    contact_names = contact_names or {}
    name2id = name2id or {}
    is_group = '@chatroom' in username
    sender_wxid = ''
    sender_display = ''
    if is_group and real_sender_id:
        sender_wxid = name2id.get(real_sender_id, '')
        sender_display = contact_names.get(sender_wxid, sender_wxid) if sender_wxid else ''

    raw_content = decode_message_content(message_content, ct_flag)
    content = raw_content
    if is_group and ':\n' in content:
        content = content.split(':\n', 1)[1]

    base_type = _base_msg_type(local_type)
    return {
        'time': datetime.fromtimestamp(create_time).strftime('%H:%M:%S'),
        'timestamp': int(create_time),
        'chat': chat_display or contact_names.get(username, username),
        'username': username,
        'is_group': is_group,
        'sender': sender_display,
        'sender_wxid': sender_wxid,
        'sender_display_name': sender_display,
        'msg_type_raw': int(local_type or 0),
        'content': content,
        'raw_content': raw_content,
        'local_id': int(local_id),
        'db_key': db_key,
        'source_db_key': db_key,
        'type': '图片' if base_type == 3 else '文本',
    }


def enrich_message_from_db(monitor, msg_data: dict) -> None:
    """从 message DB 补充 local_id、raw_text、rich（尽力而为）。"""
    username = msg_data.get('username')
    timestamp = msg_data.get('timestamp')
    if not username or not timestamp or not monitor.db_cache:
        return

    msg_type_raw = msg_data.get('msg_type_raw', 0)
    db_keys = monitor.username_db_map.get(username, [])
    if not db_keys:
        return

    table_name = f'Msg_{hashlib.md5(username.encode()).hexdigest()}'
    for db_key in db_keys:
        dec_path = monitor.db_cache.get(db_key)
        if not dec_path:
            continue
        try:
            conn = sqlite3.connect(f'file:{dec_path}?mode=ro', uri=True)
            row = conn.execute(f'''
                SELECT local_id, local_type, message_content, WCDB_CT_message_content,
                       real_sender_id
                FROM "{table_name}"
                WHERE create_time = ?
                ORDER BY local_id DESC LIMIT 1
            ''', (timestamp,)).fetchone()
            if not row:
                row = conn.execute(f'''
                    SELECT local_id, local_type, message_content, WCDB_CT_message_content,
                           real_sender_id
                    FROM "{table_name}"
                    WHERE ABS(create_time - ?) <= 3
                    ORDER BY ABS(create_time - ?) LIMIT 1
                ''', (timestamp, timestamp)).fetchone()
            conn.close()
            if not row:
                continue

            local_id, local_type, mc, ct_flag, real_sender_id = row
            msg_data['local_id'] = local_id
            msg_data['db_key'] = db_key
            msg_data['source_db_key'] = db_key
            if local_type:
                msg_data['msg_type_raw'] = local_type

            if isinstance(mc, bytes) and ct_flag == 4 and _zstd_dctx:
                try:
                    mc = _zstd_dctx.decompress(mc).decode('utf-8', errors='replace')
                except Exception:
                    mc = mc.decode('utf-8', errors='replace') if isinstance(mc, bytes) else ''
            elif isinstance(mc, bytes):
                mc = mc.decode('utf-8', errors='replace')

            if mc:
                msg_data['raw_content'] = mc
                if msg_data.get('is_group') and ':\n' in mc:
                    msg_data['content'] = mc.split(':\n', 1)[1]
                else:
                    msg_data['content'] = mc

            if real_sender_id and msg_data.get('is_group'):
                id_map = _load_name2id(dec_path)
                sender_wxid = id_map.get(real_sender_id, '')
                if sender_wxid:
                    msg_data['sender_wxid'] = sender_wxid

            base = _base_msg_type(local_type or msg_type_raw)
            if base == 49 and not msg_data.get('rich'):
                rich = monitor._parse_rich_content(username, timestamp, 49)
                if rich:
                    msg_data['rich'] = rich
            break
        except Exception:
            continue


def _load_name2id(db_path: str) -> dict:
    id_to_username = {}
    try:
        conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
        rows = conn.execute('SELECT rowid, user_name FROM Name2Id').fetchall()
        conn.close()
        for rowid, user_name in rows:
            if user_name:
                id_to_username[rowid] = user_name
    except sqlite3.Error:
        pass
    return id_to_username


def push_with_retry(
    payload: dict,
    config: RunningBotPushConfig,
) -> tuple[str, int | None, str | None, int]:
    body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    max_attempts = max(1, config.retry_count + 1)
    last_error = None
    response_status = None

    for attempt in range(max_attempts):
        try:
            req = urllib.request.Request(
                config.ingress_url, data=body, headers=headers, method='POST',
            )
            with urllib.request.urlopen(req, timeout=config.timeout_seconds) as resp:
                response_status = resp.status
                payload['delivery']['retry_count'] = attempt
                return 'success', response_status, None, attempt
        except urllib.error.HTTPError as e:
            response_status = e.code
            last_error = f'HTTP {e.code}: {e.reason}'
        except Exception as e:
            last_error = str(e)
        if attempt < max_attempts - 1:
            time.sleep(0.5 * (attempt + 1))

    payload['delivery']['retry_count'] = max_attempts - 1
    return 'failed', response_status, last_error, max_attempts - 1


def log_push(
    trace_id: str,
    event_id: str,
    chat_id: str,
    chat_name: str,
    sender_name: str,
    message_id: str,
    message_type: str,
    push_status: str,
    response_status: int | None = None,
    error_message: str | None = None,
    skipped: bool = False,
):
    parts = [
        '[running-bot-push]',
        f'trace_id={trace_id}',
        f'event_id={event_id}',
        f'chat_id={chat_id}',
        f'chat_name={chat_name}',
        f'sender_name={sender_name}',
        f'message_id={message_id}',
        f'message_type={message_type}',
        f'push_status={push_status}',
    ]
    if skipped:
        parts.append('skipped=dedupe')
    if response_status is not None:
        parts.append(f'response_status={response_status}')
    if error_message:
        parts.append(f'error_message={error_message}')
    print(' '.join(parts), flush=True)


class RunningBotOutbox:
    def __init__(self, db_path: str, config: RunningBotPushConfig):
        self.db_path = db_path
        self.config = config
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._worker: threading.Thread | None = None
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()

    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout=30000')
        return conn

    def _init_db(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS running_bot_outbox (
                      dedupe_key TEXT PRIMARY KEY,
                      message_id TEXT NOT NULL,
                      chat_id TEXT NOT NULL,
                      payload_json TEXT NOT NULL,
                      status TEXT NOT NULL,
                      attempts INTEGER NOT NULL DEFAULT 0,
                      next_retry_at REAL NOT NULL DEFAULT 0,
                      last_error TEXT,
                      created_at REAL NOT NULL,
                      updated_at REAL NOT NULL
                    )
                ''')
                conn.execute(
                    "UPDATE running_bot_outbox SET status='pending', updated_at=? "
                    "WHERE status='delivering'",
                    (time.time(),),
                )
                conn.commit()
            finally:
                conn.close()

    def enqueue(self, payload: dict) -> str:
        dedupe_key = payload['delivery']['dedupe_key']
        now = time.time()
        payload_json = json.dumps(payload, ensure_ascii=False)
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    'SELECT status FROM running_bot_outbox WHERE dedupe_key=?',
                    (dedupe_key,),
                ).fetchone()
                if row and row[0] == 'delivered':
                    return 'delivered'
                if row:
                    conn.execute('''
                        UPDATE running_bot_outbox
                        SET payload_json=?, status=CASE WHEN status='delivering' THEN 'pending' ELSE status END,
                            updated_at=?
                        WHERE dedupe_key=?
                    ''', (payload_json, now, dedupe_key))
                    conn.commit()
                    return row[0]
                conn.execute('''
                    INSERT INTO running_bot_outbox
                    (dedupe_key, message_id, chat_id, payload_json, status, attempts,
                     next_retry_at, last_error, created_at, updated_at)
                    VALUES (?, ?, ?, ?, 'pending', 0, 0, NULL, ?, ?)
                ''', (
                    dedupe_key, payload['message']['id'], payload['chat']['id'],
                    payload_json, now, now,
                ))
                conn.commit()
                return 'pending'
            finally:
                conn.close()

    def start(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        self._worker = threading.Thread(target=self._run, name='rb-outbox', daemon=True)
        self._worker.start()

    def _claim_batch(self) -> list[tuple[str, str]]:
        now = time.time()
        with self._lock:
            conn = self._connect()
            try:
                rows = conn.execute('''
                    SELECT dedupe_key, payload_json
                    FROM running_bot_outbox
                    WHERE status IN ('pending', 'failed') AND next_retry_at <= ?
                    ORDER BY created_at ASC
                    LIMIT ?
                ''', (now, self.config.outbox_batch_size)).fetchall()
                keys = [r[0] for r in rows]
                if keys:
                    conn.executemany(
                        "UPDATE running_bot_outbox SET status='delivering', updated_at=? WHERE dedupe_key=?",
                        [(now, k) for k in keys],
                    )
                    conn.commit()
                return rows
            finally:
                conn.close()

    def _mark_delivered(self, dedupe_key: str) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "UPDATE running_bot_outbox SET status='delivered', updated_at=?, last_error=NULL "
                    "WHERE dedupe_key=?",
                    (time.time(), dedupe_key),
                )
                conn.commit()
            finally:
                conn.close()

    def _mark_failed(self, dedupe_key: str, error_message: str | None) -> None:
        now = time.time()
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    'SELECT attempts FROM running_bot_outbox WHERE dedupe_key=?',
                    (dedupe_key,),
                ).fetchone()
                attempts = (row[0] if row else 0) + 1
                delay = min(300, 5 * (2 ** min(attempts - 1, 6)))
                conn.execute('''
                    UPDATE running_bot_outbox
                    SET status='failed', attempts=?, next_retry_at=?, last_error=?, updated_at=?
                    WHERE dedupe_key=?
                ''', (attempts, now + delay, error_message or '', now, dedupe_key))
                conn.commit()
            finally:
                conn.close()

    def _run(self) -> None:
        while not self._stop.is_set():
            rows = self._claim_batch()
            if not rows:
                self._stop.wait(1.0)
                continue
            for dedupe_key, payload_json in rows:
                try:
                    payload = json.loads(payload_json)
                except json.JSONDecodeError as e:
                    self._mark_failed(dedupe_key, f'payload decode failed: {e}')
                    continue
                push_status, response_status, error_message, _ = push_with_retry(payload, self.config)
                if push_status == 'success':
                    self._mark_delivered(dedupe_key)
                else:
                    self._mark_failed(dedupe_key, error_message)
                log_push(
                    payload.get('trace_id', ''), payload.get('event_id', dedupe_key),
                    payload.get('chat', {}).get('id', ''), payload.get('chat', {}).get('name', ''),
                    payload.get('sender', {}).get('name', ''), payload.get('message', {}).get('id', ''),
                    payload.get('message', {}).get('type', ''), push_status,
                    response_status=response_status,
                    error_message=error_message,
                )


class RunningBotPusher:
    def __init__(self, config: RunningBotPushConfig, contact_names: dict):
        self.config = config
        self.contact_names = contact_names
        self._pushed: set[str] = set()
        self._lock = threading.Lock()
        self.outbox = RunningBotOutbox(config.outbox_db, config) if config.reliable_mode and config.outbox_db else None
        if self.outbox:
            self.outbox.start()

    def _already_pushed(self, dedupe_key: str) -> bool:
        with self._lock:
            if dedupe_key in self._pushed:
                return True
            self._pushed.add(dedupe_key)
            if len(self._pushed) > 5000:
                self._pushed = set(list(self._pushed)[-2500:])
            return False

    def try_push(self, monitor, msg_data: dict, *, partial: bool = False) -> None:
        if not self.config.enabled:
            return

        username = msg_data.get('username', '')
        chat_display = msg_data.get('chat', '')
        is_group = bool(msg_data.get('is_group'))

        if not should_push(self.config, username, chat_display, is_group):
            return

        base_type = _base_msg_type(msg_data.get('msg_type_raw', 0))
        if base_type in _SKIP_BASE_TYPES:
            return

        if msg_data.get('local_id') is None:
            try:
                enrich_message_from_db(monitor, msg_data)
            except Exception as e:
                print(f'  [running-bot-push] enrich 失败: {e}', flush=True)

        payload = build_ingress_payload(msg_data, self.config, self.contact_names)
        ingress_type = payload['message']['type']
        images = payload['message']['images']
        if (
            self.config.reliable_mode
            and ingress_type == 'image'
            and not images
            and not msg_data.get('_allow_empty_image')
        ):
            return
        dedupe_key = payload['delivery']['dedupe_key']
        if self.outbox:
            status = self.outbox.enqueue(payload)
            if status == 'delivered':
                log_push(
                    payload['trace_id'], payload['event_id'],
                    payload['chat']['id'], payload['chat']['name'],
                    payload['sender']['name'], payload['message']['id'],
                    payload['message']['type'], 'skipped',
                    skipped=True,
                )
            return

        if self._already_pushed(dedupe_key):
            log_push(
                payload['trace_id'], payload['event_id'],
                payload['chat']['id'], payload['chat']['name'],
                payload['sender']['name'], payload['message']['id'],
                payload['message']['type'], 'skipped',
                skipped=True,
            )
            return

        msg_data['_push_trace_id'] = payload['trace_id']
        push_status, response_status, error_message, _ = push_with_retry(payload, self.config)
        if partial and push_status == 'success':
            push_status = 'partial'

        log_push(
            payload['trace_id'], payload['event_id'],
            payload['chat']['id'], payload['chat']['name'],
            payload['sender']['name'], payload['message']['id'],
            payload['message']['type'], push_status,
            response_status=response_status,
            error_message=error_message,
        )


_pusher: RunningBotPusher | None = None
_pusher_lock = threading.Lock()


def init_pusher(cfg: dict, contact_names: dict) -> RunningBotPusher | None:
    global _pusher
    config = RunningBotPushConfig.from_cfg(cfg)
    with _pusher_lock:
        _pusher = RunningBotPusher(config, contact_names)
    if config.enabled:
        print(
            f'[running-bot-push] 已启用 → {config.ingress_url} '
            f'群白名单={config.group_whitelist} 用户白名单={config.user_whitelist}',
            flush=True,
        )
    return _pusher


def get_pusher() -> RunningBotPusher | None:
    return _pusher


def should_defer_push(msg_type_raw: int) -> str | None:
    base = _base_msg_type(msg_type_raw)
    if base == 3:
        return 'image'
    if base == 49:
        return 'rich'
    return None


def schedule_push(
    monitor,
    msg_data: dict,
    *,
    partial: bool = False,
    from_reliable_scanner: bool = False,
) -> None:
    """在后台线程执行推送，不阻塞监听主流程。"""
    pusher = get_pusher()
    if not pusher:
        return
    if pusher.config.reliable_mode and not from_reliable_scanner:
        return
    _push_executor.submit(pusher.try_push, monitor, msg_data, partial=partial)


_push_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix='rb-push')
