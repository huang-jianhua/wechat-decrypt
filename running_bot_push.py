"""
将监听到的微信消息标准化后 POST 到本机 running-bot ingress。

仅调用: POST /api/ingress/wechat/message
不调用 Outbox，不实现发送或跑团业务逻辑。
"""
from __future__ import annotations

import hashlib
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

import zstandard as zstd

_zstd_dctx = zstd.ZstdDecompressor()
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


def stable_message_id(
    username: str,
    timestamp: int,
    local_id: int | None,
    sender_key: str,
    raw_text: str,
) -> str:
    if local_id is not None:
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
    image_id = os.path.splitext(img_name)[0]
    ext = os.path.splitext(img_name)[1].lower()
    mime = 'image/jpeg'
    if ext == '.png':
        mime = 'image/png'
    elif ext == '.gif':
        mime = 'image/gif'
    elif ext == '.webp':
        mime = 'image/webp'
    return [{
        'image_id': image_id,
        'url': None,
        'local_path': local_path,
        'mime_type': mime,
        'sha256': None,
        'width': None,
        'height': None,
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
            if local_type:
                msg_data['msg_type_raw'] = local_type

            if isinstance(mc, bytes) and ct_flag == 4:
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


class RunningBotPusher:
    def __init__(self, config: RunningBotPushConfig, contact_names: dict):
        self.config = config
        self.contact_names = contact_names
        self._pushed: set[str] = set()
        self._lock = threading.Lock()

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

        try:
            enrich_message_from_db(monitor, msg_data)
        except Exception as e:
            print(f'  [running-bot-push] enrich 失败: {e}', flush=True)

        payload = build_ingress_payload(msg_data, self.config, self.contact_names)
        dedupe_key = payload['delivery']['dedupe_key']
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


def schedule_push(monitor, msg_data: dict, *, partial: bool = False) -> None:
    """在后台线程执行推送，不阻塞监听主流程。"""
    pusher = get_pusher()
    if not pusher:
        return
    _push_executor.submit(pusher.try_push, monitor, msg_data, partial=partial)


_push_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix='rb-push')
