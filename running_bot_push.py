"""
将监听到的微信消息标准化后 POST 到本机 running-bot ingress。

仅调用: POST /api/ingress/wechat/message
不调用 Outbox，不实现发送或跑团业务逻辑。
"""
from __future__ import annotations

import hashlib
import base64
import glob
import json
import os
import re
import sqlite3
import threading
import time
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
    outbox_max_attempts: int = 3
    log_post_payload: bool = False
    image_aes_key: bytes | str | None = None
    image_xor_key: int = 0x88

    @classmethod
    def from_cfg(cls, cfg: dict) -> 'RunningBotPushConfig':
        aes_key = cfg.get('image_aes_key') or None
        if isinstance(aes_key, str) and aes_key:
            stripped = aes_key.strip()
            # 32 位 hex → 16 字节；否则按 ASCII 密钥原样交给 decrypt_dat_file（与 monitor 一致）
            if len(stripped) == 32 and all(c in '0123456789abcdefABCDEF' for c in stripped):
                try:
                    aes_key = bytes.fromhex(stripped)
                except ValueError:
                    aes_key = stripped
            else:
                aes_key = stripped
        xor_key = cfg.get('image_xor_key', 0x88)
        try:
            xor_key = int(xor_key)
        except (TypeError, ValueError):
            xor_key = 0x88
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
            outbox_max_attempts=int(cfg.get('running_bot_outbox_max_attempts', 3)),
            log_post_payload=bool(cfg.get('running_bot_log_post_payload', False)),
            image_aes_key=aes_key,
            image_xor_key=xor_key,
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


def stable_trace_id(event_id: str) -> str:
    """trace_id 与 event_id 稳定绑定；重试时不得更换。"""
    return event_id


def _bot_names(bot_name: str, aliases: list[str]) -> list[str]:
    names = []
    if bot_name:
        names.append(bot_name.strip())
    for alias in aliases or []:
        alias = (alias or '').strip()
        if alias and alias not in names:
            names.append(alias)
    return names


def _is_bot_mention(name: str, bot_name: str, aliases: list[str]) -> bool:
    needle = (name or '').strip()
    if not needle:
        return False
    for bot in _bot_names(bot_name, aliases):
        if needle == bot or needle.lower() == bot.lower():
            return True
    return False


def _extract_xml_document(content: str) -> str:
    """从群消息正文中截取 XML 片段（去掉 wxid: 前缀等）。"""
    text = (content or '').strip()
    if not text:
        return ''
    if ':\n' in text:
        head, tail = text.split(':\n', 1)
        if '<' not in head and ('<?xml' in tail or '<msg' in tail or '<appmsg' in tail):
            text = tail.strip()
    for marker in ('<?xml', '<msg', '<appmsg'):
        idx = text.find(marker)
        if idx >= 0:
            return text[idx:]
    return text


def _parse_refermsg_element(ref) -> dict:
    if ref is None:
        return {}
    ref_type = (ref.findtext('type') or '').strip()
    ref_svrid = (ref.findtext('svrid') or ref.findtext('msgsvrid') or '').strip()
    ref_content = (ref.findtext('content') or '') or ''
    ref_name = (ref.findtext('displayname') or '').strip()
    ref_create_time_raw = (ref.findtext('createtime') or ref.findtext('createTime') or '').strip()
    ref_create_time = None
    if ref_create_time_raw.isdigit():
        ref_create_time = int(ref_create_time_raw)
    ref_image_id = ''
    ref_image_md5 = ''
    if ref_content and '<img' in ref_content.lower():
        md5_match = re.search(r'md5=["\']([0-9a-fA-F]+)["\']', ref_content)
        if md5_match:
            ref_image_md5 = md5_match.group(1).lower()
            ref_image_id = f'wx_img_{ref_image_md5}'
    return {
        'ref_name': ref_name,
        'ref_content': ref_content,
        'ref_type': ref_type or None,
        'ref_svrid': ref_svrid or None,
        'ref_msgid': ref_svrid or None,
        'ref_create_time': ref_create_time,
        'ref_image_id': ref_image_id or None,
        'ref_image_md5': ref_image_md5 or None,
    }


def parse_appmsg_rich(content: str, local_type: int = 0) -> dict | None:
    """解析微信 appmsg XML（含 type=57 引用回复）。"""
    xml_text = _extract_xml_document(content)
    if not xml_text or '<appmsg' not in xml_text:
        return None
    try:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(xml_text)
        appmsg = root.find('.//appmsg')
        if appmsg is None:
            return None
        title = (appmsg.findtext('title') or '').strip()
        des = (appmsg.findtext('des') or '').strip()
        url = (appmsg.findtext('url') or '').strip().replace('&amp;', '&')
        sub_type = int(local_type) >> 32 if int(local_type) > 4294967296 else 0
        app_type = int(appmsg.findtext('type') or sub_type or 0)
        if app_type == 57:
            ref_fields = _parse_refermsg_element(appmsg.find('.//refermsg'))
            ref_content = ref_fields.get('ref_content') or ''
            if ref_content and str(ref_fields.get('ref_type') or '') != '3':
                ref_fields['ref_content'] = ref_content.strip()[:2000]
            return {'type': 'quote', 'title': title, **ref_fields}
        if app_type == 6:
            attach = appmsg.find('.//appattach')
            return {
                'type': 'file',
                'title': title,
                'file_ext': (attach.findtext('fileext') or '') if attach is not None else '',
                'file_size': int(attach.findtext('totallen') or 0) if attach is not None else 0,
                'app_type': app_type,
            }
        if app_type in (33, 36, 44):
            source = (appmsg.findtext('sourcedisplayname') or '').strip()
            return {
                'type': 'miniapp',
                'title': title,
                'source': source,
                'url': url,
                'app_type': app_type,
            }
        if app_type == 19:
            return {
                'type': 'chatlog',
                'title': title,
                'des': des[:200] if des else '',
                'app_type': app_type,
            }
        if app_type == 51:
            finder = appmsg.find('.//finderFeed')
            nickname = ''
            finder_desc = ''
            if finder is not None:
                nickname = (finder.findtext('nickname') or '').strip()
                finder_desc = (finder.findtext('desc') or '').strip()
            return {
                'type': 'channels',
                'title': title,
                'des': finder_desc or des[:200],
                'url': url,
                'finder_nickname': nickname,
                'app_type': app_type,
            }
        if title or url:
            return {
                'type': 'link',
                'title': title,
                'des': des[:200],
                'url': url,
                'app_type': app_type,
            }
    except Exception:
        return None
    return None


def _mention_source_text(text: str) -> str:
    if not text:
        return ''
    if '<?xml' in text or '<appmsg' in text:
        rich = parse_appmsg_rich(text)
        return (rich or {}).get('title') or ''
    return text


def parse_mentions(text: str, bot_name: str, aliases: list[str]) -> list[dict]:
    """从正文提取 @ 列表；保留微信零宽字符边界。"""
    text = _mention_source_text(text)
    if not text:
        return []
    seen: set[str] = set()
    mentions: list[dict] = []
    for match in re.finditer(r'@([^\s@​\u200b\u2005]+)', text):
        name = match.group(1).strip()
        if not name or name in seen:
            continue
        seen.add(name)
        mentions.append({
            'name': name,
            'is_bot': _is_bot_mention(name, bot_name, aliases),
        })
    return mentions


def detect_at_bot(
    raw_text: str,
    bot_name: str,
    aliases: list[str],
    mentions: list[dict] | None = None,
) -> bool:
    if mentions and any(m.get('is_bot') for m in mentions):
        return True
    text = _mention_source_text(raw_text or '')
    if not text:
        text = raw_text or ''
    for name in _bot_names(bot_name, aliases):
        if re.search(r'@' + re.escape(name) + r'(?:\s|$|[\u200b\u2005])', text, re.IGNORECASE):
            return True
        if f'@{name}' in text:
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


def _extract_wx_types(msg_type_raw: int, rich: dict | None) -> tuple[int, int | None]:
    base_type, sub_type = _split_msg_type(int(msg_type_raw or 0))
    app_type = None
    if rich and rich.get('app_type') is not None:
        try:
            app_type = int(rich['app_type'])
        except (TypeError, ValueError):
            app_type = None
    elif base_type == 49 and sub_type:
        app_type = sub_type
    return base_type, app_type


def _resolve_content_kind(base_type: int, rich: dict | None) -> str:
    if rich:
        rich_type = rich.get('type')
        if rich_type in {
            'quote', 'link', 'channels', 'miniapp', 'file', 'chatlog',
            'emoji', 'voice', 'video', 'voip',
        }:
            return str(rich_type)
    return {
        1: 'text',
        3: 'image',
        34: 'voice',
        42: 'text',
        43: 'video',
        47: 'emoji',
        48: 'text',
        49: 'link',
        50: 'voip',
    }.get(base_type, 'text')


_RICH_TEXT_KINDS = frozenset({'link', 'file', 'channels', 'miniapp', 'chatlog'})


def _format_rich_text(rich: dict) -> str:
    """把 appmsg 解析结果转为可读正文（避免推送原始 XML）。"""
    rich_type = rich.get('type')
    if rich_type == 'channels':
        nickname = (rich.get('finder_nickname') or '').strip()
        desc = (rich.get('des') or '').strip()
        if nickname and desc:
            return _collapse_text(f'[视频号] {nickname}：{desc}')
        if nickname:
            return _collapse_text(f'[视频号] {nickname}')
        title = (rich.get('title') or '').strip()
        if '当前版本不支持展示该内容' in title and desc:
            return _collapse_text(f'[视频号] {desc}')
        return _collapse_text(title or desc or '[视频号]')
    if rich_type == 'link':
        desc = (rich.get('des') or '').strip()
        title = (rich.get('title') or '').strip()
        url = (rich.get('url') or '').strip()
        if '当前版本不支持展示该内容' in title and desc:
            title = ''
        if title:
            label = f'[链接] {title}'
            return _collapse_text(label)
        parts = [p for p in (desc, url) if p]
        return _collapse_text('\n'.join(parts)) if parts else '[链接]'
    if rich_type == 'miniapp':
        title = (rich.get('title') or '').strip()
        return _collapse_text(f'[小程序] {title}' if title else '[小程序]')
    if rich_type == 'chatlog':
        title = (rich.get('title') or '').strip()
        return _collapse_text(f'[聊天记录] {title}' if title else '[聊天记录]')
    if rich_type == 'file':
        title = (rich.get('title') or '').strip()
        ext = (rich.get('file_ext') or '').strip()
        label = f'[文件] {title}' if title else '[文件]'
        if ext and not label.endswith(f'.{ext}'):
            label = f'{label}.{ext}'
        return _collapse_text(label)
    return ''


def _looks_like_raw_xml(text: str) -> bool:
    lowered = (text or '').lower()
    return '<?xml' in lowered or '<appmsg' in lowered or '<finderfeed' in lowered


def _extract_message_body(content: str, is_group: bool) -> str:
    body = content or ''
    if is_group and ':\n' in body:
        body = body.split(':\n', 1)[1]
    return body


def _clean_group_text(content: str, is_group: bool) -> tuple[str, str]:
    body = _extract_message_body(content, is_group)
    return _collapse_text(body), body


def _quote_is_image(rich: dict) -> bool:
    ref_type = rich.get('ref_type')
    if ref_type is not None:
        try:
            ref_type_int = int(ref_type)
            if ref_type_int == 3:
                return True
            if ref_type_int == 1:
                return False
        except (TypeError, ValueError):
            pass
    ref_content = (rich.get('ref_content') or '').strip()
    if not ref_content:
        return False
    lowered = ref_content.lower()
    if lowered in ('[图片]', '[image]'):
        return True
    if '<img' in lowered or '<msg><img' in lowered:
        return True
    return False


def _normalize_quote_ref_text(ref_content: str) -> str:
    """提取引用正文纯文本，供 Running admin_quote_undo 等命令使用。"""
    text = (ref_content or '').strip()
    if not text:
        return ''
    if text in ('引用文字', '[引用消息]'):
        return ''
    if ':\n' in text and not text.lstrip().startswith('<'):
        text = text.split(':\n', 1)[1].strip()
    if '<' in text and '>' in text:
        try:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(text)
            for tag in ('title', 'content', 'des'):
                for elem in root.iter(tag):
                    val = (elem.text or '').strip()
                    if val and val not in ('引用文字', '[引用消息]'):
                        return val
            joined = ''.join(root.itertext()).strip()
            if joined and joined not in ('引用文字', '[引用消息]'):
                return joined
        except Exception:
            pass
    return text


def _config_aes_key(config: RunningBotPushConfig) -> bytes | str | None:
    key = config.image_aes_key
    if not key:
        return None
    return key


def _rank_dat_files(dat_files: list[str], file_md5: str) -> list[str]:
    ranked = []
    for f in dat_files:
        fname = os.path.basename(f).lower()
        sz = os.path.getsize(f)
        if '_t_' in fname:
            rank = 5
        elif '_t.' in fname:
            rank = 4
        elif '_w.' in fname:
            rank = 2
        elif '_h.' in fname:
            rank = 1
        else:
            rank = 0
        ranked.append((rank, sz, f))
    ranked.sort(key=lambda x: (x[0], -x[1]))
    return [f for _rank, _sz, f in ranked]


def _find_dat_files_for_md5(
    config: RunningBotPushConfig,
    username: str,
    file_md5: str,
) -> tuple[list[str], str | None]:
    """在 attach 目录查找 .dat；优先当前会话 hash，再扫描其它 hash。"""
    if not config.wechat_base_dir:
        return [], 'no_wechat_base_dir'
    attach_root = os.path.join(config.wechat_base_dir, 'msg', 'attach')
    if not os.path.isdir(attach_root):
        return [], 'no_attach_dir'

    username_hashes: list[str] = []
    if username:
        username_hashes.append(hashlib.md5(username.encode()).hexdigest())
    try:
        for entry in os.listdir(attach_root):
            if entry not in username_hashes:
                username_hashes.append(entry)
    except OSError:
        pass

    found: list[str] = []
    pattern_tail = os.path.join('*', 'Img', f'{file_md5}*.dat')
    for uh in username_hashes:
        search_base = os.path.join(attach_root, uh)
        if not os.path.isdir(search_base):
            continue
        found.extend(glob.glob(os.path.join(search_base, pattern_tail)))
    return sorted(set(found)), None


def _convert_hevc_to_jpeg(hevc_path: str, jpeg_path: str) -> str | None:
    """wxgf/HEVC → JPEG（需 PyAV）。"""
    try:
        import av
    except ImportError:
        return None
    try:
        with open(hevc_path, 'rb') as f:
            data = f.read()
        vps_sig = b'\x00\x00\x00\x01\x40\x01'
        hevc_start = data.find(vps_sig)
        if hevc_start < 0:
            hevc_start = data.find(b'\x00\x00\x00\x01\x42\x01')
        if hevc_start < 0:
            return None
        h265_path = hevc_path + '.h265'
        with open(h265_path, 'wb') as f:
            f.write(data[hevc_start:])
        try:
            container = av.open(h265_path, format='hevc')
            for frame in container.decode(video=0):
                img = frame.to_image()
                img.save(jpeg_path, 'JPEG', quality=90)
                container.close()
                return jpeg_path
            container.close()
        finally:
            if os.path.isfile(h265_path):
                os.unlink(h265_path)
    except Exception:
        return None
    return None


def _extract_quote_image_md5(rich: dict) -> str | None:
    md5 = rich.get('ref_image_md5')
    if md5:
        return str(md5).lower()
    image_id = rich.get('ref_image_id') or ''
    if image_id.startswith('wx_img_'):
        return image_id[7:].lower()
    ref_content = rich.get('ref_content') or ''
    match = re.search(r'md5=["\']([0-9a-fA-F]+)["\']', ref_content)
    if match:
        return match.group(1).lower()
    return None


def _mime_for_image_path(path: str) -> str | None:
    ext = os.path.splitext(path)[1].lower()
    if ext in ('.jpg', '.jpeg'):
        return 'image/jpeg'
    if ext == '.png':
        return 'image/png'
    if ext == '.webp':
        return 'image/webp'
    return None


def _build_inline_media_from_file(
    decoded_image_dir: str,
    img_name: str,
    image_id: str | None = None,
) -> dict | None:
    local_path = os.path.join(decoded_image_dir, img_name)
    if not os.path.isabs(local_path):
        local_path = os.path.abspath(local_path)
    if not os.path.isfile(local_path):
        return None
    mime = _mime_for_image_path(local_path)
    if not mime:
        return None
    image_id = image_id or os.path.splitext(img_name)[0]
    with open(local_path, 'rb') as f:
        raw_bytes = f.read()
    digest = hashlib.sha256(raw_bytes).hexdigest()
    return {
        'image_id': image_id,
        'media': {
            'transport': 'inline_base64',
            'content_base64': base64.b64encode(raw_bytes).decode('ascii'),
            'mime_type': mime,
            'file_name': os.path.basename(local_path),
            'size_bytes': len(raw_bytes),
            'sha256': digest,
        },
    }


def _lookup_image_file_md5_from_resource_db(
    resource_db_path: str,
    *,
    svrid: str | int | None = None,
    create_time: int | None = None,
) -> str | None:
    """通过 message_resource.db 查被引用图片的本地文件 MD5（attach 文件名用）。"""
    if not resource_db_path or not os.path.isfile(resource_db_path):
        return None
    from decode_image import extract_md5_from_packed_info

    image_type_sql = (
        '(message_local_type = 3 OR message_local_type % 4294967296 = 3)'
    )
    try:
        conn = sqlite3.connect(f'file:{resource_db_path}?mode=ro', uri=True)
        row = None
        if svrid is not None and str(svrid).strip():
            try:
                svrid_int = int(str(svrid).strip())
            except (TypeError, ValueError):
                svrid_int = None
            if svrid_int is not None:
                row = conn.execute(
                    f'SELECT packed_info FROM MessageResourceInfo '
                    f'WHERE message_svr_id = ? AND {image_type_sql}',
                    (svrid_int,),
                ).fetchone()
        if not row and create_time is not None:
            row = conn.execute(
                f'SELECT packed_info FROM MessageResourceInfo '
                f'WHERE message_create_time = ? AND {image_type_sql}',
                (int(create_time),),
            ).fetchone()
        conn.close()
        if row and row[0]:
            return extract_md5_from_packed_info(row[0])
    except sqlite3.Error:
        return None
    return None


def resolve_quoted_image_local_name(
    rich: dict,
    username: str,
    config: RunningBotPushConfig,
    *,
    resource_db_path: str | None = None,
    log_miss: bool = False,
) -> str | None:
    """解析引用图片：refermsg md5 失败时，用 svrid/create_time 回查真实文件 MD5。"""
    if not rich or not _quote_is_image(rich):
        return None

    quote_md5 = _extract_quote_image_md5(rich)
    if quote_md5:
        img_name = resolve_decoded_image_by_md5(
            username, quote_md5, config, log_miss=False,
        )
        if img_name:
            return img_name

    file_md5 = None
    ref_svrid = rich.get('ref_svrid') or rich.get('ref_msgid')
    ref_create_time = rich.get('ref_create_time')
    if resource_db_path:
        file_md5 = _lookup_image_file_md5_from_resource_db(
            resource_db_path, svrid=ref_svrid, create_time=ref_create_time,
        )
    if not file_md5 or file_md5 == quote_md5:
        if log_miss:
            reason = 'no_dat'
            if quote_md5 and file_md5 and file_md5 != quote_md5:
                reason = 'no_dat'
            elif quote_md5 and resource_db_path and not file_md5:
                reason = 'no_resource'
            hint = (quote_md5 or '')[:12]
            print(
                f'  [running-bot-push] 引用图未就绪 md5={hint} reason={reason}',
                flush=True,
            )
        return None

    if quote_md5 and file_md5 != quote_md5:
        print(
            f'  [running-bot-push] 引用图 MD5 不一致 '
            f'quote={quote_md5[:12]} file={file_md5[:12]}，已通过 svrid 回查',
            flush=True,
        )
    return resolve_decoded_image_by_md5(
        username, file_md5, config, log_miss=log_miss,
    )


def resolve_decoded_image_by_md5(
    username: str,
    file_md5: str,
    config: RunningBotPushConfig,
    *,
    log_miss: bool = False,
) -> str | None:
    """根据图片 MD5 解密 .dat，返回 decoded_images 下的文件名。"""
    if not file_md5 or not config.decoded_image_dir:
        if log_miss:
            print('  [running-bot-push] 引用图未就绪: 缺少 md5 或 decoded_image_dir', flush=True)
        return None

    os.makedirs(config.decoded_image_dir, exist_ok=True)
    out_base = os.path.join(config.decoded_image_dir, file_md5)
    for ext in ('jpg', 'png', 'webp'):
        candidate = f'{out_base}.{ext}'
        if os.path.isfile(candidate):
            return os.path.basename(candidate)

    from decode_image import decrypt_dat_file, is_v2_format

    dat_files, miss_reason = _find_dat_files_for_md5(config, username, file_md5)
    if not dat_files:
        if log_miss:
            reason = miss_reason or 'no_dat'
            print(
                f'  [running-bot-push] 引用图未就绪 md5={file_md5[:12]} reason={reason}',
                flush=True,
            )
        return None

    aes_key = _config_aes_key(config)
    for selected in _rank_dat_files(dat_files, file_md5):
        if is_v2_format(selected) and not aes_key:
            continue
        result_path, fmt = decrypt_dat_file(
            selected, f'{out_base}.tmp', aes_key, config.image_xor_key,
        )
        if not result_path:
            continue
        if fmt in ('hevc', 'bin'):
            jpg_path = _convert_hevc_to_jpeg(result_path, f'{out_base}.jpg')
            try:
                os.unlink(result_path)
            except OSError:
                pass
            if jpg_path:
                return os.path.basename(jpg_path)
            if log_miss:
                print(
                    f'  [running-bot-push] 引用图 HEVC 转 JPEG 失败 md5={file_md5[:12]}',
                    flush=True,
                )
            continue
        if fmt not in ('jpg', 'png', 'webp'):
            try:
                os.unlink(result_path)
            except OSError:
                pass
            continue
        final = f'{out_base}.{fmt}'
        if os.path.isfile(final):
            os.unlink(final)
        os.rename(result_path, final)
        return os.path.basename(final)
    if log_miss:
        print(
            f'  [running-bot-push] 引用图解密失败 md5={file_md5[:12]} dat={len(dat_files)}',
            flush=True,
        )
    return None


def quote_has_inline_media(quote: dict | None) -> bool:
    if not quote or quote.get('type') != 'image':
        return True
    media = quote.get('media') or {}
    return bool(media.get('content_base64'))


def _log_quote_image_without_media(payload: dict, *, local_id=None) -> None:
    """引用图命令仍推送；仅记录 quote.media 缺失便于对账。"""
    quote = (payload.get('message') or {}).get('quote')
    if not quote or quote.get('type') != 'image' or quote_has_inline_media(quote):
        return
    image_id = (quote.get('image_id') or '')[:24]
    msg_id = (quote.get('message_id') or '')[:24]
    lid = f' local_id={local_id}' if local_id is not None else ''
    print(
        f'  [running-bot-push] 引用图无 inline_base64，仍推送命令'
        f'{lid} image_id={image_id} ref_msgid={msg_id}',
        flush=True,
    )


def _build_quote(
    rich: dict | None,
    *,
    username: str = '',
    config: RunningBotPushConfig | None = None,
    pre_image_name: str | None = None,
) -> dict | None:
    if not rich or rich.get('type') != 'quote':
        return None
    ref_content = _normalize_quote_ref_text(rich.get('ref_content') or '')
    ref_msgid = rich.get('ref_msgid') or rich.get('ref_svrid') or ''
    is_image = _quote_is_image(rich)
    quote = {
        'message_id': str(ref_msgid) if ref_msgid else None,
        'sender_name': rich.get('ref_name') or '',
        'text': '' if is_image else ref_content,
        'type': 'image' if is_image else 'text',
    }
    if is_image:
        file_md5 = _extract_quote_image_md5(rich)
        image_id = rich.get('ref_image_id') or ''
        if not image_id and file_md5:
            image_id = f'wx_img_{file_md5}'
        if not image_id and ref_msgid:
            image_id = f'wx_img_{ref_msgid}'
        quote['image_id'] = image_id or None
        if config and (pre_image_name or (username and file_md5)):
            img_name = pre_image_name
            if not img_name and file_md5:
                img_name = resolve_decoded_image_by_md5(username, file_md5, config, log_miss=False)
            if img_name:
                inline = _build_inline_media_from_file(
                    config.decoded_image_dir, img_name, image_id=image_id,
                )
                if inline:
                    quote['media'] = inline['media']
                    if not quote.get('image_id'):
                        quote['image_id'] = inline.get('image_id')
    return quote


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
    inline = _build_inline_media_from_file(decoded_image_dir, img_name)
    return [inline] if inline else []


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

    rich = msg_data.get('rich') or msg_data.get('rich_content')
    if not rich:
        raw_for_parse = msg_data.get('raw_content') or msg_data.get('content') or ''
        rich = parse_appmsg_rich(raw_for_parse, int(msg_data.get('msg_type_raw', 0) or 0))
        if rich:
            msg_data['rich'] = rich

    content = msg_data.get('raw_content') or msg_data.get('content') or ''
    text, raw_text = _clean_group_text(content, is_group)
    if msg_data.get('raw_text'):
        raw_text = msg_data['raw_text']
    if msg_data.get('text'):
        text = msg_data['text']

    rich = msg_data.get('rich') or msg_data.get('rich_content')
    if rich and rich.get('type') == 'quote':
        reply_text = rich.get('title') or text
        raw_text = reply_text
        text = _collapse_text(reply_text)
    elif rich and rich.get('type') in _RICH_TEXT_KINDS:
        rich_text = _format_rich_text(rich)
        if rich_text:
            raw_text = rich_text
            text = _collapse_text(rich_text)
    if _looks_like_raw_xml(text or raw_text):
        fallback = _format_rich_text(rich) if rich else ''
        if fallback:
            raw_text = fallback
            text = _collapse_text(fallback)
        else:
            raw_text = ''
            text = ''

    local_id = msg_data.get('local_id')
    message_id = stable_message_id(
        username, int(msg_data.get('timestamp', 0)),
        local_id, sender_id, raw_text or text,
        msg_data.get('db_key') or msg_data.get('source_db_key') or '',
    )
    chat_id = username
    event_id = stable_event_id(chat_id, message_id)
    trace_id = msg_data.get('_push_trace_id') or stable_trace_id(event_id)

    base_type = _base_msg_type(msg_data.get('msg_type_raw', 0))
    wx_base_type, wx_app_type = _extract_wx_types(msg_data.get('msg_type_raw', 0), rich)
    content_kind = _resolve_content_kind(wx_base_type, rich)
    ingress_type = _ingress_message_type(base_type, rich)
    quote = _build_quote(
        rich,
        username=username,
        config=config,
        pre_image_name=msg_data.get('_quote_image_local_name'),
    )
    images = _build_images(msg_data, config.decoded_image_dir) if ingress_type == 'image' else []
    mentions = parse_mentions(raw_text or text, config.bot_name, config.bot_aliases)
    is_at_bot = detect_at_bot(
        raw_text or text, config.bot_name, config.bot_aliases, mentions=mentions,
    )

    return {
        'source': 'wechat',
        'adapter': 'wechat-decryptor',
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
            'content_kind': content_kind,
            'wx_base_type': wx_base_type,
            **({'wx_app_type': wx_app_type} if wx_app_type is not None else {}),
            'text': text,
            'raw_text': raw_text or text,
            'mentions': mentions,
            'is_at_bot': is_at_bot,
            'quote': quote,
            'images': images,
        },
        'delivery': {
            'retry_count': 0,
            'dedupe_key': event_id,
            'received_at': _iso_cn(),
        },
    }


_WCDB_ZSTD_MAGIC = b'\x28\xb5\x2f\xfd'


def decode_wcdb_text(data, ct_flag: int = 0) -> str:
    """解码 WCDB 文本字段：ct_flag==4 或 zstd 魔数时解压，否则按 UTF-8。"""
    if data is None:
        return ''
    if isinstance(data, str):
        return data
    if not isinstance(data, bytes):
        return str(data)
    if _zstd_dctx:
        use_zstd = int(ct_flag or 0) == 4 or data.startswith(_WCDB_ZSTD_MAGIC)
        if use_zstd:
            try:
                return _zstd_dctx.decompress(data).decode('utf-8', errors='replace')
            except Exception:
                pass
    return data.decode('utf-8', errors='replace')


def decode_message_content(message_content, ct_flag) -> str:
    return decode_wcdb_text(message_content, ct_flag)


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


def _parse_ingress_response(body: str) -> dict:
    if not body:
        return {}
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return {}


def evaluate_ingress_response(
    response_status: int | None,
    response_json: dict,
) -> tuple[bool, bool, str | None]:
    """判定 ingress 是否投递成功。

    Returns:
        (success, retryable, error_message)
    """
    if response_status is None:
        return False, True, 'no_response'

    if 400 <= response_status < 500:
        err = (
            response_json.get('error_code')
            or response_json.get('error')
            or f'HTTP {response_status}'
        )
        return False, False, str(err)

    if not (200 <= response_status < 300):
        return False, True, f'HTTP {response_status}'

    if response_json.get('duplicate'):
        return True, False, None

    if response_json.get('accepted') is False:
        err = response_json.get('error_code') or response_json.get('error') or 'not_accepted'
        return False, False, str(err)

    running = response_json.get('running_response') or {}
    exec_status = running.get('execution_status')
    if exec_status in ('handled', 'queued'):
        return True, False, None

    outbox_ids = running.get('outbox_ids') or response_json.get('outbox_ids')
    if outbox_ids:
        return True, False, None

    if response_json.get('image_job_id') or running.get('image_job_id'):
        return True, False, None

    if response_json.get('accepted') is True:
        return True, False, None

    if not response_json:
        return True, False, None

    return True, False, None


def _redact_payload_for_log(payload: dict) -> dict:
    """复制 payload 供日志输出；不打印完整 base64。"""
    data = json.loads(json.dumps(payload, ensure_ascii=False))
    for img in (data.get('message') or {}).get('images') or []:
        media = img.get('media') or {}
        b64 = media.get('content_base64') or ''
        if b64:
            media['content_base64'] = f'<redacted len={len(b64)} sha256={media.get("sha256", "")[:16]}...>'
    quote = (data.get('message') or {}).get('quote') or {}
    quote_media = quote.get('media') or {}
    b64 = quote_media.get('content_base64') or ''
    if b64:
        quote_media['content_base64'] = (
            f'<redacted len={len(b64)} sha256={quote_media.get("sha256", "")[:16]}...>'
        )
    return data


def log_ingress_post(config: RunningBotPushConfig, payload: dict, attempt: int = 0) -> None:
    if not config.log_post_payload:
        return
    event_id = payload.get('event_id', '')
    print(
        f'[running-bot-push] POST {config.ingress_url} attempt={attempt} event_id={event_id}',
        flush=True,
    )
    print(json.dumps(_redact_payload_for_log(payload), ensure_ascii=False, indent=2), flush=True)


def log_ingress_response(
    payload: dict,
    response_status: int | None,
    response_json: dict | None,
) -> None:
    event_id = payload.get('event_id', '')
    print(
        f'[running-bot-push] RESPONSE status={response_status} event_id={event_id}',
        flush=True,
    )
    if response_json:
        print(json.dumps(response_json, ensure_ascii=False, indent=2), flush=True)


def push_with_retry(
    payload: dict,
    config: RunningBotPushConfig,
) -> tuple[str, int | None, str | None, int, dict]:
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    max_attempts = max(1, config.retry_count + 1)
    last_error = None
    response_status = None
    response_json: dict = {}

    for attempt in range(max_attempts):
        payload['delivery']['retry_count'] = attempt
        body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        if config.log_post_payload:
            log_ingress_post(config, payload, attempt)
        try:
            req = urllib.request.Request(
                config.ingress_url, data=body, headers=headers, method='POST',
            )
            with urllib.request.urlopen(req, timeout=config.timeout_seconds) as resp:
                response_status = resp.status
                response_json = _parse_ingress_response(
                    resp.read().decode('utf-8', errors='replace'),
                )
        except urllib.error.HTTPError as e:
            response_status = e.code
            err_body = e.read().decode('utf-8', errors='replace')
            response_json = _parse_ingress_response(err_body)
        except Exception as e:
            last_error = str(e)
            if attempt < max_attempts - 1:
                time.sleep(0.5 * (attempt + 1))
            continue

        if config.log_post_payload:
            log_ingress_response(payload, response_status, response_json)

        success, retryable, err = evaluate_ingress_response(response_status, response_json)
        if success:
            return 'success', response_status, None, attempt, response_json
        last_error = err
        if not retryable:
            break
        if attempt < max_attempts - 1:
            time.sleep(0.5 * (attempt + 1))

    payload['delivery']['retry_count'] = max_attempts - 1
    return 'failed', response_status, last_error, max_attempts - 1, response_json


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
    response_json: dict | None = None,
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
    if response_json:
        if response_json.get('duplicate'):
            parts.append('duplicate=true')
        running = response_json.get('running_response') or {}
        handler = running.get('handler_name') or response_json.get('handler_name')
        execution = running.get('execution_status') or response_json.get('execution_status')
        if handler:
            parts.append(f'handler_name={handler}')
        if execution:
            parts.append(f'execution_status={execution}')
        error_code = response_json.get('error_code')
        if error_code:
            parts.append(f'error_code={error_code}')
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
                        SET status=CASE WHEN status='delivering' THEN 'pending' ELSE status END,
                            updated_at=?
                        WHERE dedupe_key=?
                    ''', (now, dedupe_key))
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
                    WHERE status IN ('pending', 'failed')
                      AND next_retry_at <= ?
                      AND attempts < ?
                    ORDER BY created_at ASC
                    LIMIT ?
                ''', (now, self.config.outbox_max_attempts, self.config.outbox_batch_size)).fetchall()
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
                status = 'abandoned' if attempts >= self.config.outbox_max_attempts else 'failed'
                conn.execute('''
                    UPDATE running_bot_outbox
                    SET status=?, attempts=?, next_retry_at=?, last_error=?, updated_at=?
                    WHERE dedupe_key=?
                ''', (status, attempts, now + delay, error_message or '', now, dedupe_key))
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
                push_status, response_status, error_message, _, response_json = push_with_retry(
                    payload, self.config,
                )
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
                    response_json=response_json,
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

        rich = msg_data.get('rich') or msg_data.get('rich_content')
        if rich and _quote_is_image(rich) and not msg_data.get('_quote_image_local_name'):
            resource_db_path = None
            if getattr(monitor, 'db_cache', None):
                resource_db_path = monitor.db_cache.get(os.path.join('message', 'message_resource.db'))
            msg_data['_quote_image_local_name'] = resolve_quoted_image_local_name(
                rich, username, self.config,
                resource_db_path=resource_db_path,
                log_miss=False,
            )

        payload = build_ingress_payload(msg_data, self.config, self.contact_names)
        ingress_type = payload['message']['type']
        images = payload['message']['images']
        if (
            self.config.reliable_mode
            and ingress_type == 'image'
            and not images
            and not msg_data.get('_allow_empty_image')
        ):
            has_image_ref = bool(
                msg_data.get('image_local_name')
                or msg_data.get('image_url')
            )
            if not has_image_ref:
                return
            partial = True
            placeholder = '[图片 - 解码文件暂不可用]'
            if not (payload['message'].get('text') or '').strip():
                payload['message']['text'] = placeholder
            if not (payload['message'].get('raw_text') or '').strip():
                payload['message']['raw_text'] = payload['message'].get('text') or placeholder
            print(
                '  [running-bot-push] 图片解码文件缺失，best-effort partial push '
                f'local_id={msg_data.get("local_id")} '
                f'image={msg_data.get("image_local_name") or msg_data.get("image_url")}',
                flush=True,
            )
        _log_quote_image_without_media(
            payload, local_id=msg_data.get('local_id'),
        )
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
        push_status, response_status, error_message, _, response_json = push_with_retry(
            payload, self.config,
        )
        if partial and push_status == 'success':
            push_status = 'partial'

        log_push(
            payload['trace_id'], payload['event_id'],
            payload['chat']['id'], payload['chat']['name'],
            payload['sender']['name'], payload['message']['id'],
            payload['message']['type'], push_status,
            response_status=response_status,
            error_message=error_message,
            response_json=response_json,
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
