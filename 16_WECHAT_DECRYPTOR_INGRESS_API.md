# WeChat Decryptor → Running Service 入站 API 标准

更新日期：2026-05-25

| 项 | 说明 |
| --- | --- |
| 版本 | **wechat_ingress_v1** |
| 正式入口 | **`POST /api/ingress/wechat/message`** |
| 受众 | WeChat Decryptor 项目 / Decryptor AI 助手 |
| Running Service 实现 | 仓库内参考 `wechat_ingress_adapter.py`；Running 部署路径 `message_bus/wechat_ingress_adapter.py`、`message_bus/gateway.py` |
| 关联文档 | `project-docs/14_HTTP_INGRESS_INTERFACE_SPEC.md`、`project-docs/15_MEDIA_INLINE_BASE64_PLAN.md` |
| 本地验证 | `reports/verification/phase4_media_inline_base64_2026-05-25-local.md` |

---

## 1. 一句话说明

Decryptor 负责监听微信消息、标准化事件、跨机 HTTP 投递；Running Service 负责接收事件、执行业务、写 Outbox、由 Sender 回群。

**Decryptor 只投递外部微信 schema，不要投递 Running Service 内部 `StandardMessage` 格式。**

---

## 2. 端点与网络

### 2.1 必须使用

```http
POST http://{host}:{port}/api/ingress/wechat/message
Content-Type: application/json
```

默认（Running Service 本机）：

```text
http://127.0.0.1:18765/api/ingress/wechat/message
```

配置项见 Running Service 的 `config.py`：

- `ENABLE_HTTP_MESSAGE_GATEWAY=True`
- `HTTP_GATEWAY_HOST`（默认 `127.0.0.1`）
- `HTTP_GATEWAY_PORT`（默认 `18765`）

### 2.2 不要使用

| 端点 | 原因 |
| --- | --- |
| `POST /api/messages` | 仅供 Running Service 内部 `MAIN_INGRESS` **route-only** 对齐路由，**不执行业务** |
| wxauto 本地路径 `local_path` | 跨机 Decryptor 不可依赖 Running Service 机器上的文件路径 |

### 2.3 健康检查（可选）

```http
GET http://{host}:{port}/api/health
```

用于 Decryptor 启动前确认 Gateway 可达。

---

## 3. Running Service 侧前置开关

Decryptor 联调前，Running Service 负责人需确认以下配置（`config.py` / `config.example.py`）：

| 配置项 | 文本消息 | 图片 inline base64 | 说明 |
| --- | --- | --- | --- |
| `ENABLE_HTTP_MESSAGE_GATEWAY` | 必须 `True` | 必须 `True` | 启动 Gateway |
| `ENABLE_OUTBOX_WRITE` | 建议 `True` | 建议 `True` | 业务回复写 Outbox |
| `ENABLE_SENDER_AGENT_POLL` | 建议 `True` | 建议 `True` | Sender 拉 Outbox 发群 |
| `ENABLE_IMAGE_JOB_WORKER` | — | 必须 `True` | 图片异步识别 |
| `ENABLE_MEDIA_INLINE_BASE64_INGRESS` | — | 必须 `True` | 接收跨机 base64 图片 |
| `ENABLE_HTTP_INGRESS_IDEMPOTENCY` | 建议 `True` | 建议 `True` | 同一 `event_id` 重投去重 |
| `RUNNING_CLUB_GROUPS` | 必须含目标群名 | 必须含目标群名 | 与 `chat.name` 对齐 |

图片相关限制（Running Service 侧）：

| 配置项 | 默认 |
| --- | --- |
| `MAX_INLINE_IMAGE_BYTES` | `5242880`（5MB，解码后原始大小） |
| `MEDIA_ALLOWED_MIME_TYPES` | `image/jpeg,image/png,image/webp` |

---

## 4. 请求体总览（wechat_ingress_v1）

顶层结构固定为 7 块：

```json
{
  "source": "wechat",
  "adapter": "wechat-decryptor",
  "event_id": "...",
  "trace_id": "...",
  "timestamp": "...",
  "chat": { },
  "sender": { },
  "message": { },
  "delivery": { }
}
```

### 4.1 顶层字段

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `source` | string | 是 | 固定 `"wechat"` |
| `adapter` | string | 是 | Decryptor 标识，如 `"wechat-decryptor"` |
| `event_id` | string | 是 | **全局唯一**事件 ID；幂等主键 |
| `trace_id` | string | 是 | 全链路追踪 ID；建议 UUID 或稳定 hash |
| `timestamp` | string | 是 | 微信消息时间，ISO 8601，如 `2026-05-25T14:00:00+08:00` |
| `chat` | object | 是 | 会话信息 |
| `sender` | object | 是 | 发送者信息 |
| `message` | object | 是 | 消息内容 |
| `delivery` | object | 是 | 投递元数据 |

### 4.2 `chat`

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `id` | string | 是 | 稳定群 ID；暂时没有可用群名 |
| `name` | string | 是 | **微信群显示名**；Running Service 用它判断是否跑团群 |
| `type` | string | 是 | `"group"` 或 `"private"` |

**重要**：`chat.name` 必须与 Running Service 的 `RUNNING_CLUB_GROUPS` / `LISTEN_LIST` 中配置的群名**完全一致**（含 emoji、空格）。

### 4.3 `sender`

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `id` | string | 是 | 微信成员 ID；暂时没有可用昵称 hash |
| `name` | string | 是 | 微信昵称 |
| `display_name` | string | 是 | 群内展示名；跑团业务用它识别成员 |
| `is_self` | boolean | 是 | 是否机器人自己发的消息 |
| `is_admin` | boolean | 否 | 是否群管理员 |

### 4.4 `message`

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `id` | string | 是 | 微信消息 ID |
| `type` | string | 是 | `text` / `image` / `mixed` / `system` |
| `text` | string | 否 | 清洗后正文 |
| `raw_text` | string | 否 | 原始正文；`text` 为空时 Running Service 会回退用它 |
| `mentions` | array | 否 | 被 @ 的昵称或 ID 列表 |
| `is_at_bot` | boolean | 否 | 是否明确 @ 机器人 |
| `images` | array | 否 | 图片列表；见 §6 |
| `quote` | object/null | 否 | 引用消息；见 §7 |

`type` 映射规则：

- `text` → 文本消息
- `image` → 图片消息
- `mixed` → 有图按图片处理，无图按文本
- 其他未知类型 → 按文本兜底

### 4.5 `delivery`

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `dedupe_key` | string | 是 | 幂等键；**建议等于 `event_id`** |
| `received_at` | string | 是 | Decryptor 收到消息的时间，ISO 8601 |
| `retry_count` | number | 否 | Decryptor 侧重试次数，默认 `0` |

推荐格式：

```text
dedupe_key = event_id = wechat:{chat.id}:{message.id}
```

---

## 5. 文本消息

### 5.1 请求示例：文字打卡

```json
{
  "source": "wechat",
  "adapter": "wechat-decryptor",
  "event_id": "wechat:group_001:msg_txt_001",
  "trace_id": "trace_txt_001",
  "timestamp": "2026-05-25T14:00:00+08:00",
  "chat": {
    "id": "group_001",
    "name": "足霸跑团",
    "type": "group"
  },
  "sender": {
    "id": "member_001",
    "name": "张三",
    "display_name": "张三",
    "is_self": false,
    "is_admin": false
  },
  "message": {
    "id": "msg_txt_001",
    "type": "text",
    "text": "打卡 5.3km",
    "raw_text": "打卡 5.3km",
    "mentions": [],
    "is_at_bot": false,
    "images": [],
    "quote": null
  },
  "delivery": {
    "dedupe_key": "wechat:group_001:msg_txt_001",
    "received_at": "2026-05-25T14:00:01+08:00",
    "retry_count": 0
  }
}
```

### 5.2 成功响应（文本业务已执行）

HTTP `200`：

```json
{
  "accepted": true,
  "duplicate": false,
  "dedupe_key": "wechat:group_001:msg_txt_001",
  "message_id": "msg_txt_001",
  "dispatch_tasks": [
    {
      "task_id": "dt-xxxx",
      "service_name": "running_service",
      "dispatch_reason": "running_service_text_candidate",
      "delivery_mode": "sync",
      "status": "done",
      "route_snapshot": {
        "trigger_type": "FUNCTION_COMMAND",
        "handler_name": "handle_running_club_text",
        "reason": "function_intent",
        "intent_type": "checkin"
      }
    }
  ],
  "running_response": {
    "handled": true,
    "dispatch_only": false,
    "business_executed": true,
    "execution_status": "handled",
    "handler_name": "handle_running_club_text",
    "reply": "已记录 张三：5.30 公里，本周累计 5.30 公里。打卡ID：123",
    "outbox_ids": ["ob-xxxx"]
  },
  "duration_ms": 15
}
```

文本消息命中跑团功能后，Running Service 会：

1. 执行业务（打卡、排行榜、周报等）
2. 写 Outbox
3. 由 Sender Agent 发回微信群

Decryptor **不需要**也不应该自己发群回复。

---

## 6. 图片消息（inline_base64）

### 6.1 传输方式

跨机 Decryptor **必须**使用：

```json
"images": [
  {
    "image_id": "wx_img_abc",
    "media": {
      "transport": "inline_base64",
      "content_base64": "<标准 base64，无 data: 前缀>",
      "mime_type": "image/jpeg",
      "file_name": "screenshot.jpg",
      "size_bytes": 245678,
      "sha256": "a1b2c3d4e5f6..."
    }
  }
]
```

规则：

| 规则 | 说明 |
| --- | --- |
| 单图 | 当前版本 **只支持 1 张图**；多图 inline base64 返回 `400 unsupported_multi_image` |
| base64 格式 | 标准 base64 字符串，**不要**带 `data:image/jpeg;base64,` 前缀 |
| `size_bytes` | 解码后原始字节数，必须与解码结果 **严格相等** |
| `sha256` | 对解码后 bytes 做 SHA256，**小写 hex 64 字符** |
| `mime_type` | 仅允许 `image/jpeg`、`image/png`、`image/webp` |
| 大小上限 | 默认 5MB（解码后） |

Decryptor 侧伪代码：

```python
import base64
import hashlib

raw_bytes = read_image_bytes(path)
payload = {
    "transport": "inline_base64",
    "content_base64": base64.b64encode(raw_bytes).decode("ascii"),
    "mime_type": "image/jpeg",
    "file_name": "run.jpg",
    "size_bytes": len(raw_bytes),
    "sha256": hashlib.sha256(raw_bytes).hexdigest(),
}
```

### 6.2 请求示例：跑步截图

```json
{
  "source": "wechat",
  "adapter": "wechat-decryptor",
  "event_id": "wechat:group_001:msg_img_001",
  "trace_id": "trace_img_001",
  "timestamp": "2026-05-25T14:00:00+08:00",
  "chat": {
    "id": "group_001",
    "name": "足霸跑团",
    "type": "group"
  },
  "sender": {
    "id": "member_001",
    "name": "张三",
    "display_name": "张三",
    "is_self": false
  },
  "message": {
    "id": "msg_img_001",
    "type": "image",
    "text": "",
    "raw_text": "",
    "is_at_bot": false,
    "images": [
      {
        "image_id": "wx_img_abc",
        "media": {
          "transport": "inline_base64",
          "content_base64": "<BASE64_WITHOUT_PREFIX>",
          "mime_type": "image/jpeg",
          "file_name": "screenshot.jpg",
          "size_bytes": 245678,
          "sha256": "a1b2c3..."
        }
      }
    ],
    "quote": null
  },
  "delivery": {
    "dedupe_key": "wechat:group_001:msg_img_001",
    "received_at": "2026-05-25T14:00:01+08:00",
    "retry_count": 0
  }
}
```

### 6.3 成功响应（图片已入队）

HTTP `200`：

```json
{
  "accepted": true,
  "duplicate": false,
  "dedupe_key": "wechat:group_001:msg_img_001",
  "message_id": "msg_img_001",
  "media_ref": "med_sha256_a1b2c3...",
  "image_job_id": "trace_img_001",
  "dispatch_tasks": [ ... ],
  "running_response": {
    "handled": true,
    "business_executed": true,
    "execution_status": "queued",
    "handler_name": "handle_running_club_image",
    "media_ref": "med_sha256_a1b2c3...",
    "image_job_id": "trace_img_001"
  },
  "duration_ms": 28
}
```

后续链路（Decryptor 无需参与）：

```text
Gateway 解码 base64 → 落盘 media_store → 生成 media_ref
→ 创建 image_jobs（media_ref）
→ Image Worker 识别 → 入库 / 短反馈
→ Outbox → Sender → 微信群
```

**安全要求**：`content_base64` 仅用于 HTTP 传输；Running Service 落盘后立即丢弃内存中的 base64，不会写入数据库、Outbox 或日志。

---

## 7. 引用消息（quote）

管理员 `/撤销`、`/补卡` 等命令依赖引用正文。

**标准（Running Service 与 Decryptor 共用，事实源即本文档 + `message_bus/wechat_ingress_adapter.py`）：**

| 项 | 要求 |
| --- | --- |
| 正文 `text` | 含 `@跑团小助手` 与 `/撤销` 或 `/补卡`；若微信 UI 显示「引用文字」，可原样传入，Running 会剥离 |
| `is_at_bot` 或 `mentions[]` | 至少一种为真，见 §7.1 |
| `quote.text` | **必填**（或 `content`/`body`/`preview` 之一），须含机器人「已记录…」或「已为…补记…」全文及 `打卡ID` |
| 预期群内回复 | **固定业务短句**，如 `已撤销 张三：5.30 公里。`；**不是** AI 闲聊、段子或角色扮演 |
| `running_response.action_type` | 成功撤销应为 `admin_quote_undo`；**不应**为 `general_ai_reply` |

### 7.1 @ 机器人字段（二选一或同时）

```json
"is_at_bot": true,
"mentions": [{ "name": "跑团小助手", "is_bot": true }]
```

仅 UI 里 @ 但 JSON 未带 `mentions`/`is_at_bot` 时，管理员命令可能无法执行。

### 7.2 引用撤销示例

```json
"message": {
  "type": "text",
  "text": "@跑团小助手 /撤销",
  "is_at_bot": true,
  "quote": {
    "message_id": "quoted_msg_001",
    "sender_name": "跑团小助手",
    "text": "已记录 张三：5.30 公里。打卡ID：123",
    "type": "text"
  }
}
```

引用图片（跨机 `/补卡` 无距离识图）应带 `quote.media`：

```json
"quote": {
  "message_id": "quoted_img_001",
  "sender_name": "张三",
  "text": "",
  "type": "image",
  "image_id": "wx_img_<md5>",
  "media": {
    "transport": "inline_base64",
    "content_base64": "<BASE64_WITHOUT_PREFIX>",
    "mime_type": "image/jpeg",
    "file_name": "<md5>.jpg",
    "size_bytes": 245678,
    "sha256": "a1b2c3..."
  }
}
```

Decryptor 从引用 XML 的 `refermsg/content` 提取图片 MD5，解密 `.dat` 后填入 `quote.media`（与主图相同结构）。Running Service 侧需开启对应验收路径。

---

## 8. 幂等与重试

### 8.1 Decryptor 必须保证

1. 每条微信消息生成唯一 `event_id`
2. `delivery.dedupe_key` 与 `event_id` 保持一致（推荐）
3. 重试时使用**相同** `event_id` / `dedupe_key` / `trace_id`

### 8.2 Running Service 行为

当 `ENABLE_HTTP_INGRESS_IDEMPOTENCY=True`：

| 场景 | 响应 |
| --- | --- |
| 首次 POST | `duplicate=false` |
| 相同 `event_id` 重投 | `duplicate=true`，不重复执行业务、不重复创建 image_job |

图片额外保护：同一 `(group_name, trace_id)` 不重复创建 `image_jobs`。

### 8.3 Decryptor 重试策略建议

```text
HTTP 超时 / 5xx → 用相同 event_id 重试
HTTP 400 / 413 / 415 → 修正 payload 后再发（新 event_id 或修正原 event）
HTTP 200 + duplicate=true → 视为成功，停止重试
```

---

## 9. 错误码

媒体相关错误 HTTP body：

```json
{
  "accepted": false,
  "error_code": "sha256_mismatch"
}
```

| HTTP | error_code | 场景 |
| --- | --- | --- |
| 400 | `inline_base64_disabled` | Running Service 未开 `ENABLE_MEDIA_INLINE_BASE64_INGRESS` |
| 400 | `invalid_base64` | base64 解码失败 |
| 400 | `size_mismatch` | `size_bytes` 与解码长度不一致 |
| 400 | `sha256_mismatch` | sha256 与解码 bytes 不一致 |
| 400 | `unsupported_multi_image` | 多图 inline base64 |
| 413 | `payload_too_large` | 超过 `MAX_INLINE_IMAGE_BYTES` |
| 415 | `unsupported_mime` | mime 不在白名单 |
| 500 | 其他 | Gateway 内部错误 |

---

## 10. 消息类型与业务覆盖

| 场景 | message.type | 关键字段 | Running Service 行为 |
| --- | --- | --- | --- |
| 文字打卡 | `text` | `text` 含「打卡 Xkm」 | 执行业务 + Outbox 回复 |
| 我的跑量 / 排行榜 | `text` | 对应关键词 | 执行业务 + Outbox 回复 |
| 管理员命令 | `text` | `is_at_bot=true` + 命令文本 | 执行业务 + Outbox 回复 |
| 跑步截图 | `image` | `images[].media.inline_base64` | 落盘 + 创建 image_job + 异步识别 |
| @ 机器人闲聊 | `text` | `is_at_bot=true`，未命中跑团功能 | **当前 HTTP 路径未完整执行 AI 入队** |
| 机器人自己消息 | 任意 | `sender.is_self=true` | 忽略 |

---

## 11. Decryptor 实现清单

开发 Decryptor 时，按此顺序自检：

- [ ] 只 POST `/api/ingress/wechat/message`，不用 `/api/messages`
- [ ] 每条消息有唯一 `event_id`、`trace_id`、`delivery.dedupe_key`
- [ ] `chat.name` 与 Running Service 配置的群名完全一致
- [ ] `sender.display_name` 稳定传递成员展示名
- [ ] 文本消息填 `message.text` 或 `message.raw_text`
- [ ] 图片用 `media.transport=inline_base64`，并正确计算 `size_bytes` + `sha256`
- [ ] base64 **无** `data:` 前缀；日志里**不**打印完整 base64
- [ ] 引用消息填 `message.quote.text` / `sender_name` / `message_id`
- [ ] 失败重试保留相同 `event_id`
- [ ] 收到 `duplicate=true` 视为已成功投递

---

## 12. curl 联调示例

### 文本

```bash
curl -sS -X POST "http://127.0.0.1:18765/api/ingress/wechat/message" \
  -H "Content-Type: application/json" \
  -d @text_checkin.json
```

### 图片（需 Running Service 开启 inline base64）

```bash
curl -sS -X POST "http://127.0.0.1:18765/api/ingress/wechat/message" \
  -H "Content-Type: application/json" \
  -d @image_inline_base64.json
```

---

## 13. 版本与变更

| 版本 | 日期 | 变更 |
| --- | --- | --- |
| wechat_ingress_v1 | 2026-05-25 | 首版：外部 schema、文本业务执行、图片 inline_base64、media_ref / image_job |

未纳入 v1：

- 多图消息
- 引用图片 inline base64
- @ AI 普通入队 HTTP 化
- `POST /api/messages` 作为 Decryptor 入口

---

## 14. 给 Decryptor AI 助手的 Prompt 摘要

可直接复制给 Decryptor 项目 AI：

```text
你是 WeChat Decryptor 的 HTTP 客户端开发者。

目标：把微信群消息标准化后 POST 到 Running Service：
POST http://127.0.0.1:18765/api/ingress/wechat/message

必须使用外部 schema（wechat_ingress_v1）：
source / adapter / event_id / trace_id / timestamp / chat / sender / message / delivery

不要使用 POST /api/messages。
不要传 Running Service 内部 StandardMessage 字段名。
跨机图片必须用 message.images[].media.transport=inline_base64，
并附带 mime_type、size_bytes、sha256、content_base64（无 data: 前缀）。
单图 only；event_id 与 delivery.dedupe_key 保持一致；重试时保留相同 event_id。
chat.name 必须与 Running Service RUNNING_CLUB_GROUPS 完全一致。
日志禁止输出完整 content_base64。
```
