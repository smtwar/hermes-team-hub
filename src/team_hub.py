"""
Hermes Team Hub — 团队协作中心
================================
讨论区 + 任务板 + 状态墙，所有 Agent 通过 HTTP 访问。
支持 SSE 实时推送事件通知。

数据存储: /root/hermes-relay/hub/ 下的 JSON 文件
事件系统: 内存队列，SSE 订阅者实时收到推送
"""

import json, time, threading, os, queue
from pathlib import Path

HUB_DIR = Path("/root/hermes-relay/hub")
_HUB_LOCK = threading.Lock()

# ─── SSE 实时事件系统 ──────────────────────────────────────
# 订阅者列表: [(channel_pattern, queue), ...]
# channel_pattern="" 表示订阅所有频道
_sse_subscribers = []
_sse_lock = threading.Lock()

# ─── WebSocket 连接管理 ────────────────────────────────────
# 在线连接: [(handler, channel, name), ...]
_ws_connections = []
_ws_conn_lock = threading.Lock()


def ws_register(handler, channel: str = "", name: str = "unknown"):
    """注册 WebSocket 连接（用于在线状态和直接推送）"""
    with _ws_conn_lock:
        _ws_connections.append((handler, channel, name))
    # 广播上线事件
    _emit("agent_online", {
        "agent": name,
        "channel": channel,
        "online_count": len(_ws_connections),
    })


def ws_unregister(handler):
    """注销 WebSocket 连接"""
    unregistered_name = "unknown"
    with _ws_conn_lock:
        for h, ch, name in list(_ws_connections):
            if h is handler:
                _ws_connections.remove((h, ch, name))
                unregistered_name = name
                break
    # 广播离线事件
    _emit("agent_offline", {
        "agent": unregistered_name,
        "online_count": len(_ws_connections),
    })


def ws_get_online() -> list:
    """获取在线客户端列表（飞书风格在线状态）"""
    with _ws_conn_lock:
        return [{"name": name, "channel": ch} for _, ch, name in _ws_connections]


def ws_broadcast(data: str, exclude_handler=None):
    """向所有 WebSocket 客户端广播原始文本"""
    dead = []
    with _ws_conn_lock:
        conns = list(_ws_connections)
    for handler, ch, name in conns:
        if handler is exclude_handler:
            continue
        try:
            handler._ws_send(data)
        except (BrokenPipeError, ConnectionResetError, OSError, AttributeError):
            dead.append(handler)
    for h in dead:
        ws_unregister(h)

def sse_subscribe(channel: str = "") -> queue.Queue:
    """订阅事件。channel="" 表示所有事件"""
    q = queue.Queue()
    with _sse_lock:
        _sse_subscribers.append((channel, q))
    return q

def sse_unsubscribe(channel: str, q: queue.Queue):
    with _sse_lock:
        try:
            _sse_subscribers.remove((channel, q))
        except ValueError:
            pass

def _emit(event_type: str, data: dict):
    """向所有匹配的 SSE 订阅者推送事件"""
    channel = data.get("channel", "")
    with _sse_lock:
        for sub_ch, q in list(_sse_subscribers):
            # 空 pattern 匹配所有，或通道名匹配
            if not sub_ch or sub_ch == channel or sub_ch == event_type:
                try:
                    q.put_nowait({"type": event_type, **data})
                except queue.Full:
                    pass  # 队列满了就丢弃

# ─── 初始化 ────────────────────────────────────────────────

def init():
    HUB_DIR.mkdir(parents=True, exist_ok=True)
    _init_json("channels", [{"name": "general", "created_by": "system", "created_at": time.time()}])
    _init_json("tasks", [])
    _init_json("status", [])
    _init_task_id_counter()

def _init_json(name, default):
    p = HUB_DIR / f"{name}.json"
    if not p.exists():
        with open(p, "w") as f:
            json.dump(default, f, ensure_ascii=False, indent=2)

def _read_json(name):
    p = HUB_DIR / f"{name}.json"
    if not p.exists():
        return []
    with open(p, "r") as f:
        return json.load(f)

def _write_json(name, data):
    with _HUB_LOCK:
        p = HUB_DIR / f"{name}.json"
        tmp = p.with_suffix(f".tmp.{os.getpid()}")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        tmp.replace(p)

def _append_json(name, entry):
    """追加一条记录到 JSON 列表（线程安全）"""
    with _HUB_LOCK:
        p = HUB_DIR / f"{name}.json"
        data = json.loads(p.read_text(encoding="utf-8")) if p.exists() else []
        data.append(entry)
        tmp = p.with_suffix(f".tmp.{os.getpid()}")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        tmp.replace(p)
    return entry

# ─── 频道 / 讨论 ──────────────────────────────────────────

def create_channel(name: str, created_by: str = "system") -> dict:
    channels = _read_json("channels")
    if any(c["name"] == name for c in channels):
        return {"success": False, "error": f"频道 '{name}' 已存在"}
    entry = {"name": name, "created_by": created_by, "created_at": time.time()}
    _append_json("channels", entry)
    _emit("channel_created", {"channel": name, "created_by": created_by})
    return {"success": True, "channel": entry}

def list_channels() -> dict:
    return {"success": True, "channels": _read_json("channels")}

def delete_channel(name: str) -> dict:
    """删除频道及其聊天记录"""
    channels = _read_json("channels")
    if not any(c["name"] == name for c in channels):
        return {"success": False, "error": f"频道 '{name}' 不存在"}
    if name == "general":
        return {"success": False, "error": "不能删除 #general 频道"}

    with _HUB_LOCK:
        # 从频道列表移除
        channels = [c for c in channels if c["name"] != name]
        p = HUB_DIR / "channels.json"
        tmp = p.with_suffix(f".tmp.{os.getpid()}")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(channels, f, ensure_ascii=False, indent=2)
        tmp.replace(p)

        # 删除聊天记录文件
        chat_file = HUB_DIR / f"chat_{name}.json"
        if chat_file.exists():
            chat_file.unlink()

    _emit("channel_deleted", {"channel": name})
    return {"success": True, "channel": name, "message": f"已删除频道 '#{name}'"}

def post_message(channel: str, sender: str, content: str) -> dict:
    channels = {c["name"] for c in _read_json("channels")}
    if channel not in channels:
        return {"success": False, "error": f"频道 '{channel}' 不存在"}

    msg = {
        "id": int(time.time() * 1000000) % (2**53),
        "channel": channel,
        "sender": sender,
        "content": content,
        "timestamp": time.time(),
    }
    chat_file = f"chat_{channel}"
    _append_json(chat_file, msg)
    # 实时推送事件
    _emit("chat_message", {
        "channel": channel,
        "id": msg["id"],
        "sender": sender,
        "content": content,
        "timestamp": msg["timestamp"],
    })
    return {"success": True, "message": msg}

def read_messages(channel: str, since_id: int = 0, limit: int = 50) -> dict:
    msgs = _read_json(f"chat_{channel}")
    if since_id > 0:
        msgs = [m for m in msgs if m.get("id", 0) > since_id]
    msgs = msgs[-limit:]
    return {"success": True, "channel": channel, "messages": msgs, "total": len(msgs)}

# ─── 任务板 ───────────────────────────────────────────────

TASK_STATUSES = ("todo", "in_progress", "review", "done", "cancelled")
_next_task_id = [0]

def _gen_task_id():
    with _HUB_LOCK:
        _next_task_id[0] += 1
        return _next_task_id[0]

def _init_task_id_counter():
    """从已有 tasks.json 中恢复 ID 计数器，防止重启后 ID 冲突"""
    tasks = _read_json("tasks")
    max_id = max((t.get("id", 0) for t in tasks), default=0)
    _next_task_id[0] = max_id
    # 顺便去重：保留每个 ID 最后出现的条目
    seen = {}
    for t in tasks:
        tid = t.get("id")
        if tid is not None:
            seen[tid] = t  # 后出现的覆盖先出现的——保留最新状态
    if len(seen) < len(tasks):
        deduped = list(seen.values())
        _write_json("tasks", deduped)

def create_task(title: str, desc: str = "", assignee: str = "",
                priority: str = "medium", created_by: str = "system") -> dict:
    task = {
        "id": _gen_task_id(),
        "title": title,
        "desc": desc,
        "assignee": assignee,
        "status": "todo",
        "priority": priority,
        "created_by": created_by,
        "created_at": time.time(),
        "updated_at": time.time(),
        "comments": [],
    }
    _append_json("tasks", task)
    _emit("task_created", {"task_id": task["id"], "title": title,
                           "assignee": assignee, "created_by": created_by})
    # 自动在 general 频道发布任务公告（飞书风格）
    assignee_text = f" → {assignee}" if assignee else ""
    announce = f"📋 **新任务 #{task['id']}**: {title}{assignee_text}"
    if desc:
        announce += f"\n> {desc[:200]}"
    post_message("general", "📌 TaskBot", announce)
    return {"success": True, "task": task}

def list_tasks(status: str = "", assignee: str = "") -> dict:
    tasks = _read_json("tasks")
    if status:
        tasks = [t for t in tasks if t.get("status") == status]
    if assignee:
        tasks = [t for t in tasks if t.get("assignee") == assignee]
    tasks.reverse()
    return {"success": True, "tasks": tasks, "total": len(tasks)}

def view_task(task_id: int) -> dict:
    tasks = _read_json("tasks")
    for t in tasks:
        if t["id"] == task_id:
            return {"success": True, "task": t}
    return {"success": False, "error": f"任务 #{task_id} 不存在"}

def update_task(task_id: int, updates: dict, operator: str = "") -> dict:
    with _HUB_LOCK:
        p = HUB_DIR / "tasks.json"
        tasks = json.loads(p.read_text(encoding="utf-8")) if p.exists() else []
        found = False
        for t in tasks:
            if t["id"] == task_id:
                for k, v in updates.items():
                    if k in ("status", "assignee", "title", "desc", "priority"):
                        t[k] = v
                t["updated_at"] = time.time()
                if operator:
                    t.setdefault("history", []).append({
                        "action": "update",
                        "changes": updates,
                        "by": operator,
                        "at": time.time(),
                    })
                found = True
                task = t
                break
        if not found:
            return {"success": False, "error": f"任务 #{task_id} 不存在"}
        tmp = p.with_suffix(f".tmp.{os.getpid()}")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(tasks, f, ensure_ascii=False, indent=2)
        tmp.replace(p)
    _emit("task_updated", {"task_id": task_id, "updates": updates, "by": operator})
    return {"success": True, "task": task}

def add_comment(task_id: int, author: str, content: str) -> dict:
    tasks = _read_json("tasks")
    for t in tasks:
        if t["id"] == task_id:
            comment = {
                "author": author,
                "content": content,
                "timestamp": time.time(),
            }
            t.setdefault("comments", []).append(comment)
            t["updated_at"] = time.time()
            _write_json("tasks", tasks)
            _emit("task_commented", {"task_id": task_id, "author": author, "content": content})
            return {"success": True, "comment": comment, "task": t}
    return {"success": False, "error": f"任务 #{task_id} 不存在"}

# ─── 智能体状态墙 ──────────────────────────────────────────

def post_status(agent: str, status_text: str, message: str = "") -> dict:
    entry = {
        "agent": agent,
        "status": status_text,
        "message": message,
        "timestamp": time.time(),
    }
    _append_json("status", entry)
    _emit("status_update", entry)
    return {"success": True, "entry": entry}

def list_status(limit: int = 20) -> dict:
    entries = _read_json("status")
    entries.reverse()
    entries = entries[:limit]
    seen = {}
    for e in entries:
        if e["agent"] not in seen:
            seen[e["agent"]] = e
    return {"success": True, "statuses": list(seen.values())}

# ─── 初始化 ────────────────────────────────────────────────

init()

__all__ = [
    "create_channel", "list_channels", "delete_channel", "post_message", "read_messages",
    "create_task", "list_tasks", "view_task", "update_task", "add_comment",
    "post_status", "list_status",
    "sse_subscribe", "sse_unsubscribe",
    "ws_register", "ws_unregister", "ws_get_online", "ws_broadcast",
]
