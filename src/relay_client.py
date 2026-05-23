#!/usr/bin/env python3
"""
Hermes Relay Client
===================
在 Hermes Agent 的 terminal 中使用的文件中转 + 团队协作客户端。
每个设备通过此脚本与云服务器协作。

Usage:
    # 文件操作
    python3 relay_client.py upload <本地文件>
    python3 relay_client.py download <文件名> [保存路径]
    python3 relay_client.py list
    python3 relay_client.py delete <文件名>
    python3 relay_client.py send <本地文件> <目标标识>

    # 团队讨论
    python3 relay_client.py chat "消息内容"                # 发消息到 general 频道
    python3 relay_client.py chat -c 项目讨论 "消息内容"     # 发消息到指定频道
    python3 relay_client.py chat -r                         # 读取所有新消息
    python3 relay_client.py channels                        # 查看频道列表

    # 任务管理
    python3 relay_client.py task create "标题" -d "描述" -a 负责人
    python3 relay_client.py task list [--status todo|in_progress|done]
    python3 relay_client.py task view <id>
    python3 relay_client.py task update <id> --status done
    python3 relay_client.py task comment <id> "评论内容"

    # 智能体状态
    python3 relay_client.py status 忙碌中 "正在编译内核"
    python3 relay_client.py status list

环境变量:
    HERMES_RELAY_URL    服务器地址 (默认: http://39.104.86.113:8765)
    HERMES_RELAY_TOKEN  Bearer Token (默认: my-relay-secret-2025)
    HERMES_AGENT_NAME   本设备名称 (默认: 自动检测主机名)
"""

import os, sys, json, time as time_module
import urllib.request, urllib.error, urllib.parse

RELAY_URL = os.environ.get("HERMES_RELAY_URL", "http://39.104.86.113:8765")
AUTH_TOKEN = os.environ.get("HERMES_RELAY_TOKEN", "my-relay-secret-2025")
AGENT_NAME = os.environ.get("HERMES_AGENT_NAME", os.uname().nodename if hasattr(os, 'uname') else "unknown")

# ─── 通用 ──────────────────────────────────────────────────

def _headers():
    return {
        "Authorization": f"Bearer {AUTH_TOKEN}",
        "Content-Type": "application/json",
    }

def _req(method, path, data=None, raw=False):
    url = f"{RELAY_URL}{path}"
    kwargs = {"method": method, "headers": _headers()}
    if data is not None:
        if raw:
            kwargs["headers"]["Content-Type"] = "application/octet-stream"
        else:
            data = json.dumps(data, ensure_ascii=False).encode()
        kwargs["data"] = data
    try:
        resp = urllib.request.urlopen(urllib.request.Request(url, **kwargs))
        if raw:
            return resp.read()
        return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode())
        except:
            return {"error": f"HTTP {e.code}"}

def _print_json(data):
    print(json.dumps(data, ensure_ascii=False, indent=2))

# ─── 文件操作 ──────────────────────────────────────────────

def cmd_list():
    data = _req("GET", "/list")
    files = data.get("files", [])
    if not files:
        print("📭 暂无文件")
        return
    print(f"📂 中继目录 ({data['total']} 个文件):")
    for f in sorted(files, key=lambda x: x["modified"], reverse=True):
        size = f"{f['size']:,} B" if f['size'] < 1024 else f"{f['size']/1024:.1f} KB"
        t = time_module.strftime("%m-%d %H:%M", time_module.localtime(f["modified"]))
        print(f"  {f['name']:<40} {size:<10} {t}")

def cmd_upload(local_path):
    if not os.path.exists(local_path):
        print(f"❌ 文件不存在: {local_path}")
        return
    filename = os.path.basename(local_path)
    with open(local_path, "rb") as f:
        result = _req("POST", f"/upload?filename={filename}", f.read(), raw=True)
    if isinstance(result, bytes):
        result = json.loads(result.decode())
    if result.get("success"):
        print(f"✅ 上传成功: {filename}")
    else:
        print(f"❌ {result.get('error', '上传失败')}")

def cmd_download(filename, save_path=None):
    if not save_path:
        save_path = filename
    try:
        data = _req("GET", f"/download/{filename}")
        if isinstance(data, dict) and "error" in data:
            print(f"❌ {data['error']}")
            return
        with open(save_path, "wb") as f:
            f.write(data if isinstance(data, bytes) else str(data).encode())
        print(f"✅ 下载成功: {filename} → {save_path}")
    except Exception as e:
        print(f"❌ 下载失败: {e}")

def cmd_delete(filename):
    result = _req("POST", "/delete", {"filename": filename})
    if result.get("success"):
        print(f"🗑️ 已删除: {filename}")
    else:
        print(f"❌ {result.get('error', '删除失败')}")

def cmd_send(local_path, target_tag):
    if not os.path.exists(local_path):
        print(f"❌ 文件不存在: {local_path}")
        return
    filename = os.path.basename(local_path)
    tagged = f"[TO:{target_tag}]{filename}"
    with open(local_path, "rb") as f:
        result = _req("POST", f"/upload?filename={tagged}", f.read(), raw=True)
    if isinstance(result, bytes):
        result = json.loads(result.decode())
    if result.get("success"):
        print(f"📤 已发送给 [{target_tag}]: {tagged}")
    else:
        print(f"❌ {result.get('error', '发送失败')}")

# ─── 团队讨论 ──────────────────────────────────────────────

def cmd_chat(args):
    """Usage: relay_client.py chat [-c channel] [-r] [message]"""
    channel = "general"
    read_mode = False
    message = ""

    i = 1
    while i < len(args):
        if args[i] == "-c" and i + 1 < len(args):
            channel = args[i + 1]
            i += 2
        elif args[i] == "-r":
            read_mode = True
            i += 1
        else:
            message = args[i]
            i += 1

    if read_mode:
        encoded_channel = urllib.parse.quote(channel, safe='')
        result = _req("GET", f"/hub/chat?channel={encoded_channel}&limit=20")
        msgs = result.get("messages", [])
        if not msgs:
            print(f"💬 #{channel} 暂无消息")
            return
        print(f"💬 #{channel} (最近 {len(msgs)} 条):")
        for m in msgs:
            t = time_module.strftime("%H:%M", time_module.localtime(m["timestamp"]))
            print(f"  [{t}] {m['sender']}: {m['content']}")
        return

    if not message:
        print("❌ 请输入消息内容或使用 -r 读取消息")
        return

    result = _req("POST", "/hub/chat/post", {
        "channel": channel, "sender": AGENT_NAME, "content": message,
    })
    if result.get("success"):
        print(f"💬 #{channel} <{AGENT_NAME}>: {message}")
    else:
        print(f"❌ {result.get('error', '发送失败')}")

def cmd_channels(args=None):
    """Usage: relay_client.py channels [delete <name>]"""
    if args and len(args) > 2 and args[1] == "delete":
        name = args[2]
        result = _req("POST", "/hub/channel/delete", {"name": name})
        if result.get("success"):
            print(f"🗑️ 已删除频道 #{name}")
        else:
            print(f"❌ {result.get('error', '删除失败')}")
        return

    result = _req("GET", "/hub/channels")
    channels = result.get("channels", [])
    print("📋 频道列表:")
    for c in channels:
        print(f"  #{c['name']} (创建者: {c['created_by']})")

# ─── 任务管理 ──────────────────────────────────────────────

def cmd_task(args):
    if len(args) < 2:
        print("Usage: relay_client.py task create/list/view/update/comment ...")
        return

    sub = args[1]

    if sub == "create":
        title = args[2] if len(args) > 2 else ""
        desc = ""
        assignee = ""
        priority = "medium"
        i = 3
        while i < len(args):
            if args[i] == "-d" and i + 1 < len(args):
                desc = args[i + 1]; i += 2
            elif args[i] == "-a" and i + 1 < len(args):
                assignee = args[i + 1]; i += 2
            elif args[i] == "-p" and i + 1 < len(args):
                priority = args[i + 1]; i += 2
            else:
                i += 1
        if not title:
            print("❌ 需要任务标题")
            return
        result = _req("POST", "/hub/task/create", {
            "title": title, "desc": desc, "assignee": assignee,
            "priority": priority, "created_by": AGENT_NAME,
        })
        if result.get("success"):
            t = result["task"]
            print(f"✅ 任务 #{t['id']} 已创建: {t['title']}")
        else:
            print(f"❌ {result.get('error', '创建失败')}")

    elif sub == "list":
        status = ""
        assignee = ""
        for i in range(2, len(args)):
            if args[i] == "--status" and i + 1 < len(args):
                status = args[i + 1]
            elif args[i] == "--assignee" and i + 1 < len(args):
                assignee = args[i + 1]
        path = f"/hub/tasks?status={status}&assignee={assignee}"
        result = _req("GET", path)
        tasks = result.get("tasks", [])
        if not tasks:
            print("📋 暂无任务")
            return
        print(f"📋 任务列表 ({result['total']}):")
        for t in tasks:
            status_icon = {"todo": "🟢", "in_progress": "🟡", "review": "🔵", "done": "✅", "cancelled": "❌"}
            icon = status_icon.get(t.get("status", ""), "⚪")
            assign = f" → {t.get('assignee', '未分配')}" if t.get("assignee") else ""
            print(f"  {icon} #{t['id']:>3} {t['title'][:50]}{assign}")

    elif sub == "view":
        if len(args) < 3:
            print("❌ usage: task view <id>")
            return
        result = _req("GET", f"/hub/task?id={args[2]}")
        if not result.get("success"):
            print(f"❌ {result['error']}")
            return
        t = result["task"]
        status_icon = {"todo": "🟢待办", "in_progress": "🟡进行中", "review": "🔵审核", "done": "✅已完成", "cancelled": "❌已取消"}
        print(f"📋 任务 #{t['id']}: {t['title']}")
        print(f"   状态: {status_icon.get(t.get('status', ''), t.get('status', ''))}")
        print(f"   负责人: {t.get('assignee', '未分配')}")
        print(f"   优先级: {t.get('priority', 'medium')}")
        print(f"   描述: {t.get('desc', '(无)')}")
        print(f"   创建者: {t.get('created_by', 'system')}")
        comments = t.get("comments", [])
        if comments:
            print(f"   评论 ({len(comments)}):")
            for c in comments:
                ct = time_module.strftime("%H:%M", time_module.localtime(c["timestamp"]))
                print(f"     [{ct}] {c['author']}: {c['content']}")

    elif sub == "update":
        if len(args) < 3:
            print("❌ usage: task update <id> [--status x] [--assignee y]")
            return
        tid = args[2]
        updates = {}
        i = 3
        while i < len(args):
            if args[i] == "--status" and i + 1 < len(args):
                updates["status"] = args[i + 1]; i += 2
            elif args[i] == "--assignee" and i + 1 < len(args):
                updates["assignee"] = args[i + 1]; i += 2
            else:
                i += 1
        updates["id"] = int(tid)
        updates["operator"] = AGENT_NAME
        result = _req("POST", "/hub/task/update", updates)
        if result.get("success"):
            print(f"✅ 任务 #{tid} 已更新")
        else:
            print(f"❌ {result.get('error', '更新失败')}")

    elif sub == "comment":
        if len(args) < 4:
            print("❌ usage: task comment <id> <内容>")
            return
        result = _req("POST", "/hub/task/comment", {
            "id": int(args[2]), "author": AGENT_NAME, "content": " ".join(args[3:]),
        })
        if result.get("success"):
            print(f"✅ 已评论任务 #{args[2]}")
        else:
            print(f"❌ {result.get('error', '评论失败')}")

    else:
        print(f"❌ 未知子命令: {sub}")

# ─── 智能体状态 ──────────────────────────────────────────────

def cmd_status(args):
    if len(args) >= 2 and args[1] == "list":
        result = _req("GET", "/hub/status")
        statuses = result.get("statuses", [])
        if not statuses:
            print("📊 暂无状态更新")
            return
        print("📊 智能体状态:")
        for s in statuses:
            t = time_module.strftime("%H:%M", time_module.localtime(s["timestamp"]))
            print(f"  {s['agent']:<20} [{s['status']}] {s.get('message', '')} ({t})")
        return

    # post status
    status_text = args[1] if len(args) > 1 else "active"
    message = " ".join(args[2:]) if len(args) > 2 else ""
    result = _req("POST", "/hub/status/post", {
        "agent": AGENT_NAME, "status": status_text, "message": message,
    })
    if result.get("success"):
        print(f"📊 {AGENT_NAME} → [{status_text}] {message}")
    else:
        print(f"❌ {result.get('error', '更新失败')}")

# ─── 主入口 ────────────────────────────────────────────────

def cmd_watch(args):
    """实时监听模式（WebSocket）"""
    # 委托给 ws_client 模块
    channel = "general"
    i = 0
    while i < len(args):
        if args[i] == "-c" and i + 1 < len(args):
            channel = args[i + 1]
            i += 2
        elif args[i] == "-a":
            channel = ""
            i += 1
        else:
            i += 1

    try:
        from ws_client import WebSocketClient
    except ImportError:
        print("❌ 需要 ws_client.py 模块")
        print("   curl -s -o ws_client.py -H 'Authorization: Bearer ...' \\")
        print(f"     '{RELAY_URL}/download/ws_client.py'")
        return

    ws = WebSocketClient(
        RELAY_URL.replace("http://", "").replace("https://", ""),
        AUTH_TOKEN, AGENT_NAME, channel,
    )
    print(f"🔗 正在连接到 {RELAY_URL}/hub/ws ...")
    try:
        connected = ws.connect()
        if connected:
            print(f"✅ 已连接! 身份: {AGENT_NAME} | 频道: #{channel or '全部'}")

        for event in ws.listen():
            ev_type = event.get("type", event.get("_ws_type", "unknown"))
            ts = event.get("timestamp", time_module.time())
            t_str = time_module.strftime("%H:%M:%S", time_module.localtime(ts))

            if ev_type == "chat_message":
                print(f"\n💬 [{t_str}] #{event.get('channel','')} {event.get('sender','')}:")
                print(f"   {event.get('content','')}")
            elif ev_type == "task_created":
                print(f"\n📋 [{t_str}] 新任务 #{event.get('task_id','?')}: {event.get('title','')}")
            elif ev_type == "task_updated":
                print(f"\n🔄 [{t_str}] 任务 #{event.get('task_id','?')} 更新: {event.get('updates',{})}")
            elif ev_type == "task_commented":
                print(f"\n💬 [{t_str}] 任务 #{event.get('task_id','?')} {event.get('author','')}: {event.get('content','')}")
            elif ev_type == "status_update":
                print(f"\n📊 [{t_str}] {event.get('agent','')} → [{event.get('status','')}] {event.get('message','')}")
            elif ev_type == "agent_online":
                print(f"\n🟢 [{t_str}] {event.get('agent','')} 上线了 (在线: {event.get('online_count',0)})")
            elif ev_type == "agent_offline":
                print(f"\n🔴 [{t_str}] {event.get('agent','')} 离线了")
            elif ev_type == "channel_created":
                print(f"\n➕ [{t_str}] 新频道: #{event.get('channel','')}")
            elif ev_type == "ack" or ev_type == "pong":
                pass  # 静默
            else:
                print(f"\n📡 [{t_str}] {ev_type}: {json.dumps(event, ensure_ascii=False)[:200]}")
    except KeyboardInterrupt:
        print("\n👋 断开连接")
    finally:
        ws.close()


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1]

    if cmd in ("list", "ls"):
        cmd_list()
    elif cmd == "upload":
        cmd_upload(sys.argv[2] if len(sys.argv) > 2 else "")
    elif cmd == "download":
        cmd_download(sys.argv[2] if len(sys.argv) > 2 else "",
                     sys.argv[3] if len(sys.argv) > 3 else None)
    elif cmd == "delete":
        cmd_delete(sys.argv[2] if len(sys.argv) > 2 else "")
    elif cmd == "send":
        cmd_send(sys.argv[2] if len(sys.argv) > 2 else "",
                 sys.argv[3] if len(sys.argv) > 3 else "")
    elif cmd == "chat":
        cmd_chat(sys.argv[1:])
    elif cmd in ("channels", "channel"):
        cmd_channels(sys.argv[1:])
    elif cmd == "task":
        cmd_task(sys.argv[1:])  # 去掉程序名: ['task', 'list', ...]
    elif cmd == "status":
        cmd_status(sys.argv[1:])
    elif cmd == "watch":
        cmd_watch(sys.argv[2:])  # 实时监听模式
    elif cmd == "stats":
        _print_json(_req("GET", "/stats"))
    else:
        print(f"❌ 未知命令: {cmd}")
        print(__doc__)

if __name__ == "__main__":
    main()
