#!/usr/bin/env python3
"""
Hermes File Relay Server
======================
跨平台文件中转服务，用于多台 Hermes Agent 设备协作。
云服务器作为中心 Hub，各设备通过 HTTP 上传/下载文件交换数据。

Usage:
    python3 relay_server.py --port 8765 --token mysecret

Auth (可选):
    -H "Authorization: Bearer <token>"

Endpoints:
    GET  /list              → 文件列表
    GET  /download/<file>   → 下载文件
    POST /upload?filename=X → 上传文件 (raw body)
    POST /upload            → 上传文件 (multipart, curl -F)
    POST /delete            → 删除文件
"""

import os, sys, json, hmac, mimetypes, argparse, tempfile, threading, time, queue as queue_module
import hashlib, base64, struct, select
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, unquote, parse_qs

import team_hub  # 团队协作中心模块

RELAY_DIR = Path(os.environ.get("HERMES_RELAY_DIR", "/root/hermes-relay"))
AUTH_TOKEN = os.environ.get("HERMES_RELAY_TOKEN", "")
_FILE_LOCKS = {}  # filename -> threading.Lock
_LOCKS_LOCK = threading.Lock()


def _get_file_lock(filename: str) -> threading.Lock:
    """每个文件一个独立的锁，防止并发写冲突"""
    with _LOCKS_LOCK:
        if filename not in _FILE_LOCKS:
            _FILE_LOCKS[filename] = threading.Lock()
        return _FILE_LOCKS[filename]


class ThreadingRelayServer(ThreadingMixIn, HTTPServer):
    """多线程 HTTP 服务器 — 每个请求独立线程，同时处理多人上传/下载"""
    allow_reuse_address = True
    daemon_threads = True


class RelayHandler(BaseHTTPRequestHandler):
    """HTTP 请求处理器"""

    def _auth(self) -> bool:
        if not AUTH_TOKEN:
            return True  # 无 token 时不鉴权

        # Bearer Token (HTTP header)
        if hmac.compare_digest(
            self.headers.get("Authorization", ""),
            f"Bearer {AUTH_TOKEN}"
        ):
            return True

        # URL query parameter token (for browser WebSocket/SSE)
        params = parse_qs(urlparse(self.path).query)
        if params.get("token", [""])[0] == AUTH_TOKEN:
            return True

        return False

    def _json(self, data: dict, status: int = 200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode())

    def _secure_path(self, filename: str):
        """防止目录穿越"""
        p = (RELAY_DIR / filename).resolve()
        return p if str(p).startswith(str(RELAY_DIR.resolve())) else None

    # ─── SSE 实时推送 ─────────────────────────────────────
    def _handle_sse(self, channel: str):
        """Server-Sent Events 流式推送"""
        q = team_hub.sse_subscribe(channel)
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            # 发送初始连接成功事件
            self.wfile.write(f"event: connected\ndata: {json.dumps({'status': 'ok', 'channel': channel})}\n\n".encode())
            self.wfile.flush()

            while True:
                try:
                    event = q.get(timeout=30)  # 30秒无事件发心跳
                    ev_type = event.pop("type", "event")
                    data = json.dumps(event, ensure_ascii=False)
                    self.wfile.write(f"event: {ev_type}\ndata: {data}\n\n".encode())
                    self.wfile.flush()
                except queue_module.Empty:
                    # 心跳，保持连接
                    self.wfile.write(": heartbeat\n\n".encode())
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            team_hub.sse_unsubscribe(channel, q)
        return None  # 不调用 _json，我们已经自己写了响应

    # ─── WebSocket 实时全双工 ─────────────────────────────────
    WS_MAGIC = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

    def _ws_accept_key(self, key: str) -> str:
        """计算 WebSocket Accept 密钥"""
        digest = hashlib.sha1(key.encode() + self.WS_MAGIC).digest()
        return base64.b64encode(digest).decode()

    def _ws_send(self, data: str):
        """向 WebSocket 客户端发送文本帧"""
        payload = data.encode("utf-8")
        length = len(payload)
        frame = bytearray()
        frame.append(0x81)  # FIN + text opcode
        if length < 126:
            frame.append(length)
        elif length < 65536:
            frame.append(126)
            frame.extend(struct.pack(">H", length))
        else:
            frame.append(127)
            frame.extend(struct.pack(">Q", length))
        frame.extend(payload)
        try:
            self.wfile.write(bytes(frame))
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            raise  # 上层捕获后清理

    def _ws_recv(self) -> str:
        """从 WebSocket 客户端接收文本帧"""
        b1 = self.rfile.read(1)
        if not b1:
            raise ConnectionResetError("Client closed")
        opcode = b1[0] & 0x0F

        b2 = self.rfile.read(1)[0]
        masked = (b2 & 0x80) != 0
        length = b2 & 0x7F

        if length == 126:
            length = struct.unpack(">H", self.rfile.read(2))[0]
        elif length == 127:
            length = struct.unpack(">Q", self.rfile.read(8))[0]

        mask_key = self.rfile.read(4) if masked else None
        payload = bytearray(self.rfile.read(length))

        if mask_key:
            for i in range(length):
                payload[i] ^= mask_key[i % 4]

        if opcode == 0x8:  # close
            raise ConnectionResetError("Client sent close")
        if opcode == 0x9:  # ping
            self._ws_send_pong()
            return self._ws_recv()  # 递归等待下一帧

        return payload.decode("utf-8")

    def _ws_send_pong(self):
        """发送 pong 帧"""
        try:
            self.wfile.write(b"\x8A\x00")
            self.wfile.flush()
        except OSError:
            pass

    def _handle_websocket(self):
        """处理 WebSocket 连接：订阅事件 + 接收客户端消息"""
        # 完成握手
        key = self.headers.get("Sec-WebSocket-Key", "")
        accept = self._ws_accept_key(key)

        self.send_response(101)
        self.send_header("Upgrade", "websocket")
        self.send_header("Connection", "Upgrade")
        self.send_header("Sec-WebSocket-Accept", accept)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        # 获取可选参数
        params = parse_qs(urlparse(self.path).query)
        channel = params.get("channel", [""])[0]
        client_name = params.get("name", ["unknown"])[0]

        # 注册到 WebSocket 广播列表
        team_hub.ws_register(self, channel, client_name)

        # 发送初始连接成功事件（飞书风格的连接确认）
        try:
            self._ws_send(json.dumps({
                "type": "connected",
                "channel": channel,
                "name": client_name,
                "timestamp": time.time(),
                "protocol": "websocket",
            }))
        except OSError:
            team_hub.ws_unregister(self)
            return

        # 事件循环：纯推送模式（客户端消息通过 HTTP 发送）
        # WebSocket 负责服务器→客户端的实时推送
        event_q = team_hub.sse_subscribe(channel)
        try:
            while True:
                try:
                    event = event_q.get(timeout=30)  # 30秒超时心跳
                    ev_type = event.pop("type", "event")
                    event["_ws_type"] = ev_type
                    self._ws_send(json.dumps(event, ensure_ascii=False))
                except queue_module.Empty:
                    # 心跳保持连接
                    self._ws_send(json.dumps({"type": "heartbeat", "timestamp": time.time()}))
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            team_hub.ws_unregister(self)
            team_hub.sse_unsubscribe(channel, event_q)

    def _ws_handle_message(self, msg: dict, sender: str):
        """处理通过 WebSocket 发送的客户端消息"""
        action = msg.get("action", "")
        try:
            if action == "chat":
                result = team_hub.post_message(
                    msg.get("channel", "general"),
                    msg.get("sender", sender),
                    msg.get("content", ""))
            elif action == "task_create":
                result = team_hub.create_task(
                    title=msg.get("title", ""),
                    desc=msg.get("desc", ""),
                    assignee=msg.get("assignee", ""),
                    priority=msg.get("priority", "medium"),
                    created_by=msg.get("created_by", sender))
            elif action == "task_update":
                updates = {k: v for k, v in msg.items()
                           if k in ("status", "assignee", "title", "desc", "priority") and v}
                result = team_hub.update_task(
                    int(msg.get("id", 0)), updates,
                    operator=msg.get("operator", sender))
            elif action == "task_comment":
                result = team_hub.add_comment(
                    int(msg.get("id", 0)),
                    msg.get("author", sender),
                    msg.get("content", ""))
            elif action == "status":
                result = team_hub.post_status(
                    msg.get("agent", sender),
                    msg.get("status", ""),
                    msg.get("message", ""))
            elif action == "ping":
                self._ws_send(json.dumps({"type": "pong", "timestamp": time.time()}))
                return
            else:
                result = {"success": False, "error": f"未知操作: {action}"}

            # 回执
            self._ws_send(json.dumps({
                "type": "ack",
                "action": action,
                "result": result,
                "timestamp": time.time(),
            }, ensure_ascii=False))
        except Exception as e:
            self._ws_send(json.dumps({
                "type": "error",
                "action": action,
                "error": str(e),
            }))

    # ─── Web 面板 ─────────────────────────────────────────
    DASHBOARD_HTML = """\
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Hermes 团队协作面板</title>
<link rel="stylesheet" href="/hub/app.css">
</head>
<body>
<div id="app">
  <header>
    <h1>🤝 Hermes 团队协作中心</h1>
    <div class="status-bar">
      <span id="conn-status">🔴 未连接</span>
      <span id="event-count">事件: 0</span>
    </div>
  </header>
  <main>
    <aside>
      <section>
        <h2>📊 智能体状态</h2>
        <div id="agent-statuses"></div>
      </section>
      <section>
        <h2>📋 任务列表</h2>
        <div id="task-list"></div>
      </section>
    </aside>
    <section class="chat-area">
      <h2>💬 讨论区 — #<span id="current-channel">general</span></h2>
      <div id="chat-messages"></div>
      <div class="chat-input">
        <input type="text" id="sender-name" placeholder="你的名称">
        <input type="text" id="msg-input" placeholder="输入消息..." autofocus>
        <button onclick="sendMsg()">发送</button>
      </div>
      <div class="channel-bar">
        <span>频道:</span>
        <span id="channel-list"></span>
        <button onclick="createChannel()">+新建</button>
      </div>
    </section>
  </main>
</div>
<script src="/hub/app.js"></script>
</body>
</html>"""

    def _serve_dashboard(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(self.DASHBOARD_HTML.encode())

    DASHBOARD_CSS = """\
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       background: #0f172a; color: #e2e8f0; height: 100vh; overflow: hidden; }
#app { display: flex; flex-direction: column; height: 100vh; }
header { background: #1e293b; padding: 12px 20px; display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #334155; }
header h1 { font-size: 18px; }
.status-bar { display: flex; gap: 16px; font-size: 13px; color: #94a3b8; }
main { display: flex; flex: 1; overflow: hidden; }
aside { width: 320px; background: #1e293b; border-right: 1px solid #334155; display: flex; flex-direction: column; overflow-y: auto; }
aside section { padding: 16px; border-bottom: 1px solid #334155; }
aside h2 { font-size: 14px; color: #94a3b8; margin-bottom: 8px; }
.agent-card { background: #334155; border-radius: 8px; padding: 10px; margin-bottom: 8px; }
.agent-card .name { font-weight: 600; font-size: 14px; }
.agent-card .status { color: #22c55e; font-size: 12px; }
.agent-card .msg { color: #94a3b8; font-size: 12px; margin-top: 4px; }
.task-card { background: #334155; border-radius: 8px; padding: 10px; margin-bottom: 6px; font-size: 13px; }
.task-card .task-title { font-weight: 500; }
.task-card .task-meta { color: #94a3b8; font-size: 11px; margin-top: 4px; display: flex; justify-content: space-between; }
.chat-area { flex: 1; display: flex; flex-direction: column; }
.chat-area h2 { padding: 12px 20px; font-size: 15px; border-bottom: 1px solid #334155; }
#chat-messages { flex: 1; overflow-y: auto; padding: 16px 20px; }
.msg { margin-bottom: 12px; }
.msg .sender { font-weight: 600; font-size: 13px; color: #60a5fa; }
.msg .time { color: #64748b; font-size: 11px; margin-left: 8px; }
.msg .content { color: #e2e8f0; font-size: 14px; margin-top: 2px; }
.chat-input { display: flex; gap: 8px; padding: 12px 20px; border-top: 1px solid #334155; background: #1e293b; }
.chat-input input { flex: 1; background: #334155; border: 1px solid #475569; border-radius: 6px; padding: 8px 12px; color: #e2e8f0; font-size: 14px; outline: none; }
.chat-input input:focus { border-color: #60a5fa; }
.chat-input #sender-name { flex: 0 0 120px; }
.chat-input button { background: #3b82f6; color: white; border: none; border-radius: 6px; padding: 8px 20px; cursor: pointer; font-size: 14px; }
.channel-bar { padding: 8px 20px; border-top: 1px solid #334155; display: flex; gap: 8px; align-items: center; font-size: 13px; background: #1e293b; }
.channel-bar span { cursor: pointer; padding: 4px 10px; border-radius: 4px; }
.channel-bar span:hover { background: #334155; }
.channel-bar .active { background: #3b82f6; color: white; }
.channel-bar button { background: transparent; border: 1px solid #475569; color: #94a3b8; padding: 4px 10px; border-radius: 4px; cursor: pointer; font-size: 12px; }
.modal-overlay { position: fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.6); z-index:1000; display:flex; align-items:center; justify-content:center; }
.modal-content { background:#1e293b; border-radius:12px; max-width:600px; width:90%; max-height:80vh; overflow-y:auto; box-shadow:0 8px 32px rgba(0,0,0,0.4); }
.modal-header { display:flex; justify-content:space-between; align-items:center; padding:16px 20px; border-bottom:1px solid #334155; font-size:16px; font-weight:600; }
.modal-close { cursor:pointer; color:#94a3b8; font-size:20px; padding:4px 8px; border-radius:4px; }
.modal-close:hover { background:#334155; color:#e2e8f0; }
.modal-body { padding:16px 20px; }
.detail-row { display:flex; margin-bottom:8px; font-size:14px; }
.detail-row label { width:80px; color:#94a3b8; flex-shrink:0; }
.detail-desc { margin-top:12px; }
.detail-desc label { display:block; color:#94a3b8; font-size:13px; margin-bottom:4px; }
.detail-desc div { background:#334155; padding:10px 12px; border-radius:6px; font-size:13px; line-height:1.5; white-space:pre-wrap; }
.detail-section { margin-top:16px; padding-top:12px; border-top:1px solid #334155; }
.detail-section label { font-size:13px; color:#94a3b8; font-weight:600; }
.detail-comment { background:#334155; border-radius:6px; padding:8px 10px; margin-top:6px; font-size:13px; }
.comment-author { font-weight:600; color:#60a5fa; }
.comment-time { color:#64748b; font-size:11px; margin-left:6px; }
.comment-body { margin-top:2px; color:#e2e8f0; white-space:pre-wrap; }
.detail-history { font-size:12px; color:#94a3b8; margin-top:4px; padding:4px 8px; background:#334155; border-radius:4px; }

"""

    def _serve_dashboard_css(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/css; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(self.DASHBOARD_CSS.encode())

    DASHBOARD_JS = """\
const BASE = '';
const AUTH = 'Bearer my-relay-secret-2025';
const headers = { 'Authorization': AUTH, 'Content-Type': 'application/json' };
const WS_TOKEN = 'my-relay-secret-2025';

let currentChannel = 'general';
let lastMsgId = 0;
let ws = null;
let wsReconnectTimer = null;

// --- WebSocket realtime (Feishu-style full-duplex) ---
function connectWS() {
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = protocol + '//' + location.host + '/hub/ws?token=' + WS_TOKEN + '&name=dashboard&channel=' + currentChannel;
    try { ws = new WebSocket(wsUrl); } catch(e) {
        document.getElementById('conn-status').textContent = '🔴 WS not supported';
        fallbackToPolling(); return;
    }
    ws.onopen = function() {
        document.getElementById('conn-status').innerHTML = '🟢 Realtime (WebSocket)';
        loadChannels(); loadMessages(); loadTasks(); loadStatuses(); loadOnlineUsers();
    };
    ws.onmessage = function(e) {
        try {
            var data = JSON.parse(e.data);
            var evType = data.type || data._ws_type;
            updateEventCount();
            if (evType === 'connected') {
                document.getElementById('conn-status').innerHTML = '🟢 Realtime (online: ' + (data.online_count || '?') + ')';
            } else if (evType === 'chat_message') {
                if (data.channel === currentChannel) addMessage(data);
            } else if (evType === 'task_created' || evType === 'task_updated') {
                loadTasks();
            } else if (evType === 'status_update' || evType === 'agent_online' || evType === 'agent_offline') {
                loadStatuses(); loadOnlineUsers();
            } else if (evType === 'channel_created') {
                loadChannels();
            }
        } catch(ex) { console.error('WS parse error:', ex); }
    };
    ws.onerror = function() { document.getElementById('conn-status').textContent = '🔴 WS error'; };
    ws.onclose = function() {
        document.getElementById('conn-status').textContent = '🟡 Reconnecting...';
        wsReconnectTimer = setTimeout(connectWS, 3000);
    };
}

function fallbackToPolling() {
    document.getElementById('conn-status').textContent = '🟡 Polling (5s)';
    setInterval(loadMessages, 5000);
    setInterval(loadTasks, 10000);
    setInterval(loadStatuses, 10000);
}

var eventCount = 0;
function updateEventCount() {
    document.getElementById('event-count').textContent = 'Events: ' + (++eventCount);
}

async function loadOnlineUsers() {
    try {
        var r = await fetch(BASE + '/hub/online', { headers });
        var d = await r.json();
        if (d.online) {
            var names = d.online.map(function(u) { return u.name; }).join(', ');
            document.getElementById('conn-status').innerHTML = '🟢 Realtime | Online: ' + (names || 'none') + ' (' + d.count + ')';
        }
    } catch(e) {}
}

async function loadChannels() {
    var r = await fetch(BASE + '/hub/channels', { headers });
    var d = await r.json();
    var container = document.getElementById('channel-list');
    container.innerHTML = d.channels.map(function(c) {
        return '<span class="' + (c.name === currentChannel ? 'active' : '') + '" onclick="switchChannel(\'' + c.name + '\')">#' + c.name + '</span>';
    }).join('');
}

async function loadMessages() {
    var r = await fetch(BASE + '/hub/chat?channel=' + currentChannel + '&limit=50', { headers });
    var d = await r.json();
    var container = document.getElementById('chat-messages');
    container.innerHTML = d.messages.map(function(m) {
        lastMsgId = Math.max(lastMsgId, m.id || 0);
        return formatMessage(m);
    }).join('');
    container.scrollTop = container.scrollHeight;
}

async function loadTasks() {
    var r = await fetch(BASE + '/hub/tasks', { headers });
    var d = await r.json();
    var container = document.getElementById('task-list');
    container.innerHTML = d.tasks.map(function(t) {
        var icons = {todo:'🟢', in_progress:'🟡', review:'🔵', done:'✅', cancelled:'❌'};
        var labels = {todo:'待办', in_progress:'进行中', review:'审核', done:'已完成', cancelled:'已取消'};
        var i = (icons[t.status]||'');
        var label = labels[t.status] || t.status;
        return '<div class="task-card" onclick="showTaskDetail(' + t.id + ')" style="cursor:pointer">'
            + '<div class="task-title">' + i + ' #' + t.id + ' ' + escapeHtml(t.title) + '</div>'
            + '<div class="task-meta">'
            + '<span>' + escapeHtml(t.assignee || '未分配') + '</span>'
            + '<span>' + label + '</span>'
            + '</div></div>';
    }).join('');
}

function showTaskDetail(taskId) {
    fetch(BASE + '/hub/task?id=' + taskId, { headers })
    .then(function(r) { return r.json(); })
    .then(function(d) {
        if (!d.success) { alert(d.error); return; }
        var t = d.task;
        var icons = {todo:'🟢', in_progress:'🟡', review:'🔵', done:'✅', cancelled:'❌'};
        var labels = {todo:'待办', in_progress:'进行中', review:'审核', done:'已完成', cancelled:'已取消'};
        var html = '<div class="modal-overlay" onclick="closeTaskDetail()">'
            + '<div class="modal-content" onclick="event.stopPropagation()">'
            + '<div class="modal-header">'
            + '<span>' + (icons[t.status]||'') + ' #' + t.id + ' ' + escapeHtml(t.title) + '</span>'
            + '<span class="modal-close" onclick="closeTaskDetail()">✕</span>'
            + '</div>'
            + '<div class="modal-body">'
            + '<div class="detail-row"><label>状态</label><span>' + (labels[t.status] || t.status) + '</span></div>'
            + '<div class="detail-row"><label>负责人</label><span>' + escapeHtml(t.assignee || '未分配') + '</span></div>'
            + '<div class="detail-row"><label>优先级</label><span>' + (t.priority || 'medium') + '</span></div>'
            + '<div class="detail-row"><label>创建者</label><span>' + escapeHtml(t.created_by || 'system') + '</span></div>'
            + '<div class="detail-row"><label>创建时间</label><span>' + (t.created_at ? new Date(t.created_at*1000).toLocaleString() : '-') + '</span></div>';
        if (t.desc) {
            html += '<div class="detail-desc"><label>描述</label><div>' + escapeHtml(t.desc).replace(/\n/g, '<br>') + '</div></div>';
        }
        if (t.comments && t.comments.length) {
            html += '<div class="detail-section"><label>评论 (' + t.comments.length + ')</label></div>';
            t.comments.forEach(function(c) {
                var ct = new Date(c.timestamp*1000).toLocaleString();
                html += '<div class="detail-comment"><span class="comment-author">' + escapeHtml(c.author) + '</span> <span class="comment-time">' + ct + '</span><div class="comment-body">' + escapeHtml(c.content) + '</div></div>';
            });
        }
        if (t.history && t.history.length) {
            html += '<div class="detail-section"><label>变更历史 (' + t.history.length + ')</label></div>';
            t.history.slice(-5).reverse().forEach(function(h) {
                var ht = new Date(h.at*1000).toLocaleString();
                var changes = Object.keys(h.changes).map(function(k) { return k + ': ' + h.changes[k]; }).join(', ');
                html += '<div class="detail-history">[' + ht + '] ' + escapeHtml(h.by || '?') + ' — ' + escapeHtml(changes) + '</div>';
            });
        }
        html += '</div></div></div>';
        var modal = document.getElementById('task-modal');
        if (!modal) {
            modal = document.createElement('div');
            modal.id = 'task-modal';
            document.body.appendChild(modal);
        }
        modal.innerHTML = html;
        modal.style.display = 'block';
    });
}

function closeTaskDetail() {
    var modal = document.getElementById('task-modal');
    if (modal) modal.style.display = 'none';
}

async function loadStatuses() {
    var r = await fetch(BASE + '/hub/status', { headers });
    var d = await r.json();
    var container = document.getElementById('agent-statuses');
    container.innerHTML = d.statuses.map(function(s) {
        return '<div class="agent-card"><div class="name">' + s.agent + '</div><div class="status">🟢 ' + s.status + '</div>' + (s.message ? '<div class="msg">' + s.message + '</div>' : '') + '</div>';
    }).join('');
}

function addMessage(m) {
    var container = document.getElementById('chat-messages');
    container.insertAdjacentHTML('beforeend', formatMessage(m));
    container.scrollTop = container.scrollHeight;
}

function formatMessage(m) {
    var t = new Date(m.timestamp * 1000).toLocaleTimeString();
    return '<div class="msg"><span class="sender">' + m.sender + '</span><span class="time">' + t + '</span><div class="content">' + escapeHtml(m.content) + '</div></div>';
}

function escapeHtml(s) {
    var div = document.createElement('div');
    div.textContent = s;
    return div.innerHTML;
}

async function sendMsg() {
    var sender = document.getElementById('sender-name').value || 'anonymous';
    var content = document.getElementById('msg-input').value;
    if (!content) return;
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ action: 'chat', channel: currentChannel, sender: sender, content: content }));
    } else {
        await fetch(BASE + '/hub/chat/post', { method: 'POST', headers: headers, body: JSON.stringify({ channel: currentChannel, sender: sender, content: content }) });
    }
    document.getElementById('msg-input').value = '';
}

async function switchChannel(name) {
    currentChannel = name;
    document.getElementById('current-channel').textContent = name;
    loadChannels();
    loadMessages();
    if (ws) { ws.close(); ws = null; }
    clearTimeout(wsReconnectTimer);
    connectWS();
}

async function createChannel() {
    var name = prompt('New channel name:');
    if (!name) return;
    await fetch(BASE + '/hub/channel/create', { method: 'POST', headers: headers, body: JSON.stringify({ name: name, created_by: 'dashboard' }) });
    loadChannels();
}

document.addEventListener('DOMContentLoaded', function() {
    document.getElementById('msg-input').addEventListener('keydown', function(e) {
        if (e.key === 'Enter') sendMsg();
    });
});

// Init: WebSocket first, polling as fallback
connectWS();
loadChannels();
loadMessages();
loadTasks();
loadStatuses();
loadOnlineUsers();
window.addEventListener('offline', fallbackToPolling);

"""
    def _serve_dashboard_js(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/javascript; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(self.DASHBOARD_JS.encode())

    def do_GET(self):
        if not self._auth():
            return self._json({"error": "Unauthorized"}, 401)

        path = unquote(urlparse(self.path).path)

        # 文件列表
        if path in ("/list", "/"):
            files = []
            for f in sorted(RELAY_DIR.iterdir()):
                if f.is_file():
                    files.append({
                        "name": f.name,
                        "size": f.stat().st_size,
                        "modified": f.stat().st_mtime,
                    })
            return self._json({
                "success": True,
                "files": files,
                "dir": str(RELAY_DIR),
                "total": len(files),
            })

        # 下载文件
        # 并发统计
        if path == "/stats":
            active = threading.active_count()
            return self._json({
                "success": True,
                "server": "threaded",
                "active_threads": active,
                "files_cached": len(_FILE_LOCKS),
                "file_count": sum(1 for _ in RELAY_DIR.iterdir() if _.is_file()),
            })

        # ─── 团队协作 Hub GET ──────────────────────────────
        if path == "/hub/channels":
            return self._json(team_hub.list_channels())

        if path == "/hub/chat":
            params = parse_qs(urlparse(self.path).query)
            channel = params.get("channel", ["general"])[0]
            since = int(params.get("since", ["0"])[0])
            limit = int(params.get("limit", ["50"])[0])
            return self._json(team_hub.read_messages(channel, since, limit))

        if path == "/hub/tasks":
            params = parse_qs(urlparse(self.path).query)
            status = params.get("status", [""])[0]
            assignee = params.get("assignee", [""])[0]
            return self._json(team_hub.list_tasks(status, assignee))

        if path == "/hub/task":
            params = parse_qs(urlparse(self.path).query)
            tid = int(params.get("id", ["0"])[0])
            return self._json(team_hub.view_task(tid))

        if path == "/hub/status":
            return self._json(team_hub.list_status())

        # ─── 共享记忆 ────────────────────────────────────────
        if path == "/hub/memory/shared":
            params = parse_qs(urlparse(self.path).query)
            scope = params.get("scope", [""])[0]
            since = float(params.get("since", ["0"])[0])
            limit = int(params.get("limit", ["200"])[0])
            return self._json(team_hub.list_shared_memory(scope, since, limit))

        # ─── 共享技能 ────────────────────────────────────────
        if path == "/hub/skills/shared":
            return self._json(team_hub.list_shared_skills())

        if path.startswith("/hub/skill/shared/"):
            name = path[len("/hub/skill/shared/"):]
            return self._json(team_hub.get_shared_skill(name))

        if path == "/hub/online":
            # 飞书风格在线状态 API
            online = team_hub.ws_get_online()
            return self._json({
                "success": True,
                "online": online,
                "count": len(online),
                "protocol": "websocket",
            })

        # ─── WebSocket 实时事件流 ────────────────────────────────
        if path == "/hub/ws":
            # 检查是否为 WebSocket Upgrade
            upgrade = self.headers.get("Upgrade", "").lower()
            if upgrade == "websocket":
                return self._handle_websocket()
            # 非 WebSocket GET → 返回信息
            return self._json({
                "endpoint": "/hub/ws",
                "protocol": "websocket",
                "description": "飞书式实时全双工通信端点",
                "usage": "ws://<host>:<port>/hub/ws?name=AGENT&channel=general",
            })

        # ─── SSE 实时事件流 ──────────────────────────────────
        if path == "/hub/events":
            params = parse_qs(urlparse(self.path).query)
            channel = params.get("channel", [""])[0]
            return self._handle_sse(channel)

        # ─── Web 面板 ───────────────────────────────────────
        if path in ("/dashboard", "/hub", "/hub/"):
            return self._serve_dashboard()

        if path == "/hub/app.js":
            return self._serve_dashboard_js()

        if path == "/hub/app.css":
            return self._serve_dashboard_css()

        if path.startswith("/download/"):
            filename = path[len("/download/"):]
            fp = self._secure_path(filename)
            if not fp or not fp.is_file():
                return self._json({"error": "File not found"}, 404)

            ct, _ = mimetypes.guess_type(fp.name)
            self.send_response(200)
            self.send_header("Content-Type", ct or "application/octet-stream")
            self.send_header("Content-Disposition", f'attachment; filename="{fp.name}"')
            self.send_header("Content-Length", str(fp.stat().st_size))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            with open(fp, "rb") as f:
                self.wfile.write(f.read())
            return

        self._json({"error": "Not found"}, 404)

    def do_POST(self):
        if not self._auth():
            return self._json({"error": "Unauthorized"}, 401)

        path = unquote(urlparse(self.path).path)
        cl = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(cl) if cl else b""

        # ---- 上传 ----
        if path == "/upload":
            ct = self.headers.get("Content-Type", "")
            query = parse_qs(urlparse(self.path).query)
            filename = query.get("filename", [None])[0]

            # Multipart 上传 (curl -F)
            if "multipart/form-data" in ct:
                boundary = ct.split("boundary=")[-1].strip()
                for part in body.split(f"--{boundary}".encode()):
                    hdr_end = part.find(b"\r\n\r\n")
                    if hdr_end == -1:
                        continue
                    raw_hdr = part[:hdr_end].decode(errors="replace")
                    content = part[hdr_end + 4:]
                    content = content.rstrip(b"\r\n--\r\n")
                    content = content.rstrip(b"\r\n--")
                    content = content.rstrip(b"\r\n")

                    if 'filename="' in raw_hdr:
                        fn = raw_hdr.split('filename="')[1].split('"')[0]
                        fp = self._secure_path(fn)
                        if fp:
                            lock = _get_file_lock(fn)
                            with lock:
                                # 原子写入: 先写 temp 文件，再 rename
                                tmp = fp.with_suffix(f".tmp.{os.getpid()}")
                                with open(tmp, "wb") as f:
                                    f.write(content)
                                tmp.replace(fp)
                            return self._json({
                                "success": True,
                                "file": fn,
                                "size": len(content),
                                "message": f"已上传 {fn}",
                            })
                return self._json({"error": "No file in multipart"}, 400)

            # Raw body 上传 (curl -X POST --data-binary)
            if not filename:
                return self._json({"error": "需要 ?filename= 参数"}, 400)

            fp = self._secure_path(filename)
            if not fp:
                return self._json({"error": "文件名不合法"}, 403)

            lock = _get_file_lock(filename)
            with lock:
                tmp = fp.with_suffix(f".tmp.{os.getpid()}")
                with open(tmp, "wb") as f:
                    f.write(body)
                tmp.replace(fp)

            return self._json({
                "success": True,
                "file": filename,
                "size": len(body),
                "message": f"已上传 {filename}",
            })

        # ---- 删除 ----
        if path == "/delete":
            data = json.loads(body) if body else {}
            fn = data.get("filename", "")
            fp = self._secure_path(fn)
            if not fp:
                return self._json({"error": "文件名不合法"}, 403)
            if fp.exists():
                fp.unlink()
            return self._json({"success": True, "message": f"已删除 {fn}"})

        # ─── 团队协作 Hub POST ─────────────────────────────
        data = json.loads(body) if body else {}

        if path == "/hub/channel/create":
            return self._json(team_hub.create_channel(
                data.get("name", ""), data.get("created_by", "agent")))

        if path == "/hub/channel/delete":
            return self._json(team_hub.delete_channel(
                data.get("name", "")))

        if path == "/hub/chat/post":
            return self._json(team_hub.post_message(
                data.get("channel", "general"),
                data.get("sender", "anonymous"),
                data.get("content", "")))

        if path == "/hub/task/create":
            return self._json(team_hub.create_task(
                title=data.get("title", ""),
                desc=data.get("desc", ""),
                assignee=data.get("assignee", ""),
                priority=data.get("priority", "medium"),
                created_by=data.get("created_by", "agent")))

        if path == "/hub/task/update":
            return self._json(team_hub.update_task(
                int(data.get("id", 0)),
                {k: v for k, v in data.items() if k in ("status", "assignee", "title", "desc", "priority") and v},
                operator=data.get("operator", "")))

        if path == "/hub/task/comment":
            return self._json(team_hub.add_comment(
                int(data.get("id", 0)),
                data.get("author", ""),
                data.get("content", "")))

        if path == "/hub/status/post":
            return self._json(team_hub.post_status(
                data.get("agent", ""),
                data.get("status", ""),
                data.get("message", "")))

        # ─── 共享记忆 POST ────────────────────────────────────
        if path == "/hub/memory/shared":
            return self._json(team_hub.add_shared_memory(
                key=data.get("key", ""),
                value=data.get("value", ""),
                author=data.get("author", ""),
                scope=data.get("scope", "all")))

        # ─── 共享技能 POST ────────────────────────────────────
        if path == "/hub/skills/shared":
            return self._json(team_hub.upload_shared_skill(
                name=data.get("name", ""),
                content=data.get("content", ""),
                description=data.get("description", ""),
                author=data.get("author", ""),
                version=data.get("version", 1)))

        if path == "/hub/skill/shared/delete":
            return self._json(team_hub.delete_shared_skill(
                data.get("name", "")))

        self._json({"error": "Not found"}, 404)

    def do_DELETE(self):
        """DELETE /<file> 快捷删除"""
        if not self._auth():
            return self._json({"error": "Unauthorized"}, 401)
        path = unquote(urlparse(self.path).path)
        fn = path.lstrip("/")
        fp = self._secure_path(fn)
        if not fp:
            return self._json({"error": "Invalid filename"}, 403)
        if fp.exists():
            fp.unlink()
        return self._json({"success": True, "message": f"Deleted {fn}"})

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
        self.end_headers()

    def log_message(self, fmt, *args):
        sys.stderr.write(f"[Relay] {self.client_address[0]} - {args[1]} {args[2]}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hermes File Relay Server")
    parser.add_argument("--port", type=int, default=8765, help="监听端口")
    parser.add_argument("--dir", default=str(RELAY_DIR), help="文件存储目录")
    parser.add_argument("--token", default="", help="Bearer token (留空自动生成)")
    args = parser.parse_args()

    RELAY_DIR = Path(args.dir)
    RELAY_DIR.mkdir(parents=True, exist_ok=True)

    # 初始化 Team Hub（恢复任务 ID 计数器、创建默认频道等）
    team_hub.init()

    if args.token:
        AUTH_TOKEN = args.token
    elif not AUTH_TOKEN:
        AUTH_TOKEN = os.urandom(16).hex()

    server = ThreadingRelayServer(("0.0.0.0", args.port), RelayHandler)
    print(f"""
╔══════════════════════════════════════════════╗
║     Hermes 文件 + 团队协作中心 (多线程)       ║
╠══════════════════════════════════════════════╣
║  地址: http://0.0.0.0:{args.port:<5d}              ║
║  目录: {RELAY_DIR}  ║
║  Token: {AUTH_TOKEN}  ║
╠══════════════════════════════════════════════╣
║  API:                                        ║
║  文件:                                       ║
║  GET  /list          → 文件列表              ║
║  GET  /download/xxx  → 下载文件              ║
║  POST /upload        → 上传 (curl -F 或 raw) ║
║  POST /delete        → 删除文件              ║
║  协作中心:                                   ║
║  GET  /hub/channels  → 频道列表              ║
║  GET  /hub/chat      → 读取消息              ║
║  POST /hub/chat/post → 发送消息              ║
║  GET  /hub/tasks     → 任务列表              ║
║  POST /hub/task/create → 创建任务             ║
║  POST /hub/task/update → 更新任务             ║
║  POST /hub/task/comment → 任务评论            ║
║  GET  /hub/status    → 智能体状态            ║
║  POST /hub/status/post → 更新状态            ║
╠══════════════════════════════════════════════╣
║  认证: curl -H 'Authorization: Bearer {AUTH_TOKEN}'  ║
╚══════════════════════════════════════════════╝""")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[Relay] 服务器已停止")
        server.server_close()
