#!/usr/bin/env python3
"""
Hermes Team Hub WebSocket 客户端
===============================
飞书式实时全双工通信客户端。零外部依赖，纯标准库实现。

Usage:
    python3 ws_client.py watch                          # 实时监听所有事件
    python3 ws_client.py watch -c general               # 指定频道
    python3 ws_client.py send '{"action":"chat","content":"hello"}'  # 发送消息

作为库使用：
    from ws_client import WebSocketClient
    ws = WebSocketClient("39.104.86.113:8765", "my-agent")
    ws.connect()
    ws.send({"action": "chat", "content": "你好"})
    for event in ws.listen():
        print(event)
"""

import os, sys, json, socket, hashlib, base64, struct, time, select
from urllib.parse import urlparse


class WebSocketClient:
    """纯 Python WebSocket 客户端，零外部依赖"""

    WS_MAGIC = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    RECV_BUF = 65536

    def __init__(self, host: str, token: str = "", name: str = "unknown",
                 channel: str = "", timeout: float = 300.0):
        self.host = host
        self.token = token
        self.name = name
        self.channel = channel or ""
        self.timeout = timeout
        self.sock: socket.socket | None = None
        self._connected = False
        self._recv_buf = b""

    # ─── 连接管理 ─────────────────────────────────────────────

    def connect(self) -> dict | None:
        """建立 WebSocket 连接，返回 connected 事件或 None"""
        # 解析 host:port
        parsed = urlparse(f"//{self.host}")
        hostname = parsed.hostname or self.host.split(":")[0]
        port = parsed.port or 8765

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect((hostname, port))

        # WebSocket 握手
        key = base64.b64encode(os.urandom(16)).decode()
        path = f"/hub/ws?name={self.name}&channel={self.channel}"

        req = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {self.host}\r\n"
            f"Upgrade: websocket\r\n"
            f"Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            f"Sec-WebSocket-Version: 13\r\n"
        )
        if self.token:
            req += f"Authorization: Bearer {self.token}\r\n"
        req += "\r\n"

        self.sock.sendall(req.encode())

        # 读取握手响应
        response = b""
        while b"\r\n\r\n" not in response:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("握手失败：服务器关闭连接")
            response += chunk

        headers = response.split(b"\r\n")
        status_line = headers[0].decode() if headers else ""
        if "101" not in status_line:
            raise ConnectionError(f"WebSocket 握手失败: {status_line}")

        # 验证 Accept
        expected_accept = base64.b64encode(
            hashlib.sha1(key.encode() + self.WS_MAGIC).digest()
        ).decode()
        for h in headers:
            if h.startswith(b"Sec-WebSocket-Accept:"):
                actual = h.split(b":")[1].strip().decode()
                if actual != expected_accept:
                    raise ConnectionError("WebSocket Accept 验证失败")

        self._connected = True

        # 读取 connected 事件
        connected_event = self.recv()
        return connected_event

    def close(self):
        """关闭连接"""
        self._connected = False
        if self.sock:
            try:
                # 发送 close 帧
                frame = bytearray([0x88, 0x00])  # FIN + close opcode, no payload
                self.sock.sendall(bytes(frame))
            except OSError:
                pass
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    @property
    def connected(self) -> bool:
        return self._connected

    # ─── 帧收发 ───────────────────────────────────────────────

    def send(self, data: dict | str) -> bool:
        """发送 JSON 消息（文本帧）"""
        if isinstance(data, dict):
            payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        else:
            payload = data.encode("utf-8")

        frame = bytearray()
        frame.append(0x81)  # FIN + text opcode
        frame.append(0x80)  # MASK bit set (client MUST mask)

        length = len(payload)
        if length < 126:
            frame.append(0x80 | length)
        elif length < 65536:
            frame.append(0x80 | 126)
            frame.extend(struct.pack(">H", length))
        else:
            frame.append(0x80 | 127)
            frame.extend(struct.pack(">Q", length))

        mask_key = os.urandom(4)
        frame.extend(mask_key)

        masked = bytearray(length)
        for i in range(length):
            masked[i] = payload[i] ^ mask_key[i % 4]
        frame.extend(masked)

        try:
            self.sock.sendall(bytes(frame))
            return True
        except OSError:
            self._connected = False
            return False

    def recv(self, timeout: float = None) -> dict | None:
        """接收一个 WebSocket 帧并解析为 JSON dict"""
        if timeout is not None:
            self.sock.settimeout(timeout)

        try:
            while True:
                frame = self._recv_frame()
                if frame is None:
                    return None

                opcode = frame.get("opcode")
                payload = frame.get("payload", b"")

                if opcode == 0x8:  # close
                    self._handle_close(payload)
                    return None
                elif opcode == 0x9:  # ping
                    self._send_pong(payload)
                    continue
                elif opcode == 0xA:  # pong
                    continue
                elif opcode == 0x1:  # text
                    try:
                        return json.loads(payload.decode("utf-8"))
                    except json.JSONDecodeError:
                        return {"type": "raw", "data": payload.decode("utf-8", errors="replace")}
                elif opcode == 0x2:  # binary
                    return {"type": "binary", "size": len(payload)}
        except socket.timeout:
            return None

    def _recv_frame(self) -> dict | None:
        """接收一个 WebSocket 帧"""
        # 读取前 2 字节
        buf = self._recv_exact(2)
        if not buf:
            return None

        b1, b2 = buf[0], buf[1]
        fin = (b1 & 0x80) != 0
        opcode = b1 & 0x0F
        length = b2 & 0x7F

        if length == 126:
            buf = self._recv_exact(2)
            if not buf:
                return None
            length = struct.unpack(">H", buf)[0]
        elif length == 127:
            buf = self._recv_exact(8)
            if not buf:
                return None
            length = struct.unpack(">Q", buf)[0]

        payload = self._recv_exact(length) if length > 0 else b""
        return {"fin": fin, "opcode": opcode, "payload": payload}

    def _recv_exact(self, n: int) -> bytes | None:
        """精确读取 n 字节"""
        try:
            data = self.sock.recv(n)
            while len(data) < n:
                chunk = self.sock.recv(n - len(data))
                if not chunk:
                    return None
                data += chunk
            return data
        except (OSError, socket.timeout):
            return None

    def _send_pong(self, payload: bytes = b""):
        """发送 pong 帧"""
        frame = bytearray([0x8A])  # FIN + pong opcode
        if len(payload) < 126:
            frame.append(0x80 | len(payload))
        frame.extend(os.urandom(4))  # mask
        masked = bytearray(len(payload))
        mask_key = frame[-4:]
        for i in range(len(payload)):
            masked[i] = payload[i] ^ mask_key[i % 4]
        frame.extend(masked)
        try:
            self.sock.sendall(bytes(frame))
        except OSError:
            pass

    def _handle_close(self, payload: bytes):
        """处理关闭帧"""
        self._connected = False

    # ─── 高级 API ─────────────────────────────────────────────

    def listen(self, callback=None):
        """阻塞监听事件流，逐个 yield 事件 dict。
        
        Args:
            callback: 可选回调函数 callback(event)，返回 True 停止监听
        
        Yields:
            每个事件 dict
        """
        while self._connected:
            event = self.recv(timeout=1.0)
            if event is None:
                # 超时，发心跳
                if self._connected:
                    self.send({"action": "ping"})
                continue

            if callback:
                if callback(event):
                    break
            yield event

    def chat(self, content: str, channel: str = "general", sender: str = None):
        """发送聊天消息"""
        return self.send({
            "action": "chat",
            "channel": channel,
            "sender": sender or self.name,
            "content": content,
        })

    def update_status(self, status: str, message: str = ""):
        """更新在线状态"""
        return self.send({
            "action": "status",
            "agent": self.name,
            "status": status,
            "message": message,
        })

    def create_task(self, title: str, desc: str = "", assignee: str = "",
                    priority: str = "medium"):
        """创建任务"""
        return self.send({
            "action": "task_create",
            "title": title,
            "desc": desc,
            "assignee": assignee,
            "priority": priority,
            "created_by": self.name,
        })

    def update_task(self, task_id: int, **kwargs):
        """更新任务"""
        return self.send({
            "action": "task_update",
            "id": task_id,
            "operator": self.name,
            **kwargs,
        })

    def comment_task(self, task_id: int, content: str):
        """评论任务"""
        return self.send({
            "action": "task_comment",
            "id": task_id,
            "author": self.name,
            "content": content,
        })


# ─── CLI 入口 ────────────────────────────────────────────────

def main():
    # 确保实时输出（WebSocket 事件必须即时显示）
    import sys as _sys
    _sys.stdout.reconfigure(line_buffering=True) if hasattr(_sys.stdout, 'reconfigure') else None

    RELAY_URL = os.environ.get("HERMES_RELAY_URL", "39.104.86.113:8765")
    TOKEN = os.environ.get("HERMES_RELAY_TOKEN", "my-relay-secret-2025")
    NAME = os.environ.get("HERMES_AGENT_NAME",
                          os.uname().nodename if hasattr(os, "uname") else "unknown")

    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1]

    if cmd == "watch":
        channel = "general"
        args = sys.argv[2:]
        i = 0
        while i < len(args):
            if args[i] == "-c" and i + 1 < len(args):
                channel = args[i + 1]
                i += 2
            elif args[i] == "-a":
                i += 1  # all channels
                channel = ""
            else:
                i += 1

        ws = WebSocketClient(RELAY_URL, TOKEN, NAME, channel)
        print(f"🔗 正在连接到 {RELAY_URL} ...")
        try:
            connected = ws.connect()
            if connected:
                print(f"✅ 已连接! 协议: WebSocket | 身份: {NAME} | 频道: #{channel or '全部'}")
                print(f"   在线客户端: 等待事件中...")

            event_count = 0
            for event in ws.listen():
                event_count += 1
                ev_type = event.get("type", event.get("_ws_type", "unknown"))
                ts = event.get("timestamp", time.time())

                # 格式化输出
                t_str = time.strftime("%H:%M:%S", time.localtime(ts))

                if ev_type == "chat_message":
                    print(f"\n💬 [{t_str}] #{event.get('channel','')} {event.get('sender','')}:")
                    print(f"   {event.get('content','')}")

                elif ev_type == "task_created":
                    print(f"\n📋 [{t_str}] 新任务 #{event.get('task_id','?')}: {event.get('title','')}")
                    if event.get("assignee"):
                        print(f"   负责人: {event['assignee']}")

                elif ev_type == "task_updated":
                    print(f"\n🔄 [{t_str}] 任务 #{event.get('task_id','?')} 已更新: {event.get('updates',{})}")

                elif ev_type == "task_commented":
                    print(f"\n💬 [{t_str}] 任务 #{event.get('task_id','?')} {event.get('author','')}:")
                    print(f"   {event.get('content','')}")

                elif ev_type == "status_update":
                    print(f"\n📊 [{t_str}] {event.get('agent','')} → [{event.get('status','')}] {event.get('message','')}")

                elif ev_type == "agent_online":
                    print(f"\n🟢 [{t_str}] {event.get('agent','')} 上线了 (在线: {event.get('online_count',0)})")

                elif ev_type == "agent_offline":
                    print(f"\n🔴 [{t_str}] {event.get('agent','')} 离线了 (在线: {event.get('online_count',0)})")

                elif ev_type == "channel_created":
                    print(f"\n➕ [{t_str}] 新频道: #{event.get('channel','')}")

                elif ev_type == "ack":
                    pass  # 静默处理回执

                elif ev_type == "pong":
                    pass  # 静默处理心跳

                else:
                    print(f"\n📡 [{t_str}] {ev_type}: {json.dumps(event, ensure_ascii=False)[:200]}")

        except KeyboardInterrupt:
            print("\n👋 断开连接")
        except Exception as e:
            print(f"\n❌ 错误: {e}")
        finally:
            ws.close()

    elif cmd == "send":
        if len(sys.argv) < 3:
            print("Usage: ws_client.py send '<json>'")
            return
        try:
            msg = json.loads(sys.argv[2])
        except json.JSONDecodeError as e:
            print(f"❌ JSON 解析错误: {e}")
            return

        ws = WebSocketClient(RELAY_URL, TOKEN, NAME)
        try:
            connected = ws.connect()
            if not connected:
                print("❌ 连接失败")
                return
            print(f"✅ 已连接")

            ws.send(msg)
            # 等待 ack
            for event in ws.listen():
                if event.get("type") == "ack":
                    result = event.get("result", {})
                    if result.get("success"):
                        print(f"✅ {event.get('action')} 成功")
                    else:
                        print(f"❌ {result.get('error', '失败')}")
                    break
        except KeyboardInterrupt:
            pass
        finally:
            ws.close()

    elif cmd == "online":
        """查看在线用户"""
        ws = WebSocketClient(RELAY_URL, TOKEN, NAME)
        try:
            connected = ws.connect()
            if not connected:
                print("❌ 连接失败")
                return
            print(f"✅ 已连接，等待在线列表...")

            # 发 ping 获取在线列表
            ws.send({"action": "ping"})

            for event in ws.listen():
                if event.get("type") == "connected":
                    print(json.dumps(event, ensure_ascii=False, indent=2))
                    break
        except KeyboardInterrupt:
            pass
        finally:
            ws.close()

    else:
        print(f"❌ 未知命令: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
