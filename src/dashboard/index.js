\
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