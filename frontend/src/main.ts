import './style.css'

// ── Types ────────────────────────────────────────────────────────────────────

interface AuthOkMsg {
  event: 'auth_ok'
  node_id: string
  username: string
  is_leader: boolean
  leader_id: string | null
}
interface AuthFailMsg   { event: 'auth_fail';    reason: string }
interface ChatMsg       { event: 'chat_message'; seq: number; sender: string; username: string; text: string; ts: number }
interface MemberJoinMsg { event: 'member_join';  node_id: string; username: string }
interface MemberLeaveMsg{ event: 'member_leave'; node_id: string; username: string }
interface LeaderChangeMsg { event: 'leader_change'; leader_id: string; leader_username: string }
interface ViewUpdateMsg {
  event: 'view_update'
  view: {
    view_id: number
    members: Record<string, { ip: string; tcp_port: number; username: string; joined_at: number }>
  }
}
interface SystemLogMsg  { event: 'system_log';  record: { timestamp: string; node_id: string; event_type: string; level: string; data: Record<string, unknown> } }
interface ErrorMsg      { event: 'error';       reason: string }
interface PongMsg       { event: 'pong';        ts: number }

type ServerMsg = AuthOkMsg | AuthFailMsg | ChatMsg | MemberJoinMsg | MemberLeaveMsg
               | LeaderChangeMsg | ViewUpdateMsg | SystemLogMsg | ErrorMsg | PongMsg

// ── State ────────────────────────────────────────────────────────────────────

const wsPort = new URLSearchParams(location.search).get('port') ?? '8765'
const WS_URL = `ws://${location.hostname}:${wsPort}`

let ws: WebSocket | null = null
let myNodeId = ''
let myUsername = ''
let leaderId: string | null = null
let reconnectTimer: ReturnType<typeof setTimeout> | null = null
const members: Map<string, { username: string }> = new Map()
const LOG_MAX = 200

// ── DOM ──────────────────────────────────────────────────────────────────────

const app = document.getElementById('app')!
app.innerHTML = `
<div id="auth-screen">
  <div class="auth-card">
    <h1>
      <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="color:var(--accent)"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
      Distributed Chat
    </h1>
    <p class="subtitle">LAN peer-to-peer chat with total ordering &nbsp;·&nbsp; node ws:${wsPort}</p>
    <div class="field">
      <label for="inp-room">Room Code</label>
      <input id="inp-room" type="password" placeholder="Enter room code" autocomplete="off" />
    </div>
    <div class="field">
      <label for="inp-user">Username</label>
      <input id="inp-user" type="text" placeholder="Letters, digits, _ or -" maxlength="32" autocomplete="off" />
    </div>
    <button id="btn-join" class="btn-primary">Join Room</button>
    <div id="auth-error" class="auth-error"></div>
  </div>
</div>

<div id="chat-screen">
  <div class="topbar">
    <div class="topbar-title">
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="color:var(--accent);vertical-align:-2px;margin-right:6px"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
      Distributed Chat
    </div>
    <span id="leader-badge" class="leader-badge" style="display:none"></span>
    <span id="topbar-node" class="topbar-node"></span>
    <div id="conn-dot" class="conn-dot"></div>
  </div>

  <div class="chat-body">
    <aside class="sidebar">
      <div class="sidebar-head">Members</div>
      <div id="member-list" class="member-list"></div>
    </aside>

    <div class="messages-col">
      <div id="messages-area" class="messages-area"></div>
      <div class="input-bar">
        <textarea id="msg-input" class="msg-input" rows="1" placeholder="Type a message… (Enter to send)" maxlength="2000"></textarea>
        <button id="btn-send" class="btn-send" disabled>Send</button>
      </div>
    </div>

    <aside class="log-panel">
      <div class="log-panel-head">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
        Event Log
        <button class="log-toggle" id="log-clear">Clear</button>
      </div>
      <div id="log-list" class="log-list"></div>
    </aside>
  </div>
</div>
`

// ── Auth DOM refs ────────────────────────────────────────────────────────────
const authScreen  = document.getElementById('auth-screen')!
const chatScreen  = document.getElementById('chat-screen')!
const inpRoom     = document.getElementById('inp-room') as HTMLInputElement
const inpUser     = document.getElementById('inp-user') as HTMLInputElement
const btnJoin     = document.getElementById('btn-join') as HTMLButtonElement
const authError   = document.getElementById('auth-error')!

// ── Chat DOM refs ────────────────────────────────────────────────────────────
const connDot     = document.getElementById('conn-dot')!
const leaderBadge = document.getElementById('leader-badge')!
const topbarNode  = document.getElementById('topbar-node')!
const memberList  = document.getElementById('member-list')!
const messagesArea= document.getElementById('messages-area')!
const msgInput    = document.getElementById('msg-input') as HTMLTextAreaElement
const btnSend     = document.getElementById('btn-send') as HTMLButtonElement
const logList     = document.getElementById('log-list')!
const logClear    = document.getElementById('log-clear')!

// ── Helpers ──────────────────────────────────────────────────────────────────

function setConnState(state: 'connecting' | 'connected' | 'error') {
  connDot.className = 'conn-dot' + (state === 'connected' ? ' connected' : state === 'error' ? ' error' : '')
}

function showAuthError(msg: string) {
  authError.textContent = msg
  authError.classList.add('show')
}
function hideAuthError() { authError.classList.remove('show') }

function avatarInitial(name: string) {
  return name.charAt(0).toUpperCase()
}

function renderMembers() {
  memberList.innerHTML = ''
  for (const [nid, m] of members) {
    const isSelf = nid === myNodeId
    const isLeader = nid === leaderId
    const el = document.createElement('div')
    el.className = 'member-item' + (isSelf ? ' self' : '')
    el.innerHTML = `
      <div class="member-avatar">${avatarInitial(m.username || '?')}</div>
      <span class="member-name">${esc(m.username || nid.slice(0, 8))}</span>
      ${isLeader ? '<span class="crown-icon" title="Leader">★</span>' : ''}
    `
    memberList.appendChild(el)
  }
}

function updateLeaderBadge() {
  if (leaderId) {
    const lname = members.get(leaderId)?.username || leaderId.slice(0, 8)
    const isMe = leaderId === myNodeId
    leaderBadge.textContent = `★ ${isMe ? 'You are leader' : `Leader: ${lname}`}`
    leaderBadge.style.display = ''
  } else {
    leaderBadge.style.display = 'none'
  }
}

function esc(str: string): string {
  return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;')
}

function formatTime(ts: number): string {
  const d = new Date(ts * 1000)
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

function appendChatMessage(msg: ChatMsg) {
  const isSelf = msg.sender === myNodeId
  const el = document.createElement('div')
  el.className = 'msg-group'
  el.innerHTML = `
    <div class="msg-header">
      <span class="msg-username${isSelf ? ' self-name' : ''}">${esc(msg.username)}</span>
      <span class="msg-ts">${formatTime(msg.ts)}</span>
      <span class="msg-seq">#${msg.seq}</span>
    </div>
    <div class="msg-bubble${isSelf ? ' self-msg' : ''}">${esc(msg.text)}</div>
  `
  messagesArea.appendChild(el)
  scrollToBottom()
}

function appendSystemMsg(text: string) {
  const el = document.createElement('div')
  el.className = 'system-msg'
  el.textContent = text
  messagesArea.appendChild(el)
  scrollToBottom()
}

function scrollToBottom() {
  messagesArea.scrollTop = messagesArea.scrollHeight
}

function appendLog(record: SystemLogMsg['record']) {
  const el = document.createElement('div')
  el.className = 'log-entry'
  const etypeClass = `etype-${record.event_type}`
  const levelClass = record.level
  const dataStr = JSON.stringify(record.data)
  el.innerHTML = `
    <span class="log-etype ${levelClass} ${etypeClass}">${esc(record.event_type)}</span>
    <span class="log-data" title="${esc(dataStr)}">${esc(dataStr)}</span>
  `
  logList.insertBefore(el, logList.firstChild)
  // Trim
  while (logList.children.length > LOG_MAX) {
    logList.removeChild(logList.lastChild!)
  }
}

// ── WebSocket ────────────────────────────────────────────────────────────────

function connect(roomCode: string, username: string) {
  setConnState('connecting')
  ws = new WebSocket(WS_URL)

  ws.onopen = () => {
    setConnState('connecting')
    ws!.send(JSON.stringify({ action: 'auth', room_code: roomCode, username }))
  }

  ws.onmessage = (ev: MessageEvent) => {
    let msg: ServerMsg
    try { msg = JSON.parse(ev.data as string) as ServerMsg }
    catch { return }
    handleMessage(msg)
  }

  ws.onerror = () => {
    setConnState('error')
  }

  ws.onclose = () => {
    setConnState('error')
    ws = null
    // If we were in chat, show reconnect notice and schedule retry
    if (chatScreen.classList.contains('show')) {
      appendSystemMsg('Connection lost. Reconnecting…')
      btnSend.disabled = true
      scheduleReconnect(roomCode, username)
    } else {
      // Auth phase — re-enable the join button
      btnJoin.disabled = false
      btnJoin.textContent = 'Join Room'
      showAuthError('Could not connect to node. Is the server running?')
    }
  }
}

function scheduleReconnect(roomCode: string, username: string) {
  if (reconnectTimer !== null) return
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null
    connect(roomCode, username)
  }, 3000)
}

function handleMessage(msg: ServerMsg) {
  switch (msg.event) {
    case 'auth_ok': {
      myNodeId = msg.node_id
      myUsername = msg.username
      leaderId = msg.leader_id
      members.set(myNodeId, { username: myUsername })
      topbarNode.textContent = `${myUsername}  ·  ${myNodeId.slice(0, 8)}`
      setConnState('connected')
      btnSend.disabled = false
      authScreen.style.display = 'none'
      chatScreen.classList.add('show')
      updateLeaderBadge()
      renderMembers()
      appendSystemMsg(`You joined as ${myUsername}`)
      break
    }
    case 'auth_fail':
      btnJoin.disabled = false
      btnJoin.textContent = 'Join Room'
      showAuthError(msg.reason)
      ws?.close()
      break

    case 'chat_message':
      appendChatMessage(msg)
      break

    case 'member_join':
      members.set(msg.node_id, { username: msg.username })
      appendSystemMsg(`${msg.username} joined`)
      renderMembers()
      break

    case 'member_leave':
      members.delete(msg.node_id)
      appendSystemMsg(`${msg.username} left`)
      renderMembers()
      break

    case 'leader_change':
      leaderId = msg.leader_id
      updateLeaderBadge()
      renderMembers()
      appendSystemMsg(`New leader: ${msg.leader_username}`)
      break

    case 'view_update': {
      const view = msg.view
      const prev = new Map(members)
      members.clear()
      for (const [nid, info] of Object.entries(view.members)) {
        members.set(nid, {
          username: info.username || prev.get(nid)?.username || nid.slice(0, 8),
        })
      }
      renderMembers()
      break
    }

    case 'system_log':
      appendLog(msg.record)
      break

    case 'error':
      appendSystemMsg(`Error: ${msg.reason}`)
      break

    case 'pong':
      break
  }
}

function sendMessage() {
  const text = msgInput.value.trim()
  if (!text || !ws || ws.readyState !== WebSocket.OPEN) return
  ws.send(JSON.stringify({ action: 'message', text }))
  msgInput.value = ''
  msgInput.style.height = 'auto'
}

// ── Ping keepalive ────────────────────────────────────────────────────────────

setInterval(() => {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ action: 'ping' }))
  }
}, 15000)

// ── Event listeners ───────────────────────────────────────────────────────────

btnJoin.addEventListener('click', () => {
  const roomCode = inpRoom.value.trim()
  const username = inpUser.value.trim()
  hideAuthError()
  if (!roomCode) { showAuthError('Enter a room code'); return }
  if (!username) { showAuthError('Enter a username'); return }
  btnJoin.disabled = true
  btnJoin.textContent = 'Connecting…'
  connect(roomCode, username)
})

inpUser.addEventListener('keydown', (e: KeyboardEvent) => {
  if (e.key === 'Enter') btnJoin.click()
})

inpRoom.addEventListener('keydown', (e: KeyboardEvent) => {
  if (e.key === 'Enter') inpUser.focus()
})

msgInput.addEventListener('keydown', (e: KeyboardEvent) => {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault()
    sendMessage()
  }
})

// Auto-resize textarea
msgInput.addEventListener('input', () => {
  msgInput.style.height = 'auto'
  msgInput.style.height = Math.min(msgInput.scrollHeight, 120) + 'px'
})

btnSend.addEventListener('click', sendMessage)

logClear.addEventListener('click', () => {
  logList.innerHTML = ''
})
