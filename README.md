# RAATHIRI PADAM — Watch Party Server

Real-time synchronized movie watching for groups.

## What was fixed

| Problem | Fix |
|---|---|
| Black screen for viewers | Replaced Supabase realtime (unreliable, no server) with Socket.IO — server relays all events instantly |
| No real backend | Added `server.js` — Node.js + Express + Socket.IO |
| Host not enforced | Server assigns host to **first joiner**, transfers on disconnect |
| WebRTC broken | Fixed offer/answer flow; signaling now uses Socket.IO relay (reliable) |
| No invite link | Room code embedded in URL (`?room=XXXX`) — share the link, code auto-fills |
| Play/Pause drift | 5-second heartbeat + drift correction (>3s triggers resync) |
| Late joiners miss state | Server caches room state; new viewers auto-get a sync push from host |
| Duplicate `cancelTorrent` | Removed duplicate function |
| Supabase dependency | Completely removed — self-hosted Socket.IO instead |

## Setup

```bash
# 1. Install dependencies
npm install

# 2. Start server
npm start
# → Server running at http://localhost:3000

# 3. Open in browser
# http://localhost:3000
```

## How it works

```
HOST                          SERVER                         VIEWERS
 │                              │                               │
 ├─ join-room ────────────────► │                               │
 │◄─ room-joined (isHost=true) ─┤                               │
 │                              │◄──────────────── join-room ───┤
 │                              ├─ room-joined (isHost=false) ──►│
 │◄─────────────── push-sync ───┤                               │
 ├─ sync-state ────────────────►│──────────── sync-state ───────►│
 │                              │                               │
 ├─ play/pause/seek ───────────►│──────── play/pause/seek ──────►│
 ├─ heartbeat (every 5s) ──────►│──────── heartbeat ────────────►│
 │                              │                               │
 ├─ rtc-start ─────────────────►│──────── rtc-start ────────────►│
 │◄────────────────────────────────────── rtc-join ─────────────┤
 ├─ rtc-offer (to viewer) ─────►│──────── rtc-offer ────────────►│
 │◄────────────────────────────────────── rtc-answer ───────────┤
 │  [WebRTC P2P stream established — video flows directly]       │
```

## Deployment

**Railway / Render / Fly.io:**
```bash
# Set PORT env var — the server reads process.env.PORT
railway up
```

**Locally on LAN (laptop + phone):**
```bash
node server.js
# Find your LAN IP: ip addr | grep 192.168
# Open http://192.168.x.x:3000 on phone
```

**HTTPS (required for WebRTC on production):**
Put behind nginx/Caddy with SSL, or deploy to a platform that handles HTTPS.
