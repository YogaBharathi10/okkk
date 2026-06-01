const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'], credentials: false },
  allowEIO3: true,
  pingTimeout: 60000,       // 60s — handles mobile network switches / screen lock
  pingInterval: 25000,      // 25s ping
  upgradeTimeout: 30000,
  transports: ['polling', 'websocket'],
});

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

const rooms = new Map();

function getRoomState(room) {
  return room.state || { src: null, imdbId: null, mediaType: 'movie', season: null, episode: null, title: '', time: 0, playing: false };
}

function pickNewHost(room) {
  for (const [sid] of room.users) {
    if (sid !== room.hostId) return sid;
  }
  return null;
}

io.on('connection', (socket) => {
  console.log('connected:', socket.id);

  socket.on('join-room', ({ roomCode, name, userId }) => {
    const code = (roomCode || '').toUpperCase().trim();
    if (!code || !name) return;
    socket.join(code);
    socket.data.roomCode = code;
    socket.data.name = name;
    socket.data.userId = userId || socket.id;

    if (!rooms.has(code)) {
      rooms.set(code, { hostId: socket.id, users: new Map(), state: null });
    }
    const room = rooms.get(code);

    // Check if this userId already exists (reconnect case) — remove old socket entry
    let wasHost = false;
    for (const [sid, u] of room.users) {
      if (u.id === socket.data.userId && sid !== socket.id) {
        wasHost = u.isHost;
        room.users.delete(sid);
        if (room.hostId === sid) room.hostId = socket.id; // transfer host to new socket
        break;
      }
    }

    const isHost = room.users.size === 0 || wasHost || room.hostId === socket.id;
    if (isHost) room.hostId = socket.id;
    room.users.set(socket.id, { name, id: socket.data.userId, isHost });

    socket.emit('room-joined', {
      roomCode: code, isHost, hostId: room.hostId,
      state: getRoomState(room), users: [...room.users.values()],
    });

    // Only broadcast user-joined for genuinely new users (not reconnects)
    if (!wasHost || room.users.size > 1) {
      socket.to(code).emit('user-joined', { id: socket.data.userId, socketId: socket.id, name });
    }
    console.log(`[${code}] ${name} joined (host=${isHost}, reconnect=${wasHost})`);
  });

  socket.on('play', ({ time }) => {
    const code = socket.data.roomCode; const room = rooms.get(code);
    if (!room || room.hostId !== socket.id) return;
    if (room.state) { room.state.playing = true; room.state.time = time || 0; }
    socket.to(code).emit('play', { time, from: socket.data.userId });
  });

  socket.on('pause', ({ time }) => {
    const code = socket.data.roomCode; const room = rooms.get(code);
    if (!room || room.hostId !== socket.id) return;
    if (room.state) { room.state.playing = false; room.state.time = time || 0; }
    socket.to(code).emit('pause', { time, from: socket.data.userId });
  });

  socket.on('seek', ({ time }) => {
    const code = socket.data.roomCode; const room = rooms.get(code);
    if (!room || room.hostId !== socket.id) return;
    if (room.state) room.state.time = time || 0;
    socket.to(code).emit('seek', { time, from: socket.data.userId });
  });

  socket.on('load-stream', (payload) => {
    const code = socket.data.roomCode; const room = rooms.get(code);
    if (!room || room.hostId !== socket.id) return;
    room.state = {
      src: payload.url || null, imdbId: payload.imdbId || null,
      mediaType: payload.mediaType || 'movie', season: payload.season || null,
      episode: payload.episode || null, title: payload.title || '', time: 0, playing: true,
    };
    socket.to(code).emit('load-stream', { ...payload, from: socket.data.userId });
  });

  socket.on('heartbeat', ({ time, ts }) => {
    const code = socket.data.roomCode; const room = rooms.get(code);
    if (!room || room.hostId !== socket.id) return;
    if (room.state) room.state.time = time;
    socket.to(code).emit('heartbeat', { time, ts, from: socket.data.userId });
  });

  socket.on('req-sync', () => {
    const code = socket.data.roomCode; const room = rooms.get(code);
    if (!room) return;
    socket.to(code).emit('push-sync', { to: socket.id });
  });

  socket.on('sync-state', (payload) => {
    const code = socket.data.roomCode; const room = rooms.get(code);
    if (!room || room.hostId !== socket.id) return;
    if (payload.to) io.to(payload.to).emit('sync-state', { ...payload, from: socket.data.userId });
    else socket.to(code).emit('sync-state', { ...payload, from: socket.data.userId });
  });

  socket.on('chat', ({ text }) => {
    const code = socket.data.roomCode; if (!code) return;
    socket.to(code).emit('chat', { name: socket.data.name, text, from: socket.data.userId });
  });

  socket.on('rtc-start', () => {
    const code = socket.data.roomCode; const room = rooms.get(code);
    if (!room || room.hostId !== socket.id) return;
    socket.to(code).emit('rtc-start', { hostSocketId: socket.id });
  });

  socket.on('rtc-stop', () => { socket.to(socket.data.roomCode).emit('rtc-stop', {}); });

  socket.on('rtc-join', () => {
    const code = socket.data.roomCode; const room = rooms.get(code);
    if (!room) return;
    io.to(room.hostId).emit('rtc-join', { from: socket.id, name: socket.data.name });
  });

  socket.on('rtc-offer', ({ to, sdp }) => { io.to(to).emit('rtc-offer', { from: socket.id, sdp }); });
  socket.on('rtc-answer', ({ to, sdp }) => { io.to(to).emit('rtc-answer', { from: socket.id, sdp }); });
  socket.on('rtc-ice', ({ to, candidate }) => { io.to(to).emit('rtc-ice', { from: socket.id, candidate }); });

  socket.on('rtc-leave', () => {
    const code = socket.data.roomCode; const room = rooms.get(code);
    if (!room) return;
    io.to(room.hostId).emit('rtc-leave', { from: socket.id });
  });

  socket.on('disconnect', () => {
    const code = socket.data.roomCode; if (!code) return;
    const room = rooms.get(code); if (!room) return;
    const user = room.users.get(socket.id);
    room.users.delete(socket.id);
    if (room.users.size === 0) { rooms.delete(code); return; }
    if (user) socket.to(code).emit('user-left', { id: user.id, socketId: socket.id, name: user.name });
    if (room.hostId === socket.id) {
      const newHostSocketId = pickNewHost(room);
      if (newHostSocketId) {
        room.hostId = newHostSocketId;
        const newHostUser = room.users.get(newHostSocketId);
        if (newHostUser) newHostUser.isHost = true;
        io.to(code).emit('host-change', {
          newHostSocketId, newHostId: newHostUser?.id, newHostName: newHostUser?.name,
        });
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🎬 RAATHIRI PADAM running on port ${PORT}\n`);
});
