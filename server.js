/**
 * RAATHIRI PADAM — Watch Party Server
 * Node.js + Express + Socket.IO
 *
 * Features:
 *  - Room creation & joining by code or link
 *  - First user = HOST; host transfer on disconnect
 *  - Play / Pause / Seek sync to all viewers
 *  - Full state push to late joiners
 *  - WebRTC signaling relay (host screen-share to viewers)
 *  - 5-second heartbeat for drift correction
 *  - Chat broadcast
 */

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  pingTimeout: 20000,
  pingInterval: 10000,
});

// Serve the frontend
app.use(express.static(path.join(__dirname, 'public')));

// Rooms map: roomCode → { hostId, users: Map<socketId, {name, id}>, state }
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

  /* ─── JOIN ROOM ─── */
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
    const isHost = room.users.size === 0; // first joiner = host
    if (isHost) room.hostId = socket.id;

    room.users.set(socket.id, { name, id: socket.data.userId, isHost });

    // Tell the joiner their role + current room state
    socket.emit('room-joined', {
      roomCode: code,
      isHost,
      hostId: room.hostId,
      state: getRoomState(room),
      users: [...room.users.values()],
    });

    // Tell everyone else about the new user
    socket.to(code).emit('user-joined', {
      id: socket.data.userId,
      socketId: socket.id,
      name,
    });

    // If there's a host with active content, push state to new viewer
    if (!isHost && room.state) {
      // We'll let the client request sync after join-room
    }

    console.log(`[${code}] ${name} joined (host=${isHost})`);
  });

  /* ─── PLAY ─── */
  socket.on('play', ({ time }) => {
    const code = socket.data.roomCode;
    const room = rooms.get(code);
    if (!room || room.hostId !== socket.id) return;
    if (room.state) { room.state.playing = true; room.state.time = time || 0; }
    socket.to(code).emit('play', { time, from: socket.data.userId });
  });

  /* ─── PAUSE ─── */
  socket.on('pause', ({ time }) => {
    const code = socket.data.roomCode;
    const room = rooms.get(code);
    if (!room || room.hostId !== socket.id) return;
    if (room.state) { room.state.playing = false; room.state.time = time || 0; }
    socket.to(code).emit('pause', { time, from: socket.data.userId });
  });

  /* ─── SEEK ─── */
  socket.on('seek', ({ time }) => {
    const code = socket.data.roomCode;
    const room = rooms.get(code);
    if (!room || room.hostId !== socket.id) return;
    if (room.state) room.state.time = time || 0;
    socket.to(code).emit('seek', { time, from: socket.data.userId });
  });

  /* ─── LOAD STREAM ─── */
  socket.on('load-stream', (payload) => {
    const code = socket.data.roomCode;
    const room = rooms.get(code);
    if (!room || room.hostId !== socket.id) return;
    room.state = {
      src: payload.url || null,
      imdbId: payload.imdbId || null,
      mediaType: payload.mediaType || 'movie',
      season: payload.season || null,
      episode: payload.episode || null,
      title: payload.title || '',
      time: 0,
      playing: true,
    };
    socket.to(code).emit('load-stream', { ...payload, from: socket.data.userId });
  });

  /* ─── HEARTBEAT (host → all viewers) ─── */
  socket.on('heartbeat', ({ time, ts }) => {
    const code = socket.data.roomCode;
    const room = rooms.get(code);
    if (!room || room.hostId !== socket.id) return;
    if (room.state) room.state.time = time;
    socket.to(code).emit('heartbeat', { time, ts, from: socket.data.userId });
  });

  /* ─── REQ-SYNC (viewer requests full state from host) ─── */
  socket.on('req-sync', () => {
    const code = socket.data.roomCode;
    const room = rooms.get(code);
    if (!room) return;
    // Tell host to push state to this viewer
    socket.to(code).emit('push-sync', { to: socket.id });
  });

  /* ─── SYNC-STATE (host → specific viewer) ─── */
  socket.on('sync-state', (payload) => {
    const code = socket.data.roomCode;
    const room = rooms.get(code);
    if (!room || room.hostId !== socket.id) return;
    // payload.to is socket.id of target
    if (payload.to) {
      io.to(payload.to).emit('sync-state', { ...payload, from: socket.data.userId });
    } else {
      socket.to(code).emit('sync-state', { ...payload, from: socket.data.userId });
    }
  });

  /* ─── CHAT ─── */
  socket.on('chat', ({ text }) => {
    const code = socket.data.roomCode;
    if (!code) return;
    socket.to(code).emit('chat', {
      name: socket.data.name,
      text,
      from: socket.data.userId,
    });
  });

  /* ─── WebRTC SIGNALING ─── */

  // Host starts screen share → notify viewers
  socket.on('rtc-start', () => {
    const code = socket.data.roomCode;
    const room = rooms.get(code);
    if (!room || room.hostId !== socket.id) return;
    socket.to(code).emit('rtc-start', { hostSocketId: socket.id });
  });

  // Host stops screen share
  socket.on('rtc-stop', () => {
    const code = socket.data.roomCode;
    socket.to(code).emit('rtc-stop', {});
  });

  // Viewer wants to connect to host's RTC stream
  socket.on('rtc-join', () => {
    const code = socket.data.roomCode;
    const room = rooms.get(code);
    if (!room) return;
    // Forward to host
    io.to(room.hostId).emit('rtc-join', {
      from: socket.id, // use socket.id for RTC routing
      name: socket.data.name,
    });
  });

  // Host → viewer: SDP offer
  socket.on('rtc-offer', ({ to, sdp }) => {
    io.to(to).emit('rtc-offer', { from: socket.id, sdp });
  });

  // Viewer → host: SDP answer
  socket.on('rtc-answer', ({ to, sdp }) => {
    io.to(to).emit('rtc-answer', { from: socket.id, sdp });
  });

  // Both: ICE candidate relay
  socket.on('rtc-ice', ({ to, candidate }) => {
    io.to(to).emit('rtc-ice', { from: socket.id, candidate });
  });

  // Viewer leaves RTC
  socket.on('rtc-leave', () => {
    const code = socket.data.roomCode;
    const room = rooms.get(code);
    if (!room) return;
    io.to(room.hostId).emit('rtc-leave', { from: socket.id });
  });

  /* ─── DISCONNECT ─── */
  socket.on('disconnect', () => {
    const code = socket.data.roomCode;
    if (!code) return;
    const room = rooms.get(code);
    if (!room) return;

    const user = room.users.get(socket.id);
    room.users.delete(socket.id);

    if (room.users.size === 0) {
      rooms.delete(code);
      console.log(`[${code}] Room closed`);
      return;
    }

    // Notify others
    if (user) {
      socket.to(code).emit('user-left', { id: user.id, socketId: socket.id, name: user.name });
    }

    // Host transfer
    if (room.hostId === socket.id) {
      const newHostSocketId = pickNewHost(room);
      if (newHostSocketId) {
        room.hostId = newHostSocketId;
        const newHostUser = room.users.get(newHostSocketId);
        if (newHostUser) newHostUser.isHost = true;
        io.to(code).emit('host-change', {
          newHostSocketId,
          newHostId: newHostUser?.id,
          newHostName: newHostUser?.name,
        });
        console.log(`[${code}] Host transferred to ${newHostUser?.name}`);
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n🎬 RAATHIRI PADAM server running at http://localhost:${PORT}\n`);
});
