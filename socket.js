// api/socket.js — Vercel serverless Socket.IO handler
const { Server } = require('socket.io');

const rooms = new Map();

function getRoomState(room) {
  return room.state || {
    src: null, imdbId: null, mediaType: 'movie',
    season: null, episode: null, title: '',
    time: 0, playing: false
  };
}

function pickNewHost(room) {
  for (const [sid] of room.users) {
    if (sid !== room.hostId) return sid;
  }
  return null;
}

module.exports = function handler(req, res) {
  if (!res.socket.server.io) {
    const io = new Server(res.socket.server, {
      path: '/api/socket',
      addTrailingSlash: false,
      cors: { origin: '*', methods: ['GET', 'POST'] },
      pingTimeout: 20000,
      pingInterval: 10000,
    });

    io.on('connection', (socket) => {

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
        const isHost = room.users.size === 0;
        if (isHost) room.hostId = socket.id;
        room.users.set(socket.id, { name, id: socket.data.userId, isHost });
        socket.emit('room-joined', {
          roomCode: code, isHost, hostId: room.hostId,
          state: getRoomState(room), users: [...room.users.values()],
        });
        socket.to(code).emit('user-joined', { id: socket.data.userId, socketId: socket.id, name });
      });

      socket.on('play', ({ time }) => {
        const code = socket.data.roomCode;
        const room = rooms.get(code);
        if (!room || room.hostId !== socket.id) return;
        if (room.state) { room.state.playing = true; room.state.time = time || 0; }
        socket.to(code).emit('play', { time, from: socket.data.userId });
      });

      socket.on('pause', ({ time }) => {
        const code = socket.data.roomCode;
        const room = rooms.get(code);
        if (!room || room.hostId !== socket.id) return;
        if (room.state) { room.state.playing = false; room.state.time = time || 0; }
        socket.to(code).emit('pause', { time, from: socket.data.userId });
      });

      socket.on('seek', ({ time }) => {
        const code = socket.data.roomCode;
        const room = rooms.get(code);
        if (!room || room.hostId !== socket.id) return;
        if (room.state) room.state.time = time || 0;
        socket.to(code).emit('seek', { time, from: socket.data.userId });
      });

      socket.on('load-stream', (payload) => {
        const code = socket.data.roomCode;
        const room = rooms.get(code);
        if (!room || room.hostId !== socket.id) return;
        room.state = {
          src: payload.url || null, imdbId: payload.imdbId || null,
          mediaType: payload.mediaType || 'movie', season: payload.season || null,
          episode: payload.episode || null, title: payload.title || '',
          time: 0, playing: true,
        };
        socket.to(code).emit('load-stream', { ...payload, from: socket.data.userId });
      });

      socket.on('heartbeat', ({ time, ts }) => {
        const code = socket.data.roomCode;
        const room = rooms.get(code);
        if (!room || room.hostId !== socket.id) return;
        if (room.state) room.state.time = time;
        socket.to(code).emit('heartbeat', { time, ts, from: socket.data.userId });
      });

      socket.on('req-sync', () => {
        const code = socket.data.roomCode;
        const room = rooms.get(code);
        if (!room) return;
        socket.to(code).emit('push-sync', { to: socket.id });
      });

      socket.on('sync-state', (payload) => {
        const code = socket.data.roomCode;
        const room = rooms.get(code);
        if (!room || room.hostId !== socket.id) return;
        if (payload.to) {
          io.to(payload.to).emit('sync-state', { ...payload, from: socket.data.userId });
        } else {
          socket.to(code).emit('sync-state', { ...payload, from: socket.data.userId });
        }
      });

      socket.on('chat', ({ text }) => {
        const code = socket.data.roomCode;
        if (!code) return;
        socket.to(code).emit('chat', { name: socket.data.name, text, from: socket.data.userId });
      });

      socket.on('rtc-start', () => {
        const code = socket.data.roomCode;
        const room = rooms.get(code);
        if (!room || room.hostId !== socket.id) return;
        socket.to(code).emit('rtc-start', { hostSocketId: socket.id });
      });

      socket.on('rtc-stop', () => {
        socket.to(socket.data.roomCode).emit('rtc-stop', {});
      });

      socket.on('rtc-join', () => {
        const code = socket.data.roomCode;
        const room = rooms.get(code);
        if (!room) return;
        io.to(room.hostId).emit('rtc-join', { from: socket.id, name: socket.data.name });
      });

      socket.on('rtc-offer', ({ to, sdp }) => {
        io.to(to).emit('rtc-offer', { from: socket.id, sdp });
      });

      socket.on('rtc-answer', ({ to, sdp }) => {
        io.to(to).emit('rtc-answer', { from: socket.id, sdp });
      });

      socket.on('rtc-ice', ({ to, candidate }) => {
        io.to(to).emit('rtc-ice', { from: socket.id, candidate });
      });

      socket.on('rtc-leave', () => {
        const code = socket.data.roomCode;
        const room = rooms.get(code);
        if (!room) return;
        io.to(room.hostId).emit('rtc-leave', { from: socket.id });
      });

      socket.on('disconnect', () => {
        const code = socket.data.roomCode;
        if (!code) return;
        const room = rooms.get(code);
        if (!room) return;
        const user = room.users.get(socket.id);
        room.users.delete(socket.id);
        if (room.users.size === 0) { rooms.delete(code); return; }
        if (user) {
          socket.to(code).emit('user-left', { id: user.id, socketId: socket.id, name: user.name });
        }
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

    res.socket.server.io = io;
  }

  res.end();
}
