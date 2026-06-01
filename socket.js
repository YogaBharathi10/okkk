const { Server } = require('socket.io');
const { createClient } = require('redis');
const { createAdapter } = require('@socket.io/redis-adapter');

let io;

async function getIO(server) {
  if (io) return io;

  const pubClient = createClient({ url: process.env.REDIS_URL });
  const subClient = pubClient.duplicate();
  await Promise.all([pubClient.connect(), subClient.connect()]);

  io = new Server(server, {
    path: '/api/socket',
    addTrailingSlash: false,
    cors: { origin: '*', methods: ['GET', 'POST'] },
    pingTimeout: 20000,
    pingInterval: 10000,
    adapter: createAdapter(pubClient, subClient),
  });

  // Helper: get room from Redis
  async function getRoom(code) {
    const raw = await pubClient.get(`room:${code}`);
    return raw ? JSON.parse(raw) : null;
  }
  async function saveRoom(code, room) {
    await pubClient.set(`room:${code}`, JSON.stringify(room), { EX: 3600 });
  }
  async function deleteRoom(code) {
    await pubClient.del(`room:${code}`);
  }

  function defaultState() {
    return { src: null, imdbId: null, mediaType: 'movie', season: null, episode: null, title: '', time: 0, playing: false };
  }

  io.on('connection', (socket) => {

    socket.on('join-room', async ({ roomCode, name, userId }) => {
      const code = (roomCode || '').toUpperCase().trim();
      if (!code || !name) return;

      socket.join(code);
      socket.data.roomCode = code;
      socket.data.name = name;
      socket.data.userId = userId || socket.id;

      let room = await getRoom(code);
      if (!room) {
        room = { hostId: socket.id, users: {}, state: null };
      }
      const isHost = Object.keys(room.users).length === 0;
      if (isHost) room.hostId = socket.id;
      room.users[socket.id] = { name, id: socket.data.userId, isHost };
      await saveRoom(code, room);

      socket.emit('room-joined', {
        roomCode: code, isHost, hostId: room.hostId,
        state: room.state || defaultState(),
        users: Object.values(room.users),
      });
      socket.to(code).emit('user-joined', { id: socket.data.userId, socketId: socket.id, name });
    });

    socket.on('play', async ({ time }) => {
      const code = socket.data.roomCode;
      const room = await getRoom(code);
      if (!room || room.hostId !== socket.id) return;
      if (room.state) { room.state.playing = true; room.state.time = time || 0; }
      await saveRoom(code, room);
      socket.to(code).emit('play', { time, from: socket.data.userId });
    });

    socket.on('pause', async ({ time }) => {
      const code = socket.data.roomCode;
      const room = await getRoom(code);
      if (!room || room.hostId !== socket.id) return;
      if (room.state) { room.state.playing = false; room.state.time = time || 0; }
      await saveRoom(code, room);
      socket.to(code).emit('pause', { time, from: socket.data.userId });
    });

    socket.on('seek', async ({ time }) => {
      const code = socket.data.roomCode;
      const room = await getRoom(code);
      if (!room || room.hostId !== socket.id) return;
      if (room.state) room.state.time = time || 0;
      await saveRoom(code, room);
      socket.to(code).emit('seek', { time, from: socket.data.userId });
    });

    socket.on('load-stream', async (payload) => {
      const code = socket.data.roomCode;
      const room = await getRoom(code);
      if (!room || room.hostId !== socket.id) return;
      room.state = {
        src: payload.url || null, imdbId: payload.imdbId || null,
        mediaType: payload.mediaType || 'movie', season: payload.season || null,
        episode: payload.episode || null, title: payload.title || '',
        time: 0, playing: true,
      };
      await saveRoom(code, room);
      socket.to(code).emit('load-stream', { ...payload, from: socket.data.userId });
    });

    socket.on('heartbeat', async ({ time, ts }) => {
      const code = socket.data.roomCode;
      const room = await getRoom(code);
      if (!room || room.hostId !== socket.id) return;
      if (room.state) room.state.time = time;
      await saveRoom(code, room);
      socket.to(code).emit('heartbeat', { time, ts, from: socket.data.userId });
    });

    socket.on('req-sync', async () => {
      const code = socket.data.roomCode;
      const room = await getRoom(code);
      if (!room) return;
      socket.to(code).emit('push-sync', { to: socket.id });
    });

    socket.on('sync-state', async (payload) => {
      const code = socket.data.roomCode;
      const room = await getRoom(code);
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

    socket.on('rtc-start', async () => {
      const code = socket.data.roomCode;
      const room = await getRoom(code);
      if (!room || room.hostId !== socket.id) return;
      socket.to(code).emit('rtc-start', { hostSocketId: socket.id });
    });

    socket.on('rtc-stop', () => { socket.to(socket.data.roomCode).emit('rtc-stop', {}); });

    socket.on('rtc-join', async () => {
      const code = socket.data.roomCode;
      const room = await getRoom(code);
      if (!room) return;
      io.to(room.hostId).emit('rtc-join', { from: socket.id, name: socket.data.name });
    });

    socket.on('rtc-offer', ({ to, sdp }) => { io.to(to).emit('rtc-offer', { from: socket.id, sdp }); });
    socket.on('rtc-answer', ({ to, sdp }) => { io.to(to).emit('rtc-answer', { from: socket.id, sdp }); });
    socket.on('rtc-ice', ({ to, candidate }) => { io.to(to).emit('rtc-ice', { from: socket.id, candidate }); });

    socket.on('rtc-leave', async () => {
      const code = socket.data.roomCode;
      const room = await getRoom(code);
      if (!room) return;
      io.to(room.hostId).emit('rtc-leave', { from: socket.id });
    });

    socket.on('disconnect', async () => {
      const code = socket.data.roomCode;
      if (!code) return;
      const room = await getRoom(code);
      if (!room) return;

      const user = room.users[socket.id];
      delete room.users[socket.id];

      if (Object.keys(room.users).length === 0) {
        await deleteRoom(code);
        return;
      }

      await saveRoom(code, room);

      if (user) {
        socket.to(code).emit('user-left', { id: user.id, socketId: socket.id, name: user.name });
      }

      if (room.hostId === socket.id) {
        const newHostSocketId = Object.keys(room.users)[0];
        if (newHostSocketId) {
          room.hostId = newHostSocketId;
          const newHostUser = room.users[newHostSocketId];
          if (newHostUser) newHostUser.isHost = true;
          await saveRoom(code, room);
          io.to(code).emit('host-change', {
            newHostSocketId, newHostId: newHostUser?.id, newHostName: newHostUser?.name,
          });
        }
      }
    });
  });

  return io;
}

module.exports = async function handler(req, res) {
  if (!res.socket.server.io) {
    res.socket.server.io = await getIO(res.socket.server);
  }
  res.end();
};
