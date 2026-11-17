import http from "http";
import { Server } from "socket.io";
import { v4 as uuidv4 } from "uuid";
import dotenv from "dotenv";
import express from "express";

dotenv.config();

const app = express();

app.get("/", (req, res) => {
  res.send("Server is running");
});

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
  },
});

const waitingQueue = [];
const activePairs = new Map();

io.on("connection", (socket) => {
  console.log(`User connected: ${socket.id}`);

  socket.on("start-call", () => {
    if (waitingQueue.includes(socket.id) || activePairs.has(socket.id)) {
      return;
    }

    let partnerId = null;
    while (waitingQueue.length > 0) {
      const candidateId = waitingQueue.shift();
      if (candidateId !== socket.id && io.sockets.sockets.has(candidateId)) {
        partnerId = candidateId;
        break;
      }
    }

    if (partnerId) {
      const partnerSocket = io.sockets.sockets.get(partnerId);
      const roomId = uuidv4();

      socket.join(roomId);
      partnerSocket.join(roomId);

      activePairs.set(socket.id, { partnerId, roomId });
      activePairs.set(partnerId, { partnerId: socket.id, roomId });

      socket.emit("matched", { roomId, initiator: true });
      partnerSocket.emit("matched", { roomId, initiator: false });
    } else {
      waitingQueue.push(socket.id);
      socket.emit("waiting");
    }
  });

  socket.on("next", () => {
    handleLeave(socket.id);
  });

  socket.on("disconnect", () => {
    console.log(`User disconnected: ${socket.id}`);
    handleLeave(socket.id);
  });

  function handleLeave(socketId) {
    const index = waitingQueue.indexOf(socketId);
    if (index !== -1) {
      waitingQueue.splice(index, 1);
    }

    const session = activePairs.get(socketId);
    if (session) {
      const { partnerId, roomId } = session;

      const currentSocket = io.sockets.sockets.get(socketId);
      const partnerSocket = io.sockets.sockets.get(partnerId);

      if (currentSocket) currentSocket.leave(roomId);
      if (partnerSocket) {
        partnerSocket.leave(roomId);
        partnerSocket.emit("partner-left");
      }

      activePairs.delete(socketId);
      activePairs.delete(partnerId);
    }
  }
});

const PORT = process.env.PORT || 5000;
server.listen(PORT, "0.0.0.0", () => {
  console.log(`Server is running on port ${PORT}`);
});
