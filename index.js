import express from "express";
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { Server } from "socket.io";
import sqlite3 from "sqlite3";
import { open } from "sqlite";
import { availableParallelism } from "node:os";
import cluster from "node:cluster";
import { createAdapter, setupPrimary } from "@socket.io/cluster-adapter";

// --- ✅ CLUSTER SETUP ---
if (cluster.isPrimary) {
  const numCPUs = availableParallelism();
  console.log(`Primary process running. Starting ${numCPUs} workers...`);

  // Create one worker per CPU core
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork({ PORT: 3000 + i });
  }

  // Setup the cluster adapter on the primary thread
  setupPrimary();

} else {
  // --- ✅ WORKER LOGIC (Your Chat App) ---

  // Open the SQLite database
  const db = await open({
    filename: "chat.db",
    driver: sqlite3.Database,
  });

  // Create table if not exists
  await db.exec(`
    CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      client_offset TEXT UNIQUE,
      content TEXT
    );
  `);

  // Express app setup
  const app = express();
  const server = createServer(app);
  const io = new Server(server, {
    connectionStateRecovery: {},
    adapter: createAdapter() // important for cluster communication
  });

  const __dirname = dirname(fileURLToPath(import.meta.url));

  // Serve the frontend file
  app.get("/", (req, res) => {
    res.sendFile(join(__dirname, "index.html"));
  });

  // --- ✅ SOCKET.IO CHAT HANDLERS ---
  io.on("connection", async (socket) => {
    console.log(`User connected on worker ${process.pid}`);

    socket.on("chat message", async (msg, clientOffset, callback) => {
      let result;
      try {
        result = await db.run(
          "INSERT INTO messages (content, client_offset) VALUES (?, ?)",
          msg,
          clientOffset
        );
      } catch (e) {
        if (e.errno === 19 /* SQLITE_CONSTRAINT */) {
          callback(); // message already inserted
        } else {
          console.error("DB error:", e.message);
        }
        return;
      }
      io.emit("chat message", msg, result.lastID);
      callback(); // acknowledge event
    });

    // Recover missed messages if needed
    if (!socket.recovered) {
      try {
        await db.each(
          "SELECT id, content FROM messages WHERE id > ?",
          [socket.handshake.auth.serverOffset || 0],
          (_err, row) => {
            socket.emit("chat message", row.content, row.id);
          }
        );
      } catch (e) {
        console.error("Recovery error:", e.message);
      }
    }
  });

  // --- ✅ START SERVER ---
  const port = process.env.PORT || 3000;
  server.listen(port, () => {
    console.log(`Worker ${process.pid} running on http://localhost:${port}`);
  });
}
