/**
 * server.js — Aether Mesh Secure Gateway, Signaling & Static Host
 *
 * Responsibilities:
 *  • Serves static /public assets over HTTPS on port 3000 (satisfies Secure Context).
 *  • Attaches a WebSocket signaling server (WSS) on the same port.
 *  • Maintains an in-memory peer registry keyed by nodeId.
 *  • Routes WebRTC signaling payloads (offer / answer / candidate / broadcast).
 *  • Cleans up stale peers on socket close / error with reconnection guards.
 */

'use strict';

const https   = require('https');
const fs      = require('fs');
const path    = require('path');
const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');

// ─────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────
const PORT      = process.env.PORT || 3000;
const CERT_PATH = path.join(__dirname, 'cert.pem');
const KEY_PATH  = path.join(__dirname, 'key.pem');
const PUBLIC    = path.join(__dirname, 'public');

// ─────────────────────────────────────────────────────────────
// TLS Certificate Loading
// ─────────────────────────────────────────────────────────────
let tlsOptions;
try {
  tlsOptions = {
    cert: fs.readFileSync(CERT_PATH),
    key:  fs.readFileSync(KEY_PATH),
  };
  console.log('[GATEWAY] TLS certificates loaded ✓');
} catch (err) {
  console.error('[GATEWAY] ⚠  TLS certificate files not found.');
  console.error('           Falling back to HTTP-only mode (WebRTC will be limited).\n');
  tlsOptions = null;
}

// ─────────────────────────────────────────────────────────────
// Express Static Server
// ─────────────────────────────────────────────────────────────
const app = express();

app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('Referrer-Policy', 'no-referrer');
  if (req.path.endsWith('sw.js')) {
    res.setHeader('Service-Worker-Allowed', '/');
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
  }
  next();
});

// Serve static public directory
app.use(express.static(PUBLIC, { index: 'index.html' }));

// Captive portal redirect — any non-asset GET returns index.html
app.get('*', (req, res) => {
  res.sendFile(path.join(PUBLIC, 'index.html'));
});

// ─────────────────────────────────────────────────────────────
// HTTPS / HTTP Server
// ─────────────────────────────────────────────────────────────
const server = tlsOptions
  ? https.createServer(tlsOptions, app)
  : require('http').createServer(app);

// ─────────────────────────────────────────────────────────────
// In-Memory Peer Registry
// Map<nodeId: string, { ws: WebSocket, ip: string, connectedAt: number }>
// ─────────────────────────────────────────────────────────────
const peers = new Map();

function broadcastExcept(senderNodeId, message) {
  const raw = JSON.stringify(message);
  for (const [nodeId, peer] of peers) {
    if (nodeId !== senderNodeId && peer.ws.readyState === WebSocket.OPEN) {
      try {
        peer.ws.send(raw);
      } catch {}
    }
  }
}

function sendToPeer(targetNodeId, message) {
  const peer = peers.get(targetNodeId);
  if (peer && peer.ws.readyState === WebSocket.OPEN) {
    try {
      peer.ws.send(JSON.stringify(message));
      return true;
    } catch {}
  }
  return false;
}

// ─────────────────────────────────────────────────────────────
// WebSocket Signaling Server
// ─────────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  const clientIp = req.socket.remoteAddress || 'unknown';
  let registeredNodeId = null;

  console.log(`[SIGNAL] New WS connection from ${clientIp}`);

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      console.warn('[SIGNAL] Received non-JSON message — ignoring.');
      return;
    }

    const { type, from, to, payload } = msg;

    // ── Registration ──────────────────────────────────────────
    if (type === 'register') {
      registeredNodeId = from;
      peers.set(registeredNodeId, { ws, ip: clientIp, connectedAt: Date.now() });
      console.log(`[SIGNAL] Peer registered: ${registeredNodeId} (${clientIp})`);

      // Acknowledge registration with current peer list
      const peerList = [...peers.keys()].filter(id => id !== registeredNodeId);
      ws.send(JSON.stringify({ type: 'registered', nodeId: registeredNodeId, peers: peerList }));

      // Notify existing peers of new arrival
      broadcastExcept(registeredNodeId, { type: 'peer-joined', nodeId: registeredNodeId, ip: clientIp });
      return;
    }

    if (!registeredNodeId) {
      console.warn('[SIGNAL] Unregistered client sent message — ignoring.');
      return;
    }

    // ── Signaling: offer / answer / candidate / busy / reject ─
    if (['offer', 'answer', 'candidate', 'busy', 'reject'].includes(type)) {
      if (to) {
        const delivered = sendToPeer(to, { type, from, payload });
        if (!delivered) {
          console.warn(`[SIGNAL] Target peer ${to} not found or offline for ${type}`);
        }
      } else {
        broadcastExcept(registeredNodeId, { type, from, payload });
      }
      return;
    }

    // ── Peer list request ─────────────────────────────────────
    if (type === 'request-peers') {
      const peerList = [...peers.keys()].filter(id => id !== registeredNodeId);
      ws.send(JSON.stringify({ type: 'peer-list', peers: peerList }));
      return;
    }

    // ── Ping / keep-alive ─────────────────────────────────────
    if (type === 'ping') {
      ws.send(JSON.stringify({ type: 'pong', from: 'server', timestamp: Date.now() }));
      return;
    }
  });

  ws.on('close', () => {
    if (registeredNodeId) {
      const current = peers.get(registeredNodeId);
      // Guard: only delete and notify if this socket is still the active one!
      if (current && current.ws === ws) {
        peers.delete(registeredNodeId);
        console.log(`[SIGNAL] Peer disconnected: ${registeredNodeId}`);
        broadcastExcept(registeredNodeId, { type: 'peer-left', nodeId: registeredNodeId });
      } else {
        console.log(`[SIGNAL] Stale connection closed for ${registeredNodeId} (already re-registered)`);
      }
    }
  });

  ws.on('error', (err) => {
    console.error(`[SIGNAL] WS error for ${registeredNodeId || 'unknown'}: ${err.message}`);
    if (registeredNodeId) {
      const current = peers.get(registeredNodeId);
      if (current && current.ws === ws) {
        peers.delete(registeredNodeId);
        broadcastExcept(registeredNodeId, { type: 'peer-left', nodeId: registeredNodeId });
      }
    }
  });

  ws.send(JSON.stringify({ type: 'welcome', message: 'Aether Mesh Signaling Server v1.0' }));
});

// Periodic stale-peer cleanup (every 30 s)
setInterval(() => {
  for (const [nodeId, peer] of peers) {
    if (peer.ws.readyState !== WebSocket.OPEN) {
      console.log(`[SIGNAL] Removing stale peer: ${nodeId}`);
      peers.delete(nodeId);
      broadcastExcept(nodeId, { type: 'peer-left', nodeId });
    }
  }
}, 30_000);

// Start listening
server.listen(PORT, '0.0.0.0', () => {
  const proto = tlsOptions ? 'https' : 'http';
  console.log('╔══════════════════════════════════════════════════╗');
  console.log('║          AETHER MESH GATEWAY ONLINE              ║');
  console.log('╠══════════════════════════════════════════════════╣');
  console.log(`║  Static + Signaling: ${proto}://0.0.0.0:${PORT}          ║`);
  console.log(`║  WS Signaling:       wss://0.0.0.0:${PORT}           ║`);
  console.log(`║  Access from LAN:    ${proto}://<your-ip>:${PORT}       ║`);
  console.log('╚══════════════════════════════════════════════════╝');
});

server.on('error', (err) => {
  console.error('[GATEWAY] Server error:', err.message);
  process.exit(1);
});

module.exports = { server, peers };
