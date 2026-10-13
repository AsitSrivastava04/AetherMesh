# 🌐 Aether Mesh

> **Air-Gapped, Zero-Install, Disaster-Resilient P2P Mesh Network PWA**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/Platform-Web%20%2F%20PWA-teal.svg)](https://developer.mozilla.org/en-US/docs/Web/Progressive_web_apps)
[![Topology](https://img.shields.io/badge/Topology-Bounded%20K%3D3-orange.svg)](#bounded-k3-overlay-topology)
[![Storage](https://img.shields.io/badge/Storage-IndexedDB%20DTN-purple.svg)](#dtn-storage-engine)

---

## 📌 System Overview

During severe crises and natural disasters, cellular base stations, power grids, and internet backbones frequently fail. While modern devices carry capable Wi-Fi and Bluetooth radios, standard communication software requires centralized cloud infrastructure or native app installations that cannot be downloaded once connectivity is severed.

**Aether Mesh** resolves this **Deployment Paradox**. It operates entirely within standard, unmodified modern web browsers as a captive-portal-served **Progressive Web App (PWA)** over local TLS/SSL. Any survivor or first responder connecting to a local Wi-Fi hotspot is immediately redirected to the tactical mesh node without installing native software or app store binaries.

Once loaded, nodes establish browser-to-browser peer connections over WebRTC data channels, creating a resilient, decentralized mesh network equipped with delay-tolerant store-and-forward routing.

---

## ⚡ Core Architecture

```
                       [ Field Gateway / Hotspot ]
                       (Captive Portal + DNS / WSS)
                                   │
              ┌────────────────────┴────────────────────┐
              ▼                                         ▼
      ┌───────────────┐     WebRTC DataChannel     ┌───────────────┐
      │  Local Node A │◄──────────────────────────►│  Local Node B │
      │    (K ≤ 3)    │                            │    (K ≤ 3)    │
      └───────┬───────┘                            └───────┬───────┘
              │                                            │
              │             WebRTC DataChannel             │
              └───────────────────►┌───────────────┐◄──────┘
                                   │  Local Node C │
                                   │    (K ≤ 3)    │
                                   └───────────────┘
```

### 1. Bounded $K=3$ Overlay Topology (`peer-manager.js`)
* Standard WebRTC mesh architectures require full-mesh ($N(N-1)/2$) peer connections, leading to $O(N^2)$ CPU and battery exhaustion on mobile devices.
* Aether Mesh strictly bounds node degrees to **$K=3$ active WebRTC connections**.
* Periodic RTT pings (4,000 ms heartbeat) measure link quality. When a 4th candidate peer is discovered via gossip, RTT probing evaluates the candidate against existing peers, automatically pruning the link with the highest latency.

### 2. Delay-Tolerant Network (DTN) Data Mule Engine (`dtn-storage.js`)
* Real-world disaster environments suffer from intermittent link partitions and sporadic node connectivity.
* Messages destined for disconnected peers are buffered in client-side **IndexedDB (via Dexie.js)** with store-and-forward semantics.
* **Deterministic Priority Eviction**: When the local storage pool reaches capacity ($N=50$ bundles), bundles are ranked and pruned according to:

$$P = \frac{\text{remaining\_TTL}}{\text{hopCount} + 1}$$

* Packets with lower priority scores ($P$) are evicted first, ensuring high-value, fresh, low-hop messages survive storage constraints.
* Buffered bundles are automatically flushed when candidate routes or target peers reconnect.

### 3. Multi-Hop Router & Vector Clock CRDT (`router.js`)
* Dynamic multi-hop forwarding over WebRTC data channels.
* Loop prevention via packet `visitedNodes` tracking.
* **Causal Ordering & Conflict Resolution**: Employs Vector Clocks for causal consistency across partitions, with Last-Write-Wins (LWW) conflict resolution governed by synchronized timestamps.
* Delta-synchronization on peer handshake transmits only missing bundle records.

### 4. Zero-Install Offline PWA & Thread Persistence (`sw.js`, `app.js`)
* **Service Worker**: Caches the application shell and tactical interface on initial visit, allowing nodes to reload and operate with zero internet or gateway connectivity.
* **Screen Wake Lock API**: Automatically requests `navigator.wakeLock` to prevent operating systems from suspending browser background tabs and throttling the JavaScript event loop.

### 5. Tactical Dark-Mode Visualizer (`public/`)
* **Real-Time Mesh Topology Graph**: Live Vis.js visualization rendering peer nodes, node degrees, and real-time RTT latency tags.
* **Route Packet Animation**: Dynamically pulses the multi-hop transmission path across the topology graph on packet delivery.
* **Partition Simulation**: Manual network isolation mode for verifying offline DTN store-and-forward behavior.

---

## 📂 Project Structure

```text
AetherMeshImplementation/
├── server.js               # Node.js HTTPS static host & WebSocket signaling gateway
├── package.json            # Project dependencies and startup scripts
├── dnsmasq.conf            # Captive portal DNS redirect configuration
├── .gitignore              # Ignores dependencies and local private keys
├── README.md               # Architecture documentation and manual
└── public/                 # Static PWA client assets
    ├── index.html          # Tactical HUD interface & layout
    ├── style.css           # Dark-mode styling (JetBrains Mono & Orbitron)
    ├── manifest.json       # PWA web manifest
    ├── sw.js               # Service worker for offline shell caching
    └── js/
        ├── peer-manager.js # K=3 bounded overlay manager & RTT heartbeats
        ├── dtn-storage.js  # Dexie.js DTN storage & priority eviction
        ├── router.js       # Multi-hop routing & Vector Clock CRDT engine
        └── app.js          # Tactical UI orchestrator & Vis.js graph
```

---

## 🚀 Quick Start

### Prerequisites
* **Node.js** (v18.0.0 or higher)
* **npm**

### 1. Installation
Clone the repository and install the dependencies:
```bash
git clone https://github.com/AsitSrivastava04/AetherMesh.git
cd AetherMesh
npm install
```

### 2. TLS / HTTPS Certificate Generation
Browsers mandate a **Secure Context** (HTTPS) to enable WebRTC, Service Workers, and the Screen Wake Lock API. Generate self-signed certificates for local air-gapped testing:

```bash
openssl req -nodes -new -x509 -keyout key.pem -out cert.pem -days 365
```

### 3. Launch the Gateway
Start the local gateway server:
```bash
npm start
```
The server will bind to port `3000`:
* HTTPS Server: `https://0.0.0.0:3000`
* WebSocket Signaling: `wss://0.0.0.0:3000`

### 4. Connect Devices
1. Connect participating laptops/smartphones to the same local Wi-Fi router, ad-hoc network, or mobile hotspot.
2. Open **`https://<GATEWAY_LOCAL_IP>:3000`** in a modern browser (Chrome, Edge, Firefox, Safari).
3. If using self-signed certificates, accept the browser security warning (**Advanced → Proceed**).
4. Nodes will automatically establish peer connections, populate the live Vis.js topology graph, and begin mesh telemetry exchange.

---

## 📡 Field Captive Portal Setup (Linux Hotspot)

To deploy Aether Mesh as an automatic zero-config captive portal in disaster zones using a Linux host:

1. Configure Wi-Fi Access Point mode on your wireless adapter (`hostapd` or NetworkManager hotspot).
2. Edit [`dnsmasq.conf`](dnsmasq.conf) and set the gateway IP (default: `192.168.137.1`).
3. Deploy the DNS configuration:
   ```bash
   sudo cp dnsmasq.conf /etc/dnsmasq.conf
   sudo systemctl restart dnsmasq
   ```
4. Any device joining the hotspot will automatically be prompted with the captive portal and directed straight into the Aether Mesh tactical dashboard.

---

## 🔒 Security & Deployment Notes

> **Notice**: Self-signed certificates are intended strictly for air-gapped LAN environments. Do not expose unauthenticated test ports directly to the public internet. For field staging with zero browser warning dialogues, install a local root authority using tools like [`mkcert`](https://github.com/FiloSottile/mkcert).

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).
