/**
 * app.js — Aether Mesh Application Orchestrator
 *
 * Bootstraps all modules and drives the tactical UI:
 *  • Generates unique nodeId, connects to signaling WSS.
 *  • Wires PeerManager → Router → DTNStorage callbacks.
 *  • Vis.js live topology graph with RTT labels and manual zoom controls.
 *  • Viewport-locked interface (no page scrolling, chat controls always pinned).
 *  • Sidebar tab switching (Chat Terminal vs Telemetry).
 *  • Mobile responsive view switching tabs.
 *  • Screen Wake Lock API to prevent OS sleep.
 *  • Service Worker registration.
 */

'use strict';

// ─────────────────────────────────────────────────────────────
// Node ID generation (persistent across reloads via localStorage)
// ─────────────────────────────────────────────────────────────
function getOrCreateNodeId() {
  let id = localStorage.getItem('aether_node_id');
  if (!id) {
    const rand = () => Math.random().toString(36).slice(2, 6).toUpperCase();
    id = `NODE-${rand()}-${rand()}`;
    localStorage.setItem('aether_node_id', id);
  }
  return id;
}

const LOCAL_NODE_ID = getOrCreateNodeId();

// ─────────────────────────────────────────────────────────────
// Application State
// ─────────────────────────────────────────────────────────────
const state = {
  wakeLock:       null,
  partitioned:    false,
  chatMessages:   [],
  visNodes:       null,
  visEdges:       null,
  visNetwork:     null,
  animating:      false,
  currentTab:     'chat',
  currentMobileView: 'topology-panel',
};

// ─────────────────────────────────────────────────────────────
// DOM References
// ─────────────────────────────────────────────────────────────
const DOM = {};

function cacheDOMRefs() {
  DOM.nodeIdEl        = document.getElementById('node-id');
  DOM.degreeEl        = document.getElementById('degree-count');
  DOM.netStateEl      = document.getElementById('net-state');
  DOM.wakeLockEl      = document.getElementById('wakelock-state');
  DOM.sigState        = document.getElementById('sig-state');
  DOM.topoCanvas      = document.getElementById('topology-canvas');
  DOM.dtnPct          = document.getElementById('dtn-pct');
  DOM.dtnBar          = document.getElementById('dtn-bar');
  DOM.dtnCount        = document.getElementById('dtn-count');
  DOM.quickDtnText    = document.getElementById('quick-dtn-text');
  DOM.quickDtnBar     = document.getElementById('quick-dtn-bar');
  DOM.quickVcText     = document.getElementById('quick-vc-text');
  DOM.quickPeersText  = document.getElementById('quick-peers-text');
  DOM.quickGlance     = document.getElementById('quick-glance');
  DOM.vcDisplay       = document.getElementById('vc-display');
  DOM.routeTable      = document.getElementById('route-table');
  DOM.peerListEl      = document.getElementById('peer-list');
  DOM.chatFeed        = document.getElementById('chat-feed');
  DOM.chatInput       = document.getElementById('chat-input');
  DOM.sendBtn         = document.getElementById('send-btn');
  DOM.destSelect      = document.getElementById('dest-select');
  DOM.partitionToggle = document.getElementById('partition-toggle');
  DOM.partitionAlertBanner = document.getElementById('partition-alert-banner');
  DOM.partitionHelpBtn     = document.getElementById('partition-help-btn');
  DOM.partitionModal       = document.getElementById('partition-modal');
  DOM.closePartitionModal   = document.getElementById('close-partition-modal');
  DOM.gotItBtn             = document.getElementById('got-it-btn');
  DOM.tabChatBtn      = document.getElementById('tab-chat-btn');
  DOM.tabTelemetryBtn = document.getElementById('tab-telemetry-btn');
  DOM.chatContainer   = document.getElementById('chat-panel-container');
  DOM.telemetryContainer = document.getElementById('telemetry-panel-container');
  DOM.navTopoBtn      = document.getElementById('nav-topo-btn');
  DOM.navChatBtn      = document.getElementById('nav-chat-btn');
  DOM.navTelemetryBtn = document.getElementById('nav-telemetry-btn');
  DOM.chatUnreadDot   = document.getElementById('chat-unread-dot');
  DOM.zoomInBtn       = document.getElementById('zoom-in-btn');
  DOM.zoomOutBtn      = document.getElementById('zoom-out-btn');
  DOM.zoomFitBtn      = document.getElementById('zoom-fit-btn');
}

// ─────────────────────────────────────────────────────────────
// Service Worker Registration
// ─────────────────────────────────────────────────────────────
async function registerServiceWorker() {
  if (!('serviceWorker' in navigator)) return;
  try {
    const reg = await navigator.serviceWorker.register('/sw.js', { scope: '/' });
    console.log('[APP] Service Worker registered:', reg.scope);
  } catch (err) {
    console.warn('[APP] Service Worker registration failed:', err);
  }
}

// ─────────────────────────────────────────────────────────────
// Screen Wake Lock
// ─────────────────────────────────────────────────────────────
async function acquireWakeLock() {
  if (!('wakeLock' in navigator)) {
    updateUI('wakeLockEl', '⚠ N/A');
    return;
  }
  try {
    state.wakeLock = await navigator.wakeLock.request('screen');
    updateUI('wakeLockEl', '🔒 Active');
    console.log('[APP] Wake Lock acquired.');

    state.wakeLock.addEventListener('release', () => {
      updateUI('wakeLockEl', '🔓 Released');
    });
  } catch (err) {
    console.warn('[APP] Wake Lock failed:', err);
    updateUI('wakeLockEl', '⚠ Inactive');
  }
}

document.addEventListener('visibilitychange', async () => {
  if (document.visibilityState === 'visible' && !state.wakeLock) {
    await acquireWakeLock();
  }
});

// ─────────────────────────────────────────────────────────────
// Signaling WebSocket
// ─────────────────────────────────────────────────────────────
function buildSignalingUrl() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  return `${proto}://${location.host}`;
}

let sigSocket   = null;
let peerManager = null;
let router      = null;

function connectSignaling() {
  const url = buildSignalingUrl();
  updateUI('sigState', '🔄 Connecting');
  console.log(`[APP] Connecting to signaling: ${url}`);

  sigSocket = new WebSocket(url);
  window.sigSocket = sigSocket;

  sigSocket.addEventListener('open', () => {
    updateUI('sigState', '🟢 Connected');
    // If PeerManager already instantiated (reconnect scenario), update reference & register immediately
    if (peerManager) {
      peerManager.updateSignalingSocket(sigSocket);
      sigSocket.send(JSON.stringify({ type: 'register', from: LOCAL_NODE_ID }));
    }
  });

  sigSocket.addEventListener('close', () => {
    updateUI('sigState', '🔴 Reconnecting');
    console.log('[APP] Signaling disconnected. Reconnecting in 3s…');
    setTimeout(connectSignaling, 3000);
  });

  sigSocket.addEventListener('error', (err) => {
    console.error('[APP] Signaling error:', err);
    updateUI('sigState', '⚠ Error');
  });

  sigSocket.addEventListener('message', (event) => {
    let msg;
    try { msg = JSON.parse(event.data); } catch { return; }

    if (msg.type === 'registered') {
      console.log(`[APP] Registered as ${msg.nodeId}, peers: [${(msg.peers || []).join(', ')}]`);
      const live = new Set(msg.peers || []);
      live.add(LOCAL_NODE_ID);
      for (const p of peerManager?.getActivePeerIds() || []) {
        live.add(p);
      }

      // Remove stale vis nodes
      for (const n of state.visNodes.get()) {
        if (!live.has(n.id)) {
          removeVisNode(n.id);
        }
      }

      addVisNode(LOCAL_NODE_ID, true);
      for (const p of msg.peers || []) {
        addVisNode(p, false);
      }
      requestTopologySync();
    }

    if (msg.type === 'peer-joined') {
      addVisNode(msg.nodeId, false);
      requestTopologySync();
    }

    if (msg.type === 'peer-left') {
      const leftId = msg.nodeId || msg.from;
      if (!peerManager?.getActivePeerIds().includes(leftId)) {
        removeVisNode(leftId);
      }
      if (router) router.removeNode(leftId);
      requestTopologySync();
    }
  });

  // Start 15s keepalive ping to prevent routers/firewalls from dropping idle WebSocket
  if (!window._sigKeepAliveInterval) {
    window._sigKeepAliveInterval = setInterval(() => {
      if (sigSocket && sigSocket.readyState === WebSocket.OPEN) {
        sigSocket.send(JSON.stringify({ type: 'ping' }));
      }
    }, 15000);
  }

  return sigSocket;
}

// ─────────────────────────────────────────────────────────────
// Vis.js Topology Graph
// ─────────────────────────────────────────────────────────────
function initVisNetwork() {
  state.visNodes = new vis.DataSet();
  state.visEdges = new vis.DataSet();

  const options = {
    nodes: {
      shape:       'dot',
      size:        22,
      font:        { size: 11, color: '#e2efff', face: 'monospace' },
      borderWidth: 2,
      color: {
        border:     '#00d4aa',
        background: '#0a1e35',
        highlight:  { border: '#00ffcc', background: '#0d2540' },
        hover:      { border: '#00d4aa', background: '#0d2540' },
      },
    },
    edges: {
      width:   2,
      color:   { color: '#1a6a55', highlight: '#00ffcc', hover: '#00d4aa' },
      font:    { size: 10, color: '#7fdbca', face: 'monospace', align: 'middle' },
      smooth:  { type: 'curvedCW', roundness: 0.15 },
      arrows:  { to: false },
    },
    physics: {
      enabled: true,
      solver:  'barnesHut',
      barnesHut: { gravitationalConstant: -2500, springLength: 110, damping: 0.5 },
      stabilization: { iterations: 40, updateInterval: 20 },
    },
    interaction: {
      hover:        true,
      tooltipDelay: 150,
      zoomView:     true,
      dragView:     true,
    },
    layout: { improvedLayout: true },
  };

  state.visNetwork = new vis.Network(DOM.topoCanvas, {
    nodes: state.visNodes,
    edges: state.visEdges,
  }, options);

  // Permanently freeze physics once initial layout settles to prevent CPU/battery runaway
  state.visNetwork.once('stabilizationIterationsDone', () => {
    state.visNetwork.setOptions({ physics: { enabled: false } });
  });
  state.visNetwork.once('stabilized', () => {
    state.visNetwork.setOptions({ physics: { enabled: false } });
  });
  setTimeout(() => {
    if (state.visNetwork) {
      state.visNetwork.setOptions({ physics: { enabled: false } });
    }
  }, 1200);

  // Wire zoom controls
  DOM.zoomInBtn?.addEventListener('click', () => {
    const scale = state.visNetwork.getScale();
    state.visNetwork.moveTo({ scale: scale * 1.3, animation: { duration: 200 } });
  });

  DOM.zoomOutBtn?.addEventListener('click', () => {
    const scale = state.visNetwork.getScale();
    state.visNetwork.moveTo({ scale: scale / 1.3, animation: { duration: 200 } });
  });

  DOM.zoomFitBtn?.addEventListener('click', () => {
    state.visNetwork.fit({ animation: { duration: 300 } });
  });
}

function addVisNode(nodeId, isSelf = false) {
  if (state.visNodes.get(nodeId)) return;
  state.visNodes.add({
    id:    nodeId,
    label: isSelf ? `★ ${nodeId}` : nodeId,
    title: isSelf ? 'This Node (Local)' : `Peer: ${nodeId}`,
    color: isSelf ? {
      border:     '#ffd700',
      background: '#1a1400',
      highlight:  { border: '#ffe066', background: '#2a2000' },
    } : undefined,
    size: isSelf ? 28 : 22,
  });
}

function removeVisNode(nodeId) {
  state.visNodes.remove(nodeId);
  const toRemove = state.visEdges.get().filter(e => e.from === nodeId || e.to === nodeId);
  state.visEdges.remove(toRemove.map(e => e.id));
}

function getEdgeId(a, b) {
  return [a, b].sort().join('--');
}

function addVisEdge(nodeA, nodeB, rtt) {
  const edgeId = getEdgeId(nodeA, nodeB);
  const rttNum = (rtt < Infinity && rtt >= 0) ? Math.round(rtt) : -1;
  const label  = rttNum >= 0 ? `${rttNum}ms` : '…';
  const existing = state.visEdges.get(edgeId);

  if (existing) {
    const oldRtt = existing._rttNum !== undefined ? existing._rttNum : -1;
    // Only update if latency change is meaningful (>15ms or status changed) to stop canvas thrashing
    if (oldRtt === -1 || (rttNum >= 0 && Math.abs(oldRtt - rttNum) > 15) || (oldRtt >= 0 && rttNum < 0)) {
      state.visEdges.update({ id: edgeId, label, _rttNum: rttNum });
    }
  } else {
    state.visEdges.add({ id: edgeId, from: nodeA, to: nodeB, label, _rttNum: rttNum });
  }
}

function removeVisEdge(nodeA, nodeB) {
  const edgeId = getEdgeId(nodeA, nodeB);
  if (state.visEdges.get(edgeId)) {
    state.visEdges.remove(edgeId);
  }
}

// ─────────────────────────────────────────────────────────────
// Animate Packet Route
// ─────────────────────────────────────────────────────────────
function animatePacketRoute(visitedNodesArray) {
  if (document.hidden || state.animating || !state.visNetwork || visitedNodesArray.length < 2) return;
  state.animating = true;

  const PULSE_COLOR   = '#ff6b35';
  const RESTORE_DELAY = 260;

  const originalNodeColors = new Map();
  const originalEdgeColors = new Map();

  let step = 0;
  const interval = setInterval(() => {
    if (step >= visitedNodesArray.length) {
      setTimeout(() => {
        for (const [id, color] of originalNodeColors) {
          state.visNodes.update({ id, color });
        }
        for (const [id, color] of originalEdgeColors) {
          state.visEdges.update({ id, color });
        }
        state.animating = false;
      }, 700);
      clearInterval(interval);
      return;
    }

    const nodeId = visitedNodesArray[step];
    const node   = state.visNodes.get(nodeId);
    if (node) {
      originalNodeColors.set(nodeId, node.color || {});
      state.visNodes.update({
        id:    nodeId,
        color: { border: PULSE_COLOR, background: '#3d1a00' },
      });
    }

    if (step > 0) {
      const edgeId = getEdgeId(visitedNodesArray[step - 1], nodeId);
      const edge   = state.visEdges.get(edgeId);
      if (edge) {
        originalEdgeColors.set(edgeId, edge.color || {});
        state.visEdges.update({ id: edgeId, color: { color: PULSE_COLOR } });
      }
    }

    step++;
  }, RESTORE_DELAY);
}

// ─────────────────────────────────────────────────────────────
// UI Update Helpers
// ─────────────────────────────────────────────────────────────
function updateUI(domKey, value) {
  if (DOM[domKey]) DOM[domKey].textContent = value;
}

function updateDegree() {
  const k = peerManager?.getActivePeerIds().length ?? 0;
  DOM.degreeEl.textContent = `${k} / 3`;
  DOM.degreeEl.style.color = k === 3 ? '#ff6b35' : k === 0 ? '#888' : '#00d4aa';
  if (DOM.quickPeersText) DOM.quickPeersText.textContent = `${k}/3`;
}

function updateNetState() {
  if (!peerManager) return;
  const k = peerManager.getActivePeerIds().length;
  const text = k === 0 ? '⬤ ISOLATED'
             : k < 3   ? `⬤ PARTIAL (K=${k})`
             :            '⬤ MESH FULL';
  const color = k === 0 ? '#ff4444' : k < 3 ? '#ffa500' : '#00d4aa';
  DOM.netStateEl.textContent = text;
  DOM.netStateEl.style.color = color;
}

async function updateTelemetry() {
  if (!window.dtnStorage) return;
  const stats = await dtnStorage.getStats();
  const pct   = parseFloat(stats.pct);

  DOM.dtnPct.textContent   = `${stats.pct}%`;
  DOM.dtnCount.textContent = `${stats.count} / ${stats.max} bundles stored`;
  DOM.dtnBar.style.width   = `${pct}%`;
  DOM.dtnBar.style.background = pct > 80 ? '#ff4444' : pct > 50 ? '#ffa500' : '#00d4aa';

  // Quick glance updates
  if (DOM.quickDtnText) DOM.quickDtnText.textContent = `${stats.count}/${stats.max}`;
  if (DOM.quickDtnBar)  DOM.quickDtnBar.style.width   = `${pct}%`;

  // Vector clock
  if (router) {
    const vc   = router.getVectorClock();
    const totalClock = Object.values(vc).reduce((a, b) => a + b, 0);
    if (DOM.quickVcText) DOM.quickVcText.textContent = String(totalClock);

    const prevVcHtml = DOM.vcDisplay.getAttribute('data-rendered');
    if (prevVcHtml !== vcHtml) {
      DOM.vcDisplay.innerHTML = vcHtml || '<span class="muted">empty</span>';
      DOM.vcDisplay.setAttribute('data-rendered', vcHtml);
    }

    // Route table
    const rt = router.getRoutingTable();
    const rtEntries = Object.entries(rt);
    const rtHtml = rtEntries
      .map(([dest, info]) => {
        const via = typeof info === 'object' ? `${info.via.slice(0, 10)} (${info.hops}h)` : String(info).slice(0, 12);
        return `<tr><td>${dest.slice(0, 12)}</td><td>${via}</td></tr>`;
      })
      .join('');
    const prevRtHtml = DOM.routeTable.getAttribute('data-rendered');
    if (prevRtHtml !== rtHtml) {
      DOM.routeTable.innerHTML = rtHtml || '<tr><td colspan="2" class="muted">No routes discovered</td></tr>';
      DOM.routeTable.setAttribute('data-rendered', rtHtml);
    }
  }

  // Peer list (Direct peers + Blocked/Simulated out-of-range peers)
  const activePeers = peerManager?.getActivePeerIds() ?? [];
  const blocked = peerManager?.blockedPeers || new Set();

  let peerHtml = '';
  if (activePeers.length || blocked.size) {
    for (const p of activePeers) {
      const rtt = Math.round(peerManager.getRTT(p));
      const rttStr = rtt > 0 && rtt !== Infinity ? `${rtt}ms` : 'probing';
      peerHtml += `
        <div class="peer-badge-row">
          <span class="peer-badge">👤 ${p.slice(0, 14)} (${rttStr})</span>
          <button class="peer-action-btn" data-peer="${p}" data-action="block" title="Simulate moving out of range of this node">⚡ Drop Link</button>
        </div>
      `;
    }
    for (const p of blocked) {
      peerHtml += `
        <div class="peer-badge-row">
          <span class="peer-badge" style="opacity: 0.6; text-decoration: line-through;">👤 ${p.slice(0, 14)} (Blocked)</span>
          <button class="peer-action-btn unblock" data-peer="${p}" data-action="unblock" title="Reconnect direct link">🔌 Reconnect</button>
        </div>
      `;
    }
  } else {
    peerHtml = '<span class="muted">No peers connected</span>';
  }
  const prevPeerHtml = DOM.peerListEl.getAttribute('data-rendered');
  if (prevPeerHtml !== peerHtml) {
    DOM.peerListEl.innerHTML = peerHtml;
    DOM.peerListEl.setAttribute('data-rendered', peerHtml);
  }

  refreshDestSelect();
}

function refreshDestSelect() {
  if (!DOM.destSelect) return;
  const current = DOM.destSelect.value;
  const knownNodes = router ? router.getKnownNodes() : [];
  const directPeers = peerManager?.getActivePeerIds() || [];

  let html = '<option value="BROADCAST">📡 ALL (BROADCAST)</option>';

  if (knownNodes.length > 0) {
    const direct = knownNodes.filter(n => n.isDirect);
    if (direct.length > 0) {
      html += '<optgroup label="Direct Peers (1 Hop)">';
      for (const n of direct) {
        const rtt = peerManager ? Math.round(peerManager.getRTT(n.nodeId)) : 0;
        const rttStr = rtt > 0 && rtt !== Infinity ? ` • ${rtt}ms` : '';
        html += `<option value="${n.nodeId}">👤 ${n.nodeId} (Direct${rttStr})</option>`;
      }
      html += '</optgroup>';
    }

    const multiHop = knownNodes.filter(n => !n.isDirect);
    if (multiHop.length > 0) {
      html += '<optgroup label="Multi-Hop Peers (Relayed)">';
      for (const n of multiHop) {
        html += `<option value="${n.nodeId}">🌐 ${n.nodeId} (via ${n.via.slice(0, 8)} • ${n.hops} hops)</option>`;
      }
      html += '</optgroup>';
    }
  } else if (directPeers.length > 0) {
    for (const p of directPeers) {
      html += `<option value="${p}">👤 ${p}</option>`;
    }
  }

  // Preserve custom target if entered previously
  if (current && current !== 'BROADCAST' && current !== '__CUSTOM__' && !knownNodes.some(n => n.nodeId === current)) {
    html += `<optgroup label="Targeted Peer"><option value="${current}" selected>🎯 ${current}</option></optgroup>`;
  }

  html += '<option value="__CUSTOM__">✏️ Enter Custom Node ID…</option>';

  const prevHtml = DOM.destSelect.getAttribute('data-rendered');
  if (prevHtml !== html) {
    DOM.destSelect.innerHTML = html;
    DOM.destSelect.setAttribute('data-rendered', html);

    if (current && DOM.destSelect.querySelector(`option[value="${current}"]`)) {
      DOM.destSelect.value = current;
    }
  }
}

// ─────────────────────────────────────────────────────────────
// Chat Terminal
// ─────────────────────────────────────────────────────────────
function appendChatMessage(envelope, isOwn = false, deliveredDirectly = true) {
  const feed = DOM.chatFeed;
  if (!feed) return;

  // Check if user was already at the bottom before appending
  const wasAtBottom = (feed.scrollHeight - feed.scrollTop - feed.clientHeight) < 60;

  const div = document.createElement('div');
  div.className = `chat-msg ${isOwn ? 'own' : 'remote'}`;

  const ts = new Date(envelope.timestamp * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  const origin = envelope.originNode === LOCAL_NODE_ID ? '★ You' : envelope.originNode;
  const hops = envelope.hopCount;
  const dest = envelope.destNode === 'BROADCAST' ? '📡 ALL' : envelope.destNode;

  const statusBadge = isOwn
    ? (deliveredDirectly
        ? '<span class="msg-status">✓ Sent P2P</span>'
        : '<span class="msg-status dtn">⏳ DTN Buffered</span>')
    : `<span class="msg-hops">[${hops} hop${hops !== 1 ? 's' : ''}]</span>`;

  div.innerHTML = `
    <div class="msg-meta">
      <span class="msg-origin">${origin}</span>
      <span class="msg-arrow">→</span>
      <span class="msg-dest">${dest}</span>
      ${statusBadge}
      <span class="msg-time">${ts}</span>
    </div>
    <div class="msg-body">${escapeHtml(envelope.payload)}</div>
  `;

  feed.appendChild(div);

  // Auto-scroll ONLY inside the chat feed container if user was at the bottom or sent the message
  if (wasAtBottom || isOwn) {
    feed.scrollTop = feed.scrollHeight;
  }

  // If chat is currently not the active view/tab, show unread indicator
  if (state.currentTab !== 'chat' || (window.innerWidth <= 900 && state.currentMobileView !== 'chat-panel-container')) {
    DOM.chatUnreadDot?.classList.remove('hidden');
  }

  // Animate route if multi-hop
  if (envelope.visitedNodes?.length > 1) {
    animatePacketRoute(envelope.visitedNodes);
  }
}

function escapeHtml(text) {
  return String(text)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function sendChatMessage() {
  const text = DOM.chatInput?.value?.trim();
  if (!text || !router) return;

  const dest = DOM.destSelect?.value || 'BROADCAST';
  const { bundleId, sent, envelope } = router.send(dest, 'TEXT', text);

  // Display message in chat feed immediately
  appendChatMessage(envelope, true, sent);

  if (!sent) {
    console.log(`[DTN] Message saved to Mule Storage (no direct route yet) [${bundleId.slice(0, 8)}]`);
  }

  DOM.chatInput.value = '';
  DOM.chatInput.focus();
}

function addSystemMessage(text) {
  const feed = DOM.chatFeed;
  if (!feed) return;

  const wasAtBottom = (feed.scrollHeight - feed.scrollTop - feed.clientHeight) < 60;

  const div  = document.createElement('div');
  div.className = 'chat-msg system';
  div.innerHTML = `<span class="sys-msg">⚡ ${escapeHtml(text)}</span>`;
  feed.appendChild(div);

  if (wasAtBottom) {
    feed.scrollTop = feed.scrollHeight;
  }
}

// ─────────────────────────────────────────────────────────────
// Partition Simulation Toggle & Modal
// ─────────────────────────────────────────────────────────────
function setupPartitionToggle() {
  if (!DOM.partitionToggle) return;
  DOM.partitionToggle.addEventListener('change', () => {
    state.partitioned = DOM.partitionToggle.checked;
    router?.setPartitioned(state.partitioned);

    const indicator = document.getElementById('partition-label');
    if (indicator) {
      indicator.textContent = state.partitioned
        ? '🔴 PARTITION ACTIVE (DTN Mule Mode)'
        : '🟢 Network Normal';
      indicator.style.color = state.partitioned ? '#ff4444' : '#00d4aa';
    }

    if (DOM.partitionAlertBanner) {
      DOM.partitionAlertBanner.classList.toggle('hidden', !state.partitioned);
    }

    if (state.partitioned) {
      console.log('[PARTITION] Test ENABLED: Node is isolated. Outbound messages buffered in IndexedDB DTN Storage.');
    } else {
      console.log('[PARTITION] Test DISABLED: Node reconnected to mesh. Auto-flushing buffered DTN bundles…');
    }

    updateTelemetry();
  });
}

function setupPartitionModal() {
  DOM.partitionHelpBtn?.addEventListener('click', () => {
    DOM.partitionModal?.classList.remove('hidden');
  });

  const closeModal = () => {
    DOM.partitionModal?.classList.add('hidden');
  };

  DOM.closePartitionModal?.addEventListener('click', closeModal);
  DOM.gotItBtn?.addEventListener('click', closeModal);

  DOM.partitionModal?.addEventListener('click', (e) => {
    if (e.target === DOM.partitionModal) closeModal();
  });
}

function setupPeerActionHandlers() {
  DOM.peerListEl?.addEventListener('click', (e) => {
    const btn = e.target.closest('.peer-action-btn');
    if (!btn) return;
    const peerId = btn.dataset.peer;
    const action = btn.dataset.action;
    if (action === 'block') {
      peerManager.blockPeer(peerId);
      console.log(`[PEER] Direct link with ${peerId} severed (simulated out-of-range). Mesh will relay via multi-hop.`);
    } else {
      peerManager.unblockPeer(peerId);
      console.log(`[PEER] Direct link unblocked for ${peerId}. Re-establishing WebRTC connection…`);
    }
    updateTelemetry();
    refreshDestSelect();
  });
}

function setupDestSelectChange() {
  DOM.destSelect?.addEventListener('change', () => {
    if (DOM.destSelect.value === '__CUSTOM__') {
      const target = prompt('Enter recipient Node ID (e.g. node-xyz):');
      if (target && target.trim()) {
        const clean = target.trim();
        const opt = document.createElement('option');
        opt.value = clean;
        opt.textContent = `🎯 ${clean}`;
        opt.selected = true;
        DOM.destSelect.insertBefore(opt, DOM.destSelect.lastElementChild);
        DOM.destSelect.value = clean;
      } else {
        DOM.destSelect.value = 'BROADCAST';
      }
    }
  });
}

// ─────────────────────────────────────────────────────────────
// Sidebar Tabs & Navigation
// ─────────────────────────────────────────────────────────────
function setupTabsAndNavigation() {
  const appBody = document.getElementById('app-body');

  // Desktop Sidebar tab switching (Chat vs Telemetry)
  DOM.tabChatBtn?.addEventListener('click', () => {
    state.currentTab = 'chat';
    DOM.tabChatBtn.classList.add('active');
    DOM.tabTelemetryBtn?.classList.remove('active');
    DOM.chatContainer?.classList.add('active-tab-content');
    DOM.telemetryContainer?.classList.remove('active-tab-content');
    DOM.chatUnreadDot?.classList.add('hidden');
    if (DOM.chatFeed) DOM.chatFeed.scrollTop = DOM.chatFeed.scrollHeight;
  });

  DOM.tabTelemetryBtn?.addEventListener('click', () => {
    state.currentTab = 'telemetry';
    DOM.tabTelemetryBtn.classList.add('active');
    DOM.tabChatBtn?.classList.remove('active');
    DOM.telemetryContainer?.classList.add('active-tab-content');
    DOM.chatContainer?.classList.remove('active-tab-content');
  });

  // Mobile navigation tabs
  const mobTabs = [
    { btn: document.getElementById('mob-tab-chat'), view: 'chat' },
    { btn: document.getElementById('mob-tab-topo'), view: 'topology' },
    { btn: document.getElementById('mob-tab-telemetry'), view: 'telemetry' },
  ];

  mobTabs.forEach(({ btn, view }) => {
    btn?.addEventListener('click', () => {
      state.currentMobileView = view;
      mobTabs.forEach(t => t.btn?.classList.remove('active'));
      btn.classList.add('active');

      if (appBody) {
        appBody.setAttribute('data-mobile-view', view);
      }

      if (view === 'chat') {
        DOM.chatUnreadDot?.classList.add('hidden');
        DOM.tabChatBtn?.click();
        if (DOM.chatFeed) DOM.chatFeed.scrollTop = DOM.chatFeed.scrollHeight;
        DOM.chatInput?.focus();
      } else if (view === 'telemetry') {
        DOM.tabTelemetryBtn?.click();
      } else if (view === 'topology') {
        setTimeout(() => {
          if (state.visNetwork) {
            state.visNetwork.redraw();
            state.visNetwork.fit({ animation: { duration: 200 } });
          }
        }, 80);
      }
    });
  });

  // Clicking quick glance bar
  DOM.quickGlance?.addEventListener('click', () => {
    if (window.innerWidth <= 800) {
      document.getElementById('mob-tab-telemetry')?.click();
    } else {
      DOM.tabTelemetryBtn?.click();
    }
  });
}

// ─────────────────────────────────────────────────────────────
// Topology sync with PeerManager & Router state (Full Mesh Graph)
// ─────────────────────────────────────────────────────────────
let _syncTopologyTimer = null;
function requestTopologySync() {
  if (_syncTopologyTimer) return;
  _syncTopologyTimer = setTimeout(() => {
    _syncTopologyTimer = null;
    syncTopology();
  }, 350);
}

function syncTopology() {
  if (!state.visNetwork) return;

  const links = router ? router.getAllTopologyLinks() : [];
  const seenEdges = new Set();
  const seenNodes = new Set([LOCAL_NODE_ID]);

  addVisNode(LOCAL_NODE_ID, true);

  for (const link of links) {
    addVisNode(link.from, link.from === LOCAL_NODE_ID);
    addVisNode(link.to, link.to === LOCAL_NODE_ID);
    seenNodes.add(link.from);
    seenNodes.add(link.to);

    addVisEdge(link.from, link.to, link.rtt);
    seenEdges.add(link.id);
  }

  for (const edge of state.visEdges.get()) {
    if (!seenEdges.has(edge.id)) {
      state.visEdges.remove(edge.id);
    }
  }

  // Remove phantom disconnected nodes from canvas (nodes with 0 edges that are not local,
  // not in knownPeers, and not active WebRTC peers)
  for (const node of state.visNodes.get()) {
    if (node.id !== LOCAL_NODE_ID && !seenNodes.has(node.id) && !peerManager?.knownPeers.has(node.id)) {
      removeVisNode(node.id);
    }
  }
}

// ─────────────────────────────────────────────────────────────
// Wire PeerManager callbacks (UI layer)
// ─────────────────────────────────────────────────────────────
function wirePeerCallbacks() {
  const originalConnected = router.peerManager.onPeerConnected;
  const originalDropped   = router.peerManager.onPeerDropped;

  peerManager.onPeerConnected = (nodeId, channel) => {
    if (originalConnected) originalConnected(nodeId, channel);
    addVisNode(nodeId, false);
    addVisEdge(LOCAL_NODE_ID, nodeId, peerManager.getRTT(nodeId));
    console.log(`[PEER] WebRTC Direct Peer Connected: ${nodeId}`);
    updateDegree();
    updateNetState();
    requestTopologySync();
    refreshDestSelect();
  };

  peerManager.onPeerDropped = (nodeId) => {
    if (originalDropped) originalDropped(nodeId);
    removeVisEdge(LOCAL_NODE_ID, nodeId);
    console.log(`[PEER] Peer Dropped: ${nodeId}`);
    updateDegree();
    updateNetState();
    requestTopologySync();
    refreshDestSelect();
  };
}

// ─────────────────────────────────────────────────────────────
// Router message delivery → UI
// ─────────────────────────────────────────────────────────────
function wireRouterCallbacks() {
  router.onMessageDelivered = (envelope) => {
    if (envelope.payloadType === 'TEXT') {
      appendChatMessage(envelope, false);
    }
  };

  router.onVectorClockUpdate = () => {
    updateTelemetry();
  };

  router.onRoutesUpdated = () => {
    requestTopologySync();
    refreshDestSelect();
    updateTelemetry();
  };

  router.onDTNFlushed = (count) => {
    console.log(`[DTN] Mule Engine: auto-flushed ${count} stored bundle(s) across the mesh`);
    updateTelemetry();
  };
}

// ─────────────────────────────────────────────────────────────
// Periodic UI refresh loop
// ─────────────────────────────────────────────────────────────
function startUIRefreshLoop() {
  setInterval(async () => {
    updateDegree();
    updateNetState();
    requestTopologySync();
    await updateTelemetry();
  }, 3000);
}

// ─────────────────────────────────────────────────────────────
// Main Bootstrap
// ─────────────────────────────────────────────────────────────
async function bootstrap() {
  cacheDOMRefs();

  updateUI('nodeIdEl', LOCAL_NODE_ID);
  updateUI('wakeLockEl', '⏳ Requesting');
  updateUI('sigState', '⏳ Starting');
  updateUI('netStateEl', '⬤ OFFLINE');
  updateUI('degreeEl', '0 / 3');

  // Register service worker
  await registerServiceWorker();

  // Acquire screen wake lock
  await acquireWakeLock();

  // Init DTN storage
  await window.dtnStorage.init();

  // Init Vis.js topology
  initVisNetwork();
  addVisNode(LOCAL_NODE_ID, true);

  // Setup tabs and navigation
  setupTabsAndNavigation();

  // Connect signaling
  sigSocket = connectSignaling();

  // Wait briefly for socket to open, then init managers
  await new Promise(resolve => {
    const check = setInterval(() => {
      if (sigSocket.readyState === WebSocket.OPEN || sigSocket.readyState === WebSocket.CLOSED) {
        clearInterval(check);
        resolve();
      }
    }, 100);
    setTimeout(resolve, 2500);
  });

  // Init PeerManager
  peerManager = new PeerManager(LOCAL_NODE_ID, sigSocket);

  // Register with signaling server now that PeerManager is ready to receive
  if (sigSocket.readyState === WebSocket.OPEN) {
    sigSocket.send(JSON.stringify({ type: 'register', from: LOCAL_NODE_ID }));
  } else {
    sigSocket.addEventListener('open', () => {
      sigSocket.send(JSON.stringify({ type: 'register', from: LOCAL_NODE_ID }));
    }, { once: true });
  }

  // Init Router
  router = new Router(LOCAL_NODE_ID, peerManager, window.dtnStorage);

  // Wire callbacks
  wirePeerCallbacks();
  wireRouterCallbacks();

  // Setup partition toggle & modal
  setupPartitionToggle();
  setupPartitionModal();
  setupPeerActionHandlers();
  setupDestSelectChange();

  // Chat send button & enter key
  DOM.sendBtn?.addEventListener('click', sendChatMessage);
  DOM.chatInput?.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendChatMessage();
    }
  });

  // Start UI refresh
  startUIRefreshLoop();

  console.log(`[APP] Node ${LOCAL_NODE_ID} initialised. Awaiting mesh peers…`);
}

document.addEventListener('DOMContentLoaded', bootstrap);
