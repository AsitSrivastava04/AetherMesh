/**
 * peer-manager.js — Bounded K=3 Overlay & WebRTC Peer Manager
 *
 * Production-grade WebRTC lifecycle:
 *  • Hard Degree Cap K=3: strictly limits connections at both initiator and receiver.
 *  • Synchronous Slot Reservation: PeerEntry is created and stored in this.peers
 *    synchronously at time T=0 before any async WebRTC calls, eliminating race conditions,
 *    duplicate connections, and browser tab crashes.
 *  • Deterministic Symmetric Chord-Ring Overlay: computes at most 3 symmetric neighbors
 *    (ring + chord involution) guaranteeing full network connectivity and O(1) degree bounds.
 *  • Deterministic Initiator Rule (localNodeId > remoteNodeId) eliminates glare.
 *  • W3C Perfect Negotiation Pattern (polite peer rollback) cleanly handles offer collisions.
 *  • Single event listener on signaling WebSocket (prevents listener memory leak & runaway storms).
 *  • Signaling 'busy' rejection: informs remote peer local node is at capacity and sets cooldown.
 *  • 8s negotiation timeout to clear stuck / half-open handshakes.
 *  • Immediate RTT ping and smooth latency metrics.
 *  • Independent P2P DataChannels that survive signaling server disconnections.
 */

'use strict';

const K_MAX = 3; // Maximum concurrent direct peers (degree bound)
const HEARTBEAT_INTERVAL_MS = 2500; // 2.5s heartbeat interval
const ICE_SERVERS = [
  { urls: 'stun:stun.l.google.com:19302' },
];

class PeerManager {
  /**
   * @param {string} localNodeId
   * @param {WebSocket} signalingSocket
   */
  constructor(localNodeId, signalingSocket) {
    this.localNodeId     = localNodeId;
    this.signalingSocket = signalingSocket;

    /** @type {Map<string, object>} nodeId → PeerEntry */
    this.peers = new Map();

    /** Set of all known peer IDs discovered via signaling or gossip */
    this.knownPeers = new Set();

    /** Set of peer IDs manually blocked to simulate being out-of-range */
    this.blockedPeers = new Set();

    /** Cooldown timestamps for nodes that reported busy: nodeId → expiry timestamp */
    this.peerCooldowns = new Map();

    /** Fallback timers for responders awaiting offers: nodeId → timerId */
    this._fallbackTimers = new Map();

    /** Heartbeat timer */
    this._heartbeatTimer = null;

    /** Debounce timer for overlay evaluation */
    this._evalTimer = null;

    /** Bound signaling handler for clean attachment/detachment */
    this._boundSigHandler = this._handleSignalingMessage.bind(this);

    /** Callbacks (set by app.js / router.js) */
    this.onPeerConnected = null; // (nodeId, channel) => void
    this.onPeerDropped   = null; // (nodeId) => void
    this.onDataReceived  = null; // (nodeId, data) => void

    this._bindSignalingEvents();
    this._startHeartbeat();
  }

  // ── Deterministic Initiator Rule ──────────────────────────
  shouldInitiateWith(remoteNodeId) {
    return this.localNodeId > remoteNodeId;
  }

  // ── Public: simulate out-of-range (sever/block direct link) ──
  blockPeer(nodeId) {
    this.blockedPeers.add(nodeId);
    this._dropPeer(nodeId, 'simulated-out-of-range');
  }

  unblockPeer(nodeId) {
    this.blockedPeers.delete(nodeId);
    this._scheduleOverlayEval(300);
  }

  isPeerBlocked(nodeId) {
    return this.blockedPeers.has(nodeId);
  }

  // ── Degree & Slot Counting ────────────────────────────────
  /**
   * List of currently established active peer IDs (open DataChannel)
   * @returns {string[]}
   */
  getActivePeerIds() {
    const ids = [];
    for (const [nodeId, entry] of this.peers) {
      if (entry.channel && entry.channel.readyState === 'open') {
        ids.push(nodeId);
      }
    }
    return ids;
  }

  /**
   * Total count of active + in-progress negotiating connections
   * @returns {number}
   */
  getOccupiedSlotsCount() {
    let count = 0;
    for (const [, entry] of this.peers) {
      if (entry.channel && entry.channel.readyState === 'open') {
        count++;
      } else if (entry.pc && entry.pc.connectionState !== 'closed' && entry.pc.connectionState !== 'failed') {
        count++;
      }
    }
    return count;
  }

  // ── Deterministic Chord-Ring Overlay Target Selection ─────
  /**
   * Selects up to K_MAX ideal overlay targets for this node based on a mathematically
   * symmetric ring + chord topology. Guarantees:
   *  1. 100% connectivity (ring cycle spans all nodes).
   *  2. Strict degree bound <= 3 for all nodes.
   *  3. Perfect symmetry: Node A targets Node B <=> Node B targets Node A.
   *  4. Zero isolation: Every node is part of the Hamiltonian ring.
   * @returns {{ ring: string[], chord: string|null, all: string[] }}
   */
  getIdealOverlayTargetsDetail() {
    const all = Array.from(new Set([this.localNodeId, ...this.knownPeers]))
      .filter(id => !this.isPeerBlocked(id))
      .sort();

    const n = all.length;
    if (n <= 1) return { ring: [], chord: null, all: [] };

    // Small network (<= 3 nodes): all nodes connect to each other (degree <= 2)
    if (n <= K_MAX) {
      const direct = all.filter(id => id !== this.localNodeId);
      return { ring: direct, chord: null, all: direct };
    }

    const idx = all.indexOf(this.localNodeId);
    const ring = [
      all[(idx + 1) % n],
      all[(idx - 1 + n) % n]
    ].filter(id => id && id !== this.localNodeId);

    // Symmetric Chord Selection (Harary H_{3,n} 3-regular algorithm)
    let chord = null;
    if (n % 2 === 0) {
      // Even N: Diameter chord (idx + n/2) % n is inherently symmetric
      chord = all[(idx + n / 2) % n];
    } else {
      // Odd N: Symmetric Harary pairing
      const half = (n - 1) / 2;
      if (idx >= 1 && idx <= half) {
        chord = all[idx + half];
      } else if (idx > half) {
        chord = all[idx - half];
      }
      // Note: Node 0 in odd N has degree 2 (ring only) to satisfy Euler's Handshaking Lemma
    }

    if (chord === this.localNodeId || ring.includes(chord)) {
      chord = null;
    }

    const allTargets = [...ring];
    if (chord) allTargets.push(chord);

    return { ring, chord, all: allTargets.slice(0, K_MAX) };
  }

  getIdealOverlayTargets() {
    return this.getIdealOverlayTargetsDetail().all;
  }

  getPeerPriority(peerId) {
    const targets = this.getIdealOverlayTargetsDetail();
    if (targets.ring.includes(peerId)) return 1; // Class 1: Ring neighbor (Top priority)
    if (targets.chord === peerId) return 2;      // Class 2: Chord neighbor
    return 3;                                   // Class 3: Opportunistic / fallback
  }

  _findLowestPriorityPeer() {
    let lowestId = null;
    let lowestPriority = -1; // larger number = lower priority (3 is lowest)
    let highestRtt = -1;

    for (const [nodeId, entry] of this.peers) {
      const prio = this.getPeerPriority(nodeId);
      const rtt = (entry.rtt === Infinity || entry.rtt === null) ? 99999 : entry.rtt;

      if (prio > lowestPriority) {
        lowestPriority = prio;
        lowestId = nodeId;
        highestRtt = rtt;
      } else if (prio === lowestPriority && rtt > highestRtt) {
        lowestId = nodeId;
        highestRtt = rtt;
      }
    }

    return lowestId ? { nodeId: lowestId, priority: lowestPriority } : null;
  }

  // ─────────────────────────────────────────────────────────────
  // Synchronous Peer Creation (Prevents Race Conditions)
  // ─────────────────────────────────────────────────────────────
  /**
   * Synchronously creates and reserves a PeerEntry in this.peers at time T=0.
   * This guarantees that concurrent triggers will immediately see the entry and
   * never create duplicate RTCPeerConnections.
   * @param {string} remoteNodeId
   * @param {boolean} isInitiator
   * @returns {object} PeerEntry
   */
  _getOrCreatePeerEntry(remoteNodeId, isInitiator) {
    let entry = this.peers.get(remoteNodeId);
    if (entry) return entry;

    const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });

    entry = {
      nodeId:             remoteNodeId,
      pc,
      channel:            null,
      rtt:                Infinity,
      pingPend:           null,
      isInitiator:        Boolean(isInitiator),
      makingOffer:        false,
      ignoreOffer:        false,
      pendingCandidates:  [],
      disconnectTimer:    null,
      negotiationTimeout: null,
      createdAt:          Date.now(),
    };

    // Store in this.peers IMMEDIATELY and SYNCHRONOUSLY!
    this.peers.set(remoteNodeId, entry);

    // 8-second negotiation timeout: clean up if handshake fails to open
    entry.negotiationTimeout = setTimeout(() => {
      if (!entry.channel || entry.channel.readyState !== 'open') {
        console.warn(`[PEER] Negotiation timed out for ${remoteNodeId} (8s). Dropping.`);
        this._dropPeer(remoteNodeId, 'negotiation-timeout');
      }
    }, 8000);

    pc.onicecandidate = ({ candidate }) => {
      if (candidate) {
        this._sigSend({
          type:    'candidate',
          from:    this.localNodeId,
          to:      remoteNodeId,
          payload: candidate,
        });
      }
    };

    pc.onconnectionstatechange = () => {
      const state = pc.connectionState;
      console.log(`[PEER] ${remoteNodeId} connectionState → ${state}`);

      if (state === 'connected') {
        if (entry.disconnectTimer) {
          clearTimeout(entry.disconnectTimer);
          entry.disconnectTimer = null;
        }
      } else if (state === 'disconnected') {
        if (!entry.disconnectTimer) {
          entry.disconnectTimer = setTimeout(() => {
            if (pc.connectionState === 'disconnected') {
              console.warn(`[PEER] Disconnect timeout expired for ${remoteNodeId}`);
              this._dropPeer(remoteNodeId, 'disconnected-timeout');
            }
          }, 6000);
        }
      } else if (state === 'failed' || state === 'closed') {
        if (entry.disconnectTimer) clearTimeout(entry.disconnectTimer);
        this._dropPeer(remoteNodeId, state);
      }
    };

    pc.oniceconnectionstatechange = () => {
      const iceState = pc.iceConnectionState;
      if (iceState === 'connected' || iceState === 'completed') {
        if (entry.disconnectTimer) {
          clearTimeout(entry.disconnectTimer);
          entry.disconnectTimer = null;
        }
      } else if (iceState === 'failed') {
        this._dropPeer(remoteNodeId, 'ice-failed');
      }
    };

    if (isInitiator) {
      try {
        const channel = pc.createDataChannel('aether-mesh', { ordered: true });
        entry.channel = channel;
        this._bindChannelEvents(remoteNodeId, channel);
      } catch (err) {
        console.error(`[PEER] DataChannel creation error for ${remoteNodeId}:`, err);
      }
    } else {
      pc.ondatachannel = ({ channel }) => {
        console.log(`[PEER] ondatachannel received from ${remoteNodeId}`);
        entry.channel = channel;
        this._bindChannelEvents(remoteNodeId, channel);
      };
    }

    return entry;
  }

  // ── Public: initiate connection to a remote peer ──────────
  async connectTo(remoteNodeId) {
    if (remoteNodeId === this.localNodeId) return;
    if (this.isPeerBlocked(remoteNodeId)) return;

    // Check cooldown (if remote reported busy recently)
    const cooldownUntil = this.peerCooldowns.get(remoteNodeId) || 0;
    if (Date.now() < cooldownUntil) {
      return;
    }

    const existing = this.peers.get(remoteNodeId);
    if (existing) {
      if (existing.channel && existing.channel.readyState === 'open') {
        return; // Already established
      }
      if (existing.pc && (existing.pc.connectionState === 'connecting' || existing.pc.connectionState === 'new' || existing.pc.signalingState !== 'stable')) {
        return; // In-flight negotiation
      }
      this._dropPeer(remoteNodeId, 'reconnecting');
    }

    // Strict K=3 capacity limit:
    if (this.getOccupiedSlotsCount() >= K_MAX) {
      console.log(`[PEER] K=${K_MAX} degree cap reached. Cannot initiate connection to ${remoteNodeId}`);
      return;
    }

    console.log(`[PEER] Initiating WebRTC connection → ${remoteNodeId} (Current degree: ${this.getActivePeerIds().length}/${K_MAX})`);

    // Synchronously create and store the peer entry before any async execution
    const entry = this._getOrCreatePeerEntry(remoteNodeId, true);

    try {
      entry.makingOffer = true;
      const offer = await entry.pc.createOffer();
      await entry.pc.setLocalDescription(offer);
      this._sigSend({
        type:    'offer',
        from:    this.localNodeId,
        to:      remoteNodeId,
        payload: offer,
      });
    } catch (err) {
      console.error(`[PEER] Error creating offer for ${remoteNodeId}:`, err);
      this._dropPeer(remoteNodeId, 'offer-error');
    } finally {
      entry.makingOffer = false;
    }
  }

  // ── Public: send raw string to a peer ─────────────────────
  send(remoteNodeId, data) {
    const entry = this.peers.get(remoteNodeId);
    if (!entry || !entry.channel || entry.channel.readyState !== 'open') {
      return false;
    }
    try {
      entry.channel.send(data);
      return true;
    } catch (err) {
      console.error(`[PEER] Send error to ${remoteNodeId}:`, err);
      return false;
    }
  }

  // ── Public: broadcast to all open channels ─────────────────
  broadcast(data) {
    let sentCount = 0;
    for (const [nodeId, entry] of this.peers) {
      if (entry.channel && entry.channel.readyState === 'open') {
        try {
          entry.channel.send(data);
          sentCount++;
        } catch (err) {
          console.warn(`[PEER] Broadcast failed to ${nodeId}:`, err);
        }
      }
    }
    return sentCount;
  }

  // ── Public: get RTT for a peer (ms) ───────────────────────
  getRTT(nodeId) {
    return this.peers.get(nodeId)?.rtt ?? Infinity;
  }

  // ── Public: disconnect a specific peer ────────────────────
  disconnectPeer(nodeId) {
    this._dropPeer(nodeId, 'manual');
  }

  // ── Immediate RTT Ping ────────────────────────────────────
  _pingPeer(nodeId) {
    const entry = this.peers.get(nodeId);
    if (entry?.channel?.readyState === 'open') {
      const seq = Date.now();
      entry.pingPend = { seq, sentAt: Date.now() };
      try {
        entry.channel.send(JSON.stringify({ _type: 'ping', seq }));
      } catch {}
    }
  }

  // ── DataChannel event binding ──────────────────────────────
  _bindChannelEvents(remoteNodeId, channel) {
    const handleOpen = () => {
      console.log(`[PEER] DataChannel OPEN ↔ ${remoteNodeId}`);

      const entry = this.peers.get(remoteNodeId);
      if (!entry) return;

      // Clear negotiation timeout
      if (entry.negotiationTimeout) {
        clearTimeout(entry.negotiationTimeout);
        entry.negotiationTimeout = null;
      }

      // Clear responder fallback timer
      const fallback = this._fallbackTimers.get(remoteNodeId);
      if (fallback) {
        clearTimeout(fallback);
        this._fallbackTimers.delete(remoteNodeId);
      }

      // Clear any busy cooldown
      this.peerCooldowns.delete(remoteNodeId);

      // Rapid RTT probe: ping immediately on connection open!
      this._pingPeer(remoteNodeId);

      // Safeguard: prune surplus if degree strictly exceeds K_MAX
      const activeIds = this.getActivePeerIds();
      if (activeIds.length > K_MAX) {
        console.warn(`[PEER] Hard cap exceeded: ${activeIds.length} > ${K_MAX}. Pruning surplus peer immediately.`);
        this._pruneHighestLatency();
      }

      if (typeof this.onPeerConnected === 'function') {
        this.onPeerConnected(remoteNodeId, channel);
      }

      this._gossipPeers(channel);
    };

    if (channel.readyState === 'open') {
      setTimeout(handleOpen, 0);
    } else {
      channel.onopen = handleOpen;
    }

    channel.onclose = () => {
      console.log(`[PEER] DataChannel CLOSED ↔ ${remoteNodeId}`);
      this._dropPeer(remoteNodeId, 'channel-closed');
    };

    channel.onerror = (err) => {
      console.error(`[PEER] DataChannel error ↔ ${remoteNodeId}:`, err);
    };

    channel.onmessage = ({ data }) => {
      this._handleChannelMessage(remoteNodeId, data);
    };
  }

  // ── Handle incoming DataChannel messages ──────────────────
  _handleChannelMessage(remoteNodeId, raw) {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      if (typeof this.onDataReceived === 'function') {
        this.onDataReceived(remoteNodeId, raw);
      }
      return;
    }

    // Heartbeat pong
    if (msg._type === 'pong' && msg.seq !== undefined) {
      const entry = this.peers.get(remoteNodeId);
      if (entry?.pingPend?.seq === msg.seq) {
        entry.rtt = Math.max(1, Date.now() - entry.pingPend.sentAt);
        entry.pingPend = null;
      }
      return;
    }

    // Heartbeat ping — respond immediately
    if (msg._type === 'ping') {
      const entry = this.peers.get(remoteNodeId);
      if (entry?.channel?.readyState === 'open') {
        try {
          entry.channel.send(JSON.stringify({ _type: 'pong', seq: msg.seq }));
        } catch {}
      }
      return;
    }

    // Peer gossip
    if (msg._type === 'gossip' && Array.isArray(msg.peers)) {
      this._handleGossip(msg.peers);
      return;
    }

    // Application & Routing data — pass to router
    if (typeof this.onDataReceived === 'function') {
      this.onDataReceived(remoteNodeId, raw);
    }
  }

  _gossipPeers(channel) {
    const knownIds = Array.from(new Set([this.localNodeId, ...this.knownPeers, ...this.getActivePeerIds()]));
    const gossip   = JSON.stringify({ _type: 'gossip', peers: knownIds });
    try { channel.send(gossip); } catch {}
  }

  _handleGossip(peerIds) {
    let newFound = false;
    for (const id of peerIds) {
      if (id !== this.localNodeId && !this.knownPeers.has(id)) {
        this.knownPeers.add(id);
        newFound = true;
      }
    }
    if (newFound) {
      this._scheduleOverlayEval(400);
    }
  }

  // ── Debounced Overlay Evaluation ──────────────────────────
  _scheduleOverlayEval(delayMs = 300) {
    if (this._evalTimer) clearTimeout(this._evalTimer);
    this._evalTimer = setTimeout(() => {
      this._evalTimer = null;
      this._evaluateOverlayConnections();
    }, delayMs);
  }

  // ── Structured Overlay Evaluation (Self-Healing & Degree Cap) ──
  _evaluateOverlayConnections() {
    const detail = this.getIdealOverlayTargetsDetail();
    const idealTargets = detail.all;

    // Aggressive un-isolation: if this node is currently completely isolated (0 connections),
    // clear busy cooldowns on ideal targets so we don't sit stranded waiting for timers
    if (this.getActivePeerIds().length === 0 && this.knownPeers.size > 0) {
      for (const targetId of idealTargets) {
        this.peerCooldowns.delete(targetId);
      }
    }

    // 1. First prioritize ideal Chord-Ring targets (Ring first, then Chord)
    for (const targetId of idealTargets) {
      const existing = this.peers.get(targetId);
      const isConnectingOrOpen = existing && (
        (existing.channel && existing.channel.readyState === 'open') ||
        (existing.pc && existing.pc.connectionState !== 'closed' && existing.pc.connectionState !== 'failed')
      );

      const isBusy = (this.peerCooldowns.get(targetId) || 0) > Date.now();
      if (isConnectingOrOpen || this.isPeerBlocked(targetId) || isBusy) {
        continue;
      }

      const targetPriority = this.getPeerPriority(targetId);

      // If slot is available:
      if (this.getOccupiedSlotsCount() < K_MAX) {
        if (this.shouldInitiateWith(targetId)) {
          this.connectTo(targetId);
        } else {
          this._setupResponderFallback(targetId);
        }
      } else {
        // We are at capacity (K=3). Can we rebalance for this ideal target?
        const lowest = this._findLowestPriorityPeer();
        if (lowest && targetPriority < lowest.priority && this.shouldInitiateWith(targetId)) {
          console.log(`[PEER] Rebalancing for ideal target ${targetId} (priority ${targetPriority}): pruning lower-priority peer ${lowest.nodeId} (priority ${lowest.priority})`);
          this.peerCooldowns.set(lowest.nodeId, Date.now() + 12000);
          this._dropPeer(lowest.nodeId, 'rebalance-for-ideal-target');
          this.connectTo(targetId);
        }
      }
    }

    // 2. Under-capacity fallback: If degree is still < K_MAX, connect to ANY known reachable peer
    // to guarantee that no node is left under-connected
    if (this.getOccupiedSlotsCount() < K_MAX) {
      for (const candidateId of this.knownPeers) {
        if (this.getOccupiedSlotsCount() >= K_MAX) break;
        if (candidateId === this.localNodeId || this.isPeerBlocked(candidateId)) continue;

        const existing = this.peers.get(candidateId);
        const isBusy = (this.peerCooldowns.get(candidateId) || 0) > Date.now();
        if (!existing && !isBusy) {
          if (this.shouldInitiateWith(candidateId)) {
            this.connectTo(candidateId);
          } else {
            this._setupResponderFallback(candidateId);
          }
        }
      }
    }
  }

  _setupResponderFallback(remoteNodeId) {
    if (this._fallbackTimers.has(remoteNodeId)) return;
    const timer = setTimeout(() => {
      this._fallbackTimers.delete(remoteNodeId);
      const entry = this.peers.get(remoteNodeId);
      const isOpen = entry?.channel?.readyState === 'open';
      const isBusy = (this.peerCooldowns.get(remoteNodeId) || 0) > Date.now();
      if (!isOpen && !isBusy && this.getOccupiedSlotsCount() < K_MAX && !this.isPeerBlocked(remoteNodeId)) {
        console.log(`[PEER] Responder fallback: no incoming offer from ${remoteNodeId} after 7s. Initiating.`);
        this.connectTo(remoteNodeId);
      }
    }, 7000);
    this._fallbackTimers.set(remoteNodeId, timer);
  }

  // ── Prune lowest-priority / highest-latency peer if degree strictly exceeds K_MAX ──
  _pruneHighestLatency() {
    const candidates = [...this.peers.values()].filter(
      e => e.channel?.readyState === 'open'
    );

    if (candidates.length <= K_MAX) return;

    // Prune lowest priority peer first (Priority 3 first, then Priority 2, then Priority 1)
    let worstId       = null;
    let worstPriority = -1;
    let worstRTT      = -1;

    for (const e of candidates) {
      const prio  = this.getPeerPriority(e.nodeId);
      const score = (e.rtt === Infinity || e.rtt === null) ? 99999 : e.rtt;

      if (prio > worstPriority) {
        worstPriority = prio;
        worstId       = e.nodeId;
        worstRTT      = score;
      } else if (prio === worstPriority && score > worstRTT) {
        worstId  = e.nodeId;
        worstRTT = score;
      }
    }

    if (worstId) {
      console.warn(`[PEER] Dynamic Prune: dropping peer ${worstId} (priority ${worstPriority}, ${worstRTT} ms) to enforce K=${K_MAX}`);
      this._dropPeer(worstId, 'priority-latency-pruned');
    }
  }

  _dropPeer(nodeId, reason) {
    const entry = this.peers.get(nodeId);
    if (!entry) return;

    console.log(`[PEER] Dropping ${nodeId} (${reason})`);
    if (entry.disconnectTimer) {
      clearTimeout(entry.disconnectTimer);
      entry.disconnectTimer = null;
    }
    if (entry.negotiationTimeout) {
      clearTimeout(entry.negotiationTimeout);
      entry.negotiationTimeout = null;
    }
    const fallback = this._fallbackTimers.get(nodeId);
    if (fallback) {
      clearTimeout(fallback);
      this._fallbackTimers.delete(nodeId);
    }

    try { entry.channel?.close(); } catch {}
    try { entry.pc?.close();      } catch {}
    this.peers.delete(nodeId);

    if (typeof this.onPeerDropped === 'function') {
      this.onPeerDropped(nodeId);
    }

    // Calm self-healing overlay evaluation
    this._scheduleOverlayEval(1500);
  }

  // ── Heartbeat & Self-Healing Maintenance ─────────────────
  _startHeartbeat() {
    this._heartbeatTimer = setInterval(() => {
      let seq = Date.now();
      for (const [, entry] of this.peers) {
        if (entry.channel?.readyState === 'open') {
          entry.pingPend = { seq, sentAt: Date.now() };
          try {
            entry.channel.send(JSON.stringify({ _type: 'ping', seq }));
          } catch {}
          seq++;
        }
      }

      // Clean up expired cooldowns
      const now = Date.now();
      for (const [id, until] of this.peerCooldowns) {
        if (now >= until) this.peerCooldowns.delete(id);
      }

      // If degree is below K_MAX, query signaling server for peers and evaluate overlay
      if (this.getOccupiedSlotsCount() < K_MAX) {
        if (this.signalingSocket?.readyState === WebSocket.OPEN) {
          this._sigSend({ type: 'request-peers', from: this.localNodeId });
        }
        this._scheduleOverlayEval(200);
      }
    }, HEARTBEAT_INTERVAL_MS);
  }

  // ── Signaling Event Handling (Guaranteed Single Listener) ──
  _bindSignalingEvents() {
    if (!this.signalingSocket) return;
    this.signalingSocket.removeEventListener('message', this._boundSigHandler);
    this.signalingSocket.addEventListener('message', this._boundSigHandler);
  }

  updateSignalingSocket(newSocket) {
    if (this.signalingSocket) {
      this.signalingSocket.removeEventListener('message', this._boundSigHandler);
    }
    this.signalingSocket = newSocket;
    this._bindSignalingEvents();
  }

  async _handleSignalingMessage(event) {
    let msg;
    try {
      msg = JSON.parse(event.data);
    } catch {
      return;
    }

    const { type, from, payload } = msg;

    if (type === 'offer') {
      await this._handleOffer(from, payload);
    } else if (type === 'answer') {
      await this._handleAnswer(from, payload);
    } else if (type === 'candidate') {
      await this._handleCandidate(from, payload);
    } else if (type === 'busy') {
      this._handleBusy(from);
    } else if (type === 'peer-joined') {
      const joinedId = msg.nodeId || from;
      if (joinedId && joinedId !== this.localNodeId) {
        this.knownPeers.add(joinedId);
        this._scheduleOverlayEval(300);
      }
    } else if (type === 'peer-left') {
      const leftId = msg.nodeId || from;
      this.knownPeers.delete(leftId);
      this.peerCooldowns.delete(leftId);
      const entry = this.peers.get(leftId);
      if (entry && entry.channel && entry.channel.readyState === 'open') {
        console.log(`[PEER] Signaling peer-left for ${leftId}, but WebRTC channel is OPEN. Preserving P2P link.`);
      } else {
        this._dropPeer(leftId, 'signaling-peer-left');
      }
      this._scheduleOverlayEval(500);
    } else if (type === 'registered' || type === 'peer-list') {
      const peerList = msg.peers || payload?.peers || [];
      console.log(`[PEER] Discovered ${peerList.length} peer(s) via signaling.`);
      // Sync known peers with live list from server + any currently active open direct peers
      const live = new Set(peerList.filter(pid => pid !== this.localNodeId));
      for (const activeId of this.getActivePeerIds()) {
        live.add(activeId);
      }
      this.knownPeers = live;
      this._scheduleOverlayEval(300);
    }
  }

  // ── Handle Incoming Offer (Strict Capacity Guard with Priority Rebalance) ─────────
  async _handleOffer(remoteNodeId, offer) {
    if (this.isPeerBlocked(remoteNodeId)) {
      console.log(`[PEER] Ignoring offer from simulated out-of-range peer ${remoteNodeId}`);
      return;
    }

    let entry = this.peers.get(remoteNodeId);

    // If new incoming connection from a peer not currently in this.peers:
    if (!entry) {
      const incomingPriority = this.getPeerPriority(remoteNodeId);

      // ── CRITICAL K=3 DEGREE GUARD WITH ADAPTIVE REBALANCING ──
      if (this.getOccupiedSlotsCount() >= K_MAX) {
        const lowest = this._findLowestPriorityPeer();

        // If incoming peer is strictly higher priority than our lowest active peer
        // (1 = Ring, 2 = Chord, 3 = Opportunistic; so incomingPriority < lowest.priority)
        if (lowest && incomingPriority < lowest.priority) {
          console.log(`[PEER] Rebalancing overlay: dropping lower-priority peer ${lowest.nodeId} (priority ${lowest.priority}) to accept incoming priority ${incomingPriority} peer ${remoteNodeId}`);
          this.peerCooldowns.set(lowest.nodeId, Date.now() + 12000);
          this._dropPeer(lowest.nodeId, 'rebalance-for-ideal-target');
        } else {
          console.warn(`[PEER] K=${K_MAX} degree cap reached. Refusing incoming offer from ${remoteNodeId} (priority ${incomingPriority})`);
          this._sigSend({
            type:    'busy',
            from:    this.localNodeId,
            to:      remoteNodeId,
            payload: { reason: 'degree-cap-reached', k: K_MAX }
          });
          return;
        }
      }

      // Synchronously allocate and store entry
      entry = this._getOrCreatePeerEntry(remoteNodeId, false);
    }

    const pc = entry.pc;
    const isPolite = !this.shouldInitiateWith(remoteNodeId);
    const offerCollision = (entry.makingOffer || pc.signalingState !== 'stable');

    entry.ignoreOffer = !isPolite && offerCollision;
    if (entry.ignoreOffer) {
      console.warn(`[PEER] Impolite peer ignoring offer collision from ${remoteNodeId}`);
      return;
    }

    try {
      if (offerCollision && isPolite) {
        console.log(`[PEER] Polite peer rolling back local offer for ${remoteNodeId}`);
        await pc.setLocalDescription({ type: 'rollback' });
      }

      await pc.setRemoteDescription(new RTCSessionDescription(offer));
      await this._flushCandidateQueue(entry);

      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);

      this._sigSend({
        type:    'answer',
        from:    this.localNodeId,
        to:      remoteNodeId,
        payload: answer,
      });
    } catch (err) {
      console.error(`[PEER] Error handling offer from ${remoteNodeId}:`, err);
    }
  }

  // ── Remote peer reported busy/full ────────────────────────
  _handleBusy(remoteNodeId) {
    console.log(`[PEER] Remote node ${remoteNodeId} reported busy (at capacity). Cooling down for 15s.`);
    // Set 15s cooldown so we don't spam offers to this node
    this.peerCooldowns.set(remoteNodeId, Date.now() + 15000);

    const fallback = this._fallbackTimers.get(remoteNodeId);
    if (fallback) {
      clearTimeout(fallback);
      this._fallbackTimers.delete(remoteNodeId);
    }

    const entry = this.peers.get(remoteNodeId);
    if (entry && (!entry.channel || entry.channel.readyState !== 'open')) {
      this._dropPeer(remoteNodeId, 'remote-busy');
    }
  }

  async _handleAnswer(remoteNodeId, answer) {
    console.log(`[PEER] Received answer from ${remoteNodeId}`);
    const entry = this.peers.get(remoteNodeId);
    if (!entry || !entry.pc) {
      console.warn(`[PEER] Answer from unknown or missing peer ${remoteNodeId}`);
      return;
    }

    try {
      if (entry.pc.signalingState === 'have-local-offer') {
        await entry.pc.setRemoteDescription(new RTCSessionDescription(answer));
        await this._flushCandidateQueue(entry);
      }
    } catch (err) {
      console.error(`[PEER] Error handling answer from ${remoteNodeId}:`, err);
    }
  }

  async _handleCandidate(remoteNodeId, candidate) {
    let entry = this.peers.get(remoteNodeId);
    if (!entry) return;

    try {
      if (entry.pc && entry.pc.remoteDescription && entry.pc.remoteDescription.type) {
        await entry.pc.addIceCandidate(new RTCIceCandidate(candidate));
      } else {
        entry.pendingCandidates.push(candidate);
      }
    } catch (err) {
      if (!entry.ignoreOffer) {
        console.warn(`[PEER] ICE candidate error from ${remoteNodeId}:`, err);
      }
    }
  }

  async _flushCandidateQueue(entry) {
    if (!entry || !entry.pendingCandidates || !entry.pendingCandidates.length) return;
    const candidates = [...entry.pendingCandidates];
    entry.pendingCandidates = [];
    for (const cand of candidates) {
      try {
        await entry.pc.addIceCandidate(new RTCIceCandidate(cand));
      } catch (err) {
        console.warn(`[PEER] Queued ICE candidate failed for ${entry.nodeId}:`, err);
      }
    }
  }

  _sigSend(msg) {
    const ws = (this.signalingSocket && this.signalingSocket.readyState === WebSocket.OPEN)
      ? this.signalingSocket
      : (window.sigSocket && window.sigSocket.readyState === WebSocket.OPEN ? window.sigSocket : null);
    if (ws) {
      ws.send(JSON.stringify(msg));
    }
  }

  destroy() {
    clearInterval(this._heartbeatTimer);
    if (this._evalTimer) clearTimeout(this._evalTimer);
    if (this.signalingSocket) {
      this.signalingSocket.removeEventListener('message', this._boundSigHandler);
    }
    for (const [, timer] of this._fallbackTimers) {
      clearTimeout(timer);
    }
    this._fallbackTimers.clear();
    for (const [nodeId] of this.peers) {
      this._dropPeer(nodeId, 'destroy');
    }
  }
}

window.PeerManager = PeerManager;
