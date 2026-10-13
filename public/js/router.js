/**
 * router.js — Multi-Hop Distance-Vector Mesh Router & Vector Clock CRDT
 *
 * Full multi-hop networking:
 *  • Dynamic distance-vector routing: nodes gossip route tables (ROUTE_ADVERT).
 *  • Multi-hop unicast forwarding: sends to next-hop relay (Node A → Node B → Node C).
 *  • Broadcast mesh flooding with loop detection (visitedNodes set).
 *  • Multi-hop DTN Mule store-and-forward: flushes pending bundles via relay hops.
 *  • Instant partition recovery: auto-flushes all stored DTN bundles on reconnection.
 *  • Vector Clock CRDT with Last-Write-Wins (LWW) conflict resolution.
 */

'use strict';

function uuidv4() {
  if (window.crypto?.randomUUID) return window.crypto.randomUUID();
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

class Router {
  /**
   * @param {string}      localNodeId
   * @param {PeerManager} peerManager
   * @param {DTNStorage}  dtnStorage
   */
  constructor(localNodeId, peerManager, dtnStorage) {
    this.localNodeId  = localNodeId;
    this.peerManager  = peerManager;
    this.dtnStorage   = dtnStorage;

    /** Vector Clock: { [nodeId]: seqNum } */
    this.vectorClock  = { [localNodeId]: 0 };

    /** Message log for dedup: Map<bundleId, { envelope, receivedAt }> */
    this.messageLog   = new Map();

    /**
     * Routing table: Map<destNode, { nextHop: string, cost: number, updated: number }>
     */
    this.routeTable   = new Map();

    /** Callbacks */
    this.onMessageDelivered  = null; // (envelope) => void
    this.onVectorClockUpdate = null; // (clock) => void
    this.onRoutesUpdated     = null; // (table) => void

    /** Network partition simulation flag */
    this.partitioned  = false;

    /** Discovered topology links between remote nodes: key -> { from, to, updated } */
    this.topologyLinks = new Map();

    this._advertTimer = null;
    this._dtnFlushTimer = null;

    this._bindPeerCallbacks();
    this._startRoutingGossip();
    this._startPeriodicDTNFlush();
  }

  // ─────────────────────────────────────────────────────────────
  // Public: Send a user message
  // ─────────────────────────────────────────────────────────────
  /**
   * @param {string} destNode    - target nodeId or "BROADCAST"
   * @param {string} payloadType - "TEXT" | "SYSTEM" | "CRDT_SYNC"
   * @param {*}      payload     - message content
   * @param {number} [ttl=3600]  - seconds
   * @returns {{ bundleId: string, sent: boolean, envelope: object }}
   */
  send(destNode, payloadType, payload, ttl = 3600) {
    if (payloadType === 'TEXT') {
      this.vectorClock[this.localNodeId] = (this.vectorClock[this.localNodeId] || 0) + 1;
    }

    const envelope = {
      bundleId:     uuidv4(),
      originNode:   this.localNodeId,
      destNode,
      visitedNodes: [this.localNodeId],
      timestamp:    Math.floor(Date.now() / 1000),
      hopCount:     0,
      ttl,
      payloadType,
      payload:      typeof payload === 'string' ? payload : JSON.stringify(payload),
      vectorClock:  { ...this.vectorClock },
    };

    console.log(`[ROUTER] Outbound ${payloadType} → ${destNode} [${envelope.bundleId.slice(0, 8)}]`);
    const sent = this._route(envelope);
    return { bundleId: envelope.bundleId, sent, envelope };
  }

  // ─────────────────────────────────────────────────────────────
  // Internal: Route an envelope (Local delivery, Unicast, Broadcast, DTN)
  // ─────────────────────────────────────────────────────────────
  _route(envelope) {
    this.messageLog.set(envelope.bundleId, { envelope, receivedAt: Date.now() });

    const { destNode, originNode } = envelope;

    // ── Local delivery if intended for us or broadcast ──────
    if (destNode === this.localNodeId || destNode === 'BROADCAST') {
      if (originNode !== this.localNodeId) {
        this._deliverLocally(envelope);
      }
      if (destNode === this.localNodeId) return true; // unicast destination reached
    }

    // ── Partitioned mode: DTN store only ────────────────────
    if (this.partitioned) {
      console.warn(`[ROUTER] Partition active — storing bundle ${envelope.bundleId.slice(0, 8)} in DTN mule`);
      this.dtnStorage.storeBundle(envelope);
      return false;
    }

    // ── Forwarding across the mesh ──────────────────────────
    const sent = this._forward(envelope);

    if (!sent) {
      console.log(`[ROUTER] No immediate route for ${destNode} — storing bundle in DTN mule storage`);
      this.dtnStorage.storeBundle(envelope);
    }

    return sent;
  }

  // ── Multi-Hop Forwarding Engine ───────────────────────────
  _forward(envelope) {
    const fwd = {
      ...envelope,
      visitedNodes: Array.from(new Set([...(envelope.visitedNodes || []), this.localNodeId])),
      hopCount:     (envelope.hopCount || 0) + 1,
    };

    const peers = this.peerManager.getActivePeerIds();
    if (!peers.length) return false;

    let sent = false;

    // ── Unicast Routing ─────────────────────────────────────
    if (fwd.destNode !== 'BROADCAST') {
      // 1. Direct peer match?
      if (peers.includes(fwd.destNode)) {
        const raw = JSON.stringify(fwd);
        sent = this.peerManager.send(fwd.destNode, raw);
        if (sent) console.log(`[ROUTER] Unicast: direct delivery → ${fwd.destNode}`);
        return sent;
      }

      // 2. Next-hop lookup from routing table
      const route = this.routeTable.get(fwd.destNode);
      if (route && peers.includes(route.nextHop) && !fwd.visitedNodes.includes(route.nextHop)) {
        const raw = JSON.stringify(fwd);
        sent = this.peerManager.send(route.nextHop, raw);
        if (sent) {
          console.log(`[ROUTER] Unicast: routed via nextHop ${route.nextHop} (${route.cost} hops to ${fwd.destNode})`);
          return true;
        }
      }

      // 3. Fallback: forward to unvisited neighbors to find route
      for (const peerId of peers) {
        if (fwd.visitedNodes.includes(peerId)) continue;
        const raw = JSON.stringify(fwd);
        const ok  = this.peerManager.send(peerId, raw);
        if (ok) {
          sent = true;
          break; // unicast: one relay path is enough
        }
      }
      return sent;
    }

    // ── Broadcast Mesh Flooding ─────────────────────────────
    for (const peerId of peers) {
      if (fwd.visitedNodes.includes(peerId)) continue;
      const raw = JSON.stringify(fwd);
      const ok  = this.peerManager.send(peerId, raw);
      if (ok) sent = true;
    }

    return sent;
  }

  _deliverLocally(envelope) {
    if (envelope.vectorClock) {
      this._mergeVectorClock(envelope.vectorClock);
    }
    if (typeof this.onMessageDelivered === 'function') {
      this.onMessageDelivered(envelope);
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Dynamic Distance-Vector Routing Protocol
  // ─────────────────────────────────────────────────────────────
  _startRoutingGossip() {
    this._advertTimer = setInterval(() => {
      this._broadcastRouteAdvert();
    }, 3500);
  }

  _broadcastRouteAdvert() {
    if (this.partitioned) return;
    const activePeers = this.peerManager.getActivePeerIds();
    if (!activePeers.length) return;

    // Build distance vector: { [destNode]: cost }
    const vector = { [this.localNodeId]: 0 };
    for (const [dest, info] of this.routeTable) {
      vector[dest] = info.cost;
    }

    // Direct neighbors with measured RTT
    const neighbors = activePeers.map(p => ({
      peer: p,
      rtt:  Math.round(this.peerManager.getRTT(p))
    }));

    // Gossip known mesh topology across all hops
    const cutoff = Date.now() - 30000;
    const topology = [];
    for (const [, link] of this.topologyLinks) {
      if (link.updated >= cutoff) {
        topology.push({
          from:    link.from,
          to:      link.to,
          rtt:     link.rtt,
          updated: link.updated,
        });
      }
    }

    const payload = JSON.stringify({
      _type:     'ROUTE_ADVERT',
      from:      this.localNodeId,
      neighbors,
      topology,
      vector,
    });

    this.peerManager.broadcast(payload);
  }

  _handleRouteAdvert(fromPeerId, advert) {
    if (!advert || typeof advert.vector !== 'object') return;
    let changed = false;

    // 1. Record direct topology links advertised by neighbors
    if (Array.isArray(advert.neighbors)) {
      for (const item of advert.neighbors) {
        const nbr = typeof item === 'object' ? item.peer : item;
        const rtt = typeof item === 'object' ? item.rtt : null;
        if (nbr && nbr !== fromPeerId) {
          const key = [fromPeerId, nbr].sort().join('--');
          const existing = this.topologyLinks.get(key);
          if (!existing || rtt !== null || Date.now() - existing.updated > 3000) {
            this.topologyLinks.set(key, {
              from:    fromPeerId,
              to:      nbr,
              rtt:     rtt !== null && rtt !== undefined && rtt !== Infinity ? rtt : (existing?.rtt ?? null),
              updated: Date.now(),
            });
            changed = true;
          }
        }
      }
    }

    // 2. Merge propagated mesh-wide topology links (ensures all devices see identical graph)
    if (Array.isArray(advert.topology)) {
      for (const link of advert.topology) {
        if (link && link.from && link.to && link.from !== link.to) {
          const key = [link.from, link.to].sort().join('--');
          const existing = this.topologyLinks.get(key);
          if (!existing || existing.updated < (link.updated || Date.now())) {
            this.topologyLinks.set(key, {
              from:    link.from,
              to:      link.to,
              rtt:     link.rtt || existing?.rtt || null,
              updated: link.updated || Date.now(),
            });
            changed = true;
          }
        }
      }
    }

    // Ensure direct link to advertising peer has cost 1
    if (!this.routeTable.has(fromPeerId) || this.routeTable.get(fromPeerId).cost > 1) {
      this.routeTable.set(fromPeerId, { nextHop: fromPeerId, cost: 1, updated: Date.now() });
      changed = true;
    }

    for (const [dest, cost] of Object.entries(advert.vector)) {
      if (dest === this.localNodeId) continue;

      const newCost = cost + 1;
      const current = this.routeTable.get(dest);

      // If new destination or found a shorter path, or update from current next hop
      if (!current || newCost < current.cost || current.nextHop === fromPeerId) {
        if (!current || current.nextHop !== fromPeerId || current.cost !== newCost) {
          this.routeTable.set(dest, {
            nextHop: fromPeerId,
            cost:    newCost,
            updated: Date.now(),
          });
          changed = true;
        }
      }
    }

    if (changed) {
      if (typeof this.onRoutesUpdated === 'function') {
        this.onRoutesUpdated(this.getRoutingTable());
      }
      // Instant DTN flush to newly learned routes
      this.flushAllDTNBundles();
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Instant DTN Data Mule Flushing
  // ─────────────────────────────────────────────────────────────
  _startPeriodicDTNFlush() {
    this._dtnFlushTimer = setInterval(() => {
      if (!this.partitioned && this.peerManager.getActivePeerIds().length > 0) {
        this.flushAllDTNBundles();
      }
    }, 4000);
  }

  /**
   * Flushes all stored DTN bundles to all active peers.
   * Uses routing table to relay multi-hop bundles through appropriate hops.
   */
  async flushAllDTNBundles() {
    if (this._isFlushingDTN || this.partitioned) return 0;
    const activePeers = this.peerManager.getActivePeerIds();
    if (!activePeers.length) return 0;

    this._isFlushingDTN = true;
    let totalFlushed = 0;
    try {

    for (const peerId of activePeers) {
      const isRouteFn = (dest) => {
        const route = this.routeTable.get(dest);
        if (route) {
          return route.nextHop === peerId;
        }
        // DTN Data Mule mode: if no explicit route is mapped yet,
        // relay bundle to any connected peer that hasn't seen it yet
        return true;
      };

      const count = await this.dtnStorage.flushPendingBundles(
        peerId,
        async (bundle) => {
          const fwd = {
            ...bundle,
            visitedNodes: Array.from(new Set([...(bundle.visitedNodes || []), this.localNodeId])),
            hopCount:     (bundle.hopCount || 0) + 1,
          };
          const raw = JSON.stringify(fwd);
          return this.peerManager.send(peerId, raw);
        },
        isRouteFn
      );
      totalFlushed += count;
    }

    if (totalFlushed > 0) {
      console.log(`[ROUTER] DTN Mule Engine: auto-flushed ${totalFlushed} bundles to mesh peers!`);
      if (typeof this.onDTNFlushed === 'function') {
        this.onDTNFlushed(totalFlushed);
      }
    }

      return totalFlushed;
    } finally {
      this._isFlushingDTN = false;
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Vector Clock: Merge & Delta Sync
  // ─────────────────────────────────────────────────────────────
  _mergeVectorClock(remoteClock) {
    let changed = false;
    for (const [nodeId, seq] of Object.entries(remoteClock)) {
      if ((this.vectorClock[nodeId] || 0) < seq) {
        this.vectorClock[nodeId] = seq;
        changed = true;
      }
    }
    if (changed && typeof this.onVectorClockUpdate === 'function') {
      this.onVectorClockUpdate({ ...this.vectorClock });
    }
  }

  _syncWithPeer(peerId) {
    const syncPayload = {
      clock:    this.vectorClock,
      messages: this._getDeltaMessages(peerId),
    };
    this.send(peerId, 'CRDT_SYNC', syncPayload, 300);
  }

  _getDeltaMessages(peerId) {
    const cutoff = Date.now() - 5 * 60 * 1000;
    const delta  = [];
    for (const [, { envelope, receivedAt }] of this.messageLog) {
      if (receivedAt >= cutoff && !envelope.visitedNodes?.includes(peerId)) {
        delta.push(envelope);
      }
    }
    return delta;
  }

  _handleCRDTSync(remotePeerId, syncPayload) {
    let parsed;
    try {
      parsed = typeof syncPayload === 'string' ? JSON.parse(syncPayload) : syncPayload;
    } catch {
      return;
    }

    if (parsed.clock) {
      this._mergeVectorClock(parsed.clock);
    }

    if (Array.isArray(parsed.messages)) {
      for (const envelope of parsed.messages) {
        if (!this.messageLog.has(envelope.bundleId)) {
          console.log(`[ROUTER] Delta sync: replaying ${envelope.bundleId.slice(0, 8)}`);
          this._route(envelope);
        }
      }
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Bind PeerManager callbacks
  // ─────────────────────────────────────────────────────────────
  _bindPeerCallbacks() {
    this.peerManager.onPeerConnected = (nodeId) => {
      console.log(`[ROUTER] Direct peer connected: ${nodeId}`);
      this.routeTable.set(nodeId, { nextHop: nodeId, cost: 1, updated: Date.now() });

      // Immediate broadcast of routing advertisement
      this._broadcastRouteAdvert();

      // Flush any pending DTN bundles
      this.flushAllDTNBundles();

      // Vector clock delta sync
      setTimeout(() => this._syncWithPeer(nodeId), 400);

      if (typeof this.onRoutesUpdated === 'function') {
        this.onRoutesUpdated(this.getRoutingTable());
      }
    };

    this.peerManager.onPeerDropped = (nodeId) => {
      console.log(`[ROUTER] Peer dropped: ${nodeId}`);
      this.routeTable.delete(nodeId);

      // Clean up routes that depended on this dropped peer
      for (const [dest, info] of this.routeTable) {
        if (info.nextHop === nodeId) {
          this.routeTable.delete(dest);
        }
      }

      if (typeof this.onRoutesUpdated === 'function') {
        this.onRoutesUpdated(this.getRoutingTable());
      }
    };

    this.peerManager.onDataReceived = (nodeId, raw) => {
      this._handleIncomingData(nodeId, raw);
    };
  }

  // ─────────────────────────────────────────────────────────────
  // Handle incoming raw data from a peer
  // ─────────────────────────────────────────────────────────────
  _handleIncomingData(fromPeerId, raw) {
    let data;
    try {
      data = JSON.parse(raw);
    } catch {
      return;
    }

    // Handle distance-vector routing advertisement
    if (data._type === 'ROUTE_ADVERT') {
      this._handleRouteAdvert(fromPeerId, data);
      return;
    }

    const envelope = data;
    if (!envelope.bundleId || !envelope.originNode) return;

    // Deduplication
    if (this.messageLog.has(envelope.bundleId)) return;

    // Loop detection
    if (envelope.visitedNodes?.includes(this.localNodeId)) return;

    // CRDT sync
    if (envelope.payloadType === 'CRDT_SYNC') {
      this._handleCRDTSync(fromPeerId, envelope.payload);
      this.messageLog.set(envelope.bundleId, { envelope, receivedAt: Date.now() });
      return;
    }

    console.log(`[ROUTER] Inbound ${envelope.payloadType} from ${envelope.originNode} via ${fromPeerId} [hops=${envelope.hopCount}]`);

    // Route (deliver locally if dest, and forward if multi-hop or broadcast)
    this._route(envelope);
  }

  // ─────────────────────────────────────────────────────────────
  // Partition simulation toggle (Instant DTN Mule trigger)
  // ─────────────────────────────────────────────────────────────
  setPartitioned(state) {
    this.partitioned = state;
    console.log(`[ROUTER] Network partition: ${state ? 'ENABLED (Mule Storage Active)' : 'DISABLED (Flushing to Mesh)'}`);

    // If partition is lifted, immediately auto-flush all buffered DTN bundles!
    if (!state) {
      setTimeout(() => {
        this.flushAllDTNBundles();
      }, 50);
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Public accessors for UI
  // ─────────────────────────────────────────────────────────────
  getRoutingTable() {
    const table = {};
    for (const [dest, info] of this.routeTable) {
      table[dest] = { via: info.nextHop, hops: info.cost };
    }
    return table;
  }

  /**
   * Returns list of all known destinations in the mesh with route info
   * @returns {Array<{ nodeId: string, isDirect: boolean, via: string, hops: number }>}
   */
  getKnownNodes() {
    const activePeers = this.peerManager.getActivePeerIds();
    const result = [];
    const seen = new Set();

    // 1. Direct peers
    for (const p of activePeers) {
      seen.add(p);
      result.push({ nodeId: p, isDirect: true, via: p, hops: 1 });
    }

    // 2. Multi-hop routes
    for (const [dest, info] of this.routeTable) {
      if (dest !== this.localNodeId && !seen.has(dest)) {
        seen.add(dest);
        result.push({
          nodeId:   dest,
          isDirect: false,
          via:      info.nextHop,
          hops:     info.cost,
        });
      }
    }

    // 3. Known peers discovered via gossip
    for (const p of this.peerManager.knownPeers) {
      if (p !== this.localNodeId && !seen.has(p)) {
        seen.add(p);
        result.push({
          nodeId:   p,
          isDirect: false,
          via:      'mesh',
          hops:     2,
        });
      }
    }

    return result;
  }

  /**
   * Returns all known links across the mesh (direct links + relayed links reported by peers)
   * @returns {Array<{ id: string, from: string, to: string, isDirect: boolean, rtt: number|null }>}
   */
  getAllTopologyLinks() {
    const links = [];
    const activePeers = this.peerManager.getActivePeerIds();
    const seen = new Set();

    // 1. Direct links with local node (highest fidelity)
    for (const p of activePeers) {
      const key = [this.localNodeId, p].sort().join('--');
      seen.add(key);
      links.push({
        id:       key,
        from:     this.localNodeId,
        to:       p,
        isDirect: true,
        rtt:      this.peerManager.getRTT(p),
      });
    }

    // 2. Discovered links reported by whole mesh topology gossip
    const cutoff = Date.now() - 30000;
    for (const [key, link] of this.topologyLinks) {
      if (link.updated < cutoff) {
        this.topologyLinks.delete(key);
        continue;
      }
      if (!seen.has(key)) {
        seen.add(key);
        links.push({
          id:       key,
          from:     link.from,
          to:       link.to,
          isDirect: false,
          rtt:      link.rtt || null,
        });
      }
    }

    // 3. Fallback: links from distance-vector routing table
    for (const [dest, info] of this.routeTable) {
      if (dest !== this.localNodeId && info.cost === 2 && info.nextHop) {
        const key = [info.nextHop, dest].sort().join('--');
        if (!seen.has(key)) {
          seen.add(key);
          links.push({
            id:       key,
            from:     info.nextHop,
            to:       dest,
            isDirect: false,
            rtt:      null,
          });
        }
      }
    }

    return links;
  }

  removeNode(nodeId) {
    this.routeTable.delete(nodeId);
    for (const [dest, info] of this.routeTable) {
      if (info.nextHop === nodeId) {
        this.routeTable.delete(dest);
      }
    }
    for (const [key, link] of this.topologyLinks) {
      if (link.from === nodeId || link.to === nodeId) {
        this.topologyLinks.delete(key);
      }
    }
  }

  getVectorClock() {
    return { ...this.vectorClock };
  }

  getMessageCount() {
    return this.messageLog.size;
  }

  destroy() {
    clearInterval(this._advertTimer);
    clearInterval(this._dtnFlushTimer);
  }
}

window.Router = Router;
