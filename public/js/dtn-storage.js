/**
 * dtn-storage.js — Sandboxed DTN Storage & Priority Eviction Engine
 *
 * Uses Dexie.js (IndexedDB wrapper) to persist bundles across page reloads
 * and network partitions. Implements deterministic priority eviction so the
 * store never exceeds MAX_STORAGE_THRESHOLD bundles.
 *
 * Priority Formula:  P = remainingTTL / (hopCount + 1)
 * Multi-Hop Ready: Can flush bundles via next-hop relay peers.
 */

'use strict';

const MAX_STORAGE_THRESHOLD = 50;

// ─────────────────────────────────────────────────────────────
// Dexie Database Initialisation
// ─────────────────────────────────────────────────────────────
const db = new Dexie('AetherMesh_DTN');

db.version(1).stores({
  bundles: 'bundleId, destNode, originNode, hopCount, ttl, timestamp, priorityScore',
});

// ─────────────────────────────────────────────────────────────
// Priority Calculation
// ─────────────────────────────────────────────────────────────
function computePriority(timestamp, ttl, hopCount) {
  const nowSeconds   = Date.now() / 1000;
  const remainingTTL = (timestamp + ttl) - nowSeconds;
  if (remainingTTL <= 0) return -Infinity;
  return remainingTTL / (hopCount + 1);
}

// ─────────────────────────────────────────────────────────────
// DTNStorage Class
// ─────────────────────────────────────────────────────────────
class DTNStorage {
  constructor() {
    this.db = db;
  }

  async init() {
    try {
      await this.db.open();
      const purged = await this.purgeExpired();
      console.log(`[DTN] Storage ready. Purged ${purged} expired bundles.`);
    } catch (err) {
      console.error('[DTN] Failed to open IndexedDB:', err);
      throw err;
    }
  }

  async storeBundle(bundle) {
    const priority = computePriority(bundle.timestamp, bundle.ttl, bundle.hopCount);

    if (!isFinite(priority) || priority <= 0) {
      console.warn(`[DTN] Rejecting expired bundle ${bundle.bundleId}`);
      return null;
    }

    const record = { ...bundle, priorityScore: priority };

    try {
      await this.db.transaction('rw', this.db.bundles, async () => {
        const count = await this.db.bundles.count();
        if (count >= MAX_STORAGE_THRESHOLD) {
          await this._evictLowestPriority();
        }
        await this.db.bundles.put(record);
      });

      console.log(`[DTN] Stored bundle ${bundle.bundleId.slice(0, 8)} (P=${priority.toFixed(3)}, dest=${bundle.destNode})`);
      return bundle.bundleId;
    } catch (err) {
      console.error('[DTN] Failed to store bundle:', err);
      return null;
    }
  }

  async _evictLowestPriority() {
    const allBundles = await this.db.bundles.toArray();
    let lowestScore  = Infinity;
    let lowestId     = null;

    for (const b of allBundles) {
      const live = computePriority(b.timestamp, b.ttl, b.hopCount);
      if (live < lowestScore) {
        lowestScore = live;
        lowestId    = b.bundleId;
      }
    }

    if (lowestId) {
      await this.db.bundles.delete(lowestId);
      console.log(`[DTN] Evicted lowest-P bundle ${lowestId}`);
    }
  }

  async purgeExpired() {
    const nowSeconds = Date.now() / 1000;
    const allBundles = await this.db.bundles.toArray();
    const expiredIds = allBundles
      .filter(b => (b.timestamp + b.ttl) - nowSeconds <= 0)
      .map(b => b.bundleId);

    if (expiredIds.length > 0) {
      await this.db.bundles.bulkDelete(expiredIds);
    }
    return expiredIds.length;
  }

  /**
   * Flushes bundles destined for `peerNodeId`, or broadcast bundles,
   * OR multi-hop bundles where `peerNodeId` is the relay hop.
   *
   * @param {string}   peerNodeId  - Direct peer node ID
   * @param {Function} sendFn      - async fn(bundle) → boolean
   * @param {Function} [isRouteFn] - (destNode) → boolean (whether peerNodeId is next hop)
   * @returns {Promise<number>}
   */
  async flushPendingBundles(peerNodeId, sendFn, isRouteFn = null) {
    const nowSeconds = Date.now() / 1000;
    const allBundles = await this.db.bundles.toArray();

    const valid = allBundles
      .filter(b => {
        const isExpired = (b.timestamp + b.ttl) - nowSeconds <= 0;
        if (isExpired) return false;

        const isDirect = b.destNode === peerNodeId;
        const isBroadcast = b.destNode === 'BROADCAST' && !b.visitedNodes?.includes(peerNodeId);
        const isRelay = Boolean(isRouteFn && typeof isRouteFn === 'function' && isRouteFn(b.destNode));

        return isDirect || isBroadcast || isRelay;
      })
      .map(b => ({ ...b, _liveP: computePriority(b.timestamp, b.ttl, b.hopCount) }))
      .sort((a, b) => b._liveP - a._liveP);

    let flushed = 0;
    for (const bundle of valid) {
      try {
        const delivered = await sendFn(bundle);
        if (delivered) {
          if (bundle.destNode !== 'BROADCAST') {
            await this.db.bundles.delete(bundle.bundleId);
          } else {
            bundle.visitedNodes = [...(bundle.visitedNodes || []), peerNodeId];
            delete bundle._liveP;
            await this.db.bundles.put(bundle);
          }
          flushed++;
          console.log(`[DTN] Flushed bundle ${bundle.bundleId.slice(0, 8)} → ${peerNodeId} (dest=${bundle.destNode})`);
        }
      } catch (err) {
        console.warn(`[DTN] Flush failed for ${bundle.bundleId}:`, err);
      }
    }

    return flushed;
  }

  async getStats() {
    await this.purgeExpired();
    const bundles = await this.db.bundles.toArray();
    const nowSec  = Date.now() / 1000;
    const live    = bundles.map(b => ({
      ...b,
      remainingTTL: Math.max(0, (b.timestamp + b.ttl) - nowSec),
      liveScore:    computePriority(b.timestamp, b.ttl, b.hopCount),
    }));
    return {
      count:   live.length,
      max:     MAX_STORAGE_THRESHOLD,
      pct:     ((live.length / MAX_STORAGE_THRESHOLD) * 100).toFixed(1),
      bundles: live,
    };
  }

  async deleteBundle(bundleId) {
    await this.db.bundles.delete(bundleId);
  }

  async clearAll() {
    await this.db.bundles.clear();
    console.log('[DTN] All bundles cleared.');
  }
}

// Singleton export
window.DTNStorage = DTNStorage;
window.dtnStorage = new DTNStorage();
