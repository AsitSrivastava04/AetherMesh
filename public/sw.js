/**
 * sw.js — Aether Mesh Service Worker
 *
 * Provides:
 *  • Pre-caching of all app shell assets (offline operability after first visit).
 *  • Cache-first strategy for app shell, network-first for signaling.
 *  • Automatic cache versioning and stale-cache cleanup.
 */

'use strict';

const CACHE_NAME = 'aether-mesh-v1.0.7';

const PRECACHE_ASSETS = [
  '/',
  '/index.html',
  '/style.css',
  '/js/dtn-storage.js',
  '/js/peer-manager.js',
  '/js/router.js',
  '/js/app.js',
  // CDN assets must be explicitly cached (fetched on first visit)
  'https://unpkg.com/dexie@3.2.4/dist/dexie.min.js',
  'https://unpkg.com/vis-network@9.1.9/standalone/umd/vis-network.min.js',
];

// ─────────────────────────────────────────────────────────────
// Install: pre-cache all app shell assets
// ─────────────────────────────────────────────────────────────
self.addEventListener('install', (event) => {
  console.log('[SW] Installing Aether Mesh Service Worker…');
  event.waitUntil(
    caches.open(CACHE_NAME).then(async (cache) => {
      // Cache local assets (will throw on network failures during install)
      const localAssets = PRECACHE_ASSETS.filter(url => url.startsWith('/') || url.startsWith(self.location.origin));
      await cache.addAll(localAssets);

      // Cache CDN assets individually (non-fatal if offline during install)
      const cdnAssets = PRECACHE_ASSETS.filter(url => url.startsWith('http'));
      for (const url of cdnAssets) {
        try {
          const response = await fetch(url, { mode: 'cors' });
          if (response.ok) await cache.put(url, response);
        } catch (err) {
          console.warn(`[SW] Could not pre-cache CDN asset: ${url}`, err.message);
        }
      }

      console.log('[SW] Pre-cache complete.');
    }).catch(err => {
      console.error('[SW] Pre-cache failed:', err);
    })
  );
  // Activate immediately without waiting for existing clients to close
  self.skipWaiting();
});

// ─────────────────────────────────────────────────────────────
// Activate: clear old caches
// ─────────────────────────────────────────────────────────────
self.addEventListener('activate', (event) => {
  console.log('[SW] Activating…');
  event.waitUntil(
    caches.keys().then(async (keys) => {
      for (const key of keys) {
        if (key !== CACHE_NAME) {
          console.log(`[SW] Deleting stale cache: ${key}`);
          await caches.delete(key);
        }
      }
    })
  );
  // Take control of all existing clients immediately
  self.clients.claim();
});

// ─────────────────────────────────────────────────────────────
// Fetch: cache-first for app shell, network-first for WS/API
// ─────────────────────────────────────────────────────────────
self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  // Never intercept WebSocket upgrades or cross-origin non-GET requests
  if (event.request.method !== 'GET') return;
  if (url.protocol === 'ws:' || url.protocol === 'wss:') return;

  // Network-first for HTML (ensures fresh content when online)
  if (event.request.mode === 'navigate') {
    event.respondWith(
      fetch(event.request)
        .then(response => {
          const clone = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
          return response;
        })
        .catch(() => caches.match('/index.html'))
    );
    return;
  }

  // Cache-first for all other assets (JS, CSS, CDN libs)
  event.respondWith(
    caches.match(event.request).then(cached => {
      if (cached) return cached;

      return fetch(event.request).then(response => {
        if (!response || response.status !== 200) return response;
        const clone = response.clone();
        caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
        return response;
      }).catch(() => {
        // Offline fallback for HTML requests
        if (event.request.headers.get('accept')?.includes('text/html')) {
          return caches.match('/index.html');
        }
      });
    })
  );
});

// ─────────────────────────────────────────────────────────────
// Message: cache management commands from app.js
// ─────────────────────────────────────────────────────────────
self.addEventListener('message', (event) => {
  if (event.data?.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
  if (event.data?.type === 'CACHE_ASSET') {
    caches.open(CACHE_NAME).then(cache => cache.add(event.data.url));
  }
});
