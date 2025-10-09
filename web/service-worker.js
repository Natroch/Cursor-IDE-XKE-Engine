const CACHE_NAME = 'xke-cache-v3';
const CORE_ASSETS = [
  '/',                 // landing
  '/dashboard',        // dashboard route
  '/web/dashboard.html',
  '/web/accounting.html',
  '/web/app.js',
  '/web/accounting.js',
  '/web/manifest.json'
];

self.addEventListener('install', (e) => {
  e.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(CORE_ASSETS))
  );
  self.skipWaiting();
});

self.addEventListener('activate', (e) => {
  e.waitUntil(
    caches.keys().then((keys) => Promise.all(keys.map((k) => (k === CACHE_NAME ? null : caches.delete(k)))))
  );
  self.clients.claim();
});

self.addEventListener('fetch', (e) => {
  const url = new URL(e.request.url);
  if (url.pathname.startsWith('/api/')) {
    // Network-first for APIs
    e.respondWith(
      fetch(e.request).catch(() => caches.match(e.request))
    );
    return;
  }
  // Always try network first for HTML and JS to avoid stale UI
  const dest = e.request.destination;
  if (dest === 'document' || dest === 'script') {
    e.respondWith(
      fetch(e.request).then((resp) => {
        const copy = resp.clone();
        caches.open(CACHE_NAME).then((c) => c.put(e.request, copy));
        return resp;
      }).catch(() => caches.match(e.request))
    );
    return;
  }
  // Cache-first for static
  e.respondWith(
    caches.match(e.request).then((resp) => resp || fetch(e.request))
  );
});


