/* 高配当株スクリーナー サービスワーカー
   方針：通信できる時は常に最新を取得（ネットワーク優先）。電波がない時だけ前回の保存分で開く。
   アプリを更新したら VERSION を上げると、古い保存分を掃除する。 */
const VERSION = 'v1';
const CACHE = 'kabu-screener-' + VERSION;
const SHELL = ['./', './index.html', './manifest.webmanifest', './icon-192.png', './icon-512.png', './apple-touch-icon.png'];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(SHELL)).then(() => self.skipWaiting()));
});
self.addEventListener('activate', e => {
  e.waitUntil(caches.keys()
    .then(keys => Promise.all(keys.filter(k => k.startsWith('kabu-screener-') && k !== CACHE).map(k => caches.delete(k))))
    .then(() => self.clients.claim()));
});
self.addEventListener('fetch', e => {
  const req = e.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);
  if (url.origin === location.origin) {
    // 自分のファイル（アプリ本体・data.json）：ネットワーク優先、失敗時は保存分
    e.respondWith(fetch(req).then(res => {
      if (res.ok) { const copy = res.clone(); caches.open(CACHE).then(c => c.put(url.pathname.endsWith('/data.json') ? './data.json' : req, copy)); }
      return res;
    }).catch(() => caches.match(url.pathname.endsWith('/data.json') ? './data.json' : req, { ignoreSearch: true })
      .then(r => r || caches.match('./index.html'))));
  } else if (url.hostname.endsWith('fonts.googleapis.com') || url.hostname.endsWith('fonts.gstatic.com')) {
    // フォント：保存分を優先（変わらないため）
    e.respondWith(caches.match(req).then(r => r || fetch(req).then(res => {
      if (res.ok || res.type === 'opaque') { const copy = res.clone(); caches.open(CACHE).then(c => c.put(req, copy)); }
      return res;
    }).catch(() => Response.error())));
  }
});
