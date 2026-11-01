/* MedInter – databáze registrovaných léčivých přípravků (SÚKL/EMA), shrnutí SPC a kontrola lékových interakcí.
   Data nejsou součástí aplikace: dodává je stránka, která MedInter vkládá do iframe (viz „přístup k datům“ níže).
   Aplikace nedostává žádné přihlašovací údaje – jen krátkodobé odkazy na jednotlivé datové soubory.
   Struktura dat: current.json → {"release": "rYYYYMMDD"}; rYYYYMMDD/meta.json.gz, index.json.gz, substances.json.gz,
   atc.json.gz, interactions.json.gz, vpl.json.gz, products-<shard>.json.gz, spc-<shard>.json.gz. */
(function () {
  'use strict';
  const N_PROD_SHARDS = 128, N_SPC_SHARDS = 1024, PAGE = 20;
  const S = {
    meta: null, products: [], byId: Object.create(null), substances: [], subByCode: Object.create(null), subByKey: Object.create(null), atc: {}, ix: null,
    prodShards: {}, spcShards: {},
    list: { q: '', rx: 'all', marketed: true, hideHomeo: true, atc1: '', page: 1 },
    subs: { q: '', page: 1 },
    sel: [], food: { FOOD_GRAPEFRUIT: false, FOOD_ALCOHOL: false, FOOD_DAIRY: false, HERB_STJOHNSWORT: false }
  };
  const $ = (sel, el) => (el || document).querySelector(sel);
  const $$ = (sel, el) => Array.from((el || document).querySelectorAll(sel));
  const view = $('#view');

  // ---------- utils ----------
  function hash31(s) { let h = 0; for (let i = 0; i < s.length; i++) h = (Math.imul(h, 31) + s.charCodeAt(i)) >>> 0; return h; }
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const norm = s => String(s || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  function lc(s) { return s ? s.charAt(0) + s.slice(1).toLowerCase() : s; }
  function subLabel(sub) { return sub.c || lc(sub.k) || sub.e; }
  function pname(p) { return p.dn || p.n; }
  function debounce(fn, ms) { let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); }; }
  const fmtN = n => Number(n || 0).toLocaleString('cs');
  // ---------- přístup k datům ----------
  // Aplikace žádá vkládající stránku o odkazy na soubory, které potřebuje:
  //   → { type: 'medinter:files-request', v: 1, id, paths: ['current.json', 'rYYYYMMDD/index.json.gz', …] }
  //   ← { type: 'medinter:files', id, urls: { <cesta>: <krátkodobý odkaz> }, expiresIn?, theme?, shareUrl?, links?, scope? }
  //     scope: neprůhledná hodnota přihlášeného účtu (8–64 znaků); jiná hodnota = jiný člověk, uložený výběr léků se zapomene
  //   ← { type: 'medinter:files', id, error: 'login' | 'denied' }
  const HOST = { origin: null, shareUrl: null, links: {}, urls: {}, pending: new Map(), seq: 0, queue: [], qTimer: 0 };
  let RELEASE = '';
  class AccessError extends Error { constructor(kind, msg) { super(msg || kind); this.kind = kind; } }
  const isFramed = (() => { try { return window.top !== window.self; } catch (e) { return true; } })();
  function okUrl(u) { try { return new URL(u).protocol === 'https:'; } catch (e) { return false; } }
  function requestUrls(paths) {
    return new Promise((resolve, reject) => {
      if (!isFramed) return reject(new AccessError('standalone'));
      const id = 'r' + (++HOST.seq);
      const timer = setTimeout(() => { if (HOST.pending.delete(id)) reject(new AccessError('network')); }, HOST.origin ? 8000 : 20000);
      HOST.pending.set(id, { resolve, reject, timer });
      // žádost obsahuje jen názvy souborů; po první odpovědi jde jen na doménu vkládající stránky
      try { window.parent.postMessage({ type: 'medinter:files-request', v: 1, id, paths }, HOST.origin || '*'); } catch (e) {}
    });
  }
  // Volitelné události pro vkládající stránku (její vlastní statistiky používání): jen druh zobrazené stránky a u kontroly
  // interakcí počet položek – nikdy názvy léků ani jiný obsah. Odchází jen na doménu, která už odpověděla na žádost o data.
  //   → { type: 'medinter:event', v: 1, name: 'view', page: 'leky' | 'lek' | 'vpl' | 'latky' | 'latka' | 'interakce' | 'o-databazi' }
  //   → { type: 'medinter:event', v: 1, name: 'check', items: <počet> }   (nová kombinace vyhodnocená a zobrazená ≥ 2 s)
  function tellHost(ev) {
    if (!isFramed || !HOST.origin) return;
    try { window.parent.postMessage(Object.assign({ type: 'medinter:event', v: 1 }, ev), HOST.origin); } catch (e) {}
  }
  const MAX_PATHS = 50;   // the embedding page signs at most 50 files per request
  function flushQueue() {
    const q = HOST.queue.splice(0); HOST.qTimer = 0;
    const paths = [...new Set(q.map(x => x.path))];
    for (let i = 0; i < paths.length; i += MAX_PATHS) {
      const part = new Set(paths.slice(i, i + MAX_PATHS)), mine = q.filter(x => part.has(x.path));
      requestUrls([...part]).then(urls => mine.forEach(x => urls[x.path] ? x.resolve(urls[x.path]) : x.reject(new AccessError('denied', x.path))),
        err => mine.forEach(x => x.reject(err)));
    }
  }
  function fileUrl(path, fresh) { // žádosti z jednoho okamžiku se posílají hromadně
    const c = HOST.urls[path];
    if (!fresh && c && c.exp - Date.now() > 10000) return Promise.resolve(c.url);
    return new Promise((resolve, reject) => { HOST.queue.push({ path, resolve, reject }); if (!HOST.qTimer) HOST.qTimer = setTimeout(flushQueue, 0); });
  }
  window.addEventListener('message', e => {
    if (!isFramed || e.source !== window.parent) return;   // jen od stránky, která aplikaci vložila
    if (HOST.origin && HOST.origin !== e.origin) return;   // po první odpovědi jen od téže domény
    const d = e.data || {};
    if (d.type === 'medinter:files' && typeof d.id === 'string' && HOST.pending.has(d.id)) {
      const pnd = HOST.pending.get(d.id); HOST.pending.delete(d.id); clearTimeout(pnd.timer);
      HOST.origin = e.origin;
      if (typeof d.scope === 'string' && /^[A-Za-z0-9_-]{8,64}$/.test(d.scope) && load('scope', null) !== d.scope) {
        // another account in this browser: its selection of medicines is not shown to the next one
        try { localStorage.removeItem('medinter-sel'); localStorage.removeItem('medinter-food'); } catch (err) {}
        S.sel = []; Object.keys(S.food).forEach(k => { S.food[k] = false; });
        store('scope', d.scope);
      }
      if (typeof d.shareUrl === 'string' && d.shareUrl.startsWith(e.origin + '/')) HOST.shareUrl = d.shareUrl;
      if (d.links) { const L = d.links; HOST.links = { login: okUrl(L.login) ? L.login : '', info: okUrl(L.info) ? L.info : '' }; }
      if (d.theme === 'light' || d.theme === 'dark') applyTheme(d.theme, true);
      if (d.error) return pnd.reject(new AccessError(d.error === 'login' ? 'login' : d.error === 'network' ? 'network' : 'denied'));
      const ttl = Math.max(20, Math.min(86400, Number(d.expiresIn) || 60)) * 1000, out = {};
      Object.entries(d.urls || {}).forEach(([p, u]) => { if (typeof u === 'string' && okUrl(u)) { HOST.urls[p] = { url: u, exp: Date.now() + ttl }; out[p] = u; } });
      pnd.resolve(out);
    } else if (d.type === 'medinter:theme' && (d.theme === 'light' || d.theme === 'dark')) applyTheme(d.theme, true);
    else if (d.type === 'medinter:focus-search') { const g = $('#gq'); if (g && !document.documentElement.classList.contains('locked')) { g.focus(); g.select(); } }
  });
  async function fetchFile(path, opts) {
    for (let attempt = 0; attempt < 2; attempt++) {
      const url = await fileUrl(path, attempt > 0);
      let r;
      try { r = await fetch(url, Object.assign({ credentials: 'omit', referrerPolicy: 'no-referrer' }, opts || {})); }
      catch (e) { throw new AccessError('network', e.message); }
      if (r.ok) return r;
      delete HOST.urls[path];
      if (attempt === 0 && r.status < 500) continue; // nejčastěji vypršelý odkaz → vyžádat nový a zkusit znovu
      throw new AccessError(r.status < 500 ? 'denied' : 'network', path + ' ' + r.status);
    }
  }
  async function readJSON(r) {
    const buf = new Uint8Array(await r.arrayBuffer());
    if (buf[0] === 0x1f && buf[1] === 0x8b) { // gzip (datové soubory jsou komprimované)
      const ds = new Blob([buf]).stream().pipeThrough(new DecompressionStream('gzip'));
      return JSON.parse(await new Response(ds).text());
    }
    return JSON.parse(new TextDecoder().decode(buf));
  }
  async function getJSON(path, opts) {
    const r = await fetchFile(RELEASE + '/' + path.replace(/\//g, '-') + '.gz', opts);
    try { return await readJSON(r); } catch (e) { throw new Error(path + ': ' + e.message); }
  }
  async function prodShard(id) { const k = hash31(id) % N_PROD_SHARDS; if (!S.prodShards[k]) S.prodShards[k] = getJSON('products/' + k + '.json').catch(e => { delete S.prodShards[k]; throw e; }); return (await S.prodShards[k])[id]; }
  async function spcDoc(spcId) { if (!spcId) return null; const k = hash31(spcId) % N_SPC_SHARDS; if (!S.spcShards[k]) S.spcShards[k] = getJSON('spc/' + k + '.json').catch(e => { delete S.spcShards[k]; throw e; }); return (await S.spcShards[k])[spcId] || null; }
  function store(key, val) { try { localStorage.setItem('medinter-' + key, JSON.stringify(val)); } catch (e) {} }
  function load(key, def) { try { const v = localStorage.getItem('medinter-' + key); return v ? JSON.parse(v) : def; } catch (e) { return def; } }
  function spcUrl(kod, type) { return 'https://prehledy.sukl.cz/dlp/v1/dokumenty/' + encodeURIComponent(kod) + '/' + (type === 'pil' ? 'pil' : 'spc'); }
  function suklProductUrl(kod) { return 'https://prehledy.sukl.cz/prehled_leciv.html#/leciva/' + encodeURIComponent(kod); }
  function dec(x) { try { return decodeURIComponent(x); } catch (e) { return ''; } }
  function re(p) { try { return new RegExp(p, 'i'); } catch (e) { return null; } }   // a pattern from the data that is not valid is skipped
  const VYDEJ = { R: 'na lékařský předpis', F: 'volně prodejný', V: 'vyhrazené léčivo', O: 'bez předpisu s omezením', P: 'bez předpisu s omezením', L: 'na předpis s omezením', C: 'na předpis s omezením' };
  const SEV = { X: 'Kontraindikace', D: 'Závažná', C: 'Významná', B: 'Mírná', I: 'Informace' };
  const SEV_ORD = { X: 0, D: 1, C: 2, B: 3, I: 4 };

  // ---------- icons (line icons) ----------
  const IP = {
    search: '<circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/>',
    moon: '<path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z"/>',
    sun: '<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/>',
    pill: '<path d="m10.5 20.5 10-10a4.95 4.95 0 1 0-7-7l-10 10a4.95 4.95 0 1 0 7 7Z"/><path d="m8.5 8.5 7 7"/>',
    steth: '<path d="M11 2v2M5 2v2M5 3H4a2 2 0 0 0-2 2v4a6 6 0 0 0 12 0V5a2 2 0 0 0-2-2h-1"/><path d="M8 15a6 6 0 0 0 12 0v-3"/><circle cx="20" cy="10" r="2"/>',
    heart: '<path d="M19 14c1.49-1.46 3-3.21 3-5.5A5.5 5.5 0 0 0 16.5 3c-1.76 0-3 .5-4.5 2-1.5-1.5-2.74-2-4.5-2A5.5 5.5 0 0 0 2 8.5c0 2.3 1.5 4.05 3 5.5l7 7Z"/><path d="M3.22 12H9.5l.5-1 2 4.5 2-7 1.5 3.5h5.27"/>',
    droplet: '<path d="M12 22a7 7 0 0 0 7-7c0-2-1-3.9-3-5.5s-3.5-4-4-6.5c-.5 2.5-2 4.9-4 6.5C6 11.1 5 13 5 15a7 7 0 0 0 7 7z"/>',
    syringe: '<path d="m18 2 4 4"/><path d="m17 7 3-3"/><path d="M19 9 8.7 19.3c-1 1-2.5 1-3.4 0l-.6-.6c-1-1-1-2.5 0-3.4L15 5"/><path d="m9 11 4 4"/><path d="m5 19-3 3"/><path d="m14 4 6 6"/>',
    dna: '<path d="m10 16 1.5 1.5"/><path d="m14 8-1.5-1.5"/><path d="M15 2c-1.798 1.998-2.518 3.995-2.807 5.993"/><path d="m16.5 10.5 1 1"/><path d="m17 6-2.891-2.891"/><path d="M2 15c6.667-6 13.333 0 20-6"/><path d="m20 9 .891.891"/><path d="M3.109 14.109 4 15"/><path d="m6.5 12.5 1 1"/><path d="m7 18 2.891 2.891"/><path d="M9 22c1.798-1.998 2.518-3.995 2.807-5.993"/>',
    droplets: '<path d="M7 16.3c2.2 0 4-1.83 4-4.05 0-1.16-.57-2.26-1.71-3.19S7.29 6.75 7 5.3c-.29 1.45-1.14 2.84-2.29 3.76S3 11.1 3 12.25c0 2.22 1.8 4.05 4 4.05z"/><path d="M12.56 6.6A10.97 10.97 0 0 0 14 3.02c.5 2.5 2 4.9 4 6.5s3 3.5 3 5.5a6.98 6.98 0 0 1-11.91 4.97"/>',
    utensils: '<path d="M3 2v7c0 1.1.9 2 2 2h4a2 2 0 0 0 2-2V2"/><path d="M7 2v20"/><path d="M21 15V2a5 5 0 0 0-5 5v6c0 1.1.9 2 2 2h3Zm0 0v7"/>',
    citrus: '<path d="M21.66 17.67a1.08 1.08 0 0 1-.04 1.6A12 12 0 0 1 4.73 2.38a1.1 1.1 0 0 1 1.61-.04z"/><path d="M19.65 15.66A8 8 0 0 1 8.35 4.34"/><path d="m14 10-5.5 5.5"/><path d="M14 17.85V10H6.15"/>',
    bug: '<path d="m8 2 1.88 1.88"/><path d="M14.12 3.88 16 2"/><path d="M9 7.13v-1a3.003 3.003 0 1 1 6 0v1"/><path d="M12 20c-3.3 0-6-2.7-6-6v-3a4 4 0 0 1 4-4h4a4 4 0 0 1 4 4v3c0 3.3-2.7 6-6 6"/><path d="M12 20v-9"/><path d="M6.53 9C4.6 8.8 3 7.1 3 5"/><path d="M6 13H2"/><path d="M3 21c0-2.1 1.7-3.9 3.8-4"/><path d="M20.97 5c0 2.1-1.6 3.8-3.5 4"/><path d="M22 13h-4"/><path d="M17.2 17c2.1.1 3.8 1.9 3.8 4"/>',
    wind: '<path d="M17.7 7.7a2.5 2.5 0 1 1 1.8 4.3H2"/><path d="M9.6 4.6A2 2 0 1 1 11 8H2"/><path d="M12.6 19.4A2 2 0 1 0 14 16H2"/>',
    flower: '<path d="M12 5a3 3 0 1 1 3 3m-3-3a3 3 0 1 0-3 3m3-3v1M9 8a3 3 0 1 0 3 3M9 8h1m5 0a3 3 0 1 1-3 3m3-3h-1m-2 3v-1"/><circle cx="12" cy="8" r="2"/><path d="M12 10v12"/><path d="M12 22c4.2 0 7-1.667 7-5-4.2 0-7 1.667-7 5Z"/><path d="M12 22c-4.2 0-7-1.667-7-5 4.2 0 7 1.667 7 5Z"/>',
    bone: '<path d="M17 10c.7-.7 1.69 0 2.5 0a2.5 2.5 0 1 0 0-5 .5.5 0 0 1-.5-.5 2.5 2.5 0 1 0-5 0c0 .81.7 1.8 0 2.5l-7 7c-.7.7-1.69 0-2.5 0a2.5 2.5 0 0 0 0 5c.28 0 .5.22.5.5a2.5 2.5 0 1 0 5 0c0-.81-.7-1.8 0-2.5Z"/>',
    brain: '<path d="M12 5a3 3 0 1 0-5.997.125 4 4 0 0 0-2.526 5.77 4 4 0 0 0 .556 6.588A4 4 0 1 0 12 18Z"/><path d="M12 5a3 3 0 1 1 5.997.125 4 4 0 0 1 2.526 5.77 4 4 0 0 1-.556 6.588A4 4 0 1 1 12 18Z"/><path d="M15 13a4.5 4.5 0 0 1-3-4 4.5 4.5 0 0 1-3 4"/>',
    smile: '<circle cx="12" cy="12" r="10"/><path d="M8 14s1.5 2 4 2 4-2 4-2"/><path d="M9 9h.01"/><path d="M15 9h.01"/>',
    mars: '<path d="M16 3h5v5"/><path d="m21 3-6.75 6.75"/><circle cx="10" cy="14" r="6"/>',
    flask: '<path d="M10 2v7.53a2 2 0 0 1-.21.9L4.72 20.55a1 1 0 0 0 .9 1.45h12.76a1 1 0 0 0 .9-1.45l-5.07-10.12a2 2 0 0 1-.21-.9V2"/><path d="M8.5 2h7"/><path d="M7 16h10"/>',
    zap: '<path d="M4 14a1 1 0 0 1-.78-1.63l9.9-10.2a.5.5 0 0 1 .86.46l-1.92 6.02A1 1 0 0 0 13 10h7a1 1 0 0 1 .78 1.63l-9.9 10.2a.5.5 0 0 1-.86-.46l1.92-6.02A1 1 0 0 0 11 14z"/>',
    info: '<circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/>',
    check: '<circle cx="12" cy="12" r="10"/><path d="m9 12 2 2 4-4"/>',
    clock: '<circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/>',
    ban: '<circle cx="12" cy="12" r="10"/><path d="m4.9 4.9 14.2 14.2"/>',
    alert: '<path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"/><path d="M12 9v4"/><path d="M12 17h.01"/>',
    baby: '<path d="M9 12h.01"/><path d="M15 12h.01"/><path d="M10 16c.5.3 1.2.5 2 .5s1.5-.2 2-.5"/><path d="M19 6.3a9 9 0 0 1 1.8 3.9 2 2 0 0 1 0 3.6 9 9 0 0 1-17.6 0 2 2 0 0 1 0-3.6A9 9 0 0 1 12 3c2 0 3.5 1.1 3.5 2.5s-.9 2.5-2 2.5c-.8 0-1.5-.4-1.5-1"/>',
    car: '<path d="M19 17h2c.6 0 1-.4 1-1v-3c0-.9-.7-1.7-1.5-1.9C18.7 10.6 16 10 16 10s-1.3-1.4-2.2-2.3c-.5-.4-1.1-.7-1.8-.7H5c-.6 0-1.1.4-1.4.9l-1.4 2.9A3.7 3.7 0 0 0 2 12v4c0 .6.4 1 1 1h2"/><circle cx="7" cy="17" r="2"/><path d="M9 17h6"/><circle cx="17" cy="17" r="2"/>',
    activity: '<path d="M22 12h-2.48a2 2 0 0 0-1.93 1.46l-2.35 8.36a.25.25 0 0 1-.48 0L9.24 2.18a.25.25 0 0 0-.48 0l-2.35 8.36A2 2 0 0 1 4.49 12H2"/>',
    droplet: '<path d="M12 22a7 7 0 0 0 7-7c0-2-1-3.9-3-5.5s-3.5-4-4-6.5c-.5 2.5-2 4.9-4 6.5C6 11.1 5 13 5 15a7 7 0 0 0 7 7z"/>',
    ext: '<path d="M15 3h6v6"/><path d="M10 14 21 3"/><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/>',
    plus: '<path d="M5 12h14"/><path d="M12 5v14"/>',
    x: '<path d="M18 6 6 18"/><path d="m6 6 12 12"/>',
    down: '<path d="m6 9 6 6 6-6"/>', right: '<path d="m9 18 6-6-6-6"/>', left: '<path d="m15 18-6-6 6-6"/>',
    file: '<path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7Z"/><path d="M14 2v4a2 2 0 0 0 2 2h4"/><path d="M10 9H8"/><path d="M16 13H8"/><path d="M16 17H8"/>',
    layers: '<path d="M12.83 2.18a2 2 0 0 0-1.66 0L2.6 6.08a1 1 0 0 0 0 1.83l8.58 3.91a2 2 0 0 0 1.66 0l8.58-3.9a1 1 0 0 0 0-1.83Z"/><path d="m22 17.65-9.17 4.16a2 2 0 0 1-1.66 0L2 17.65"/><path d="m22 12.65-9.17 4.16a2 2 0 0 1-1.66 0L2 12.65"/>',
    db: '<ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14a9 3 0 0 0 18 0V5"/><path d="M3 12a9 3 0 0 0 18 0"/>',
    sparkles: '<path d="M9.94 15.5A2 2 0 0 0 8.5 14.06l-6.14-1.58a.5.5 0 0 1 0-.96L8.5 9.94A2 2 0 0 0 9.94 8.5l1.58-6.14a.5.5 0 0 1 .96 0l1.58 6.14a2 2 0 0 0 1.44 1.44l6.14 1.58a.5.5 0 0 1 0 .96l-6.14 1.58a2 2 0 0 0-1.44 1.44l-1.58 6.14a.5.5 0 0 1-.96 0z"/>',
    building: '<rect width="16" height="20" x="4" y="2" rx="2"/><path d="M9 22v-4h6v4"/><path d="M8 6h.01M16 6h.01M12 6h.01M12 10h.01M12 14h.01M16 10h.01M16 14h.01M8 10h.01M8 14h.01"/>',
    box: '<path d="m7.5 4.27 9 5.15"/><path d="M21 8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16Z"/><path d="m3.3 7 8.7 5 8.7-5"/><path d="M12 22V12"/>',
    receipt: '<path d="M4 2v20l2-1 2 1 2-1 2 1 2-1 2 1 2-1 2 1V2l-2 1-2-1-2 1-2-1-2 1-2-1-2 1Z"/><path d="M8 7h8M8 11h8M8 15h5"/>',
    scale: '<path d="m16 16 3-8 3 8c-.87.65-1.92 1-3 1s-2.13-.35-3-1Z"/><path d="m2 16 3-8 3 8c-.87.65-1.92 1-3 1s-2.13-.35-3-1Z"/><path d="M7 21h10"/><path d="M12 3v18"/><path d="M3 7h2c2 0 5-1 7-2 2 1 5 2 7 2h2"/>',
    globe: '<circle cx="12" cy="12" r="10"/><path d="M12 2a14.5 14.5 0 0 0 0 20 14.5 14.5 0 0 0 0-20"/><path d="M2 12h20"/>',
    truck: '<path d="M14 18V6a2 2 0 0 0-2-2H4a2 2 0 0 0-2 2v11a1 1 0 0 0 1 1h2"/><path d="M15 18H9"/><path d="M19 18h2a1 1 0 0 0 1-1v-3.65a1 1 0 0 0-.22-.62l-3.48-4.35A1 1 0 0 0 17.52 8H14"/><circle cx="17" cy="18" r="2"/><circle cx="7" cy="18" r="2"/>',
    wine: '<path d="M8 22h8"/><path d="M7 10h10"/><path d="M12 15v7"/><path d="M12 15a5 5 0 0 0 5-5c0-2-.5-4-2-8H9c-1.5 4-2 6-2 8a5 5 0 0 0 5 5Z"/>',
    leaf: '<path d="M11 20A7 7 0 0 1 9.8 6.1C15.5 5 17 4.48 19 2c1 2 2 4.18 2 8 0 5.5-4.78 10-10 10Z"/><path d="M2 21c0-3 1.85-5.36 5.08-6C9.5 14.52 12 13 13 12"/>',
    milk: '<path d="M8 2h8"/><path d="M9 2v2.79a4 4 0 0 1-.67 2.22l-.66.98A4 4 0 0 0 7 10.21V20a2 2 0 0 0 2 2h6a2 2 0 0 0 2-2v-9.79a4 4 0 0 0-.67-2.22l-.66-.98A4 4 0 0 1 15 4.79V2"/><path d="M7 15a6.47 6.47 0 0 1 5 0 6.47 6.47 0 0 0 5 0"/>',
    citrus: '<path d="M21.66 17.67a1.08 1.08 0 0 1-.04 1.6A12 12 0 0 1 4.73 2.38a1.1 1.1 0 0 1 1.61-.04z"/><path d="M19.65 15.66A8 8 0 0 1 8.35 4.34"/><path d="m14 10-5.5 5.5"/><path d="M14 17.85V10H6.15"/>',
    link: '<path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/>',
    trash: '<path d="M3 6h18"/><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"/><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"/>',
    expand: '<path d="m7 15 5 5 5-5"/><path d="m7 9 5-5 5 5"/>',
    shield: '<path d="M20 13c0 5-3.5 7.5-7.66 8.95a1 1 0 0 1-.67-.01C7.5 20.5 4 18 4 13V6a1 1 0 0 1 1-1c2 0 4.5-1.2 6.24-2.72a1.17 1.17 0 0 1 1.52 0C14.51 3.81 17 5 19 5a1 1 0 0 1 1 1z"/>',
    lock: '<rect width="18" height="11" x="3" y="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/>'
  };
  const ic = (n, cls) => `<svg class="i ${cls || ''}" viewBox="0 0 24 24" aria-hidden="true">${Object.prototype.hasOwnProperty.call(IP, n) ? IP[n] : ''}</svg>`;

  // ---------- theme ----------
  function currentTheme() { const t = document.documentElement.getAttribute('data-theme'); if (t) return t; return window.matchMedia && matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'; }
  function paintThemeBtn() { $('#themeBtn').innerHTML = ic(currentTheme() === 'dark' ? 'sun' : 'moon'); }
  const topbar = $('.topbar');
  window.addEventListener('scroll', () => { topbar.classList.toggle('scrolled', window.scrollY > 4); }, { passive: true });
  $('#themeBtn').addEventListener('click', () => { const t = currentTheme() === 'dark' ? 'light' : 'dark'; document.documentElement.setAttribute('data-theme', t); try { localStorage.setItem('medinter-theme', t); } catch (e) {} paintThemeBtn(); });
  // motiv předaný vkládající stránkou: aplikace ho přebírá a vlastní přepínač skryje
  function applyTheme(t, fromParent) { document.documentElement.setAttribute('data-theme', t); if (fromParent) document.documentElement.classList.add('theme-follow'); paintThemeBtn(); }
  paintThemeBtn();

  // ---------- text → HTML (full SPC section) ----------
  const SUBHEAD = /^(dávkování|pediatrická populace|starší (pacienti|osoby)|porucha funkce (ledvin|jater)|způsob podání|dospělí|děti( a dospívající)?|zvláštní (skupiny|populace)|těhotenství|kojení|fertilita|farmakokinetické interakce|farmakodynamické interakce|souhrn bezpečnostního profilu|tabulkový (seznam|přehled)|hlášení podezření|popis vybraných|příznaky|léčba|antidot|obecn[éá]|účinky .* na |vliv .* na |další informace|zvláštní upozornění|opatření)/i;
  function textToHtml(t) {
    if (!t) return '<p class="nothing">V SPC neuvedeno nebo se text nepodařilo načíst.</p>';
    const lines = t.split('\n');
    let html = '', inList = false;
    for (let i = 0; i < lines.length; i++) {
      const ln = lines[i].trim();
      if (!ln) continue;
      const m = ln.match(/^([-–•·▪●*]|o)\s+(.*)$/);
      if (m) { if (!inList) { html += '<ul>'; inList = true; } html += '<li>' + esc(m[2]) + '</li>'; continue; }
      if (inList) { html += '</ul>'; inList = false; }
      const next = lines[i + 1] || '';
      const isHead = ln.length <= 70 && !/[.;,)]$/.test(ln) && /^[A-ZÁ-Ž]/.test(ln) && next.length > ln.length && (SUBHEAD.test(ln) || ln.split(' ').length <= 5);
      html += isHead ? '<h4>' + esc(ln.replace(/:$/, '')) + '</h4>' : '<p>' + esc(ln) + '</p>';
    }
    if (inList) html += '</ul>';
    return html;
  }

  // ---------- page header + tabs ----------
  // [route, label, icon, short label for phones]
  const TABS = [['leky', 'Přehled léků', 'pill', 'Léky'], ['vpl', 'Nejčastější ve VPL', 'steth', 'VPL'], ['interakce', 'Kontrola interakcí', 'zap', 'Interakce'], ['latky', 'Léčivé látky', 'flask', 'Látky'], ['o-databazi', 'O databázi', 'db', 'Info']];
  function paintTabs(active) {
    const bar = $('#mainTabs'); if (!bar) return;   // an older cached index.html has no tab bar: the pages still work
    const n = S.sel.length + Object.values(S.food).filter(Boolean).length;
    // The label is in its own span so narrow screens can swap it for the short one (CSS, data-s); aria-label keeps the full name.
    bar.innerHTML = TABS.map(([r, l, i, s]) => {
      const on = active === r, k = r === 'interakce' && n ? n : 0;
      return `<a class="tab ${on ? 'active' : ''}" href="#/${r}"${on ? ' aria-current="page"' : ''} data-s="${esc(s)}" aria-label="${esc(k ? l + ' (' + k + ')' : l)}">${ic(i, 'sm')}<span class="tl">${esc(l)}</span>${k ? ` <span class="n">${k}</span>` : ''}</a>`;
    }).join('');
  }
  function header(active, title, sub) {
    paintTabs(active);
    return `<div class="head"><div><h1>${esc(title)}</h1>${sub ? `<p>${sub}</p>` : ''}</div></div>`;
  }
  function loading(msg) { return `<div class="loading"><span class="spin"></span>${esc(msg || 'Načítám…')}</div>`; }

  // ---------- router ----------
  window.addEventListener('hashchange', route);
  let NAV = 0;   // roste s každou navigací: stránka, která se dočte pozdě, se už nevykreslí přes jinou
  function route() {
    if (!S.ready) return; // před načtením dat (nebo na zamčené obrazovce) nic nevykreslovat
    NAV++; document.documentElement.classList.remove('locked');
    setTimeout(() => { const t = $('.tabs'), a = t && $('.tab.active', t); if (!a || t.scrollWidth <= t.clientWidth) return; const r = a.getBoundingClientRect(), q = t.getBoundingClientRect(); if (r.left < q.left) t.scrollLeft += r.left - q.left - 8; else if (r.right > q.right) t.scrollLeft += r.right - q.right + 8; }, 0);
    closeDD();
    const h = location.hash.replace(/^#\/?/, '');
    const [path, qs] = h.split('?');
    const parts = path.split('/').filter(Boolean);
    const p0 = parts[0] || 'leky';
    paintTabs({ lek: 'leky', latka: 'latky' }[p0] || p0);
    window.scrollTo(0, 0);
    tellHost({ name: 'view', page: ['lek', 'vpl', 'latky', 'latka', 'interakce', 'o-databazi'].includes(p0) ? p0 : 'leky' });
    if (p0 === 'leky') { if (qs) { const m = new URLSearchParams(qs); if (m.has('q')) { S.list.q = m.get('q'); S.list.page = 1; $('#gq').value = S.list.q; } } return renderList(); }
    if (p0 === 'lek') return renderDrug(dec(parts[1] || ''));
    if (p0 === 'vpl') return renderVpl();
    if (p0 === 'latky') return renderSubstances();
    if (p0 === 'latka') return renderSubstance(dec(parts[1] || ''));
    if (p0 === 'interakce') { if (qs) parseSelection(qs); return renderInteractions(); }
    if (p0 === 'o-databazi') return renderAbout();
    renderList();
  }

  // ---------- search ----------
  function matchesQuery(p, nq, tokens) {
    if (!nq) return 1;
    const name = norm(p.n);
    if (name.startsWith(nq)) return 100;
    if (name.includes(nq)) return 80;
    let score = 0;
    for (const s of p.s) { const c = norm(s.c), k = norm(s.k), e = norm(s.e); if (c.startsWith(nq) || k.startsWith(nq) || e.startsWith(nq)) score = Math.max(score, 70); else if (c.includes(nq) || k.includes(nq) || e.includes(nq)) score = Math.max(score, 50); }
    if (score) return score;
    const atc = p.a.join(' ').toLowerCase(); if (nq.length >= 3 && atc.includes(nq)) return 40;
    const an = norm(p.an.join(' ')); if (nq.length >= 4 && an.includes(nq)) return 30;
    if (tokens.length > 1) { const hay = name + ' ' + p.s.map(s => norm(s.c) + ' ' + norm(s.k)).join(' ') + ' ' + an; if (tokens.every(t => hay.includes(t))) return 20; }
    return 0;
  }
  function filterProducts() {
    const L = S.list, nq = norm(L.q.trim()), tokens = nq.split(/\s+/).filter(Boolean);
    const out = [];
    for (const p of S.products) {
      if (L.marketed && !p.m) continue;
      if (L.hideHomeo && p.t.includes('HO')) continue;
      if (L.rx === 'R' && !p.v.some(v => 'RLC'.includes(v))) continue;
      if (L.rx === 'F' && !p.v.some(v => 'FVOP'.includes(v))) continue;
      if (L.atc1 && !p.a.some(a => a.startsWith(L.atc1))) continue;
      const sc = matchesQuery(p, nq, tokens); if (!sc) continue;
      out.push([sc, p]);
    }
    out.sort((a, b) => b[0] - a[0] || (b[1].m - a[1].m) || pname(a[1]).localeCompare(pname(b[1]), 'cs'));
    return out.map(x => x[1]);
  }
  function suggest(q, opts) {
    const nq = norm(q.trim()); if (nq.length < 2) return { products: [], substances: [] };
    const subs = [];
    for (const s of S.substances) { if (!s.n) continue; const c = norm(s.cs), k = norm(s.inn), e = norm(s.en); const sc = c.startsWith(nq) || k.startsWith(nq) || e.startsWith(nq) ? 90 : (c.includes(nq) || e.includes(nq) ? 60 : 0); if (sc) subs.push([sc + Math.min(s.n, 30) / 10, s]); }
    const prods = [];
    for (const p of S.products) { if (!p.m && (!opts || !opts.all)) continue; if (p.t.includes('HO')) continue; const sc = matchesQuery(p, nq, []); if (sc >= 50) prods.push([sc, p]); }
    subs.sort((a, b) => b[0] - a[0]); prods.sort((a, b) => b[0] - a[0] || pname(a[1]).localeCompare(pname(b[1]), 'cs'));
    return { products: prods.slice(0, (opts && opts.np) || 6).map(x => x[1]), substances: subs.slice(0, (opts && opts.ns) || 3).map(x => x[1]) };
  }
  function prodSub(p) { return [p.s.map(subLabel).join(' + ') || 'bez léčivé látky', p.a[0], p.f.slice(0, 2).join(', ')].filter(Boolean).join(' · '); }

  // global search with dropdown
  const gq = $('#gq'), gdd = $('#gdd');
  let ddItems = [], ddAct = -1;
  function closeDD() { gdd.classList.remove('open'); ddItems = []; ddAct = -1; }
  function drawDD() {
    const q = gq.value; const r = suggest(q);
    ddItems = [];
    let html = '';
    if (r.products.length) { html += '<div class="dd-sec">Léčivé přípravky</div>'; r.products.forEach(p => { ddItems.push('#/lek/' + encodeURIComponent(p.i)); html += `<div class="dd-item" data-k="${ddItems.length - 1}"><span class="sq sm">${ic('pill', 'sm')}</span><div><div class="t">${esc(pname(p))}</div><div class="s">${esc(prodSub(p))}</div></div></div>`; }); }
    if (r.substances.length) { html += '<div class="dd-sec">Léčivé látky</div>'; r.substances.forEach(s => { ddItems.push('#/latka/' + encodeURIComponent(s.code)); html += `<div class="dd-item" data-k="${ddItems.length - 1}"><span class="sq sm">${ic('flask', 'sm')}</span><div><div class="t">${esc(s.cs || lc(s.inn))}</div><div class="s">${esc(s.inn)} · ${Number(s.n) || 0} přípravků</div></div></div>`; }); }
    if (!html) { closeDD(); return; }
    html += `<div class="dd-foot">Enter = zobrazit všechny výsledky v seznamu</div>`;
    gdd.innerHTML = html; gdd.classList.add('open'); ddAct = -1;
    $$('.dd-item', gdd).forEach(el => el.addEventListener('mousedown', e => { e.preventDefault(); location.hash = ddItems[+el.dataset.k]; gq.blur(); }));
  }
  gq.addEventListener('input', debounce(() => {
    if (location.hash.startsWith('#/leky') || location.hash === '' || location.hash === '#/') { S.list.q = gq.value; S.list.page = 1; if ($('#plist')) drawList(); }
    drawDD();
  }, 110));
  gq.addEventListener('keydown', e => {
    const items = $$('.dd-item', gdd);
    if (e.key === 'ArrowDown' && items.length) { ddAct = Math.min(ddAct + 1, items.length - 1); items.forEach((x, i) => x.classList.toggle('active', i === ddAct)); e.preventDefault(); }
    else if (e.key === 'ArrowUp' && items.length) { ddAct = Math.max(ddAct - 1, 0); items.forEach((x, i) => x.classList.toggle('active', i === ddAct)); e.preventDefault(); }
    else if (e.key === 'Enter') { e.preventDefault(); if (ddAct >= 0 && ddItems[ddAct]) location.hash = ddItems[ddAct]; else { S.list.q = gq.value; S.list.page = 1; if (location.hash.startsWith('#/leky')) { closeDD(); drawList(); } else location.hash = '#/leky'; } gq.blur(); }
    else if (e.key === 'Escape') { closeDD(); gq.blur(); }
  });
  gq.addEventListener('focus', () => { if (gq.value.trim().length >= 2) drawDD(); });
  gq.addEventListener('blur', () => setTimeout(closeDD, 150));
  document.addEventListener('keydown', e => {
    const tag = (document.activeElement && document.activeElement.tagName) || '';
    if ((e.key === 'k' && (e.ctrlKey || e.metaKey)) || (e.key === '/' && !/INPUT|TEXTAREA|SELECT/.test(tag))) { e.preventDefault(); gq.focus(); gq.select(); }
  });

  // ---------- LIST ----------
  function badges(p, compact) {
    let b = '';
    if (p.v.some(v => 'RLC'.includes(v))) b += '<span class="badge blue keep">Rp</span>';
    if (p.v.some(v => 'FVOP'.includes(v))) b += '<span class="badge mint keep">Volně</span>';
    if (p.eu) b += '<span class="badge violet" title="Centralizovaná registrace EU (EMA)">EU</span>';
    if (p.t.includes('HO')) b += '<span class="badge amber">Homeopatikum</span>';
    if (!p.m) b += '<span class="badge gray" title="Registrován, ale v posledních 6 měsících nebyl dodáván do lékáren">Nedodáván</span>';
    else if (!compact) b += '<span class="badge mint" title="Dodáván do lékáren v posledních 6 měsících">Dodáván</span>';
    return b;
  }
  function prodRow(p) {
    const st = p.st.slice(0, 3).join(' · ') + (p.st.length > 3 ? ' …' : '');
    return `<a class="row" href="#/lek/${encodeURIComponent(p.i)}"><span class="sq sm">${ic('pill', 'sm')}</span>
      <div class="main"><div class="t">${esc(pname(p))}${st ? `<small>${esc(st)}</small>` : ''}</div><div class="s">${esc(prodSub(p))}</div></div>
      <div class="r">${badges(p, true)}${ic('right', 'sm chev')}</div></a>`;
  }
  function renderList() {
    const L = S.list;
    const atc1 = Object.keys(S.atc).filter(k => k.length === 1).sort();
    if (gq.value !== L.q) gq.value = L.q;
    view.innerHTML = header('leky', 'Léky', `Registrované léčivé přípravky · SÚKL k ${esc(S.meta.dlp_release_cs)} · SPC podle SÚKL a EMA`) + `
      <section class="card">
        <div class="card-h"><h2>Léčivé přípravky <span class="count" id="pcount"></span></h2>
          <div class="filters">
            <div class="seg" role="group" aria-label="Výdej">
              <button data-rx="all" aria-pressed="${L.rx === 'all'}">Vše</button><button data-rx="R" aria-pressed="${L.rx === 'R'}">Na předpis</button><button data-rx="F" aria-pressed="${L.rx === 'F'}">Bez předpisu</button>
            </div>
            <select class="select" id="f-atc" aria-label="ATC skupina"><option value="">Všechny ATC skupiny</option>${atc1.map(a => `<option value="${esc(a)}" ${L.atc1 === a ? 'selected' : ''}>${esc(a)} · ${esc(lc(S.atc[a]))}</option>`).join('')}</select>
            <button class="chip" id="c-marketed" aria-pressed="${L.marketed}">${ic('truck', 'xs')}Jen dodávané</button>
            <button class="chip" id="c-homeo" aria-pressed="${L.hideHomeo}">Bez homeopatik</button>
          </div>
        </div>
        ${L.q ? `<div class="small" style="margin:-4px 0 10px">Výsledky pro „<b>${esc(L.q)}</b>“ · <button class="btn link" id="qclear">zrušit hledání</button></div>` : ''}
        <div class="rows" id="plist"></div>
        <div class="pager" id="pager"></div>
      </section>`;
    $$('.seg button', view).forEach(b => b.onclick = () => { L.rx = b.dataset.rx; L.page = 1; $$('.seg button', view).forEach(x => x.setAttribute('aria-pressed', x === b)); drawList(); });
    $('#f-atc').onchange = e => { L.atc1 = e.target.value; L.page = 1; drawList(); };
    $('#c-marketed').onclick = e => { L.marketed = !L.marketed; e.currentTarget.setAttribute('aria-pressed', L.marketed); L.page = 1; drawList(); };
    $('#c-homeo').onclick = e => { L.hideHomeo = !L.hideHomeo; e.currentTarget.setAttribute('aria-pressed', L.hideHomeo); L.page = 1; drawList(); };
    const qc = $('#qclear'); if (qc) qc.onclick = () => { L.q = ''; gq.value = ''; L.page = 1; renderList(); };
    drawList();
  }
  function pagerHtml(page, pages) {
    if (pages <= 1) return '';
    return `<button class="btn sm" data-pg="${page - 1}" ${page <= 1 ? 'disabled style="opacity:.45;cursor:default"' : ''}>${ic('left', 'xs')}Předchozí</button><span class="pos">${page} / ${fmtN(pages)}</span><button class="btn sm" data-pg="${page + 1}" ${page >= pages ? 'disabled style="opacity:.45;cursor:default"' : ''}>Další${ic('right', 'xs')}</button>`;
  }
  function drawList() {
    const L = S.list, res = filterProducts(), pages = Math.max(1, Math.ceil(res.length / PAGE));
    if (L.page > pages) L.page = pages;
    const c = $('#pcount'); if (c) c.textContent = fmtN(res.length) + ' přípravků';
    const list = $('#plist'); if (!list) return;
    if (!res.length) { list.innerHTML = `<div class="empty"><span class="sq">${ic('search')}</span>Nic nenalezeno. Zkuste jiný tvar názvu nebo léčivou látku, případně vypněte filtr „Jen dodávané“.</div>`; $('#pager').innerHTML = ''; return; }
    list.innerHTML = res.slice((L.page - 1) * PAGE, L.page * PAGE).map(prodRow).join('');
    const pg = $('#pager'); pg.innerHTML = pagerHtml(L.page, pages);
    $$('[data-pg]', pg).forEach(b => b.onclick = () => { const n = +b.dataset.pg; if (n >= 1 && n <= pages) { L.page = n; drawList(); $('#plist').scrollIntoView({ block: 'start', behavior: 'smooth' }); } });
  }

  // ---------- DRUG DETAIL ----------
  const PREG_L = { ok: 'Lze použít', caution: 'S opatrností', avoid: 'Nedoporučuje se', ci: 'Kontraindikováno', na: 'Neuvedeno' };
  const DRIVE_L = { none: 'Bez vlivu / zanedbatelný', minor: 'Malý vliv', moderate: 'Mírný vliv', major: 'Výrazný vliv', na: 'Neuvedeno' };
  const FLAG_LABEL = { qt: 'Prodloužení QT', serotonin: 'Serotonergní', hyperk: 'Hyperkalemie', cns: 'Útlum CNS', nephro: 'Nefrotoxicita', antichol: 'Anticholinergní' };
  function lst(items, icon) {
    if (!items || !items.length) return '';
    return '<ul class="lst">' + items.map(x => `<li>${icon ? ic(icon, 'xs') : '<span class="dot"></span>'}<span>${esc(x)}</span></li>`).join('') + '</ul>';
  }
  const NOTHING = mode => `<p class="nothing">${mode === 'ai' ? 'SPC k tomuto bodu nic podstatného neuvádí.' : 'Bez automatického výtahu – otevřete celé znění níže.'}</p>`;
  // otevřené body SPC platí na počítači pro celou návštěvu; na mobilu začíná každý lék se sbalenými boxy
  const isNarrow = () => matchMedia('(max-width:760px)').matches;
  function secOpen() { if (!S.secOpen) S.secOpen = new Set(isNarrow() ? [] : ['4.1', '4.2', '4.3', '4.4', '4.5', '4.6', '4.7', '4.8', '4.9']); return S.secOpen; }
  function bubble(cls, icon, title, num, body, full, wide, hint) {
    const open = secOpen().has(num);
    return `<section class="bubble ${cls} ${wide ? 'wide' : ''} ${(full || '').length > 1400 ? 'long' : ''} ${open ? '' : 'closed'}" data-sec="${num}">
      <h3 class="bhw"><button class="bh" aria-expanded="${open}"><span class="sq">${ic(icon, 'sm')}</span><span class="bt"><span class="bn">${esc(title)}</span>${hint ? `<span class="bsub">${esc(hint)}</span>` : ''}</span><span class="tag hide-m">SPC ${num}</span>${ic('down', 'sm chv')}</button></h3>
      <div class="bb">${body}</div>
      <div class="bf"><button class="toggle-full" aria-expanded="false">${ic('down', 'xs')}<span>Celé znění z SPC</span></button></div>
      <div class="fulltext">${textToHtml(full)}</div>
    </section>`;
  }
  function mxChips(doc) {
    if (!doc || !doc.mx || !doc.mx.length) return '';
    const STOP = new Set(['GLUCOSUM', 'GLUCOSUM MONOHYDRICUM', 'NATRII CHLORIDUM', 'AQUA PRO INIECTIONE', 'SACCHAROSUM', 'LACTOSUM', 'LACTOSUM MONOHYDRICUM', 'PROTHROMBINUM', 'PROTHROMBINUM MULTIPLEX HUMANUM', 'OXYGENIUM', 'ALBUMINUM HUMANUM', 'UREA', 'GLYCEROLUM', 'ETHANOLUM']);
    const subsShown = doc.mx.filter(m => m.t === 's').map(m => S.subByCode[m.k]).filter(s => s && !STOP.has(s.inn));
    const coveredCls = new Set(subsShown.flatMap(s => s.cls || []));
    const clsChips = doc.mx.filter(m => m.t === 'c' && !/^(CYP|PGP)/.test(m.k) && !coveredCls.has(m.k)).map(m => S.ix.classes.find(x => x.id === m.k)).filter(Boolean)
      .map(c => `<span class="tagc cls">${esc(c.label)}</span>`);
    const items = clsChips.concat(subsShown.map(s => `<a class="tagc" href="#/latka/${encodeURIComponent(s.code)}">${esc(s.cs || lc(s.inn))}</a>`));
    if (!items.length) return '';
    const shown = items.slice(0, 18);
    return `<div class="mx-h">V bodě 4.5 jsou zmíněny</div><div class="tags">${shown.join('')}${items.length > 18 ? `<span class="tagc">+${items.length - 18}</span>` : ''}</div>`;
  }
  function renderBubbles(p, doc, block, mode) {
    const sm = mode === 'ai' ? doc.ai : block.sum;
    const s = block.s || {};
    const dose = (sm.dose || []).length ? `<div class="dose">${sm.dose.map(d => `<div class="dose-row"><span class="dose-g">${esc(d.g || 'Dávkování')}</span><span class="dose-t">${esc(d.t)}</span></div>`).join('')}</div>` : '';
    const admin = sm.admin ? `<div class="admin">${ic('info', 'xs')}<span><b>Způsob podání:</b> ${esc(sm.admin)}</span></div>` : '';
    const st = (lab, o, L) => `<div class="st-row"><span class="lab">${lab}</span><div><span class="lvl ${esc(o && o.level || 'na')}">${esc(L[(o && o.level) || 'na'])}</span>${o && o.t ? `<div class="st-text">${esc(o.t)}</div>` : ''}</div></div>`;
    const ae = sm.ae || {};
    const aeBody = [
      (ae.vc || []).length ? `<div class="ae-grp"><div class="lab">Velmi časté (≥ 1/10)</div><div class="tags">${ae.vc.map(x => `<span class="tagc ae">${esc(x)}</span>`).join('')}</div></div>` : '',
      (ae.c || []).length ? `<div class="ae-grp"><div class="lab">Časté (≥ 1/100)</div><div class="tags">${ae.c.map(x => `<span class="tagc">${esc(x)}</span>`).join('')}</div></div>` : '',
      (ae.serious || []).length ? `<div class="ae-grp"><div class="lab">Závažné – sledovat</div>${lst(ae.serious, 'alert')}</div>` : '',
      ae.note ? `<p>${esc(ae.note)}</p>` : ''
    ].join('');
    const ixBody = (lst(sm.ix) || '') + mxChips(doc);
    const html = [
      bubble('c-ind', 'check', 'Indikace', '4.1', lst(sm.ind, 'check') || NOTHING(mode), s['4.1']),
      bubble('c-ki', 'ban', 'Kontraindikace', '4.3', lst(sm.ki, 'x') || NOTHING(mode), s['4.3']),
      bubble('c-dose', 'clock', 'Dávkování a způsob podání', '4.2', (dose + admin) || NOTHING(mode), s['4.2'], true),
      bubble('c-warn', 'alert', 'Upozornění a relativní kontraindikace', '4.4', lst(sm.warn) || NOTHING(mode), s['4.4']),
      bubble('c-ix', 'zap', 'Interakce', '4.5', (ixBody || NOTHING(mode)) + `<div style="margin-top:12px"><button class="btn sm mint" data-ixadd="1">${ic('plus', 'xs')}Přidat do kontroly interakcí</button></div>`, s['4.5']),
      bubble('c-preg', 'baby', 'Těhotenství a kojení', '4.6', `<div class="status">${st('Těhotenství', sm.preg, PREG_L)}${st('Kojení', sm.lact, PREG_L)}</div>`, s['4.6'], false, `Těhotenství: ${PREG_L[(sm.preg && sm.preg.level) || 'na']} · Kojení: ${PREG_L[(sm.lact && sm.lact.level) || 'na']}`),
      bubble('c-drive', 'car', 'Řízení a obsluha strojů', '4.7', `<div class="status">${st('Vliv', sm.drive, DRIVE_L)}</div>`, s['4.7'], false, DRIVE_L[(sm.drive && sm.drive.level) || 'na']),
      bubble('c-ae', 'activity', 'Nežádoucí účinky', '4.8', aeBody || NOTHING(mode), s['4.8']),
      bubble('c-od', 'droplet', 'Předávkování', '4.9', sm.od ? `<p>${esc(sm.od)}</p>` : NOTHING(mode), s['4.9'])
    ].join('');
    return html;
  }
  function pickBlock(doc, variant) {
    if (!doc || !doc.blocks || !doc.blocks.length) return -1;
    if (doc.blocks.length === 1 || !variant) return 0;
    if (doc.ai) return 0;
    const sila = norm(variant.sila).replace(/\s+/g, ''), forma = norm(variant.forma_n || '');
    let best = 0, bestScore = -1;
    doc.blocks.forEach((b, i) => { const nm = norm(b.name).replace(/\s+/g, ''); let sc = 0; if (sila && nm.includes(sila)) sc += 2; if (forma && norm(b.name).includes(forma.split(' ')[0])) sc += 1; if (sc > bestScore) { bestScore = sc; best = i; } });
    return best;
  }
  async function renderDrug(id) {
    const p = S.byId[id];
    if (!p) { view.innerHTML = header('leky', 'Léky') + `<div class="card empty"><span class="sq">${ic('search')}</span>Přípravek nenalezen.</div>`; return; }
    view.innerHTML = loading('Načítám údaje o přípravku…');
    if (isNarrow()) S.secOpen = null;
    const nav = NAV;
    let full;
    try { full = await prodShard(id); } catch (e) { if (nav === NAV) failPage(e); return; }
    if (nav !== NAV) return;
    const variants = full ? full.variants : [];
    const spcs = (full ? full.spcs : []).filter(s => s.ok);
    const marketedVars = variants.filter(v => v.dodavky);
    const spcsSorted = spcs.slice().sort((a, b) => (b.marketed - a.marketed) || (b.kody.length - a.kody.length));
    const cur = spcsSorted[0] || null;
    const firstKod = (marketedVars[0] || variants[0] || {}).kod;
    const inSel = S.sel.some(x => x.type === 'product' && x.id === p.i);
    const subs = p.s.map(s => `<a class="chip" href="#/latka/${encodeURIComponent(s.code)}">${ic('flask', 'xs')}${esc(subLabel(s))}</a>`).join('');
    const generics = S.products.filter(o => o.i !== p.i && o.s.length && o.s.length === p.s.length && o.s.every((s, i) => s.code === p.s[i].code)).sort((a, b) => (b.m - a.m) || pname(a).localeCompare(pname(b), 'cs'));
    const forms = [...new Set(variants.map(v => lc(v.forma_n)))];
    const spcLink = cur ? (cur.eu && okUrl(cur.url) ? cur.url : spcUrl(cur.kody[0], 'spc')) : (firstKod ? spcUrl(firstKod, 'spc') : '');
    view.innerHTML = `
      <div class="crumbs"><a href="#/leky">Léky</a>${ic('right', 'xs')}<span>${esc(pname(p))}</span></div>
      <section class="card">
        <div class="hero lp-hero">
          <span class="sq lg">${ic('pill', 'lg')}</span>
          <div class="main">
            <h1>${esc(pname(p))}</h1>
            <div class="subs">${subs || '<span class="muted">bez léčivé látky</span>'}<span class="small">${p.a.length ? 'ATC ' + esc(p.a.join(', ')) + (p.an[0] ? ' · ' + esc(lc(p.an[0])) : '') : ''}</span></div>
            <div class="flags" style="margin-top:10px">${badges(p)}</div>
          </div>
          <div class="actions lp-actions">
            <button class="btn ${inSel ? 'mint' : 'primary'}" id="addix">${ic(inSel ? 'check' : 'plus', 'sm')}${inSel ? 'V kontrole interakcí' : 'Přidat do kontroly interakcí'}</button>
            ${spcLink ? `<a class="btn" target="_blank" rel="noopener" href="${esc(spcLink)}">${ic('file', 'sm')}<span>SPC<span class="hide-m"> (PDF)</span></span></a>` : ''}
            ${firstKod ? `<a class="btn" target="_blank" rel="noopener" href="${esc(spcUrl(firstKod, 'pil'))}">${ic('file', 'sm')}<span>Příbalová<span class="hide-m"> informace</span></span></a><a class="btn" target="_blank" rel="noopener" href="${esc(suklProductUrl(firstKod))}">${ic('ext', 'sm')}SÚKL</a>` : ''}
          </div>
        </div>
        <details class="lp-more"><summary>${ic('info', 'sm')}<span>Informace o přípravku</span><span class="small hide-m">výdej, formy, síly, držitel, registrace, dodávky</span>${ic('down', 'sm chv')}</summary><div class="facts">
          <div class="fact">${ic('receipt', 'sm')}<div><div class="k">Výdej</div><div class="v">${esc(p.v.map(v => VYDEJ[v] || v).join(', ') || '—')}</div></div></div>
          <div class="fact">${ic('box', 'sm')}<div><div class="k">Lékové formy</div><div class="v">${esc(forms.join(', ') || '—')}</div></div></div>
          <div class="fact">${ic('scale', 'sm')}<div><div class="k">Síly</div><div class="v">${esc(p.st.join(', ') || '—')}</div></div></div>
          <div class="fact">${ic('building', 'sm')}<div><div class="k">Držitel registrace</div><div class="v">${esc(p.h.join('; '))}</div></div></div>
          <div class="fact">${ic('globe', 'sm')}<div><div class="k">Registrace</div><div class="v">${p.eu ? 'Centralizovaná (EU) · SPC spravuje EMA' : 'Národní / MRP-DCP · SPC spravuje SÚKL'}</div></div></div>
          <div class="fact">${ic('truck', 'sm')}<div><div class="k">Dodávky na trh</div><div class="v">${marketedVars.length} z ${variants.length} variant dodáváno</div></div></div>
        </div></details>
      </section>
      <div id="spcwrap">${loading('Načítám SPC…')}</div>
      <details class="card"><summary><span style="display:flex;align-items:center;gap:8px">${ic('layers', 'sm')}Varianty, složení a balení <span class="count">(${variants.length})</span></span>${ic('down', 'sm chv')}</summary>
        <div class="table-wrap"><table class="vars"><thead><tr><th>Kód SÚKL</th><th>Síla · forma · balení</th><th>Léčivé látky</th><th>Výdej</th><th>Stav</th><th>SPC</th></tr></thead><tbody>
        ${variants.slice().sort((a, b) => (b.dodavky - a.dodavky) || a.doplnek.localeCompare(b.doplnek, 'cs')).map(v => `<tr class="${v.dodavky ? '' : 'na'}"><td class="code">${esc(v.kod)}</td><td>${esc(v.sila)} ${esc(lc(v.forma_n))} · ${esc(v.baleni)} · ${esc(lc(v.cesta_n))}</td><td>${compHtml(v)}</td><td>${esc(VYDEJ[v.vydej] || v.vydej || '—')}</td><td>${v.dodavky ? 'dodáván' : 'nedodáván'}${v.sou_dovoz ? ' · souběžný dovoz' : ''}</td><td>${v.spc ? `<a target="_blank" rel="noopener" href="${esc(v.eu && okUrl(v.spc) ? v.spc : spcUrl(v.kod, 'spc'))}">${esc(v.spc_date || 'SPC')}</a>` : '—'}</td></tr>`).join('')}
        </tbody></table></div></details>
      ${generics.length ? `<details class="card"><summary><span style="display:flex;align-items:center;gap:8px">${ic('flask', 'sm')}Další přípravky se stejnou léčivou látkou <span class="count">(${generics.length})</span></span>${ic('down', 'sm chv')}</summary><div class="rows">${generics.slice(0, 8).map(prodRow).join('')}</div>${p.s.length === 1 ? `<div style="margin-top:10px"><a class="btn sm" href="#/latka/${encodeURIComponent(p.s[0].code)}">Zobrazit všechny${ic('right', 'xs')}</a></div>` : ''}</details>` : ''}
      <div id="lpnote"></div>`;
    $('#addix').onclick = () => { if (!S.sel.some(x => x.type === 'product' && x.id === p.i)) addSelection({ type: 'product', id: p.i }); const b = $('#addix'); b.className = 'btn mint'; b.innerHTML = ic('check', 'sm') + 'V kontrole interakcí'; };
    if (!cur) { $('#spcwrap').innerHTML = `<div class="card"><div class="src-note">${ic('info', 'sm')}<span>K tomuto přípravku SÚKL nezveřejňuje text SPC ve strojově čitelné podobě (typicky homeopatika nebo přípravky bez schváleného SPC). Dokumenty najdete u variant níže.</span></div></div>`; return; }
    await drawSpc(p, spcsSorted, cur, variants);
  }
  function compHtml(v) {
    if (!v.comp || !v.comp.length) return '<i>bez léčivé látky</i>';
    return v.comp.map(e => { let s = esc(lc(e.cs || e.inn)) + (e.amount ? ' ' + esc((e.amount_from ? e.amount_from + '–' : '') + e.amount + ' ' + lc(e.unit)) : '');
      if (e.corresponds && e.corresponds.length) s += ' <span class="small">(odp. ' + e.corresponds.map(c => esc(lc(c.cs || c.inn)) + (c.amount ? ' ' + esc(c.amount + ' ' + lc(c.unit)) : '')).join(', ') + ')</span>';
      return s; }).join('; ');
  }
  let SPC_RUN = 0;
  async function drawSpc(p, spcs, cur, variants, blockIdx) {
    const nav = NAV, run = ++SPC_RUN;
    let doc;
    try { doc = await spcDoc(cur.id); } catch (e) { if (nav === NAV && run === SPC_RUN) failPage(e, $('#spcwrap')); return; }
    if (nav !== NAV || run !== SPC_RUN) return;   // jiná stránka nebo jiná síla mezitím
    const wrap = $('#spcwrap'); if (!wrap) return;
    const v0 = variants.find(v => cur.kody.includes(v.kod)) || variants[0];
    const bi = blockIdx != null ? blockIdx : pickBlock(doc, v0);
    const block = doc && bi >= 0 ? doc.blocks[bi] : null;
    const origLink = cur.eu && okUrl(cur.url) ? cur.url : spcUrl(cur.kody[0], 'spc');
    const narrow = isNarrow();
    const seenLab = {};
    const tabLabs = spcs.map(s => { seenLab[s.label] = (seenLab[s.label] || 0) + 1; return [s, seenLab[s.label] > 1 ? `${s.label} (${seenLab[s.label]})` : s.label]; });
    const chips = tabLabs.map(([s, lab]) => `<button class="chip" data-spc="${esc(s.id)}" aria-pressed="${s.id === cur.id}">${esc(lab)}</button>`).join('');
    const curLab = (tabLabs.find(([s]) => s.id === cur.id) || [])[1] || '';
    const tabs = spcs.length < 2 ? '' : narrow
      ? `<details class="spc-pick"><summary><span class="k">Síla a forma</span><span class="v">${esc(curLab)}</span><span class="n">${spcs.length}</span>${ic('down', 'sm chv')}</summary><div class="filters">${chips}</div></details>`
      : `<div class="filters" style="margin-bottom:12px">${chips}</div>`;
    if (!doc || !block) { wrap.innerHTML = tabs + `<div class="card"><div class="src-note">${ic('info', 'sm')}<span>Text SPC se nepodařilo strojově zpracovat. Otevřete prosím <a target="_blank" rel="noopener" href="${esc(origLink)}">originál SPC (${cur.eu ? 'EMA' : 'SÚKL'})</a>.</span></div></div>`; bindTabs(p, spcs, variants); return; }
    const mode = doc.ai && bi === 0 ? 'ai' : 'auto';
    const flags = doc.flags ? Object.entries(doc.flags).filter(([k, v]) => v && FLAG_LABEL[k]).map(([k]) => `<span class="badge amber" title="Zmíněno v SPC (bod 4.4/4.5)">${esc(FLAG_LABEL[k])}</span>`).join('') : '';
    const blockSel = doc.blocks.length > 1 ? `<div class="block-sel" style="margin-top:8px"><span class="small">Dokument obsahuje ${doc.blocks.length} SPC (různé síly/formy):</span><select class="select" id="blocksel">${doc.blocks.map((b, i) => `<option value="${i}" ${i === bi ? 'selected' : ''}>${esc(b.name.split('\n')[0].slice(0, 90))}</option>`).join('')}</select></div>` : '';
    const OFFICIAL = `Pro celé znění a úplnost informací se řiďte oficiálním SPC na <a href="https://opendata.sukl.cz" target="_blank" rel="noopener">opendata.sukl.cz</a>${cur.eu ? ' (u centrálně registrovaných přípravků zveřejňuje SPC EMA)' : ''}.`;
    const modeShort = mode === 'ai'
      ? `<b>Stručné shrnutí je interpretací MedInter</b> vytvořenou s pomocí AI z textu SPC, bez lékařské kontroly. Rozhoduje oficiální SPC.`
      : `<b>Výtah ze SPC</b> – věty převzaté doslovně z SPC, výběr a zkrácení provedl MedInter. Rozhoduje oficiální SPC.`;
    const modeMore = (mode === 'ai' ? `Automaticky se ověřuje jen to, že každé uvedené číslo (dávka, věk, eGFR…) se v textu SPC vyskytuje – ne jeho souvislost ani úplnost. ` : '') + `Celé znění jednotlivých bodů rozkliknete u každé části.`;
    const extras = `${flags ? `<div class="flags" style="margin-top:8px">${flags}</div>` : ''}${blockSel}`;
    const note = `<div class="src-note">${ic('info', 'sm')}<div><div>${modeShort}</div><details class="src-more"><summary>Zdroj a způsob ověření</summary><div>${modeMore} ${OFFICIAL}</div><div style="margin-top:4px">${esc(block.name.split('\n').join(' · '))} · ${cur.eu ? 'EMA – informace o přípravku' : 'SÚKL – schválené SPC'}${doc.rev ? ' · revize textu ' + esc(doc.rev) : ''}${cur.date ? ' · rozhodnutí ' + esc(cur.date) : ''}</div></details>${narrow ? '' : extras}</div></div>`;
    $('#lpnote').innerHTML = narrow ? note : '';
    wrap.innerHTML = `${tabs}
      <div class="sumbar"><h2>${ic(mode === 'ai' ? 'sparkles' : 'file', 'sm')}Souhrn údajů o přípravku${mode === 'ai' ? ' <span class="badge mint">Shrnutí</span>' : ' <span class="badge gray">Výtah</span>'}${doc.black_triangle ? ' <span class="badge amber" title="Přípravek podléhá dalšímu sledování">▼ Další sledování</span>' : ''}</h2>
        <div class="filters"><button class="btn sm" id="expandAll"></button><a class="btn sm hide-m" target="_blank" rel="noopener" href="${esc(origLink)}">${ic('ext', 'xs')}Originál SPC</a></div></div>
      ${narrow ? (extras ? `<div class="m-extras">${extras}</div>` : '') : note}
      <div class="bubbles">${renderBubbles(p, doc, block, mode)}</div>`;
    $$('.toggle-full', wrap).forEach(b => b.onclick = () => { const bub = b.closest('.bubble'); const open = bub.classList.toggle('open'); b.setAttribute('aria-expanded', open); b.querySelector('span').textContent = open ? 'Skrýt celé znění' : 'Celé znění z SPC'; });
    const ea = $('#expandAll');
    const paintEa = () => { const open = !$$('.bubble.closed', wrap).length; ea.innerHTML = ic('expand', 'xs') + (open ? 'Sbalit vše' : 'Rozbalit vše'); };
    const setBox = (x, open) => { x.classList.toggle('closed', !open); $('.bh', x).setAttribute('aria-expanded', open); if (open) secOpen().add(x.dataset.sec); else secOpen().delete(x.dataset.sec); };
    $$('.bh', wrap).forEach(b => b.onclick = () => { const x = b.closest('.bubble'); setBox(x, x.classList.contains('closed')); paintEa(); });
    ea.onclick = () => { const open = !!$$('.bubble.closed', wrap).length; $$('.bubble', wrap).forEach(x => setBox(x, open)); paintEa(); };
    paintEa();
    const ixb = $('[data-ixadd]', wrap); if (ixb) ixb.onclick = () => { addSelection({ type: 'product', id: p.i }); location.hash = '#/interakce'; };
    const bs = $('#blocksel'); if (bs) bs.onchange = () => drawSpc(p, spcs, cur, variants, +bs.value);
    bindTabs(p, spcs, variants);
  }
  function bindTabs(p, spcs, variants) { $$('[data-spc]').forEach(b => { b.onclick = () => { const s = spcs.find(x => x.id === b.dataset.spc); if (s) drawSpc(p, spcs, s, variants); }; }); }

  // ---------- NEJČASTĚJŠÍ VE VPL ----------
  function fmtPacks(n) { return n >= 1e6 ? (n / 1e6).toFixed(1).replace('.', ',') + ' mil.' : Math.round(n / 1000) + ' tis.'; }
  function cleanVpl(v) { // jen očekávaná pole: čísla jako čísla, barva jen jako název proměnné (písmena)
    const str = x => typeof x === 'string' ? x : '', num = x => typeof x === 'number' && isFinite(x) && x > 0 ? x : 0;
    const boxes = (v && Array.isArray(v.boxes) ? v.boxes : []).filter(b => b && typeof b === 'object').map(b => ({
      o: str(b.o), icon: str(b.icon), tone: /^[a-z]{1,8}$/.test(b.tone) ? b.tone : 'od',
      items: (Array.isArray(b.items) ? b.items : []).filter(it => it && Array.isArray(it.s))
        .map(it => ({ atc: str(it.atc), packs: num(it.packs), s: it.s.filter(x => x && typeof x.c === 'string' && x.c).map(x => ({ c: x.c, n: str(x.n) })) }))
        .filter(it => it.s.length)
    }));
    return { period: str(v && v.period), boxes };
  }
  async function renderVpl() {
    const head = header('vpl', 'Nejčastější ve VPL', 'Studijní rozcestník na nejčastěji předepisované léčivé látky ve VPL, rozdělený podle oborů');
    if (!S.vpl) {
      view.innerHTML = head + loading();
      const nav = NAV;
      try { S.vpl = cleanVpl(await getJSON('vpl.json')); }
      catch (e) { if (nav === NAV) view.innerHTML = head + `<div class="card empty">Seznam se nepodařilo načíst (${esc(e.message)}).</div>`; return; }
      if (nav !== NAV) return;
    }
    const boxes = S.vpl.boxes, top = Math.max(1, ...boxes.flatMap(b => b.items.map(i => i.packs)));
    const tone = b => `--tbg:var(--${b.tone}-bg);--tfg:var(--${b.tone}-fg)`;
    const name = s => { const x = S.subByCode[s.c]; return (x && x.cs) || s.n; };
    const row = it => {
      const meter = `<span class="vpl-meter"><i style="width:${Math.max(2, it.packs / top * 100).toFixed(1)}%"></i></span>`;
      const num = `<span class="vpl-n" title="${fmtN(it.packs)} předepsaných balení">${fmtPacks(it.packs)}</span>`;
      if (it.s.length === 1) {
        const x = S.subByCode[it.s[0].c];
        return `<a class="vpl-row" href="#/latka/${encodeURIComponent(it.s[0].c)}"><div class="vpl-row-t"><span class="vpl-name">${esc(name(it.s[0]))}</span>${num}</div><div class="vpl-meta">${esc(it.atc)}${x ? ' · ' + (Number(x.n) || 0) + ' LP' : ''}</div>${meter}</a>`;
      }
      const nLp = S.products.filter(p => it.s.every(c => p.s.some(y => y.code === c.c))).length;
      return `<div class="vpl-row"><div class="vpl-row-t"><span class="vpl-name">${it.s.map(s => `<a href="#/latka/${encodeURIComponent(s.c)}">${esc(name(s))}</a>`).join(' + ')}</span>${num}</div><div class="vpl-meta">${esc(it.atc)} · kombinace · ${nLp} LP</div>${meter}</div>`;
    };
    view.innerHTML = head + `<div class="vpl-tools"><div class="ix-search field-wrap" style="flex:1 1 280px;max-width:420px"><div class="field">${ic('search', 'sm')}<input id="vq" type="search" placeholder="Najít látku nebo ATC v seznamu…" autocomplete="off"/></div></div></div>
      <details class="vpl-src"><summary>Odkud čísla pocházejí</summary><p>Předepsaná balení z eReceptu SÚKL za ${esc(S.vpl.period)} podle ATC, od všech odborností, protože data odbornost lékaře neuvádějí. Výběr 100 položek je proto odhad pro obor VPL: ručně jsou vyřazeny PZLÚ, oční přípravky, antikoncepce, inzuliny a čistě specializovaná léčiva. Paracetamol a ibuprofen jsou doplněny kvůli volnému prodeji, eRecept je podhodnocuje. Délka pruhu ukazuje počet balení vůči nejčastější položce, zařazení do oborů je orientační.</p></details>
      <div class="vpl-chips" id="vchips"></div><div class="vpl-grid" id="vgrid"></div>`;
    const draw = q => {
      const nq = norm(q.trim());
      const hit = it => !nq || it.atc.toLowerCase().startsWith(nq) || it.s.some(s => norm(name(s)).includes(nq));
      const vis = boxes.map(b => ({ b, items: b.items.filter(hit) }));
      $('#vchips').innerHTML = vis.map(({ b, items }, i) => `<button class="vpl-chip" data-i="${i}" style="${tone(b)}" ${items.length ? '' : 'disabled'}><span class="tile">${ic(b.icon, 'xs')}</span>${esc(b.o)} <b>${items.length}</b></button>`).join('');
      $('#vgrid').innerHTML = vis.filter(v => v.items.length).map(({ b, items }) => `<section class="vpl-box" id="vbox-${boxes.indexOf(b)}" style="${tone(b)}"><div class="vpl-box-h"><span class="tile">${ic(b.icon)}</span><div><h2>${esc(b.o)}</h2><p>${items.length} ${items.length === 1 ? 'položka' : items.length < 5 ? 'položky' : 'položek'} · ${fmtPacks(items.reduce((s, r) => s + r.packs, 0))} balení</p></div></div>${items.map(row).join('')}</section>`).join('') || `<div class="vpl-empty">Nic nenalezeno.</div>`;
      $$('.vpl-chip').forEach(c => c.onclick = () => { const el = $('#vbox-' + c.dataset.i); if (!el) return; el.scrollIntoView({ behavior: 'smooth', block: 'start' }); el.classList.add('flash'); setTimeout(() => el.classList.remove('flash'), 1400); });
    };
    $('#vq').addEventListener('input', debounce(e => draw(e.target.value), 100));
    draw('');
  }

  // ---------- SUBSTANCES ----------
  function renderSubstances() {
    view.innerHTML = header('latky', 'Léčivé látky', 'Účinné látky obsažené v registrovaných přípravcích') + `
      <section class="card"><div class="card-h"><h2>Léčivé látky <span class="count" id="scount"></span></h2>
        <div class="ix-search" style="min-width:min(360px,100%)"><div class="field">${ic('search', 'sm')}<input id="sq" type="search" placeholder="Hledat látku nebo INN…" value="${esc(S.subs.q)}" autocomplete="off"/></div></div></div>
        <div class="rows" id="slist"></div><div class="pager" id="spager"></div></section>`;
    const draw = () => {
      const nq = norm(S.subs.q.trim());
      const res = S.substances.filter(s => s.n > 0 && (!nq || norm(s.cs).includes(nq) || norm(s.inn).includes(nq) || norm(s.en).includes(nq)));
      res.sort((a, b) => (nq ? (norm(b.cs).startsWith(nq) - norm(a.cs).startsWith(nq)) : 0) || b.n - a.n || (a.cs || '').localeCompare(b.cs || '', 'cs'));
      const pages = Math.max(1, Math.ceil(res.length / PAGE)); if (S.subs.page > pages) S.subs.page = pages;
      $('#scount').textContent = fmtN(res.length) + ' látek';
      $('#slist').innerHTML = res.slice((S.subs.page - 1) * PAGE, S.subs.page * PAGE).map(s => `<a class="row" href="#/latka/${encodeURIComponent(s.code)}"><span class="sq sm">${ic('flask', 'sm')}</span><div class="main"><div class="t">${esc(s.cs || lc(s.inn))}</div><div class="s">${esc(s.inn)} · ${esc(s.atc.slice(0, 3).join(', '))}</div></div><div class="r"><span class="badge gray keep">${Number(s.n) || 0} LP</span>${ic('right', 'sm chev')}</div></a>`).join('') || `<div class="empty">Nic nenalezeno.</div>`;
      const pg = $('#spager'); pg.innerHTML = pagerHtml(S.subs.page, pages);
      $$('[data-pg]', pg).forEach(b => b.onclick = () => { const n = +b.dataset.pg; if (n >= 1 && n <= pages) { S.subs.page = n; draw(); } });
    };
    $('#sq').addEventListener('input', debounce(e => { S.subs.q = e.target.value; S.subs.page = 1; draw(); }, 120));
    draw();
  }
  function classLabel(id) { const c = S.ix && S.ix.classes.find(c => c.id === id); return c ? c.label : id; }
  const ROLE_CS = { strong_inhibitor: 'silný inhibitor', moderate_inhibitor: 'střední inhibitor', weak_inhibitor: 'slabý inhibitor', inhibitor: 'inhibitor', strong_inducer: 'silný induktor', moderate_inducer: 'střední induktor', weak_inducer: 'slabý induktor', inducer: 'induktor', sensitive_substrate: 'citlivý substrát', moderate_sensitive_substrate: 'středně citlivý substrát', narrow_TI_substrate: 'substrát s úzkým terapeutickým indexem', substrate: 'substrát' };
  function renderSubstance(code) {
    const s = S.subByCode[code];
    if (!s) { view.innerHTML = header('latky', 'Léčivé látky') + `<div class="card empty">Látka nenalezena.</div>`; return; }
    const prods = S.products.filter(p => p.s.some(x => x.code === code)).sort((a, b) => (a.s.length - b.s.length) || (b.m - a.m) || pname(a).localeCompare(pname(b), 'cs'));
    const mono = prods.filter(p => p.s.length === 1), combo = prods.filter(p => p.s.length > 1);
    const cls = (s.cls || []).map(c => `<span class="tagc cls">${esc(classLabel(c))}</span>`).join('');
    const cyp = (S.ix ? S.ix.cyp.filter(r => r.key === s.inn) : []).map(r => `<span class="tagc">${esc(r.enzyme)}: ${esc(ROLE_CS[r.role] || r.role)}</span>`).join('');
    const inSel = S.sel.some(x => x.type === 'substance' && x.code === code);
    view.innerHTML = `<div class="crumbs"><a href="#/latky">Léčivé látky</a>${ic('right', 'xs')}<span>${esc(s.cs || lc(s.inn))}</span></div>
      <section class="card"><div class="hero"><span class="sq lg">${ic('flask', 'lg')}</span><div class="main"><h1>${esc(s.cs || lc(s.inn))}</h1>
        <div class="subs"><span class="small">${esc(s.inn)} · ${esc(s.en)}${s.atc.length ? ' · ATC ' + esc(s.atc.join(', ')) : ''}</span></div>
        ${s.salts && s.salts.length ? `<div class="small" style="margin-top:6px">Soli a formy v přípravcích: ${esc(s.salts.map(x => lc(x.cs || x.inn)).join(', '))}</div>` : ''}
        ${cls || cyp ? `<div class="mx-h">Farmakologické skupiny a metabolismus</div><div class="tags">${cls}${cyp}</div>` : ''}</div>
        <div class="actions"><button class="btn ${inSel ? 'mint' : 'primary'}" id="addix">${ic(inSel ? 'check' : 'plus', 'sm')}${inSel ? 'V kontrole interakcí' : 'Přidat do kontroly interakcí'}</button></div></div></section>
      ${mono.length ? `<section class="card"><div class="card-h"><h2>Přípravky s touto látkou <span class="count">${mono.length}</span></h2></div><div class="rows">${mono.map(prodRow).join('')}</div></section>` : ''}
      ${combo.length ? `<section class="card"><div class="card-h"><h2>Kombinované přípravky <span class="count">${combo.length}</span></h2></div><div class="rows">${combo.map(prodRow).join('')}</div></section>` : ''}`;
    $('#addix').onclick = () => { addSelection({ type: 'substance', code }); const b = $('#addix'); b.className = 'btn mint'; b.innerHTML = ic('check', 'sm') + 'V kontrole interakcí'; };
  }

  // ---------- interactions: selection ----------
  function selItem(x) {
    if (x.type === 'product') { const p = S.byId[x.id]; if (!p) return null; return { type: 'product', id: p.i, label: pname(p), sub: p.s.map(subLabel).join(' + '), keys: p.s.map(s => s.k), codes: p.s.map(s => s.code), atc: p.a, spc: p.sp && p.sp[0] }; }
    if (x.type === 'substance') { const s = S.subByCode[x.code]; if (!s) return null; return { type: 'substance', code: s.code, label: s.cs || lc(s.inn), sub: s.inn, keys: [s.inn], codes: [s.code], atc: s.atc, spc: s.rep }; }
    return null;
  }
  function addSelection(x) { ixTouched = true; if (!S.sel.some(y => y.type === x.type && (y.id || y.code) === (x.id || x.code))) S.sel.push(x); saveSel(); }
  function saveSel() { store('sel', S.sel); store('food', S.food); }
  const SEL_MAX_LINK = 30, SEL_MAX_STORED = 100;   // a shared link / the stored selection; what the user adds is not limited
  function cleanSel(list, max) {
    const seen = new Set(), out = [];
    for (const x of Array.isArray(list) ? list : []) {
      if (!x || typeof x !== 'object') continue;
      const k = x.type === 'product' && typeof x.id === 'string' && S.byId[x.id] ? 'p:' + x.id : x.type === 'substance' && typeof x.code === 'string' && S.subByCode[x.code] ? 's:' + x.code : '';
      if (!k || seen.has(k)) continue;
      seen.add(k); out.push(x.type === 'product' ? { type: 'product', id: x.id } : { type: 'substance', code: x.code });
      if (out.length >= max) break;
    }
    return out;
  }
  function parseSelection(qs) { const m = new URLSearchParams(qs); const l = m.get('l'); if (l) { ixTouched = true; S.sel = cleanSel(l.split(',').slice(0, 200).filter(Boolean).map(t => t.startsWith('s:') ? { type: 'substance', code: t.slice(2) } : { type: 'product', id: t }), SEL_MAX_LINK); } ['FOOD_GRAPEFRUIT', 'FOOD_ALCOHOL', 'FOOD_DAIRY', 'HERB_STJOHNSWORT'].forEach(k => { S.food[k] = m.get(k) === '1'; }); saveSel(); }
  function selectionLink(max) { const l = S.sel.slice(0, max || S.sel.length).map(x => x.type === 'substance' ? 's:' + x.code : x.id).join(','); const f = Object.entries(S.food).filter(([k, v]) => v).map(([k]) => '&' + k + '=1').join(''); return '#/interakce?l=' + l + f; }

  // ---------- interactions: engine ----------
  function itemClasses(it) {
    const cls = new Set();
    for (const k of it.keys) { const sc = S.ix.subst_classes[k]; if (sc) sc.forEach(c => cls.add(c)); }
    if (it.type === 'food') cls.add(it.key);
    return cls;
  }
  function sideMatches(side, it, cls) { return side.type === 'substance' ? it.keys.includes(side.key) : cls.has(side.key); }
  function stemPatterns(key) { const s = S.subByKey[key]; return s && Array.isArray(s.pat) ? s.pat.map(re).filter(Boolean) : []; }
  function classPatterns(cid) { const c = S.ix.classes.find(c => c.id === cid); return c && Array.isArray(c.patterns) ? c.patterns.map(re).filter(Boolean) : []; }
  function splitSentences(t) { return t.split(/\n+/).flatMap(par => par.split(/(?<=[.;])\s+(?=[A-ZÁ-Ž(])/)).map(x => x.trim()).filter(x => x.length > 15); }
  function findQuotes(text, pats) {
    if (!text || !pats.length) return [];
    const out = []; const sents = splitSentences(text);
    for (let i = 0; i < sents.length; i++) {
      let sent = sents[i]; const n = norm(sent);
      for (const re of pats) { if (re.test(n)) { if (/:$/.test(sent) && sent.length < 90 && sents[i + 1]) sent = sent + ' ' + sents[i + 1]; out.push({ sent, re }); break; } }
      if (out.length >= 6) break;
    }
    return out;
  }
  function highlight(sent, re) { const n = norm(sent); const m = n.match(re); if (!m) return esc(sent); const i = m.index, j = i + m[0].length; return esc(sent.slice(0, i)) + '<mark>' + esc(sent.slice(i, j)) + '</mark>' + esc(sent.slice(j)); }
  async function spcInteractionText(it) {
    if (!it.spc) return null; const doc = await spcDoc(it.spc); if (!doc || !doc.blocks.length) return null; const b = doc.blocks[0]; return { text: (b.s['4.5'] || ''), ki: (b.s['4.3'] || ''), name: b.name.split('\n')[0], url: doc.url || null, src: doc.src };
  }
  async function analyse(items) {
    const findings = [];
    const cls = items.map(itemClasses);
    const rules = S.ix.rules;
    const texts = await Promise.all(items.map(it => it.type === 'food' ? null : spcInteractionText(it)));
    for (let i = 0; i < items.length; i++) for (let j = i + 1; j < items.length; j++) {
      const A = items[i], B = items[j], ca = cls[i], cb = cls[j];
      const covered = new Set();
      // 1. duplicity
      const dup = A.keys.filter(k => B.keys.includes(k));
      if (dup.length) findings.push({ pair: [i, j], sev: 'D', src: 'Duplicita', title: 'Stejná léčivá látka ve dvou přípravcích', body: `<p>Oba přípravky obsahují <b>${esc(dup.map(k => subLabel({ k, c: (S.subByKey[k] || {}).cs })).join(', '))}</b>. Hrozí nechtěné zdvojení dávky (často u OTC + Rp. nebo u kombinovaných přípravků). Zkontrolujte celkovou denní dávku a pacienta poučte.</p>` });
      else { const a4 = new Set(A.atc.map(a => a.slice(0, 5))); const shared = B.atc.map(a => a.slice(0, 5)).filter(a => a4.has(a)); if (shared.length && A.type !== 'food' && B.type !== 'food') findings.push({ pair: [i, j], sev: 'B', src: 'Duplicita', title: 'Dvě látky ze stejné farmakologické podskupiny (ATC ' + esc(shared[0]) + ')', body: `<p>Obě látky patří do skupiny <b>${esc(lc(S.atc[shared[0]] || shared[0]))}</b>. Ověřte, zda jde o záměrnou kombinaci; u NSAID, benzodiazepinů, antidepresiv či antipsychotik jde obvykle o nevhodnou duplicitu (STOPP kritéria).</p>` }); }
      // 2. curated rules
      for (const r of rules) {
        let hit = null;
        if (sideMatches(r.a, A, ca) && sideMatches(r.b, B, cb)) hit = [A, B]; else if (sideMatches(r.a, B, cb) && sideMatches(r.b, A, ca)) hit = [B, A];
        if (!hit) continue;
        if (r.a.type === 'class' && r.b.type === 'class' && r.a.key === r.b.key && A.keys.some(k => B.keys.includes(k))) continue;
        (r.tags || []).forEach(t => covered.add(t)); covered.add('rule');
        findings.push({ pair: [i, j], sev: SEV[r.severity] ? r.severity : 'C', src: 'Kurátorovaný přehled', title: `${esc(r.a.label)} × ${esc(r.b.label)}`, rule: r,
          body: `<div class="lab">Mechanismus</div><p>${esc(r.mechanism)}</p><div class="lab">Riziko</div><p>${esc(r.effect)}</p><div class="lab">Doporučení</div><p>${esc(r.management)}</p>${r.onset ? '<div class="small" style="margin-top:6px">Nástup: ' + esc(r.onset) + '</div>' : ''}<div class="ev">Zdroje: ${esc((r.evidence || []).join('; '))}${r.confidence === 'medium' ? ' · <i>střední míra jistoty</i>' : ''}</div>` });
      }
      // 3. CYP / P-gp table (FDA) – PK pairs
      for (const [X, Y] of [[A, B], [B, A]]) {
        for (const enz of S.ix.enzymes) {
          const inh = S.ix.cyp.filter(r => r.enzyme === enz && X.keys.includes(r.key) && /inhibitor|inducer/.test(r.role) && !/weak/.test(r.role));
          if (!inh.length) continue;
          const sub = S.ix.cyp.filter(r => r.enzyme === enz && Y.keys.includes(r.key) && /substrate/.test(r.role));
          if (!sub.length) continue;
          const ri = inh[0], rs = sub[0];
          const strong = /strong/.test(ri.role), sens = /sensitive|narrow/.test(rs.role), inducer = /inducer/.test(ri.role);
          const sev = strong && sens ? 'D' : (strong || sens) ? 'C' : 'B';
          const what = inducer ? 'snížení expozice a riziko selhání účinku' : 'zvýšení expozice a riziko toxicity';
          const xl = (S.subByKey[ri.key] || {}).cs || lc(ri.key), yl = (S.subByKey[rs.key] || {}).cs || lc(rs.key);
          findings.push({ pair: [i, j], sev, src: 'Farmakokinetika (CYP/P-gp)', title: `${esc(xl)} (${esc(ROLE_CS[ri.role])} ${esc(enz)}) × ${esc(yl)} (${esc(ROLE_CS[rs.role])} ${esc(enz)})`,
            body: `<p><b>${esc(xl)}</b> je ${esc(ROLE_CS[ri.role])} ${esc(enz)}; <b>${esc(yl)}</b> je ${esc(ROLE_CS[rs.role])} ${esc(enz)}. Očekávané ${what} látky ${esc(yl)}${inducer ? ' (efekt indukce nastupuje během 1–2 týdnů a odeznívá podobně dlouho)' : ' (efekt inhibice nastupuje během hodin až dní)'}. Ověřte v SPC (bod 4.5) látky ${esc(yl)}, zda je nutná úprava dávky nebo alternativa.</p><div class="ev">Zdroj: ${esc(ri.source === 'FDA' ? 'FDA – Table of Substrates, Inhibitors and Inducers' : 'SPC / odborná literatura')}; ${esc(rs.source === 'FDA' ? 'FDA tabulka substrátů' : 'SPC / literatura')}</div>` });
        }
      }
      // 4. additive special classes
      for (const c of ['QT', 'SEROTONERGIC', 'CNS_DEPRESSANT', 'HYPERKALEMIA', 'NEPHROTOXIC', 'ANTICHOLINERGIC', 'BLEEDING']) {
        if (ca.has(c) && cb.has(c) && !covered.has(ADD_TAG[c])) findings.push({ pair: [i, j], sev: 'C', src: 'Aditivní účinek', title: `${esc(A.label)} × ${esc(B.label)}: ${esc(ADD_LABEL[c])}`, body: `<p>${esc(ADD_TEXT[c])}</p><div class="ev">Zařazení do skupiny: podle ATC a textu SPC (bod 4.4/4.5) obou látek.</div>` });
      }
      // 5. SPC text cross-matching
      for (const [X, Y, tx, cy] of [[A, B, texts[i], cb], [B, A, texts[j], ca]]) {
        if (!tx || !tx.text) continue;
        const pats = []; Y.keys.filter(k => !X.keys.includes(k)).forEach(k => pats.push(...stemPatterns(k)));
        if (Y.type === 'food') pats.push(...classPatterns(Y.key));
        const qs = findQuotes(tx.text, pats).filter(q => foodQuoteOk(Y, q));
        const cpats = []; const cx = itemClasses(X); cy.forEach(c => { if (cx.has(c) && X.keys.some(k => Y.keys.includes(k))) return; if (!c.startsWith('CYP') && !c.startsWith('PGP') && !/^(QT|SEROTONERGIC|CNS_DEPRESSANT|NEPHROTOXIC|HYPERKALEMIA|BLEEDING|ANTICHOLINERGIC)$/.test(c)) cpats.push(...classPatterns(c)); });
        const qc = qs.length < 3 ? findQuotes(tx.text, cpats).filter(q => !qs.some(z => z.sent === q.sent)) : [];
        const kq = findQuotes(tx.ki, pats).filter(q => foodQuoteOk(Y, q));
        if (!qs.length && !qc.length && !kq.length) continue;
        // přecitlivělost na látku nebo zkřížená reakce v 4.3 není kontraindikace kombinace
        const kqReal = kq.filter(q => !/hypersenzit|přecitliv|alerg|anafylakt|intoleranc/i.test(q.sent));
        const sev = kqReal.length ? 'X' : qs.length ? 'C' : 'I';
        findings.push({ pair: [i, j], sev, src: 'Text SPC', title: `SPC ${esc(tx.name)} – zmínka o ${esc(Y.label)}${qs.length ? '' : ' (skupina)'}`,
          body: `${kq.length ? '<div class="lab">Bod 4.3 Kontraindikace</div>' + kq.map(q => `<div class="quote">${highlight(q.sent, q.re)}</div>`).join('') : ''}${qs.length || qc.length ? '<div class="lab">Bod 4.5 Interakce</div>' : ''}${qs.concat(qc).map(q => `<div class="quote">${highlight(q.sent, q.re)}</div>`).join('')}<div class="ev">Citace ze schváleného SPC (${tx.src === 'ema' ? 'EMA' : 'SÚKL'}) přípravku ${esc(tx.name)}. Vyhledáno podle názvu látky/skupiny – přečtěte celý odstavec v kontextu.</div>` });
      }
    }
    findings.sort((a, b) => SEV_ORD[a.sev] - SEV_ORD[b.sev] || (a.pair[0] - b.pair[0]) || (a.pair[1] - b.pair[1]));
    return findings;
  }
  // "vylučuje se do (mateřského) mléka" = kojení, ne mléčné výrobky
  const NOT_FOOD = { FOOD_DAIRY: [/mateřsk|lidsk\w*\s+mlé|koj[eíi]|laktac|do\s+mléka/i, /mléčn|jogurt|sýr|vápník|kalci/i] };
  function foodQuoteOk(Y, q) { const r = Y.type === 'food' && NOT_FOOD[Y.key]; return !r || !(r[0].test(q.sent) && !r[1].test(q.sent)); }
  const ADD_TAG = { QT: 'QT', SEROTONERGIC: 'serotoninový syndrom', CNS_DEPRESSANT: 'CNS útlum', HYPERKALEMIA: 'hyperkalemie', NEPHROTOXIC: 'nefrotoxicita', ANTICHOLINERGIC: 'anticholinergní', BLEEDING: 'krvácení' };
  const ADD_LABEL = { QT: 'součet rizika prodloužení QT', SEROTONERGIC: 'součet serotonergního účinku', CNS_DEPRESSANT: 'součet útlumu CNS', HYPERKALEMIA: 'součet rizika hyperkalemie', NEPHROTOXIC: 'součet nefrotoxicity', ANTICHOLINERGIC: 'součet anticholinergní zátěže', BLEEDING: 'součet rizika krvácení' };
  const ADD_TEXT = { QT: 'Obě látky mohou prodlužovat QT interval. Riziko torsade de pointes roste s počtem QT-prodlužujících léčiv, při hypokalemii/hypomagnezemii, bradykardii, u žen a seniorů. Zvažte alternativu; při nutnosti kombinace EKG (QTc) před zahájením a po dosažení ustáleného stavu, korekce iontů.',
    SEROTONERGIC: 'Obě látky zvyšují serotonergní aktivitu – riziko serotoninového syndromu (neklid, třes, myoklonus, pocení, tachykardie, hypertermie). Kombinaci se pokud možno vyhněte nebo začínejte nejnižší dávkou a poučte pacienta o příznacích; při podezření ihned vysadit.',
    CNS_DEPRESSANT: 'Obě látky tlumí CNS – aditivní sedace, útlum dechu (zejména opioid + benzodiazepin/gabapentinoid), pády u seniorů, zhoršení řízení. Omezte počet CNS-aktivních léčiv, používejte nejnižší dávky, poučte o řízení a alkoholu.',
    HYPERKALEMIA: 'Obě látky zvyšují kalemii. Riziko roste při CKD, dehydrataci, diabetu a u seniorů. Kontrola K⁺ a kreatininu do 1–2 týdnů po zahájení/změně dávky a poté pravidelně; poučit o příznacích a dietě bohaté na draslík.',
    NEPHROTOXIC: 'Obě látky mohou zhoršit renální funkci – riziko akutního poškození ledvin, zvláště při dehydrataci, u seniorů a při CKD. Kontrola kreatininu/eGFR, dostatečná hydratace, zvažte přerušení při interkurentním onemocnění („sick day rules“).',
    ANTICHOLINERGIC: 'Obě látky mají anticholinergní účinek – kumulativní zátěž (zmatenost, retence moči, zácpa, suchost sliznic, tachykardie, pády, zhoršení kognice u seniorů). Zvažte snížení anticholinergní zátěže, zejména u pacientů nad 65 let.',
    BLEEDING: 'Obě látky zvyšují riziko krvácení. Zvažte nutnost kombinace, nejkratší možnou dobu, gastroprotekci (PPI) při kombinaci s NSAID/antiagregancii, kontrolu krevního obrazu/INR dle typu léčiv a poučení pacienta o příznacích krvácení.' };

  // ---------- interactions: view ----------
  const FOODS = [['FOOD_GRAPEFRUIT', 'Grapefruitová šťáva', 'citrus'], ['FOOD_ALCOHOL', 'Alkohol', 'wine'], ['FOOD_DAIRY', 'Mléčné výrobky / vápník současně s lékem', 'milk'], ['HERB_STJOHNSWORT', 'Třezalka (čaj, doplňky)', 'leaf']];
  // Výsledky, které si uživatel rozbalil (dvojice podle léků samotných, ne podle pořadí v seznamu): zůstanou rozbalené,
  // i když se stránka překreslí (další lék, přepínač). Na začátku je vše sbalené.
  const ixOpen = new Set();
  // kontrola interakcí pro statistiky vkládající stránky: nová kombinace, která zůstala zobrazená aspoň 2 s (rychlé
  // přidávání léků jeden po druhém se tak počítá jednou); odchází jen počet položek
  // ixTouched: uživatel v tomto otevření výběr změnil (nebo otevřel sdílený odkaz) – výběr obnovený z minula se nepočítá
  let ixReported = '', ixPending = 0, ixTouched = false;
  function reportCheck(sig, n) {
    clearTimeout(ixPending);
    if (!ixTouched) return;
    ixPending = setTimeout(() => {
      if (sig === ixReported || !/^#\/?interakce/.test(location.hash)) return;
      ixReported = sig;
      tellHost({ name: 'check', items: n });
    }, 2000);
  }
  let IX_RUN = 0;
  async function renderInteractions() {
    clearTimeout(ixPending);
    const nav = NAV, run = ++IX_RUN;
    const items = S.sel.map(selItem).filter(Boolean);
    const foodItems = Object.entries(S.food).filter(([k, v]) => v).map(([k]) => ({ type: 'food', key: k, label: classLabel(k), sub: 'potravina / bylina', keys: [], codes: [], atc: [] }));
    view.innerHTML = header('interakce', 'Kontrola interakcí', 'Sestavte kombinaci léků – vyhodnotí se všechny dvojice podle kurátorovaných pravidel, tabulek CYP/P-gp a textů SPC. Studijní pomůcka – není určena k rozhodování o léčbě konkrétního pacienta a nenahrazuje SPC.') + `
      <div class="ix-grid">
        <section class="card">
          <div class="card-h"><h2>${ic('pill', 'sm')}Kombinace léků <span class="count">${items.length}</span></h2></div>
          <div class="ix-search"><div class="field">${ic('search', 'sm')}<input id="ixq" type="search" placeholder="Přidat lék nebo látku…" autocomplete="off"/></div><div class="dropdown" id="ixdd"></div></div>
          <div class="sel-list">${items.map((it, i) => `<div class="sel-row"><span class="sq sm">${ic(it.type === 'substance' ? 'flask' : 'pill', 'sm')}</span><div class="main"><div class="t">${esc(it.label)}</div><div class="s">${esc(it.sub)}</div></div><button class="x-btn" data-rm="${i}" title="Odebrat" aria-label="Odebrat ${esc(it.label)}">${ic('x', 'sm')}</button></div>`).join('') || `<div class="small" style="padding:14px 2px">Zatím žádné léky. Začněte psát název přípravku nebo léčivé látky.</div>`}</div>
          <div class="switches">${FOODS.map(([k, l, i]) => `<label class="switch"><input type="checkbox" data-food="${k}" ${S.food[k] ? 'checked' : ''}/><span class="tr"></span>${ic(i, 'sm')}<span>${esc(l)}</span></label>`).join('')}</div>
          <div class="filters" style="margin-top:14px"><button class="btn sm" id="ixclear">${ic('trash', 'xs')}Vymazat vše</button><button class="btn sm" id="ixlink">${ic('link', 'xs')}Kopírovat odkaz</button></div>
        </section>
        <section class="card" id="ixres"><div class="empty"><span class="sq">${ic('zap')}</span>Přidejte alespoň dva léky – výsledky se zobrazí zde.</div></section>
      </div>`;
    const q = $('#ixq'), dd = $('#ixdd'); let sg = [], act = -1;
    const drawSug = () => {
      const r = suggest(q.value, { np: 7, ns: 3, all: false });
      sg = r.products.map(p => ({ type: 'product', id: p.i, label: pname(p), sub: prodSub(p) })).concat(r.substances.map(s => ({ type: 'substance', code: s.code, label: (s.cs || lc(s.inn)), sub: 'léčivá látka · ' + s.inn })));
      if (!sg.length) { dd.classList.remove('open'); return; }
      dd.innerHTML = sg.map((s, i) => `<div class="dd-item ${i === act ? 'active' : ''}" data-i="${i}"><span class="sq sm">${ic(s.type === 'substance' ? 'flask' : 'pill', 'sm')}</span><div><div class="t">${esc(s.label)}</div><div class="s">${esc(s.sub)}</div></div></div>`).join('');
      dd.classList.add('open');
      $$('.dd-item', dd).forEach(el => el.addEventListener('mousedown', e => { e.preventDefault(); pick(+el.dataset.i); }));
    };
    const pick = i => { const s = sg[i]; if (!s) return; addSelection(s.type === 'product' ? { type: 'product', id: s.id } : { type: 'substance', code: s.code }); renderInteractions().then(() => $('#ixq') && $('#ixq').focus()); };
    q.addEventListener('input', debounce(() => { act = 0; drawSug(); }, 100));
    q.addEventListener('keydown', e => { if (e.key === 'ArrowDown') { act = Math.min(act + 1, sg.length - 1); drawSug(); e.preventDefault(); } else if (e.key === 'ArrowUp') { act = Math.max(act - 1, 0); drawSug(); e.preventDefault(); } else if (e.key === 'Enter') { if (act >= 0) pick(act); e.preventDefault(); } else if (e.key === 'Escape') dd.classList.remove('open'); });
    q.addEventListener('blur', () => setTimeout(() => dd.classList.remove('open'), 150));
    $$('[data-rm]').forEach(b => b.onclick = () => { ixTouched = true; S.sel.splice(+b.dataset.rm, 1); saveSel(); renderInteractions(); });
    $$('[data-food]').forEach(c => c.onchange = () => { ixTouched = true; S.food[c.dataset.food] = c.checked; saveSel(); renderInteractions(); });
    $('#ixclear').onclick = () => { S.sel = []; S.food = { FOOD_GRAPEFRUIT: false, FOOD_ALCOHOL: false, FOOD_DAIRY: false, HERB_STJOHNSWORT: false }; ixOpen.clear(); saveSel(); renderInteractions(); };
    // sdílený odkaz vede na vkládající stránku (shareUrl + trasa) – samotná aplikace bez ní data nemá
    if (!HOST.shareUrl) $('#ixlink').style.display = 'none';
    $('#ixlink').onclick = () => {
      const over = S.sel.length > SEL_MAX_LINK, url = HOST.shareUrl + encodeURIComponent(selectionLink(SEL_MAX_LINK).slice(1));
      const done = msg => { const b = $('#ixlink'); if (!b) return; b.innerHTML = ic(msg && !/^Zkopírováno/.test(msg) ? 'info' : 'check', 'xs') + (msg || 'Zkopírováno'); setTimeout(() => { const b2 = $('#ixlink'); if (b2) b2.innerHTML = ic('link', 'xs') + 'Kopírovat odkaz'; }, 1800); };
      if (navigator.clipboard) navigator.clipboard.writeText(url).then(() => done(over ? 'Zkopírováno (prvních ' + SEL_MAX_LINK + ')' : ''), () => done('Kopírování nepovoleno'));
    };
    if (history.replaceState) history.replaceState(null, '', selectionLink());
    const all = items.concat(foodItems);
    if (all.length < 2) return;
    $('#ixres').innerHTML = loading('Vyhodnocuji kombinace…');
    let f;
    try { f = await analyse(all); } catch (e) { if (nav === NAV && run === IX_RUN) failPage(e, $('#ixres')); return; }
    if (nav !== NAV || run !== IX_RUN) return;   // výběr nebo stránka se mezitím změnily: platí novější výpočet
    reportCheck(all.map(it => it.type === 'product' ? 'p:' + it.id : it.type === 'substance' ? 's:' + it.code : 'f:' + it.key).sort().join('|'), all.length);
    const nPairs = all.length * (all.length - 1) / 2;
    if (!f.length) { $('#ixres').innerHTML = `<div class="empty"><span class="sq">${ic('shield')}</span>V dostupných zdrojích nebyla pro zadané kombinace nalezena žádná interakce.<br><span class="small">To neznamená, že interakce neexistuje – vždy ověřte bod 4.5 SPC.</span></div>`; return; }
    const groups = new Map();
    for (const x of f) { const k = x.pair.join('-'); if (!groups.has(k)) groups.set(k, { pair: x.pair, sev: x.sev, items: [] }); const g = groups.get(k); g.items.push(x); if (SEV_ORD[x.sev] < SEV_ORD[g.sev]) g.sev = x.sev; }
    const gl = [...groups.values()].sort((a, b) => SEV_ORD[a.sev] - SEV_ORD[b.sev] || a.pair[0] - b.pair[0] || a.pair[1] - b.pair[1]);
    const counts = {}; gl.forEach(g => counts[g.sev] = (counts[g.sev] || 0) + 1);
    const SRC_ORD = { 'Kurátorovaný přehled': 0, 'Duplicita': 1, 'Farmakokinetika (CYP/P-gp)': 2, 'Aditivní účinek': 3, 'Text SPC': 4 };
    const ik = it => it.type === 'product' ? 'p:' + it.id : it.type === 'substance' ? 's:' + it.code : 'f:' + it.key;
    const pk = g => [ik(all[g.pair[0]]), ik(all[g.pair[1]])].sort().join('|');
    $('#ixres').innerHTML = `<div class="card-h"><h2>${ic('zap', 'sm')}Výsledek <span class="count">${gl.length} z ${nPairs} dvojic s nálezem</span></h2><div class="legend">${Object.keys(SEV).map(k => counts[k] ? `<span class="sev ${k}">${k === 'I' ? 'i' : k} · ${counts[k]} ${SEV[k].toLowerCase()}</span>` : '').join('')}</div></div>` +
      gl.map(g => { const its = g.items.slice().sort((a, b) => SEV_ORD[a.sev] - SEV_ORD[b.sev] || (SRC_ORD[a.src] || 9) - (SRC_ORD[b.src] || 9));
        const key = pk(g), o = ixOpen.has(key);
        return `<div class="pair${o ? ' open' : ''}" data-pk="${esc(key)}"><div class="pair-h" role="button" tabindex="0" aria-expanded="${o}"><span class="sev ${g.sev}">${esc(SEV[g.sev])}</span><div class="main"><div class="t">${esc(all[g.pair[0]].label)}<small>×</small>${esc(all[g.pair[1]].label)}</div><div class="s">${its.map(x => x.title).slice(0, 2).join(' · ')}</div></div><span class="small">${its.length} ${its.length === 1 ? 'nález' : its.length < 5 ? 'nálezy' : 'nálezů'}</span>${ic('down', 'sm chv')}</div>
          <div class="pair-b">${its.map(x => `<div class="find"><div class="find-h"><span class="sev ${x.sev}">${x.sev === 'I' ? 'i' : x.sev}</span><b>${x.title}</b><span class="src">· ${esc(x.src)}</span></div>${x.body}</div>`).join('')}</div></div>`; }).join('') +
      `<div class="small" style="margin-top:10px">Vyhodnocení kombinuje kurátorovaný přehled interakcí (sestavený ze SPC a veřejných zdrojů, s automatickou kontrolou proti SPC), tabulky inhibitorů a induktorů CYP a P-gp, skupinová pravidla (ATC, SPC) a vyhledání zmínek ve schválených SPC. Studijní pomůcka: není určena k rozhodování o léčbě konkrétního pacienta a nenahrazuje úplné znění SPC.</div>`;
    $$('.pair-h').forEach(h => { const t = () => { const p = h.parentElement, o = p.classList.toggle('open'); h.setAttribute('aria-expanded', o); if (o) ixOpen.add(p.dataset.pk); else ixOpen.delete(p.dataset.pk); }; h.onclick = t; h.onkeydown = e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); t(); } }; });
  }

  // ---------- about ----------
  function renderAbout() {
    const m = S.meta;
    view.innerHTML = header('o-databazi', 'O databázi a zdrojích', 'Odkud data pocházejí, jak vznikají shrnutí a jak funguje kontrola interakcí') + `<section class="card about">
      <h3>Co databáze obsahuje</h3>
      <p>Všechny léčivé přípravky se stavem registrace <b>„R – registrovaný“</b> v Databázi léčivých přípravků SÚKL (otevřená data, sada DLP${esc(m.dlp_release.replace(/-/g, ''))}, zveřejněno ${esc(m.dlp_release_cs)}): <b>${fmtN(m.n_products)}</b> přípravků (název × složení), ${fmtN(m.n_variants)} variant (kódů SÚKL) a ${fmtN(m.n_substances)} léčivých látek. Přípravky dodávané do lékáren v posledních 6 měsících jsou označeny „Dodáván“. Neregistrované přípravky (individuální dovoz, specifické léčebné programy, potraviny pro zvláštní lékařské účely) zařazeny nejsou.</p>
      <h3>Zdroje textů SPC</h3>
      <ul><li><b>SÚKL</b> – měsíční sada schválených SPC (otevřená data) propojená s kódem přípravku. Zpracováno ${fmtN(m.n_spc_sukl)} dokumentů.</li>
      <li><b>EMA</b> – u centrálně registrovaných přípravků (registrační číslo EU/…) informace o přípravku, na které odkazuje SÚKL. Zpracováno ${fmtN(m.n_spc_ema)} dokumentů.</li>
      <li>Z každého SPC jsou převzaty body 2 a 4.1–4.9 beze změny znění (opraveny jsou jen znaky poškozené při převodu z PDF – např. horní indexy 10⁹/l, m², symboly ≥ ≤ ±). Bod 4.3 odpovídá absolutním kontraindikacím, bod 4.4 obsahuje relativní kontraindikace a situace vyžadující opatrnost.</li></ul>
      <h3>Jak vznikají shrnutí v bublinách</h3>
      <ul><li><b>Stručné shrnutí</b> – interpretace MedInter: zkrácený přepis bodů 4.1–4.9 daného SPC. Vzniká s pomocí AI výhradně z textu tohoto SPC a lékařem kontrolováno není. Automaticky se ověřuje, že každé číslo (dávka, věk, eGFR, interval) se v textu SPC vyskytuje; položky, které neprojdou, jsou vyřazeny. Kontrola nepostihne číslo vytržené ze souvislosti ani vynechanou informaci. Pro celé znění a úplnost informací je třeba se řídit oficiálním SPC na <a href="https://opendata.sukl.cz" target="_blank" rel="noopener">opendata.sukl.cz</a>.</li>
      <li><b>Výtah ze SPC</b> – u ostatních dokumentů věty převzaté doslovně z SPC (indikace, kontraindikace, věty s dávkou pod nadpisem z SPC, klíčová upozornění).</li>
      <li>U každé bubliny lze rozkliknout <b>celé znění</b> příslušného bodu SPC; vždy je k dispozici i odkaz na originální dokument SÚKL/EMA.</li></ul>
      <h3>Jak funguje kontrola interakcí</h3>
      <ul><li><b>Kurátorovaný přehled</b> (${fmtN(m.n_rules)} položek) klinicky významných interakcí ve VPL, sestavený s pomocí AI z bodů 4.3 a 4.5 SPC registrovaných přípravků a z veřejných zdrojů (tabulky FDA, ONC high-priority DDI list, kritéria STOPP/START v3 – viz citace níže). Každá položka prošla automatizovanou kontrolou proti textu SPC; nejde o lékařsky recenzovaný seznam.</li>
      <li><b>Farmakokinetická pravidla</b> – inhibitor/induktor × substrát pro CYP3A4, 2D6, 2C9, 2C19, 1A2, 2C8, 2B6 a P-gp.</li>
      <li><b>Aditivní účinky</b> – prodloužení QT, serotonergní účinek, útlum CNS, hyperkalemie, nefrotoxicita, anticholinergní zátěž, krvácení.</li>
      <li><b>Vyhledání v SPC</b> – zmínky o druhém léku nebo jeho skupině v bodech 4.5 a 4.3 SPC s citací.</li></ul>
      <h3>Citace zdrojů</h3>
      <ul><li><b>Klasifikace ATC</b> – WHO Collaborating Centre for Drug Statistics Methodology, Oslo (<a href="https://atcddd.fhi.no" target="_blank" rel="noopener">atcddd.fhi.no</a>). Kódy ATC a jejich názvy přebíráme z otevřených dat SÚKL.</li>
      <li><b>STOPP/START v3</b> – O'Mahony D, et al. STOPP/START criteria for potentially inappropriate prescribing in older people: version 3. Eur Geriatr Med. 2023;14(4):625–632. <a href="https://doi.org/10.1007/s41999-023-00777-y" target="_blank" rel="noopener">doi:10.1007/s41999-023-00777-y</a>. Licence <a href="https://creativecommons.org/licenses/by/4.0/" target="_blank" rel="noopener">CC BY 4.0</a>; v nástroji použito upraveně (vybraná kritéria přeložena, zkrácena a převedena na pravidla dvojic léků).</li>
      <li><b>FDA</b> – Drug Development and Drug Interactions: Table of Substrates, Inhibitors and Inducers (U.S. Food and Drug Administration, volné dílo).</li>
      <li><b>ONC high-priority DDI list</b> – Phansalkar S, et al. High-priority drug–drug interactions for use in electronic health records. J Am Med Inform Assoc. 2012;19(5):735–743.</li></ul>
      <h3>Podmínky užití</h3>
      <p>Otevřená data SÚKL lze užívat i komerčně s uvedením zdroje a bez změny významu – viz <a href="https://opendata.sukl.cz/?q=podminky-uziti-otevrenych-dat" target="_blank" rel="noopener">Podmínky užití otevřených dat SÚKL</a>. Informace o přípravku EMA lze reprodukovat i komerčně s uvedením EMA jako zdroje (© European Medicines Agency). SÚKL ani EMA tento nástroj neschvalují ani nepodporují. Nástroj MedInter je vzdělávací pomůcka určená výhradně ke studiu; není určen k rozhodování o léčbě konkrétního pacienta a nenahrazuje aktuální SPC. Aktualizováno ${esc(m.built_cs)}.</p>
    </section>`;
  }

  // ---------- zamčená obrazovka (bez dat / nepřihlášen / bez přístupu) ----------
  function renderLock(kind) {
    document.documentElement.classList.add('locked');
    const L = HOST.links || {};
    const T = {
      standalone: ['MedInter', 'Databáze registrovaných léčivých přípravků, stručná shrnutí SPC a kontrola lékových interakcí. Tato aplikace je určena k vložení do jiné webové stránky, která jí poskytuje data; samostatně otevřená nic nezobrazuje.', '', '', 'pill'],
      login: ['Přihlaste se', 'Pro použití aplikace je potřeba se přihlásit.', 'Přihlásit se', L.login, 'lock'],
      denied: ['Přístup není k dispozici', 'Tento účet nemá k aplikaci přístup.', 'Více informací', L.info, 'lock'],
      network: ['Databázi se nepodařilo načíst', 'Zkontrolujte připojení k internetu a zkuste to znovu.', 'Zkusit znovu', '', 'info']
    }[kind] || ['Databázi se nepodařilo načíst', '', 'Zkusit znovu', '', 'info'];
    const btn = T[3] ? `<a class="btn primary" href="${esc(T[3])}" target="_blank" rel="noopener">${esc(T[2])}</a>`
      : kind === 'network' ? `<button class="btn primary" id="lockRetry">${esc(T[2])}</button>` : '';
    view.innerHTML = `<section class="card lock"><span class="sq">${ic(T[4])}</span><h1>${esc(T[0])}</h1><p>${esc(T[1])}</p>${btn}</section>`;
    const rb = $('#lockRetry'); if (rb) rb.addEventListener('click', () => location.reload());
  }
  // Soubor, který stránka potřebuje, se nepodařilo načíst. Odhlášení nebo výpadek sítě → zamčená obrazovka; jinak (po úspěšném
  // spuštění nejčastěji chybějící soubor) jen zpráva na místě dané stránky – ne celé „bez přístupu“.
  function failPage(e, box) {
    if (e instanceof AccessError && (e.kind === 'login' || e.kind === 'network' || !S.ready)) return renderLock(e.kind);
    const el = box || view; if (!el) return;
    el.innerHTML = `<div class="card empty"><span class="sq">${ic('info')}</span>Tuto část databáze se nepodařilo načíst.<br><span class="small">Zkuste to prosím znovu za chvíli.</span></div>`;
  }
  window.addEventListener('unhandledrejection', e => { if (e.reason instanceof AccessError) { e.preventDefault(); failPage(e.reason); } });

  // ---------- init ----------
  async function init() {
    if (!isFramed) return renderLock('standalone');
    try {
      const cur = await readJSON(await fetchFile('current.json', { cache: 'no-store' }));
      if (!cur || !/^[\w.-]+$/.test(cur.release || '')) throw new Error('current.json: chybí "release"');
      RELEASE = cur.release;
      const [meta0, idx, subs, atc, ix] = await Promise.all([getJSON('meta.json'), getJSON('index.json'), getJSON('substances.json'), getJSON('atc.json'), getJSON('interactions.json')]);
      S.meta = meta0; S.products = idx.p; S.substances = subs; S.atc = atc; S.ix = ix;
      S.products = (Array.isArray(S.products) ? S.products : []).filter(p => p && typeof p.i === 'string' && typeof p.n === 'string');
      S.products.forEach(p => { ['s', 'a', 'an', 'v', 'h', 'st', 'sp'].forEach(f => { if (!Array.isArray(p[f])) p[f] = []; }); if (typeof p.t !== 'string' && !Array.isArray(p.t)) p.t = ''; S.byId[p.i] = p; });
      S.substances.forEach(s => { S.subByCode[s.code] = s; S.subByKey[s.inn] = s; });
      S.ix.enzymes = [...new Set(S.ix.cyp.map(r => r.enzyme))];
      S.sel = cleanSel(load('sel', []), SEL_MAX_STORED); const fd = load('food', {}); Object.keys(S.food).forEach(k => { S.food[k] = !!(fd && fd[k] === true); });
      S.ready = true;
      route();
    } catch (e) {
      if (e instanceof AccessError) return renderLock(e.kind);
      view.innerHTML = `<div class="card empty"><span class="sq">${ic('info')}</span>Databázi se nepodařilo načíst (${esc(e.message)}).</div>`;
    }
  }
  init();
})();
