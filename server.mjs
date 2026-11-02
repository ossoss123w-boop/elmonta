import Fastify from 'fastify';
import { Innertube } from 'youtubei.js';
import { Readable } from 'node:stream';
import { execFile, spawn } from 'node:child_process';
import { promisify } from 'node:util';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const execFileAsync = promisify(execFile);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = Fastify({ logger: false });

const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)';
const STREAM_TTL = 90 * 60 * 1000;
const searchCache = new Map();
const resolveCache = new Map();
const streamCache = new Map();
const inflight = new Map();

console.log('Iniciando motores de audio...');
let yt = null;
try {
  yt = await Innertube.create({ retrieve_player: true });
  console.log('✓ Innertube optimizado.');
} catch (e) {
  console.warn('Innertube warning:', e.message);
}

/* ---------- utilidades de velocidad ---------- */
const withTimeout = (p, ms) => Promise.race([p, new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), ms))]);

// máx. 3 resoluciones a la vez (yt-dlp es pesado)
let running = 0;
const waiting = [];
const limit = fn => new Promise((res, rej) => {
  const run = async () => {
    running++;
    try { res(await fn()); } catch (e) { rej(e); }
    finally { running--; if (waiting.length) waiting.shift()(); }
  };
  running < 3 ? run() : waiting.push(run);
});

// Vía rápida: youtubei.js (~0.3 s). No necesita lanzar Python.
async function viaInnertube(id) {
  if (!yt) throw new Error('sin innertube');
  for (const client of ['IOS', 'ANDROID', 'MWEB']) {
    try {
      let info;
      try { info = await yt.getBasicInfo(id, { client }); }
      catch (_) { info = await yt.getBasicInfo(id, client); }
      let f;
      try { f = info.chooseFormat({ type: 'audio', quality: 'best', format: 'mp4' }); }
      catch (_) { f = info.chooseFormat({ type: 'audio', quality: 'best' }); }
      const url = f.url || await f.decipher(yt.session.player);
      if (url) return url;
    } catch (_) {}
  }
  throw new Error('innertube sin URL');
}

// Plan B: yt-dlp (más lento, pero muy fiable)
async function viaYtDlp(id) {
  const { stdout } = await execFileAsync('yt-dlp', [
    `https://www.youtube.com/watch?v=${id}`,
    '-g', '-f', 'ba[ext=m4a]/ba[ext=webm]/ba',
    '--extractor-args', 'youtube:player_client=android',
    '--no-playlist', '--no-warnings'
  ], { timeout: 9000 });
  const u = stdout.trim().split('\n')[0];
  if (!u.startsWith('http')) throw new Error('sin URL');
  return u;
}

const METHODS = { innertube: [viaInnertube, 5000], ytdlp: [viaYtDlp, 10000] };

// Devuelve la URL directa del audio. Reutiliza caché y evita resolver dos veces el mismo id.
async function getStream(id, skip = new Set()) {
  const c = streamCache.get(id);
  if (c && c.exp > Date.now() && !skip.has(c.via)) return c;
  const key = id + '|' + [...skip].join(',');
  if (inflight.has(key)) return inflight.get(key);
  const p = limit(async () => {
    for (const via of Object.keys(METHODS)) {
      if (skip.has(via)) continue;
      try {
        const [fn, ms] = METHODS[via];
        const url = await withTimeout(fn(id), ms);
        const v = { url, via, exp: Date.now() + STREAM_TTL };
        streamCache.set(id, v);
        return v;
      } catch (_) {}
    }
    return null;
  }).finally(() => inflight.delete(key));
  inflight.set(key, p);
  return p;
}
setInterval(() => { const n = Date.now(); for (const [k, v] of streamCache) if (v.exp < n) streamCache.delete(k); }, 10 * 60 * 1000);

/* ---------- rutas ---------- */
app.get('/', async (req, reply) => {
  const htmlPath = path.join(__dirname, 'index.html');
  if (!fs.existsSync(htmlPath)) return reply.status(404).send('index.html no encontrado');
  reply.type('text/html').send(fs.readFileSync(htmlPath, 'utf-8'));
});
app.get('/favicon.ico', async (req, reply) => reply.code(204).send());

// Trending inicial para el Home
const INITIAL_TRENDING = [
  { id: '1b8fP0Lw0nI', title: 'Monaco', artist: 'Bad Bunny', album: 'Nadie sabe lo que va a pasar mañana', durationSec: 267, artworkUrl: 'https://images.unsplash.com/photo-1514525253161-7a46d19cd819?w=300&h=300&fit=crop' },
  { id: 'q94rXw12Q0s', title: 'Nos Perdone', artist: 'Morad', album: 'Reinsertado', durationSec: 335, artworkUrl: 'https://images.unsplash.com/photo-1614613535308-eb5fbd3d2c17?w=300&h=300&fit=crop' },
  { id: 'KJQP7kiw5Fk', title: 'VALKYRIE', artist: 'Beny Jr', album: 'VALKYRIE', durationSec: 148, artworkUrl: 'https://images.unsplash.com/photo-1518709268805-4e9042af9f23?w=300&h=300&fit=crop' },
  { id: 'JGwWNGJdvx8', title: 'WHERE SHE GOES', artist: 'Bad Bunny', album: 'Un Verano Sin Ti', durationSec: 231, artworkUrl: 'https://images.unsplash.com/photo-1508700115892-45ecd05ae2ad?w=300&h=300&fit=crop' },
  { id: 'fJ9rUzIMcZQ', title: 'Lo Niego', artist: 'Morad', album: 'M.D.L.R', durationSec: 206, artworkUrl: 'https://images.unsplash.com/photo-1470225620780-dba8ba36b745?w=300&h=300&fit=crop' },
  { id: 'kXYiU_JCYtU', title: 'Columbia', artist: 'Quevedo', album: 'Buenas Noches', durationSec: 186, artworkUrl: 'https://images.unsplash.com/photo-1493225457124-a3eb161ffa5f?w=300&h=300&fit=crop' }
];
app.get('/api/trending', async () => INITIAL_TRENDING);

// Búsqueda con caché en RAM + calentamiento de los 3 primeros resultados
app.get('/api/search', async (req) => {
  const query = (req.query.q || '').trim();
  if (!query) return [];

  const cacheKey = query.toLowerCase();
  if (searchCache.has(cacheKey)) return searchCache.get(cacheKey);

  let songs = [];
  if (yt) {
    try {
      const res = await yt.music.search(query, { type: 'song' });
      const rawList = res.songs?.contents || res.songs || res.contents || [];
      songs = rawList.map(s => {
        const artist = Array.isArray(s.artists)
          ? s.artists.map(a => a.name).join(', ')
          : (s.author?.name || 'Desconocido');
        const thumb = s.thumbnails?.[s.thumbnails.length - 1]?.url
                   || s.thumbnail?.contents?.[0]?.url
                   || '';
        return {
          id: s.id,
          title: s.title?.text || s.title || 'Sin título',
          artist,
          album: s.album?.name || '',
          durationSec: Number(s.duration?.seconds) || 0,
          artworkUrl: thumb
        };
      }).filter(s => Boolean(s.id) && (!s.durationSec || s.durationSec >= 45));
    } catch (e) {}

    if (!songs.length) {
      try {
        const genRes = await yt.search(query, { type: 'video' });
        const vids = genRes.videos || genRes.results || [];
        songs = vids.map(v => ({
          id: v.id,
          title: v.title?.text || v.title || '',
          artist: v.author?.name || 'Desconocido',
          album: 'YouTube',
          durationSec: Number(v.duration?.seconds) || 0,
          artworkUrl: v.thumbnails?.[0]?.url || ''
        })).filter(s => Boolean(s.id) && (!s.durationSec || s.durationSec >= 45));
      } catch (e) {}
    }
  }

  if (songs.length) {
    searchCache.set(cacheKey, songs);
    if (searchCache.size > 200) searchCache.delete(searchCache.keys().next().value);
    songs.slice(0, 3).forEach(s => getStream(s.id).catch(() => {})); // listo para sonar al instante
  }
  return songs;
});

// Pre-resolver una canción (la web lo llama al pasar el ratón / precargar)
app.get('/api/warm/:id', async (req, reply) => {
  const { id } = req.params;
  if (/^[a-zA-Z0-9_\-]{6,16}$/.test(id)) getStream(id).catch(() => {});
  return reply.code(204).send();
});

// Resolver dinámico para canciones de Spotify
app.get('/api/resolve', async (req, reply) => {
  const q = (req.query.q || '').trim();
  if (!q) return reply.status(400).send({ error: 'Query requerida' });

  const cKey = q.toLowerCase();
  if (resolveCache.has(cKey)) return { id: resolveCache.get(cKey) };

  try {
    if (yt) {
      let id = null;
      const res = await yt.music.search(q, { type: 'song' });
      const list = res.songs?.contents || res.songs || res.contents || [];
      if (list[0]?.id) id = list[0].id;
      if (!id) {
        const gen = await yt.search(q, { type: 'video' });
        const vids = gen.videos || gen.results || [];
        if (vids[0]?.id) id = vids[0].id;
      }
      if (id) {
        resolveCache.set(cKey, id);
        getStream(id).catch(() => {});
        return { id };
      }
    }
  } catch (e) {}

  return reply.status(404).send({ error: 'No encontrado' });
});

// Importador completo de Spotify sin límite de temas
app.get('/api/spotify', async (req, reply) => {
  const spotifyUrl = req.query.url;
  if (!spotifyUrl) return reply.status(400).send({ error: 'URL requerida' });

  const match = spotifyUrl.match(/playlist\/([a-zA-Z0-9]+)/);
  if (!match) return reply.status(400).send({ error: 'Enlace no válido' });
  const playlistId = match[1];

  let name = 'Spotify Playlist';
  let cover = '';
  let tracks = [];

  try {
    const tokenRes = await fetch('https://open.spotify.com/get_access_token?reason=transport&productType=web_player', {
      headers: { 'User-Agent': UA }
    });
    if (tokenRes.ok) {
      const tokenData = await tokenRes.json();
      const accessToken = tokenData.accessToken;
      if (accessToken) {
        const plRes = await fetch(`https://api.spotify.com/v1/playlists/${playlistId}?fields=name,images,tracks.total`, {
          headers: { 'Authorization': `Bearer ${accessToken}` }
        });
        if (plRes.ok) {
          const plData = await plRes.json();
          name = plData.name || name;
          cover = plData.images?.[0]?.url || '';
          const total = plData.tracks?.total || 0;

          for (let offset = 0; offset < Math.min(total, 500); offset += 100) {
            const tracksRes = await fetch(`https://api.spotify.com/v1/playlists/${playlistId}/tracks?offset=${offset}&limit=100&fields=items(track(name,artists(name),duration_ms,album(images)))`, {
              headers: { 'Authorization': `Bearer ${accessToken}` }
            });
            if (!tracksRes.ok) break;
            const tracksData = await tracksRes.json();
            for (const item of (tracksData.items || [])) {
              if (!item?.track) continue;
              tracks.push({
                title: item.track.name,
                artist: item.track.artists?.map(a => a.name).join(', ') || 'Desconocido',
                durationSec: Math.round((item.track.duration_ms || 0) / 1000),
                artworkUrl: item.track.album?.images?.[0]?.url || cover
              });
            }
          }
        }
      }
    }
  } catch (e) {}

  if (!tracks.length) {
    try {
      const resp = await fetch(`https://open.spotify.com/embed/playlist/${playlistId}`, { headers: { 'User-Agent': UA } });
      const html = await resp.text();
      const jsonMatch = html.match(/<script id="__NEXT_DATA__" type="application\/json">([\s\S]*?)<\/script>/);
      if (jsonMatch) {
        const data = JSON.parse(jsonMatch[1]);
        const entity = data?.props?.pageProps?.state?.data?.entity;
        name = entity?.title || name;
        cover = entity?.coverArt?.sources?.[0]?.url || cover;
        tracks = (entity?.trackList || []).map(t => ({
          title: t.title,
          artist: t.subtitle || 'Desconocido',
          durationSec: Math.round((t.duration || 0) / 1000),
          artworkUrl: cover
        }));
      }
    } catch (err) {}
  }

  return { name, cover, tracks };
});

// STREAMING: soporta Range (necesario para mover la barra de reproducción)
app.get('/api/stream/:id', async (req, reply) => {
  const { id } = req.params;
  if (!/^[a-zA-Z0-9_\-]{6,16}$/.test(id)) return reply.status(400).send({ error: 'ID inválido' });

  const range = req.headers.range || 'bytes=0-';
  const ac = new AbortController();
  req.raw.on('close', () => ac.abort()); // si el navegador cancela, cortamos la descarga
  const skip = new Set();

  for (let i = 0; i < 2; i++) {
    const s = await getStream(id, skip);
    if (!s) break;
    try {
      const up = await fetch(s.url, { headers: { Range: range, 'User-Agent': UA }, signal: ac.signal });
      if (up.ok || up.status === 206) {
        reply.status(up.status).header('Accept-Ranges', 'bytes').header('Cache-Control', 'private, max-age=3600');
        for (const h of ['content-range', 'content-length', 'content-type']) {
          const v = up.headers.get(h);
          if (v) reply.header(h, v);
        }
        return reply.send(Readable.fromWeb(up.body));
      }
    } catch (e) {
      if (ac.signal.aborted) return reply.code(204).send();
    }
    skip.add(s.via);
    streamCache.delete(id);
  }

  // Último recurso: tubería directa (sin saltar en la barra)
  try {
    const proc = spawn('yt-dlp', [`https://www.youtube.com/watch?v=${id}`, '-o', '-', '-f', 'ba[ext=webm]/ba', '--no-playlist', '--quiet']);
    reply.header('Content-Type', 'audio/webm');
    req.raw.on('close', () => { try { proc.kill('SIGKILL'); } catch (_) {} });
    return reply.send(proc.stdout);
  } catch (err) {
    return reply.status(500).send({ error: 'Fallo al transmitir' });
  }
});

try {
  await app.listen({ port: 3000, host: '0.0.0.0' });
  console.log('\n🚀 Tonal Server listo: http://localhost:3000\n');
} catch (err) {
  console.error(err);
  process.exit(1);
}
