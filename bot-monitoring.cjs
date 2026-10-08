const crypto = require('node:crypto');
const util = require('node:util');

const STYLES = {
  start: ['🚀', 'START PROCESU', 0x539eff],
  restart: ['🔄', 'RESTART · NOWA SESJA', 0x9b8aff],
  ready: ['🟢', 'BOT GOTOWY', 0x36d399],
  stop: ['⏹️', 'ZAMYKANIE PROCESU', 0xf6bd60],
  error: ['❌', 'BŁĄD ZADANIA', 0xf87171],
  warning: ['⚠️', 'OSTRZEŻENIE DISCORDA', 0xf6bd60],
  fatal: ['🚨', 'AWARIA · PROCES ZOSTANIE ZAMKNIĘTY', 0xef4444],
  disconnect: ['🔌', 'UTRATA POŁĄCZENIA', 0xf87171],
  reconnect: ['🔄', 'PONOWNE ŁĄCZENIE', 0xf6bd60],
  resume: ['🟢', 'POŁĄCZENIE ODZYSKANE', 0x36d399],
  httpDown: ['⚠️', 'HTTP · PROBLEM Z ODPOWIEDZIĄ', 0xf6bd60],
  httpUp: ['🟢', 'HTTP · ODPOWIEDŹ PRZYWRÓCONA', 0x36d399],
  status: ['📡', 'PANEL STANU · AKTUALIZOWANY', 0x539eff],
};

function formatUptime(ms) {
  const seconds = Math.max(0, Math.floor(ms / 1000));
  return `${Math.floor(seconds / 86400)}d ${Math.floor(seconds / 3600) % 24}h ${Math.floor(seconds / 60) % 60}m ${seconds % 60}s`;
}

function sanitize(value, secrets = []) {
  let text = String(value ?? 'Brak szczegółów');
  for (const secret of secrets.filter(v => typeof v === 'string' && v.length > 7)) text = text.split(secret).join('[UKRYTO]');
  return text.replace(/https?:\/\/[^\s]*\/api(?:\/v\d+)?\/webhooks\/\d+\/[^\s]+/gi, '[WEBHOOK UKRYTY]')
    .replace(/Bearer\s+\S+/gi, 'Bearer [UKRYTO]').replace(/`/g, 'ˋ').slice(0, 1700);
}

function createBotMonitoring({ client, events = {}, webhookUrl, monitorUrl, footer, store,
  env = process.env, fetchImpl = global.fetch, now = Date.now, timers = global,
  installConsole = true, processRef = process, startTimers = true }) {
  const startedAt = now();
  const sessionId = crypto.randomUUID().slice(0, 8);
  const version = String(env.RENDER_GIT_COMMIT || env.BOT_VERSION || 'lokalna').slice(0, 12);
  const secrets = [env.BOT_TOKEN, env.SUPABASE_ANON_KEY, env.SUPABASE_SERVICE_ROLE_KEY, webhookUrl];
  const originalError = console.error.bind(console);
  let queue = Promise.resolve(), queued = 0, stopping = false, fatal = false, statusMessageId = null;
  let errorCount = 0, lastErrorTime = null, httpState = 'unknown', httpDetail = 'Oczekiwanie na pierwszy pomiar';
  let httpBusy = false, previous = null, storeAvailable = false;
  let recentErrors = [], pingHistory = [];
  const lastEvents = new Map(), intervals = [], shards = new Map();

  function snapshot() {
    const ping = Number(client.ws?.ping);
    const ready = Boolean(client.isReady?.());
    recentErrors = recentErrors.filter(t => now() - t < 15 * 60 * 1000);
    const status = !ready ? '🔴 Brak połączenia z Discordem'
      : recentErrors.length || httpState === 'down' || ping > 200 ? '🟠 Wymaga uwagi' : '🟢 Działa prawidłowo';
    return { status, statusColor: !ready ? 0xf87171 : status.startsWith('🟠') ? 0xf6bd60 : 0x36d399,
      ping: Number.isFinite(ping) && ping >= 0 ? ping : null,
      avgPing: pingHistory.length ? Math.round(pingHistory.reduce((a, b) => a + b, 0) / pingHistory.length) : null,
      uptime: formatUptime(now() - startedAt), errorCount, lastErrorTime, httpState, httpDetail,
      sessionId, version, guilds: client.guilds?.cache?.size || 0,
      users: client.users?.cache?.size || 0, channels: client.channels?.cache?.size || 0 };
  }

  function payload(kind, detail) {
    const [icon, title, color] = STYLES[kind] || STYLES.error;
    const s = snapshot();
    let brand = { text: '© 2026 New Shop' };
    try { if (typeof footer === 'function') brand = footer(); } catch { /* Startup can precede brand initialization. */ }
    return { username: 'New Shop · System', allowed_mentions: { parse: [] }, embeds: [{
      author: { name: 'NEW SHOP / DZIENNIK SYSTEMU' }, title: `${icon} ${title}`, color: kind === 'status' ? s.statusColor : color,
      description: `**${sanitize(detail, secrets)}**`,
      fields: [
        { name: 'STAN DISCORDA', value: s.status, inline: false },
        { name: 'CZAS DZIAŁANIA', value: s.uptime, inline: true },
        { name: 'PING', value: s.ping === null ? 'Brak pomiaru' : `${s.ping} ms`, inline: true },
        { name: 'BŁĘDY W SESJI', value: String(errorCount), inline: true },
        { name: 'SESJA / WERSJA', value: `\`${sessionId}\` / \`${version}\``, inline: false },
        { name: 'URUCHOMIONY', value: `<t:${Math.floor(startedAt / 1000)}:F>`, inline: false },
      ], footer: brand,
      timestamp: new Date(now()).toISOString(),
    }] };
  }

  async function request(url, method, body) {
    const controller = new AbortController();
    const timeout = timers.setTimeout(() => controller.abort(), 4000);
    try {
      const response = await fetchImpl(url, { method, headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body), signal: controller.signal });
      if (!response.ok) throw new Error(`Monitoring: HTTP ${response.status}`);
      return await response.json();
    } finally { timers.clearTimeout(timeout); }
  }

  function send(kind, detail, { dedupeKey, editStatus = false } = {}) {
    if (!webhookUrl || queued >= 40) return Promise.resolve(false);
    const key = dedupeKey || (kind === 'status' ? null : `${kind}:${detail}`);
    if (key && lastEvents.has(key) && now() - lastEvents.get(key) < 60000) return Promise.resolve(false);
    if (key) lastEvents.set(key, now());
    if (lastEvents.size > 200) lastEvents.delete(lastEvents.keys().next().value);
    const body = payload(kind, detail);
    queued++;
    queue = queue.then(async () => {
      try {
        let url = new URL(webhookUrl);
        if (editStatus && statusMessageId) url.pathname += `/messages/${statusMessageId}`;
        else url.searchParams.set('wait', 'true');
        let result;
        try { result = await request(url, editStatus && statusMessageId ? 'PATCH' : 'POST', body); }
        catch (err) {
          if (!editStatus || !statusMessageId || !String(err.message).endsWith('HTTP 404')) throw err;
          statusMessageId = null;
          url = new URL(webhookUrl); url.searchParams.set('wait', 'true');
          result = await request(url, 'POST', body);
        }
        if (editStatus && result?.id) statusMessageId = result.id;
        return true;
      } catch (err) { originalError('[MONITORING]', sanitize(err.message, secrets)); return false; }
      finally { queued--; }
    });
    return queue;
  }

  async function bounded(operation, ms = 3000) {
    let timer;
    try { return await Promise.race([Promise.resolve().then(operation), new Promise((_, reject) => {
      timer = timers.setTimeout(() => reject(new Error('Timeout zapisu/odczytu sesji')), ms);
    })]); } finally { timers.clearTimeout(timer); }
  }
  async function savePhase(phase, reason) {
    if (!storeAvailable || !store) return;
    try { await bounded(() => store.save({ sessionId, startedAt, version, phase, reason, updatedAt: now() })); }
    catch (err) { originalError('[MONITORING/SESJA]', sanitize(err.message, secrets)); }
  }

  const boot = (async () => {
    if (store) {
      try { previous = await bounded(() => store.load()); storeAvailable = true; }
      catch (err) { originalError('[MONITORING/SESJA]', sanitize(err.message, secrets)); }
    }
    const detail = previous
      ? `Poprzednia sesja: ${previous.sessionId}. ${previous.phase === 'stopped' ? `Zamknięcie: ${previous.reason || 'brak powodu'}.` : 'Poprzedni proces nie potwierdził zamknięcia — powód resetu nieznany.'}${previous.version !== version ? ' Uruchamiana jest nowa wersja.' : ''}`
      : `Uruchamianie procesu. ${storeAvailable ? 'Pierwsza zarejestrowana sesja.' : 'Historia restartów niedostępna; nie określam powodu startu.'}`;
    await send(previous ? 'restart' : 'start', detail);
    await savePhase('starting', 'Uruchamianie');
  })();

  function captureError(...args) {
    errorCount++; lastErrorTime = now(); recentErrors.push(now()); recentErrors = recentErrors.slice(-100);
    const detail = sanitize(args.map(a => a instanceof Error ? `${a.name}: ${a.message}\n${a.stack || ''}`
      : typeof a === 'string' ? a : util.inspect(a, { depth: 2 })).join(' '), secrets);
    return send('error', detail, { dedupeKey: `error:${detail}` });
  }
  if (installConsole) console.error = (...args) => { originalError(...args); void captureError(...args); };

  async function updateStatus() {
    if (stopping) return;
    await boot;
    const ping = Number(client.ws?.ping);
    if (Number.isFinite(ping) && ping >= 0) { pingHistory.push(ping); pingHistory = pingHistory.slice(-12); }
    const s = snapshot();
    return send('status', `${s.status}\nHTTP: ${s.httpDetail}\nTen panel jest odświeżany co 5 minut. Ważne zdarzenia pojawiają się osobno.`, { editStatus: true });
  }

  async function checkHttp() {
    if (!monitorUrl || httpBusy || stopping) return;
    httpBusy = true;
    const start = now(), controller = new AbortController();
    const timer = timers.setTimeout(() => controller.abort(), 10000);
    try {
      const response = await fetchImpl(monitorUrl, { signal: controller.signal });
      await response.body?.cancel?.();
      httpDetail = `HTTP ${response.status} · ${now() - start} ms`;
      const next = response.ok ? 'up' : 'down';
      if (next !== httpState && (next === 'down' || httpState === 'down'))
        await send(next === 'up' ? 'httpUp' : 'httpDown', httpDetail);
      httpState = next;
    } catch (err) {
      httpDetail = err.name === 'AbortError' ? 'Brak odpowiedzi przez 10 sekund' : sanitize(err.message, secrets);
      if (httpState !== 'down') await send('httpDown', httpDetail);
      httpState = 'down';
    } finally { timers.clearTimeout(timer); httpBusy = false; }
  }

  client.on(events.ClientReady || 'clientReady', async () => {
    await boot;
    if (stopping) return;
    await savePhase('running', 'Połączono z Discordem');
    await send('ready', `Zalogowano jako ${client.user?.tag || 'New Shop'}. Komendy i zdarzenia Discorda są dostępne.`);
    await checkHttp(); await updateStatus();
  });
  client.on(events.ShardDisconnect || 'shardDisconnect', (event, shard) => {
    shards.set(shard, 'down');
    void send('disconnect', `Shard ${shard} · kod ${event?.code ?? 'nieznany'}. ${sanitize(event?.reason || 'Połączenie zostało przerwane.', secrets)}`, { dedupeKey: `disconnect:${shard}` });
  });
  client.on(events.Warn || 'warn', message => { void send('warning', sanitize(message, secrets)); });
  client.on(events.ShardReconnecting || 'shardReconnecting', shard => {
    shards.set(shard, 'connecting');
    void send('reconnect', `Shard ${shard} · czekam na ponowne połączenie. To reconnect, a nie restart procesu.`, { dedupeKey: `reconnect:${shard}` });
  });
  client.on(events.ShardResume || 'shardResume', (shard, replayed) => {
    shards.set(shard, 'up');
    void send('resume', `Shard ${shard} · wznowiono sesję. Odtworzone zdarzenia: ${replayed || 0}.`);
  });
  client.on(events.ShardReady || 'shardReady', shard => {
    const previousState = shards.get(shard);
    shards.set(shard, 'up');
    if (previousState === 'down' || previousState === 'connecting')
      void send('resume', `Shard ${shard} · nawiązano połączenie.`, { dedupeKey: `shard-ready:${shard}` });
  });
  if (startTimers) {
    intervals.push(timers.setInterval(() => { void updateStatus(); }, 5 * 60 * 1000));
    intervals.push(timers.setInterval(() => { void checkHttp(); }, 10 * 60 * 1000));
  }

  async function shutdown(reason, exitCode = 0, flush = () => {}) {
    if (stopping) return;
    stopping = true;
    intervals.forEach(id => timers.clearInterval(id));
    try {
      // The signal handler must await the log; async work in "exit" cannot run.
      await bounded(async () => { await boot; await savePhase('stopped', reason); await send('stop', `${reason}. Kod zakończenia: ${exitCode}.`); }, 5000);
    } catch (err) { originalError('[MONITORING/ZAMYKANIE]', sanitize(err.message, secrets)); }
    finally { try { flush(); } finally { try { client.destroy?.(); } finally { processRef.exit(exitCode); } } }
  }
  async function crash(err, flush) {
    if (fatal) return;
    fatal = true;
    errorCount++; lastErrorTime = now();
    try { await bounded(() => send('fatal', sanitize(err?.stack || err, secrets)), 3000); }
    catch (error) { originalError('[MONITORING/AWARIA]', sanitize(error.message, secrets)); }
    await shutdown('Nieobsłużony wyjątek krytyczny', 1, flush);
  }
  function dispose() {
    intervals.forEach(id => timers.clearInterval(id));
    if (installConsole) console.error = originalError;
  }
  return { boot, snapshot, captureError, updateStatus, checkHttp, shutdown, crash, dispose, payload };
}

module.exports = { createBotMonitoring, formatUptime, sanitize };
