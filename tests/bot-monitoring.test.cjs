const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { createBotMonitoring, sanitize } = require('../bot-monitoring.cjs');

function fixture(options = {}) {
  let clock = 1791488660000;
  const client = new EventEmitter();
  Object.assign(client, { ws: { ping: 101 }, isReady: () => true, user: { tag: 'NEW SHOP' }, destroy() {} });
  const calls = [], saved = [], lifecycle = [];
  const webhook = 'https://discord.com/api/webhooks/123/hidden-token';
  const logger = createBotMonitoring({ client, webhookUrl: webhook, installConsole: false, startTimers: false,
    now: () => clock, env: { BOT_TOKEN: 'TOPSECRET123', RENDER_GIT_COMMIT: 'abcd1234' },
    store: { load: async () => options.previous || null, save: async data => saved.push(data) },
    fetchImpl: async (url, request) => {
      calls.push({ url: String(url), method: request.method, body: JSON.parse(request.body) });
      lifecycle.push('sent');
      return { ok: true, status: 200, json: async () => ({ id: 'message-1' }) };
    },
    processRef: { exit: code => lifecycle.push(`exit:${code}`) },
    ...options.overrides,
  });
  return { client, logger, calls, saved, lifecycle, advance: ms => { clock += ms; } };
}

test('first boot and previous sessions have distinct start/restart messages', async () => {
  const first = fixture(); await first.logger.boot;
  assert.match(first.calls[0].body.embeds[0].title, /START PROCESU/);
  assert.equal(first.saved[0].phase, 'starting');
  const next = fixture({ previous: { sessionId: 'old-id', phase: 'stopped', reason: 'SIGTERM', version: 'old-version' } });
  await next.logger.boot;
  assert.match(next.calls[0].body.embeds[0].title, /RESTART/);
  assert.match(next.calls[0].body.embeds[0].description, /old-id.*SIGTERM.*nowa wersja/s);
});

test('unconfirmed termination never invents a crash reason', async () => {
  const f = fixture({ previous: { sessionId: 'old', phase: 'running' } }); await f.logger.boot;
  assert.match(f.calls[0].body.embeds[0].description, /powód resetu nieznany/);
});

test('status updates edit one message instead of sending heartbeat spam', async () => {
  const f = fixture(); await f.logger.boot;
  await f.logger.updateStatus(); f.advance(300000); await f.logger.updateStatus();
  assert.equal(f.calls[1].method, 'POST');
  assert.equal(f.calls[2].method, 'PATCH');
  assert.match(f.calls[2].url, /\/messages\/message-1/);
  assert.equal(f.calls[2].body.allowed_mentions.parse.length, 0);
});

test('errors are counted, secrets removed and repeated warnings deduplicated', async () => {
  const f = fixture(); await f.logger.boot;
  await f.logger.captureError(new Error('TOPSECRET123 Bearer private-token'));
  await f.logger.captureError('same failure'); await f.logger.captureError('same failure');
  assert.equal(f.logger.snapshot().errorCount, 3);
  assert.equal(f.calls.length, 3);
  assert.doesNotMatch(JSON.stringify(f.calls), /TOPSECRET123|private-token/);
  f.advance(16 * 60000);
  assert.match(f.logger.snapshot().status, /Działa prawidłowo/);
});

test('webhook URLs and ping without connection are never misleading', () => {
  assert.equal(sanitize('https://discord.com/api/webhooks/123/abc'), '[WEBHOOK UKRYTY]');
  const f = fixture(); f.client.ws.ping = -1; f.client.isReady = () => false;
  assert.equal(f.logger.snapshot().ping, null);
  assert.match(f.logger.snapshot().status, /Brak połączenia/);
});

test('first shard connection is not incorrectly logged as recovered', async () => {
  const f = fixture(); await f.logger.boot;
  f.client.emit('shardReady', 0); await Promise.resolve();
  assert.equal(f.calls.length, 1);
  f.client.emit('shardDisconnect', { code: 1006 }, 0);
  f.client.emit('shardReconnecting', 0);
  f.client.emit('shardResume', 0, 5);
  await f.logger.updateStatus();
  const titles = f.calls.map(c => c.body.embeds[0].title);
  assert.ok(titles.some(t => t.includes('UTRATA POŁĄCZENIA')));
  assert.ok(titles.some(t => t.includes('PONOWNE ŁĄCZENIE')));
  assert.ok(titles.some(t => t.includes('POŁĄCZENIE ODZYSKANE')));
});

test('HTTP emits only failure and recovery changes, not every OK check', async () => {
  let fail = false;
  const f = fixture({ overrides: { monitorUrl: 'https://bot.example/health', fetchImpl: async (url, request) => {
    if (String(url).includes('bot.example')) {
      if (fail) throw new Error('network timeout');
      return { ok: true, status: 200 };
    }
    seen.push(JSON.parse(request.body));
    return { ok: true, json: async () => ({ id: '1' }) };
  } } });
  const seen = []; await f.logger.boot;
  await f.logger.checkHttp(); await f.logger.checkHttp();
  fail = true; await f.logger.checkHttp(); await f.logger.checkHttp();
  fail = false; await f.logger.checkHttp();
  assert.equal(seen.filter(p => /HTTP · PROBLEM/.test(p.embeds[0].title)).length, 1);
  assert.equal(seen.filter(p => /HTTP · ODPOWIEDŹ/.test(p.embeds[0].title)).length, 1);
});

test('shutdown saves the session and sends its log before flush and process exit', async () => {
  const f = fixture(); await f.logger.boot;
  await f.logger.shutdown('SIGTERM', 0, () => f.lifecycle.push('flushed'));
  assert.equal(f.saved.at(-1).phase, 'stopped');
  assert.match(f.calls.at(-1).body.embeds[0].title, /ZAMYKANIE/);
  assert.deepEqual(f.lifecycle.slice(-3), ['sent', 'flushed', 'exit:0']);
  await f.logger.shutdown('SIGINT');
  assert.equal(f.lifecycle.filter(x => x.startsWith('exit')).length, 1);
});

test('fatal exception logs the failure then exits with code 1', async () => {
  const f = fixture(); await f.logger.boot;
  await f.logger.crash(new Error('crash'));
  assert.ok(f.calls.some(c => c.body.embeds[0].title.includes('AWARIA')));
  assert.equal(f.lifecycle.at(-1), 'exit:1');
});
