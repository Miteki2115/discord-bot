function createDiscordRateLimitGate({ now = Date.now, fallbackMs = 60_000 } = {}) {
  let globalUntil = 0;
  const routes = new Map();
  let lastLimitedAt = null;
  function snapshot() {
    return { global_retry_after_ms: Math.max(0, globalUntil - now()), last_limited_at: lastLimitedAt };
  }
  function assertAllowed(key) {
    const remaining = Math.max(globalUntil, routes.get(key) || 0) - now();
    if (remaining <= 0) return;
    const error = new Error('Discord API: trwa przerwa po HTTP 429.');
    Object.assign(error, { status: 429, code: 429, retryAfterMs: remaining, localCooldown: true });
    throw error;
  }
  function block(key, ms, global = false) {
    const delay = Number.isFinite(ms) && ms > 0 ? ms : fallbackMs;
    const until = now() + delay;
    if (global) globalUntil = Math.max(globalUntil, until);
    else routes.set(key, Math.max(routes.get(key) || 0, until));
    lastLimitedAt = new Date(now()).toISOString();
    if (routes.size > 200) {
      for (const [route, expiry] of routes) if (expiry <= now()) routes.delete(route);
    }
    return delay;
  }
  function observe(key, response, data, { globalOnly = false } = {}) {
    if (response.status !== 429) return 0;
    const header = response.headers?.get?.('retry-after');
    const seconds = [data?.retry_after, header, response.headers?.get?.('x-ratelimit-reset-after')]
      .filter(value => value !== null && value !== undefined && value !== '')
      .map(Number).filter(value => Number.isFinite(value) && value >= 0);
    const dateMs = header && !Number.isFinite(Number(header)) ? Date.parse(header) - now() : 0;
    const ms = Math.max(0, dateMs || 0, ...seconds.map(value => value * 1000));
    const global = data?.global === true || response.headers?.get?.('x-ratelimit-global') === 'true' ||
      response.headers?.get?.('x-ratelimit-scope') === 'global' ||
      /blocked from accessing our API temporarily|exceeding global rate limits/i.test(data?.message || '');
    // discord.js already queues individual REST buckets. Never promote a
    // channel rename/category limit to a pause of unrelated bot operations.
    if (globalOnly && !global) return 0;
    return block(key, ms > 0 ? ms + 100 : fallbackMs, global);
  }
  return { snapshot, assertAllowed, observe, block };
}
module.exports = { createDiscordRateLimitGate };
