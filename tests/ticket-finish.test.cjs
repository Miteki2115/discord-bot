const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const { isGuildOwner, isActiveSeller } = require('../command-access.cjs');
const source = fs.readFileSync(path.join(__dirname, '..', 'index.cjs'), 'utf8');
function gate() {
  let release;
  return { promise: new Promise(resolve => { release = resolve; }), release: () => release() };
}
function fixture(overrides = {}) {
  const events = [], pending = new Map(), locks = new Set();
  let id = 0;
  const context = vm.createContext({
    console: { log() {}, error() {} }, isGuildOwner, isActiveSeller,
    isTicketChannel: channel => channel?.id === 'ticket',
    MessageFlags: { Ephemeral: 64 }, ticketOwners: new Map(),
    ticketFinishInFlight: locks, pendingTicketClose: pending, ticketRepMessages: new Map(),
    resolveTicketOwnerId: async () => { events.push('owner'); return 'customer'; },
    parsePLN: value => parseInt(value, 10),
    db: { addUserSpent: async () => events.push('spent'), addUserPurchase: async () => events.push('purchase') },
    syncUserSpentRoles: async () => events.push('roles'),
    buildPendingLegitCheckPayload: () => ({ components: ['panel'] }),
    scheduleSavePersistentState: () => events.push('save'),
    hasUserReactedInCzyLegit: async () => { events.push('pings'); return true; },
    hasUserPublishedOpinion: async () => true,
    ticketCategoryManager: { move: async () => events.push('move') },
    sendTicketLogEntry: async () => events.push('log'), COLOR_YELLOW: 1,
    ...overrides,
  });
  vm.runInContext(source.slice(source.indexOf('async function handleTicketZakonczCommand('), source.indexOf('// ----------------- /anonim handler')), context);
  function interaction() {
    return {
      user: { id: 'seller', username: 'sprzedawca' }, member: { roles: ['1350786945944391733'] },
      guildId: 'guild', guild: { id: 'guild', ownerId: 'owner', channels: { fetch: async () => null } },
      channel: { id: 'ticket', send: async payload => { events.push(payload.components ? 'panel' : 'message'); return { id: String(++id) }; } },
      options: { getString: key => key === 'typ' ? 'zakup' : 'INNE', getInteger: () => 35 },
      async deferReply(payload) { assert.equal(payload.flags[0], 64); events.push('defer'); this.deferred = true; },
      async reply(payload) { events.push('reply'); this.result = payload; },
      async editReply(payload) { assert.equal(this.deferred, true); events.push('edit'); this.result = payload; },
    };
  }
  return { context, events, pending, locks, interaction };
}

test('seller with raw role IDs is acknowledged before slow owner/database/role requests', async () => {
  const blocked = gate();
  const entered = gate();
  const f = fixture({ resolveTicketOwnerId: async () => { f.events.push('owner'); entered.release(); await blocked.promise; return 'customer'; } });
  const interaction = f.interaction();
  const run = f.context.handleTicketZakonczCommand(interaction);
  await entered.promise;
  assert.deepEqual(f.events, ['defer', 'owner']);
  blocked.release(); await run;
  assert.match(interaction.result.content, /Poprawnie użyto/);
  assert.equal(f.pending.get('ticket').awaitingRep, true);
  assert.ok(f.events.indexOf('defer') < f.events.indexOf('spent'));
});

test('parallel finish requests never double the purchase or client spending', async () => {
  const blocked = gate();
  const f = fixture({ resolveTicketOwnerId: async () => { await blocked.promise; return 'customer'; } });
  const first = f.context.handleTicketZakonczCommand(f.interaction());
  await Promise.resolve(); await Promise.resolve();
  const second = f.interaction();
  await f.context.handleTicketZakonczCommand(second);
  assert.match(second.result.content, /już przetwarzane/);
  blocked.release(); await first;
  assert.equal(f.events.filter(e => e === 'spent').length, 1);
  assert.equal(f.events.filter(e => e === 'purchase').length, 1);
  const retry = f.interaction();
  await f.context.handleTicketZakonczCommand(retry);
  assert.match(retry.result.content, /już oczekuje/);
  assert.equal(f.events.filter(e => e === 'spent').length, 1);
});

test('missing ticket owner produces a private final response and frees the lock', async () => {
  const f = fixture({ resolveTicketOwnerId: async () => null });
  const interaction = f.interaction();
  await f.context.handleTicketZakonczCommand(interaction);
  assert.match(interaction.result.content, /zidentyfikować/);
  assert.equal(f.locks.size, 0);
  assert.equal(f.events.includes('spent'), false);
});

test('pending +rep state exists before any slow optional pings', async () => {
  const blocked = gate();
  const f = fixture({ hasUserReactedInCzyLegit: async () => {
    assert.equal(f.pending.get('ticket').awaitingRep, true);
    await blocked.promise;
    return true;
  } });
  const interaction = f.interaction();
  const run = f.context.handleTicketZakonczCommand(interaction);
  for (let i = 0; i < 30 && !interaction.result; i++) await Promise.resolve();
  assert.match(interaction.result.content, /Poprawnie użyto/);
  assert.equal(f.events.includes('save'), true);
  blocked.release(); await run;
});

test('owner without seller roles is accepted; suspended sellers remain denied', async () => {
  const f = fixture();
  const owner = f.interaction(); owner.user.id = 'owner'; owner.member = {};
  await f.context.handleTicketZakonczCommand(owner);
  assert.match(owner.result.content, /Poprawnie użyto/);
  const denied = f.interaction(); denied.member.roles.push('1537090439239442483');
  await f.context.handleTicketZakonczCommand(denied);
  assert.match(denied.result.content, /Brak wymaganych/);
});

test('failed panel send does not add purchase or spending, report success, or leave a lock', async () => {
  const f = fixture();
  const interaction = f.interaction();
  interaction.channel.send = async () => { throw new Error('Missing permissions'); };
  await assert.rejects(f.context.handleTicketZakonczCommand(interaction), /panelu/);
  assert.equal(f.events.includes('spent'), false);
  assert.equal(f.events.includes('purchase'), false);
  assert.equal(f.pending.size, 0);
  assert.equal(f.locks.size, 0);
  assert.equal(interaction.result, undefined);
});
