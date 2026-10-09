const test = require('node:test');
const assert = require('node:assert/strict');
const { parseDeliveryAmount, deliverySummary, createTicketDelivery } = require('../ticket-delivery.cjs');
const { canUseSlashCommand, SELLER_ROLE_ID } = require('../command-access.cjs');

function fixture(initial = {}) {
  let saved = structuredClone(initial);
  let failSave = false, failSend = false, failDelete = false;
  let id = 0;
  const messages = new Map();
  const channel = { id: 'ticket', guild: { id: 'guild' },
    send: async payload => {
      if (failSend) throw new Error('send failed');
      const message = { id: String(++id), payload, delete: async () => messages.delete(message.id) };
      messages.set(message.id, message);
      return message;
    },
    messages: {
      edit: async (id, payload) => {
        if (!messages.has(id)) throw Object.assign(new Error('missing'), { code: 10008 });
        messages.get(id).payload = payload;
      },
      delete: async id => {
        if (failDelete) throw new Error('delete failed');
        if (!messages.delete(id)) throw Object.assign(new Error('missing'), { code: 10008 });
      },
    },
  };
  const make = () => createTicketDelivery({
    load: async () => structuredClone(saved),
    save: async data => { if (failSave) throw new Error('database offline'); saved = structuredClone(data); },
    payload: state => deliverySummary(state),
  });
  return { channel, messages, make, message: () => ({ author: { bot: false }, guild: channel.guild, channel }),
    saved: () => saved,
    failSave: value => { failSave = value; }, failSend: value => { failSend = value; }, failDelete: value => { failDelete = value; } };
}

test('accepts 2m, 570k and Polish decimals, rejects invalid or fractional currency', () => {
  for (const [raw, amount] of [['2m', 2000000], ['570k', 570000], ['1,43m', 1430000], ['2 000 000', 2000000], ['0.001k', 1]]) {
    assert.equal(parseDeliveryAmount(raw), amount);
  }
  for (const raw of ['-2m', '0', 'Infinity', 'NaN', '0.5', '570k extra', '1e9', '9007199254740992']) {
    assert.throws(() => parseDeliveryAmount(raw));
  }
});

test('reported purchase sums instalments and survives restart without losing its panel', async () => {
  const f = fixture(), manager = f.make();
  await manager.change(f.channel, 'start', '2m', '1');
  const result = await manager.change(f.channel, 'nadalem', '570k', '2');
  assert.equal(result.delivered, 570000);
  assert.equal(result.total - result.delivered, 1430000);
  const restarted = f.make();
  await restarted.change(f.channel, 'nadalem', '430k', '3');
  assert.equal(f.saved().ticket.delivered, 1000000);
  assert.equal(f.messages.size, 1);
  await assert.rejects(restarted.change(f.channel, 'start', '2m', '4'), /już licznik/);
});

test('each sixth human message moves the panel; bot posts do not count', async () => {
  const f = fixture(), manager = f.make();
  await manager.change(f.channel, 'start', '2m', '1');
  const original = f.saved().ticket.messageId;
  await manager.onMessage({ ...f.message(), author: { bot: true } });
  for (let i = 0; i < 5; i++) await manager.onMessage(f.message());
  assert.equal(f.saved().ticket.messageId, original);
  await manager.onMessage(f.message());
  assert.notEqual(f.saved().ticket.messageId, original);
  assert.equal(f.saved().ticket.count, 0);
  assert.equal(f.messages.size, 1);
});

test('concurrent payments sum exactly once and over-delivery is rejected', async () => {
  const f = fixture(), manager = f.make();
  await manager.change(f.channel, 'start', '2m', '1');
  await Promise.all([manager.change(f.channel, 'nadalem', '570k', '2'), manager.change(f.channel, 'nadalem', '430k', '3')]);
  await manager.change(f.channel, 'nadalem', '570k', '2');
  assert.equal(f.saved().ticket.delivered, 1000000);
  await assert.rejects(manager.change(f.channel, 'nadalem', '2m', '4'), /przekracza/);
  await manager.change(f.channel, 'nadalem', '1m', '5');
  const completed = deliverySummary(f.saved().ticket);
  assert.match(completed, /100%/);
  assert.ok(completed.endsWith('`' + '━'.repeat(36) + '` **100%**'));
  assert.ok(!completed.includes('Postęp:') && !completed.includes('Cała kwota'));
});

test('database failure cannot acknowledge a payment or alter the saved sum', async () => {
  const f = fixture(), manager = f.make();
  await manager.change(f.channel, 'start', '2m', '1');
  f.failSave(true);
  await assert.rejects(manager.change(f.channel, 'nadalem', '570k', '2'), /database offline/);
  assert.equal(f.saved().ticket.delivered, 0);
  f.failSave(false);
  await manager.change(f.channel, 'nadalem', '570k', '2');
  assert.equal(f.saved().ticket.delivered, 570000);
});

test('failed panel send preserves the payment; retrying the same interaction never adds twice', async () => {
  const f = fixture(), manager = f.make();
  f.failSend(true);
  await assert.rejects(manager.change(f.channel, 'start', '2m', '1'), error => error.deliverySaved);
  f.failSend(false);
  await manager.change(f.channel, 'nadalem', '570k', '2');
  assert.equal(f.messages.size, 1);
  assert.equal(f.saved().ticket.delivered, 570000);
});

test('missing message is recreated; failed deletion rolls back the replacement panel', async () => {
  const f = fixture(), manager = f.make();
  await manager.change(f.channel, 'start', '2m', '1');
  f.messages.clear();
  await manager.change(f.channel, 'nadalem', '570k', '2');
  assert.equal(f.messages.size, 1);
  f.failDelete(true);
  for (let i = 0; i < 5; i++) await manager.onMessage(f.message());
  await assert.rejects(manager.onMessage(f.message()), /delete failed/);
  assert.equal(f.messages.size, 1);
  f.failDelete(false);
  await manager.onMessage(f.message());
  assert.equal(f.messages.size, 1);
  assert.equal(f.saved().ticket.count, 0);
});

test('deleted ticket removes persistent counter and command is limited to seller/owner', async () => {
  const f = fixture(), manager = f.make();
  await manager.change(f.channel, 'start', '2m', '1');
  await manager.remove('ticket');
  assert.deepEqual(f.saved(), {});
  const interaction = { guildId: 'guild', guild: { ownerId: 'owner' }, user: { id: 'seller' }, commandName: 'ile-brakuje', member: { roles: [SELLER_ROLE_ID] } };
  assert.equal(canUseSlashCommand(interaction), true);
  assert.equal(canUseSlashCommand({ ...interaction, member: { roles: [] } }), false);
  assert.equal(canUseSlashCommand({ ...interaction, user: { id: 'owner' }, member: { roles: [] } }), true);
});
