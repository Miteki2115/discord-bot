const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '..', 'index.cjs'), 'utf8').replace(/\r\n/g, '\n');
class Embed {
  setColor(color) { this.color = color; return this; }
  setDescription(description) { this.description = description; return this; }
}
function fixture(cached = null) {
  const sent = [], edits = [], owners = new Map();
  let scans = 0;
  const context = vm.createContext({
    EmbedBuilder: Embed, COLOR_BLUE: 1, console: { warn() {} },
    ticketOwners: owners, channelId: 'channel', claimerId: 'seller',
    opts: {}, ticketData: { userId: 'client', claimedBy: 'seller' },
    getCachedSellerTicketCount: () => cached,
    getSellerTicketCount: async () => { scans++; return 1022; },
    formatTicketsPlural: () => 'ticketów', scheduleSavePersistentState() {},
    ch: { send: async payload => { sent.push(payload); return { id: 'claim' }; } },
  });
  const start = source.indexOf('    const publicClaimerLabel =', source.indexOf('async function ticketClaimCommon('));
  const end = source.indexOf('\n    try {\n      if (ticketData.ticketTypeLabel', start);
  assert.ok(start > 0 && end > start);
  vm.runInContext(`async function publishClaim() { ${source.slice(start, end)} }`, context);
  vm.runInContext(source.slice(source.indexOf('async function refreshTicketClaimStatistics('), source.indexOf('function getPingRolesForTicketType(')), context);
  const message = { id: 'claim', edit: async payload => edits.push(payload) };
  return { context, sent, edits, owners, message, scans: () => scans };
}

test('claim publishes immediately without fetching history when stats are unverified', async () => {
  const f = fixture();
  f.context.getSellerTicketCount = () => { throw new Error('Claim must not wait for history'); };
  await f.context.publishClaim();
  assert.equal(f.owners.get('channel').lastClaimMsgId, 'claim');
  assert.match(f.sent[0].embeds[0].description, /Ticket został przejęty/);
  assert.match(f.sent[0].embeds[0].description, /aktualizowanie/);
  assert.equal(f.scans(), 0);
});

test('verified count, including zero, is used immediately without a history scan', async () => {
  for (const count of [0, 1022]) {
    const f = fixture(count);
    await f.context.publishClaim();
    assert.match(f.sent[0].embeds[0].description, new RegExp(`\\*\\*${count}\\*\\*`));
    assert.equal(f.scans(), 0);
  }
});

test('background result updates only the original message and does not ping the client again', async () => {
  const f = fixture();
  await f.context.publishClaim();
  await f.context.refreshTicketClaimStatistics(f.message, 'channel', 'seller', '<@seller>');
  assert.match(f.edits[0].embeds[0].description, /1022/);
  assert.equal(f.edits[0].allowedMentions.parse.length, 0);
  assert.equal(f.sent.length, 1);
});

test('unclaim or a newer claim prevents stale background edits', async () => {
  for (const owner of [{ claimedBy: null, lastClaimMsgId: 'claim' }, { claimedBy: 'other', lastClaimMsgId: 'new' }]) {
    const f = fixture();
    f.owners.set('channel', owner);
    await f.context.refreshTicketClaimStatistics(f.message, 'channel', 'seller', '<@seller>');
    assert.equal(f.edits.length, 0);
  }
});

test('failed background recount leaves the ticket claimed and never fabricates a count', async () => {
  const f = fixture();
  await f.context.publishClaim();
  f.context.getSellerTicketCount = async () => { throw new Error('HTTP 429'); };
  await f.context.refreshTicketClaimStatistics(f.message, 'channel', 'seller', '<@seller>');
  assert.equal(f.owners.get('channel').claimedBy, 'seller');
  assert.match(f.edits[0].embeds[0].description, /chwilowo niedostępna/);
});
