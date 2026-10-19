const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '..', 'index.cjs'), 'utf8');

function fixture(fetchMessages, fetchUser = async () => ({ username: 'Kolczasty' })) {
  let saves = 0;
  let now = 1000;
  const context = vm.createContext({
    console: { error() {} }, Date: { now: () => now },
    REP_CHANNEL_ID: 'rep',
    scheduleSavePersistentState() { saves++; },
    client: {
      channels: { fetch: async () => ({ isTextBased: () => true, messages: { fetch: fetchMessages } }) },
      users: { fetch: fetchUser },
    },
  });
  vm.runInContext(
    source.slice(source.indexOf('const sellerTicketCounts ='), source.indexOf('const DAILY_LEGIT_CHANNEL_ID =')) +
    source.slice(source.indexOf('function getCachedSellerTicketCount('), source.indexOf('async function handleDeletedLegitRepMessage(')), context);
  return {
    context,
    counts: vm.runInContext('sellerTicketCounts', context),
    checked: vm.runInContext('sellerTicketCountCheckedAt', context),
    saves: () => saves,
    elapse: ms => { now += ms; },
  };
}
function batch(messages) {
  const result = new Map(messages.map(message => [message.id, message]));
  result.last = () => [...result.values()].at(-1);
  return result;
}
const rep = (id, seller = '<@42>') => ({ id: String(id), content: `+rep ${seller} ZAKUP 20 PLN INNE` });

test('claim replaces the stale persisted 918 with the complete 1022 from history', async () => {
  const all = Array.from({ length: 1022 }, (_, i) => rep(2000 - i));
  const f = fixture(async ({ limit, before }) => batch(all.filter(m => !before || BigInt(m.id) < BigInt(before)).slice(0, limit)));
  f.counts.set('42', 918);
  assert.equal(await f.context.getSellerTicketCount('42'), 1022);
  assert.equal(await f.context.getSellerTicketCount('42', true), 1022);
});

test('failed second page preserves the old count and does not mark it verified', async () => {
  let calls = 0;
  const f = fixture(async () => {
    if (++calls === 2) throw new Error('Discord unavailable');
    return batch(Array.from({ length: 100 }, (_, i) => rep(1000 - i)));
  });
  f.counts.set('42', 918);
  await assert.rejects(f.context.getSellerTicketCount('42', true), /Discord unavailable/);
  assert.equal(f.counts.get('42'), 918);
  assert.equal(f.checked.has('42'), false);
  assert.equal(f.saves(), 0);
});

test('verified counts refresh after five minutes and immediate recalculation bypasses cache', async () => {
  let messages = [rep(100)];
  const f = fixture(async () => batch(messages));
  assert.equal(await f.context.getSellerTicketCount('42'), 1);
  messages.push(rep(99));
  assert.equal(await f.context.getSellerTicketCount('42'), 1);
  f.elapse(5 * 60 * 1000);
  assert.equal(await f.context.getSellerTicketCount('42'), 2);
  messages.push(rep(98));
  assert.equal(await f.context.getSellerTicketCount('42', true), 3);
});

test('incremental completions agree with a full recalculation, including anonymous reps', async () => {
  const messages = [rep(100)];
  const f = fixture(async () => batch(messages));
  await f.context.getSellerTicketCount('42');
  messages.push(rep(99));
  f.context.creditSellerTicket('42');
  assert.equal(await f.context.getSellerTicketCount('42'), 2);
  assert.equal(await f.context.getSellerTicketCount('42', true), 2);
});

test('deleting or editing history invalidates a verified count', async () => {
  const messages = [rep(100), rep(99)];
  const f = fixture(async () => batch(messages));
  await f.context.getSellerTicketCount('42');
  messages.pop();
  f.context.invalidateSellerTicketCounts();
  assert.equal(await f.context.getSellerTicketCount('42'), 1);
});

test('concurrent requests share a single scan', async () => {
  let calls = 0;
  const f = fixture(async () => { calls++; return batch([rep(100)]); });
  assert.deepEqual(await Promise.all([f.context.getSellerTicketCount('42'), f.context.getSellerTicketCount('42', true)]), [1, 1]);
  assert.equal(calls, 1);
});

test('a new completion during the scan cannot be overwritten by a stale snapshot', async () => {
  const f = fixture(async () => { f.context.creditSellerTicket('42'); return batch([rep(100)]); });
  f.counts.set('42', 918);
  await assert.rejects(f.context.getSellerTicketCount('42'), /zmieniła się/);
  assert.equal(f.counts.get('42'), 919);
  assert.equal(f.saves(), 0);
});

test('counts mentions and nickname aliases without counting mentions of a different seller', async () => {
  const f = fixture(async () => batch([rep(100), rep(99, '<@!42>'), rep(98, '@kolczasty'), rep(97, '<@43>')]));
  assert.equal(await f.context.getSellerTicketCount('42'), 3);
});

test('failed user lookup does not silently omit historical nickname reps', async () => {
  const f = fixture(async () => batch([rep(100)]), async () => { throw new Error('network'); });
  f.counts.set('42', 918);
  await assert.rejects(f.context.getSellerTicketCount('42'), /nicku/);
  assert.equal(f.counts.get('42'), 918);
});

test('repeated history pages are rejected rather than looped or double counted', async () => {
  const page = batch(Array.from({ length: 100 }, (_, i) => rep(1000 - i)));
  const f = fixture(async () => page);
  await assert.rejects(f.context.getSellerTicketCount('42'), /starszych/);
  assert.equal(f.saves(), 0);
});
