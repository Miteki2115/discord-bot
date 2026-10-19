const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '..', 'index.cjs'), 'utf8');
function fixture(messages) {
  let target;
  const channel = { name: 'czy-legit', isTextBased: () => true, messages };
  const context = vm.createContext({
    client: { channels: { cache: { find: predicate => predicate(channel) ? channel : undefined } } },
    console: { warn() {}, log() {} },
    CZY_LEGIT_CHANNEL_MARKER: 'czy-legit', CZY_LEGIT_MESSAGE_MARKER: 'LEGIT SHOP',
    getCzyLegitReactionCount: message => message.count || 0,
    syncCzyLegitMessage: async message => { target = message; },
  });
  vm.runInContext(source.slice(source.indexOf('async function initializeCzyLegitCounter('), source.indexOf('async function handleCzyLegitReactionChange(')), context);
  return { run: context.initializeCzyLegitCounter, target: () => target };
}

test('paginated fetchPins finds old pinned posts and refreshes their missing reaction data', async () => {
  const pages = [], refreshed = [];
  const f = fixture({
    fetchPins: async options => {
      pages.push(options);
      return options.before === undefined
        ? { items: [{ message: { id: 'other', content: 'Other pin' }, pinnedTimestamp: 2000 }], hasMore: true }
        : { items: [{ message: { id: 'legit', content: 'LEGIT SHOP' }, pinnedTimestamp: 1000 }], hasMore: false };
    },
    fetch: async options => {
      if (!options.message) return new Map([['recent', { id: 'recent', content: 'LEGIT SHOP', count: 2 }]]);
      refreshed.push(options.message);
      return { id: options.message, content: 'LEGIT SHOP', count: options.message === 'legit' ? 50 : 2 };
    },
  });
  await f.run();
  assert.equal(pages.length, 2);
  assert.equal(pages[1].before, 2000);
  assert.equal(f.target().id, 'legit');
  assert.ok(refreshed.includes('legit'));
});

test('failed pin fetch still checks recent channel messages', async () => {
  const f = fixture({
    fetchPins: async () => { throw new Error('API unavailable'); },
    fetch: async options => options.message
      ? { id: 'recent', content: 'LEGIT SHOP', count: 5 }
      : new Map([['recent', { id: 'recent', content: 'LEGIT SHOP' }]]),
  });
  await f.run();
  assert.equal(f.target().id, 'recent');
});
