const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

// Evaluate the production helpers without logging the bot in or loading secrets.
const source = fs.readFileSync(path.join(__dirname, '..', 'index.cjs'), 'utf8');
const parser = source.slice(source.indexOf('const LEGIT_REP_SERVERS ='), source.indexOf('function legitRepMatchesPendingTicket('));
const anonStart = source.indexOf('async function sendAnonRep(');
const anon = source.slice(anonStart, source.indexOf('\n}', anonStart) + 2);
const sent = [];
const context = vm.createContext({
  console,
  client: { user: { id: 'bot', displayAvatarURL: () => 'avatar' } },
});
vm.runInContext(parser + '\n' + anon, context);
const parse = context.parseLegitRepContent;

test('accepts the reported 102 PLN INNE legit check', () => {
  const rep = parse('+rep @Kolczasty ZAKUP 102 PLN INNE');
  assert.equal(rep.amount, 102);
  assert.equal(rep.server, 'INNE');
  assert.equal(rep.seller, '@Kolczasty');
});

test('accepts every selectable category with a real Discord mention', () => {
  for (const server of vm.runInContext('LEGIT_REP_SERVERS', context)) {
    assert.equal(parse(`+rep <@1305200545979437129> ZAKUP 102 PLN ${server}`).server, server);
  }
});

test('INNE also supports sales and invitation rewards', () => {
  for (const verb of ['SPRZEDAŻ', 'SPRZEDAZ', 'WRĘCZYŁ NAGRODĘ', 'WRECZYL NAGRODE']) {
    assert.ok(parse(`+rep <@!1305200545979437129> ${verb} 102 PLN INNE`));
  }
});

test('still rejects invalid formats and extra text', () => {
  for (const rep of [
    '+rep @Kolczasty ZAKUP 102 PLN INNE komentarz',
    '+rep @Kolczasty ZAKUP 102 PLN NIEZNANY',
    '+rep @Kolczasty ZAKUP abc PLN INNE',
    '+rep ZAKUP 102 PLN INNE',
  ]) assert.equal(parse(rep), null);
});

test('anonymous INNE rep uses the same validator before sending', async () => {
  const channel = { fetchWebhooks: async () => ({ find: () => ({ send: async payload => {
    sent.push(payload); return { id: 'rep-id' };
  } }) }) };
  const content = '+rep <@1305200545979437129> ZAKUP 102 PLN INNE';
  const result = await context.sendAnonRep(channel, content);
  assert.equal(result.id, 'rep-id');
  assert.equal(sent.at(-1).content, content);
});

test('invalid anonymous rep is rejected before any Discord request', async () => {
  let calls = 0;
  const channel = { fetchWebhooks: async () => { calls++; } };
  await assert.rejects(context.sendAnonRep(channel, '+rep @Kolczasty ZAKUP 102 PLN NIEZNANY'), /Nieprawidłowy wzór/);
  assert.equal(calls, 0);
});
