const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(require('node:path').join(__dirname, '..', 'index.cjs'), 'utf8');
const start = source.indexOf('function trackChatCommandCompletion(');
const end = source.indexOf('client.on(Events.InteractionCreate', start);
function fixture() {
  const calls = [];
  const context = vm.createContext({ console: { error() {} }, setInteractionDiagnostic() {}, MessageFlags: { Ephemeral: 64 } });
  vm.runInContext(source.slice(start, end), context);
  const interaction = {
    id: 'test', customId: 'ticket_anon_close', deferred: false, replied: false,
    isRepliable: () => true,
    async deferUpdate() { calls.push('deferUpdate'); this.deferred = true; },
    async deferReply() { calls.push('deferReply'); this.deferred = true; },
    async editReply() { calls.push('editReply'); },
    async reply() { calls.push('reply'); this.replied = true; },
  };
  return { context, interaction, calls, completion: context.trackChatCommandCompletion(interaction) };
}
test('deferUpdate must not trigger fallback edit of an existing V2/deleted ticket message', async () => {
  const f = fixture();
  await f.interaction.deferUpdate();
  await f.context.finishUnansweredChatCommand(f.interaction, f.completion, 'handler-finished');
  assert.equal(f.completion.isFinalized(), true);
  assert.deepEqual(f.calls, ['deferUpdate']);
});
test('deferReply still needs a final response', async () => {
  const f = fixture();
  await f.interaction.deferReply();
  assert.equal(f.completion.isFinalized(), false);
  await f.context.finishUnansweredChatCommand(f.interaction, f.completion, 'handler-finished');
  assert.deepEqual(f.calls, ['deferReply', 'editReply']);
});
test('failed acknowledgement does not become a successful response', async () => {
  const f = fixture();
  const failing = { isRepliable: () => true, async deferUpdate() { throw new Error('network failure'); } };
  const completion = f.context.trackChatCommandCompletion(failing);
  await assert.rejects(failing.deferUpdate());
  assert.equal(completion.isFinalized(), false);
  assert.equal(completion.hasPendingResponse(), false);
});

test('429 on defer prevents fallback and repeated response requests for the same interaction', async () => {
  const f = fixture();
  let requests = 0;
  const failing = {
    isRepliable: () => true,
    async deferReply() { requests++; throw Object.assign(new Error('global limit'), { status: 429 }); },
    async reply() { requests++; },
  };
  const completion = f.context.trackChatCommandCompletion(failing);
  await assert.rejects(failing.deferReply(), /global limit/);
  await f.context.finishUnansweredChatCommand(failing, completion, 'handler-finished');
  await assert.rejects(failing.reply(), /HTTP 429/);
  assert.equal(requests, 1);
  assert.equal(completion.isFinalized(), false);
  assert.equal(completion.hasFinalResponseAttempt(), true);
});
