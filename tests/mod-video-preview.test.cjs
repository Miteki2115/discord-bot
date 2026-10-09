const test = require('node:test');
const assert = require('node:assert/strict');
const { buildModVideoPreview } = require('../mod-video-preview.cjs');

test('remote MP4 and MOV recordings use explicit media instead of a plain link', () => {
  for (const extension of ['mp4', 'mov']) {
    const url = `https://cdn.discordapp.com/attachments/123/456/demo.${extension}`;
    const payload = buildModVideoPreview('## Mod: Demo', url);
    assert.equal(payload.content, null);
    assert.deepEqual(payload.embeds, []);
    assert.equal(payload.flags, 32768);
    assert.equal(payload.components[0].content, '## Mod: Demo');
    assert.equal(payload.components[1].type, 12);
    assert.equal(payload.components[1].items[0].media.url, url);
    assert.deepEqual(payload.files, []);
    assert.equal(payload.flags | 64, 32832);
  }
});

test('local uploads are referenced by their exact filename', () => {
  const file = { name: 'auto_dripstone.mov' };
  const payload = buildModVideoPreview('Mod', `attachment://${file.name}`, [file]);
  assert.equal(payload.components[1].items[0].media.url, 'attachment://auto_dripstone.mov');
  assert.equal(payload.files[0], file);
});

test('rejects non-media URL schemes', () => {
  assert.throws(() => buildModVideoPreview('Mod', 'file:///private.mov'), TypeError);
});

test('first reply and every subsequent recording retain the gallery and privacy', async () => {
  const fs = require('node:fs');
  const path = require('node:path');
  const vm = require('node:vm');
  const source = fs.readFileSync(path.join(__dirname, '..', 'index.cjs'), 'utf8');
  const start = source.indexOf('      const sendVideoMessage = async (payload)');
  const end = source.indexOf('\n      for (let i = 0; i < videosToSend.length;', start);
  const sent = [];
  const context = vm.createContext({
    MessageFlags: { Ephemeral: 64 },
    interaction: {
      editReply: async payload => sent.push(['edit', payload]),
      followUp: async payload => sent.push(['follow', payload]),
    },
  });
  vm.runInContext('let firstResponseSent = false;\n' + source.slice(start, end)
    + '\nglobalThis.send = sendVideoMessage;', context);
  for (let i = 0; i < 4; i++) {
    await context.send(buildModVideoPreview(`Mod ${i}`, `https://example.com/${i}.mp4`));
  }
  assert.equal(sent[0][0], 'edit');
  assert.equal(sent[0][1].flags, 32768); // Deferred reply is already ephemeral.
  for (const [kind, payload] of sent.slice(1)) {
    assert.equal(kind, 'follow');
    assert.equal(payload.flags, 32832);
    assert.equal(payload.content, null);
    assert.equal(payload.components[1].type, 12);
  }
});

test('all four recordings are packaged, below 10 MB and ready for streaming', () => {
  const fs = require('node:fs');
  const path = require('node:path');
  for (const name of ['Auto_Dripstone.mp4', 'Auto_dzwignia.mp4', 'No_entities.mp4', 'Sprawdz_procenty.mp4']) {
    const data = fs.readFileSync(path.join(__dirname, '..', 'attached_assets', name));
    assert.ok(data.length > 1000 && data.length < 10 * 1024 * 1024, name);
    assert.equal(data.toString('ascii', 4, 8), 'ftyp');
    const boxes = [];
    for (let offset = 0; offset + 8 <= data.length;) {
      const size = data.readUInt32BE(offset);
      const type = data.toString('ascii', offset + 4, offset + 8);
      boxes.push(type);
      assert.ok(size >= 8, `${name}: invalid MP4 box`);
      offset += size;
    }
    assert.ok(boxes.includes('moov') && boxes.includes('mdat'), name);
    assert.ok(boxes.indexOf('moov') < boxes.indexOf('mdat'), `${name}: missing faststart`);
  }
});

test('signed source URL is retained and packaged video bypasses stale environment links', async () => {
  const fs = require('node:fs');
  const path = require('node:path');
  const vm = require('node:vm');
  const source = fs.readFileSync(path.join(__dirname, '..', 'index.cjs'), 'utf8');
  const normalize = source.slice(source.indexOf('function normalizeDiscordCdnVideoUrl('),
    source.indexOf('\nfunction isDiscordAttachmentUrl('));
  const resolver = source.slice(source.indexOf('async function resolveModsVideoUrl('),
    source.indexOf('\n// Helper: sprawdź czy użytkownik', source.indexOf('async function resolveModsVideoUrl(')));
  const context = vm.createContext({
    process: { env: { SOURCE: 'https://cdn.discordapp.com/expired.mp4' } },
    resolveLocalModsVideoPath: () => '/packaged/demo.mp4',
    getLocalModsVideoPublicUrl: () => null,
  });
  vm.runInContext(normalize + '\n' + resolver, context);
  const signed = 'https://cdn.discordapp.com/attachments/123/456/demo.mp4?ex=abc&is=def&hm=signature';
  assert.equal(context.normalizeDiscordCdnVideoUrl(signed), signed);
  // A packaged upload needs no public server URL and must not fall back to an expired link.
  assert.equal(await context.resolveModsVideoUrl(null, { envVar: 'SOURCE' }), null);
});
