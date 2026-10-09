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
