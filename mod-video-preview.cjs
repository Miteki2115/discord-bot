// Explicit media components also render in ephemeral interaction responses.
// Plain CDN links do not reliably produce a video preview there.
function buildModVideoPreview(caption, url, files = []) {
  if (!/^https?:\/\//i.test(url) && !url.startsWith('attachment://')) {
    throw new TypeError('A direct media URL or attachment reference is required');
  }
  return {
    content: null,
    embeds: [],
    components: [
      { type: 10, content: caption },
      { type: 12, items: [{ media: { url } }] },
    ],
    flags: 1 << 15,
    files,
  };
}

module.exports = { buildModVideoPreview };
