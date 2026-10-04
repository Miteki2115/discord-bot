const DEFINITIONS = {
  "zakup-0-20": ["🛒 TICKETY × ZAKUP 0-20", "1449526840942268526"],
  "zakup-20-50": ["🛒 TICKETY × ZAKUP 20-50", "1449526958508474409"],
  "zakup-50-100": ["🛒 TICKETY × ZAKUP 50-100", "1449451716129984595"],
  "zakup-100-200": ["🛒 TICKETY × ZAKUP 100-200", "1449452354201190485"],
  "zakup-200-400": ["🛒 TICKETY × ZAKUP 200-400"],
  "zakup-400-999": ["🛒 TICKETY × ZAKUP 400-999+"],
  "boty-mody": ["⚙️ TICKETY × BOTY/MODY", "1491435227866857483"],
  "odbior-nagrody": ["🎁 TICKETY × ODBIÓR NAGRODY", "1449455567641907351"],
  sprzedaz: ["💵 TICKETY × SPRZEDAŻ", "1449455848043708426"],
  inne: ["❓ TICKETY × POMOC/INNE", "1449527585271976131"],
  przejete: ["🔒 TICKETY × PRZEJĘTE", "1457446529395593338"],
  zrealizowane: ["✅ TICKETY × ZREALIZOWANE", "1469059216303198261"],
};
const normalize = value => String(value || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/ł/g, "l").toLowerCase().replace(/[^a-z0-9]/g, "");
const canonicalKey = key => ["zakup-mody", "zakup-autorynku", "zakup-boty"].includes(key) ? "boty-mody" : key;

function createTicketCategoryManager({ categoryMap, templates, channelType, flags, persist }) {
  const locks = new Map();
  async function locked(guild, action) {
    const previous = locks.get(guild.id) || Promise.resolve();
    const current = previous.catch(() => {}).then(action);
    locks.set(guild.id, current);
    try { return await current; } finally { if (locks.get(guild.id) === current) locks.delete(guild.id); }
  }
  function keyFor(guild, id) {
    if (!id) return null;
    for (const [key, value] of Object.entries(categoryMap.get(guild.id) || {})) {
      if (value === id && DEFINITIONS[canonicalKey(key)]) return canonicalKey(key);
    }
    for (const [key, definition] of Object.entries(DEFINITIONS)) {
      const saved = templates.get(guild.id)?.[key];
      if (definition[1] === id || saved?.id === id || saved?.previousIds?.includes(id)) return key;
    }
    const channel = guild.channels.cache.get(id);
    if (channel?.type !== channelType.GuildCategory) return null;
    return Object.keys(DEFINITIONS).find(key => normalize(channel.name) === normalize(DEFINITIONS[key][0])) || null;
  }
  function remember(guild, key, category) {
    const saved = templates.get(guild.id) || {};
    const old = saved[key];
    saved[key] = {
      id: category.id, name: category.name, position: category.rawPosition ?? category.position,
      previousIds: [...new Set([...(old?.previousIds || []), old?.id].filter(Boolean))],
      permissionOverwrites: [...category.permissionOverwrites.cache.values()].map(o => ({
        id: o.id, type: o.type, allow: o.allow.bitfield.toString(), deny: o.deny.bitfield.toString(),
      })),
    };
    templates.set(guild.id, saved);
    const map = categoryMap.get(guild.id) || {};
    map[key] = category.id;
    categoryMap.set(guild.id, map);
  }
  async function ensure(guild, rawKey, channels) {
    const key = canonicalKey(rawKey);
    if (!DEFINITIONS[key]) throw new Error(`Nieznany typ kategorii ticketu: ${rawKey}`);
    const existing = [...channels.values()].find(c => c.type === channelType.GuildCategory && keyFor(guild, c.id) === key);
    if (existing) { remember(guild, key, existing); return existing; }
    const saved = templates.get(guild.id)?.[key];
    const category = await guild.channels.create({
      name: saved?.name || DEFINITIONS[key][0], type: channelType.GuildCategory,
      ...(Number.isInteger(saved?.position) ? { position: saved.position } : {}),
      permissionOverwrites: saved?.permissionOverwrites || [
        { id: guild.id, deny: [flags.ViewChannel] },
        { id: guild.members.me.id, allow: [flags.ViewChannel, flags.ManageChannels] },
      ], reason: "Pierwszy ticket w kategorii",
    });
    remember(guild, key, category);
    return category;
  }
  async function cleanupUnlocked(guild, onlyId = null) {
    // Pełny odczyt serwera: nigdy nie usuwamy kategorii na podstawie niepełnego cache.
    const channels = await guild.channels.fetch();
    const candidates = [...channels.values()].filter(c => c.type === channelType.GuildCategory && (!onlyId || c.id === onlyId) && keyFor(guild, c.id));
    for (const category of candidates) remember(guild, keyFor(guild, category.id), category);
    if (!candidates.length) return;
    // Zachowaj nazwę, pozycję, uprawnienia i historię ID przed usunięciem.
    if (!(await persist())) return;
    for (const category of candidates) {
      if ([...channels.values()].some(c => c.parentId === category.id)) continue;
      await category.delete("Pusta kategoria ticketów");
    }
  }
  return {
    keyFor,
    cleanup: (guild, id = null) => locked(guild, () => cleanupUnlocked(guild, id)),
    create: (guild, key, options) => locked(guild, async () => {
      const channels = await guild.channels.fetch();
      const category = await ensure(guild, key, channels);
      try {
        const channel = await guild.channels.create({ ...options, parent: category.id });
        await persist();
        return channel;
      } catch (error) {
        await cleanupUnlocked(guild, category.id).catch(() => {});
        throw error;
      }
    }),
    move: (channel, key) => locked(channel.guild, async () => {
      const guild = channel.guild, oldParent = channel.parentId;
      const channels = await guild.channels.fetch();
      const category = await ensure(guild, key, channels);
      await channel.setParent(category.id, { lockPermissions: false });
      await persist();
      if (oldParent && oldParent !== category.id) await cleanupUnlocked(guild, oldParent);
      return category.id;
    }),
  };
}
module.exports = { createTicketCategoryManager, DEFINITIONS };
