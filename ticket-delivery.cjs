function parseDeliveryAmount(raw) {
  const text = String(raw ?? '').trim().toLowerCase().replace(/\s+/g, '');
  const match = /^(\d+(?:[.,]\d+)?)([km]?)$/.exec(text);
  if (!match) throw new Error('Podaj dodatnią kwotę, np. 570k, 2m albo 2000000.');
  const amount = Number(match[1].replace(',', '.')) * ({ k: 1000, m: 1000000 }[match[2]] || 1);
  if (!Number.isSafeInteger(amount) || amount <= 0) throw new Error('Kwota musi być dodatnią liczbą całkowitą.');
  return amount;
}

function deliverySummary(state) {
  const format = n => n.toLocaleString('pl-PL');
  const percent = Math.floor(state.delivered / state.total * 100);
  const filled = Math.floor(state.delivered / state.total * 36);
  const progress = '━'.repeat(filled) + '─'.repeat(36 - filled);
  return [
    `> \`💰\` × **Do przekazania łącznie:** \`${format(state.total)}$\``,
    `> \`✅\` × **Przekazano:** \`${format(state.delivered)}$\``,
    `> \`⏳\` × **Pozostało:** \`${format(state.total - state.delivered)}$\``,
    `\`${progress}\` **${percent}%**`,
  ].join('\n');
}

function createTicketDelivery({ load, save, payload }) {
  const states = new Map();
  let queue = Promise.resolve();
  let ready;
  function initialize() {
    if (!ready) ready = load().then(data => {
      for (const [id, value] of Object.entries(data || {})) {
        if (Number.isSafeInteger(value?.total) && value.total > 0
            && Number.isSafeInteger(value.delivered) && value.delivered >= 0 && value.delivered <= value.total) {
          states.set(id, { ...value, count: Number.isInteger(value.count) ? value.count : 0 });
        }
      }
    }).catch(error => { ready = null; throw error; });
    return ready;
  }
  function serial(operation) {
    const result = queue.then(async () => { await initialize(); return operation(); });
    queue = result.catch(() => {});
    return result;
  }
  async function commit(id, state) {
    const next = Object.fromEntries(states);
    if (state) next[id] = state; else delete next[id];
    await save(next); // No success acknowledgement unless the database accepted it.
    if (state) states.set(id, state); else states.delete(id);
  }
  async function publish(channel, state, move = false) {
    const oldId = state.messageId;
    if (oldId && !move) {
      try {
        await channel.messages.edit(oldId, payload(state, channel.guild.id));
        return state;
      } catch (error) { if (error.code !== 10008) throw error; }
    }
    const message = await channel.send(payload(state, channel.guild.id));
    if (oldId) {
      try { await channel.messages.delete(oldId); }
      catch (error) {
        if (error.code !== 10008) {
          await message.delete().catch(() => {});
          throw error;
        }
      }
    }
    const updated = { ...state, messageId: message.id, count: 0 };
    // Keep the live message ID even if persisting its ID temporarily fails.
    states.set(channel.id, updated);
    await commit(channel.id, updated);
    return updated;
  }
  return {
    initialize,
    change(channel, action, raw, operationId) {
      return serial(async () => {
        const old = states.get(channel.id);
        if (old?.operationIds?.includes(operationId)) return old;
        const amount = parseDeliveryAmount(raw);
        if (action === 'start' && old) throw new Error('Ten ticket ma już licznik. Użyj akcji „Nadałem”, aby dopisać przekazaną kwotę.');
        if (action !== 'start' && action !== 'nadalem') throw new Error('Nieznana akcja.');
        if (action === 'nadalem' && !old) throw new Error('Najpierw ustaw całość: /ile-brakuje akcja:Start wartosc:2m');
        if (action === 'nadalem' && old.delivered + amount > old.total) {
          throw new Error(`Ta kwota przekracza pozostałe ${old.total - old.delivered}$.`);
        }
        const next = action === 'start'
          ? { total: amount, delivered: 0, count: 0, messageId: null, operationIds: [operationId] }
          : { ...old, delivered: old.delivered + amount, operationIds: [...(old.operationIds || []), operationId].slice(-64) };
        await commit(channel.id, next);
        try { return await publish(channel, next); }
        catch (error) {
          error.deliverySaved = true;
          throw error;
        }
      });
    },
    onMessage(message) {
      if (message.author?.bot || message.webhookId || !message.guild) return Promise.resolve();
      return serial(async () => {
        const old = states.get(message.channel.id);
        if (!old) return;
        const next = { ...old, count: old.count + 1 };
        await commit(message.channel.id, next);
        if (next.count >= 3) await publish(message.channel, next, true);
      });
    },
    remove(id) { return serial(() => states.has(id) ? commit(id, null) : undefined); },
  };
}

module.exports = { parseDeliveryAmount, deliverySummary, createTicketDelivery };
