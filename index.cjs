const {
  Client,
  GatewayIntentBits,
  Partials,
  Events,
  EmbedBuilder,
  SlashCommandBuilder,
  REST,
  Routes,
  PermissionFlagsBits,
  ChannelType,
  ActionRowBuilder,
  ContainerBuilder,
  MediaGalleryBuilder,
  MediaGalleryItemBuilder,
  StringSelectMenuBuilder,
  LabelBuilder,
  ModalBuilder,
  SeparatorBuilder,
  TextInputBuilder,
  TextDisplayBuilder,
  TextInputStyle,
  PermissionsBitField,
  OverwriteType,
  ButtonBuilder,
  ButtonStyle,
  AttachmentBuilder,
  MessageFlags,
  AuditLogEvent,
} = require("discord.js");
const { createClient } = require("@supabase/supabase-js");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const DISCORD_API_BASE = process.env.DISCORD_API_BASE || "https://canary.discord.com/api";
const ENABLE_HEAVY_STARTUP_SYNC = process.env.ENABLE_HEAVY_STARTUP_SYNC === "true";

// Load local .env when running on a PC (Render ma własne env vars)
try {
  require("dotenv").config({ path: path.resolve(__dirname, ".env") });
} catch (err) {
  console.warn("[ENV] Nie udało się załadować .env:", err?.message || err);
}
const db = require("./database.js");

// ==== EXPRESS SERVER (RENDER COMPATIBILITY) ====
const express = require('express');
const app = express();
const PORT = process.env.PORT || 3000;
let lastInteractionDiagnostic = {
  command: null,
  stage: 'none',
  timestamp: null,
};
let lastDiscordLoginDiagnostic = {
  stage: 'not_started',
  attempt: 0,
  timestamp: null,
};
let discordApplicationDiagnostic = {
  rest_authenticated: false,
  interactions_http_endpoint_configured: null,
  command_registration: 'not_started',
  timestamp: null,
};
let lastGatewayEventDiagnostic = {
  event: null,
  timestamp: null,
};

app.get('/health', (req, res) => {
  const discordConnected = client.isReady();
  res.status(200).json({
    status: discordConnected ? 'healthy' : 'degraded',
    discord_connected: discordConnected,
    bot_id: client.user?.id || null,
    bot_tag: client.user?.tag || null,
    guilds: client.guilds?.cache?.size || 0,
    gateway_ping_ms: Number.isFinite(client.ws?.ping) ? client.ws.ping : null,
    discord_api_base: DISCORD_API_BASE,
    startup_mode: ENABLE_HEAVY_STARTUP_SYNC ? 'full' : 'core',
    discord_application: discordApplicationDiagnostic,
    last_gateway_event: lastGatewayEventDiagnostic,
    commit: process.env.RENDER_GIT_COMMIT?.slice(0, 7) || 'local',
    discord_login: lastDiscordLoginDiagnostic,
    last_interaction: lastInteractionDiagnostic,
    timestamp: new Date().toISOString(),
  });
});

app.get('/', (req, res) => {
  const discordConnected = client.isReady();
  res.status(200).send(
    discordConnected
      ? "Bot is running and connected to Discord!"
      : "HTTP is running, but Discord is disconnected.",
  );
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`[HTTP] Serwer Express pomyślnie uruchomiony na porcie ${PORT} (0.0.0.0)!`);
});
// ===============================================

const client = new Client({
  rest: {
    api: DISCORD_API_BASE,
    timeout: 15_000,
    retries: 2,
  },
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.GuildMessageReactions,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.DirectMessages,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildPresences,
    GatewayIntentBits.GuildModeration,
  ],
  partials: [
    Partials.Channel,
    Partials.Message,
    Partials.Reaction,
    Partials.User,
  ]
});

const { createBotMonitoring } = require("./bot-monitoring.cjs");
const monitoring = createBotMonitoring({
  client,
  events: Events,
  webhookUrl: process.env.UPTIME_WEBHOOK,
  monitorUrl: process.env.MONITOR_HTTP_URL || process.env.RENDER_EXTERNAL_URL,
  footer: () => getBrandFooterObject(),
  // Separate row: monitoring never replaces shop counters stored at id=1.
  store: {
    async load() {
      const { data, error } = await db.supabase.from('bot_state').select('data').eq('id', 2).single();
      if (error && error.code !== 'PGRST116') throw error;
      return data?.data || null;
    },
    async save(data) {
      const row = { data, updated_at: new Date().toISOString() };
      // A retiring Render instance must not overwrite the newer instance's session.
      const query = data.phase === 'starting'
        ? db.supabase.from('bot_state').upsert({ id: 2, ...row }, { onConflict: 'id' })
        : db.supabase.from('bot_state').update(row).eq('id', 2).eq('data->>sessionId', data.sessionId);
      const { error } = await query;
      if (error) throw error;
    },
  },
});

// Render potrafi zawiesić samo GET /gateway/bot, mimo że WebSocket gateway jest
// osiągalny. Dla jednego, nieszardowanego procesu odpowiedź jest deterministyczna,
// więc omijamy wyłącznie ten request startowy. Cały pozostały REST nadal obsługuje
// discord.js wraz z jego kolejką i respektowaniem retry_after.
const originalClientRestGet = client.rest.get.bind(client.rest);
client.rest.get = async (route, options) => {
  if (route === Routes.gatewayBot()) {
    lastDiscordLoginDiagnostic = {
      stage: 'gateway_fallback',
      attempt: lastDiscordLoginDiagnostic.attempt || 1,
      timestamp: new Date().toISOString(),
    };
    return {
      url: 'wss://gateway.discord.gg',
      shards: 1,
      session_start_limit: {
        total: 1000,
        remaining: 1000,
        reset_after: 60_000,
        max_concurrency: 1,
      },
    };
  }
  return originalClientRestGet(route, options);
};

function isInteractionResponseRoute(route) {
  const value = String(route || "");
  return (
    /^\/interactions\/\d+\/[^/]+\/callback$/.test(value) ||
    /^\/webhooks\/\d+\/[^/]+(?:\/messages\/[^/?]+)?$/.test(value)
  );
}

async function directInteractionRestRequest(method, route, options = {}) {
  const query = options.query ? `?${new URLSearchParams(options.query).toString()}` : "";
  const controller = new AbortController();
  const isInitialResponse = /^\/interactions\/\d+\/[^/]+\/callback$/.test(String(route || ""));
  // Discord wymaga pierwszej odpowiedzi w ok. 3 sekundy. Nie pozwalamy,
  // żeby pojedyncza zawieszona próba udawała sukces przez dłuższy czas.
  const timeoutId = setTimeout(() => controller.abort(), isInitialResponse ? 2_500 : 5_000);
  try {
    const response = await fetch(`${DISCORD_API_BASE}/v10${route}${query}`, {
        method,
        headers: options.body === undefined ? undefined : { "content-type": "application/json" },
        body: options.body === undefined ? undefined : JSON.stringify(options.body),
        signal: controller.signal,
    });
    const text = await response.text();
    let data = null;
    if (text) {
      try {
        data = JSON.parse(text);
      } catch (_) {
        data = text;
      }
    }
    if (!response.ok) {
      const error = new Error(
        `Discord interaction HTTP ${response.status} via ${new URL(DISCORD_API_BASE).hostname}`,
      );
      error.status = response.status;
      error.code = data?.code || response.status;
      error.responseBody = typeof data === "string" ? data.slice(0, 200) : data;
      throw error;
    }
    return data;
  } finally {
    clearTimeout(timeoutId);
  }
}

for (const methodName of ["post", "patch", "delete"]) {
  const originalMethod = client.rest[methodName].bind(client.rest);
  client.rest[methodName] = (route, options = {}) => {
    // Pliki wymagają multipart i pozostają w standardowym kliencie REST.
    if (isInteractionResponseRoute(route) && !options.files?.length) {
      return directInteractionRestRequest(methodName.toUpperCase(), route, options);
    }
    return originalMethod(route, options);
  };
}

client.rest.on("rateLimited", (info) => {
  console.warn("[DISCORD_REST] Rate limit:", {
    global: Boolean(info?.global),
    route: info?.route,
    method: info?.method,
    retryAfterMs: info?.timeToReset,
    limit: info?.limit,
  });
});

client.on(Events.Error, (error) => {
  console.error("[DISCORD_CLIENT] Błąd klienta:", error);
});

client.on(Events.Raw, (packet) => {
  if (!packet?.t) return;
  lastGatewayEventDiagnostic = {
    event: packet.t,
    timestamp: new Date().toISOString(),
  };
});

process.on("unhandledRejection", (error) => {
  // Błąd pojedynczego zadania asynchronicznego (np. chwilowo niedostępny REST)
  // nie może wyłączać całego gateway i restartować usługi.
  console.error("[UNHANDLED_REJECTION]", error);
});

const NEWSHOP_EMOJI_ID = "1502672633026707667";
const NEWSHOP_EMOJI_NAME = "NewShop";
const NEWSHOP_EMOJI_MARKUP = `<:${NEWSHOP_EMOJI_NAME}:${NEWSHOP_EMOJI_ID}>`;
const BRAND_FOOTER_COMPONENT_TEXT = `${NEWSHOP_EMOJI_MARKUP} \u00A9 2026 New Shop`;
const BRAND_FOOTER_TEXT = "\u00A9 2026 New Shop";
const BRAND_FOOTER_ICON_URL = `https://cdn.discordapp.com/emojis/${NEWSHOP_EMOJI_ID}.png?size=64&quality=lossless`;

const CZY_LEGIT_CHANNEL_MARKER = "czy-legit";
const CZY_LEGIT_MESSAGE_MARKER = "LEGIT SHOP";
const CZY_LEGIT_REACTION = "✅";
let czyLegitTrackedMessageId = null;
let czyLegitTrackedChannelId = null;
const czyLegitReactionUsers = new Set();

function getCzyLegitReactionCount(message) {
  const reaction = message?.reactions?.cache?.find(
    (entry) => entry.emoji?.name === CZY_LEGIT_REACTION,
  );
  return Math.max(0, Number(reaction?.count) || 0);
}

async function syncCzyLegitReactionUsers(message) {
  if (!message) return;
  try {
    const reaction = message.reactions?.cache?.find(
      (entry) => entry.emoji?.name === CZY_LEGIT_REACTION,
    );
    if (!reaction) return;
    czyLegitReactionUsers.clear();
    let lastId;
    while (true) {
      const users = await reaction.users.fetch({ limit: 100, after: lastId }).catch(() => null);
      if (!users || users.size === 0) break;
      for (const [uid] of users) {
        czyLegitReactionUsers.add(uid);
      }
      lastId = users.lastKey();
      if (users.size < 100) break;
    }
    console.log(`[czy-legit] Zsynchronizowano ${czyLegitReactionUsers.size} użytkowników z reakcją ${CZY_LEGIT_REACTION}`);
  } catch (error) {
    console.error("[czy-legit] Błąd synchronizacji użytkowników reakcji:", error);
  }
}

async function renameCzyLegitChannel(channel, count) {
  if (!channel || typeof channel.setName !== "function") return;

  const safeCount = Math.max(0, Number(count) || 0);
  const desiredName = `🤔×〢czy-legit➔${safeCount}`;
  if (channel.name === desiredName) return;

  await channel.setName(desiredName, `Aktualizacja reakcji ${CZY_LEGIT_REACTION}`);
  console.log(`[czy-legit] Zmieniono nazwę kanału na ${desiredName}`);
}

async function syncCzyLegitMessage(message) {
  if (!message) return false;
  if (message.partial) message = await message.fetch().catch(() => null);
  if (!message) return false;

  const content = String(message.content || "").toUpperCase();
  if (!content.includes(CZY_LEGIT_MESSAGE_MARKER)) return false;

  const channel = message.channel;
  if (!channel?.name?.includes(CZY_LEGIT_CHANNEL_MARKER)) return false;

  czyLegitTrackedMessageId = message.id;
  czyLegitTrackedChannelId = channel.id;
  await renameCzyLegitChannel(channel, getCzyLegitReactionCount(message));
  await syncCzyLegitReactionUsers(message);
  return true;
}

async function initializeCzyLegitCounter() {
  const channel = client.channels.cache.find(
    (entry) => entry?.isTextBased?.() && entry.name?.includes(CZY_LEGIT_CHANNEL_MARKER),
  );
  if (!channel) {
    console.warn("[czy-legit] Nie znaleziono kanału zawierającego nazwę czy-legit.");
    return;
  }

  const candidates = [];
  const pinned = await channel.messages.fetchPinned().catch(() => null);
  if (pinned) candidates.push(...pinned.values());

  const recent = await channel.messages.fetch({ limit: 100 }).catch(() => null);
  if (recent) candidates.push(...recent.values());

  const uniqueCandidates = [...new Map(candidates.map((message) => [message.id, message])).values()];
  const target = uniqueCandidates
    .filter((message) =>
      String(message.content || "").toUpperCase().includes(CZY_LEGIT_MESSAGE_MARKER),
    )
    .sort((a, b) => getCzyLegitReactionCount(b) - getCzyLegitReactionCount(a))[0];

  if (!target) {
    console.warn("[czy-legit] Kanał znaleziony, ale nie znaleziono wiadomości LEGIT SHOP w przypiętych ani ostatnich 100 wiadomościach.");
    return;
  }

  await syncCzyLegitMessage(target);
  console.log(`[czy-legit] Obserwuję wiadomość ${target.id} (${getCzyLegitReactionCount(target)} reakcji).`);
}

async function handleCzyLegitReactionChange(reaction, user) {
  try {
    if (reaction.partial) reaction = await reaction.fetch();
    if (reaction.emoji?.name !== CZY_LEGIT_REACTION) return;

    let message = reaction.message;
    if (message?.partial) message = await message.fetch();
    if (!message) return;
    if (czyLegitTrackedMessageId && message.id !== czyLegitTrackedMessageId) return;

    await syncCzyLegitMessage(message);
  } catch (error) {
    console.error("[czy-legit] Błąd synchronizacji reakcji:", error);
  }
}

async function hasUserReactedInCzyLegit(userId, guild) {
  if (!userId) return false;
  if (czyLegitReactionUsers.has(userId)) return true;

  if (!czyLegitTrackedMessageId || !czyLegitTrackedChannelId) {
    await initializeCzyLegitCounter().catch(() => null);
  }
  if (czyLegitReactionUsers.has(userId)) return true;

  try {
    const chId = czyLegitTrackedChannelId || "1350446732365926494";
    const ch = guild?.channels?.cache?.get(chId) || (guild?.channels ? await guild.channels.fetch(chId).catch(() => null) : null);
    if (ch && czyLegitTrackedMessageId) {
      const msg = await ch.messages.fetch(czyLegitTrackedMessageId).catch(() => null);
      if (msg) {
        const reaction = msg.reactions?.cache?.find((entry) => entry.emoji?.name === CZY_LEGIT_REACTION);
        if (reaction) {
          const fetchedUsers = await reaction.users.fetch({ limit: 100 }).catch(() => null);
          if (fetchedUsers?.has(userId)) {
            czyLegitReactionUsers.add(userId);
            return true;
          }
        }
      }
    }
  } catch (e) { }

  return false;
}

client.on(Events.MessageReactionAdd, async (reaction, user) => {
  try {
    if (reaction.emoji?.name === CZY_LEGIT_REACTION && (!czyLegitTrackedMessageId || reaction.message?.id === czyLegitTrackedMessageId) && user?.id) {
      czyLegitReactionUsers.add(user.id);
    }
  } catch (e) { }
  await handleCzyLegitReactionChange(reaction, user);
});

client.on(Events.MessageReactionRemove, async (reaction, user) => {
  try {
    if (reaction.emoji?.name === CZY_LEGIT_REACTION && (!czyLegitTrackedMessageId || reaction.message?.id === czyLegitTrackedMessageId) && user?.id) {
      czyLegitReactionUsers.delete(user.id);
    }
  } catch (e) { }
  await handleCzyLegitReactionChange(reaction, user);
});

client.on(Events.MessageReactionRemoveAll, async (message) => {
  try {
    if (message.partial) message = await message.fetch();
    if (message.id !== czyLegitTrackedMessageId) return;
    czyLegitReactionUsers.clear();
    await renameCzyLegitChannel(message.channel, 0);
  } catch (error) {
    console.error("[czy-legit] Błąd po usunięciu wszystkich reakcji:", error);
  }
});

client.on(Events.MessageCreate, (message) => {
  (async () => {
    const deleted = await moderatePublishedOpinionMessage(message);
    if (!deleted) await handleNewOpinionMessage(message);
  })().catch((error) =>
    console.error("[opinie-counter] Błąd po dodaniu lub moderacji opinii:", error),
  );
});

client.on(Events.MessageDelete, (message) => {
  Promise.all([
    handleDeletedOpinionMessage(message),
    handleDeletedLegitRepMessage(message),
  ]).catch((error) =>
    console.error("[message-counter] Błąd po usunięciu wiadomości:", error),
  );
});

client.on(Events.MessageBulkDelete, (messages) => {
  for (const message of messages.values()) {
    Promise.all([
      handleDeletedOpinionMessage(message),
      handleDeletedLegitRepMessage(message),
    ]).catch((error) =>
      console.error("[message-counter] Błąd po masowym usunięciu wiadomości:", error),
    );
  }
});

function getBrandFooterIconUrl() {
  return BRAND_FOOTER_ICON_URL;
}

function getBrandFooterObject() {
  const iconUrl = getBrandFooterIconUrl();
  return iconUrl
    ? { text: BRAND_FOOTER_TEXT, icon_url: iconUrl }
    : { text: BRAND_FOOTER_TEXT };
}

function getBrandFooterBuilderObject() {
  const iconUrl = getBrandFooterIconUrl();
  return iconUrl
    ? { text: BRAND_FOOTER_TEXT, iconURL: iconUrl }
    : { text: BRAND_FOOTER_TEXT };
}

function getBrandFooterCaption(guildId) {
  const resolved = guildId
    ? replaceNamedGuildEmojis(BRAND_FOOTER_COMPONENT_TEXT, guildId)
    : BRAND_FOOTER_COMPONENT_TEXT;
  return `-# ${resolved}`;
}

function appendBrandFooterToContainer(container, guildId) {
  if (!container) return;

  if (container.components.length) {
    container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
  }

  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(getBrandFooterCaption(guildId)),
  );
}

/**
 * Usuwa z tekstu stopkę brandingową "© 2026 New Shop" oraz jej warianty,
 * aby uniknąć duplikacji przy imporcie/aktualizacji starych embedów.
 */
function sanitizeBranding(text) {
  if (!text || typeof text !== "string") return text || "";
  // Najbardziej agresywne usuwanie wszystkiego, co zawiera "New Shop" i rok
  return text
    .replace(/(-#\s*)?(<:[A-Za-z0-9_]+:\d+>\s*)?[©\u00A9]\s*202[0-9]\s*New\s*Shop/gi, "")
    .replace(/.*[©\u00A9]\s*202[0-9]\s*New\s*Shop.*/gi, "")
    .replace(/[-\s#]*[©\u00A9]\s*202[0-9]\s*New\s*Shop[-\s#]*/gi, "")
    .replace(/\n\s*_{3,}\s*$/g, "")
    .replace(/\n\s*-{3,}\s*$/g, "")
    .replace(/\n\s*\n\s*$/g, "\n")
    .trim();
}

if (!EmbedBuilder.prototype.__newShopFooterPatchApplied) {
  const originalEmbedBuilderToJSON = EmbedBuilder.prototype.toJSON;

  /**
   * Pozwala włączyć automatyczną stopkę brandową dla konkretnego embeda.
   */
  EmbedBuilder.prototype.setBrandFooter = function () {
    this._useBrandFooter = true;
    return this;
  };

  EmbedBuilder.prototype.toJSON = function (...args) {
    const data = originalEmbedBuilderToJSON.apply(this, args);

    if (data && typeof data === "object") {
      if (!data.footer && this._useBrandFooter) {
        data.footer = getBrandFooterObject();
      }
    }

    return data;
  };

  Object.defineProperty(EmbedBuilder.prototype, "__newShopFooterPatchApplied", {
    value: true,
    enumerable: false,
    configurable: false,
    writable: false,
  });
}

/*
  In-memory stores
*/
const activeCodes = new Map();
const opinieChannels = new Map();
const ticketCounter = new Map();
const fourMonthBlockList = new Map(); // guildId -> Set(userId)
const ticketCategories = new Map();
const ticketCategoryTemplates = new Map();
const { createTicketCategoryManager } = require("./ticket-category-manager.cjs");
const ticketCategoryManager = createTicketCategoryManager({
  categoryMap: ticketCategories, templates: ticketCategoryTemplates,
  channelType: ChannelType, flags: PermissionFlagsBits,
  persist: () => saveStateToSupabase(buildPersistentStateData()),
});

client.on(Events.ChannelDelete, (channel) => {
  if (channel.guild && channel.parentId && channel.type !== ChannelType.GuildCategory && ticketCategoryManager.keyFor(channel.guild, channel.parentId)) {
    ticketCategoryManager.cleanup(channel.guild, channel.parentId).catch(error => console.error("[ticket-category] cleanup:", error));
  }
});
client.on(Events.ChannelUpdate, (oldChannel, channel) => {
  if (channel.guild && oldChannel.parentId && oldChannel.parentId !== channel.parentId && ticketCategoryManager.keyFor(channel.guild, oldChannel.parentId)) {
    ticketCategoryManager.cleanup(channel.guild, oldChannel.parentId).catch(error => console.error("[ticket-category] move cleanup:", error));
  }
});
const legitRepCooldown = new Map(); // userId -> timestamp ostatniego poprawnego +rep
const dropChannels = new Map(); // <-- mapa kanałów gdzie można używać /drop
const sprawdzZaproszeniaCooldowns = new Map(); // userId -> lastTs
const inviteTotalJoined = new Map(); // guild -> userId -> liczba wszystkich dołączeń
const inviteFakeAccounts = new Map(); // guild -> userId -> liczba kont < 4 miesiące
const inviteBonusInvites = new Map(); // guild -> userId -> dodatkowe zaproszenia (z /ustawzaproszenia)
const inviteRewardsGiven = new Map(); // NEW: guild -> userId -> ile nagród już przyznano

// Helper: funkcja zwracająca poprawną formę słowa "zaproszenie"
function getInviteWord(count) {
  if (count === 1) return "zaproszenie";
  if (count >= 2 && count <= 4) return "zaproszenia";
  return "zaproszeń";
}

// NEW: weryfikacja
const verificationRoles = new Map(); // guildId -> roleId
const pendingVerifications = new Map(); // modalId -> { answer, guildId, userId, roleId }

const ticketOwners = new Map(); // channelId -> { claimedBy, userId, ticketMessageId, locked, lastClaimMsgId }
const creatingTicketUsers = new Set(); // userId set lock to prevent duplicate creation race conditions
const ticketLogCards = new Map(); // channelId -> messageId; jeden czytelny log na cały cykl ticketu
const ticketLogEventDedupe = new Map(); // channelId -> { key, timestamp }

// (Usunięto nadpisywanie ticketOwners.set, timer 5 min od pierwszej wiadomosci jest w Events.MessageCreate)

// --- DYNAMICZNY GENERATOR CAPTCHY (Quiz Przejmowania) ---
function generateClaimQuiz() {
  // Proste działania matematyczne
  const ops = [
    () => { const a = Math.floor(Math.random() * 9) + 1; const b = Math.floor(Math.random() * 9) + 1; return { q: `Ile to ${a} + ${b}?`, a: (a + b).toString() }; },
    () => { const a = Math.floor(Math.random() * 8) + 2; const b = Math.floor(Math.random() * (a - 1)) + 1; return { q: `Ile to ${a} - ${b}?`, a: (a - b).toString() }; },
    () => { const a = Math.floor(Math.random() * 5) + 2; const b = Math.floor(Math.random() * 5) + 2; return { q: `Ile to ${a} x ${b}?`, a: (a * b).toString() }; },
    () => { const b = Math.floor(Math.random() * 8) + 2; const a = b * (Math.floor(Math.random() * 4) + 2); return { q: `Ile to ${a} / ${b}?`, a: (a / b).toString() }; },
  ];
  return ops[Math.floor(Math.random() * ops.length)]();
}
// ----------------------------------------------------------------
const pendingClaimQuiz = new Map(); // modalId -> { channelId, userId, answer }
const claimingInProgress = new Set(); // channelId currently being claimed (race-condition lock)
const autoPrzejmijSettings = new Map(); // guildId -> { enabled, ownerId, ownerName, enabledAt }
const autoVerifySettings = new Map(); // guildId -> boolean
const pendingAutoPrzejmijQuiz = new Map(); // modalId -> { guildId, userId, ownerId, ownerName, answer }
const sellerPaymentProfiles = new Map(); // `${guildId}:${userId}` -> { phone, transferTitle, receiverName, updatedAt }
const sellerDataBackups = new Map();
const sellerWarnings = new Map(); // `${guildId}:${userId}` -> number
const sellerSavedLimitRoles = new Map(); // `${guildId}:${userId}` -> Array<string> of roleIds
const sellerWarnMessages = new Map(); // `${guildId}:${userId}` -> Array<{ pingMessageId, embedMessageId, channelId }>
const OSTRZEZENIA_CHANNEL_ID = "1457447223452237834";
const embedTestStates = new Map(); // messageId -> editable preview state for /embed-2
const regulationPanels = new Map(); // messageId -> persisted regulation panel state
const pendingEmbedTestPublish = new Map(); // guildId:userId -> { messageId, sourceChannelId, expiresAt }
const embedTestEmojiCacheReady = new Map(); // guildId -> timestamp ostatniego fetch emoji

let globalCommissionRate = 0.10;
const sellerCommissionRates = new Map(); // userId -> number (np. 0.05 dla 5%)

function getUserCommissionRate(userId) {
  if (userId && sellerCommissionRates.has(userId)) {
    return sellerCommissionRates.get(userId);
  }
  return typeof globalCommissionRate === "number" ? globalCommissionRate : 0.10;
}

function getNextSundayTimestamp() {
  const now = new Date();
  const polandDateStr = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Warsaw",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(now);

  const [year, month, day] = polandDateStr.split("-").map(Number);
  const polandDayStr = new Intl.DateTimeFormat("en-US", {
    timeZone: "Europe/Warsaw",
    weekday: "short",
  }).format(now);
  const dayMap = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
  const currentDay = dayMap[polandDayStr] ?? 0;

  let daysUntilSunday = (7 - currentDay) % 7;
  if (daysUntilSunday === 0) {
    daysUntilSunday = 7;
  }

  const targetUtc = new Date(Date.UTC(year, month - 1, day + daysUntilSunday));
  const targetY = targetUtc.getUTCFullYear();
  const targetM = String(targetUtc.getUTCMonth() + 1).padStart(2, "0");
  const targetD = String(targetUtc.getUTCDate()).padStart(2, "0");

  const sample = new Date(`${targetY}-${targetM}-${targetD}T12:00:00Z`);
  const warsawTimeString = new Intl.DateTimeFormat("en-US", {
    timeZone: "Europe/Warsaw",
    hour: "numeric",
    hour12: false,
  }).format(sample);
  const offset = Number(warsawTimeString) - 12;

  const finalDate = new Date(Date.UTC(targetY, Number(targetM) - 1, Number(targetD), -offset, 0, 0, 0));
  return Math.floor(finalDate.getTime() / 1000);
}

function getTodaySunday20Timestamp() {
  const now = new Date();
  const polandDateStr = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Warsaw",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(now);
  const [year, month, day] = polandDateStr.split("-").map(Number);
  const sample = new Date(`${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}T12:00:00Z`);
  const warsawTimeString = new Intl.DateTimeFormat("en-US", {
    timeZone: "Europe/Warsaw",
    hour: "numeric",
    hour12: false,
  }).format(sample);
  const offset = Number(warsawTimeString) - 12;
  const finalDate = new Date(Date.UTC(year, month - 1, day, 20 - offset, 0, 0, 0));
  return Math.floor(finalDate.getTime() / 1000);
}

function getTodaySunday0022Timestamp() {
  const now = new Date();
  const polandDateStr = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Warsaw",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(now);
  const [year, month, day] = polandDateStr.split("-").map(Number);
  const sample = new Date(`${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}T12:00:00Z`);
  const warsawTimeString = new Intl.DateTimeFormat("en-US", {
    timeZone: "Europe/Warsaw",
    hour: "numeric",
    hour12: false,
  }).format(sample);
  const offset = Number(warsawTimeString) - 12;
  const finalDate = new Date(Date.UTC(year, month - 1, day, -offset, 22, 0, 0));
  return Math.floor(finalDate.getTime() / 1000);
}

function getPolandTime() {
  const now = new Date();
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone: "Europe/Warsaw",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  const parts = formatter.formatToParts(now);
  const getPart = (type) => parts.find((p) => p.type === type)?.value;

  const year = Number(getPart("year"));
  const month = Number(getPart("month"));
  const day = Number(getPart("day"));
  let hour = Number(getPart("hour"));
  if (hour === 24) hour = 0;
  const minute = Number(getPart("minute"));

  const weekdayStr = new Intl.DateTimeFormat("en-US", {
    timeZone: "Europe/Warsaw",
    weekday: "short",
  }).format(now);
  const dayMap = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
  const dayOfWeek = dayMap[weekdayStr] ?? 0;
  const dateKey = `${year}-${month}-${day}`;

  return { year, month, day, hour, minute, dayOfWeek, dateKey };
}

// NEW: keep last posted instruction message per channel so we can delete & re-post
const lastOpinionInstruction = new Map(); // channelId -> messageId
const lastDropInstruction = new Map(); // channelId -> messageId  <-- NEW for drop instructions
const lastInviteInstruction = new Map(); // channelId -> messageId  <-- NEW for invite instructions
const lastFreeKasaInstruction = new Map(); // channelId -> messageId

// Mapa do przechowywania wyborów użytkowników dla kalkulatora
const kalkulatorData = new Map(); // userId -> { tryb, metoda, typ }

// Contest maps (new)
const contestParticipants = new Map(); // messageId -> Set(userId)
const contests = new Map(); // messageId -> { channelId, endsAt, winnersCount, title, prize, imageUrl }
const contestLeaveBlocks = new Map(); // userId -> { messageId: { leaveCount: number, blockedUntil: number } }

// --- LEGITCHECK-REP info behavior --------------------------------------------------
// channel ID where users post freeform reps and the bot should post the informational embed
const REP_CHANNEL_ID = "1449840030947217529";
const antiNukeWindows = new Map();
const antiNukeCooldowns = new Map();
const ANTI_NUKE_RULES = new Map([
  [AuditLogEvent.MemberKick, { label: "wyrzucenia użytkowników", limit: 5, windowMs: 60_000 }],
  [AuditLogEvent.MemberBanAdd, { label: "banowania użytkowników", limit: 5, windowMs: 60_000 }],
  [AuditLogEvent.ChannelDelete, { label: "usuwania kanałów", limit: 3, windowMs: 60_000 }],
  [AuditLogEvent.RoleDelete, { label: "usuwania ról", limit: 3, windowMs: 60_000 }],
  [AuditLogEvent.WebhookCreate, { label: "tworzenia webhooków", limit: 4, windowMs: 60_000 }],
  [AuditLogEvent.WebhookDelete, { label: "usuwania webhooków", limit: 4, windowMs: 60_000 }],
]);
const ANTI_NUKE_DANGEROUS_PERMISSIONS = [
  PermissionsBitField.Flags.Administrator,
  PermissionsBitField.Flags.BanMembers,
  PermissionsBitField.Flags.KickMembers,
  PermissionsBitField.Flags.ManageChannels,
  PermissionsBitField.Flags.ManageRoles,
  PermissionsBitField.Flags.ManageWebhooks,
  PermissionsBitField.Flags.ManageGuild,
];
const legitRepMessageIds = new Set();
let legitRepHistoryInitialized = false;
const LEGIT_REP_PING_DELETE_DELAY_MS = 4_000;
const LEGIT_REP_WARNING_DELETE_DELAY_MS = 15_000;
const DEFAULT_SELECT_EMPTY_PLACEHOLDER = "❌ × Nie wybrałeś/aś żadnej opcji.";

// cooldown (ms) per user between the bot posting the info embed
const INFO_EMBED_COOLDOWN_MS = 5 * 1000; // default 5s — change to desired value

// map used for throttling per-user
const infoCooldowns = new Map(); // userId -> timestamp (ms)

// banner/gif url to show at bottom of embed (change this to your gif/url)
const REP_EMBED_BANNER_URL =
  "https://cdn.discordapp.com/attachments/1449367698374004869/1450192787894046751/standard_1.gif";

function getTicketBannerAttachment() {
  const gifPath = path.join(__dirname, "attached_assets", "standard_(1)_1766946611653.gif");
  if (fs.existsSync(gifPath)) {
    return new AttachmentBuilder(gifPath, { name: "standard_1.gif" });
  }
  return null;
}

function buildTicketContainerPayload({ headerText, bodyText, buttonRow, guildId }) {
  const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);
  container.addTextDisplayComponents(new TextDisplayBuilder().setContent(headerText));
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

  container.addTextDisplayComponents(new TextDisplayBuilder().setContent(bodyText));
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

  container.addActionRowComponents(buttonRow);
  appendBrandFooterToContainer(container, guildId);

  return {
    components: [container],
    flags: MessageFlags.IsComponentsV2,
  };
}

// track last info message posted by the bot per channel so we can delete it before posting a new one
const repLastInfoMessage = new Map(); // channelId -> messageId

// /mody: list of proof videos shown after clicking the button
const MODS_VIDEO_FILES = [
  {
    key: "no_entities",
    label: "No_entities (1440x2560)",
    modName: "NoEntities",
    filename: "No_entities.mov",
    filenameAliases: ["No_entities.mp4"],
    localPath: path.join(__dirname, "attached_assets", "No_entities.mov"),
    envVar: "MODS_VIDEO_URL_NO_ENTITIES",
  },
  {
    key: "sprawdz_procenty",
    label: "Sprawdz_procenty",
    modName: "SprawdzProcenty",
    filename: "Sprawdz_procenty.mov",
    filenameAliases: ["Sprawdz_procenty.mp4"],
    localPath: path.join(__dirname, "attached_assets", "Sprawdz_procenty.mov"),
    envVar: "MODS_VIDEO_URL_SPRAWDZ_PROCENTY",
  },
  {
    key: "auto_dzwignia",
    label: "Auto_dźwignia",
    modName: "AutoDzwignia",
    filename: "Auto_dźwignia.mov",
    filenameAliases: [
      "Auto_dźwignia (1).mov",
      "Auto_dzwignia.mov",
      "Auto_dzwignia (1).mov",
    ],
    localPath: path.join(__dirname, "attached_assets", "Auto_dźwignia.mov"),
    envVar: "MODS_VIDEO_URL_AUTO_DZWIGNIA",
    defaultUrl:
      "https://cdn.discordapp.com/attachments/1350603811512909914/1477659247511605340/Auto_dzwignia.mov?ex=69a590ea&is=69a43f6a&hm=045a8441610b16e22135e2a267ba139021cd498791c71861627d4dc486506284",
  },
  {
    key: "auto_dripstone",
    label: "Auto_Dripstone",
    modName: "AutoDripstone",
    filename: "Auto_Dripstone.mov",
    filenameAliases: ["Auto_Dripstone.mp4"],
    localPath: path.join(__dirname, "attached_assets", "Auto_Dripstone.mov"),
    envVar: "MODS_VIDEO_URL_AUTO_DRIPSTONE",
    defaultUrl:
      "https://cdn.discordapp.com/attachments/1350603811512909914/1477659253664780402/Auto_Dripstone.mov?ex=69a590eb&is=69a43f6b&hm=51a15faf631c567393b82b6fcc017661cb20775ddd517b723100456f914b1fed",
  },
];
const modsVideoUrlCache = new Map(); // key -> url
const DISCORD_MAX_UPLOAD_BYTES = 25 * 1024 * 1024;
const MODS_VIDEO_SEND_ORDER = [
  "auto_dripstone",
  "no_entities",
  "auto_dzwignia",
  "sprawdz_procenty",
];
const modsVideoOrderRanks = new Map(
  MODS_VIDEO_SEND_ORDER.map((key, idx) => [key, idx]),
);

// legit rep counter
let legitRepCount = 15;
const sellerTicketCounts = new Map(); // <userId, count>
const DAILY_LEGIT_CHANNEL_ID = "1532859726033846292";
const DAILY_LEGIT_TIME_ZONE = "Europe/Warsaw";
let dailyLegitStats = createEmptyDailyLegitStats();
let dailyLegitRecords = {};
let dailyLegitPublishQueue = Promise.resolve();
let dailyLegitMidnightTimer = null;
let dailyLegitRenameTimer = null;
let lastDailyLegitChannelRename = 0;
const DAILY_LEGIT_RENAME_COOLDOWN = 10 * 60 * 1000;
let lastChannelRename = 0;
const CHANNEL_RENAME_COOLDOWN = 10 * 60 * 1000; // 10 minutes (Discord limit)
let pendingRename = false;
let latestRepRenameCount = null;

function isLegitRepMessage(message) {
  return /^\+rep(?:\s|$)/.test(String(message?.content || "").trim());
}

const LEGIT_REP_SERVERS = [
  "ANARCHIA LIFESTEAL",
  "ANARCHIA BOXPVP",
  "MINESTAR LIFESTEAL",
  "MINESTAR SKYPVP",
  "DONUT SMP",
  "INNE",
];

function normalizeLegitRepPart(value) {
  return String(value || "").trim().replace(/\s+/g, " ").toLocaleUpperCase("pl-PL");
}

function normalizeLegitRepVerb(value) {
  const normalized = normalizeLegitRepPart(value)
    .replace("SPRZEDAZ", "SPRZEDAŻ")
    .replace("WRECZYL NAGRODE", "WRĘCZYŁ NAGRODĘ");
  return normalized;
}

function parseLegitRepContent(content) {
  const raw = String(content || "");
  if (!raw.startsWith("+rep ")) return null;
  const serverPattern = LEGIT_REP_SERVERS.join("|");
  const pattern = new RegExp(
    `^\\+rep (<@!?\\d+>|@[^\\s]+) (ZAKUP|SPRZEDAŻ|SPRZEDAZ|WRĘCZYŁ NAGRODĘ|WRECZYL NAGRODE) (\\d+) PLN (${serverPattern})$`,
    "u",
  );
  const match = raw.match(pattern);
  if (!match) return null;

  return {
    seller: match[1],
    verb: normalizeLegitRepVerb(match[2]),
    amount: Number(match[3]),
    server: normalizeLegitRepPart(match[4]),
  };
}

function legitRepMatchesPendingTicket(parsed, message, ticketData) {
  if (!parsed || !ticketData) return false;

  const expectedSellerMention = `<@${ticketData.commandUserId}>`;
  const expectedSellerNickname = `@${String(ticketData.commandUsername || "").toLocaleLowerCase("pl-PL")}`;
  const parsedSeller = parsed.seller.toLocaleLowerCase("pl-PL");
  const sellerMatches =
    message.mentions.users.has(ticketData.commandUserId) ||
    parsedSeller === expectedSellerMention.toLocaleLowerCase("pl-PL") ||
    parsedSeller === `<@!${ticketData.commandUserId}>`.toLocaleLowerCase("pl-PL") ||
    parsedSeller === expectedSellerNickname;

  return (
    sellerMatches &&
    parsed.verb === normalizeLegitRepVerb(ticketData.typ) &&
    parsed.amount === parsePLN(ticketData.co) &&
    parsed.server === normalizeLegitRepPart(ticketData.serwer)
  );
}

async function countExistingLegitRepMessages(channel) {
  if (!channel?.isTextBased?.()) return null;

  const foundIds = new Set();
  let before;

  try {
    while (true) {
      const options = { limit: 100 };
      if (before) options.before = before;
      const batch = await channel.messages.fetch(options);
      if (!batch.size) break;

      for (const message of batch.values()) {
        if (isLegitRepMessage(message)) foundIds.add(message.id);
      }

      before = batch.last()?.id;
      if (batch.size < 100 || !before) break;
    }
  } catch (error) {
    console.error("[legit-counter] Nie udało się odczytać całej historii kanału:", error);
    return null;
  }

  legitRepMessageIds.clear();
  for (const messageId of foundIds) legitRepMessageIds.add(messageId);
  legitRepHistoryInitialized = true;
  console.log(`[legit-counter] Policzone wiadomości +rep: ${legitRepMessageIds.size}. Żadna wiadomość nie została usunięta.`);
  return legitRepMessageIds.size;
}

function formatTicketsPlural(count) {
  const n = Math.abs(Number(count) || 0);
  if (n === 1) return "ticket";
  const mod10 = n % 10;
  const mod100 = n % 100;
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14)) {
    return "tickety";
  }
  return "ticketów";
}

async function getSellerTicketCount(userId, forceRecalculate = false) {
  if (!forceRecalculate && sellerTicketCounts.has(userId)) {
    return sellerTicketCounts.get(userId);
  }

  const channel = await client.channels.fetch(REP_CHANNEL_ID).catch(() => null);
  if (!channel?.isTextBased?.()) {
    return sellerTicketCounts.get(userId) || 0;
  }

  let count = 0;
  let before;
  const targetUser = await client.users.fetch(userId).catch(() => null);
  const sellerAliases = new Set(
    [targetUser?.username, targetUser?.globalName]
      .filter(Boolean)
      .map((name) => `@${name}`.toLocaleLowerCase("pl-PL")),
  );
  try {
    while (true) {
      const options = { limit: 100 };
      if (before) options.before = before;
      const batch = await channel.messages.fetch(options);
      if (!batch.size) break;

      for (const message of batch.values()) {
        const repMatch = String(message.content || "").trim().match(/^\+rep\s+(\S+)/i);
        if (!repMatch) continue;

        const sellerToken = repMatch[1];
        const sellerMention = sellerToken.match(/^<@!?(\d+)>$/);
        const sellerId = sellerMention?.[1] || null;
        const sellerAlias = sellerToken.toLocaleLowerCase("pl-PL");
        if (sellerId === String(userId) || (!sellerId && sellerAliases.has(sellerAlias))) count++;
      }

      before = batch.last()?.id;
      if (batch.size < 100 || !before) break;
    }
  } catch (error) {
    console.error("Error fetching rep channel messages for getSellerTicketCount:", error);
  }

  sellerTicketCounts.set(userId, count);
  scheduleSavePersistentState();
  return count;
}

async function handleDeletedLegitRepMessage(message) {
  if (!legitRepHistoryInitialized || message?.channelId !== REP_CHANNEL_ID) return;
  if (!legitRepMessageIds.delete(message.id)) return;

  legitRepCount = legitRepMessageIds.size;
  const channel = message.channel || await client.channels.fetch(REP_CHANNEL_ID).catch(() => null);
  scheduleRepChannelRename(channel, legitRepCount).catch(() => null);
  scheduleSavePersistentState();
  console.log(`[legit-counter] Usunięto wpis z licznika po ręcznym usunięciu wiadomości: ${legitRepCount}`);
}

function getWarsawDateParts(date = new Date()) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: DAILY_LEGIT_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    hourCycle: "h23",
  }).formatToParts(date);
  const value = (type) => parts.find((part) => part.type === type)?.value;
  return {
    dateKey: `${value("year")}-${value("month")}-${value("day")}`,
    hour: Number(value("hour")),
  };
}

function createEmptyDailyLegitStats(date = new Date()) {
  return {
    dateKey: getWarsawDateParts(date).dateKey,
    hourly: Array(24).fill(0),
    total: 0,
    totalPln: 0,
    messageId: null,
    updatedAt: null,
  };
}

function normalizeDailyLegitStats(value) {
  if (!value || typeof value !== "object") return createEmptyDailyLegitStats();
  const hourly = Array.from({ length: 24 }, (_, hour) =>
    Math.max(0, Math.floor(Number(value.hourly?.[hour] || 0))),
  );
  const storedTotalPln = Number(value.totalPln || 0);
  return {
    dateKey: /^\d{4}-\d{2}-\d{2}$/.test(String(value.dateKey || ""))
      ? value.dateKey
      : getWarsawDateParts().dateKey,
    hourly,
    total: Math.max(0, Math.floor(Number(value.total ?? hourly.reduce((a, b) => a + b, 0)))),
    totalPln: Number.isFinite(storedTotalPln) ? Math.max(0, storedTotalPln) : 0,
    messageId: value.messageId ? String(value.messageId) : null,
    updatedAt: value.updatedAt ? String(value.updatedAt) : null,
  };
}

function normalizeDailyLegitRecords(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  const normalized = {};
  for (const [dateKey, record] of Object.entries(value)) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateKey) || !record || typeof record !== "object") continue;
    const total = Math.max(0, Math.floor(Number(record.total || 0)));
    const totalPln = Number(record.totalPln || 0);
    normalized[dateKey] = {
      dateKey,
      total,
      totalPln: Number.isFinite(totalPln) ? Math.max(0, totalPln) : 0,
    };
  }
  return normalized;
}

function archiveDailyLegitStats(stats) {
  if (!stats || !/^\d{4}-\d{2}-\d{2}$/.test(String(stats.dateKey || ""))) return;
  dailyLegitRecords[stats.dateKey] = {
    dateKey: stats.dateKey,
    total: Math.max(0, Math.floor(Number(stats.total || 0))),
    totalPln: Math.max(0, Number(stats.totalPln || 0)),
  };

  const orderedKeys = Object.keys(dailyLegitRecords).sort();
  while (orderedKeys.length > 730) {
    delete dailyLegitRecords[orderedKeys.shift()];
  }
}

function rollDailyLegitStatsIfNeeded(now = new Date()) {
  const currentDateKey = getWarsawDateParts(now).dateKey;
  if (dailyLegitStats.dateKey === currentDateKey) return false;
  archiveDailyLegitStats(dailyLegitStats);
  dailyLegitStats = createEmptyDailyLegitStats(now);
  lastDailyLegitChannelRename = 0;
  scheduleSavePersistentState(true);
  console.log(`[daily-legit] Rozpoczęto nowy dzień: ${currentDateKey}`);
  return true;
}

function buildDailyLegitContainer({ dateKey, total, totalPln, guildId, showRecordsButton = true }) {
  const displayDate = /^\d{4}-\d{2}-\d{2}$/.test(dateKey)
    ? `${dateKey.slice(8, 10)}.${dateKey.slice(5, 7)}.${dateKey.slice(0, 4)}`
    : dateKey;
  const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);
  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      "```\n" +
      "📊 New Shop × DZIENNE LC\n" +
      "```",
    ),
  );
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      `> \`📅\` <a:arrowwhite:1491476759290449984> **Data:** \`${displayDate}\`\n` +
      `> \`✅\` <a:arrowwhite:1491476759290449984> **Wystawione legit checki:** \`${total}\`\n` +
      `> \`💰\` <a:arrowwhite:1491476759290449984> **Łączna wydana kwota:** \`${Number(totalPln || 0).toFixed(2)} PLN\``,
    ),
  );
  if (showRecordsButton) {
    container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
    container.addActionRowComponents(
      new ActionRowBuilder().addComponents(
        new ButtonBuilder()
          .setCustomId("daily_legit_record")
          .setLabel("🏆︲Rekordy")
          .setStyle(ButtonStyle.Secondary),
      ),
    );
  }
  appendBrandFooterToContainer(container, guildId);
  return container;
}

async function removeRecordButtonsFromOldDailyLegitMessages(channel, currentMessageId) {
  if (!channel?.isTextBased?.()) return;
  try {
    const fetched = await channel.messages.fetch({ limit: 50 });
    // Zbierz wszystkie wiadomości dzienne LC bota posortowane chronologicznie (od najstarszej do najnowszej)
    const dailyMessages = Array.from(fetched.values())
      .filter((msg) => {
        if (msg.author.id !== client.user?.id) return false;
        const text = JSON.stringify(msg);
        return text.includes("DZIENNE LC");
      })
      .sort((a, b) => a.createdTimestamp - b.createdTimestamp);

    if (dailyMessages.length <= 1) return;

    // Ostatnia (na samym dole) ma zachować przycisk
    const latestDailyMsg = dailyMessages[dailyMessages.length - 1];

    for (const msg of dailyMessages) {
      if (msg.id === latestDailyMsg.id) continue; // Pomiń najnowszą wiadomość na dole!

      const fullJsonStr = JSON.stringify(msg);
      if (!fullJsonStr.includes("daily_legit_record") && !fullJsonStr.includes("Rekordy")) continue;

      console.log(`[daily-legit] Starsza wiadomość ${msg.id} ma przycisk - usuwam...`);

      const dateMatch = fullJsonStr.match(/Data:\*\*?\s*`([^`]+)`/) || fullJsonStr.match(/Data:[^`]*`([^`]+)`/);
      const totalMatch = fullJsonStr.match(/Wystawione legit checki:\*\*?\s*`([^`]+)`/) || fullJsonStr.match(/Wystawione legit checki:[^`]*`([^`]+)`/);
      const amountMatch = fullJsonStr.match(/Łączna wydana kwota:\*\*?\s*`([^`]+)\s*PLN`/) || fullJsonStr.match(/Łączna wydana kwota:[^`]*`([^`]+)\s*PLN`/);

      const msgDateKey = dateMatch ? dateMatch[1] : dailyLegitStats.dateKey;
      const msgTotal = totalMatch ? parseInt(totalMatch[1], 10) || 0 : 0;
      const msgTotalPln = amountMatch ? parseFloat(amountMatch[1]) || 0 : 0;

      const cleanContainer = buildDailyLegitContainer({
        dateKey: msgDateKey,
        total: msgTotal,
        totalPln: msgTotalPln,
        guildId: channel.guildId,
        showRecordsButton: false,
      });

      await msg.edit({
        content: null,
        embeds: [],
        components: [cleanContainer],
        attachments: [],
        flags: MessageFlags.IsComponentsV2,
      }).then(() => {
        console.log(`[daily-legit] Pomyślnie zdjęto przycisk Rekordy ze starszej wiadomości ${msg.id}`);
      }).catch((err) => {
        console.error(`[daily-legit] Błąd edycji starszej wiadomości ${msg.id}:`, err);
      });
    }
  } catch (err) {
    console.error("[daily-legit] Błąd w removeRecordButtonsFromOldDailyLegitMessages:", err);
  }
}

async function publishDailyLegitChart({ forceNew = false, forceChannelRename = false } = {}) {
  rollDailyLegitStatsIfNeeded();
  const channel = await client.channels.fetch(DAILY_LEGIT_CHANNEL_ID).catch(() => null);
  if (!channel?.isTextBased()) {
    throw new Error(`Nie znaleziono kanału tekstowego ${DAILY_LEGIT_CHANNEL_ID}`);
  }

  const container = buildDailyLegitContainer({
    dateKey: dailyLegitStats.dateKey,
    total: dailyLegitStats.total,
    totalPln: dailyLegitStats.totalPln,
    guildId: channel.guildId,
    showRecordsButton: true,
  });

  let chartMessage = null;
  let oldChartMessage = null;
  if (!forceNew && dailyLegitStats.messageId) {
    chartMessage = await channel.messages.fetch(dailyLegitStats.messageId).catch(() => null);
  }

  if (chartMessage) {
    try {
      await chartMessage.edit({
        content: null,
        embeds: [],
        components: [container],
        attachments: [],
        flags: MessageFlags.IsComponentsV2,
      });
    } catch (error) {
      console.warn("[daily-legit] Nie udało się zmienić starego embeda na Components V2:", error);
      oldChartMessage = chartMessage;
      chartMessage = null;
    }
  }

  if (!chartMessage) {
    // Jeśli tworzymy nową wiadomość podsumowania, ze starej zdejmujemy przycisk Rekordy
    if (dailyLegitStats.messageId) {
      const prevMessage = await channel.messages.fetch(dailyLegitStats.messageId).catch(() => null);
      if (prevMessage) {
        const noButtonContainer = buildDailyLegitContainer({
          dateKey: dailyLegitStats.dateKey,
          total: dailyLegitStats.total,
          totalPln: dailyLegitStats.totalPln,
          guildId: channel.guildId,
          showRecordsButton: false,
        });
        await prevMessage.edit({
          content: null,
          embeds: [],
          components: [noButtonContainer],
          attachments: [],
          flags: MessageFlags.IsComponentsV2,
        }).catch(() => null);
      }
    }

    chartMessage = await channel.send({
      components: [container],
      flags: MessageFlags.IsComponentsV2,
    });
    dailyLegitStats.messageId = chartMessage.id;
    scheduleSavePersistentState(true);
    if (oldChartMessage) {
      await oldChartMessage.delete().catch(() => null);
    }
  }

  // Wyczyść przycisk Rekordy ze wszystkich starszych embedów w kanale
  await removeRecordButtonsFromOldDailyLegitMessages(channel, chartMessage.id);

  console.log(`[daily-legit] Podsumowanie zaktualizowane: ${dailyLegitStats.total} legit checków`);
  scheduleDailyLegitChannelRename({ force: forceChannelRename });
}

async function renameDailyLegitChannel() {
  const channel = await client.channels.fetch(DAILY_LEGIT_CHANNEL_ID).catch(() => null);
  if (!channel || typeof channel.setName !== "function") return;
  const desiredName = `🗓️×〢dzienne-lc➔${dailyLegitStats.total}`;
  if (channel.name === desiredName) return;
  await channel.setName(desiredName, "Aktualizacja dziennego licznika legit checków");
  lastDailyLegitChannelRename = Date.now();
  console.log(`[daily-legit] Zmieniono nazwę kanału na ${desiredName}`);
}

function scheduleDailyLegitChannelRename({ force = false } = {}) {
  if (dailyLegitRenameTimer) {
    clearTimeout(dailyLegitRenameTimer);
    dailyLegitRenameTimer = null;
  }

  if (force) lastDailyLegitChannelRename = 0;
  const wait = Math.max(0, DAILY_LEGIT_RENAME_COOLDOWN - (Date.now() - lastDailyLegitChannelRename));
  if (wait === 0) {
    lastDailyLegitChannelRename = Date.now();
    renameDailyLegitChannel().catch((error) =>
      console.error("[daily-legit] Błąd zmiany nazwy kanału:", error),
    );
    return;
  }

  dailyLegitRenameTimer = setTimeout(() => {
    dailyLegitRenameTimer = null;
    renameDailyLegitChannel().catch((error) =>
      console.error("[daily-legit] Błąd opóźnionej zmiany nazwy kanału:", error),
    );
  }, wait + 250);
}

function queueDailyLegitChartPublish(options = {}) {
  dailyLegitPublishQueue = dailyLegitPublishQueue
    .then(() => publishDailyLegitChart(options))
    .catch((error) => console.error("[daily-legit] Błąd publikacji wykresu:", error));
  return dailyLegitPublishQueue;
}

function recordDailyLegitRep(source, amountPln = 0) {
  rollDailyLegitStatsIfNeeded();
  const { hour } = getWarsawDateParts();
  dailyLegitStats.hourly[hour] += 1;
  dailyLegitStats.total += 1;
  dailyLegitStats.totalPln = Math.round(
    (dailyLegitStats.totalPln + Math.max(0, Number(amountPln || 0))) * 100,
  ) / 100;
  dailyLegitStats.updatedAt = new Date().toISOString();
  scheduleSavePersistentState(true);
  console.log(`[daily-legit] +1 (${source}), godzina ${hour}, razem ${dailyLegitStats.total}`);
  queueDailyLegitChartPublish();
}

function isDailyLegitMoneyTransaction(type) {
  const normalized = String(type || "").trim().toLowerCase();
  return normalized === "zakup" || normalized === "sprzedaz" || normalized === "sprzedaż";
}

function millisecondsUntilNextWarsawDay() {
  const now = Date.now();
  const currentDateKey = getWarsawDateParts(new Date(now)).dateKey;
  let low = now + 1;
  let high = now + 27 * 60 * 60 * 1000;
  while (high - low > 1000) {
    const middle = Math.floor((low + high) / 2);
    if (getWarsawDateParts(new Date(middle)).dateKey === currentDateKey) low = middle + 1;
    else high = middle;
  }
  return Math.max(1000, high - now + 500);
}

function scheduleDailyLegitMidnightRollover() {
  if (dailyLegitMidnightTimer) clearTimeout(dailyLegitMidnightTimer);
  dailyLegitMidnightTimer = setTimeout(async () => {
    const changed = rollDailyLegitStatsIfNeeded();
    if (changed) {
      await queueDailyLegitChartPublish({ forceNew: true, forceChannelRename: true });
    }
    scheduleDailyLegitMidnightRollover();
  }, millisecondsUntilNextWarsawDay());
}

async function initializeDailyLegitChart() {
  const channel = await client.channels.fetch(DAILY_LEGIT_CHANNEL_ID).catch(() => null);
  if (channel?.isTextBased?.()) {
    await removeRecordButtonsFromOldDailyLegitMessages(channel, dailyLegitStats.messageId).catch(() => null);
  }
  const changed = rollDailyLegitStatsIfNeeded();
  await queueDailyLegitChartPublish({
    forceNew: changed,
    forceChannelRename: changed,
  });
  scheduleDailyLegitMidnightRollover();
}

// NEW: cooldowns & limits
const DROP_COOLDOWN_MS = 4 * 60 * 60 * 1000; // 4 hours per user
const OPINION_COOLDOWN_MS = 48 * 60 * 60 * 1000; // 48 hours per user
const OPINION_BTN_STAR = "<:star:1505298878096871546>";
const OPINION_STAR = "⭐";
const OPINION_NO_STAR = "☆";
const OPINION_DEFAULT_TEXT = "Transakcja przebiegła sprawnie, wszystko zgodne i bez żadnych problemów. Polecam.";
const OPINION_RATING_OPTIONS = Array.from({ length: 5 }, (_, index) => {
  const value = index + 1;
  return {
    label: `${OPINION_STAR.repeat(value)} (${value}/5)`,
    value: String(value),
    default: value === 5,
  };
});

const opinionMessageIdsByChannel = new Map();
const opinionCounterSyncInProgress = new Set();
const opinionAuthorUserIds = new Set();

function isPublishedOpinionMessage(message) {
  return Boolean(
    message?.embeds?.some((embed) => {
      const description = String(embed?.description || "");
      return description.includes("New Shop") && description.includes("OPINIA");
    }),
  );
}

function isOpinionChannel(channel) {
  if (!channel?.isTextBased?.() || !channel.guildId) return false;
  if (opinieChannels.get(channel.guildId) === channel.id) return true;
  return String(channel.name || "").toLowerCase().includes("opinie-klientow");
}

async function renameOpinionChannel(channel, count) {
  if (!channel || typeof channel.setName !== "function") return;
  const safeCount = Math.max(0, Number(count) || 0);
  const desiredName = `⭐×〢opinie-klientow➔${safeCount}`;
  if (channel.name === desiredName) return;

  await channel.setName(desiredName, "Aktualizacja licznika opinii klientów");
  console.log(`[opinie-counter] Zmieniono nazwę kanału na ${desiredName}`);
}

async function countExistingOpinions(channel) {
  if (!isOpinionChannel(channel) || opinionCounterSyncInProgress.has(channel.id)) return;
  opinionCounterSyncInProgress.add(channel.id);

  try {
    const opinionIds = new Set();
    let before;

    while (true) {
      const options = { limit: 100 };
      if (before) options.before = before;
      const batch = await channel.messages.fetch(options);
      if (!batch.size) break;

      for (const message of batch.values()) {
        if (isPublishedOpinionMessage(message)) {
          opinionIds.add(message.id);
          const desc = String(message.embeds?.[0]?.description || "");
          const authorMatch = desc.match(/<@!?(\d+)>/);
          if (authorMatch?.[1]) {
            opinionAuthorUserIds.add(authorMatch[1]);
          }
        }
      }

      before = batch.last()?.id;
      if (batch.size < 100 || !before) break;
    }

    opinionMessageIdsByChannel.set(channel.id, opinionIds);
    await renameOpinionChannel(channel, opinionIds.size);
    console.log(`[opinie-counter] Policzone istniejące opinie: ${opinionIds.size}, unikalnych autorów: ${opinionAuthorUserIds.size}`);
  } catch (error) {
    console.error("[opinie-counter] Nie udało się policzyć istniejących opinii:", error);
  } finally {
    opinionCounterSyncInProgress.delete(channel.id);
  }
}

async function initializeOpinionCounters() {
  const channels = client.channels.cache.filter((channel) => isOpinionChannel(channel));
  for (const channel of channels.values()) {
    await countExistingOpinions(channel);
  }
}

async function handleNewOpinionMessage(message) {
  if (!isOpinionChannel(message?.channel) || !isPublishedOpinionMessage(message)) return;

  const desc = String(message.embeds?.[0]?.description || "");
  const authorMatch = desc.match(/<@!?(\d+)>/);
  if (authorMatch?.[1]) {
    opinionAuthorUserIds.add(authorMatch[1]);
  }

  let opinionIds = opinionMessageIdsByChannel.get(message.channel.id);
  if (!opinionIds) {
    await countExistingOpinions(message.channel);
    opinionIds = opinionMessageIdsByChannel.get(message.channel.id);
  }
  if (!opinionIds || opinionIds.has(message.id)) return;

  opinionIds.add(message.id);
  await renameOpinionChannel(message.channel, opinionIds.size);
}

async function handleDeletedOpinionMessage(message) {
  const channelId = message?.channelId || message?.channel?.id;
  const opinionIds = channelId ? opinionMessageIdsByChannel.get(channelId) : null;
  if (!opinionIds?.delete(message.id)) return;

  const channel = message.channel || await client.channels.fetch(channelId).catch(() => null);
  await renameOpinionChannel(channel, opinionIds.size);
}

async function hasUserPublishedOpinion(userId, guild) {
  if (!userId) return false;
  if (opinionAuthorUserIds.has(userId)) return true;

  const opChannel = guild?.channels?.cache?.find((c) => isOpinionChannel(c)) ||
    (guild?.channels ? await guild.channels.fetch("1449783959306375198").catch(() => null) : null);
  if (opChannel && opinionAuthorUserIds.size === 0) {
    await countExistingOpinions(opChannel).catch(() => null);
  }
  return opinionAuthorUserIds.has(userId);
}

// FREE KASA cooldown (12h) and allowed channel
const FREE_KASA_COOLDOWN_MS = 12 * 60 * 60 * 1000;
const FREE_KASA_CHANNEL_ID = "1470103962245005454";
const REWARDS_CHANNEL_ID = "1449159372293935104";
const SPRAWDZ_ZAPROSZENIA_CHANNEL_ID = "1553811862317961307";
const FREE_KASA_CODE_EXPIRES_MS = 24 * 60 * 60 * 1000;
const FREE_KASA_REQUIRED_STATUS = ".gg/newshop";
const FREE_KASA_CASH_CLAIM_THRESHOLD = 50_000;
const FREE_KASA_HISTORY_LIMIT = 20;
const FREE_KASA_REQUIRED_STATUS_ALIASES = [
  FREE_KASA_REQUIRED_STATUS,
  "discord.gg/newshop",
];
const FREE_KASA_STATUS_GUIDE_IMAGE_NAME = "free_kasa_status_guide.png";
const FREE_KASA_STATUS_GUIDE_IMAGE_PATH = path.join(
  __dirname,
  "attached_assets",
  FREE_KASA_STATUS_GUIDE_IMAGE_NAME,
);
const FREE_KASA_SYNC_INTERVAL_MS = 30_000;

// DISBOARD auto-bump (/bump co 121 min)
const DISBOARD_APP_ID = "302050872383242240";
const DISBOARD_BUMP_COMMAND_ID = "947088344167366698";
const DISBOARD_BUMP_CHANNEL_ID = "1350603811512909914";
const DISBOARD_BUMP_INTERVAL_MS = 121 * 60 * 1000;
const DISBOARD_BUMP_RETRY_MS = 10 * 60 * 1000;
const DISBOARD_BUMP_ERROR_RETRY_MS = 5 * 60 * 1000;
const DISBOARD_BUMP_VERSION_RETRY_MS = 30 * 60 * 1000;

let disboardBumpTimeout = null;
let disboardBumpInFlight = false;
let disboardBumpCommandVersion = null;

function clearDisboardCommandVersionCache() {
  disboardBumpCommandVersion = null;
}

function isDisboardInvalidVersionError(body) {
  return (
    typeof body === "string" &&
    body.includes("INTERACTION_APPLICATION_COMMAND_INVALID_VERSION")
  );
}

function extractDisboardBumpVersionFromPayload(rawText, commandId) {
  if (!rawText || typeof rawText !== "string") return null;

  const patterns = [
    new RegExp(
      `"id"\\s*:\\s*"${commandId}"[\\s\\S]{0,400}?"version"\\s*:\\s*"?([0-9]+)"?`,
    ),
    new RegExp(
      `"version"\\s*:\\s*"?([0-9]+)"?[\\s\\S]{0,400}?"id"\\s*:\\s*"${commandId}"`,
    ),
  ];

  for (const pattern of patterns) {
    const match = rawText.match(pattern);
    if (match?.[1]) return match[1];
  }

  return null;
}

function parseDisboardCooldownMinutes(text) {
  if (!text || typeof text !== "string") return null;
  const match = text.match(/(\d+)\s*min/i);
  return match ? parseInt(match[1], 10) : null;
}

function getDisboardBumpToken() {
  return (
    process.env.DISBOARD_BUMP_USER_TOKEN ||
    process.env.DISBOARD_BUMP_TOKEN ||
    ""
  ).trim();
}

async function resolveDisboardBumpCommandVersion(
  guildId,
  bumpToken,
  forceRefresh = false,
) {
  if (disboardBumpCommandVersion && !forceRefresh) {
    return disboardBumpCommandVersion;
  }

  const envVersion = process.env.DISBOARD_BUMP_COMMAND_VERSION?.trim();
  if (envVersion && !forceRefresh) {
    disboardBumpCommandVersion = envVersion;
    console.log(
      `[DISBOARD] Wersja komendy /bump z env: ${disboardBumpCommandVersion}`,
    );
    return disboardBumpCommandVersion;
  }

  const endpoints = [
    `https://discord.com/api/v10/guilds/${guildId}/application-command-index`,
    `https://discord.com/api/v9/guilds/${guildId}/application-command-index`,
    `https://discord.com/api/v10/applications/${DISBOARD_APP_ID}/guilds/${guildId}/commands`,
    `https://discord.com/api/v10/applications/${DISBOARD_APP_ID}/commands`,
    `https://discord.com/api/v10/applications/${DISBOARD_APP_ID}/commands/${DISBOARD_BUMP_COMMAND_ID}`,
  ];

  for (const endpoint of endpoints) {
    try {
      const response = await fetch(endpoint, {
        headers: { Authorization: bumpToken },
      });
      const rawText = await response.text().catch(() => "");

      if (!response.ok) {
        console.warn(
          `[DISBOARD] Nie udało się pobrać wersji (${response.status}) z ${endpoint}`,
        );
        continue;
      }

      const versionFromRaw = extractDisboardBumpVersionFromPayload(
        rawText,
        DISBOARD_BUMP_COMMAND_ID,
      );
      if (versionFromRaw) {
        disboardBumpCommandVersion = versionFromRaw;
        console.log(
          `[DISBOARD] Wersja komendy /bump: ${disboardBumpCommandVersion}`,
        );
        return disboardBumpCommandVersion;
      }
    } catch (error) {
      console.warn("[DISBOARD] Nie udało się pobrać wersji komendy:", error);
    }
  }

  console.error(
    "[DISBOARD] Nie znaleziono wersji /bump — ustaw DISBOARD_BUMP_COMMAND_VERSION w Render (F12 → Network → interactions → Payload → version)",
  );
  return null;
}

async function invokeDisboardBump(channel) {
  const bumpToken = getDisboardBumpToken();
  if (!bumpToken) {
    console.warn(
      "[DISBOARD] Brak DISBOARD_BUMP_USER_TOKEN w .env — nie można wysłać /bump",
    );
    return { sent: false, cooldownMinutes: null };
  }

  const guildId = channel.guildId;
  if (!guildId) {
    console.error("[DISBOARD] Kanał bump nie należy do żadnego serwera");
    return { sent: false, cooldownMinutes: null };
  }

  for (let attempt = 0; attempt < 2; attempt++) {
    const commandVersion = await resolveDisboardBumpCommandVersion(
      guildId,
      bumpToken,
      attempt > 0,
    );

    if (!commandVersion) {
      return { sent: false, cooldownMinutes: null, invalidVersion: true };
    }

    const response = await fetch("https://discord.com/api/v10/interactions", {
      method: "POST",
      headers: {
        Authorization: bumpToken,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        type: 2,
        application_id: DISBOARD_APP_ID,
        guild_id: guildId,
        channel_id: channel.id,
        session_id: crypto.randomBytes(16).toString("hex"),
        data: {
          id: DISBOARD_BUMP_COMMAND_ID,
          name: "bump",
          type: 1,
          version: commandVersion,
        },
        nonce: Date.now().toString(),
      }),
    });

    const rawBody = await response.text().catch(() => "");

    if (!response.ok) {
      if (isDisboardInvalidVersionError(rawBody) && attempt === 0) {
        clearDisboardCommandVersionCache();
        console.warn(
          "[DISBOARD] Nieaktualna wersja komendy — pobieram ponownie...",
        );
        continue;
      }

      console.error(
        `[DISBOARD] Nie udało się wysłać /bump: HTTP ${response.status} ${rawBody}`,
      );
      return {
        sent: false,
        cooldownMinutes: null,
        invalidVersion: isDisboardInvalidVersionError(rawBody),
      };
    }

    return processDisboardBumpResponse(channel, rawBody);
  }

  return { sent: false, cooldownMinutes: null, invalidVersion: true };
}

function processDisboardBumpResponse(channel, rawBody) {

  let cooldownMinutes = parseDisboardCooldownMinutes(rawBody);
  if (!cooldownMinutes && rawBody) {
    try {
      const parsed = JSON.parse(rawBody);
      const embedText =
        parsed?.data?.embeds?.[0]?.description ||
        parsed?.embeds?.[0]?.description ||
        "";
      cooldownMinutes = parseDisboardCooldownMinutes(embedText);
    } catch {
      // odpowiedź nie-JSON — już sprawdziliśmy rawBody
    }
  }

  if (cooldownMinutes) {
    console.log(
      `[DISBOARD] Cooldown DISBOARD — kolejna próba za ${cooldownMinutes} min`,
    );
    return { sent: true, cooldownMinutes, invalidVersion: false };
  }

  console.log(
    `[DISBOARD] Wysłano /bump na kanale ${channel.name} (${channel.id})`,
  );
  return { sent: true, cooldownMinutes: null, invalidVersion: false };
}

function scheduleDisboardBump(delayMs = DISBOARD_BUMP_INTERVAL_MS) {
  if (disboardBumpTimeout) {
    clearTimeout(disboardBumpTimeout);
    disboardBumpTimeout = null;
  }

  disboardBumpTimeout = setTimeout(() => {
    runDisboardBumpCycle().catch((error) =>
      console.error("[DISBOARD] Błąd cyklu bumpa:", error),
    );
  }, delayMs);

  const nextAt = new Date(Date.now() + delayMs).toLocaleString("pl-PL");
  console.log(
    `[DISBOARD] Następny bump zaplanowany za ${Math.round(delayMs / 60000)} min (${nextAt})`,
  );
}

async function runDisboardBumpCycle() {
  if (disboardBumpInFlight) {
    scheduleDisboardBump(60_000);
    return;
  }

  disboardBumpInFlight = true;
  let nextDelay = DISBOARD_BUMP_INTERVAL_MS;

  try {
    const channel = await client.channels
      .fetch(DISBOARD_BUMP_CHANNEL_ID)
      .catch(() => null);

    if (!channel || !channel.isTextBased()) {
      console.error(
        `[DISBOARD] Nie znaleziono kanału bump: ${DISBOARD_BUMP_CHANNEL_ID}`,
      );
      nextDelay = DISBOARD_BUMP_RETRY_MS;
    } else {
      const result = await invokeDisboardBump(channel);
      if (result.cooldownMinutes) {
        nextDelay = (result.cooldownMinutes + 1) * 60_000;
      } else if (result.invalidVersion) {
        nextDelay = DISBOARD_BUMP_VERSION_RETRY_MS;
      } else if (result.sent) {
        nextDelay = DISBOARD_BUMP_RETRY_MS;
      } else {
        nextDelay = DISBOARD_BUMP_ERROR_RETRY_MS;
      }
    }
  } catch (error) {
    console.error("[DISBOARD] Błąd podczas wysyłania /bump:", error);
    nextDelay = DISBOARD_BUMP_ERROR_RETRY_MS;
  } finally {
    disboardBumpInFlight = false;
    scheduleDisboardBump(nextDelay);
  }
}

function setupDisboardAutoBump() {
  if (!getDisboardBumpToken()) {
    console.warn(
      "[DISBOARD] Auto-bump wyłączony — dodaj DISBOARD_BUMP_USER_TOKEN do .env (token konta z uprawnieniem /bump)",
    );
    return;
  }

  console.log(
    `[DISBOARD] Auto-bump włączony: co ${DISBOARD_BUMP_INTERVAL_MS / 60000} min na kanale ${DISBOARD_BUMP_CHANNEL_ID}`,
  );
  scheduleDisboardBump(30_000);
}

function handleDisboardBumpFeedback(message) {
  if (message.channelId !== DISBOARD_BUMP_CHANNEL_ID) return;
  if (message.author?.id !== DISBOARD_APP_ID) return;

  const description = message.embeds?.[0]?.description;
  if (!description || typeof description !== "string") return;

  const normalized = description.toLowerCase();

  if (normalized.includes("poczekaj")) {
    const cooldownMinutes = parseDisboardCooldownMinutes(description);
    if (cooldownMinutes) {
      console.log(
        `[DISBOARD] Cooldown DISBOARD — kolejna próba za ${cooldownMinutes} min`,
      );
      scheduleDisboardBump((cooldownMinutes + 1) * 60_000);
    }
    return;
  }

  if (
    normalized.includes("bump done") ||
    normalized.includes("podbito") ||
    normalized.includes("bumped")
  ) {
    scheduleDisboardBump(DISBOARD_BUMP_INTERVAL_MS);
  }
}
const FREE_KASA_ACCESS_ROLE_NAME = "free-kasa-access";
const FREE_KASA_SETUP_CACHE_MS = 2 * 60 * 1000;
const FREE_KASA_REWARD_CODE_EXPIRES_MS = 24 * 60 * 60 * 1000;
const FREE_KASA_CASH_EMOJI = "<:kasa_2:1476700165082710178>";
const FREE_KASA_SWORD_EMOJI = "<:ana_miecz:1476679184813260822>";
const FREE_KASA_PICKAXE_EMOJI = "<:ana_kilof:1476679224331862169>";
const FREE_KASA_ELYTRA_EMOJI = "<:elytra:1476679447846588416>";
const FREE_KASA_BASE_WIN_CHANCE = 2.0;
const FREE_KASA_PITY_START = 15;
const FREE_KASA_PITY_STEP = 0.5;
const FREE_KASA_PITY_CAP = 15;
const FREE_KASA_PITY_GUARANTEE_AFTER = 40;
const PURCHASE_CODE_USAGE_TEXT =
  "> `❓` × **Kodu użyjesz**, używając komendy `/zniżka <KOD>` na kanale ticketu.";
const REWARD_CODE_USAGE_TEXT =
  "> `❓` × Aby odebrać nagrodę, wpisz kod podany u góry w kategorii **ODBIERZ NAGRODĘ**.";
let INVITE_REWARD_MILESTONES = [
  { threshold: 5, amount: 90_000, label: "90k$" },
  { threshold: 10, amount: 200_000, label: "200k$" },
];
let INVITE_REWARD_THRESHOLD = 5;
let INVITE_REWARD_TEXT = "90k$";

function syncInviteRewardThresholds() {
  if (Array.isArray(INVITE_REWARD_MILESTONES) && INVITE_REWARD_MILESTONES.length > 0) {
    INVITE_REWARD_MILESTONES.sort((a, b) => a.threshold - b.threshold);
    INVITE_REWARD_THRESHOLD = INVITE_REWARD_MILESTONES[0].threshold;
    INVITE_REWARD_TEXT = INVITE_REWARD_MILESTONES[0].label;
  }
}
const BASE_SELLER_ROLE_ID = "1350786945944391733";
const SELLER_PING_ROLE_ID = "1350786945944391733";
const ROLE_LIMIT_20 = "1449448705563557918";
const ROLE_LIMIT_50 = "1449448702925209651";
const ROLE_LIMIT_100 = "1449448686156255333";
const ROLE_LIMIT_200 = "1449448860517798061";
const ROLE_LIMIT_400 = "1541553589833695393";
const ROLE_NO_LIMIT = "1541553994000891984";

const PURCHASE_STAFF_ROLE_IDS = [
  ROLE_LIMIT_20,
  ROLE_LIMIT_50,
  ROLE_LIMIT_100,
  ROLE_LIMIT_200,
  ROLE_LIMIT_400,
  ROLE_NO_LIMIT,
];
const PRIVATE_SPECIAL_PURCHASE_CATEGORY_ID = "1491435227866857483";
const ownerInviteCountingSettings = new Map(); // guildId -> boolean

const dropCooldowns = new Map(); // userId -> timestamp (ms)
const freeKasaCooldowns = new Map(); // userId -> timestamp (ms)
const opinionCooldowns = new Map(); // userId -> timestamp (ms)
const freeKasaAccessSyncInFlight = new Set();
const freeKasaAccessRoleIds = new Map();
const freeKasaChannelSetupAt = new Map();
const freeKasaRewardProgress = new Map(); // userId -> { cashBalance, totalWonCash, pendingSwords, history[] }
const rewardTicketClaims = new Map(); // channelId -> { userId, inviteMilestones, freeKasaCashToClaim, freeKasaSwordCount, createdAt }
const claimedInviteRewardMilestones = new Map(); // guildId -> Map<userId, Set<milestone>>
let freeKasaLossStreak = 0;

// Colors
const COLOR_BLUE = 0x00aaff;
const COLOR_YELLOW = 0xffd700;
const COLOR_GREEN = 0x57f287;
const COLOR_GRAY = 0x808080;
const COLOR_RED = 0x8b0000;
const COLOR_ORANGE = 0xff7a00;

// Regex patterns for payment validation
const PHONE_REGEX = /^(?:\+?\d{1,3})?\d{3,15}$/;
const EMAIL_REGEX = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
const LTC_REGEX = /^[LM3][a-km-zA-HJ-NP-Z1-9]{26,33}$|^ltc1[ac-hj-np-z02-9]{39,59}$/;

const FREE_KASA_REWARD_POOL = [
  {
    key: "cash_50k",
    kind: "reward",
    rewardText: "50k$ na Anarchia LF",
    rewardAmount: 50000,
    weight: 4,
  },
  {
    key: "cash_40k",
    kind: "reward",
    rewardText: "40k$ na Anarchia LF",
    rewardAmount: 40000,
    weight: 7,
  },
  {
    key: "cash_30k",
    kind: "reward",
    rewardText: "30k$ na Anarchia LF",
    rewardAmount: 30000,
    weight: 12,
  },
  {
    key: "cash_20k",
    kind: "reward",
    rewardText: "20k$ na Anarchia LF",
    rewardAmount: 20000,
    weight: 28,
  },
  {
    key: "cash_10k",
    kind: "reward",
    rewardText: "10k$ na Anarchia LF",
    rewardAmount: 10000,
    weight: 202,
  },
  {
    key: "discount_10",
    kind: "discount",
    rewardText: "Zniżka -10% na zakupy",
    discount: 10,
    weight: 32,
  },
  {
    key: "discount_5",
    kind: "discount",
    rewardText: "Zniżka -5% na zakupy",
    discount: 5,
    weight: 304,
  },
  {
    key: "item_sword",
    kind: "reward",
    rewardText: "Anarchiczny miecz",
    rewardItem: "Anarchiczny miecz",
    weight: 12,
  },
  {
    key: "item_pickaxe",
    kind: "reward",
    rewardText: "Anarchiczny kilof",
    rewardItem: "Anarchiczny kilof",
    weight: 7,
  },
  {
    key: "item_elytra",
    kind: "reward",
    rewardText: "ELYTRA",
    rewardItem: "ELYTRA",
    weight: 1,
  },
];
const FREE_KASA_TOTAL_WEIGHT = FREE_KASA_REWARD_POOL.reduce(
  (sum, reward) => sum + reward.weight,
  0,
);

// New maps for ticket close confirmation
const pendingTicketClose = new Map(); // channelId -> { userId, ts }
const ticketRepMessages = new Map(); // channelId or messageId -> repMessage string
const closingTickets = new Set(); // channelId - currently being deleted, block re-close

// ------------------ Invite tracking & protections ------------------
const guildInvites = new Map(); // guildId -> Map<code, uses>
const guildVanityUses = new Map(); // guildId -> last known vanity invite uses
const inviteCounts = new Map(); // guildId -> Map<inviterId, count>  (current cycle count)
const inviterOfMember = new Map(); // `${guildId}:${memberId}` -> inviterId


// Nowa struktura do śledzenia nagród za konkretne progi
// guildId -> Map<userId, Set<rewardLevel>> gdzie rewardLevel to "5", "10", "15", etc.
const inviteRewardLevels = new Map();

// additional maps:
const inviteRewards = new Map(); // guildId -> Map<inviterId, rewardsGiven>
const inviterRateLimit = new Map(); // guildId -> Map<inviterId, [timestamps]> to limit invites per hour
// track members who left so we can undo "leave" counters if they rejoin
const leaveRecords = new Map(); // key = `${guildId}:${memberId}` -> inviterId
const recentDeletedInvites = new Map(); // guildId -> [{ code, inviterId, deletedAt, uses }]

function getStoredInviterId(record) {
  if (!record) return null;
  if (typeof record === "string") return record;
  return typeof record.inviterId === "string" ? record.inviterId : null;
}

function rememberDeletedInvite(invite) {
  if (!invite?.guild?.id || !invite.code) return;

  const guildId = invite.guild.id;
  const now = Date.now();
  const existing = recentDeletedInvites.get(guildId) || [];
  const trimmed = existing.filter((entry) => now - entry.deletedAt < 30_000);

  trimmed.push({
    code: invite.code,
    inviterId: invite.inviter?.id || null,
    deletedAt: now,
    uses: invite.uses || 0,
  });

  recentDeletedInvites.set(guildId, trimmed);
}

function consumeRecentDeletedInvite(guildId) {
  const now = Date.now();
  const existing = recentDeletedInvites.get(guildId) || [];
  const trimmed = existing
    .filter((entry) => now - entry.deletedAt < 30_000)
    .sort((a, b) => b.deletedAt - a.deletedAt);

  if (!trimmed.length) {
    recentDeletedInvites.delete(guildId);
    return null;
  }

  const [latest, ...rest] = trimmed;
  if (rest.length) {
    recentDeletedInvites.set(guildId, rest);
  } else {
    recentDeletedInvites.delete(guildId);
  }

  return latest;
}

// keep invite cache up-to-date (global listeners, NOT inside GuildMemberAdd)
client.on("inviteCreate", (invite) => {
  try {
    const map = guildInvites.get(invite.guild.id) || new Map();
    map.set(invite.code, invite.uses || 0);
    guildInvites.set(invite.guild.id, map);
    scheduleSavePersistentState();
  } catch (e) {
    console.warn("inviteCreate handler error:", e);
  }
});
client.on("inviteDelete", (invite) => {
  try {
    rememberDeletedInvite(invite);
    const map = guildInvites.get(invite.guild.id);
    if (map) {
      map.delete(invite.code);
      guildInvites.set(invite.guild.id, map);
      scheduleSavePersistentState();
    }
  } catch (e) {
    console.warn("inviteDelete handler error:", e);
  }
});

async function sendAntiNukeAlert(guild, executorId, rule, count, removedRoles, failedRoles) {
  const description = [
    "🚨 **ANTY-NUKE ZAREAGOWAŁ**",
    `> **Sprawca:** <@${executorId}> (\`${executorId}\`)`,
    `> **Wykryto:** ${rule.label}`,
    `> **Liczba:** ${count}/${rule.limit} w ${Math.round(rule.windowMs / 1000)} sekund`,
    `> **Odebrane niebezpieczne role:** ${removedRoles.length ? removedRoles.map((id) => `<@&${id}>`).join(", ") : "brak"}`,
  ];
  if (failedRoles.length) {
    description.push(`> **Nie udało się odebrać:** ${failedRoles.map((id) => `<@&${id}>`).join(", ")}`);
  }

  const alert = {
    embeds: [new EmbedBuilder().setColor(COLOR_RED).setDescription(description.join("\n")).setTimestamp()],
  };
  const logChannel = guild.channels.cache.find((channel) => {
    const name = String(channel.name || "").toLowerCase();
    return channel.isTextBased?.() && (name.includes("logi-bota") || name.includes("logi-antynuke"));
  });
  if (logChannel) await logChannel.send(alert).catch(() => null);

  const owner = await guild.fetchOwner().catch(() => null);
  if (owner) {
    await owner.send({ content: `Alarm anty-nuke na serwerze **${guild.name}**`, ...alert }).catch(() => null);
  }
}

client.on(Events.GuildAuditLogEntryCreate, async (entry, guild) => {
  try {
    const rule = ANTI_NUKE_RULES.get(entry.action);
    const executorId = entry.executorId;
    if (!rule || !executorId) return;

    // Właściciel serwera i sam bot nigdy nie podlegają anty-nuke.
    if (executorId === guild.ownerId || executorId === client.user?.id) return;

    const now = Date.now();
    const key = `${guild.id}:${executorId}:${entry.action}`;
    const recent = (antiNukeWindows.get(key) || []).filter((timestamp) => now - timestamp < rule.windowMs);
    recent.push(now);
    antiNukeWindows.set(key, recent);
    if (recent.length < rule.limit) return;

    const cooldownKey = `${guild.id}:${executorId}`;
    if (now - (antiNukeCooldowns.get(cooldownKey) || 0) < 5 * 60_000) return;
    antiNukeCooldowns.set(cooldownKey, now);

    const member = await guild.members.fetch(executorId).catch(() => null);
    const dangerousRoles = member
      ? member.roles.cache.filter((role) =>
          role.id !== guild.id &&
          !role.managed &&
          role.editable &&
          ANTI_NUKE_DANGEROUS_PERMISSIONS.some((permission) => role.permissions.has(permission)),
        )
      : null;
    const roleIds = dangerousRoles ? [...dangerousRoles.keys()] : [];
    const removedRoles = [];
    const failedRoles = [];

    for (const roleId of roleIds) {
      const removed = await member.roles
        .remove(roleId, `Anty-nuke: ${rule.label}`)
        .then(() => true)
        .catch(() => false);
      (removed ? removedRoles : failedRoles).push(roleId);
    }

    await sendAntiNukeAlert(guild, executorId, rule, recent.length, removedRoles, failedRoles);
    console.warn(`[anti-nuke] ${guild.id}: ${executorId}, ${rule.label}, ${recent.length}/${rule.limit}`);
  } catch (error) {
    console.error("[anti-nuke] Błąd obsługi wpisu audytu:", error);
  }
});
// Invite rate-limit settings (zapobiega nadużyciom liczenia zaproszeń)
const INVITER_RATE_LIMIT_WINDOW_MS = 60 * 60 * 1000; // 1 godzina
const INVITER_RATE_LIMIT_MAX = 999999; // praktycznie wyłączony limit, żeby nie ucinało zaproszeń przy większym ruchu
// track how many people left per inviter (for /sprawdz-zaproszenia)
const inviteLeaves = new Map(); // guildId -> Map<inviterId, leftCount>
// -----------------------------------------------------

client.on(Events.PresenceUpdate, async (_oldPresence, newPresence) => {
  const member = newPresence?.member;
  if (!member) return;

  const statusText = getFreeKasaStatusTextFromPresence(newPresence);
  await syncFreeKasaChannelAccess(member, { statusTextOverride: statusText }).catch(
    (error) => console.error("Błąd presenceUpdate dla free-kasa:", error),
  );
});

client.on(Events.GuildMemberAdd, async (member) => {
  // Autoverify
  if (autoVerifySettings.get(member.guild.id)) {
    const roleId = "1425935544273338532";
    await member.roles.add(roleId).catch((error) => {
      console.error(`[autoverify] Błąd nadawania roli klient dla ${member.user.tag}:`, error);
    });
  }

  await syncFreeKasaChannelAccess(member).catch((error) =>
    console.error("Błąd syncu free-kasa po dołączeniu:", error),
  );
});

// Konfiguracja Supabase
const supabaseUrl = process.env.SUPABASE_URL || 'https://your-project.supabase.co';
const supabaseKey = process.env.SUPABASE_ANON_KEY || 'your-anon-key';
const supabase = createClient(supabaseUrl, supabaseKey);

// Prefer Persistent Disk on Render, fallback to local file (tylko jako backup)
const STORE_FILE = process.env.STORE_FILE
  ? path.resolve(process.env.STORE_FILE)
  : (fs.existsSync("/opt/render/project") ? "/opt/render/project/data/legit_store.json" : path.join(__dirname, "legit_store.json"));

// Force Render persistent disk path
if (fs.existsSync("/opt/render/project")) {
  process.env.STORE_FILE = "/opt/render/project/data/legit_store.json";
}

try {
  const dir = path.dirname(STORE_FILE);
  if (dir && !fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
} catch (e) {
  console.warn("Nie udało się przygotować katalogu dla STORE_FILE:", e);
}

try {
  const exists = fs.existsSync(STORE_FILE);
  const size = exists ? fs.statSync(STORE_FILE).size : 0;
  console.log(`[state] STORE_FILE=${STORE_FILE} exists=${exists} size=${size}`);
} catch (e) {
  console.warn("[state] Nie udało się odczytać informacji o STORE_FILE:", e);
}

// -------- Persistent storage helpers (invites, tickets, legit-rep) --------
function nestedObjectToMapOfMaps(source) {
  const top = new Map();
  if (!source || typeof source !== "object") return top;
  for (const [outerKey, innerObj] of Object.entries(source)) {
    const innerMap = new Map();
    if (innerObj && typeof innerObj === "object") {
      for (const [innerKey, value] of Object.entries(innerObj)) {
        innerMap.set(innerKey, value);
      }
    }
    top.set(outerKey, innerMap);
  }
  return top;
}

function mapOfMapsToPlainObject(topMap) {
  const obj = {};
  for (const [outerKey, innerMap] of topMap.entries()) {
    obj[outerKey] = {};
    if (innerMap && typeof innerMap.forEach === "function") {
      innerMap.forEach((value, innerKey) => {
        obj[outerKey][innerKey] = value;
      });
    }
  }
  return obj;
}

let saveStateTimeout = null;
function buildPersistentStateData() {
  // Convert contests to plain object
  const contestsObj = {};
  for (const [msgId, meta] of contests.entries()) {
    // ensure meta is serializable (avoid functions)
    contestsObj[msgId] = {
      ...(meta || {}),
      endsAt: meta && meta.endsAt ? meta.endsAt : null,
    };
  }

  // Convert contest participants to plain object
  const participantsObj = {};
  for (const [msgId, setOrMap] of contestParticipants.entries()) {
    // contestParticipants may store Set or Map — normalize to array of [userId, nick] pairs
    if (setOrMap instanceof Set) {
      // Convert Set to array of [userId, ""] pairs (backward compatibility)
      participantsObj[msgId] = Array.from(setOrMap).map(userId => [userId, ""]);
    } else if (
      typeof setOrMap === "object" &&
      typeof setOrMap.forEach === "function"
    ) {
      // Convert Map(userId -> nick) to array of [userId, nick] pairs
      participantsObj[msgId] = Array.from(setOrMap.entries());
    } else {
      participantsObj[msgId] = [];
    }
  }

  // Convert contest leave blocks to plain object
  const leaveBlocksObj = {};
  if (typeof contestLeaveBlocks !== "undefined" && contestLeaveBlocks instanceof Map) {
    for (const [userId, contestBlocks] of contestLeaveBlocks.entries()) {
      if (contestBlocks && typeof contestBlocks === "object") {
        leaveBlocksObj[userId] = {};
        for (const [msgId, blockData] of Object.entries(contestBlocks)) {
          leaveBlocksObj[userId][msgId] = {
            leaveCount: blockData.leaveCount || 0,
            blockedUntil: blockData.blockedUntil || 0
          };
        }
      }
    }
  }

  // optional: serialize fourMonthBlockList if you've added it
  const fourMonthObj = {};
  if (
    typeof fourMonthBlockList !== "undefined" &&
    fourMonthBlockList instanceof Map
  ) {
    for (const [gId, setOfUsers] of fourMonthBlockList.entries()) {
      fourMonthObj[gId] = Array.from(setOfUsers || []);
    }
  }

  // Convert guildInvites to plain object
  const guildInvitesObj = {};
  if (typeof guildInvites !== "undefined" && guildInvites instanceof Map) {
    for (const [guildId, inviteMap] of guildInvites.entries()) {
      if (inviteMap && typeof inviteMap.forEach === "function") {
        guildInvitesObj[guildId] = {};
        inviteMap.forEach((uses, code) => {
          guildInvitesObj[guildId][code] = uses;
        });
      }
    }
  }

  // Convert inviterOfMember to plain object
  const inviterOfMemberObj = {};
  if (typeof inviterOfMember !== "undefined" && inviterOfMember instanceof Map) {
    for (const [key, inviterId] of inviterOfMember.entries()) {
      inviterOfMemberObj[key] = inviterId;
    }
  }

  // Convert inviterRateLimit to plain object
  const inviterRateLimitObj = {};
  if (typeof inviterRateLimit !== "undefined" && inviterRateLimit instanceof Map) {
    for (const [guildId, rateMap] of inviterRateLimit.entries()) {
      if (rateMap && typeof rateMap.forEach === "function") {
        inviterRateLimitObj[guildId] = {};
        rateMap.forEach((timestamps, inviterId) => {
          inviterRateLimitObj[guildId][inviterId] = timestamps;
        });
      }
    }
  }

  // Convert leaveRecords to plain object
  const leaveRecordsObj = {};
  if (typeof leaveRecords !== "undefined" && leaveRecords instanceof Map) {
    for (const [key, inviterId] of leaveRecords.entries()) {
      leaveRecordsObj[key] = inviterId;
    }
  }

  // Convert verificationRoles to plain object
  const verificationRolesObj = {};
  if (typeof verificationRoles !== "undefined" && verificationRoles instanceof Map) {
    for (const [guildId, roleId] of verificationRoles.entries()) {
      verificationRolesObj[guildId] = roleId;
    }
  }

  // Convert pendingVerifications to plain object
  const pendingVerificationsObj = {};
  if (typeof pendingVerifications !== "undefined" && pendingVerifications instanceof Map) {
    for (const [modalId, data] of pendingVerifications.entries()) {
      pendingVerificationsObj[modalId] = data;
    }
  }

  // Convert ticketCategories to plain object
  const ticketCategoriesObj = {};
  if (typeof ticketCategories !== "undefined" && ticketCategories instanceof Map) {
    for (const [guildId, categories] of ticketCategories.entries()) {
      ticketCategoriesObj[guildId] = categories;
    }
  }

  // Convert dropChannels to plain object
  const dropChannelsObj = {};
  if (typeof dropChannels !== "undefined" && dropChannels instanceof Map) {
    for (const [guildId, channelId] of dropChannels.entries()) {
      dropChannelsObj[guildId] = channelId;
    }
  }

  // Convert sprawdzZaproszeniaCooldowns to plain object
  const sprawdzZaproszeniaCooldownsObj = {};
  if (typeof sprawdzZaproszeniaCooldowns !== "undefined" && sprawdzZaproszeniaCooldowns instanceof Map) {
    for (const [userId, timestamp] of sprawdzZaproszeniaCooldowns.entries()) {
      sprawdzZaproszeniaCooldownsObj[userId] = timestamp;
    }
  }

  // Convert lastOpinionInstruction to plain object
  const lastOpinionInstructionObj = {};
  if (typeof lastOpinionInstruction !== "undefined" && lastOpinionInstruction instanceof Map) {
    for (const [channelId, messageId] of lastOpinionInstruction.entries()) {
      lastOpinionInstructionObj[channelId] = messageId;
    }
  }

  // Convert lastDropInstruction to plain object
  const lastDropInstructionObj = {};
  if (typeof lastDropInstruction !== "undefined" && lastDropInstruction instanceof Map) {
    for (const [channelId, messageId] of lastDropInstruction.entries()) {
      lastDropInstructionObj[channelId] = messageId;
    }
  }

  // Convert kalkulatorData to plain object
  const kalkulatorDataObj = {};
  if (typeof kalkulatorData !== "undefined" && kalkulatorData instanceof Map) {
    for (const [userId, data] of kalkulatorData.entries()) {
      kalkulatorDataObj[userId] = data;
    }
  }

  // Convert infoCooldowns to plain object
  const infoCooldownsObj = {};
  if (typeof infoCooldowns !== "undefined" && infoCooldowns instanceof Map) {
    for (const [userId, timestamp] of infoCooldowns.entries()) {
      infoCooldownsObj[userId] = timestamp;
    }
  }

  // Convert repLastInfoMessage to plain object
  const repLastInfoMessageObj = {};
  if (typeof repLastInfoMessage !== "undefined" && repLastInfoMessage instanceof Map) {
    for (const [channelId, messageId] of repLastInfoMessage.entries()) {
      repLastInfoMessageObj[channelId] = messageId;
    }
  }

  // Convert dropCooldowns to plain object
  const dropCooldownsObj = {};
  if (typeof dropCooldowns !== "undefined" && dropCooldowns instanceof Map) {
    for (const [userId, timestamp] of dropCooldowns.entries()) {
      dropCooldownsObj[userId] = timestamp;
    }
  }

  // Convert freeKasaCooldowns to plain object
  const freeKasaCooldownsObj = {};
  if (typeof freeKasaCooldowns !== "undefined" && freeKasaCooldowns instanceof Map) {
    for (const [userId, timestamp] of freeKasaCooldowns.entries()) {
      freeKasaCooldownsObj[userId] = timestamp;
    }
  }

  const freeKasaRewardProgressObj = {};
  if (
    typeof freeKasaRewardProgress !== "undefined" &&
    freeKasaRewardProgress instanceof Map
  ) {
    for (const [userId, progress] of freeKasaRewardProgress.entries()) {
      freeKasaRewardProgressObj[userId] = {
        cashBalance: Number(progress?.cashBalance || 0),
        totalWonCash: Number(progress?.totalWonCash || 0),
        pendingSwords: Number(progress?.pendingSwords || 0),
        history: Array.isArray(progress?.history)
          ? progress.history.slice(0, FREE_KASA_HISTORY_LIMIT)
          : [],
      };
    }
  }

  // Convert opinionCooldowns to plain object
  const opinionCooldownsObj = {};
  if (typeof opinionCooldowns !== "undefined" && opinionCooldowns instanceof Map) {
    for (const [userId, timestamp] of opinionCooldowns.entries()) {
      opinionCooldownsObj[userId] = timestamp;
    }
  }

  const rewardTicketClaimsObj = {};
  if (typeof rewardTicketClaims !== "undefined" && rewardTicketClaims instanceof Map) {
    for (const [channelId, claimData] of rewardTicketClaims.entries()) {
      rewardTicketClaimsObj[channelId] = {
        guildId: claimData?.guildId || null,
        userId: claimData?.userId || null,
        inviteMilestones: Array.isArray(claimData?.inviteMilestones)
          ? claimData.inviteMilestones
          : [],
        freeKasaCashToClaim: Number(claimData?.freeKasaCashToClaim || 0),
        freeKasaSwordCount: Number(claimData?.freeKasaSwordCount || 0),
        createdAt: Number(claimData?.createdAt || Date.now()),
      };
    }
  }

  // Convert pendingTicketClose to plain object
  const pendingTicketCloseObj = {};
  if (typeof pendingTicketClose !== "undefined" && pendingTicketClose instanceof Map) {
    for (const [channelId, data] of pendingTicketClose.entries()) {
      pendingTicketCloseObj[channelId] = data;
    }
  }

  // Convert inviteRewardLevels to plain object
  const inviteRewardLevelsObj = {};
  if (typeof inviteRewardLevels !== "undefined" && inviteRewardLevels instanceof Map) {
    for (const [guildId, userMap] of inviteRewardLevels.entries()) {
      inviteRewardLevelsObj[guildId] = {};
      if (userMap && typeof userMap.forEach === "function") {
        userMap.forEach((levelSet, userId) => {
          inviteRewardLevelsObj[guildId][userId] = Array.from(levelSet || []);
        });
      }
    }
  }

  const claimedInviteRewardMilestonesObj = {};
  if (
    typeof claimedInviteRewardMilestones !== "undefined" &&
    claimedInviteRewardMilestones instanceof Map
  ) {
    for (const [guildId, userMap] of claimedInviteRewardMilestones.entries()) {
      claimedInviteRewardMilestonesObj[guildId] = {};
      if (userMap && typeof userMap.forEach === "function") {
        userMap.forEach((levelSet, userId) => {
          claimedInviteRewardMilestonesObj[guildId][userId] = Array.from(levelSet || []);
        });
      }
    }
  }

  // Convert opinieChannels to plain object
  const opinieChannelsObj = {};
  if (typeof opinieChannels !== "undefined" && opinieChannels instanceof Map) {
    for (const [guildId, channelId] of opinieChannels.entries()) {
      opinieChannelsObj[guildId] = channelId;
    }
  }

  // Convert embedTestStates to plain object
  const embedTestStatesObj = {};
  if (typeof embedTestStates !== "undefined" && embedTestStates instanceof Map) {
    for (const [messageId, state] of embedTestStates.entries()) {
      embedTestStatesObj[messageId] = state;
    }
  }

  const regulationPanelsObj = {};
  if (
    typeof regulationPanels !== "undefined" &&
    regulationPanels instanceof Map
  ) {
    for (const [messageId, panelState] of regulationPanels.entries()) {
      regulationPanelsObj[messageId] = cloneRegulationPanelState(panelState, {
        messageId,
        persistPanel: true,
      });
    }
  }

  const data = {
    legitRepCount,
    dailyLegitStats,
    dailyLegitRecords,
    legitRepCooldown: Object.fromEntries(legitRepCooldown),
    ticketCounter: Object.fromEntries(ticketCounter),
    ticketOwners: Object.fromEntries(ticketOwners),
    inviteCounts: mapOfMapsToPlainObject(inviteCounts),
    inviteRewards: mapOfMapsToPlainObject(inviteRewards),
    inviteLeaves: mapOfMapsToPlainObject(inviteLeaves),
    inviteRewardsGiven: mapOfMapsToPlainObject(inviteRewardsGiven),
    inviteRewardLevels: inviteRewardLevelsObj,
    claimedInviteRewardMilestones: claimedInviteRewardMilestonesObj,
    inviteTotalJoined: mapOfMapsToPlainObject(inviteTotalJoined),
    inviteFakeAccounts: mapOfMapsToPlainObject(inviteFakeAccounts),
    inviteBonusInvites: mapOfMapsToPlainObject(inviteBonusInvites),
    lastInviteInstruction: Object.fromEntries(lastInviteInstruction),
    contests: contestsObj,
    contestParticipants: participantsObj,
    contestLeaveBlocks: leaveBlocksObj,
    fourMonthBlockList: fourMonthObj,
    weeklySales: Object.fromEntries(weeklySales),
    activeCodes: Object.fromEntries(activeCodes),
    guildInvites: guildInvitesObj,
    inviterOfMember: inviterOfMemberObj,
    embedTestStates: embedTestStatesObj,
    regulationPanels: regulationPanelsObj,
    inviterRateLimit: inviterRateLimitObj,
    leaveRecords: leaveRecordsObj,
    verificationRoles: verificationRolesObj,
    pendingVerifications: pendingVerificationsObj,
    ticketCategories: ticketCategoriesObj,
    ticketCategoryTemplates: Object.fromEntries(ticketCategoryTemplates),
    dropChannels: dropChannelsObj,
    sprawdzZaproszeniaCooldowns: sprawdzZaproszeniaCooldownsObj,
    lastOpinionInstruction: lastOpinionInstructionObj,
    lastDropInstruction: lastDropInstructionObj,
    kalkulatorData: kalkulatorDataObj,
    infoCooldowns: infoCooldownsObj,
    repLastInfoMessage: repLastInfoMessageObj,
    dropCooldowns: dropCooldownsObj,
    freeKasaCooldowns: freeKasaCooldownsObj,
    freeKasaRewardProgress: freeKasaRewardProgressObj,
    freeKasaLossStreak: Number(freeKasaLossStreak || 0),
    opinionCooldowns: opinionCooldownsObj,
    rewardTicketClaims: rewardTicketClaimsObj,
    pendingTicketClose: pendingTicketCloseObj,
    opinieChannels: opinieChannelsObj,
    regulationPanels: regulationPanelsObj,
    autoPrzejmijSettings: Object.fromEntries(autoPrzejmijSettings),
    autoVerifySettings: Object.fromEntries(autoVerifySettings),
    sellerPaymentProfiles: Object.fromEntries(sellerPaymentProfiles),
    sellerWarnings: Object.fromEntries(sellerWarnings),
    sellerSavedLimitRoles: Object.fromEntries(sellerSavedLimitRoles),
    globalCommissionRate: typeof globalCommissionRate === "number" ? globalCommissionRate : 0.10,
    sellerCommissionRates: Object.fromEntries(sellerCommissionRates),
    isSundayResetTriggered: Boolean(isSundayResetTriggered),
    rozliczeniaReportMessageId: rozliczeniaReportMessageId || null,
    sellerWarnMessages: Object.fromEntries(sellerWarnMessages),
    ownerInviteCountingSettings: Object.fromEntries(ownerInviteCountingSettings),
    inviteRewardMilestones: INVITE_REWARD_MILESTONES,
    calculatorRates: {
      anarchiaLifestealRate: ANARCHIA_LIFESTEAL_RATE,
      anarchiaLifestealBulkRate: ANARCHIA_LIFESTEAL_BULK_RATE,
      anarchiaLifestealBulkThresholdPln: ANARCHIA_LIFESTEAL_BULK_THRESHOLD_PLN,
      anarchiaBoxpvpRate: ANARCHIA_BOXPVP_RATE,
      minestarLfRate: MINESTAR_LF_RATE,
      minestarLfBulkRate: MINESTAR_LF_BULK_RATE,
      minestarLfBulkThresholdPln: MINESTAR_LF_BULK_THRESHOLD_PLN,
      donutSmpRate: DONUT_SMP_RATE,
      minestarSkypvpRate: MINESTAR_SKY_RATE,
    },
    sellerTicketCounts: Object.fromEntries(sellerTicketCounts),
    autoLcEnabled: Boolean(autoLcEnabled),
    autoLcNextFireAt: Number(autoLcNextFireAt || 0),
    autoLcDailyQueue: Array.isArray(autoLcDailyQueue) ? autoLcDailyQueue : [],
  };

  return data;
}

// Funkcje do obsługi Supabase
async function saveStateToSupabase(data) {
  try {
    const { error } = await supabase
      .from('bot_state')
      .upsert({
        id: 1,
        data: data,
        updated_at: new Date().toISOString()
      }, {
        onConflict: 'id'
      });

    if (error) {
      console.error('[supabase] Błąd zapisu:', error);
      return false;
    }

    console.log('[supabase] Stan zapisany pomyślnie');
    return true;
  } catch (error) {
    console.error('[supabase] Błąd podczas zapisu:', error);
    return false;
  }
}

// ----------------- FREE KASA -----------------
function pickFreeKasaReward() {
  // 15% łącznie. Dodatkowe 10 punktów procentowych trafia do 10k$ i zniżki -5%.
  // Suma wag wzrosła trzykrotnie (203 -> 609), więc szanse innych nagród są zachowane.
  const WIN_CHANCE = 15.0;

  if (Math.random() * 100 > WIN_CHANCE) {
    return null; // Pusty los
  }

  return rollFreeKasaRewardFromPool();
}

function rollFreeKasaRewardFromPool() {
  let roll = Math.floor(Math.random() * FREE_KASA_TOTAL_WEIGHT) + 1;
  for (const reward of FREE_KASA_REWARD_POOL) {
    roll -= reward.weight;
    if (roll <= 0) {
      return reward;
    }
  }

  return null;
}

function getFreeKasaRewardEmoji(reward) {
  switch (reward?.key) {
    case "cash_10k":
    case "cash_20k":
    case "cash_30k":
    case "cash_40k":
    case "cash_50k":
      return FREE_KASA_CASH_EMOJI;
    case "item_sword":
      return FREE_KASA_SWORD_EMOJI;
    case "item_pickaxe":
      return FREE_KASA_PICKAXE_EMOJI;
    case "item_elytra":
      return FREE_KASA_ELYTRA_EMOJI;
    default:
      return reward?.kind === "discount" ? "🎟️" : "🎁";
  }
}

function buildFreeKasaRewardLine(reward) {
  return `${getFreeKasaRewardEmoji(reward)} \`${reward?.rewardText || "Nagroda"}\``;
}

function buildFreeKasaResultEmbed({
  user,
  reward = null,
  loss = false,
  retryTimestamp = null,
}) {
  const description = [
    "```",
    "🎀 New Shop × FREE KASA",
    "```",
    `\`👤\` × **Użytkownik:** ${user}`,
  ];

  if (loss) {
    description.push(
      "`😢` × **Niestety, tym razem nie udało się.**",
      retryTimestamp
        ? `\`⏰\` × **Spróbuj ponownie:** <t:${retryTimestamp}:R>`
        : "`⏰` × **Spróbuj ponownie później.**",
    );
  } else if (reward?.kind === "discount") {
    description.push(
      `\`🎉\` × **Wygrałeś:** ${buildFreeKasaRewardLine(reward)}`,
      "`📩` × **Kod rabatowy został wysłany na PV.**",
    );
  } else {
    description.push(
      `\`🎉\` × **Wygrałeś:** ${buildFreeKasaRewardLine(reward)}`,
      "`📩` × **Kod odbioru został wysłany na PV.**",
    );
  }

  return new EmbedBuilder()
    .setColor(loss ? COLOR_GRAY : COLOR_YELLOW)
    .setDescription(description.join("\n"));
}

function formatRewardCashAmount(amount = 0) {
  const numeric = Number(amount || 0);
  if (!Number.isFinite(numeric) || numeric <= 0) return "0$";
  if (numeric % 1000 === 0) return `${numeric / 1000}k$`;
  return `${(numeric / 1000).toString().replace(".", ",")}k$`;
}

function getFreeKasaRewardProgress(userId) {
  const existing = freeKasaRewardProgress.get(userId);
  if (existing && typeof existing === "object") {
    existing.cashBalance = Number(existing.cashBalance || 0);
    existing.totalWonCash = Number(existing.totalWonCash || 0);
    existing.pendingSwords = Number(existing.pendingSwords || 0);
    existing.history = Array.isArray(existing.history)
      ? existing.history.slice(0, FREE_KASA_HISTORY_LIMIT)
      : [];
    return existing;
  }

  const created = {
    cashBalance: 0,
    totalWonCash: 0,
    pendingSwords: 0,
    history: [],
  };
  freeKasaRewardProgress.set(userId, created);
  return created;
}

function pushFreeKasaHistoryEntry(userId, entry) {
  const state = getFreeKasaRewardProgress(userId);
  state.history.unshift({
    kind: entry?.kind || "reward",
    rewardText: entry?.rewardText || "Nagroda",
    amount: Number(entry?.amount || 0),
    createdAt: Number(entry?.createdAt || Date.now()),
  });
  state.history = state.history.slice(0, FREE_KASA_HISTORY_LIMIT);
  freeKasaRewardProgress.set(userId, state);
  return state;
}

function registerFreeKasaRewardWin(userId, reward) {
  const state = getFreeKasaRewardProgress(userId);
  const createdAt = Date.now();

  if (reward?.rewardAmount) {
    state.cashBalance += Number(reward.rewardAmount || 0);
    state.totalWonCash += Number(reward.rewardAmount || 0);
    pushFreeKasaHistoryEntry(userId, {
      kind: "cash",
      rewardText: reward.rewardText,
      amount: reward.rewardAmount,
      createdAt,
    });
  } else {
    state.pendingSwords += 1;
    pushFreeKasaHistoryEntry(userId, {
      kind: "item",
      rewardText: reward?.rewardText || "Nagroda",
      amount: 0,
      createdAt,
    });
  }

  freeKasaRewardProgress.set(userId, state);
  scheduleSavePersistentState(true);
  return state;
}

async function createFreeKasaRewardCode(userId, reward) {
  return createTimedRewardCode({
    userId,
    rewardText: reward?.rewardText || "Nagroda",
    rewardAmount: Number(reward?.rewardAmount || 0),
    rewardItem: reward?.rewardItem || null,
    type: "free_kasa_reward",
    expiresMs: FREE_KASA_REWARD_CODE_EXPIRES_MS,
  });
}

async function createTimedRewardCode({
  userId,
  rewardText,
  rewardAmount = 0,
  rewardItem = null,
  type,
  expiresMs = FREE_KASA_REWARD_CODE_EXPIRES_MS,
}) {
  const code = normalizeCodeInput(generateCode());
  const expiresAt = Date.now() + expiresMs;
  const payload = {
    oderId: userId,
    rewardText: rewardText || "Nagroda",
    rewardAmount: Number(rewardAmount || 0),
    rewardItem: rewardItem || null,
    type,
    expiresAt,
    created: Date.now(),
  };

  activeCodes.set(code, payload);
  await persistActiveCodeAndVerify(code, payload);
  scheduleSavePersistentState(true);

  setTimeout(() => {
    activeCodes.delete(code);
    db.deleteActiveCode(code).catch(() => null);
    scheduleSavePersistentState();
  }, expiresMs);

  return {
    code,
    expiresAt,
    expiryTimestamp: Math.floor(expiresAt / 1000),
    payload,
  };
}

async function createInviteRewardCode(userId, milestone) {
  return createTimedRewardCode({
    userId,
    rewardText: `${milestone?.label || INVITE_REWARD_TEXT} na Anarchia LF`,
    rewardAmount: Number(milestone?.amount || 0),
    type: "invite_cash",
    expiresMs: FREE_KASA_REWARD_CODE_EXPIRES_MS,
  });
}

function buildCodeDeliveryDmPayload({
  code,
  rewardLine,
  expiryTimestamp,
  instructionText,
  guildId = null,
}) {
  const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);

  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      "```\n" +
      "🎁 New Shop × NAGRODA\n" +
      "```"
    )
  );
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      `> \`🏷️\` × **Kod:** \`${code}\`\n` +
      `${rewardLine}\n` +
      `> \`⏰\` × **Kod wygaśnie:** <t:${expiryTimestamp}:R>`
    )
  );
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      `${instructionText}`
    )
  );
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

  const btnKanalTicketu = new ButtonBuilder()
    .setLabel("🔨︲Stwórz ticket")
    .setStyle(ButtonStyle.Link)
    .setURL("https://discord.com/channels/1350446732365926491/1449453853731983484");

  const btnSkopiujKod = new ButtonBuilder()
    .setCustomId(`copy_dm_code_${code}`)
    .setLabel("︲Skopiuj kod")
    .setStyle(ButtonStyle.Secondary)
    .setEmoji({ id: "1537166632613322792", name: "YES", animated: true });

  container.addActionRowComponents(
    new ActionRowBuilder().addComponents(btnKanalTicketu, btnSkopiujKod)
  );

  appendBrandFooterToContainer(container, guildId);

  return {
    components: [container],
    flags: MessageFlags.IsComponentsV2,
  };
}

function buildDiscountRedemptionPayload({ code, discount, guildId = null }) {
  const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);

  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      "```\n" +
      "📉 New Shop × WYKORZYSTANO KOD RABATOWY\n" +
      "```",
    ),
  );
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      `> \`🏷️\` × **Kod:** \`${code}\`\n` +
      `> \`💸\` × **Otrzymany rabat:** \`-${discount}%\``,
    ),
  );

  appendBrandFooterToContainer(container, guildId);

  return {
    components: [container],
    flags: MessageFlags.IsComponentsV2,
  };
}

async function sendInviteRewardCodeDm(user, milestone, rewardCodeData) {
  const payload = buildCodeDeliveryDmPayload({
    code: rewardCodeData.code,
    rewardLine: `> \`💸\` × **Wygrałeś:** \`${milestone.label} na Anarchia LF\``,
    expiryTimestamp: rewardCodeData.expiryTimestamp,
    instructionText: REWARD_CODE_USAGE_TEXT,
    guildId: user.guild?.id || null,
  });

  await user.send(payload);
}

async function deliverPendingInviteRewardCodes(guild, userId) {
  if (!guild || !userId) {
    return { deliveredCount: 0, deliveredLabels: [], blocked: false };
  }

  if (!inviteRewardsGiven.has(guild.id)) {
    inviteRewardsGiven.set(guild.id, new Map());
  }

  const rewardsGivenMap = inviteRewardsGiven.get(guild.id);
  const displayedInvites = getInviteDisplayCount(guild.id, userId);
  const eligibleMilestones = INVITE_REWARD_MILESTONES.filter(
    (milestone) => displayedInvites >= milestone.threshold,
  );
  const alreadyGiven = Math.max(0, Number(rewardsGivenMap.get(userId) || 0));
  const milestonesToGive = eligibleMilestones.slice(alreadyGiven);

  if (!milestonesToGive.length) {
    return { deliveredCount: 0, deliveredLabels: [], blocked: false };
  }

  const targetUser = await client.users.fetch(userId).catch(() => null);
  if (!targetUser) {
    console.warn(`[invites] Nie udało się pobrać użytkownika ${userId} do wysłania kodu nagrody.`);
    return { deliveredCount: 0, deliveredLabels: [], blocked: true };
  }

  let deliveredCount = 0;
  const deliveredLabels = [];
  let blocked = false;

  for (const milestone of milestonesToGive) {
    let rewardCodeData = null;
    try {
      rewardCodeData = await createInviteRewardCode(userId, milestone);
      await sendInviteRewardCodeDm(targetUser, milestone, rewardCodeData);
      deliveredCount += 1;
      deliveredLabels.push(milestone.label);
    } catch (error) {
      blocked = true;
      if (rewardCodeData?.code) {
        activeCodes.delete(rewardCodeData.code);
        await db.deleteActiveCode(rewardCodeData.code).catch(() => null);
      }
      console.error(
        `[invites] Nie udało się wysłać kodu nagrody za próg ${milestone.threshold} do ${userId}:`,
        error,
      );
    }
  }

  if (deliveredCount > 0) {
    rewardsGivenMap.set(userId, alreadyGiven + deliveredCount);
    inviteRewardsGiven.set(guild.id, rewardsGivenMap);
    scheduleSavePersistentState(true);
  }

  return { deliveredCount, deliveredLabels, blocked };
}

function queueInviteRewardDeliveryRetry(guildId, userId, delayMs = 5000) {
  setTimeout(async () => {
    try {
      const guild =
        client.guilds.cache.get(guildId) ||
        (await client.guilds.fetch(guildId).catch(() => null));
      if (!guild) return;
      await deliverPendingInviteRewardCodes(guild, userId);
    } catch (error) {
      console.error("[invites] Błąd retry wysyłki kodu za zaproszenia:", error);
    }
  }, delayMs);
}

function queueInviteRewardDeliveryRetryBurst(guildId, userId) {
  [3000, 10000, 30000].forEach((delayMs) => {
    queueInviteRewardDeliveryRetry(guildId, userId, delayMs);
  });
}

function inviteCounterValue(value) {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
}

function getInviteDisplayCount(guildId, userId) {
  const valid = inviteCounterValue(inviteCounts.get(guildId)?.get(userId));
  const bonus = inviteCounterValue(inviteBonusInvites.get(guildId)?.get(userId));
  return valid + bonus;
}

function getClaimedInviteRewardLevels(guildId, userId) {
  if (!claimedInviteRewardMilestones.has(guildId)) {
    claimedInviteRewardMilestones.set(guildId, new Map());
  }
  const guildLevels = claimedInviteRewardMilestones.get(guildId);
  if (!guildLevels.has(userId)) {
    guildLevels.set(userId, new Set());
  }
  return guildLevels.get(userId);
}

function getAvailableInviteRewardMilestones(guildId, userId) {
  const displayedInvites = getInviteDisplayCount(guildId, userId);
  const claimedLevels = getClaimedInviteRewardLevels(guildId, userId);
  const issuedLevels = getIssuedInviteRewardLevels(guildId, userId);

  return INVITE_REWARD_MILESTONES.filter(
    (milestone) =>
      displayedInvites >= milestone.threshold &&
      !claimedLevels.has(String(milestone.threshold)) &&
      !issuedLevels.has(String(milestone.threshold)),
  );
}

function getNextInviteRewardMilestone(guildId, userId) {
  const displayedInvites = getInviteDisplayCount(guildId, userId);
  const claimedLevels = getClaimedInviteRewardLevels(guildId, userId);
  const issuedLevels = getIssuedInviteRewardLevels(guildId, userId);

  return (
    INVITE_REWARD_MILESTONES.find(
      (milestone) =>
        !claimedLevels.has(String(milestone.threshold)) &&
        !issuedLevels.has(String(milestone.threshold)) &&
        displayedInvites < milestone.threshold,
    ) || null
  );
}

function getIssuedInviteRewardLevels(guildId, userId) {
  const givenCount = Math.max(
    0,
    Number(inviteRewardsGiven.get(guildId)?.get(userId) || 0),
  );

  return new Set(
    INVITE_REWARD_MILESTONES.slice(0, givenCount).map((milestone) =>
      String(milestone.threshold),
    ),
  );
}

function buildFreeKasaHistoryLines(userId, limit = 6) {
  const state = getFreeKasaRewardProgress(userId);
  const entries = Array.isArray(state.history) ? state.history.slice(0, limit) : [];
  if (!entries.length) {
    return ["• Brak zapisanej historii nagród z FREE KASA."];
  }

  return entries.map((entry) => {
    const rewardLabel =
      entry.kind === "cash" && entry.amount
        ? `${formatRewardCashAmount(entry.amount)} na Anarchia LF`
        : entry.rewardText || "Nagroda";
    const timeTag = entry.createdAt
      ? ` <t:${Math.floor(Number(entry.createdAt) / 1000)}:R>`
      : "";
    return `• ${rewardLabel}${timeTag}`;
  });
}

function getRewardClaimAvailability(guildId, userId) {
  const inviteMilestones = getAvailableInviteRewardMilestones(guildId, userId);
  const nextInviteMilestone = getNextInviteRewardMilestone(guildId, userId);
  const displayedInvites = getInviteDisplayCount(guildId, userId);
  const freeKasaState = getFreeKasaRewardProgress(userId);
  const freeKasaCashToClaim = Math.max(0, Number(freeKasaState.cashBalance || 0));
  const freeKasaCashRemainder = 0;

  return {
    displayedInvites,
    inviteMilestones,
    nextInviteMilestone,
    freeKasaState,
    freeKasaCashToClaim,
    freeKasaCashRemainder,
    freeKasaSwordCount: Number(freeKasaState.pendingSwords || 0),
    hasAnyClaim:
      inviteMilestones.length > 0 ||
      freeKasaCashToClaim > 0 ||
      Number(freeKasaState.pendingSwords || 0) > 0,
  };
}

function isRewardTicketLabel(label = "") {
  const normalized = String(label || "").toUpperCase();
  return (
    normalized === "NAGRODA" ||
    normalized === "NAGRODA ZA ZAPROSZENIA" ||
    normalized === "FREE KASA"
  );
}

function buildFreeKasaInstructionPayload(guildId = null) {
  const rawDescription = [
    "```",
    "💰 NEW SHOP × free kasa",
    "```",
    "### `📌` × Ustaw w statusie `.gg/newshop`",
    "`⏰` × Masz **1** próbę co **12** godzin",
    "`📩` × Nagrodę odebrać będziesz mógł od **1** zaproszenia!",
    "",
    "🎁 × **Nagrody do wygrania:**",
    ":arrowwhite: :kasa_2: `10k$` **/** `20k$` **/** `30k$` **/** `40k$` **/** `50k$`",
    ":arrowwhite: :jump_dirt: Zniżka -5% na zakupy",
    ":arrowwhite: :jump_dirt: Zniżka -10% na zakupy",
    ":arrowwhite: :ana_miecz: Anarchiczny miecz",
    ":arrowwhite: :ana_kilof: Anarchiczny kilof",
    ":arrowwhite: :elytra: Elytra",
  ].join("\n");

  const description = guildId
    ? replaceNamedGuildEmojis(replaceEmbedAliasTokens(rawDescription), guildId)
    : replaceEmbedAliasTokens(rawDescription);

  const embed = new EmbedBuilder()
    .setColor(COLOR_YELLOW)
    .setBrandFooter()
    .setDescription(description);

  const row = new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId("free_kasa_roll")
      .setLabel("Losuj nagrodę")
      .setStyle(ButtonStyle.Secondary)
      .setEmoji("🎰"),
    new ButtonBuilder()
      .setCustomId("free_kasa_claim")
      .setLabel("🎁︲Odbierz nagrodę")
      .setStyle(ButtonStyle.Secondary),
  );

  return {
    embeds: [embed],
    components: [row],
  };
}

function normalizeFreeKasaStatusText(value = "") {
  return (value || "")
    .toString()
    .toLowerCase()
    .replace(/\s+/g, "");
}

function getFreeKasaPresence(member) {
  if (!member) return null;
  return member.presence || member.guild?.presences?.cache?.get(member.id) || null;
}

function getFreeKasaStatusTextFromPresence(presence) {
  if (!presence?.activities?.length) {
    return "";
  }

  const customStatusActivity =
    presence.activities.find((activity) => activity?.type === 4) || null;

  if (customStatusActivity?.state) {
    return customStatusActivity.state;
  }

  return presence.activities
    .map((activity) => activity?.state || activity?.details || activity?.name || "")
    .filter(Boolean)
    .join(" ");
}

function getMemberFreeKasaStatusText(member) {
  return getFreeKasaStatusTextFromPresence(getFreeKasaPresence(member));
}

function resolveFreeKasaStatusText(member, statusTextOverride = "") {
  const rawOverride = (statusTextOverride || "").toString().trim();
  return rawOverride || getMemberFreeKasaStatusText(member);
}

function freeKasaStatusTextMatches(statusText = "") {
  const normalized = normalizeFreeKasaStatusText(statusText);
  return FREE_KASA_REQUIRED_STATUS_ALIASES.some((alias) =>
    normalized.includes(normalizeFreeKasaStatusText(alias)),
  );
}

function formatFreeKasaStatusDebug(member, statusTextOverride = "") {
  const raw = resolveFreeKasaStatusText(member, statusTextOverride).trim();
  return raw ? `\`${raw}\`` : "`brak statusu w cache bota`";
}

function memberHasFreeKasaStatus(member, statusTextOverride = "") {
  return freeKasaStatusTextMatches(
    resolveFreeKasaStatusText(member, statusTextOverride),
  );
}

async function fetchMemberWithPresence(guild, userId) {
  if (!guild || !userId) return null;

  try {
    const fetched = await guild.members.fetch({
      user: userId,
      withPresences: true,
      force: true,
      time: 10_000,
    });
    if (fetched?.first) {
      return fetched.first() || guild.members.cache.get(userId) || null;
    }
  } catch (error) {
    // ignore and fallback below
  }

  return guild.members.cache.get(userId) || (await guild.members.fetch(userId).catch(() => null));
}

async function getFreeKasaChannel(guild) {
  if (!guild) return null;
  const channel =
    guild.channels.cache.get(FREE_KASA_CHANNEL_ID) ||
    (await guild.channels.fetch(FREE_KASA_CHANNEL_ID).catch(() => null));
  return channel?.type === ChannelType.GuildText ? channel : null;
}

async function memberCanSendFreeKasa(member) {
  if (!member?.guild) return false;
  const channel = await getFreeKasaChannel(member.guild);
  if (!channel) return false;
  return channel.permissionsFor(member)?.has(PermissionFlagsBits.SendMessages) || false;
}

async function cleanupFreeKasaMemberOverwrites(channel) {
  if (!channel?.permissionOverwrites?.cache) return;

  const memberOverwrites = channel.permissionOverwrites.cache.filter(
    (overwrite) => overwrite.type === OverwriteType.Member,
  );

  for (const overwrite of memberOverwrites.values()) {
    await channel.permissionOverwrites.delete(overwrite.id).catch(() => null);
  }
}

async function cleanupFreeKasaRoleOverwrites(guild, channel, accessRole) {
  if (!guild || !channel?.permissionOverwrites?.cache || !accessRole) return;

  const botRoleIds = new Set(guild.members.me?.roles?.cache?.keys() || []);
  const protectedRoleIds = new Set([guild.id, accessRole.id, ...botRoleIds]);

  const roleOverwrites = channel.permissionOverwrites.cache.filter(
    (overwrite) =>
      overwrite.type === OverwriteType.Role && !protectedRoleIds.has(overwrite.id),
  );

  for (const overwrite of roleOverwrites.values()) {
    await channel.permissionOverwrites.delete(overwrite.id).catch(() => null);
  }
}

function isFreeKasaInstructionMessage(message) {
  if (!message || message.author?.id !== client.user?.id) return false;

  const embedMatch = message.embeds.some((embed) => {
    const description = `${embed?.title || ""}\n${embed?.description || ""}`.toLowerCase();
    return (
      description.includes("new shop × free kasa") &&
      (description.includes(".gg/newshop") ||
        description.includes("wymagany status") ||
        description.includes("użyj komendy"))
    );
  });

  if (embedMatch) return true;

  try {
    const componentDump = JSON.stringify(
      message.components.map((component) =>
        typeof component?.toJSON === "function" ? component.toJSON() : component,
      ),
    ).toLowerCase();

    return (
      componentDump.includes("new shop × free kasa") &&
      (componentDump.includes(".gg/newshop") ||
        componentDump.includes("spróbuj swojego szczęścia"))
    );
  } catch (_error) {
    return false;
  }
}

async function cleanupFreeKasaPermissionArtifacts(guild) {
  return;
}

async function getOrCreateFreeKasaAccessRole(guild) {
  if (!guild) return null;

  const cachedRoleId = freeKasaAccessRoleIds.get(guild.id);
  if (cachedRoleId) {
    const cachedRole = guild.roles.cache.get(cachedRoleId) || null;
    if (cachedRole) return cachedRole;
  }

  let role =
    guild.roles.cache.find(
      (item) => item.name?.toLowerCase() === FREE_KASA_ACCESS_ROLE_NAME,
    ) || null;

  if (!role) {
    try {
      role = await guild.roles.create({
        name: FREE_KASA_ACCESS_ROLE_NAME,
        permissions: [],
        mentionable: false,
        hoist: false,
        reason: "Automatyczny dostęp do kanału free-kasa",
      });
    } catch (error) {
      console.error("[free-kasa] Nie udało się utworzyć roli access:", error);
      return null;
    }
  }

  freeKasaAccessRoleIds.set(guild.id, role.id);
  return role;
}

async function ensureFreeKasaChannelRoleSetup(guild, channel, role, options = {}) {
  const { force = false } = options;
  if (!guild || !channel || !role) return false;

  try {
    const setupKey = `${guild.id}:${channel.id}`;
    const lastSetupAt = freeKasaChannelSetupAt.get(setupKey) || 0;
    const everyoneOverwrite = channel.permissionOverwrites.cache.get(guild.id) || null;
    const accessOverwrite = channel.permissionOverwrites.cache.get(role.id) || null;
    const botRoleIds = new Set(guild.members.me?.roles?.cache?.keys() || []);
    const hasMemberOverwrites = channel.permissionOverwrites.cache.some(
      (overwrite) => overwrite.type === OverwriteType.Member,
    );
    const hasExtraRoleOverwrites = channel.permissionOverwrites.cache.some(
      (overwrite) =>
        overwrite.type === OverwriteType.Role &&
        ![guild.id, role.id, ...botRoleIds].includes(overwrite.id),
    );
    const baseConfigured =
      everyoneOverwrite?.deny?.has?.(PermissionFlagsBits.SendMessages) &&
      accessOverwrite?.allow?.has?.(PermissionFlagsBits.SendMessages);

    if (
      !force &&
      baseConfigured &&
      !hasMemberOverwrites &&
      !hasExtraRoleOverwrites &&
      Date.now() - lastSetupAt < FREE_KASA_SETUP_CACHE_MS
    ) {
      return true;
    }

    await cleanupFreeKasaMemberOverwrites(channel);
    await cleanupFreeKasaRoleOverwrites(guild, channel, role);

    await channel.permissionOverwrites
      .edit(guild.id, { SendMessages: false })
      .catch((error) => {
        console.error("[free-kasa] Nie udało się ustawić deny dla @everyone:", error);
      });

    await channel.permissionOverwrites
      .edit(role.id, { SendMessages: true, ViewChannel: true, ReadMessageHistory: true })
      .catch((error) => {
        console.error("[free-kasa] Nie udało się ustawić allow dla roli access:", error);
      });
    freeKasaChannelSetupAt.set(setupKey, Date.now());

    return true;
  } catch (error) {
    console.error("[free-kasa] Błąd konfiguracji kanału pod rolę access:", error);
    return false;
  }
}

async function syncFreeKasaChannelAccess(member, options = {}) {
  return;
}

async function syncTrackedFreeKasaMembers(guild) {
  return;
}

async function refreshFreeKasaInstruction(channel) {
  if (!channel?.isTextBased?.()) return;

  try {
    if (channel.guild?.id) {
      await ensureEmbedTestEmojiCache(channel.guild.id);
    }

    const recentMessages = await channel.messages.fetch({ limit: 30 }).catch(() => null);
    if (recentMessages?.size) {
      for (const message of recentMessages.values()) {
        if (isFreeKasaInstructionMessage(message) && message.deletable) {
          await message.delete().catch(() => null);
        }
      }
    }

    const sent = await channel.send(
      buildFreeKasaInstructionPayload(channel.guild?.id || null),
    );
    lastFreeKasaInstruction.set(channel.id, sent.id);
  } catch (error) {
    console.error("Błąd odświeżania instrukcji free-kasa:", error);
  }
}

async function updateFreeKasaInstructionInPlace() {
  const channel = await client.channels.fetch(FREE_KASA_CHANNEL_ID).catch(() => null);
  if (!channel?.isTextBased?.()) return;

  const recent = await channel.messages.fetch({ limit: 100 }).catch(() => null);
  const existing = recent?.find((message) => isFreeKasaInstructionMessage(message));
  if (!existing?.editable) return;

  await existing.edit(buildFreeKasaInstructionPayload(channel.guildId));
  lastFreeKasaInstruction.set(channel.id, existing.id);
  console.log(`[free-kasa] Zaktualizowano istniejący panel bez ponownego wysyłania: ${existing.id}`);
}

function buildFreeKasaInstructionPayload(guildId = null) {
  const container = new ContainerBuilder().setAccentColor(COLOR_YELLOW);

  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      "```\n" +
      "💰 New Shop × Wylosuj nagrodę\n" +
      "```"
    )
  );

  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

  const rawBody = [
    "### `📌` × Ustaw w statusie `.gg/newshop`",
    "`⏰` × Masz **1** próbę co **12** godzin",
    "`📩` × Nagrodę odebrać będziesz mógł od **1** zaproszenia!",
    "",
    "🎁 × **Nagrody do wygrania:**",
    ":arrowwhite: :kasa_2: `10k$` **/** `20k$` **/** `30k$` **/** `40k$` **/** `50k$`",
    ":arrowwhite: :jump_dirt: Zniżka -5% na zakupy",
    ":arrowwhite: :jump_dirt: Zniżka -10% na zakupy",
    ":arrowwhite: :ana_miecz: Anarchiczny miecz",
    ":arrowwhite: :ana_kilof: Anarchiczny kilof",
    ":arrowwhite: :elytra: Elytra",
  ].join("\n");

  const bodyContent = guildId
    ? replaceNamedGuildEmojis(replaceEmbedAliasTokens(rawBody), guildId)
    : replaceEmbedAliasTokens(rawBody);

  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(bodyContent)
  );

  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

  const btnRoll = new ButtonBuilder()
    .setCustomId("free_kasa_roll")
    .setLabel("︲Losuj nagrodę")
    .setStyle(ButtonStyle.Secondary)
    .setEmoji("🎰");

  const btnClaim = new ButtonBuilder()
    .setCustomId("free_kasa_claim")
    .setLabel("🎁︲Odbierz nagrodę")
    .setStyle(ButtonStyle.Secondary);

  container.addActionRowComponents(
    new ActionRowBuilder().addComponents(btnRoll, btnClaim)
  );

  appendBrandFooterToContainer(container, guildId);

  return {
    components: [container],
    flags: MessageFlags.IsComponentsV2,
  };
}

function isFreeKasaInstructionMessage(message) {
  if (!message || message.author?.id !== client.user?.id) return false;

  const matchesDescription = (text) => {
    const normalized = String(text || "").toLowerCase();
    const hasHeader =
      normalized.includes("new shop × wylosuj nagrodę") ||
      normalized.includes("new shop × free kasa");
    const hasBody =
      normalized.includes(".gg/newshop") &&
      (normalized.includes("nagrody do wygrania") ||
        normalized.includes("wylosuj nagrodę") ||
        normalized.includes("free kasa"));
    return hasHeader && hasBody;
  };

  const embedMatch = message.embeds.some((embed) =>
    matchesDescription(`${embed?.title || ""}\n${embed?.description || ""}`),
  );
  if (embedMatch) return true;

  try {
    const componentDump = JSON.stringify(
      message.components.map((component) =>
        typeof component?.toJSON === "function" ? component.toJSON() : component,
      ),
    );
    return matchesDescription(componentDump);
  } catch (_error) {
    return false;
  }
}

function buildFreeKasaResultEmbed({
  user,
  reward = null,
  loss = false,
  retryTimestamp = null,
}) {
  const description = [
    "```",
    "🎀 New Shop × Wylosuj nagrodę",
    "```",
    `\`👤\` × **Użytkownik:** ${user}`,
  ];

  if (loss) {
    description.push(
      "`😢` × **Niestety, tym razem nie udało się.**",
      retryTimestamp
        ? `\`⏰\` × **Spróbuj ponownie:** <t:${retryTimestamp}:R>`
        : "`⏰` × **Spróbuj ponownie później.**",
    );
  } else if (reward?.kind === "discount") {
    description.push(
      `\`🎉\` × **Wygrałeś:** ${buildFreeKasaRewardLine(reward)}`,
      "`📩` × **Kod rabatowy wysłałem Ci na prywatne wiadomości.**",
    );
  } else {
    description.push(
      `\`🎉\` × **Wygrałeś:** ${buildFreeKasaRewardLine(reward)}`,
      "`📩` × **Kod odbioru wysłałem Ci na prywatne wiadomości.**",
    );
  }

  return new EmbedBuilder()
    .setColor(loss ? COLOR_GRAY : COLOR_YELLOW)
    .setDescription(description.join("\n"));
}

function buildFreeKasaResultPayload({ user, guildId, reward = null, loss = false, retryTimestamp = null }) {
  const embed = buildFreeKasaResultEmbed({ user, reward, loss, retryTimestamp });
  const container = new ContainerBuilder().setAccentColor(embed.data.color);
  const description = embed.data.description;
  const headerEnd = description.indexOf("```", 3) + 3;
  container.addTextDisplayComponents(new TextDisplayBuilder().setContent(description.slice(0, headerEnd)));
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
  container.addTextDisplayComponents(new TextDisplayBuilder().setContent(description.slice(headerEnd).trim()));
  appendBrandFooterToContainer(container, guildId);
  return {
    components: [new TextDisplayBuilder().setContent(`<@${user.id}>`), container],
    flags: MessageFlags.IsComponentsV2,
    allowedMentions: { users: [user.id] },
  };
}

async function sendFreeKasaPublicResult(interaction, payload) {
  if (typeof interaction?.isMessageComponent === "function" && interaction.isMessageComponent()) {
    if (!interaction.deferred && !interaction.replied) {
      try { await interaction.deferUpdate(); } catch (e) { }
    }
    return interaction.channel?.send(payload).catch(() => null);
  }

  if (interaction?.deferred || interaction?.replied) {
    return interaction.followUp(payload).catch(() => null);
  }

  return interaction.reply(payload).catch(() => null);
}

async function handleFreeKasaCommand(interaction) {
  const user = interaction.user;
  const guildId = interaction.guildId;
  const member =
    (await fetchMemberWithPresence(interaction.guild, user.id)) ||
    interaction.member;

  if (!guildId) {
    await interaction.reply({
      content: "> `❌` × **Ta komenda** działa tylko na **serwerze**!",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // wymagany kanał
  if (interaction.channelId !== FREE_KASA_CHANNEL_ID) {
    await interaction.reply({
      content: `> \`❌\` × Użyj tej **komendy** na kanale <#${FREE_KASA_CHANNEL_ID}>`,
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const channel = interaction.channel;

  if (!memberHasFreeKasaStatus(member)) {
    let statusGuideAttachment = null;
    if (fs.existsSync(FREE_KASA_STATUS_GUIDE_IMAGE_PATH)) {
      try {
        statusGuideAttachment = new AttachmentBuilder(
          FREE_KASA_STATUS_GUIDE_IMAGE_PATH,
          { name: FREE_KASA_STATUS_GUIDE_IMAGE_NAME },
        );
      } catch (error) {
        console.warn(
          "[free-kasa] Nie udało się załadować obrazka instrukcji statusu:",
          error,
        );
      }
    }

    const statusGuideEmbed = statusGuideAttachment
      ? new EmbedBuilder()
        .setColor(COLOR_GRAY)
        .setImage(`attachment://${FREE_KASA_STATUS_GUIDE_IMAGE_NAME}`)
      : null;

    await interaction.reply({
      content:
        "> `❌` × Aby **wylosować nagrodę**, ustaw status **`.gg/newshop`**\n" +
        "> `☁️` × Kliknij **profil** i szarą chmurkę obok nicku. Status musi być **aktywny**. Podgląd masz **na dole**.",
      embeds: statusGuideEmbed ? [statusGuideEmbed] : undefined,
      files: statusGuideAttachment ? [statusGuideAttachment] : undefined,
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const last = freeKasaCooldowns.get(user.id) || 0;
  const now = Date.now();
  if (now - last < FREE_KASA_COOLDOWN_MS) {
    const remaining = FREE_KASA_COOLDOWN_MS - (now - last);
    await interaction.reply({
      content: `> \`❌\` × Możesz ponownie losować nagrodę za \`${humanizeMs(remaining)}\``,
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  freeKasaCooldowns.set(user.id, now);
  scheduleSavePersistentState(true);

  try {
    await interaction.deferUpdate();
  } catch (e) { }

  const reward = pickFreeKasaReward();
  const retryTimestamp = Math.floor((now + FREE_KASA_COOLDOWN_MS) / 1000);

  if (!reward) {
    await sendFreeKasaPublicResult(interaction,
      buildFreeKasaResultPayload({ user, guildId, loss: true, retryTimestamp }));
    await refreshFreeKasaInstruction(channel);
    return;
  }

  if (reward.kind === "discount") {
    const code = generateCode();
    const expiresAt = Date.now() + FREE_KASA_CODE_EXPIRES_MS;
    const expiryTimestamp = Math.floor(expiresAt / 1000);
    const codePayload = {
      oderId: user.id,
      discount: reward.discount,
      expiresAt,
      created: Date.now(),
      type: "discount",
      rewardText: reward.rewardText,
    };
    activeCodes.set(code, codePayload);
    await db.saveActiveCode(code, codePayload);
    scheduleSavePersistentState(true);

    setTimeout(() => {
      activeCodes.delete(code);
      db.deleteActiveCode(code).catch(() => null);
      scheduleSavePersistentState();
    }, FREE_KASA_CODE_EXPIRES_MS);

    let dmDelivered = true;
    try {
      const payload = buildCodeDeliveryDmPayload({
        code,
        rewardLine: `> \`💸\` × **Otrzymałeś:** \`-${reward.discount}%\``,
        expiryTimestamp,
        instructionText: PURCHASE_CODE_USAGE_TEXT,
        guildId: interaction.guild?.id || null,
      });
      await user.send(payload);
    } catch (_error) {
      dmDelivered = false;
    }

    await sendFreeKasaPublicResult(interaction,
      buildFreeKasaResultPayload({ user, guildId, reward }));
    await refreshFreeKasaInstruction(channel);

    if (!dmDelivered) {
      await interaction.followUp({
        content:
          `> \`📩\` × Nie mogłem wysłać DM, więc masz kod tutaj: ||\`${code}\`||\n` +
          `> \`🎁\` × Nagroda: \`${reward.rewardText}\`\n` +
          `> \`⏰\` × Kod wygaśnie: <t:${expiryTimestamp}:R>`,
        flags: [MessageFlags.Ephemeral],
      }).catch(() => null);
    }

    return;
  }

  const rewardCodeData = await createFreeKasaRewardCode(user.id, reward);

  let dmDelivered = true;
  try {
    const payload = buildCodeDeliveryDmPayload({
      code: rewardCodeData.code,
      rewardLine: `> \`💸\` × **Wygrałeś:** \`${reward.rewardText}\``,
      expiryTimestamp: rewardCodeData.expiryTimestamp,
      instructionText: REWARD_CODE_USAGE_TEXT,
      guildId: interaction.guild?.id || null,
    });
    await user.send(payload);
  } catch (_error) {
    dmDelivered = false;
  }

  await sendFreeKasaPublicResult(interaction,
    buildFreeKasaResultPayload({ user, guildId, reward }));
  await refreshFreeKasaInstruction(channel);

  if (!dmDelivered) {
    await interaction.followUp({
      content:
        `> \`📩\` × Nie mogłem wysłać DM, więc masz kod tutaj: ||\`${rewardCodeData.code}\`||\n` +
        `> \`🎁\` × Wygrałeś: \`${reward.rewardText}\`\n` +
        `> \`🕑\` × Kod wygaśnie za: <t:${rewardCodeData.expiryTimestamp}:R>`,
      flags: [MessageFlags.Ephemeral],
    }).catch(() => null);
  }
}

function buildDmAutoReplyPayload(guildId = null) {
  const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);

  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      "```\n" +
      "❓ New Shop × POMOC\n" +
      "```"
    )
  );
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      "> `👋` × **Cześć! Jestem jedynie botem.**\n" +
      "> `📝` × Jeżeli masz **jakiekolwiek pytanie**, otwórz ticket w kategorii **POMOC**!"
    )
  );
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

  const btnKanalTicketu = new ButtonBuilder()
    .setLabel("🔨︲Stwórz ticket")
    .setStyle(ButtonStyle.Link)
    .setURL("https://discord.com/channels/1350446732365926491/1449453853731983484");

  container.addActionRowComponents(
    new ActionRowBuilder().addComponents(btnKanalTicketu)
  );

  appendBrandFooterToContainer(container, guildId);

  return {
    components: [container],
    flags: MessageFlags.IsComponentsV2,
  };
}

const ticketHistoryStore = new Map(); // shortTicketId OR channelId -> ticketRecord

function generateTicketShortCode(channelId = "") {
  if (!channelId) {
    const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let res = "";
    for (let i = 0; i < 12; i++) {
      res += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return res;
  }
  const crypto = require("crypto");
  const hash = crypto.createHash("md5").update(`ticket_${channelId}`).digest("base64url");
  return hash.slice(0, 12);
}

function buildTicketClosedDmPayload({
  powod,
  channelId = null,
  closedTimestamp = null,
  guildId = null,
}) {
  const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);

  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      "```\n" +
      "🎫 New Shop × TICKETY\n" +
      "```"
    )
  );
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

  const noEmoji = '<a:NO:1537166621527908382>';
  const arrowEmoji = '<a:arrowwhite:1491476759290449984>';
  const shortTicketId = generateTicketShortCode(channelId);
  const safeClosedTime = closedTimestamp || Math.floor(Date.now() / 1000);

  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      `### \`❓\` × **Twój ticket został zamknięty z powodu:**\n` +
      `> \`🗒️\` × \`${powod}\``
    )
  );
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      `### \`🛍️\` × **Informację o tickecie:**\n` +
      `> ${arrowEmoji} × **ID Ticketa:** \`${shortTicketId}\`\n` +
      `> ${arrowEmoji} × **Zachowaj ten identyfikator!** Przyda się w razie pytań lub reklamacji.\n` +
      `> ${arrowEmoji} × **Data zamknięcia:** <t:${safeClosedTime}:f>`
    )
  );

  appendBrandFooterToContainer(container, guildId);

  return {
    components: [container],
    flags: MessageFlags.IsComponentsV2,
  };
}

function buildWezwijDmPayload({
  isOwner,
  channelLink,
  channelId,
  guildId = null,
}) {
  const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);

  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      "```\n" +
      "🚨 New Shop × JESTEŚ WZYWANY\n" +
      "```"
    )
  );
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

  const arrowEmoji = '<a:arrowwhite:1491476759290449984>';
  const textContent = isOwner
    ? `${arrowEmoji} **jesteś wzywany** na **swojego ticketa**!\n` +
      `${arrowEmoji} **Masz** **__4 godziny__** na odpowiedź lub ticket **zostanie zamknięty!**`
    : `${arrowEmoji} **jesteś wzywany** na kanał`;

  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(textContent)
  );
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

  const btnPrzejdz = new ButtonBuilder()
    .setLabel("📫︲Przejdź na ticket")
    .setStyle(ButtonStyle.Link)
    .setURL(channelLink);

  container.addActionRowComponents(
    new ActionRowBuilder().addComponents(btnPrzejdz)
  );

  appendBrandFooterToContainer(container, guildId);

  return {
    components: [container],
    flags: MessageFlags.IsComponentsV2,
  };
}

// Handler dla komendy /wezwij
async function handleWezwijCommand(interaction) {
  const channel = interaction.channel;

  if (!channel || channel.type !== ChannelType.GuildText || !isTicketChannel(channel)) {
    await interaction.reply({
      content: "> `❌` × Użyj tej komendy na kanale ticketu.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // Wybrana osoba jest opcją wyłącznie właściciela, przed nadaniem dostępu i wysłaniem DM.
  const targetUser = interaction.options.getUser("uzytkownik");
  if (!isGuildOwner(interaction) && (!isActiveSeller(interaction) || targetUser || interaction.commandName === "wezwij-osobe")) {
    await interaction.reply({
      content: "> `❌` × Brak uprawnień do użycia tej komendy.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const channelLink = `https://discord.com/channels/${interaction.guildId}/${channel.id}`;

  if (targetUser) {
    try {
      // Nadaj uprawnienia do kanału dla wzywanej osoby
      await channel.permissionOverwrites.edit(targetUser.id, {
        ViewChannel: true,
        SendMessages: true,
        ReadMessageHistory: true,
      });

      const payload = buildWezwijDmPayload({
        isOwner: false,
        channelLink,
        channelId: channel.id,
        guildId: interaction.guildId,
      });

      await targetUser.send(payload);

      await interaction.reply({
        content: `> \`✅\` × Dodano użytkownika <@${targetUser.id}> do kanału i wysłano wezwanie na PV.`,
        flags: [MessageFlags.Ephemeral],
      });
    } catch (err) {
      console.error("[wezwij] Błąd przy wzywaniu wybranej osoby:", err);
      await interaction.reply({
        content: `> \`❌\` × Nie udało się wezwać użytkownika <@${targetUser.id}> (ma wyłączone DM lub wystąpił błąd uprawnień).`,
        flags: [MessageFlags.Ephemeral],
      });
    }
  } else {
    // Klasyczne wezwanie właściciela ticketu
    const ownerId = await resolveTicketOwnerId(channel);

    if (!ownerId) {
      await interaction.reply({
        content: "> `❌` × Nie mogę znaleźć właściciela tego ticketu.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    try {
      const user = await client.users.fetch(ownerId);

      const payload = buildWezwijDmPayload({
        isOwner: true,
        channelLink,
        channelId: channel.id,
        guildId: interaction.guildId,
      });

      await user.send(payload);

      await interaction.reply({
        content: `> \`✅\` × Wysłano wezwanie do właściciela ticketu.`,
        flags: [MessageFlags.Ephemeral],
      });
    } catch (err) {
      console.error("[wezwij] Błąd DM:", err);
      await interaction.reply({
        content: "> `❌` × Nie udało się wysłać wiadomości do właściciela (ma wyłączone DM lub nie znaleziono użytkownika).",
        flags: [MessageFlags.Ephemeral],
      });
    }
  }
}

async function loadStateFromSupabase() {
  try {
    const { data, error } = await supabase
      .from('bot_state')
      .select('data')
      .eq('id', 1)
      .single();

    if (error) {
      if (error.code === 'PGRST116') {
        console.log('[supabase] Nie znaleziono stanu, tworzę nowy');
        return null;
      }
      console.error('[supabase] Błąd odczytu:', error);
      return null;
    }

    console.log('[supabase] Stan wczytany pomyślnie');
    return data.data;
  } catch (error) {
    console.error('[supabase] Błąd podczas odczytu:', error);
    return null;
  }
}

function flushPersistentStateSync() {
  try {
    const data = buildPersistentStateData();

    // Tylko zapis do Supabase
    saveStateToSupabase(data);

    console.log(`[state] flush ok -> supabase only`);
  } catch (e) {
    console.error("[state] flush failed:", e);
  }
}

function scheduleSavePersistentState(immediate = false) {
  // debounce writes to avoid spamming disk
  if (saveStateTimeout) return;

  if (immediate) {
    // Natychmiastowy zapis dla krytycznych danych
    saveStateTimeout = setTimeout(() => {
      saveStateTimeout = null;
      try {
        const data = buildPersistentStateData();
        // Tylko zapis do Supabase
        saveStateToSupabase(data);
        console.log(`[state] immediate save ok -> supabase only`);
      } catch (err) {
        console.error("Nie udało się zapisać stanu bota (immediate):", err);
      }
    }, 100); // Bardzo krótkie opóźnienie
  } else {
    // Standardowy debounced save
    saveStateTimeout = setTimeout(() => {
      saveStateTimeout = null;
      try {
        const data = buildPersistentStateData();
        // Tylko zapis do Supabase
        saveStateToSupabase(data);
        console.log(`[state] save ok -> supabase only`);
      } catch (err) {
        console.error("Błąd serializacji stanu bota:", err);
      }
    }, 2000);
  }
}

async function loadPersistentState() {
  try {
    console.log("[state] Rozpoczynam wczytywanie stanu...");

    // Tylko wczytywanie z Supabase
    const supabaseData = await loadStateFromSupabase();

    if (supabaseData) {
      console.log("[state] Używam danych z Supabase");
      const botStateData = supabaseData;

      if (typeof botStateData.legitRepCount === "number") {
        legitRepCount = botStateData.legitRepCount;
      }

      if (typeof botStateData.autoLcEnabled === "boolean") {
        autoLcEnabled = botStateData.autoLcEnabled;
        console.log(`[state] Wczytano autoLcEnabled: ${autoLcEnabled}`);
      }

      if (typeof botStateData.autoLcNextFireAt === "number") {
        autoLcNextFireAt = botStateData.autoLcNextFireAt;
      }

      if (Array.isArray(botStateData.autoLcDailyQueue)) {
        autoLcDailyQueue = botStateData.autoLcDailyQueue.filter((ts) => typeof ts === "number");
      }

      if (botStateData.sellerTicketCounts && typeof botStateData.sellerTicketCounts === "object") {
        for (const [userId, count] of Object.entries(botStateData.sellerTicketCounts)) {
          if (typeof count === "number") {
            sellerTicketCounts.set(userId, count);
          }
        }
      }

      if (botStateData.sellerTicketCounts && typeof botStateData.sellerTicketCounts === "object") {
        for (const [userId, count] of Object.entries(botStateData.sellerTicketCounts)) {
          if (typeof count === "number") {
            sellerTicketCounts.set(userId, count);
          }
        }
      }

      dailyLegitStats = normalizeDailyLegitStats(botStateData.dailyLegitStats);
      dailyLegitRecords = normalizeDailyLegitRecords(botStateData.dailyLegitRecords);

      if (botStateData.legitRepCooldown && typeof botStateData.legitRepCooldown === "object") {
        for (const [userId, ts] of Object.entries(botStateData.legitRepCooldown)) {
          if (typeof ts === "number") {
            legitRepCooldown.set(userId, ts);
          }
        }
      }

      if (botStateData.ticketCounter && typeof botStateData.ticketCounter === "object") {
        for (const [guildId, value] of Object.entries(botStateData.ticketCounter)) {
          if (typeof value === "number") {
            ticketCounter.set(guildId, value);
          }
        }
      }

      if (botStateData.ticketOwners && typeof botStateData.ticketOwners === "object") {
        for (const [channelId, ticketData] of Object.entries(botStateData.ticketOwners)) {
          if (ticketData && typeof ticketData === "object") {
            ticketOwners.set(channelId, ticketData);
          }
        }
      }

      if (botStateData.activeCodes && typeof botStateData.activeCodes === "object") {
        for (const [storedCode, storedData] of Object.entries(botStateData.activeCodes)) {
          if (!storedData || typeof storedData !== "object") continue;
          const normalizedCode = normalizeCodeInput(storedCode);
          if (!normalizedCode) continue;
          activeCodes.set(normalizedCode, {
            ...storedData,
            expiresAt: Number(storedData.expiresAt || 0),
            used: !!storedData.used,
            rewardAmount: Number(storedData.rewardAmount || 0),
          });
        }
        console.log(`[state] Wczytano activeCodes ze stanu: ${activeCodes.size} kodów`);
      }

      if (
        botStateData.fourMonthBlockList &&
        typeof botStateData.fourMonthBlockList === "object"
      ) {
        for (const [gId, arr] of Object.entries(botStateData.fourMonthBlockList)) {
          if (Array.isArray(arr)) {
            fourMonthBlockList.set(gId, new Set(arr));
          }
        }
      }

      if (botStateData.inviteCounts) {
        const loaded = nestedObjectToMapOfMaps(botStateData.inviteCounts);
        loaded.forEach((inner, guildId) => {
          inviteCounts.set(guildId, inner);
          console.log(`[state] Wczytano inviteCounts dla guild ${guildId}: ${inner.size} wpisów`);
        });
      }

      if (botStateData.inviteRewards) {
        const loaded = nestedObjectToMapOfMaps(botStateData.inviteRewards);
        loaded.forEach((inner, guildId) => {
          inviteRewards.set(guildId, inner);
        });
      }

      if (botStateData.inviteLeaves) {
        const loaded = nestedObjectToMapOfMaps(botStateData.inviteLeaves);
        loaded.forEach((inner, guildId) => {
          inviteLeaves.set(guildId, inner);
        });
      }

      if (botStateData.inviteRewardsGiven) {
        // NEW
        const loaded = nestedObjectToMapOfMaps(botStateData.inviteRewardsGiven);
        loaded.forEach((inner, guildId) => {
          inviteRewardsGiven.set(guildId, inner);
          console.log(`[state] Wczytano inviteRewardsGiven dla guild ${guildId}: ${inner.size} wpisów`);
        });
      }

      if (botStateData.inviteRewardLevels) {
        // Load inviteRewardLevels
        for (const [guildId, userObj] of Object.entries(botStateData.inviteRewardLevels)) {
          const userMap = new Map();
          for (const [userId, levelsArray] of Object.entries(userObj)) {
            if (Array.isArray(levelsArray)) {
              userMap.set(userId, new Set(levelsArray));
            }
          }
          inviteRewardLevels.set(guildId, userMap);
        }
        console.log("[state] Wczytano inviteRewardLevels");
      }

      if (botStateData.claimedInviteRewardMilestones) {
        for (const [guildId, userObj] of Object.entries(botStateData.claimedInviteRewardMilestones)) {
          const userMap = new Map();
          for (const [userId, levelsArray] of Object.entries(userObj)) {
            if (Array.isArray(levelsArray)) {
              userMap.set(userId, new Set(levelsArray));
            }
          }
          claimedInviteRewardMilestones.set(guildId, userMap);
        }
        console.log("[state] Wczytano claimedInviteRewardMilestones");
      }

      if (
        botStateData.lastInviteInstruction &&
        typeof botStateData.lastInviteInstruction === "object"
      ) {
        for (const [channelId, messageId] of Object.entries(
          botStateData.lastInviteInstruction,
        )) {
          if (typeof messageId === "string") {
            lastInviteInstruction.set(channelId, messageId);
          }
        }
      }

      // Load contests
      if (botStateData.contests && typeof botStateData.contests === "object") {
        for (const [msgId, meta] of Object.entries(botStateData.contests)) {
          if (meta && typeof meta.endsAt === "number") {
            contests.set(msgId, meta);
            // Schedule contest end if it hasn't ended yet
            const now = Date.now();
            if (meta.endsAt > now) {
              const delay = meta.endsAt - now;
              setTimeout(() => {
                endContestByMessageId(msgId).catch((e) => console.error(e));
              }, delay);
              console.log(
                `[contests] Przywrócono konkurs ${msgId}, zakończy się za ${Math.round(delay / 1000)}s`,
              );
            } else {
              // Contest should have ended, end it now
              setImmediate(() => {
                endContestByMessageId(msgId).catch((e) => console.error(e));
              });
            }
          }
        }
      }

      // Load contest participants
      if (
        botStateData.contestParticipants &&
        typeof botStateData.contestParticipants === "object"
      ) {
        for (const [msgId, participantData] of Object.entries(botStateData.contestParticipants)) {
          if (Array.isArray(participantData)) {
            // Check if participantData is array of [userId, nick] pairs or just userIds (backward compatibility)
            if (participantData.length > 0 && Array.isArray(participantData[0])) {
              // New format: array of [userId, nick] pairs
              contestParticipants.set(msgId, new Map(participantData));
            } else {
              // Old format: array of userIds - convert to Map with empty nicks
              const participantsMap = new Map();
              participantData.forEach(userId => {
                participantsMap.set(userId, "");
              });
              contestParticipants.set(msgId, participantsMap);
            }
          }
        }
        console.log("[state] Wczytano contestParticipants");
      }

      // Load contest leave blocks
      if (
        botStateData.contestLeaveBlocks &&
        typeof botStateData.contestLeaveBlocks === "object"
      ) {
        for (const [userId, contestBlocks] of Object.entries(botStateData.contestLeaveBlocks)) {
          if (contestBlocks && typeof contestBlocks === "object") {
            const userBlocks = {};
            for (const [msgId, blockData] of Object.entries(contestBlocks)) {
              userBlocks[msgId] = {
                leaveCount: blockData.leaveCount || 0,
                blockedUntil: blockData.blockedUntil || 0
              };
            }
            contestLeaveBlocks.set(userId, userBlocks);
          }
        }
        console.log("[state] Wczytano contestLeaveBlocks");
      }

      if (botStateData.weeklySales && typeof botStateData.weeklySales === "object") {
        for (const [userId, saleData] of Object.entries(botStateData.weeklySales)) {
          if (!saleData || typeof saleData !== "object") continue;
          weeklySales.set(userId, {
            amount: Number(saleData.amount || 0),
            lastUpdate: Number(saleData.lastUpdate || Date.now()),
            paid: !!saleData.paid,
            paidAt: saleData.paidAt || null,
            guildId: saleData.guildId || null,
          });
        }
        console.log(`[state] Wczytano weeklySales ze snapshotu: ${weeklySales.size} użytkowników`);
      }

      if (
        botStateData.embedTestStates &&
        typeof botStateData.embedTestStates === "object"
      ) {
        for (const [messageId, state] of Object.entries(
          botStateData.embedTestStates,
        )) {
          if (!state || typeof state !== "object") continue;
          embedTestStates.set(messageId, state);
        }
        console.log(
          `[state] Wczytano embedTestStates: ${embedTestStates.size} stanów`,
        );
      }

      if (
        botStateData.regulationPanels &&
        typeof botStateData.regulationPanels === "object"
      ) {
        for (const [messageId, panelState] of Object.entries(
          botStateData.regulationPanels,
        )) {
          if (!panelState || typeof panelState !== "object") continue;
          regulationPanels.set(
            messageId,
            cloneRegulationPanelState(panelState, {
              messageId,
              persistPanel: true,
            }),
          );
        }
        console.log(
          `[state] Wczytano regulationPanels: ${regulationPanels.size} paneli`,
        );
      }

      // Load weekly sales from Supabase
      try {
        const sales = await db.getWeeklySales();
        sales.forEach(({ user_id, amount, paid, paid_at, guild_id, updated_at }) => {
          weeklySales.set(user_id, {
            amount: Number(amount || 0),
            lastUpdate: updated_at ? new Date(updated_at).getTime() : Date.now(),
            paid: paid || false,
            paidAt: paid_at ? new Date(paid_at).getTime() : null,
            guildId: guild_id || null,
          });
        });
        console.log(`[Supabase] Wczytano weeklySales: ${sales.length} użytkowników`);
      } catch (error) {
        console.error("[Supabase] Błąd wczytywania weeklySales:", error);
      }

      // Load active codes
      try {
        const codes = await db.getActiveCodes();
        codes.forEach(({ code, ...codeData }) => {
          const normalizedCode = normalizeCodeInput(code);
          if (!normalizedCode) return;
          // Konwertuj nazwy pól na format używany w bocie
          const botCodeData = {
            oderId: codeData.user_id,
            discount: codeData.discount,
            expiresAt: new Date(codeData.expires_at).getTime(),
            used: codeData.used,
            reward: codeData.reward,
            rewardAmount: codeData.reward_amount,
            rewardText: codeData.reward_text,
            type: codeData.type
          };
          activeCodes.set(normalizedCode, botCodeData);
        });
        console.log(`[Supabase] Wczytano activeCodes: ${codes.length} kodów`);
      } catch (error) {
        console.error("[Supabase] Błąd wczytywania activeCodes:", error);
      }

      // Ensure test discount codes exist for testing /znizka
      const defaultTestCodes = [
        { code: "TEST10", discount: 10 },
        { code: "TEST20", discount: 20 },
        { code: "TEST50", discount: 50 },
        { code: "NEWSHOP15", discount: 15 },
      ];
      const futureExpiry = Date.now() + 30 * 86400 * 1000;
      for (const tCode of defaultTestCodes) {
        if (!activeCodes.has(tCode.code)) {
          const payload = {
            discount: tCode.discount,
            expiresAt: futureExpiry,
            created: Date.now(),
            type: "discount",
            used: false,
          };
          activeCodes.set(tCode.code, payload);
          db.saveActiveCode(tCode.code, payload).catch(() => null);
        }
      }

      // Load ticket owners from Supabase
      try {
        const ticketOwnersData = await db.getTicketOwners();
        if (Array.isArray(ticketOwnersData)) {
          for (const item of ticketOwnersData) {
            if (item.channel_id) {
              ticketOwners.set(item.channel_id, item);
            }
          }
        }
        console.log(`[Supabase] Wczytano ticketOwners: ${ticketOwners.size} wpisów`);
      } catch (error) {
        console.error("[Supabase] Błąd wczytywania ticketOwners:", error);
      }

      // Load invite total joined
      if (botStateData.inviteTotalJoined) {
        const loaded = nestedObjectToMapOfMaps(botStateData.inviteTotalJoined);
        loaded.forEach((inner, guildId) => {
          inviteTotalJoined.set(guildId, inner);
        });
      }

      // Load invite fake accounts
      if (botStateData.inviteFakeAccounts) {
        const loaded = nestedObjectToMapOfMaps(botStateData.inviteFakeAccounts);
        loaded.forEach((inner, guildId) => {
          inviteFakeAccounts.set(guildId, inner);
        });
      }

      // Load invite bonus invites
      if (botStateData.inviteBonusInvites) {
        const loaded = nestedObjectToMapOfMaps(botStateData.inviteBonusInvites);
        loaded.forEach((inner, guildId) => {
          inviteBonusInvites.set(guildId, inner);
        });
      }

      // Load guildInvites
      if (botStateData.guildInvites && typeof botStateData.guildInvites === "object") {
        for (const [guildId, inviteMap] of Object.entries(botStateData.guildInvites)) {
          if (inviteMap && typeof inviteMap === "object") {
            const map = new Map();
            for (const [code, uses] of Object.entries(inviteMap)) {
              map.set(code, uses);
            }
            guildInvites.set(guildId, map);
          }
        }
      }

      // Load inviterOfMember
      if (botStateData.inviterOfMember && typeof botStateData.inviterOfMember === "object") {
        for (const [key, memberData] of Object.entries(botStateData.inviterOfMember)) {
          if (memberData && typeof memberData === "object") {
            inviterOfMember.set(key, memberData);
          }
        }
      }

      // Load inviterRateLimit
      if (botStateData.inviterRateLimit && typeof botStateData.inviterRateLimit === "object") {
        for (const [guildId, rateMap] of Object.entries(botStateData.inviterRateLimit)) {
          if (rateMap && typeof rateMap === "object") {
            const map = new Map();
            for (const [inviterId, timestamps] of Object.entries(rateMap)) {
              map.set(inviterId, timestamps);
            }
            inviterRateLimit.set(guildId, map);
          }
        }
      }

      // Load leaveRecords
      if (botStateData.leaveRecords && typeof botStateData.leaveRecords === "object") {
        for (const [key, inviterId] of Object.entries(botStateData.leaveRecords)) {
          leaveRecords.set(key, inviterId);
        }
      }

      // Load verificationRoles
      if (botStateData.verificationRoles && typeof botStateData.verificationRoles === "object") {
        for (const [guildId, roleId] of Object.entries(botStateData.verificationRoles)) {
          verificationRoles.set(guildId, roleId);
        }
      }

      // Load pendingVerifications
      if (botStateData.pendingVerifications && typeof botStateData.pendingVerifications === "object") {
        for (const [modalId, verificationData] of Object.entries(botStateData.pendingVerifications)) {
          pendingVerifications.set(modalId, verificationData);
        }
      }

      // Load ticketCategories
      for (const [guildId, templates] of Object.entries(botStateData.ticketCategoryTemplates || {})) {
        ticketCategoryTemplates.set(guildId, templates);
      }
      if (botStateData.ticketCategories && typeof botStateData.ticketCategories === "object") {
        for (const [guildId, categories] of Object.entries(botStateData.ticketCategories)) {
          ticketCategories.set(guildId, categories);
        }
      }

      // Load dropChannels
      if (botStateData.dropChannels && typeof botStateData.dropChannels === "object") {
        for (const [guildId, channelId] of Object.entries(botStateData.dropChannels)) {
          dropChannels.set(guildId, channelId);
        }
      }

      // Load sprawdzZaproszeniaCooldowns
      if (botStateData.sprawdzZaproszeniaCooldowns && typeof botStateData.sprawdzZaproszeniaCooldowns === "object") {
        for (const [userId, timestamp] of Object.entries(botStateData.sprawdzZaproszeniaCooldowns)) {
          sprawdzZaproszeniaCooldowns.set(userId, timestamp);
        }
      }

      // Load lastOpinionInstruction
      if (botStateData.lastOpinionInstruction && typeof botStateData.lastOpinionInstruction === "object") {
        for (const [channelId, messageId] of Object.entries(botStateData.lastOpinionInstruction)) {
          lastOpinionInstruction.set(channelId, messageId);
        }
      }

      // Load lastDropInstruction
      if (botStateData.lastDropInstruction && typeof botStateData.lastDropInstruction === "object") {
        for (const [channelId, messageId] of Object.entries(botStateData.lastDropInstruction)) {
          lastDropInstruction.set(channelId, messageId);
        }
      }

      // Load kalkulatorData
      if (botStateData.kalkulatorData && typeof botStateData.kalkulatorData === "object") {
        for (const [userId, calcData] of Object.entries(botStateData.kalkulatorData)) {
          kalkulatorData.set(userId, calcData);
        }
      }

      // Load infoCooldowns
      if (botStateData.infoCooldowns && typeof botStateData.infoCooldowns === "object") {
        for (const [userId, timestamp] of Object.entries(botStateData.infoCooldowns)) {
          infoCooldowns.set(userId, timestamp);
        }
      }

      // Load repLastInfoMessage
      if (botStateData.repLastInfoMessage && typeof botStateData.repLastInfoMessage === "object") {
        for (const [channelId, messageId] of Object.entries(botStateData.repLastInfoMessage)) {
          repLastInfoMessage.set(channelId, messageId);
        }
      }

      // Load dropCooldowns
      if (botStateData.dropCooldowns && typeof botStateData.dropCooldowns === "object") {
        for (const [userId, timestamp] of Object.entries(botStateData.dropCooldowns)) {
          dropCooldowns.set(userId, timestamp);
        }
      }

      // Load freeKasaCooldowns
      if (botStateData.freeKasaCooldowns && typeof botStateData.freeKasaCooldowns === "object") {
        for (const [userId, timestamp] of Object.entries(botStateData.freeKasaCooldowns)) {
          freeKasaCooldowns.set(userId, timestamp);
        }
      }

      freeKasaLossStreak = Math.max(
        0,
        Number(botStateData.freeKasaLossStreak || 0),
      );

      if (
        botStateData.freeKasaRewardProgress &&
        typeof botStateData.freeKasaRewardProgress === "object"
      ) {
        for (const [userId, progress] of Object.entries(botStateData.freeKasaRewardProgress)) {
          if (!progress || typeof progress !== "object") continue;
          freeKasaRewardProgress.set(userId, {
            cashBalance: Number(progress.cashBalance || 0),
            totalWonCash: Number(progress.totalWonCash || 0),
            pendingSwords: Number(progress.pendingSwords || 0),
            history: Array.isArray(progress.history)
              ? progress.history.slice(0, FREE_KASA_HISTORY_LIMIT)
              : [],
          });
        }
      }

      // Load opinionCooldowns
      if (botStateData.opinionCooldowns && typeof botStateData.opinionCooldowns === "object") {
        for (const [userId, timestamp] of Object.entries(botStateData.opinionCooldowns)) {
          opinionCooldowns.set(userId, timestamp);
        }
      }

      if (
        botStateData.rewardTicketClaims &&
        typeof botStateData.rewardTicketClaims === "object"
      ) {
        for (const [channelId, claimData] of Object.entries(botStateData.rewardTicketClaims)) {
          if (!claimData || typeof claimData !== "object") continue;
          rewardTicketClaims.set(channelId, {
            guildId: claimData.guildId || null,
            userId: claimData.userId || null,
            inviteMilestones: Array.isArray(claimData.inviteMilestones)
              ? claimData.inviteMilestones.map((value) => Number(value)).filter(Boolean)
              : [],
            freeKasaCashToClaim: Number(claimData.freeKasaCashToClaim || 0),
            freeKasaSwordCount: Number(claimData.freeKasaSwordCount || 0),
            createdAt: Number(claimData.createdAt || Date.now()),
          });
        }
      }

      // Load pendingTicketClose
      if (botStateData.pendingTicketClose && typeof botStateData.pendingTicketClose === "object") {
        for (const [channelId, ticketData] of Object.entries(botStateData.pendingTicketClose)) {
          pendingTicketClose.set(channelId, ticketData);
        }
      }

      // Load opinieChannels
      if (botStateData.opinieChannels && typeof botStateData.opinieChannels === "object") {
        for (const [guildId, channelId] of Object.entries(botStateData.opinieChannels)) {
          opinieChannels.set(guildId, channelId);
        }
      }

      // Load autoPrzejmijSettings
      if (botStateData.autoPrzejmijSettings && typeof botStateData.autoPrzejmijSettings === "object") {
        for (const [guildId, cfg] of Object.entries(botStateData.autoPrzejmijSettings)) {
          if (cfg && typeof cfg === "object" && cfg.enabled) {
            autoPrzejmijSettings.set(guildId, cfg);
          }
        }
      }

      // Load autoVerifySettings
      if (botStateData.autoVerifySettings && typeof botStateData.autoVerifySettings === "object") {
        for (const [guildId, val] of Object.entries(botStateData.autoVerifySettings)) {
          autoVerifySettings.set(guildId, !!val);
        }
      }

      if (
        botStateData.sellerPaymentProfiles &&
        typeof botStateData.sellerPaymentProfiles === "object"
      ) {
        for (const [profileKey, profile] of Object.entries(
          botStateData.sellerPaymentProfiles,
        )) {
          if (!profile || typeof profile !== "object") continue;
          sellerPaymentProfiles.set(profileKey, {
            phone: String(profile.phone || "").slice(0, 80),
            transferTitle: String(profile.transferTitle || "").slice(0, 120),
            recipient: String(profile.recipient || profile.receiverName || "").slice(0, 120),
            paypalEmail: String(profile.paypalEmail || "").slice(0, 120),
            ltcWallet: String(profile.ltcWallet || "").slice(0, 180),
            mypscEmail: String(profile.mypscEmail || "").slice(0, 120),
            updatedAt: Number(profile.updatedAt || Date.now()),
          });
        }
        console.log(
          `[state] Wczytano sellerPaymentProfiles: ${sellerPaymentProfiles.size} wpisów`,
        );
      }

      if (
        botStateData.sellerWarnings &&
        typeof botStateData.sellerWarnings === "object"
      ) {
        for (const [key, count] of Object.entries(botStateData.sellerWarnings)) {
          sellerWarnings.set(key, Number(count) || 0);
        }
        console.log(
          `[state] Wczytano sellerWarnings: ${sellerWarnings.size} wpisów`,
        );
      }

      if (
        botStateData.sellerSavedLimitRoles &&
        typeof botStateData.sellerSavedLimitRoles === "object"
      ) {
        for (const [key, roles] of Object.entries(botStateData.sellerSavedLimitRoles)) {
          if (Array.isArray(roles)) {
            sellerSavedLimitRoles.set(key, roles);
          }
        }
        console.log(
          `[state] Wczytano sellerSavedLimitRoles: ${sellerSavedLimitRoles.size} wpisów`,
        );
      }

      if (typeof botStateData.globalCommissionRate === "number") {
        globalCommissionRate = botStateData.globalCommissionRate;
      }
      if (
        botStateData.sellerCommissionRates &&
        typeof botStateData.sellerCommissionRates === "object"
      ) {
        for (const [key, rate] of Object.entries(botStateData.sellerCommissionRates)) {
          if (typeof rate === "number") {
            sellerCommissionRates.set(key, rate);
          }
        }
      }
      if (typeof botStateData.isSundayResetTriggered === "boolean") {
        isSundayResetTriggered = botStateData.isSundayResetTriggered;
      }
      if (botStateData.rozliczeniaReportMessageId) {
        rozliczeniaReportMessageId = botStateData.rozliczeniaReportMessageId;
      }

      if (
        botStateData.sellerWarnMessages &&
        typeof botStateData.sellerWarnMessages === "object"
      ) {
        for (const [key, msgs] of Object.entries(botStateData.sellerWarnMessages)) {
          if (Array.isArray(msgs)) {
            sellerWarnMessages.set(key, msgs);
          }
        }
        console.log(
          `[state] Wczytano sellerWarnMessages: ${sellerWarnMessages.size} wpisów`,
        );
      }

      if (
        botStateData.ownerInviteCountingSettings &&
        typeof botStateData.ownerInviteCountingSettings === "object"
      ) {
        for (const [guildId, enabled] of Object.entries(botStateData.ownerInviteCountingSettings)) {
          ownerInviteCountingSettings.set(guildId, !!enabled);
        }
      }

      if (botStateData.calculatorRates && typeof botStateData.calculatorRates === "object") {
        const rates = botStateData.calculatorRates;
        if (typeof rates.anarchiaLifestealRate === "number") ANARCHIA_LIFESTEAL_RATE = rates.anarchiaLifestealRate;
        if (typeof rates.anarchiaLifestealBulkRate === "number") ANARCHIA_LIFESTEAL_BULK_RATE = rates.anarchiaLifestealBulkRate;
        if (typeof rates.anarchiaLifestealBulkThresholdPln === "number") ANARCHIA_LIFESTEAL_BULK_THRESHOLD_PLN = rates.anarchiaLifestealBulkThresholdPln;
        if (typeof rates.anarchiaBoxpvpRate === "number") ANARCHIA_BOXPVP_RATE = rates.anarchiaBoxpvpRate;
        if (typeof rates.minestarLfRate === "number") MINESTAR_LF_RATE = rates.minestarLfRate;
        if (typeof rates.minestarLfBulkRate === "number") MINESTAR_LF_BULK_RATE = rates.minestarLfBulkRate;
        if (typeof rates.minestarLfBulkThresholdPln === "number") MINESTAR_LF_BULK_THRESHOLD_PLN = rates.minestarLfBulkThresholdPln;
        if (typeof rates.donutSmpRate === "number") DONUT_SMP_RATE = rates.donutSmpRate;
        if (typeof rates.minestarSkypvpRate === "number") MINESTAR_SKY_RATE = rates.minestarSkypvpRate;
        if (typeof rates.minestarSkyRate === "number") MINESTAR_SKY_RATE = rates.minestarSkyRate;
        console.log("[state] Wczytano calculatorRates");
      }

      if (Array.isArray(botStateData.inviteRewardMilestones)) {
        INVITE_REWARD_MILESTONES = botStateData.inviteRewardMilestones;
        syncInviteRewardThresholds();
        console.log("[state] Wczytano inviteRewardMilestones:", INVITE_REWARD_MILESTONES);
      }

      // Load pendingTicketClose
      if (botStateData.pendingTicketClose && typeof botStateData.pendingTicketClose === "object") {
        for (const [channelId, data] of Object.entries(botStateData.pendingTicketClose)) {
          pendingTicketClose.set(channelId, data);
        }
        console.log(`[state] Wczytano pendingTicketClose: ${pendingTicketClose.size} ticketów`);
      }

      try {
        let fakeGuilds = 0;
        let fakeEntries = 0;
        for (const [gId, inner] of inviteFakeAccounts.entries()) {
          fakeGuilds++;
          if (inner && typeof inner.size === "number") fakeEntries += inner.size;
        }
        console.log(
          `[state] load ok <- supabase inviteFakeAccounts guilds=${fakeGuilds} entries=${fakeEntries}`,
        );
      } catch (e) {
        // ignore
      }
      console.log("Załadowano zapisany stan bota z Supabase.");
      console.log("[state] Zakończono wczytywanie stanu");
    } else {
      console.log("[state] Nie znaleziono danych w Supabase, zaczynam z pustym stanem");
    }
  } catch (err) {
    console.error("Nie udało się odczytać stanu bota z Supabase:", err);
  }
}

function generateCode() {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  let code = "";
  for (let i = 0; i < 12; i++) {
    code += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return code;
}

function normalizeCodeInput(input) {
  return String(input || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
}

async function getActiveCodeData(codeInput) {
  const normalizedCode = normalizeCodeInput(codeInput);
  if (!normalizedCode) return { code: "", codeData: null };

  const directCached = activeCodes.get(normalizedCode);
  if (directCached) {
    return { code: normalizedCode, codeData: directCached };
  }

  for (const [storedCode, storedData] of activeCodes.entries()) {
    if (normalizeCodeInput(storedCode) === normalizedCode) {
      activeCodes.set(normalizedCode, storedData);
      return { code: normalizedCode, codeData: storedData };
    }
  }

  try {
    let found = null;
    if (typeof db.getActiveCode === "function") {
      found = await db.getActiveCode(normalizedCode);
    }
    if (!found) {
      const codes = await db.getActiveCodes();
      found = codes.find(
        (entry) => normalizeCodeInput(entry?.code) === normalizedCode,
      );
    }

    if (!found) {
      return { code: normalizedCode, codeData: null };
    }

    const hydrated = {
      oderId: found.user_id,
      discount: found.discount,
      expiresAt: found.expires_at ? new Date(found.expires_at).getTime() : 0,
      used: found.used,
      reward: found.reward,
      rewardAmount: found.reward_amount,
      rewardText: found.reward_text,
      type: found.type,
    };

    activeCodes.set(normalizedCode, hydrated);
    return { code: normalizedCode, codeData: hydrated };
  } catch (error) {
    console.error("Błąd pobierania kodu z bazy:", error);
    return { code: normalizedCode, codeData: null };
  }
}

async function persistActiveCodeAndVerify(code, payload) {
  const normalizedCode = normalizeCodeInput(code);
  await db.saveActiveCode(normalizedCode, payload);

  let verified = null;
  if (typeof db.getActiveCode === "function") {
    verified = await db.getActiveCode(normalizedCode).catch(() => null);
  }

  if (!verified) {
    const codes = await db.getActiveCodes().catch(() => []);
    verified = Array.isArray(codes)
      ? codes.find((entry) => normalizeCodeInput(entry?.code) === normalizedCode)
      : null;
  }

  if (!verified) {
    await db.saveActiveCode(normalizedCode, payload);
    verified =
      (typeof db.getActiveCode === "function"
        ? await db.getActiveCode(normalizedCode).catch(() => null)
        : null) || verified;
  }

  if (!verified) {
    console.warn(`[codes] Nie udało się zweryfikować zapisu kodu ${normalizedCode} w bazie.`);
  }
}

function getNextTicketNumber(guildId) {
  const current = ticketCounter.get(guildId) || 0;
  const next = current + 1;
  ticketCounter.set(guildId, next);
  scheduleSavePersistentState();
  return next;
}

// Load persisted state once on startup (IMMEDIATELY after maps are defined)
console.log("[state] Wywołuję loadPersistentState()...");
const persistentStateReady = loadPersistentState().then(() => {
  console.log("[state] loadPersistentState() zakończone");
}).catch(err => {
  console.error("[state] Błąd loadPersistentState():", err);
});

// Save pending shop data and await lifecycle logging before leaving the process.
function flushOnShutdown() {
  if (saveStateTimeout) { clearTimeout(saveStateTimeout); saveStateTimeout = null; }
  flushPersistentStateSync();
}
process.once("SIGINT", () => { void monitoring.shutdown("SIGINT — zatrzymanie procesu", 0, flushOnShutdown); });
process.once("SIGTERM", () => { void monitoring.shutdown("SIGTERM — zatrzymanie przez hosting lub wdrożenie", 0, flushOnShutdown); });
process.once("uncaughtException", (err) => { void monitoring.crash(err, flushOnShutdown); });

// Defaults provided by user (kept mainly for categories / names)
const DEFAULT_GUILD_ID = "1350446732365926491";
const REWARDS_CATEGORY_ID = "1449455567641907351";
const DEFAULT_NAMES = {
  dropChannelName: "🎁-×┃dropy",
  verificationRoleName: "@> | 💲 klient",
  categories: {
    "zakup-0-20": "zakup 0-20",
    "zakup-20-50": "zakup 20-50",
    "zakup-50-100": "zakup 50-100",
    "zakup-100-200": "zakup 100-200",
    "zakup-200-400": "zakup 200-400",
    "zakup-400-999": "zakup 400-999",
    sprzedaz: "sprzedaz",
    "odbior-nagrody": "odbierz nagrode",
    inne: "inne",
  },
};

const IMPORTED_PANEL_NAMES = [
  ["cennik-anarchia-lf", "Cennik Anarchia LF"],
  ["cennik-anarchia-boxpvp", "Cennik Anarchia BoxPvP"],
  ["cennik-minestar-skypvp", "Cennik MineStar SkyPvP"],
  ["cennik-minestar-lf", "Cennik MineStar LF"],
  ["cennik-donut-smp", "Cennik Donut SMP"],
  ["cennik-bedrock-finder", "Cennik Bedrock Finder"],
  ["cennik-boty", "Cennik boty"],
  ["cennik-taktyki", "Cennik taktyki"],
  ["cennik-mody", "Cennik mody"],
  ["sprzedaj-itemy", "Sprzedaj itemy"],
  ["bonusy-klientow", "Bonusy klientów"],
  ["nagrody-za-zaproszenia", "Nagrody za zaproszenia"],
  ["rekrutacja", "Rekrutacja"],
];

const PANEL_CATEGORIES = [
  { name: "Cenniki", value: "cenniki" },
  { name: "Klient i nagrody", value: "klient" },
  { name: "Serwer i tickety", value: "serwer" },
  { name: "Sprzedawca i kalkulator", value: "sprzedawca" },
];

const SENDABLE_PANELS = [
  { name: "Ticketowy (ticketpanel)", value: "ticketowy", handler: handleTicketPanelCommand },
  { name: "Panel klienta", value: "panel-klienta", handler: handlePanelKlientaCommand },
  { name: "Ustaw dane sprzedawcy", value: "ustaw-dane", handler: handlePanelDaneCommand },
  { name: "Kalkulator waluty", value: "kalkulator", handler: handlePanelKalkulatorCommand },
  { name: "Weryfikacja", value: "weryfikacja", handler: handlePanelWeryfikacjaCommand },
  { name: "Zaproszenia", value: "zaproszenia", handler: (interaction) => handlePanelZaproszenCommand(interaction, interaction.channel) },
  { name: "Opinie klientów", value: "opinie", payload: () => buildOpinionInstructionPayload() },
  { name: "Darmowa kasa", value: "darmowa-kasa", payload: (interaction) => buildFreeKasaInstructionPayload(interaction.guildId) },
  { name: "Legit checki", value: "legit-checki", send: (interaction) => sendLegitCheckInfoMessage(interaction.channel) },
  { name: "Rozliczenia sprzedawców", value: "rozliczenia", payload: (interaction) => buildRozliczeniaPanelPayload(interaction.guildId) },
  { name: "Regulamin", value: "regulamin", send: sendSavedRegulationPanel },
  ...IMPORTED_PANEL_NAMES.map(([file, name]) => ({
    name, value: name.toLocaleLowerCase("pl-PL"), send: (interaction) => sendImportedPanel(interaction, file),
  })),
].map((panel) => ({ ...panel, category: panel.name.startsWith("Cennik ") ? "cenniki"
  : ["ticketowy", "weryfikacja", "regulamin", "legit-checki", "rekrutacja"].includes(panel.value) ? "serwer"
  : ["ustaw-dane", "rozliczenia", "kalkulator"].includes(panel.value) ? "sprzedawca" : "klient" }));

function getPanelAutocompleteChoices(category, query = "") {
  const text = query.toLocaleLowerCase("pl-PL").trim();
  return SENDABLE_PANELS.filter((panel) => panel.category === category && panel.name.toLocaleLowerCase("pl-PL").includes(text))
    .slice(0, 25).map(({ name, value }) => ({ name, value }));
}

const INVITE_COUNTER_CHOICES = [
  { name: "Dodatkowe — ręcznie przyznane zaproszenia", value: "dodatkowe" },
  { name: "Prawdziwe — policzone zaproszenia", value: "prawdziwe" },
  { name: "Opuszczone — osoby, które wyszły", value: "opuszczone" },
  { name: "Nieprawidłowe — konta młodsze niż 2 miesiące", value: "mniej2mies" },
];

function buildInviteStatsCommand() {
  const command = new SlashCommandBuilder()
    .setName("zaproszenia-edytuj")
    .setDescription("Liczniki zaproszeń: podgląd i zmiany (tylko właściciel serwera)")
    .setDMPermission(false)
    .setDefaultMemberPermissions(PermissionFlagsBits.Administrator)
    .addStringOption((o) => o.setName("akcja").setDescription("Co chcesz zrobić?").setRequired(true)
      .addChoices(
        { name: "Pokaż statystyki", value: "pokaz" },
        { name: "Dodaj zaproszenia", value: "dodaj" },
        { name: "Odejmij zaproszenia", value: "odejmij" },
        { name: "Ustaw licznik", value: "ustaw" },
        { name: "Wyzeruj licznik", value: "wyzeruj" },
        { name: "Odblokuj ponowny odbiór nagród", value: "odblokuj-nagrody" },
        { name: "Zmień kwotę nagrody", value: "nagroda-kwota" }))
    .addUserOption((o) => o.setName("osoba").setDescription("Dla podglądu, zmiany licznika lub odblokowania nagród"))
    .addStringOption((o) => o.setName("licznik").setDescription("Dla dodaj, odejmij, ustaw i wyzeruj").addChoices(...INVITE_COUNTER_CHOICES))
    .addIntegerOption((o) => o.setName("liczba").setDescription("Dla dodaj/odejmij: ile; dla ustaw: nowa wartość")
      .setMinValue(0).setMaxValue(1000000))
    .addIntegerOption((o) => o.setName("prog").setDescription("Tylko przy zmianie kwoty nagrody")
      .addChoices({ name: "5 zaproszeń", value: 5 }, { name: "10 zaproszeń", value: 10 }))
    .addIntegerOption((o) => o.setName("kwota").setDescription("Tylko przy zmianie nagrody: nowa kwota w $ (np. 130000)").setMinValue(1));
  return command.toJSON();
}

const { isGuildOwner, isActiveSeller, canUseSlashCommand, applyCommandVisibility } = require("./command-access.cjs");

const commands = [
  new SlashCommandBuilder()
    .setName("panel-wyslij")
    .setDescription("Wyślij gotowy panel na bieżący kanał")
    .setDMPermission(false)
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addStringOption((option) => option.setName("kategoria").setDescription("Wybierz grupę paneli")
      .setRequired(true).addChoices(...PANEL_CATEGORIES))
    .addStringOption((option) => option
      .setName("panel")
      .setDescription("Wybierz panel do wysłania")
      .setRequired(true)
      .setAutocomplete(true))
    .toJSON(),
  new SlashCommandBuilder()
    .setName("cennik")
    .setDescription("Pokazuje lub zmienia cennik kalkulatora (tylko administracja)")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addStringOption((option) =>
      option
        .setName("serwer")
        .setDescription("Wybierz serwer/stawkę do zmiany")
        .setRequired(false)
        .addChoices(
          { name: "Anarchia LF - Normalna", value: "anarchia_lf_normal" },
          { name: "Anarchia LF - Hurtowa (>=50zł)", value: "anarchia_lf_bulk" },
          { name: "Anarchia LF - Próg (zł)", value: "anarchia_lf_threshold" },
          { name: "Anarchia BoxPvP", value: "anarchia_boxpvp" },
          { name: "MineStar LF - Normalna", value: "minestar_lf_normal" },
          { name: "MineStar LF - Hurtowa (>=50zł)", value: "minestar_lf_bulk" },
          { name: "MineStar LF - Próg (zł)", value: "minestar_lf_threshold" },
          { name: "MineStar SkyPvP", value: "minestar_skypvp" },
          { name: "Donut SMP", value: "donut_smp" }
        )
    )
    .addNumberOption((option) =>
      option
        .setName("stawka")
        .setDescription("Nowa stawka za 1 zł, może być ułamkowa (np. 2,5)")
        .setRequired(false)
        .setMinValue(0.01)
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("zaproszenia")
    .setDescription("Sprawdź szczegółowe logi zaproszeń (Tylko dla właściciela)")
    .addUserOption((option) =>
      option.setName("nick").setDescription("Użytkownik do sprawdzenia").setRequired(true),
    )
    .setDefaultMemberPermissions(PermissionFlagsBits.Administrator)
    .toJSON(),
  new SlashCommandBuilder()
    .setName("ticket-zakoncz")
    .setDescription("Użyj tej komendy jeżeli będziesz chciał zakończyć ticket")
    .setDefaultMemberPermissions(null)
    .addStringOption((option) =>
      option
        .setName("typ")
        .setDescription("Typ transakcji")
        .setRequired(true)
        .addChoices(
          { name: "ZAKUP", value: "zakup" },
          { name: "SPRZEDAŻ", value: "sprzedaż" },
          { name: "WRĘCZYŁ NAGRODĘ", value: "wręczył nagrodę" }
        )
    )
    .addIntegerOption((option) =>
      option
        .setName("ile")
        .setDescription("Kwota w PLN - wpisz tylko liczbę (np. 300)")
        .setMinValue(1)
        .setRequired(true)
    )
    .addStringOption((option) =>
      option
        .setName("serwer")
        .setDescription("Wybierz serwer")
        .setRequired(true)
        .addChoices(
          ...LEGIT_REP_SERVERS.map((server) => ({ name: server, value: server }))
        )
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("wydane")
    .setDescription("Sprawdź ile wydałeś w naszym sklepie")
    .setDefaultMemberPermissions(null)
    .addUserOption((option) =>
      option
        .setName("gracz")
        .setDescription("Wybierz gracza, którego wydaną kwotę chcesz sprawdzić (opcjonalnie)")
        .setRequired(false)
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("topwydane")
    .setDescription("Pokaż ranking graczy z największymi wydatkami w sklepie")
    .setDefaultMemberPermissions(null)
    .toJSON(),
  new SlashCommandBuilder()
    .setName("top-wydane")
    .setDescription("Pokaż ranking graczy z największymi wydatkami w sklepie")
    .setDefaultMemberPermissions(null)
    .toJSON(),
  new SlashCommandBuilder()
    .setName("znizka")
    .setDescription("Wpisz kod rabatowy na tickecie (np. /znizka ABC123XYZ0Q)")
    .setDefaultMemberPermissions(null)
    .addStringOption((option) =>
      option
        .setName("kod")
        .setDescription("Twój kod rabatowy")
        .setRequired(true)
        .setMinLength(10)
        .setMaxLength(12)
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("ustawienia")
    .setDescription("Zarządzaj ticketem (dodaj/usuń użytkownika, zmień nazwę)")
    .setDefaultMemberPermissions(null)
    .toJSON(),
  new SlashCommandBuilder()
    .setName("wydaneshop")
    .setDescription("Pokaż łączne wydatki i obrót całego sklepu (tylko właściciel)")
    .setDefaultMemberPermissions(PermissionFlagsBits.Administrator)
    .toJSON(),
  new SlashCommandBuilder()
    .setName("wydatkishop")
    .setDescription("Pokaż łączne wydatki i obrót całego sklepu (tylko właściciel)")
    .setDefaultMemberPermissions(PermissionFlagsBits.Administrator)
    .toJSON(),
  new SlashCommandBuilder()
    .setName("wydanestats")
    .setDescription("Zarządzaj wydaną kwotą graczy (Tylko Administrator/Sprzedawca)")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addSubcommand((sub) =>
      sub
        .setName("dodaj")
        .setDescription("Dodaj kwotę do wydatków gracza")
        .addUserOption((o) => o.setName("gracz").setDescription("Wybierz gracza").setRequired(true))
        .addIntegerOption((o) => o.setName("kwota").setDescription("Kwota w PLN").setRequired(true))
    )
    .addSubcommand((sub) =>
      sub
        .setName("odejmij")
        .setDescription("Odejmij kwotę z wydatków gracza")
        .addUserOption((o) => o.setName("gracz").setDescription("Wybierz gracza").setRequired(true))
        .addIntegerOption((o) => o.setName("kwota").setDescription("Kwota w PLN").setRequired(true))
    )
    .addSubcommand((sub) =>
      sub
        .setName("ustaw")
        .setDescription("Ustaw kwotę wydatków gracza")
        .addUserOption((o) => o.setName("gracz").setDescription("Wybierz gracza").setRequired(true))
        .addIntegerOption((o) => o.setName("kwota").setDescription("Kwota w PLN").setRequired(true))
    )
    .addSubcommand((sub) =>
      sub
        .setName("usun")
        .setDescription("Zresetuj/usuń wydatki gracza do 0")
        .addUserOption((o) => o.setName("gracz").setDescription("Wybierz gracza").setRequired(true))
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("usunzakup")
    .setDescription("Wyświetl zakupy gracza i usuń wybrane (Tylko Właściciel)")
    .setDefaultMemberPermissions(PermissionFlagsBits.Administrator)
    .addUserOption((o) =>
      o.setName("gracz").setDescription("Wybierz gracza").setRequired(true)
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("anonim")
    .setDescription("Bot wystawia legit check i zamyka ticket anonimowo (po /ticket-zakoncz)")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .toJSON(),
  new SlashCommandBuilder()
    .setName("zamknij-z-powodem")
    .setDescription("Zamknij ticket z powodem (tylko właściciel)")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addStringOption((option) =>
      option
        .setName("powod")
        .setDescription("Powód zamknięcia")
        .setRequired(true)
        .addChoices(
          { name: "Brak odpowiedzi", value: "Brak odpowiedzi" },
          { name: "Fake ticket", value: "Fake ticket" },
          { name: "Próba oszustwa", value: "Próba oszustwa" },
          { name: "Brak kultury", value: "Brak kultury" },
          { name: "Spam", value: "Spam" },
          { name: "Zamówienie zrealizowane", value: "Zamówienie zrealizowane" },
          { name: "Inny powód", value: "Inny powód" }
        )
    )
    .addStringOption((option) =>
      option
        .setName("powod_custom")
        .setDescription("Własny powód zamknięcia")
        .setRequired(false)
        .setMaxLength(200)
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("legit-check-ustaw")
    .setDescription("Ustaw licznik legit checków i zmień nazwę kanału")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addIntegerOption((option) =>
      option
        .setName("ile")
        .setDescription("Liczba legit checków (0-9999)")
        .setRequired(true)
        .setMinValue(0)
        .setMaxValue(9999)
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("wykres-legit-test")
    .setDescription("Testuj dzienne statystyki bez wystawiania legit checka")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addStringOption((option) =>
      option
        .setName("akcja")
        .setDescription("Wybierz operację testową")
        .setRequired(true)
        .addChoices(
          { name: "Dodaj testowe legit checki", value: "dodaj" },
          { name: "Odejmij testowe legit checki", value: "odejmij" },
          { name: "Wyzeruj dzisiejszy wykres", value: "wyzeruj" },
          { name: "Odśwież obraz wykresu", value: "odswiez" },
        ),
    )
    .addIntegerOption((option) =>
      option
        .setName("ile")
        .setDescription("Ile testowych legit checków dodać lub odjąć (domyślnie 1)")
        .setRequired(false)
        .setMinValue(1)
        .setMaxValue(100),
    )
    .addIntegerOption((option) =>
      option
        .setName("godzina")
        .setDescription("Godzina 0-23 (domyślnie aktualna)")
        .setRequired(false)
        .setMinValue(0)
        .setMaxValue(23),
    )
    .addIntegerOption((option) =>
      option
        .setName("kwota")
        .setDescription("Testowa kwota wydana w PLN (domyślnie 0)")
        .setRequired(false)
        .setMinValue(0)
        .setMaxValue(100000),
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("dzienne-lc-ustaw")
    .setDescription("Dodaj, odejmij lub wyzeruj dzienne statystyki legit checków")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addStringOption((option) =>
      option
        .setName("akcja")
        .setDescription("Wybierz operację")
        .setRequired(true)
        .addChoices(
          { name: "Dodaj dzienne legit checki", value: "dodaj" },
          { name: "Odejmij dzienne legit checki", value: "odejmij" },
          { name: "Wyzeruj dzisiejsze LC", value: "wyzeruj" },
          { name: "Odśwież obraz wykresu", value: "odswiez" },
        ),
    )
    .addIntegerOption((option) =>
      option
        .setName("ile")
        .setDescription("Ile legit checków dodać lub odjąć (domyślnie 1)")
        .setRequired(false)
        .setMinValue(1)
        .setMaxValue(500),
    )
    .addIntegerOption((option) =>
      option
        .setName("godzina")
        .setDescription("Godzina 0-23 (domyślnie aktualna)")
        .setRequired(false)
        .setMinValue(0)
        .setMaxValue(23),
    )
    .addIntegerOption((option) =>
      option
        .setName("kwota")
        .setDescription("Testowa kwota wydana w PLN (domyślnie 0)")
        .setRequired(false)
        .setMinValue(0)
        .setMaxValue(100000),
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("help")
    .setDescription("Spis podstawowych komend bota")
    .toJSON(),
  buildInviteStatsCommand(),
  new SlashCommandBuilder()
    .setName("zamknij")
    .setDescription("Zamknij ticket")
    .setDefaultMemberPermissions(null)
    .toJSON(),
  new SlashCommandBuilder()
    .setName("dodaj")
    .setDescription("Dodaj osobę do ticketa")
    .setDefaultMemberPermissions(null)
    .addUserOption((option) =>
      option
        .setName("osoba")
        .setDescription("Osoba, która ma otrzymać dostęp do ticketu")
        .setRequired(true),
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("opinia")
    .setDescription("Podziel sie opinią o naszym sklepie!")
    .addIntegerOption((option) =>
      option
        .setName("czas_oczekiwania")
        .setDescription("Ocena dotycząca czasu oczekiwania (1-5 gwiazdek)")
        .setRequired(true)
        .addChoices(
          { name: "⭐", value: 1 },
          { name: "⭐ ⭐", value: 2 },
          { name: "⭐ ⭐ ⭐", value: 3 },
          { name: "⭐ ⭐ ⭐ ⭐", value: 4 },
          { name: "⭐ ⭐ ⭐ ⭐ ⭐", value: 5 },
        ),
    )
    .addIntegerOption((option) =>
      option
        .setName("jakosc_produktu")
        .setDescription("Ocena jakości produktu (1-5)")
        .setRequired(true)
        .addChoices(
          { name: "⭐", value: 1 },
          { name: "⭐ ⭐", value: 2 },
          { name: "⭐ ⭐ ⭐", value: 3 },
          { name: "⭐ ⭐ ⭐ ⭐", value: 4 },
          { name: "⭐ ⭐ ⭐ ⭐ ⭐", value: 5 },
        ),
    )
    .addIntegerOption((option) =>
      option
        .setName("cena_produktu")
        .setDescription("Ocena ceny produktu (1-5)")
        .setRequired(true)
        .addChoices(
          { name: "⭐", value: 1 },
          { name: "⭐ ⭐", value: 2 },
          { name: "⭐ ⭐ ⭐", value: 3 },
          { name: "⭐ ⭐ ⭐ ⭐", value: 4 },
          { name: "⭐ ⭐ ⭐ ⭐ ⭐", value: 5 },
        ),
    )
    .addStringOption((option) =>
      option
        .setName("tresc_opinii")
        .setDescription("Treść opinii")
        .setRequired(true),
    )
    .toJSON(),
  // NEW: /wyczysckanal command
  new SlashCommandBuilder()
    .setName("wyczysc")
    .setDescription(
      "Wyczyść wiadomości na kanale (wszystko / ilosc-wiadomosci)",
    )
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addStringOption((option) =>
      option
        .setName("tryb")
        .setDescription("Wybierz tryb: wszystko lub ilosc")
        .setRequired(true)
        .addChoices(
          { name: "Wszystko", value: "wszystko" },
          { name: "Ilość wiadomości", value: "ilosc" },
        ),
    )
    .addIntegerOption((option) =>
      option
        .setName("ilosc")
        .setDescription(
          "Ile wiadomości usunąć (1-100) — wymagane gdy tryb=ilosc",
        )
        .setRequired(false),
    )
    .toJSON(),
  // NEW: /resetlc command - reset legitcheck counter
  new SlashCommandBuilder()
    .setName("resetlc")
    .setDescription("Reset liczby legitchecków do zera")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .toJSON(),
  // NEW: /autolc command - natychmiast wystawia auto legit check (test)
  new SlashCommandBuilder()
    .setName("autolc")
    .setDescription("Test: natychmiast wystawia automatyczny legit check")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .toJSON(),
  // NEW: /autolc-status command - pokazuje stan auto legit checka (test)
  new SlashCommandBuilder()
    .setName("autolc-status")
    .setDescription("Test: pokazuje kiedy zaplanowany jest następny auto legit check")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .toJSON(),
  // NEW: /autolc-timer command - start/stop/status timera auto legit checka
  new SlashCommandBuilder()
    .setName("autolc-timer")
    .setDescription("Start/stop/status timera automatycznych legit checków")
    .addStringOption((option) =>
      option
        .setName("akcja")
        .setDescription("Co zrobić z timerem")
        .setRequired(true)
        .addChoices(
          { name: "start", value: "start" },
          { name: "stop", value: "stop" },
          { name: "status", value: "status" }
        ),
    )
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .toJSON(),
  // NEW: /zresetujczasoczekiwania command - clear cooldowns for core public actions
  new SlashCommandBuilder()
    .setName("zco")
    .setDescription("Zresetuj czas oczekiwania (opinia / zaproszenia / +rep / wylosuj nagrodę)")
    .addStringOption((option) =>
      option
        .setName("co")
        .setDescription("Co zresetować")
        .setRequired(true)
        .addChoices(
          { name: "/opinia", value: "opinia" },
          { name: "/sprawdz-zaproszenia", value: "zaproszenia" },
          { name: "+rep", value: "rep" },
          { name: "Wylosuj nagrodę", value: "free-kasa" },
          { name: "wszystko", value: "all" }
        ),
    )
    .addUserOption((option) =>
      option
        .setName("kto")
        .setDescription("Użytkownik do resetu (domyślnie Ty)")
        .setRequired(false)
    )
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .toJSON(),
  // NEW helper admin commands for claiming/unclaiming
  new SlashCommandBuilder()
    .setName("przejmij")
    .setDescription("Przejmij aktualny ticket")
    .setDefaultMemberPermissions(null)
    .toJSON(),
  new SlashCommandBuilder()
    .setName("odprzejmij")
    .setDescription("Zwolnij aktualny ticket")
    .setDefaultMemberPermissions(null)
    .toJSON(),
  new SlashCommandBuilder()
    .setName("autoprzejmij")
    .setDescription("Ukryj lub przywróć widoczność ticketów zakupowych dla sprzedawców")
    .setDefaultMemberPermissions(PermissionFlagsBits.Administrator)
    .addStringOption((option) =>
      option
        .setName("status")
        .setDescription("Włącz lub wyłącz tryb tylko dla właściciela")
        .setRequired(true)
        .addChoices(
          { name: "WLACZ", value: "wlacz" },
          { name: "WYLACZ", value: "wylacz" }
        )
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("zacznijliczycwlasicicielowi")
    .setDescription("Włącz lub wyłącz liczenie zaproszeń właścicielowi")
    .setDefaultMemberPermissions(PermissionFlagsBits.Administrator)
    .addStringOption((option) =>
      option
        .setName("status")
        .setDescription("Włącz lub wyłącz liczenie")
        .setRequired(true)
        .addChoices(
          { name: "ON", value: "on" },
          { name: "OFF", value: "off" },
        )
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("autoverify")
    .setDescription("Automatycznie nadawaj rolę klient nowym użytkownikom po dołączeniu")
    .setDefaultMemberPermissions(PermissionFlagsBits.Administrator)
    .addStringOption((option) =>
      option
        .setName("status")
        .setDescription("Włącz lub wyłącz automatyczną weryfikację")
        .setRequired(true)
        .addChoices(
          { name: "WLACZ", value: "wlacz" },
          { name: "WYLACZ", value: "wylacz" }
        )
    )
    .toJSON(),
  // UPDATED: embed (interactive flow)
  new SlashCommandBuilder()
    .setName("embed")
    .setDescription("Wyślij wiadomość przez bota (tylko właściciel)")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addChannelOption((o) =>
      o
        .setName("kanal")
        .setDescription(
          "Kanał docelowy (opcjonalnie). Jeśli nie podasz, użyty zostanie aktualny kanał.",
        )
        .setRequired(false)
        .addChannelTypes(ChannelType.GuildText),
    )
    .addStringOption((o) =>
      o
        .setName("data")
        .setDescription("Czy dodać datę na dole karty")
        .setRequired(false)
        .addChoices(
          { name: "zdata", value: "zdata" },
          { name: "bezdaty", value: "bezdaty" },
        ),
    )
    .addStringOption((o) =>
      o
        .setName("pingi")
        .setDescription("Jak obsłużyć pingi w treści")
        .setRequired(false)
        .addChoices(
          { name: "zpingiem", value: "zpingiem" },
          { name: "bezpingu", value: "bezpingu" },
        ),
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("embed-2")
    .setDescription("Wyślij testowy embed w stylu cennika i edytuj go przyciskami")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addChannelOption((o) =>
      o
        .setName("kanal")
        .setDescription(
          "Kanał docelowy (opcjonalnie). Jeśli nie podasz, użyty zostanie aktualny kanał.",
        )
        .setRequired(false)
        .addChannelTypes(ChannelType.GuildText),
    )
    .addAttachmentOption((o) =>
      o
        .setName("filmik")
        .setDescription("Opcjonalny filmik, gif albo obraz do osadzenia w karcie")
        .setRequired(false),
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("sprawdz-embed-2")
    .setDescription("Podepnij istniejący embed testowy na kanale i edytuj go dalej")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addChannelOption((o) =>
      o
        .setName("kanal")
        .setDescription(
          "Kanał z istniejącym embedem testowym. Jeśli nie podasz, użyty zostanie aktualny kanał.",
        )
        .setRequired(false)
        .addChannelTypes(ChannelType.GuildText),
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("zaaktualizuj-film")
    .setDescription("Podmień film/obraz w najbliższym embed /embed-2 na nowy plik")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addChannelOption((o) =>
      o
        .setName("kanal")
        .setDescription("Kanał z embedem /embed-2. Jeśli nie podasz, użyty zostanie aktualny kanał.")
        .setRequired(false)
        .addChannelTypes(ChannelType.GuildText),
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("ustaw-tickety-sprzedawcy")
    .setDescription("Ustaw ręcznie liczbę wykonanych ticketów dla sprzedawcy")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addUserOption((o) =>
      o.setName("uzytkownik").setDescription("Sprzedawca").setRequired(true),
    )
    .addIntegerOption((o) =>
      o.setName("ilosc").setDescription("Liczba ticketów").setRequired(true),
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("przelicz-tickety-sprzedawcy")
    .setDescription("Przelicz ponownie liczbę ticketów sprzedawcy z historii kanału +rep")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addUserOption((o) =>
      o.setName("uzytkownik").setDescription("Sprzedawca").setRequired(true),
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("aktualizacja-embed")
    .setDescription("Usuń i wyślij ponownie najbliższy embedtest w aktualnym wydaniu")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addChannelOption((o) =>
      o
        .setName("kanal")
        .setDescription("Kanał z embedem /embed-2. Jeśli nie podasz, użyty zostanie aktualny kanał.")
        .setRequired(false)
        .addChannelTypes(ChannelType.GuildText),
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("mody")
    .setDescription("Wyślij embed z przyciskiem do nagrań modów (tylko właściciel)")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addChannelOption((o) =>
      o
        .setName("kanal")
        .setDescription(
          "Kanał docelowy (opcjonalnie). Jeśli nie podasz, użyty zostanie aktualny kanał.",
        )
        .setRequired(false)
        .addChannelTypes(ChannelType.GuildText),
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("znajdz-ticket")
    .setDescription("Znajdź szczegóły ticketu po kodzie ID lub sprawdź historię ticketów danego gracza")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addStringOption((option) =>
      option
        .setName("kod")
        .setDescription("Kod ID ticketu (np. MAi1TRSwRAJj4) lub ID kanału")
        .setRequired(false)
    )
    .addUserOption((option) =>
      option
        .setName("uzytkownik")
        .setDescription("Gracz, którego tickety chcesz sprawdzić")
        .setRequired(false)
    )
    .toJSON(),
  // RENAMED: sprawdz-zaproszenia (was sprawdz-zapro)
  new SlashCommandBuilder()
    .setName("sprawdz-zaproszenia")
    .setDescription("Sprawdź ile posiadasz zaproszeń")
    .toJSON(),
  new SlashCommandBuilder()
    .setName("rozliczenie")
    .setDescription("Dodaj kwote do rozliczeń")
    .setDefaultMemberPermissions(null)
    .addIntegerOption((option) =>
      option
        .setName("kwota")
        .setDescription("Kwota w zł")
        .setRequired(true)
    )
    .addUserOption((option) =>
      option
        .setName("uzytkownik")
        .setDescription("Użytkownik (opcjonalnie, domyślnie ty)")
        .setRequired(false)
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("rozliczeniazaplacil")
    .setDescription("Oznacz rozliczenie jako zapłacone, usuń zawieszenie i przywróć limity")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addUserOption((option) =>
      option
        .setName("uzytkownik")
        .setDescription("Użytkownik do oznaczenia")
        .setRequired(true)
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("rozliczeniezaplacil")
    .setDescription("Oznacz rozliczenie jako zapłacone, usuń zawieszenie i przywróć limity")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addUserOption((option) =>
      option
        .setName("uzytkownik")
        .setDescription("Użytkownik do oznaczenia")
        .setRequired(true)
    )
    .toJSON(),

  new SlashCommandBuilder()
    .setName("test-pv")
    .setDescription("Przetestuj wiadomości wysyłane użytkownikom na PV (wygrane kody, weryfikacja itp.)")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addStringOption((option) =>
      option
        .setName("typ")
        .setDescription("Wybierz typ wiadomości prywatnej do przetestowania")
        .setRequired(true)
        .addChoices(
          { name: "🎁 Wygrany kod rabatowy (-10%)", value: "wygrana-znizka" },
          { name: "💰 Wygrany kod pieniężny (50k$)", value: "wygrana-kasa" },
          { name: "⚔️ Wygrana przedmiot (Anarchiczny miecz)", value: "wygrana-przedmiot" },
          { name: "🏆 Nagroda za zaproszenia (90k$)", value: "wygrana-zaproszenia" },
          { name: "🚨 Wezwanie na ticket (/wezwij)", value: "wezwanie" },
          { name: "🔒 Zamknięcie ticketu", value: "zamkniecie-ticketu" },
          { name: "🤖 Auto-odpowiedź na PV", value: "auto-odpowiedz" },
          { name: "✨ Powiadomienie po weryfikacji", value: "weryfikacja" },
          { name: "🚀 Wszystkie wiadomości na raz", value: "wszystkie" },
        ),
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("test-legit-check-wzor")
    .setDescription("Przetestuj wygląd panelu wygenerowanego po /ticket-zakoncz (wzór + przycisk)")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addStringOption((o) =>
      o.setName("typ")
        .setDescription("Typ transakcji (zakup, sprzedaż, wręczył nagrodę)")
        .setRequired(false)
        .addChoices(
          { name: "zakup", value: "zakup" },
          { name: "sprzedaż", value: "sprzedaż" },
          { name: "wręczył nagrodę", value: "wręczył nagrodę" },
        )
    )
    .addIntegerOption((o) =>
      o.setName("ile").setDescription("Kwota PLN (domyślnie 50)").setRequired(false)
    )
    .addStringOption((o) =>
      o.setName("serwer").setDescription("Nazwa serwera (domyślnie ANARCHIA LIFESTEAL)").setRequired(false)
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("test-legit-check-sukces")
    .setDescription("Przetestuj podmienienie embeda na sukces w aktualnym tickecie")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .toJSON(),
  new SlashCommandBuilder()
    .setName("test-rozliczenia-ping")
    .setDescription("Przetestuj wysłanie krótkiego pinga rozliczenia na kanał rozliczeń (usuwa się po 5s)")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addUserOption((o) =>
      o.setName("sprzedawca").setDescription("Sprzedawca do oznaczenia").setRequired(true)
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("test-pingi")
    .setDescription("Przetestuj logikę oznaczania po /ticket-zakoncz (czy-legit, opinie-klientow, legit-checki)")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addUserOption((o) =>
      o
        .setName("uzytkownik")
        .setDescription("Użytkownik do przetestowania (domyślnie ty)")
        .setRequired(false),
    )
    .addBooleanOption((o) =>
      o
        .setName("wyslij")
        .setDescription("Czy rzeczywiście wysłać testowe ghost-pingi na kanały (usunięcie po 4s)")
        .setRequired(false),
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("rozliczenieprowizja")
    .setDescription("Ustaw procent prowizji rozliczenia (dla wszystkich lub wybranej osoby)")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addIntegerOption((option) =>
      option
        .setName("prowizja")
        .setDescription("Procent prowizji (np. 10 dla 10%, 5 dla 5%)")
        .setRequired(true)
        .setMinValue(0)
        .setMaxValue(100)
    )
    .addUserOption((option) =>
      option
        .setName("uzytkownik")
        .setDescription("Użytkownik (opcjonalnie, bez podania zmienia dla wszystkich)")
        .setRequired(false)
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("rozliczenie-prowizja")
    .setDescription("Ustaw procent prowizji rozliczenia (dla wszystkich lub wybranej osoby)")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addIntegerOption((option) =>
      option
        .setName("prowizja")
        .setDescription("Procent prowizji (np. 10 dla 10%, 5 dla 5%)")
        .setRequired(true)
        .setMinValue(0)
        .setMaxValue(100)
    )
    .addUserOption((option) =>
      option
        .setName("uzytkownik")
        .setDescription("Użytkownik (opcjonalnie, bez podania zmienia dla wszystkich)")
        .setRequired(false)
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("rozliczeniezakoncz")
    .setDescription("Wyślij podsumowanie rozliczeń (tylko właściciel)")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .toJSON(),
  new SlashCommandBuilder()
    .setName("wezwij")
    .setDescription("Wezwij klienta")
    .setDefaultMemberPermissions(null)
    .toJSON(),
  new SlashCommandBuilder()
    .setName("wezwij-osobe")
    .setDescription("Wezwij wybraną osobę do ticketa")
    .setDefaultMemberPermissions(0)
    .addUserOption((option) =>
      option
        .setName("uzytkownik")
        .setDescription("Osoba, którą chcesz wezwać")
        .setRequired(true)
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("statusbota")
    .setDescription("Pokaż szczegółowy status bota")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .toJSON(),
  new SlashCommandBuilder()
    .setName("stworz-kod")
    .setDescription("Wygeneruj nowy kod zniżkowy (Tylko Sprzedawca/Admin)")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addIntegerOption((option) =>
      option
        .setName("znizka")
        .setDescription("Procent zniżki (np. 10 dla 10%, 20 dla 20%)")
        .setRequired(true)
        .setMinValue(1)
        .setMaxValue(100)
    )
    .addStringOption((option) =>
      option
        .setName("kod")
        .setDescription("Twój własny kod (opcjonalnie, np. ZNIZKA10)")
        .setRequired(false)
    )
    .addIntegerOption((option) =>
      option
        .setName("dni")
        .setDescription("Liczba dni ważności (domyślnie 30)")
        .setRequired(false)
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("rozliczenieustaw")
    .setDescription("Ustaw tygodniową sumę rozliczenia dla użytkownika (tylko właściciel)")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .addUserOption((option) =>
      option
        .setName("uzytkownik")
        .setDescription("Użytkownik")
        .setRequired(true)
    )
    .addStringOption((option) =>
      option
        .setName("akcja")
        .setDescription("Dodaj lub odejmij kwotę")
        .setRequired(true)
        .addChoices(
          { name: "Dodaj", value: "dodaj" },
          { name: "Odejmij", value: "odejmij" },
          { name: "Ustaw", value: "ustaw" }
        )
    )
    .addIntegerOption((option) =>
      option
        .setName("kwota")
        .setDescription("Kwota do dodania/odejmowania/ustawienia")
        .setRequired(true)
        .setMinValue(0)
        .setMaxValue(999999)
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("utworz-konkurs")
    .setDescription(
      "Utwórz konkurs z przyciskiem do udziału i losowaniem zwycięzców",
    )
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .toJSON(),
  new SlashCommandBuilder()
    .setName("end-giveaways")
    .setDescription("Zakończ wszystkie aktywne konkursy (tylko właściciel serwera)")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .toJSON(),
  new SlashCommandBuilder()
    .setName("ostrzezenie")
    .setDescription("Wystaw ostrzeżenie dla sprzedawcy na kanale ostrzeżeń")
    .addUserOption((o) =>
      o.setName("sprzedawca").setDescription("Wybierz sprzedawcę").setRequired(true)
    )
    .addStringOption((o) =>
      o.setName("powod").setDescription("Powód ostrzeżenia").setRequired(true)
    )
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .toJSON(),
  new SlashCommandBuilder()
    .setName("warn")
    .setDescription("Wystaw ostrzeżenie dla sprzedawcy na kanale ostrzeżeń")
    .addUserOption((o) =>
      o.setName("sprzedawca").setDescription("Wybierz sprzedawcę").setRequired(true)
    )
    .addStringOption((o) =>
      o.setName("powod").setDescription("Powód ostrzeżenia").setRequired(true)
    )
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .toJSON(),
  new SlashCommandBuilder()
    .setName("ostrzezenie-usun")
    .setDescription("Usuń 1 ostrzeżenie sprzedawcy")
    .addUserOption((o) =>
      o.setName("sprzedawca").setDescription("Wybierz sprzedawcę").setRequired(true)
    )
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .toJSON(),
  new SlashCommandBuilder()
    .setName("unwarn")
    .setDescription("Usuń 1 ostrzeżenie sprzedawcy")
    .addUserOption((o) =>
      o.setName("sprzedawca").setDescription("Wybierz sprzedawcę").setRequired(true)
    )
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels)
    .toJSON(),
  new SlashCommandBuilder()
    .setName("ostrzezenia")
    .setDescription("Wyświetla liczbę ostrzeżeń sprzedawcy")
    .addUserOption((o) =>
      o.setName("sprzedawca").setDescription("Wybierz sprzedawcę").setRequired(false)
    )
    .toJSON(),
  new SlashCommandBuilder()
    .setName("warns")
    .setDescription("Wyświetla liczbę ostrzeżeń sprzedawcy")
    .addUserOption((o) =>
      o.setName("sprzedawca").setDescription("Wybierz sprzedawcę").setRequired(false)
    )
    .toJSON(),
].map(applyCommandVisibility);

const rest = new REST({ version: "10", api: DISCORD_API_BASE }).setToken(process.env.BOT_TOKEN);

// Helper: human-readable ms
function humanizeMs(ms) {
  const s = Math.floor(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${sec}s`;
  return `${sec}s`;
}

async function fetchGuildVanityDataSafe(guild) {
  if (!guild || typeof guild.fetchVanityData !== "function") return null;
  try {
    const vanityData = await guild.fetchVanityData();
    if (!vanityData) return null;
    return {
      code:
        typeof vanityData.code === "string" && vanityData.code.trim()
          ? vanityData.code.trim()
          : null,
      uses: typeof vanityData.uses === "number" ? vanityData.uses : null,
    };
  } catch {
    return null;
  }
}

async function fetchGuildVanityUses(guild) {
  const vanityData = await fetchGuildVanityDataSafe(guild);
  return typeof vanityData?.uses === "number" ? vanityData.uses : null;
}

function isHttpUrl(value) {
  try {
    const u = new URL((value || "").trim());
    return u.protocol === "http:" || u.protocol === "https:";
  } catch {
    return false;
  }
}

function normalizeDiscordCdnVideoUrl(rawUrl) {
  const value = (rawUrl || "").toString().trim();
  if (!isHttpUrl(value)) return value;
  try {
    const u = new URL(value);
    const host = u.hostname.toLowerCase();
    const isDiscordCdn =
      host.endsWith("discordapp.com") || host.endsWith("discord.com");
    const isAttachmentPath = u.pathname.includes("/attachments/");
    if (isDiscordCdn && isAttachmentPath) {
      return `${u.protocol}//${u.host}${u.pathname}`;
    }
    return value;
  } catch {
    return value;
  }
}

function isDiscordAttachmentUrl(rawUrl) {
  const value = (rawUrl || "").toString().trim();
  if (!isHttpUrl(value)) return false;
  try {
    const u = new URL(value);
    const host = u.hostname.toLowerCase();
    const isDiscordHost =
      host.endsWith("discordapp.com") || host.endsWith("discord.com");
    return isDiscordHost && u.pathname.includes("/attachments/");
  } catch {
    return false;
  }
}

function isVideoAttachment(att) {
  if (!att) return false;
  const ct = (att.contentType || "").toLowerCase();
  if (ct.startsWith("video/")) return true;

  const name = (att.name || "").toLowerCase();
  return (
    name.endsWith(".mp4") ||
    name.endsWith(".mov") ||
    name.endsWith(".webm") ||
    name.endsWith(".m4v") ||
    name.endsWith(".mkv") ||
    name.endsWith(".avi")
  );
}

function getModsVideoCandidateFilenames(videoCfg) {
  if (!videoCfg || typeof videoCfg !== "object") return [];

  const rawCandidates = [];
  if (videoCfg.filename) rawCandidates.push(videoCfg.filename);
  if (Array.isArray(videoCfg.filenameAliases)) {
    rawCandidates.push(...videoCfg.filenameAliases);
  }

  const unique = [];
  const seen = new Set();
  for (const raw of rawCandidates) {
    const name = (raw || "").toString().trim();
    if (!name) continue;
    const key = name.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    unique.push(name);
  }
  return unique;
}

function getNormalizedVideoStem(value) {
  return (value || "")
    .toString()
    .trim()
    .toLowerCase()
    .replace(/\.[^.]+$/, "")
    .replace(/\s*\(\d+\)$/, "");
}

function resolveLocalModsVideoPath(videoCfg) {
  if (!videoCfg || typeof videoCfg !== "object") return null;

  const candidates = [];
  for (const filename of getModsVideoCandidateFilenames(videoCfg)) {
    candidates.push(path.join(__dirname, "attached_assets", filename));
  }
  if (videoCfg.localPath) {
    candidates.push(videoCfg.localPath);
  }

  const seen = new Set();
  for (const candidate of candidates) {
    const p = (candidate || "").toString().trim();
    if (!p) continue;
    const key = p.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    if (fs.existsSync(p)) return p;
  }

  return null;
}

function getModsVideoConfigByFilename(filename) {
  const normalized = (filename || "").toString().trim().toLowerCase();
  if (!normalized) return null;

  const normalizedNoExt = getNormalizedVideoStem(normalized);

  for (const cfg of MODS_VIDEO_FILES) {
    const candidateNames = getModsVideoCandidateFilenames(cfg);
    for (const candidateNameRaw of candidateNames) {
      const candidateName = candidateNameRaw.toLowerCase();
      const candidateStem = getNormalizedVideoStem(candidateName);
      if (
        normalized === candidateName ||
        normalizedNoExt === candidateStem ||
        normalizedNoExt.startsWith(candidateStem) ||
        candidateStem.startsWith(normalizedNoExt)
      ) {
        return cfg;
      }
    }
  }

  return null;
}

const { buildModVideoPreview } = require("./mod-video-preview.cjs");

function getModsVideoCaption(videoCfg, fallbackName = "Nagranie") {
  const arrowEmoji = "<a:arrowwhite:1491476759290449984>";
  const safeName = (videoCfg?.modName || fallbackName)
    .toString()
    .replace(/[\r\n`*_~|<>]/g, "")
    .trim();
  const modName = safeName || "Nagranie";
  return `## ${arrowEmoji} Mod: **__${modName}__**`;
}

function getModsVideoOrderRank(videoCfg) {
  const key = videoCfg?.key;
  if (!key) return Number.MAX_SAFE_INTEGER;
  return modsVideoOrderRanks.has(key)
    ? modsVideoOrderRanks.get(key)
    : Number.MAX_SAFE_INTEGER;
}

function collectVideoLinksFromMessage(msg) {
  const out = [];
  if (!msg?.attachments?.size) return out;

  for (const att of msg.attachments.values()) {
    if (!isVideoAttachment(att)) continue;
    const normalizedUrl = normalizeDiscordCdnVideoUrl(att.url);
    if (!isHttpUrl(normalizedUrl)) continue;
    const cfg = getModsVideoConfigByFilename(att.name || "");
    out.push({
      label: att.name || "nagranie",
      key: cfg?.key || null,
      modName: cfg?.modName || null,
      url: normalizedUrl,
    });
  }
  return out;
}

function getPublicBaseUrl() {
  const candidates = [
    process.env.PUBLIC_BASE_URL,
    process.env.MONITOR_HTTP_URL,
    process.env.RENDER_EXTERNAL_URL,
    process.env.RENDER_URL,
  ];

  for (const raw of candidates) {
    const value = (raw || "").trim();
    if (!value) continue;
    try {
      const parsed = new URL(value);
      return `${parsed.protocol}//${parsed.host}`;
    } catch {
      // ignore invalid URL candidate
    }
  }

  const host = (process.env.RENDER_EXTERNAL_HOSTNAME || "").trim();
  if (host) {
    return `https://${host}`;
  }

  return null;
}

function getLocalModsVideoPublicUrl(videoCfg) {
  if (!videoCfg?.key) return null;
  const localPath = resolveLocalModsVideoPath(videoCfg);
  if (!localPath) return null;

  const baseUrl = getPublicBaseUrl();
  if (!baseUrl) return null;

  return `${baseUrl}/videos/${encodeURIComponent(videoCfg.key)}`;
}

async function findVideoAttachmentUrlByName(guild, filenames) {
  const list = Array.isArray(filenames) ? filenames : [filenames];
  const filenameLowerList = list
    .map((f) => (f || "").toString().trim().toLowerCase())
    .filter(Boolean);
  if (!guild || filenameLowerList.length === 0) return null;

  const filenameStemList = filenameLowerList.map((f) => getNormalizedVideoStem(f));
  const meRef = guild.members?.me || client.user?.id || null;
  const channels = guild.channels.cache.filter(
    (ch) => ch.type === ChannelType.GuildText,
  );

  // Limit scan scope to keep this interaction responsive.
  for (const channel of channels.values()) {
    try {
      const perms = meRef ? channel.permissionsFor(meRef) : null;
      if (
        !perms ||
        !perms.has(PermissionFlagsBits.ViewChannel) ||
        !perms.has(PermissionFlagsBits.ReadMessageHistory)
      ) {
        continue;
      }

      const fetched = await channel.messages.fetch({ limit: 100 }).catch(() => null);
      if (!fetched) continue;

      for (const msg of fetched.values()) {
        for (const att of msg.attachments.values()) {
          const attName = (att.name || "").toLowerCase();
          const attStem = getNormalizedVideoStem(attName);
          const matchesName =
            filenameLowerList.some(
              (filenameLower) =>
                attName === filenameLower ||
                attName.startsWith(filenameLower.replace(/\.[^.]+$/, "")),
            ) ||
            filenameStemList.some(
              (filenameStem) =>
                attStem === filenameStem ||
                attStem.startsWith(filenameStem) ||
                filenameStem.startsWith(attStem),
            );
          if (!matchesName) continue;
          if (isHttpUrl(att.url)) {
            return att.url;
          }
        }
      }
    } catch {
      // ignore per-channel fetch errors
    }
  }

  return null;
}

async function resolveModsVideoUrl(guild, videoCfg, options = {}) {
  const allowSlowScan = options.allowSlowScan !== false;

  if (!videoCfg) return null;

  const fromEnv = normalizeDiscordCdnVideoUrl(
    (process.env[videoCfg.envVar] || "").trim(),
  );
  if (isHttpUrl(fromEnv)) {
    modsVideoUrlCache.set(videoCfg.key, fromEnv);
    return fromEnv;
  }

  const cached = normalizeDiscordCdnVideoUrl(
    (modsVideoUrlCache.get(videoCfg.key) || "").trim(),
  );

  // Przy wolnym skanie preferujemy linki Discord CDN (najlepiej działają w podglądzie).
  if (allowSlowScan) {
    if (isDiscordAttachmentUrl(cached)) return cached;

    const found = await findVideoAttachmentUrlByName(
      guild,
      getModsVideoCandidateFilenames(videoCfg),
    );
    const normalizedFound = normalizeDiscordCdnVideoUrl(found);
    if (isHttpUrl(normalizedFound)) {
      modsVideoUrlCache.set(videoCfg.key, normalizedFound);
      return normalizedFound;
    }
  }

  const fromDefault = normalizeDiscordCdnVideoUrl(
    (videoCfg.defaultUrl || "").trim(),
  );
  if (isHttpUrl(fromDefault)) {
    modsVideoUrlCache.set(videoCfg.key, fromDefault);
    return fromDefault;
  }

  if (isHttpUrl(cached)) return cached;

  const localRouteUrl = getLocalModsVideoPublicUrl(videoCfg);
  if (isHttpUrl(localRouteUrl)) {
    modsVideoUrlCache.set(videoCfg.key, localRouteUrl);
    return localRouteUrl;
  }

  return null;
}

// Helper: sprawdź czy użytkownik jest admin, sprzedawca lub helper
function isAdminOrSeller(member) {
  if (!member) return false;

  // Administrator ma pełne uprawnienia bez względu na rolę zawieszony
  if (member.permissions && member.permissions.has(PermissionFlagsBits.Administrator)) {
    return true;
  }

  const SUSPENDED_ROLE_ID = "1537090439239442483";
  if (member.roles && member.roles.cache && member.roles.cache.has(SUSPENDED_ROLE_ID)) {
    return false;
  }

  const SELLER_ROLE_ID = "1350786945944391733";
  const HELPER_ROLE_ID = "1519069239254974475";

  // Sprawdź czy ma rolę sprzedawcy lub helpera
  if (
    member.roles &&
    member.roles.cache &&
    (member.roles.cache.has(SELLER_ROLE_ID) || member.roles.cache.has(HELPER_ROLE_ID))
  ) {
    return true;
  }

  return false;
}

function parseShortNumber(input) {
  if (!input) return NaN;
  const str = input.toString().trim().toLowerCase().replace(/\s+/g, "");
  const match = str.match(/^(\d+)(k|m)?$/);
  if (!match) return NaN;
  const base = parseInt(match[1], 10);
  const suffix = match[2];
  if (!suffix) return base;
  if (suffix === "k") return base * 1000;
  if (suffix === "m") return base * 1_000_000;
  return NaN;
}

function round2(n) {
  return Math.round((Number(n) + Number.EPSILON) * 100) / 100;
}

function formatShortWaluta(n) {
  const v = Number(n) || 0;
  const abs = Math.abs(v);
  const fmt = (x) => {
    const rounded = Math.round((Number(x) + Number.EPSILON) * 100) / 100;
    if (Number.isInteger(rounded)) return `${rounded}`;
    return `${rounded}`.replace(/\.0+$/, "").replace(/(\.\d*[1-9])0+$/, "$1");
  };

  if (abs >= 1_000_000) return `${fmt(v / 1_000_000)}m`;
  if (abs >= 1_000) return `${fmt(v / 1_000)}k`;
  return fmt(v).replace(".", ",");
}

function getPaymentFeePercent(methodRaw) {
  const m = (methodRaw || "").toString().trim().toLowerCase();

  if (m.startsWith("blik")) return 0;
  if (m.startsWith("kod blik")) return 10;
  if (m.includes("mypsc")) return 20;
  if (m === "psc bez paragonu" || m.startsWith("psc bez paragonu")) return 20;
  if (m === "psc" || m.startsWith("psc ")) return 10;
  if (m.includes("paypal")) return 10;
  if (m.includes("ltc")) return 10;

  return 0;
}

function getMinPurchasePln(methodRaw) {
  const m = (methodRaw || "").toString().trim().toLowerCase();
  if (m.includes("mypsc")) return 11; // min zakupy dla MYPSC
  if (m.startsWith("blik") || m.startsWith("kod blik")) return 5;
  if (m.includes("psc")) return 5;
  if (m.includes("paypal")) return 5;
  if (m.includes("ltc")) return 5;
  return 5;
}

function calculateFeePln(basePln, methodRaw) {
  const percent = getPaymentFeePercent(methodRaw);
  let fee = basePln * (percent / 100);
  let feeLabel = `${percent}%`;

  if ((methodRaw || "").toString().toLowerCase().includes("mypsc")) {
    fee = Math.max(fee, 10); // min 10 zł
    feeLabel = `${percent}% (min 10zł)`;
  }

  return { fee, feeLabel, percent };
}

let ANARCHIA_LIFESTEAL_RATE = 8200;
let ANARCHIA_LIFESTEAL_BULK_RATE = 8500;
let ANARCHIA_LIFESTEAL_BULK_THRESHOLD_PLN = 50;
let ANARCHIA_BOXPVP_RATE = 600;
let MINESTAR_LF_RATE = 300;
let MINESTAR_LF_BULK_RATE = 400;
let MINESTAR_LF_BULK_THRESHOLD_PLN = 50;
let DONUT_SMP_RATE = 5_500_000;
let MINESTAR_SKY_RATE = 3;

function getAnarchiaLifestealRateForPln(pln) {
  return Number(pln) >= ANARCHIA_LIFESTEAL_BULK_THRESHOLD_PLN
    ? ANARCHIA_LIFESTEAL_BULK_RATE
    : ANARCHIA_LIFESTEAL_RATE;
}

function getAnarchiaLifestealRateForWaluta(waluta, methodRaw) {
  const basePlnHighRate = Number(waluta) / ANARCHIA_LIFESTEAL_BULK_RATE;
  const { fee: highRateFee } = calculateFeePln(basePlnHighRate, methodRaw);
  const totalPlnHighRate = round2(basePlnHighRate + highRateFee);

  return totalPlnHighRate > ANARCHIA_LIFESTEAL_BULK_THRESHOLD_PLN
    ? ANARCHIA_LIFESTEAL_BULK_RATE
    : ANARCHIA_LIFESTEAL_RATE;
}

function getMinestarLfRateForWaluta(waluta, methodRaw) {
  const basePlnHighRate = Number(waluta) / MINESTAR_LF_BULK_RATE;
  const { fee: highRateFee } = calculateFeePln(basePlnHighRate, methodRaw);
  const totalPlnHighRate = round2(basePlnHighRate + highRateFee);

  return totalPlnHighRate >= MINESTAR_LF_BULK_THRESHOLD_PLN
    ? MINESTAR_LF_BULK_RATE
    : MINESTAR_LF_RATE;
}

function getRateForPlnAmount(pln, serverRaw) {
  const server = (serverRaw || "").toString().trim().toUpperCase().replace(/\s+/g, "_");

  if (server === "MINESTAR_SKY" || server === "MINESTAR_SKYPVP" || server === "MINESTAR_SKY_PVP") return MINESTAR_SKY_RATE;
  if (server === "ANARCHIA_BOXPVP") return ANARCHIA_BOXPVP_RATE;
  if (server === "ANARCHIA_LIFESTEAL" || server === "ANARCHIA_LF") return getAnarchiaLifestealRateForPln(pln);
  if (server === "MINESTAR_LF" || server === "MINESTAR_LIFESTEAL") return Number(pln) >= MINESTAR_LF_BULK_THRESHOLD_PLN ? MINESTAR_LF_BULK_RATE : MINESTAR_LF_RATE;
  if (server === "DONUT_SMP") return DONUT_SMP_RATE;

  // fallback (stary cennik)
  return ANARCHIA_LIFESTEAL_RATE;
}

// Helper: find a bot message in a channel matching a predicate on embed
async function findBotMessageWithEmbed(channel, matchFn) {
  try {
    const fetched = await channel.messages.fetch({ limit: 100 });
    for (const msg of fetched.values()) {
      if (
        msg.author?.id === client.user.id &&
        msg.embeds &&
        msg.embeds.length
      ) {
        const emb = msg.embeds[0];
        try {
          if (matchFn(emb)) return msg;
        } catch (e) {
          // match function error — skip
        }
      }
    }
  } catch (e) {
    // ignore fetch errors (no perms)
  }
  return null;
}

// Helper: determine if a channel is considered a ticket channel (based on categories)
function isTicketChannel(channel) {
  if (!channel || !channel.guild) return false;
  if (ticketOwners.has(channel.id)) return true;
  if (channel.parentId && String(channel.parentId) === String(REWARDS_CATEGORY_ID))
    return true;
  const cats = ticketCategories.get(channel.guild.id);
  if (cats) {
    for (const id of Object.values(cats)) {
      if (id === channel.parentId) return true;
    }
  }
  // fallback: name starts with ticket-
  if (channel.name && channel.name.toLowerCase().startsWith("ticket-"))
    return true;
  if (isModernPurchaseTicketChannelName(channel.name)) return true;
  return false;
}

async function inferTicketOwnerFromChannel(channel) {
  const guild = channel?.guild;
  if (!guild?.members?.fetch) return null;

  const overwrites = channel.permissionOverwrites?.cache;
  if (!overwrites?.size) return null;

  const SELLER_ROLE_ID = "1350786945944391733";
  const HELPER_ROLE_ID = "1519069239254974475";

  const candidates = [];
  for (const [, ow] of overwrites) {
    if (ow.type !== OverwriteType.Member) continue;
    if (!ow.allow.has(PermissionsBitField.Flags.ViewChannel)) continue;
    if (ow.id === guild.id) continue;
    if (ow.id === client.user?.id) continue;
    
    const member = await guild.members.fetch(ow.id).catch(() => null);
    if (!member || member.user.bot) continue;
    
    // Skip sellers, helpers, and owner to prevent identifying them as ticket owner
    if (member.roles.cache.has(SELLER_ROLE_ID) || member.roles.cache.has(HELPER_ROLE_ID)) continue;
    if (ow.id === guild.ownerId) continue;

    candidates.push(ow.id);
  }

  if (candidates.length === 0) return null;
  if (candidates.length === 1) return candidates[0];

  const nonGuildOwner = candidates.filter((id) => id !== guild.ownerId);
  if (nonGuildOwner.length === 1) return nonGuildOwner[0];
  if (nonGuildOwner.length > 0) return nonGuildOwner[0];
  return candidates[0];
}

async function resolveTicketOwnerId(channel, options = {}) {
  const { persist = true } = options;
  if (!channel) return null;

  const ticketData = ticketOwners.get(channel.id);
  let ownerId =
    ticketData?.userId ||
    ticketData?.user_id ||
    ticketData?.ownerId ||
    rewardTicketClaims.get(channel.id)?.userId ||
    null;

  if (!ownerId) {
    ownerId = await inferTicketOwnerFromChannel(channel);
  }

  if (ownerId && persist) {
    const existing = ticketOwners.get(channel.id) || {};
    if (!existing.userId || !existing.user_id) {
      ticketOwners.set(channel.id, {
        ...existing,
        userId: ownerId,
        user_id: ownerId,
        claimedBy: existing.claimedBy ?? null,
        ticketMessageId: existing.ticketMessageId ?? null,
        locked: !!existing.locked,
      });
      scheduleSavePersistentState();
    }
  }

  return ownerId;
}

// Helper: rebuild/edit ticket message components to reflect claim/unclaim state in a safe manner
async function editTicketMessageButtons(channel, messageId, claimerId = null) {
  try {
    const ch = channel;
    if (!ch) return;
    const msg = await ch.messages.fetch(messageId).catch(() => null);
    if (!msg) return;

    const buildClaimToggleButton = (cid) => {
      const channelIdPart = cid.replace(/^(ticket_claim_|ticket_unclaim_)/, "").split("_")[0];
      if (claimerId) {
        return new ButtonBuilder()
          .setCustomId(`ticket_unclaim_${channelIdPart}_${claimerId}`)
          .setLabel("🔓︲Odprzejmij")
          .setStyle(ButtonStyle.Secondary);
      } else {
        return new ButtonBuilder()
          .setCustomId(`ticket_claim_${channelIdPart}`)
          .setLabel("🔒︲Przejmij")
          .setStyle(ButtonStyle.Secondary);
      }
    };

    const textDisplays = [];
    if (msg.components && msg.components[0] && msg.components[0].components) {
      for (const comp of msg.components[0].components) {
        if (comp.type === 10 || comp.type === "TextDisplay" || typeof comp.content === "string") {
          const c = String(comp.content || "").trim();
          if (c && !c.startsWith("-#") && !c.includes("© 2026")) {
            textDisplays.push(c);
          }
        }
      }
    }

    const headerText = textDisplays[0] || `\`\`\`text\n🛒 NEW SHOP × TICKET\n\`\`\``;
    const bodyText = textDisplays.slice(1).join("\n") || "";

    const closeButton = new ButtonBuilder()
      .setCustomId(`ticket_close_${ch.id}`)
      .setLabel("︲Zamknij")
      .setStyle(ButtonStyle.Secondary)
      .setEmoji({ id: "1537166621527908382", name: "NO", animated: true });

    const settingsButton = new ButtonBuilder()
      .setCustomId(`ticket_settings_${ch.id}`)
      .setLabel("⚙️︲Ustawienia")
      .setStyle(ButtonStyle.Secondary);

    const claimButton = buildClaimToggleButton(`ticket_claim_${ch.id}`);

    const buttonRow = new ActionRowBuilder().addComponents(
      closeButton,
      settingsButton,
      claimButton,
    );

    const payload = buildTicketContainerPayload({
      headerText,
      bodyText,
      buttonRow,
      guildId: ch.guildId,
    });

    await msg.edit(payload);
  } catch (err) {
    console.error("editTicketMessageButtons error:", err);
  }
}

async function registerCommands() {
  try {
    console.log("Rejestrowanie slash commands...");

    // Prefer ustawienie BOT_ID przez zmienną środowiskową
    const BOT_ID = process.env.DISCORD_BOT_ID || "1449397101032112139";
    let guildCommandsRegistered = false;

    // Rejestruj komendy na konkretnym serwerze (szybsze, natychmiastowe)
    try {
      await rest.put(
        Routes.applicationGuildCommands(BOT_ID, DEFAULT_GUILD_ID),
        {
          body: commands,
        },
      );
      guildCommandsRegistered = true;
      discordApplicationDiagnostic = {
        ...discordApplicationDiagnostic,
        command_registration: 'guild_success',
        timestamp: new Date().toISOString(),
      };
      console.log(`Komendy zarejestrowane dla guild ${DEFAULT_GUILD_ID}`);
    } catch (e) {
      discordApplicationDiagnostic = {
        ...discordApplicationDiagnostic,
        command_registration: 'guild_error',
        command_registration_error: e?.code || e?.status || e?.name || 'unknown',
        timestamp: new Date().toISOString(),
      };
      console.warn(
        "Nie udało się zarejestrować komend na serwerze:",
        e.message || e,
      );
    }

    // Opcjonalnie: rejestruj globalnie tylko gdy jawnie to włączysz (globalne propagują się długo)
    if (process.env.REGISTER_GLOBAL === "true") {
      try {
        // Krótka przerwa żeby Discord mógł przepuścić zmiany (opcjonalne)
        await new Promise((r) => setTimeout(r, 1500));
        await rest.put(Routes.applicationCommands(BOT_ID), {
          body: commands,
        });
        console.log("Globalne slash commands zarejestrowane!");
      } catch (e) {
        console.warn(
          "Nie udało się zarejestrować globalnych komend:",
          e.message || e,
        );
      }
    } else if (guildCommandsRegistered) {
      // Pominięcie rejestracji nie usuwa wcześniejszych globalnych komend.
      // Czyścimy slash commands dopiero po potwierdzeniu komend serwerowych.
      // Komendy kontekstowe innych typów pozostają bez zmian.
      try {
        const globalCommands = await rest.get(Routes.applicationCommands(BOT_ID));
        for (const command of globalCommands.filter((command) => command.type === 1)) {
          await rest.delete(Routes.applicationCommand(BOT_ID, command.id));
          console.log(`[COMMANDS] Usunięto starą globalną komendę /${command.name}`);
        }
      } catch (e) {
        console.warn("Nie udało się usunąć starych globalnych komend:", e.message || e);
      }
    } else {
      console.warn("Pominięto czyszczenie globalnych komend: rejestracja serwerowych nie powiodła się.");
    }
  } catch (error) {
    console.error("Błąd rejestracji komend:", error);
  }
}

// improved apply defaults (tries to find resources by name / fallback)
async function applyDefaultsForGuild(guildId) {
  try {
    const guild =
      client.guilds.cache.get(guildId) || (await client.guilds.fetch(guildId));
    if (!guild) return;

    const normalize = (s = "") =>
      s
        .toString()
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .replace(/[^a-z0-9 ]/gi, "")
        .trim()
        .toLowerCase();

    // find opinie channel by name
    const opinie = guild.channels.cache.find(
      (c) =>
        c.type === ChannelType.GuildText &&
        (c.name === "⭐-×┃opinie-klientow" ||
          normalize(c.name).includes("opinie") ||
          normalize(c.name).includes("opinie-klientow")),
    );
    if (opinie) {
      opinieChannels.set(guildId, opinie.id);
      console.log(`Ustawiono domyślny kanał opinii: ${opinie.id}`);
    }

    // find verification role by exact name OR fallback to searching for "klient"
    let role =
      guild.roles.cache.find(
        (r) => r.name === DEFAULT_NAMES.verificationRoleName,
      ) ||
      guild.roles.cache.find((r) =>
        normalize(r.name).includes(normalize("klient")),
      );

    if (role) {
      verificationRoles.set(guildId, role.id);
      scheduleSavePersistentState();
      console.log(
        `Ustawiono domyślną rolę weryfikacji: ${role.id} (${role.name})`,
      );
    } else {
      console.log(
        `Nie znaleziono domyślnej roli weryfikacji w guild ${guildId}. Szukana nazwa: "${DEFAULT_NAMES.verificationRoleName}" lub zawierająca "klient".`,
      );
    }

    // find and set ticket categories (by name or normalized fallback)
    const categoriesMap = {};
    for (const key of Object.keys(DEFAULT_NAMES.categories)) {
      const catName = DEFAULT_NAMES.categories[key];
      const cat = guild.channels.cache.find(
        (c) =>
          c.type === ChannelType.GuildCategory &&
          (c.name === catName ||
            normalize(c.name).includes(normalize(catName))),
      );
      if (cat) {
        categoriesMap[key] = cat.id;
        console.log(`Ustawiono kategorię ${key} -> ${cat.id}`);
      }
    }
    if (Object.keys(categoriesMap).length > 0) {
      ticketCategories.set(guildId, categoriesMap);
    }
  } catch (error) {
    console.error("Błąd ustawiania domyślnych zasobów:", error);
  }
}

client.once(Events.ClientReady, async (c) => {
  c.user.setPresence({ status: "online" });
  console.log(`[READY] Bot zalogowany jako ${c.user.tag}`);
  console.log(`[READY] Bot jest na ${c.guilds.cache.size} serwerach`);
  console.log(`[READY] Bot jest online i gotowy do pracy!`);

  await persistentStateReady;

  // Timer wraca po wczytaniu stanu także w trybie core, przed jego early return.
  scheduleRandomAutoLegitCheck();
  for (const guild of c.guilds.cache.values()) {
    await ticketCategoryManager.cleanup(guild).catch(error => console.error("[ticket-category] startup cleanup:", error));
  }

  if (!ENABLE_HEAVY_STARTUP_SYNC) {
    // Najpierw dostępność komend. Pełne skanowanie historii wiadomości i paneli
    // wykonywało setki GET-ów, wpadało w globalny rate limit i blokowało reply.
    try {
      c.user.setActivity(`LegitChecki: ${legitRepCount} 🛒`, { type: 0 });
      setInterval(
        () => c.user.setActivity(`LegitChecki: ${legitRepCount} 🛒`, { type: 0 }),
        60_000,
      );
    } catch (_) {
      // Obecność nie jest krytyczna dla obsługi komend.
    }
    scheduleDailyLegitMidnightRollover();
    // Rejestracja slash commands jest lekka i musi wykonać się również w trybie
    // core. Bez tego Discord zachowuje poprzedni schemat opcji (np. Integer
    // zamiast Number dla /cennik), mimo że uruchomiony kod jest już nowszy.
    await registerCommands();
    discordApplicationDiagnostic = {
      ...discordApplicationDiagnostic,
      command_registration: 'core_registration_finished',
      timestamp: new Date().toISOString(),
    };
    console.log('[READY] Tryb core: pomijam ciężkie skanowanie historii i paneli przy starcie.');
    return;
  }

  Promise.all([
    client.rest.get('/users/@me'),
    client.rest.get('/oauth2/applications/@me'),
  ]).then(([, application]) => {
    discordApplicationDiagnostic = {
      ...discordApplicationDiagnostic,
      rest_authenticated: true,
      interactions_http_endpoint_configured: Boolean(application?.interactions_endpoint_url),
      timestamp: new Date().toISOString(),
    };
  }).catch((error) => {
    discordApplicationDiagnostic = {
      ...discordApplicationDiagnostic,
      rest_authenticated: false,
      rest_error: error?.code || error?.status || error?.name || 'unknown',
      timestamp: new Date().toISOString(),
    };
    console.error('[DISCORD_DIAGNOSTIC] Nie udało się odczytać aplikacji:', error);
  });

  for (const guild of c.guilds.cache.values()) {
    const botMember = guild.members.me;
    const missing = [];
    if (!botMember?.permissions.has(PermissionsBitField.Flags.ViewAuditLog)) missing.push("Wyświetlanie dziennika audytu");
    if (!botMember?.permissions.has(PermissionsBitField.Flags.ManageRoles)) missing.push("Zarządzanie rolami");
    if (missing.length) {
      console.error(`[anti-nuke] Brak uprawnień na ${guild.name}: ${missing.join(", ")}`);
      const owner = await guild.fetchOwner().catch(() => null);
      if (owner) {
        await owner.send(
          `⚠️ Anty-nuke na serwerze **${guild.name}** nie ma wymaganych uprawnień bota: ${missing.join(", ")}.`,
        ).catch(() => null);
      }
    }
  }

  await persistentStateReady;

  await initializeCzyLegitCounter().catch((error) =>
    console.error("[czy-legit] Blad inicjalizacji licznika reakcji:", error),
  );

  await initializeDailyLegitChart().catch((error) =>
    console.error("[daily-legit] Błąd inicjalizacji:", error),
  );

  // --- Webhook startowy do Discorda ---
  try {
    const webhookUrl = process.env.UPTIME_WEBHOOK;
    if (webhookUrl) {
      await fetch(webhookUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          content: `🟢 Bot **${c.user.tag}** został uruchomiony i działa poprawnie.`
        })
      });
      console.log("Wysłano webhook startowy.");
    } else {
      console.log("Brak UPTIME_WEBHOOK w zmiennych środowiskowych.");
    }
  } catch (err) {
    console.error("Błąd wysyłania webhooka startowego:", err);
  }

  // Ustaw status - gra w NewShop
  try {
    c.user.setActivity(`LegitChecki: ${legitRepCount} 🛒`, { type: 0 });
    setInterval(
      () => c.user.setActivity(`LegitChecki: ${legitRepCount} 🛒`, { type: 0 }),
      60000,
    );
  } catch (e) {
    // aktywność może być niedostępna na bocie, ignoruj błąd
  }

  await registerCommands();

  // try to apply defaults on the provided server id
  await applyDefaultsForGuild(DEFAULT_GUILD_ID);

  await initializeOpinionCounters().catch((error) =>
    console.error("[opinie-counter] Błąd inicjalizacji licznika:", error),
  );

  await updateFreeKasaInstructionInPlace().catch((error) =>
    console.error("[free-kasa] Nie udało się zaktualizować istniejącego panelu:", error),
  );

  // also apply defaults for all cached guilds (if names match)
  client.guilds.cache.forEach((g) => {
    applyDefaultsForGuild(g.id).catch((e) => console.error(e));
  });

  // Policz faktyczne wiadomości +rep w całej historii kanału.
  try {
    const repChannel = await c.channels.fetch(REP_CHANNEL_ID).catch(() => null);
    if (repChannel) {
      const countedRepMessages = await countExistingLegitRepMessages(repChannel);
      if (countedRepMessages !== null) {
        legitRepCount = countedRepMessages;
        scheduleSavePersistentState();
      }
      scheduleRepChannelRename(repChannel, legitRepCount).catch(() => null);
    }

  // Try to find previously sent rep info message so we can reuse it
  if (repChannel) {
    try {
      const recent = await repChannel.messages.fetch({ limit: 50 }).catch(() => null);
      if (recent) {
        for (const msg of recent.values()) {
          if (msg.author?.id === client.user.id && (msg.components?.length > 0 || msg.embeds?.length > 0)) {
            const rawStr = JSON.stringify(msg.components || []) + " " + JSON.stringify(msg.embeds || []);
            if (rawStr.includes("Jak napisać") || rawStr.includes("LEGIT CHECKI")) {
              repLastInfoMessage.set(repChannel.id, msg.id);
              console.log(`[ready] Znalazłem istniejącą wiadomość info-rep: ${msg.id}`);
              break;
            }
          }
        }
      }
    } catch (e) {
      console.warn("[ready] Błąd wyszukiwania info-rep:", e?.message);
    }
  }

  // Try to find previously sent opinion instruction messages in cached guilds
    client.guilds.cache.forEach(async (g) => {
      const opinId = opinieChannels.get(g.id);
      if (opinId) {
        try {
          const ch = await client.channels.fetch(opinId).catch(() => null);
          if (ch) {
            const found = await findBotMessageWithEmbed(
              ch,
              (emb) =>
                typeof emb.description === "string" &&
                (emb.description.includes(
                  "Użyj **komendy** </opinia:1464015495392133321>",
                ) ||
                  emb.description.includes("Użyj **komendy** `/opinia`")),
            );
            if (found) {
              lastOpinionInstruction.set(ch.id, found.id);
              console.log(
                `[ready] Znalazłem istniejącą instrukcję opinii: ${found.id} w kanale ${ch.id}`,
              );
            }
          }
        } catch (e) {
          // ignore
        }
      }

      // Try to find previously sent invite instruction messages (zaproszenia)
      try {
        const zapCh =
          g.channels.cache.get(SPRAWDZ_ZAPROSZENIA_CHANNEL_ID) ||
          (await client.channels.fetch(SPRAWDZ_ZAPROSZENIA_CHANNEL_ID).catch(() => null));
        if (zapCh) {
          await ensureInvitePanel(zapCh).catch(() => null);
        }
      } catch (e) {
        // ignore
      }
    });
  } catch (err) {
    console.error(
      "Błąd odczytywania licznika repów lub wyszukiwania wiadomości:",
      err,
    );
  }

  // Zapewnij panel zaproszeń na wyznaczonym kanale
  try {
    const zapChannel =
      client.channels.cache.get(SPRAWDZ_ZAPROSZENIA_CHANNEL_ID) ||
      (await client.channels.fetch(SPRAWDZ_ZAPROSZENIA_CHANNEL_ID).catch((e) => {
        console.error(`[ready] Błąd pobierania kanału zaproszeń ${SPRAWDZ_ZAPROSZENIA_CHANNEL_ID}:`, e?.message || e);
        return null;
      }));
    if (zapChannel) {
      console.log(`[ready] Inicjalizuję panel zaproszeń na kanale #${zapChannel.name || zapChannel.id}`);
      await ensureInvitePanel(zapChannel);
    } else {
      console.warn(`[ready] Nie znaleziono kanału zaproszeń o ID ${SPRAWDZ_ZAPROSZENIA_CHANNEL_ID}`);
    }
  } catch (err) {
    console.error("[ready] Błąd inicjalizacji panelu zaproszeń:", err);
  }

  // Initialize invite cache for all guilds
  client.guilds.cache.forEach(async (guild) => {
    try {
      const invites = await guild.invites.fetch().catch(() => null);
      const map = new Map();
      if (invites) {
        invites.each((inv) => map.set(inv.code, inv.uses));
      } else {
        console.warn(
          `[invites] Nie udało się pobrać invite'ów dla guild ${guild.id} przy starcie.`,
        );
      }
      guildInvites.set(guild.id, map);

      const vanityUses = await fetchGuildVanityUses(guild);
      if (typeof vanityUses === "number") {
        guildVanityUses.set(guild.id, vanityUses);
      }

      // ensure inviteCounts map exists
      if (!inviteCounts.has(guild.id)) inviteCounts.set(guild.id, new Map());
      if (!inviteRewards.has(guild.id)) inviteRewards.set(guild.id, new Map());
      if (!inviteRewardsGiven.has(guild.id))
        inviteRewardsGiven.set(guild.id, new Map()); // NEW
      if (!inviterRateLimit.has(guild.id))
        inviterRateLimit.set(guild.id, new Map());
      if (!inviteLeaves.has(guild.id)) inviteLeaves.set(guild.id, new Map());
      if (!inviteTotalJoined.has(guild.id)) inviteTotalJoined.set(guild.id, new Map());
      if (!inviteFakeAccounts.has(guild.id)) inviteFakeAccounts.set(guild.id, new Map());
      if (!inviteBonusInvites.has(guild.id)) inviteBonusInvites.set(guild.id, new Map());
      console.log(`[invites] Zainicjalizowano invites cache dla ${guild.id}`);
    } catch (err) {
      console.warn("[invites] Nie udało się pobrać invite'ów dla guild:", err);
    }
  });

  client.guilds.cache.forEach(async (guild) => {
    await syncTrackedFreeKasaMembers(guild).catch((error) =>
      console.error("[free-kasa] Nie udało się zsynchronizować kanału:", error),
    );
  });

  setInterval(() => {
    client.guilds.cache.forEach(async (guild) => {
      await syncTrackedFreeKasaMembers(guild).catch((error) =>
        console.error("[free-kasa] Błąd okresowej synchronizacji:", error),
      );
    });
  }, FREE_KASA_SYNC_INTERVAL_MS);

  setupDisboardAutoBump();
});

client.on(Events.MessageCreate, (message) => {
  try {
    handleDisboardBumpFeedback(message);
  } catch (error) {
    console.error("[DISBOARD] Błąd obsługi odpowiedzi DISBOARD:", error);
  }
});

const INTERACTION_WATCHDOG_MS = 30000;

function setInteractionDiagnostic(interaction, stage, details = {}) {
  lastInteractionDiagnostic = {
    command:
      interaction?.commandName ||
      interaction?.customId ||
      interaction?.constructor?.name ||
      null,
    stage,
    ...details,
    timestamp: new Date().toISOString(),
  };
}

function trackChatCommandCompletion(interaction) {
  let finalized = Boolean(interaction.replied);
  let pendingResponses = 0;
  let finalResponseAttempted = false;
  const originalMethods = {};
  const responseMethods = [
    "deferReply",
    "deferUpdate",
    "reply",
    "editReply",
    "followUp",
    "deleteReply",
    "update",
    "showModal",
  ];

  for (const methodName of responseMethods) {
    if (typeof interaction[methodName] === "function") {
      originalMethods[methodName] = interaction[methodName].bind(interaction);
    }
  }

  for (const methodName of responseMethods) {
    const original = originalMethods[methodName];
    if (!original) continue;
    interaction[methodName] = async (...args) => {
      setInteractionDiagnostic(interaction, `${methodName}-start`);
      const isDeferredAck = methodName === "deferReply" || methodName === "deferUpdate";
      if (!isDeferredAck) finalResponseAttempted = true;
      pendingResponses++;

      try {
        // Discord.js posiada własną kolejkę i respektuje retry_after. Nie nakładamy
        // krótkiego Promise.race, bo ono nie anuluje żądania i wcześniej uruchamiało
        // równoległe editReply/followUp oraz surowe webhooki.
        const result = await original(...args);
        if (!isDeferredAck) finalized = true;
        setInteractionDiagnostic(interaction, `${methodName}-success`);
        return result;
      } catch (error) {
        setInteractionDiagnostic(interaction, `${methodName}-error`, {
          error_code: error?.code || error?.status || error?.name || "unknown",
        });
        console.error(
          `[INTERACTION_RESPONSE] ${interaction.commandName || interaction.id}.${methodName} nie powiodło się:`,
          error,
        );
        throw error;
      } finally {
        pendingResponses--;
      }
    };
  }

  return {
    isFinalized: () => finalized,
    markFinalized: () => { finalized = true; },
    hasPendingResponse: () => pendingResponses > 0,
    hasFinalResponseAttempt: () => finalResponseAttempted,
  };
}

async function finishUnansweredChatCommand(interaction, completion, reason) {
  if (
    !interaction.isRepliable?.() ||
    completion.isFinalized() ||
    completion.hasPendingResponse?.() ||
    completion.hasFinalResponseAttempt?.()
  ) return;

  const fallbackPayload = {
    content: "> `❌` × Komenda nie otrzymała na czas danych potrzebnych do odpowiedzi. Spróbuj ponownie za chwilę.",
  };
  console.error(
    `[INTERACTION_RESPONSE] Brak końcowej odpowiedzi kind=${interaction.commandName || interaction.customId} id=${interaction.id} reason=${reason}`,
  );

  try {
    if (interaction.deferred) {
      await interaction.editReply(fallbackPayload);
      completion.markFinalized?.();
    } else if (interaction.replied) {
      // Odpowiedź już istnieje; dokładanie followUp tylko zwiększało rate limit.
      completion.markFinalized?.();
    } else if (interaction.isRepliable()) {
      await interaction.reply({ ...fallbackPayload, flags: [MessageFlags.Ephemeral] });
      completion.markFinalized?.();
    }
  } catch (error) {
    setInteractionDiagnostic(interaction, "fallback-error", {
      error_code: error?.code || error?.status || error?.name || "unknown",
    });
    console.error("[INTERACTION_RESPONSE] Nie udało się wysłać odpowiedzi zastępczej:", error);
  }
}

client.on(Events.InteractionCreate, async (interaction) => {
  if (interaction.isAutocomplete()) {
    try {
      const focused = interaction.options.getFocused(true);
      const choices = interaction.commandName === "panel-wyslij" && focused.name === "panel"
        ? getPanelAutocompleteChoices(interaction.options.getString("kategoria"), String(focused.value || "")) : [];
      await interaction.respond(choices);
    } catch (error) {
      console.error("[panel-wyslij] Błąd podpowiedzi:", error);
    }
    return;
  }
  const startedAt = Date.now();
  const shouldTrackResponse = Boolean(interaction.isRepliable?.());
  if (shouldTrackResponse) setInteractionDiagnostic(interaction, "started");
  const completion = shouldTrackResponse
    ? trackChatCommandCompletion(interaction)
    : { isFinalized: () => true };
  const watchdog = shouldTrackResponse
    ? setTimeout(() => {
        finishUnansweredChatCommand(interaction, completion, "watchdog-30s").catch((error) => {
          console.error("[INTERACTION_RESPONSE] Watchdog error:", error);
        });
      }, INTERACTION_WATCHDOG_MS)
    : null;

  if (shouldTrackResponse) {
    console.log(
      `[INTERACTION] Start kind=${interaction.commandName || interaction.customId || interaction.constructor?.name} id=${interaction.id} user=${interaction.user.id}`,
    );
  }

  try {
    if (interaction.isChatInputCommand()) {
      await handleSlashCommand(interaction);
    } else if (interaction.isStringSelectMenu() || interaction.isChannelSelectMenu()) {
      await handleSelectMenu(interaction);
    } else if (interaction.isModalSubmit()) {
      await handleModalSubmit(interaction);
    } else if (interaction.isButton()) {
      await handleButtonInteraction(interaction);
    }
  } catch (error) {
    if (error?.code === 10062 || error?.message?.includes("Unknown interaction")) {
      console.error("[INTERACTION] Interakcja wygasła przed odpowiedzią:", interaction.id, error?.message || error);
      return;
    }
    console.error("Błąd obsługi interakcji:", error);
    if (completion.hasPendingResponse?.() || completion.hasFinalResponseAttempt?.()) {
      console.error(
        `[INTERACTION] Pomijam kolejną odpowiedź awaryjną dla ${interaction.id}; odpowiedź końcowa była już wysyłana.`,
      );
      return;
    }
    try {
      const content = "> `❌` × Komenda nie została poprawnie zakończona. Spróbuj ponownie za chwilę.";
      if (interaction.deferred) {
        await interaction.editReply({ content });
      } else if (interaction.replied) {
        await interaction.followUp({ content, flags: [MessageFlags.Ephemeral] });
      } else if (interaction.isRepliable()) {
        await interaction.reply({ content, flags: [MessageFlags.Ephemeral] });
      }
    } catch (replyError) {
      console.error("[INTERACTION] Nie udało się wysłać odpowiedzi awaryjnej:", replyError);
    }
  } finally {
    if (watchdog) clearTimeout(watchdog);
    if (shouldTrackResponse) {
      await finishUnansweredChatCommand(interaction, completion, "handler-finished");
      const previousDiagnostic = lastInteractionDiagnostic;
      setInteractionDiagnostic(interaction, completion.isFinalized() ? "finished" : "finished-without-response", {
        duration_ms: Date.now() - startedAt,
        previous_stage: previousDiagnostic?.stage || null,
        error_code: previousDiagnostic?.error_code || null,
        http_status: previousDiagnostic?.http_status || null,
      });
      console.log(
        `[INTERACTION] Koniec kind=${interaction.commandName || interaction.customId || interaction.constructor?.name} id=${interaction.id} durationMs=${Date.now() - startedAt} deferred=${interaction.deferred} replied=${interaction.replied} finalized=${completion.isFinalized()}`,
      );
    }
  }
});


async function processDiscountCodeRedemption(interaction, inputCode) {
  // Defer first to prevent "Aplikacja nie reaguje"
  if (!interaction.deferred && !interaction.replied) {
    await interaction.deferReply({ flags: [MessageFlags.Ephemeral] }).catch(() => null);
  }

  const { code: enteredCode, codeData } = await getActiveCodeData(inputCode);

  const replyFn = async (opts) => {
    if (interaction.deferred || interaction.replied) {
      return interaction.editReply(opts).catch(() => null);
    }
    return interaction.reply({ ...opts, flags: [MessageFlags.Ephemeral] }).catch(() => null);
  };

  if (!codeData) {
    await replyFn({ content: "> `❌` × **Nieprawidłowy kod!**" });
    return;
  }

  if (
    codeData.type === "invite_cash" ||
    codeData.type === "invite_reward" ||
    codeData.type === "free_kasa_reward"
  ) {
    await replyFn({ content: "> `❌` × Ten kod odbierzesz tylko w kategorii 'Odbierz nagrodę' w TicketPanel." });
    return;
  }

  if (codeData.used) {
    await replyFn({ content: "> `❌` × **Kod** został już wykorzystany!" });
    return;
  }

  if (Date.now() > codeData.expiresAt) {
    activeCodes.delete(enteredCode);
    await db.deleteActiveCode(enteredCode).catch(() => null);
    scheduleSavePersistentState();
    await replyFn({ content: "> `❌` × **Kod** wygasł!" });
    return;
  }

  codeData.used = true;
  activeCodes.delete(enteredCode);
  await db.deleteActiveCode(enteredCode).catch(() => null);
  await db.updateActiveCode(enteredCode, { used: true }).catch(() => null);
  scheduleSavePersistentState();

  const redeemPayload = buildDiscountRedemptionPayload({
    code: enteredCode,
    discount: codeData.discount,
    guildId: interaction.guildId || null,
  });

  // Wyslij embed publiczne w kanale ticketu zeby sprzedawca widzial
  if (interaction.channel) {
    await interaction.channel.send(redeemPayload).catch(() => null);
  }
  await replyFn({ content: "> `✅` × **Pomyślnie zrealizowano kod rabatowy!**" });

  console.log(
    `Użytkownik ${interaction.user.username} odebrał kod rabatowy ${enteredCode} (-${codeData.discount}%)`,
  );
}

async function handleModalSubmit(interaction) {
  // Sprawdź czy interakcja już została odpowiedziana
  if (interaction.replied || interaction.deferred) return;

  const id = interaction.customId;

  if (id.startsWith("modal_odprzejmij")) {
    const reason = getModalTextInputValueSafe(interaction, "powod_odprzejmij");
    const expectedClaimer = id.split("_")[2] || null;
    await ticketUnclaimCommon(interaction, interaction.channelId || interaction.channel?.id, expectedClaimer, reason);
    return;
  }

  // --- DODAJ ROZLICZENIE (PRZYCISK) ---
  if (id === "modal_rozliczenie_dodaj") {
    const kwotaRaw = interaction.fields.getTextInputValue("kwota_sprzedazy");
    const kwota = parseInt(kwotaRaw.replace(/[^0-9]/g, ""), 10);

    if (isNaN(kwota) || kwota <= 0) {
      await interaction.reply({
        content: "> `❌` × Podano nieprawidłową kwotę. Wpisz samą liczbę dodatnią (np. 50).",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const userId = interaction.user.id;
    if (!weeklySales.has(userId)) {
      weeklySales.set(userId, {
        amount: 0,
        lastUpdate: Date.now(),
        paid: false,
        paidAt: null,
        guildId: interaction.guild.id,
      });
    }

    const userData = weeklySales.get(userId);
    userData.amount += kwota;
    userData.lastUpdate = Date.now();
    userData.guildId = interaction.guild.id;
    weeklySales.set(userId, userData);

    await db.saveWeeklySale(
      userId,
      userData.amount,
      interaction.guild.id,
      userData.paid || false,
      userData.paidAt || null,
      userData.lastUpdate,
    );
    scheduleSavePersistentState(true);

    const prowizja = (userData.amount * ROZLICZENIA_PROWIZJA).toFixed(2);

    const klientEmoji1 = findGuildEmojiByName(interaction.guildId, "klient");
    const userEmojiStr1 = klientEmoji1 ? toGuildEmojiMarkup(klientEmoji1) : "👤";

    const embed = new EmbedBuilder()
      .setColor(COLOR_BLUE)
      .setTitle("`💱` Rozliczenie dodane")
      .setDescription(
        `> ${userEmojiStr1} **Użytkownik:** <@${userId}>\n` +
        `> \`✅\` × **Dodano sprzedaż:** ${kwota.toLocaleString("pl-PL")} zł\n` +
        `> \`📊\` × **Suma tygodniowa:** ${userData.amount.toLocaleString("pl-PL")} zł`
      );

    await interaction.channel.send({
      embeds: [embed],
    }).catch(() => null);

    await interaction.reply({
      content: `> \`✅\` × Pomyślnie dodano **${kwota} PLN** do Twojego rozliczenia.`,
      flags: [MessageFlags.Ephemeral],
    });

    // Odśwież wiadomość z przyciskiem na dole kanału
    setTimeout(sendRozliczeniaMessage, 1000);
    return;
  }

  // --- ILE OTRZYMAM ---
  if (id === "modal_ile_otrzymam") {
    const kwotaStr = interaction.fields.getTextInputValue("kwota");
    const tryb = interaction.fields.getTextInputValue("tryb");
    const metoda = interaction.fields.getTextInputValue("metoda");

    const kwota = Number(kwotaStr);
    if (isNaN(kwota) || kwota <= 0) {
      return interaction.reply({
        flags: [MessageFlags.Ephemeral],
        content: "> `❌` × Podaj **poprawną** kwotę w PLN.",
      });
    }

    if (kwota < 5) {
      return interaction.reply({
        flags: [MessageFlags.Ephemeral],
        content: "> `❌` × Minimalna kwota to **5zł** (MYPSC **11zł**).",
      });
    }

    if (kwota > 10_000) {
      return interaction.reply({
        flags: [MessageFlags.Ephemeral],
        content: "> `❌` × Maksymalna kwota to **10 000zł**.",
      });
    }

    const rate = getRateForPlnAmount(kwota, tryb);
    const feePercent = getPaymentFeePercent(metoda);

    const base = kwota * rate;
    const fee = base * (feePercent / 100);
    const finalAmount = Math.floor(base - fee);

    return interaction.reply({
      flags: [MessageFlags.Ephemeral],
      content:
        `💰 **Otrzymasz:** ${finalAmount.toLocaleString()}\n` +
        `📉 Kurs: ${rate}\n` +
        `💸 Prowizja: ${feePercent}%\n` +
        `📌 Tryb: ${tryb}\n` +
        `📌 Metoda: ${metoda}`,
    });
  }

  // --- ILE MUSZĘ DAĆ ---
  if (id === "modal_ile_musze_dac") {
    const walutaStr = interaction.fields.getTextInputValue("waluta");
    const tryb = interaction.fields.getTextInputValue("tryb");
    const metoda = interaction.fields.getTextInputValue("metoda");

    const amount = parseShortNumber(walutaStr);
    if (isNaN(amount) || amount <= 0) {
      return interaction.reply({
        flags: [MessageFlags.Ephemeral],
        content: "> `❌` × Podaj **poprawną** ilość waluty (np. 125k / 1m).",
      });
    }

    if (amount < 22_500) {
      return interaction.reply({
        flags: [MessageFlags.Ephemeral],
        content: "> `❌` × Minimalna ilość to **22,5k** waluty.",
      });
    }

    if (amount > 999_000_000) {
      return interaction.reply({
        flags: [MessageFlags.Ephemeral],
        content: "> `❌` × Maksymalna ilość to **999 000 000** waluty.",
      });
    }

    const rate = getRateForPlnAmount(100, tryb);
    const feePercent = getPaymentFeePercent(metoda);

    const plnBase = amount / rate;
    const fee = plnBase * (feePercent / 100);
    const finalPln = Number((plnBase + fee).toFixed(2));

    return interaction.reply({
      flags: [MessageFlags.Ephemeral],
      content:
        `💸 **Musisz zapłacić:** ${finalPln} PLN\n` +
        `📉 Kurs: ${rate}\n` +
        `💸 Prowizja: ${feePercent}%\n` +
        `📌 Tryb: ${tryb}\n` +
        `📌 Metoda: ${metoda}`,
    });
  }

  // --- INNE MODALE (TWOJE) ---
  // NEW: verification modal handling
  if (interaction.customId.startsWith("modal_verify_")) {
    const modalId = interaction.customId;
    const record = pendingVerifications.get(modalId);

    if (!record) {
      await interaction.reply({
        content:
          "> `❌` × **Nie mogę** znaleźć zapisanego zadania **weryfikacji** (spróbuj ponownie).",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    if (record.userId !== interaction.user.id) {
      await interaction.reply({
        content:
          "> `❌` × **Tylko** użytkownik, który kliknął **przycisk**, może rozwiązać tę zagadkę.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const answer = interaction.fields.getTextInputValue("verification_answer");
    const isCorrect = answer.toLowerCase().trim() === record.correctAnswer.toLowerCase().trim();

    if (isCorrect) {
      try {
        // Dodaj rolę weryfikacji
        const member = await interaction.guild.members.fetch(interaction.user.id);
        await member.roles.add(record.roleId);

        // Wyślij embed potwierdzający
        const embed = new EmbedBuilder()
          .setColor(0x00ff00)
          .setTitle("✅ Weryfikacja pomyślna!")
          .setDescription(`Gratulacje! Pomyślnie przeszedłeś weryfikację.`)
          .setTimestamp();

        await interaction.reply({ embeds: [embed] });

        // Usuń z oczekujących
        pendingVerifications.delete(modalId);

        console.log(
          `Użytkownik ${interaction.user.username} przeszedł weryfikację na serwerze ${interaction.guild.id}`,
        );
      } catch (error) {
        console.error("Błąd przy nadawaniu roli po weryfikacji:", error);
        await interaction.reply({
          content: "> `❌` **Wystąpił błąd przy nadawaniu roli.**",
          flags: [MessageFlags.Ephemeral],
        });
      }
    } else {
      await interaction.reply({
        content: "> `❌` **Niepoprawna odpowiedź.** Spróbuj ponownie.",
        flags: [MessageFlags.Ephemeral],
      });
    }
    return;
  }

  // NEW: konkurs join modal
  if (interaction.customId.startsWith("konkurs_join_modal_")) {
    const msgId = interaction.customId.replace("konkurs_join_modal_", "");
    await handleKonkursJoinModal(interaction, msgId);
    return;
  }

  // KALKULATOR: ile otrzymam?
  if (interaction.customId === "modal_ile_otrzymam") {
    try {
      const kwotaStr = interaction.fields.getTextInputValue("kwota");
      const kwota = parseFloat(kwotaStr.replace(",", "."));
      const selectedServer =
        getModalStringSelectValueSafe(interaction, "kalkulator_server") || "";
      const selectedPayment =
        getModalStringSelectValueSafe(interaction, "kalkulator_payment") || "";

      if (isNaN(kwota) || kwota <= 0) {
        await interaction.reply({
          content: "> `❌` × Podaj **poprawną** kwotę w PLN.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      // globalne minimum: 5zł (MYPSC 11zł dalej w metodach)
      if (kwota < 5) {
        await interaction.reply({
          content: "> `❌` × Minimalna kwota to **5zł** (MYPSC **11zł**). Podaj większą kwotę.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      // maksymalnie 10 000 zł
      if (kwota > 10_000) {
        await interaction.reply({
          content: "> `❌` × Maksymalna kwota to **10 000zł**. Podaj mniejszą kwotę.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      if (selectedServer && selectedPayment) {
        const result = buildKalkulatorResultMessage({
          typ: "otrzymam",
          kwota,
          tryb: selectedServer,
          metoda: selectedPayment,
        });

        await interaction.reply({
          content: result.error || result.message,
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      // Fallback dla starszych wiadomości kalkulatora
      const userId = interaction.user.id;
      kalkulatorData.set(userId, { kwota, typ: "otrzymam" });

      const trybSelect = new StringSelectMenuBuilder()
        .setCustomId("kalkulator_tryb")
        .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
        .addOptions(KALKULATOR_SERVER_OPTIONS);

      const metodaSelect = new StringSelectMenuBuilder()
        .setCustomId("kalkulator_metoda")
        .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
        .addOptions(KALKULATOR_PAYMENT_OPTIONS);

      const embed = new EmbedBuilder()
        .setColor(COLOR_BLUE)
        .setDescription(
          "```\n" +
          "🔢 New Shop × Obliczanie\n" +
          "```\n" +
          `> 💵 × **Wybrana kwota:** \`${kwota.toFixed(2)}zł\`\n> ❗ × **Wybierz serwer i metodę płatności __poniżej:__`);

      await interaction.reply({
        embeds: [embed],
        components: [
          new ActionRowBuilder().addComponents(trybSelect),
          new ActionRowBuilder().addComponents(metodaSelect)
        ],
        flags: [MessageFlags.Ephemeral]
      });
    } catch (error) {
      console.error("Błąd w modal_ile_otrzymam:", error);
      await interaction.reply({
        content: "> `❌` × **Wystąpił** błąd podczas przetwarzania. Spróbuj **ponownie**.",
        flags: [MessageFlags.Ephemeral]
      });
    }
    return;
  }

  // KALKULATOR: ile muszę dać?
  if (interaction.customId === "modal_ile_musze_dac") {
    try {
      const walutaStr = interaction.fields.getTextInputValue("waluta");
      const waluta = parseShortNumber(walutaStr);
      const selectedServer =
        getModalStringSelectValueSafe(interaction, "kalkulator_server") || "";
      const selectedPayment =
        getModalStringSelectValueSafe(interaction, "kalkulator_payment") || "";

      if (!waluta || waluta <= 0 || waluta > 999_000_000) {
        await interaction.reply({
          content: "> `❌` × Podaj **poprawną** ilość waluty (1–999 000 000, możesz użyć k/m).",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      // minimalne zakupy dla "ile muszę dać" = 22.5k
      if (waluta < 22_500) {
        await interaction.reply({
          content: "> `❌` × Minimalna ilość to **22,5k** waluty. Podaj większą wartość.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      if (selectedServer && selectedPayment) {
        const result = buildKalkulatorResultMessage({
          typ: "muszedac",
          waluta,
          tryb: selectedServer,
          metoda: selectedPayment,
        });

        await interaction.reply({
          content: result.error || result.message,
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      // Fallback dla starszych wiadomości kalkulatora
      const userId = interaction.user.id;
      kalkulatorData.set(userId, { waluta, typ: "muszedac" });

      const trybSelect = new StringSelectMenuBuilder()
        .setCustomId("kalkulator_tryb")
        .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
        .addOptions(KALKULATOR_SERVER_OPTIONS);

      const metodaSelect = new StringSelectMenuBuilder()
        .setCustomId("kalkulator_metoda")
        .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
        .addOptions(KALKULATOR_PAYMENT_OPTIONS);

      const embed = new EmbedBuilder()
        .setColor(COLOR_BLUE)
        .setDescription(
          "```\n" +
          "🔢 New Shop × Obliczanie\n" +
          "```\n" +
          `> 💵 × **Wybrana waluta:** \`${formatShortWaluta(waluta)}\`\n> ❗ × **Wybierz serwer i metodę płatności __poniżej:__`);

      await interaction.reply({
        embeds: [embed],
        components: [
          new ActionRowBuilder().addComponents(trybSelect),
          new ActionRowBuilder().addComponents(metodaSelect)
        ],
        flags: [MessageFlags.Ephemeral]
      });
    } catch (error) {
      console.error("Błąd w modal_ile_musze_dac:", error);
      await interaction.reply({
        content: "> `❌` × **Wystąpił** błąd podczas przetwarzania. Spróbuj **ponownie**.",
        flags: [MessageFlags.Ephemeral]
      });
    }
    return;
  }

  // NEW: konkurs create modal
  if (interaction.customId === "konkurs_create_modal") {
    await handleKonkursCreateModal(interaction);
    return;
  }

  // redeem code modal handling (used in tickets)
  if (interaction.customId.startsWith("modal_redeem_code_")) {
    const inputCode = getModalTextInputValueSafe(interaction, "discount_code");
    await processDiscountCodeRedemption(interaction, inputCode);
    return;
  }

  // Ticket settings modals: rename/add/remove
  if (interaction.customId.startsWith("modal_rename_")) {
    const chId = interaction.customId.replace("modal_rename_", "");
    const newName = interaction.fields
      .getTextInputValue("new_ticket_name")
      .trim();
    const channel = await interaction.guild.channels
      .fetch(chId)
      .catch(() => null);
    if (!channel) {
      await interaction.reply({
        content: "> `❌` × **Kanał** nie znaleziony.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }
    const data = ticketOwners.get(chId) || { claimedBy: null };
    const claimer = data.claimedBy;

    if (!isAdminOrSeller(interaction.member)) {
      await interaction.reply({
        content: "> `❗` × Brak wymaganych uprawnień.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }
    if (
      claimer &&
      claimer !== interaction.user.id &&
      !isAdminOrSeller(interaction.member)
    ) {
      await interaction.reply({
        content: "> `❗` × Brak wymaganych uprawnień.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }
    try {
      await channel.setName(newName);
      await interaction.reply({
        content: `✅ Nazwa ticketu zmieniona na: ${newName}`,
        flags: [MessageFlags.Ephemeral],
      });
    } catch (err) {
      console.error("Błąd zmiany nazwy ticketu:", err);
      await interaction.reply({
        content: "> `❌` × **Nie udało się** zmienić nazwy (sprawdź uprawnienia).",
        flags: [MessageFlags.Ephemeral],
      });
    }
    return;
  }

  if (interaction.customId.startsWith("modal_add_")) {
    const chId = interaction.customId.replace("modal_add_", "");
    const userInput = interaction.fields
      .getTextInputValue("user_to_add")
      .trim();
    const channel = await interaction.guild.channels
      .fetch(chId)
      .catch(() => null);
    if (!channel) {
      await interaction.reply({
        content: "> `❌` × **Kanał** nie znaleziony.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }
    const data = ticketOwners.get(chId) || { claimedBy: null };
    const claimer = data.claimedBy;

    if (
      claimer &&
      claimer !== interaction.user.id &&
      !isAdminOrSeller(interaction.member)
    ) {
      await interaction.reply({
        content: "> `❗` × Brak wymaganych uprawnień.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const match = userInput.match(/^<@!?(\d+)>$/);
    if (!match) {
      await interaction.reply({
        content: "> `❌` × **Nieprawidłowy** format użytkownika. Użyj **@mention**.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }
    const userIdToAdd = match[1];
    try {
      await channel.permissionOverwrites.edit(userIdToAdd, {
        ViewChannel: true,
        SendMessages: true,
        ReadMessageHistory: true,
      });
      await interaction.reply({
        content: `✅ Dodano <@${userIdToAdd}> do ticketu.`,
        flags: [MessageFlags.Ephemeral],
      });
    } catch (err) {
      console.error("Błąd dodawania użytkownika do ticketu:", err);
      await interaction.reply({
        content: "> `❌` × **Nie udało się** dodać użytkownika (sprawdź uprawnienia).",
        flags: [MessageFlags.Ephemeral],
      });
    }
    return;
  }

  if (interaction.customId.startsWith("modal_remove_")) {
    const chId = interaction.customId.replace("modal_remove_", "");
    const userInput = interaction.fields
      .getTextInputValue("user_to_remove")
      .trim();
    const channel = await interaction.guild.channels
      .fetch(chId)
      .catch(() => null);
    if (!channel) {
      await interaction.reply({
        content: "> `❌` × **Kanał** nie znaleziony.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }
    const data = ticketOwners.get(chId) || { claimedBy: null };
    const claimer = data.claimedBy;

    if (!isAdminOrSeller(interaction.member)) {
      await interaction.reply({
        content: "> `❗` × Brak wymaganych uprawnień.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }
    if (
      claimer &&
      claimer !== interaction.user.id &&
      !isAdminOrSeller(interaction.member)
    ) {
      await interaction.reply({
        content: "> `❗` × Brak wymaganych uprawnień.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const match = userInput.match(/^<@!?(\d+)>$/);
    if (!match) {
      await interaction.reply({
        content: "> `❌` × **Nieprawidłowy** format użytkownika. Użyj **@mention**.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }
    const userIdToRemove = match[1];
    try {
      await channel.permissionOverwrites.edit(userIdToRemove, {
        ViewChannel: false,
        SendMessages: false,
        ReadMessageHistory: false,
      });
      await interaction.reply({
        content: `✅ Usunięto <@${userIdToRemove}> z ticketu.`,
        flags: [MessageFlags.Ephemeral],
      });
    } catch (err) {
      console.error("Błąd usuwania użytkownika z ticketu:", err);
      await interaction.reply({
        content: "> `❌` × **Nie udało się** usunąć użytkownika (sprawdź uprawnienia).",
        flags: [MessageFlags.Ephemeral],
      });
    }
    return;
  }

  // Ticket creation modals
  let categoryId = null;
  let ticketType = null;
  let ticketTypeLabel = null;
  let formInfo = "";

  const guild = interaction.guild;
  const user = interaction.user;
  const categories = ticketCategories.get(guild.id) || {};

  switch (interaction.customId) {
    case "modal_odbior": {
      const enteredCodeRaw =
        interaction.fields.getTextInputValue("reward_code") || "";
      const { code: enteredCode, codeData } =
        await getActiveCodeData(enteredCodeRaw);

      if (!enteredCode) {
        await interaction.reply({
          content: "> `❌` × Wpisz kod nagrody przed wysłaniem formularza.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      if (!codeData) {
        await interaction.reply({
          content: "> `❌` × Ten kod jest nieprawidłowy.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      if (
        codeData.type !== "invite_cash" &&
        codeData.type !== "invite_reward" &&
        codeData.type !== "free_kasa_reward"
      ) {
        await interaction.reply({
          content:
            "> `❌` × Ten kod nie jest kodem nagrody do odbioru w tej kategorii.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      if (codeData.used) {
        await interaction.reply({
          content: "> `❌` × Ten kod został już wykorzystany.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      if (Date.now() > codeData.expiresAt) {
        activeCodes.delete(enteredCode);
        scheduleSavePersistentState();
        await interaction.reply({
          content: "> `❌` × Ten kod wygasł.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      // Mark code as used
      codeData.used = true;
      activeCodes.delete(enteredCode);
      await db.deleteActiveCode(enteredCode);
      scheduleSavePersistentState();

      categoryId = REWARDS_CATEGORY_ID;
      ticketType = "odbior-nagrody";
      ticketTypeLabel = "NAGRODA";
      formInfo = `> <a:arrowwhite:1491476759290449984> × **Kod:** \`${enteredCode}\`\n> <a:arrowwhite:1491476759290449984> × **Nagroda:** \`${codeData.rewardText || codeData.reward || "Brak"}\``;
      break;
    }
    case "modal_inne": {
      const sprawa = interaction.fields.getTextInputValue("sprawa");

      categoryId = categories["inne"];
      ticketType = "inne";
      ticketTypeLabel = "PYTANIE";
      formInfo = `> <a:arrowwhite:1491476759290449984> × **Sprawa:** ${formatInlineCodeText(sprawa)}`;
      break;
    }
    default:
      break;
  }

  // If ticketType not set it was probably a settings modal handled above or unknown
  if (!ticketType) return;

  try {
    // ENFORCE: One ticket per user
    // Search ticketOwners for existing open ticket owned by this user
    for (const [channelId, ticketData] of ticketOwners.entries()) {
      if (ticketData.userId === user.id) {
        await interaction.reply({
          content:
            `> \`❌\` × **Masz już otwarty** ticket: <#${channelId}>\n` +
            "> `ℹ️` × Zamknij go, zanim otworzysz nowy.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }
    }

    const parentToUse = categoryId || categories["zakup-0-20"];

    const createOptions = {
      name: `ticket-${getNextTicketNumber(guild.id)}`,
      type: ChannelType.GuildText,
      parent: parentToUse,
      permissionOverwrites: [
        {
          id: interaction.guild.id,
          deny: [PermissionsBitField.Flags.ViewChannel], // @everyone nie widzi ticketów
        },
        {
          id: "1537090439239442483", // zawieszony – nie widzi żadnych ticketów
          deny: [PermissionsBitField.Flags.ViewChannel],
        },
        {
          id: interaction.user.id,
          allow: [
            PermissionsBitField.Flags.ViewChannel,
            PermissionsBitField.Flags.SendMessages,
            PermissionsBitField.Flags.ReadMessageHistory,
          ],
        },
      ],
    };

    // Limity wynikają z typu ticketu, a nie z ID odtwarzanej kategorii.
    if (ticketType === "inne") {
      createOptions.permissionOverwrites.push({ id: guild.ownerId, allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory] });
    } else if (ticketType.startsWith("zakup-") || ticketType === "sprzedaz") {
      createOptions.permissionOverwrites.push(...getLimitRolePermissions(ticketType, null));
    }

    const channel = await ticketCategoryManager.create(interaction.guild, ticketType, createOptions);

    const headerText = `\`\`\`text\n🛒 NEW SHOP × ${ticketTypeLabel}\n\`\`\``;
    const bodyText =
      `### ・ \`👤\` × **Informacje o kliencie:**\n` +
      `> <a:arrowwhite:1491476759290449984> × **Ping:** <@${user.id}>\n` +
      `> <a:arrowwhite:1491476759290449984> × **Nick:** ${formatInlineCodeText(getSafeTicketDisplayName(interaction.member, user))}\n` +
      `> <a:arrowwhite:1491476759290449984> × **ID:** \`${user.id}\`\n` +
      `### ・ \`📋\` × **Informacje z formularza:**\n` +
      `${formInfo}`;

    const closeButton = new ButtonBuilder()
      .setCustomId(`ticket_close_${channel.id}`)
      .setLabel("︲Zamknij")
      .setStyle(ButtonStyle.Secondary)
      .setEmoji({ id: "1537166621527908382", name: "NO", animated: true });
    const settingsButton = new ButtonBuilder()
      .setCustomId(`ticket_settings_${channel.id}`)
      .setLabel("⚙️︲Ustawienia")
      .setStyle(ButtonStyle.Secondary);
    const claimButton = new ButtonBuilder()
      .setCustomId(`ticket_claim_${channel.id}`)
      .setLabel("🔒︲Przejmij")
      .setStyle(ButtonStyle.Secondary);

    const buttonRow = new ActionRowBuilder().addComponents(
      closeButton,
      settingsButton,
      claimButton,
    );

    const payload = buildTicketContainerPayload({
      headerText,
      bodyText,
      buttonRow,
      guildId: interaction.guild?.id,
    });

    {
      const _pingRoles = getPingRolesForTicketType(ticketType);
      if (_pingRoles.length > 0) {
        await channel.send({
          content: _pingRoles.map(id => `<@&${id}>`).join(" "),
          allowedMentions: { roles: _pingRoles },
        }).catch(() => null);
      }
    }

    const sentMsg = await channel.send(payload);

    ticketOwners.set(channel.id, {
      claimedBy: null,
      userId: user.id,
      ticketMessageId: sentMsg.id,
      locked: false,
      ticketTypeLabel,
      formInfo,
      openedAt: Date.now(),
    });
    if (isRewardTicketLabel(ticketTypeLabel)) {
      rewardTicketClaims.set(channel.id, {
        guildId: guild.id,
        userId: user.id,
        createdAt: Date.now(),
      });
    }
    scheduleSavePersistentState();

    await logTicketCreation(interaction.guild, channel, {
      openerId: user.id,
      ticketTypeLabel,
      formInfo,
      ticketChannelId: channel.id,
      ticketMessageId: sentMsg.id,
    }).catch(() => { });

    await interaction.reply({
      content: `> ✅ **Utworzono ticket! Przejdź do:** <#${channel.id}>.`,
      flags: [MessageFlags.Ephemeral],
    });
  } catch (err) {
    console.error("Błąd tworzenia ticketu (odbior):", err);
    await interaction.reply({
      content: "> `❌` × **Wystąpił** błąd podczas tworzenia **ticketa**.",
      flags: [MessageFlags.Ephemeral],
    });
  }
}

async function handleKalkulatorSelect(interaction) {
  try {
    // Defer the interaction to avoid timeout
    await interaction.deferUpdate();

    const userId = interaction.user.id;
    const customId = interaction.customId;
    const selectedValue = interaction.values[0];

    // Pobierz aktualne dane użytkownika
    const userData = kalkulatorData.get(userId) || {};

    // Zaktualizuj odpowiednie pole
    if (customId === "kalkulator_tryb") {
      userData.tryb = selectedValue;
    } else if (customId === "kalkulator_metoda") {
      userData.metoda = selectedValue;
    }

    // Zapisz dane
    kalkulatorData.set(userId, userData);

    // Jeśli oba pola są wypełnione, oblicz i pokaż wynik
    if (userData.tryb && userData.metoda) {
      await handleKalkulatorSubmit(interaction, userData.typ);
    }
  } catch (error) {
    console.error("Błąd w handleKalkulatorSelect:", error);
    if (!interaction.replied && !interaction.deferred) {
      await interaction.reply({
        content: "> `❌` × **Wystąpił** błąd podczas przetwarzania wyboru. Spróbuj **ponownie**.",
        flags: [MessageFlags.Ephemeral]
      });
    } else {
      await interaction.followUp({
        content: "> `❌` × **Wystąpił** błąd podczas przetwarzania wyboru. Spróbuj **ponownie**.",
        flags: [MessageFlags.Ephemeral]
      });
    }
  }
}

async function handleKalkulatorSubmit(interaction, typ) {
  try {
    const userId = interaction.user.id;
    const userData = kalkulatorData.get(userId) || {};

    if (!userData.tryb || !userData.metoda) {
      await interaction.followUp({
        content: "> `❌` × **Proszę** wybrać zarówno tryb jak i metodę **płatności**.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const result = buildKalkulatorResultMessage({
      typ,
      kwota: userData.kwota,
      waluta: userData.waluta,
      tryb: userData.tryb,
      metoda: userData.metoda,
    });

    await interaction.editReply({
      content: result.error || result.message,
      embeds: [],
      components: []
    });

    // Wyczyść dane użytkownika
    kalkulatorData.delete(userId);
  } catch (error) {
    console.error("Błąd w handleKalkulatorSubmit:", error);
    if (!interaction.replied && !interaction.deferred) {
      await interaction.reply({
        content: "> `❌` × **Wystąpił** błąd podczas obliczania. Spróbuj **ponownie**.",
        flags: [MessageFlags.Ephemeral]
      });
    } else {
      await interaction.followUp({
        content: "> `❌` × **Wystąpił** błąd podczas obliczania. Spróbuj **ponownie**.",
        flags: [MessageFlags.Ephemeral]
      });
    }
  }
}

async function detectServerFromContext(interaction) {
  let textToSearch = "";

  // 1. Channel name (try cache, then fetch if missing)
  let channel = interaction.channel;
  if (!channel || !channel.name) {
    if (interaction.guild) {
      channel = interaction.guild.channels.cache.get(interaction.channelId);
      // Removed fetch to prevent 3-second modal timeout
      // if (!channel) {
      //   channel = await interaction.guild.channels.fetch(interaction.channelId).catch(() => null);
      // }
    }
  }

  if (channel && channel.name) {
    textToSearch += " " + channel.name;
  }

  // 2. Message content
  if (interaction.message?.content) {
    textToSearch += " " + interaction.message.content;
  }

  // 3. Message embeds (title, description, fields)
  if (interaction.message?.embeds) {
    for (const embed of interaction.message.embeds) {
      if (embed.title) textToSearch += " " + embed.title;
      if (embed.description) textToSearch += " " + embed.description;
      if (embed.author?.name) textToSearch += " " + embed.author.name;
      if (Array.isArray(embed.fields)) {
        for (const field of embed.fields) {
          if (field.name) textToSearch += " " + field.name;
          if (field.value) textToSearch += " " + field.value;
        }
      }
    }
  }

  const normalized = textToSearch.toLowerCase();

  if (normalized.includes("skypvp") || normalized.includes("minestar-sky") || normalized.includes("minestarsky")) {
    return { testValue: "minestar_skypvp", calcValue: "MINESTAR_SKY" };
  }
  if (normalized.includes("minestar")) {
    return { testValue: "minestar_lf", calcValue: "MINESTAR_LF" };
  }
  if (normalized.includes("donut")) {
    return { testValue: "donut_smp", calcValue: "DONUT_SMP" };
  }
  if (normalized.includes("boxpvp") || normalized.includes("anarchia-box")) {
    return { testValue: "anarchia_boxpvp", calcValue: "ANARCHIA_BOXPVP" };
  }
  if (normalized.includes("anarchia-lf") || normalized.includes("anarchialf") || normalized.includes("anarchia lf")) {
    return { testValue: "anarchia_lf", calcValue: "ANARCHIA_LIFESTEAL" };
  }

  return null;
}

async function handleButtonInteraction(interaction) {
  const customId = interaction.customId;
  const botName = client.user?.username || "NEWSHOP";
  const detectedServer = await detectServerFromContext(interaction);

  if (customId && customId.startsWith("copy_dm_code_")) {
    const code = customId.replace("copy_dm_code_", "");
    await interaction.reply({
      content: `\`${code}\``,
      flags: [MessageFlags.Ephemeral],
    }).catch(() => null);
    return;
  }

  if (customId === "ticket_copy_rep") {
    let repText = ticketRepMessages.get(interaction.message?.id) || ticketRepMessages.get(interaction.channelId) || null;
    if (!repText) {
      const ticketData = pendingTicketClose.get(interaction.channelId);
      if (ticketData?.repMessage) {
        repText = ticketData.repMessage;
      } else if (ticketData?.commandUsername && ticketData?.co) {
        const verb = String(ticketData.typ || "zakup").toUpperCase();
        repText = `+rep @${ticketData.commandUsername} ${verb} ${ticketData.co}${ticketData.serwer ? ` ${ticketData.serwer}` : ""}`;
      }
    }

    if (!repText && interaction.channel) {
      const recent = await interaction.channel.messages.fetch({ limit: 15 }).catch(() => null);
      if (recent) {
        const plainMsg = recent.find((m) => m.author.id === client.user.id && m.content && m.content.startsWith("+rep"));
        if (plainMsg) {
          repText = plainMsg.content.trim();
        } else {
          for (const msg of recent.values()) {
            if (msg.author.id === client.user.id) {
              const fullText = JSON.stringify(msg.components || []) + " " + JSON.stringify(msg.embeds || []);
              const match = fullText.match(/\+rep\s+@[^\s`"\\]+\s+[^`"\\]+/);
              if (match) {
                repText = match[0].trim();
                break;
              }
            }
          }
        }
      }
    }

    if (!repText) {
      repText = `+rep @${client.user?.username || "sprzedawca"} ZAKUP 50 PLN`;
    }

    await interaction.reply({
      content: `\`${repText}\``,
      flags: [MessageFlags.Ephemeral],
    }).catch(() => null);
    return;
  }

  if (customId === "daily_legit_record") {
    const records = {
      ...dailyLegitRecords,
      [dailyLegitStats.dateKey]: {
        dateKey: dailyLegitStats.dateKey,
        total: dailyLegitStats.total,
        totalPln: dailyLegitStats.totalPln,
      },
    };
    const allRecords = Object.values(records);
    const legitRecord = allRecords.reduce(
      (best, record) => (!best || record.total > best.total ? record : best),
      null,
    );
    const moneyRecord = allRecords.reduce(
      (best, record) => (!best || record.totalPln > best.totalPln ? record : best),
      null,
    );
    const formatRecordDate = (dateKey) =>
      /^\d{4}-\d{2}-\d{2}$/.test(String(dateKey || ""))
        ? `${dateKey.slice(8, 10)}.${dateKey.slice(5, 7)}.${dateKey.slice(0, 4)}`
        : String(dateKey || "-");

    const recordContainer = new ContainerBuilder().setAccentColor(COLOR_BLUE);
    recordContainer.addTextDisplayComponents(
      new TextDisplayBuilder().setContent(
        "```\n" +
        "🏆 New Shop × REKORDY LC\n" +
        "```",
      ),
    );
    recordContainer.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
    recordContainer.addTextDisplayComponents(
      new TextDisplayBuilder().setContent(
        `> \`✅\` <a:arrowwhite:1491476759290449984> **Najwięcej legit checków:** \`${legitRecord?.total || 0}\` - \`${formatRecordDate(legitRecord?.dateKey)}\`\n` +
        `> \`💰\` <a:arrowwhite:1491476759290449984> **Największa wydana kwota:** \`${Number(moneyRecord?.totalPln || 0).toFixed(2)} PLN\` - \`${formatRecordDate(moneyRecord?.dateKey)}\``,
      ),
    );
    appendBrandFooterToContainer(recordContainer, interaction.guildId);
    await interaction.reply({
      components: [recordContainer],
      flags: MessageFlags.IsComponentsV2 | MessageFlags.Ephemeral,
    });
    return;
  }

  if (customId.startsWith("usunzakup_")) {
    await handleUsunZakupButton(interaction);
    return;
  }

  if (customId === "panel_klienta_spent") {
    await handlePanelKlientaSpent(interaction);
    return;
  }

  const clientPanelCodesMatch = customId.match(/^panel_klienta_(?:codes|history)_(\d+)$/);
  if (clientPanelCodesMatch) {
    const pageIndex = Number(clientPanelCodesMatch[1]);
    await handlePanelKlientaActiveCodes(interaction, pageIndex);
    return;
  }

  if (customId === "ticket_anon_close") {
    const ticketData = pendingTicketClose.get(interaction.channelId);
    if (!ticketData || !ticketData.awaitingRep) {
      await interaction.reply({
        content: "> `❌` Brak oczekującego legit checka w tym kanale! Najpierw sprzedawca musi użyć komendy **/ticket-zakoncz**.",
        flags: [MessageFlags.Ephemeral]
      });
      return;
    }

    if (ticketData.processing) {
      await interaction.reply({
        content: "> `⏳` Ten ticket jest już w trakcie zamykania...",
        flags: [MessageFlags.Ephemeral]
      });
      return;
    }

    // Lock immediately using processing flag to prevent double/triple-click concurrency
    ticketData.processing = true;

    await interaction.deferUpdate();

    try {
      await closeTicketAnonymously(interaction.channel, interaction.guild, interaction.user.id);
    } catch (err) {
      // Re-enable if closing failed
      ticketData.processing = false;
      console.error("Błąd przy zamykaniu ticketu przez przycisk anonimowo:", err);
      await interaction.followUp({
        content: `> \`❌\` Wystąpił błąd: ${err.message}`,
        flags: [MessageFlags.Ephemeral]
      }).catch(() => null);
    }
    return;
  }

  if (customId === "btn_sprawdz_zaproszenia") {
    await handleSprawdzZaproszeniaCommand(interaction);
    return;
  }

  if (customId === "btn_wystaw_opinie") {
    // Sprawdź cooldown (48h)
    const lastUsed = opinionCooldowns.get(interaction.user.id) || 0;
    if (Date.now() - lastUsed < OPINION_COOLDOWN_MS) {
      const remaining = OPINION_COOLDOWN_MS - (Date.now() - lastUsed);
      await interaction.reply({
        content: `> \`❌\` × Możesz wystawić opinię ponownie za \`${humanizeMs(remaining)}\``,
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    await interaction.showModal(buildOpinionModal());
    return;
  }

  if (customId === "seller_data_edit_main") {
    if (!isAdminOrSeller(interaction.member)) {
      await interaction.reply({
        content: "> `❗` × Ten panel jest tylko dla sprzedawców.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    await interaction.showModal(buildSellerPaymentDataModalMain(interaction));
    return;
  }

  if (customId === "seller_data_edit_extra") {
    if (!isAdminOrSeller(interaction.member)) {
      await interaction.reply({
        content: "> `❗` × Ten panel jest tylko dla sprzedawców.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    await interaction.showModal(buildSellerPaymentDataModalExtra(interaction));
    return;
  }
  if (customId === "seller_data_clear") {
    if (!isAdminOrSeller(interaction.member)) {
      await interaction.reply({
        content: "> `❗` × Ten panel jest tylko dla sprzedawców.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const key = getSellerPaymentProfileKey(interaction.guildId, interaction.user.id);
    const existing = sellerPaymentProfiles.get(key);

    if (existing) {
      sellerDataBackups.set(key, { ...existing });
      sellerPaymentProfiles.delete(key);
      scheduleSavePersistentState(true);
    }

    const container = new ContainerBuilder()
      .setAccentColor(COLOR_BLUE)
      .addTextDisplayComponents(
        new TextDisplayBuilder().setContent(
          "> `🧹` × Pomyślnie wyczyszczono dane płatności."
        )
      )
      .addSeparatorComponents(new SeparatorBuilder().setDivider(true));

    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId("seller_data_restore")
        .setLabel("↩️︲Przywróć")
        .setStyle(ButtonStyle.Secondary)
    );

    container.addActionRowComponents(row);
    appendBrandFooterToContainer(container, interaction.guildId);

    await interaction.reply({
      components: [container],
      flags: MessageFlags.Ephemeral | MessageFlags.IsComponentsV2,
    });
    return;
  }

  if (customId === "seller_data_restore") {
    const key = getSellerPaymentProfileKey(interaction.guildId, interaction.user.id);
    const backup = sellerDataBackups.get(key);
    if (!backup) {
      await interaction.reply({
        content: "> `❌` × Brak kopii danych do przywrócenia.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    sellerPaymentProfiles.set(key, backup);
    sellerDataBackups.delete(key);
    scheduleSavePersistentState(true);

    await interaction.reply({
      content: "> `✅` × Pomyślnie przywrócono dane płatności.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  if (customId === "free_kasa_roll") {
    await handleFreeKasaCommand(interaction);
    return;
  }

  if (customId === "free_kasa_claim") {
    await showOdbiorModal(interaction);
    return;
  }

  // KONKURSY: obsługa przycisków konkursowych
  if (customId.startsWith("konkurs_join_")) {
    const msgId = customId.replace("konkurs_join_", "");
    await handleKonkursJoinDirect(interaction, msgId);
    return;
  }

  if (customId.startsWith("konkurs_leave_")) {
    const msgId = customId.replace("konkurs_leave_", "");
    await handleKonkursLeave(interaction, msgId);
    return;
  }

  if (customId.startsWith("konkurs_cancel_leave_")) {
    const msgId = customId.replace("konkurs_cancel_leave_", "");
    await handleKonkursCancelLeave(interaction, msgId);
    return;
  }

  if (customId.startsWith("confirm_leave_")) {
    const msgId = customId.replace("confirm_leave_", "");
    await handleKonkursLeave(interaction, msgId);
    return;
  }

  if (customId.startsWith("cancel_leave_")) {
    const cancelEmbed = new EmbedBuilder()
      .setColor(COLOR_BLUE)
      .setDescription("> `📋` × Anulowano");

    await interaction.update({
      embeds: [cancelEmbed],
      components: [],
    });
    return;
  }

  async function handleModyVideosAction(interaction) {
    try {
      await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });
    } catch (err) {
      // Interaction token already expired or already acknowledged.
      console.warn("[mody] Nie udało się potwierdzić interakcji przycisku:", err?.code || err);
      return;
    }

    const resolvedVideos = [];
    const seenKeys = new Set();
    const seenUrls = new Set();

    const addResolvedVideo = (videoCfg, url, labelFallback = "Nagranie") => {
      if (!isHttpUrl(url)) return;
      const key = videoCfg?.key ? `key:${videoCfg.key}` : `url:${url}`;
      if (seenKeys.has(key) || seenUrls.has(url)) return;
      seenKeys.add(key);
      seenUrls.add(url);
      resolvedVideos.push({
        videoCfg: videoCfg || null,
        url,
        labelFallback,
      });
    };

    // 1) Najpierw bierzemy video z wiadomości panelu (to najszybsza ścieżka).
    const fromCurrentMessage = collectVideoLinksFromMessage(interaction.message);
    for (const item of fromCurrentMessage) {
      const cfgFromAttachment =
        (item?.key && MODS_VIDEO_FILES.find((v) => v.key === item.key)) ||
        getModsVideoConfigByFilename(item?.label || "");
      addResolvedVideo(
        cfgFromAttachment,
        item?.url || "",
        item?.modName || item?.label || "Nagranie",
      );
    }

    // 2) Dołóż źródła z resolvera z preferencją Discord CDN (slow-scan + fallbacki).
    for (const videoCfg of MODS_VIDEO_FILES) {
      const url = await resolveModsVideoUrl(interaction.guild, videoCfg, {
        allowSlowScan: true,
      });
      addResolvedVideo(
        videoCfg,
        url,
        videoCfg.modName || videoCfg.label || "Nagranie",
      );
    }

    if (resolvedVideos.length > 0) {
      const MAX_VIDEO_MESSAGES = 10;
      resolvedVideos.sort((a, b) => {
        const rankA = getModsVideoOrderRank(a.videoCfg);
        const rankB = getModsVideoOrderRank(b.videoCfg);
        if (rankA !== rankB) return rankA - rankB;
        const keyA = a.videoCfg?.key || a.labelFallback || "";
        const keyB = b.videoCfg?.key || b.labelFallback || "";
        return keyA.localeCompare(keyB, "pl");
      });
      const videosToSend = resolvedVideos.slice(0, MAX_VIDEO_MESSAGES);
      let sentAtLeastOneVideo = false;
      let firstResponseSent = false;

      const sendVideoMessage = async (payload) => {
        if (!firstResponseSent) {
          await interaction.editReply(payload);
          firstResponseSent = true;
          return;
        }
        await interaction.followUp({
          ...payload,
          flags: payload.flags | MessageFlags.Ephemeral,
        });
      };

      for (let i = 0; i < videosToSend.length; i += 1) {
        const video = videosToSend[i];
        const videoCfg = video.videoCfg || null;
        const caption = getModsVideoCaption(videoCfg, video.labelFallback || "Nagranie");
        const localPath = resolveLocalModsVideoPath(videoCfg);
        let sentThisVideo = false;

        if (localPath) {
          let size = 0;
          try {
            size = fs.statSync(localPath).size || 0;
          } catch {
            size = 0;
          }

          if (size > 0 && size <= DISCORD_MAX_UPLOAD_BYTES) {
            const ext = path.extname(localPath) || ".mp4";
            const baseName =
              (videoCfg?.key || `video_${i + 1}`)
                .toString()
                .replace(/[^a-z0-9_-]/gi, "_") || `video_${i + 1}`;
            const attachment = new AttachmentBuilder(localPath, {
              name: `${baseName}${ext.toLowerCase()}`,
            });

            try {
              await sendVideoMessage(buildModVideoPreview(
                caption, `attachment://${attachment.name}`, [attachment],
              ));
              sentAtLeastOneVideo = true;
              sentThisVideo = true;
              continue;
            } catch (err) {
              console.warn(
                `[mody] Nie udało się wysłać pliku ${path.basename(localPath)}; próbuję link fallback.`,
                err?.code || err?.message || err,
              );
            }
          }
        }

        // Reference remote media explicitly; ephemeral replies don't unfurl plain links.
        if (!sentThisVideo && isHttpUrl(video.url)) {
          try {
            await sendVideoMessage(buildModVideoPreview(caption, video.url));
            sentAtLeastOneVideo = true;
            sentThisVideo = true;
          } catch (err) {
            console.warn(
              "[mody] Nie udało się wysłać fallback linku:",
              err?.code || err?.message || err,
            );
          }
        }

        if (!sentThisVideo) {
          console.warn(
            `[mody] Pominięto video ${videoCfg?.key || video.labelFallback || i + 1} (brak pliku <= limit i brak działającego URL).`,
          );
        }
      }

      if (!sentAtLeastOneVideo) {
        const failMsg =
          "> `❌` × Nie udało się wysłać nagrań. Sprawdź uprawnienia i źródła plików.";
        if (!firstResponseSent) {
          await interaction.editReply({ content: failMsg, embeds: [], components: [] });
        } else {
          await interaction.followUp({
            content: failMsg,
            flags: [MessageFlags.Ephemeral],
          });
        }
      }
      return;
    }

    const localVideo =
      MODS_VIDEO_FILES
        .map((cfg) => ({
          cfg,
          localPath: resolveLocalModsVideoPath(cfg),
        }))
        .find((item) => !!item.localPath) || null;

    if (localVideo) {
      let videoSize = 0;
      try {
        videoSize = fs.statSync(localVideo.localPath).size || 0;
      } catch {
        videoSize = 0;
      }

      const sizeMb = (videoSize / 1024 / 1024).toFixed(1);
      const limitMb = (DISCORD_MAX_UPLOAD_BYTES / 1024 / 1024).toFixed(0);
      await interaction.editReply({
        content:
          `> \`❌\` × Nie mam publicznego linku do **${path.basename(localVideo.localPath)}**.\n` +
          `> \`ℹ️\` × Lokalny plik ma \`${sizeMb} MB\`, a limit uploadu Discord to ok. \`${limitMb} MB\`.\n` +
          `> \`✅\` × Ustaw URL w env \`${localVideo.cfg.envVar}\` (albo wrzuć film na kanał i kliknij przycisk ponownie).`,
      });
      return;
    }

    await interaction.editReply({
      content:
        "> `❌` × Nie znaleziono żadnych nagrań modów ani linków do nich.",
    });
  }

  if (customId.startsWith("mody_videos_")) {
    await handleModyVideosAction(interaction);
    return;
  }

  if (customId.startsWith("mody_buy_")) {
    await showModyZakupModal(interaction);
    return;
  }

  const embedTestPublishStartMatch = customId.match(
    /^embedtest_publish_start_(\d+)$/,
  );
  if (embedTestPublishStartMatch) {
    const [, messageId] = embedTestPublishStartMatch;
    const state = embedTestStates.get(messageId);

    if (!state) {
      await interaction.reply({
        content: "> `❌` × Ta sesja edycji wygasła. Użyj `/embed-2` ponownie.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    if (state.ownerId !== interaction.user.id) {
      await interaction.reply({
        content: "> `❗` × Tylko autor testu może zakończyć ten embed.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    pendingEmbedTestPublish.set(
      getPendingEmbedTestPublishKey(interaction.guildId, interaction.user.id),
      {
        messageId,
        sourceChannelId: interaction.channelId,
        expiresAt: Date.now() + 2 * 60 * 1000,
      },
    );

    await interaction.reply(buildEmbedTestPublishPrompt(state));
    return;
  }

  const embedTestBuyOpenMatch = customId.match(
    /^embedtest_buy_open(?:_(zakup|zakup_autorynku|zakup_moda|zostansprzedawca|sprzedaz|odbior|inne|panel|regulamin|nagrania|kalkulator|sprawdz_bonusy))?$/,
  );
  if (embedTestBuyOpenMatch) {
    const action = embedTestBuyOpenMatch[1] || "zakup";

    switch (action) {
      case "zostansprzedawca":
        await showSellerLimitModal(interaction);
        break;
      case "zakup":
        await showZakupModal(interaction, detectedServer);
        break;
      case "zakup_autorynku":
        await showAutoRynekZakupModal(interaction);
        break;
      case "zakup_moda":
        await showModyZakupModal(interaction);
        break;
      case "sprzedaz":
        await showSprzedazModal(interaction);
        break;
      case "odbior":
        await showOdbiorModal(interaction);
        break;
      case "inne":
        await showInneModal(interaction);
        break;
      case "panel":
        await interaction.reply({
          ...buildTicketPanelPayload(),
          flags: [MessageFlags.Ephemeral],
        });
        break;
      case "regulamin":
        await openRegulationPanelViewer(interaction, interaction.message?.id || "");
        break;
      case "nagrania":
        await handleModyVideosAction(interaction);
        break;
      case "kalkulator":
        await interaction.showModal(buildKalkulatorModal("otrzymam", detectedServer));
        break;
      case "sprawdz_bonusy":
        await handleSprawdzBonusyButton(interaction);
        break;
      default:
        await showZakupModal(interaction, detectedServer);
        break;
    }
    return;
  }

  const regulationPageMatch = customId.match(/^regulamin_page_(\d+)_(\d+)$/);
  if (regulationPageMatch) {
    const [, panelMessageId, pageIndex] = regulationPageMatch;
    await openRegulationPanelViewer(
      interaction,
      panelMessageId,
      Number(pageIndex),
      true,
    );
    return;
  }

  const regulationEditorMatch = customId.match(
    /^regulamin_editor_(prev|next|edit|add|delete)_(\d+)_(\d+)$/,
  );
  if (regulationEditorMatch) {
    const [, action, messageId, rawPageIndex] = regulationEditorMatch;
    const state = embedTestStates.get(messageId);

    if (!state || !isRegulationEmbedState(state)) {
      await interaction.reply({
        content: "> `❌` × Ta sesja edycji wygasła. Użyj `/sprawdz-embed-2` ponownie.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    if (state.ownerId !== interaction.user.id) {
      await interaction.reply({
        content: "> `❗` × Tylko autor panelu może edytować ten regulamin.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const pages = getRegulationPanelPages(state);
    const safeIndex = Math.max(
      0,
      Math.min(Number(rawPageIndex) || 0, pages.length - 1),
    );

    if (action === "edit") {
      await interaction.showModal(buildRegulationPageModal(state, safeIndex));
      return;
    }

    if (action === "prev" || action === "next") {
      const nextIndex =
        action === "prev"
          ? Math.max(0, safeIndex - 1)
          : Math.min(pages.length - 1, safeIndex + 1);
      await interaction.update(buildRegulationPagesEditorPayload(state, nextIndex));
      return;
    }

    const nextPages = pages.map((page) => normalizeRegulationPage(page));
    let nextIndex = safeIndex;

    if (action === "add") {
      nextIndex = Math.min(safeIndex + 1, nextPages.length);
      nextPages.splice(nextIndex, 0, {
        title: `> # ${nextIndex + 1}. __Nowa strona__`,
        body: "> :strzałka: Uzupełnij treść tej strony regulaminu.",
      });
    } else if (action === "delete") {
      if (nextPages.length <= 1) {
        await interaction.update(buildRegulationPagesEditorPayload(state, safeIndex));
        return;
      }

      nextPages.splice(safeIndex, 1);
      nextIndex = Math.max(0, Math.min(safeIndex, nextPages.length - 1));
    }

    setRegulationPagesOnState(state, nextPages);
    embedTestStates.set(messageId, state);

    const updated = await updateEmbedTestMessage(state);
    if (!updated) {
      embedTestStates.delete(messageId);
      await interaction.update({
        content:
          "> `❌` × Nie udało się zaktualizować panelu regulaminu. Użyj `/sprawdz-embed-2` ponownie.",
        embeds: [],
        components: [],
      });
      return;
    }

    await interaction.update(buildRegulationPagesEditorPayload(state, nextIndex));
    return;
  }

  const embedTestEditMatch = customId.match(
    /^embedtest_edit_(header|content|content_extra|buttons|emojis)_(\d+)$/,
  );
  if (embedTestEditMatch) {
    const [, mode, messageId] = embedTestEditMatch;
    const state = embedTestStates.get(messageId);

    if (!state) {
      await interaction.reply({
        content: "> `❌` × Ta sesja edycji wygasła. Użyj `/embed-2` ponownie.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    if (state.ownerId !== interaction.user.id) {
      await interaction.reply({
        content: "> `❗` × Tylko autor testu może edytować ten embed.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    if (mode === "header") {
      await interaction.showModal(buildEmbedTestHeaderModal(state));
      return;
    }

    if (mode === "content") {
      if (isRegulationEmbedState(state)) {
        await interaction.reply({
          ...buildRegulationPagesEditorPayload(state, 0),
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      await interaction.showModal(buildEmbedTestContentModal(state));
      return;
    }

    if (mode === "content_extra") {
      if (isRegulationEmbedState(state)) {
        await interaction.reply({
          ...buildRegulationPagesEditorPayload(state, 0),
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      await interaction.showModal(buildEmbedTestExtraContentModal(state));
      return;
    }

    if (mode === "emojis") {
      await interaction.showModal(buildEmbedTestEmojisModal(state));
      return;
    }

    await interaction.showModal(buildEmbedTestButtonsModal(state));
    return;
  }

  // NEW: verification panel button
  if (customId.startsWith("verify_panel_")) {
    // very simple puzzles for preschool level: addition and multiplication with small numbers
    let expression;
    let answer;

    const operators = ["+", "*"];
    const op = operators[Math.floor(Math.random() * operators.length)];

    if (op === "+") {
      // addition: numbers 1-5
      const left = Math.floor(Math.random() * 5) + 1; // 1-5
      const right = Math.floor(Math.random() * 5) + 1; // 1-5
      expression = `${left} + ${right}`;
      answer = left + right;
    } else {
      // multiplication: small multiplier 1-3
      const left = Math.floor(Math.random() * 5) + 1; // 1-5
      const right = Math.floor(Math.random() * 3) + 1; // 1-3
      expression = `${left} * ${right}`;
      answer = left * right;
    }

    const modalId = `modal_verify_${interaction.guildId}_${interaction.user.id}_${Date.now()}`;

    // store answer for this modal
    const roleId = verificationRoles.get(interaction.guildId) || null;
    pendingVerifications.set(modalId, {
      answer,
      guildId: interaction.guildId,
      userId: interaction.user.id,
      roleId,
    });
    scheduleSavePersistentState();

    const modal = new ModalBuilder()
      .setCustomId(modalId)
      .setTitle("WERYFIKACJA");

    const answerInput = new TextInputBuilder()
      .setCustomId("verify_answer")
      .setLabel(`Ile to ${expression}?`)
      .setStyle(TextInputStyle.Short)
      .setPlaceholder("Wpisz wynik")
      .setRequired(true);

    modal.addComponents(new ActionRowBuilder().addComponents(answerInput));

    await interaction.showModal(modal);
    return;
  }

  // KALKULATOR: ile otrzymam?
  if (customId === "kalkulator_ile_otrzymam") {
    await interaction.showModal(buildKalkulatorModal("otrzymam", detectedServer));
    return;
  }

  // KALKULATOR: ile muszę dać?
  if (customId === "kalkulator_ile_musze_dac") {
    await interaction.showModal(buildKalkulatorModal("muszedac", detectedServer));
    return;
  }

  if (customId.startsWith("ticket_close_")) {
    const channel = interaction.channel;
    if (!isTicketChannel(channel)) {
      await interaction.reply({
        content: "> `❌` × Ta **komenda** działa jedynie na **ticketach**!",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    // Block if already being deleted
    if (closingTickets.has(channel.id)) {
      await interaction.reply({
        content: "> `⏳` × Ten ticket jest właśnie zamykany, poczekaj chwilę.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    if (!isAdminOrSeller(interaction.member)) {
      await interaction.reply({
        content: "> `❗` × Brak wymaganych uprawnień.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const chId = channel.id;
    const now = Date.now();
    const pending = pendingTicketClose.get(chId);

    // If there's a pending close and it's by same user and not expired -> proceed
    if (
      pending &&
      pending.userId === interaction.user.id &&
      now - pending.ts < 30_000
    ) {
      pendingTicketClose.delete(chId);
      closingTickets.add(chId);
      // remove ticketOwners entry immediately
      const ticketMeta = ticketOwners.get(chId) || null;
      await commitRewardTicketClaim(chId).catch(() => null);
      ticketOwners.delete(chId);
      scheduleSavePersistentState();

      await interaction.reply({
        embeds: [
          new EmbedBuilder()
            .setColor(COLOR_BLUE)
            .setDescription("> \`ℹ️\` × **Ticket zostanie zamknięty w ciągu 5 sekund...**")
        ]
      });

      // Archive & log immediately, then delete channel shortly after
      try {
        await archiveTicketOnClose(
          channel,
          interaction.user.id,
          ticketMeta,
          { closeMethod: "Przycisk zamknięcia" },
        ).catch((e) => console.error("archiveTicketOnClose error:", e));
      } catch (e) {
        console.error("Błąd archiwizacji ticketu (button):", e);
      }

      setTimeout(async () => {
        try {
          await channel.delete();
          console.log(`Zamknięto ticket ${channel.name}`);
        } catch (error) {
          console.error("Błąd zamykania ticketu:", error);
        } finally {
          closingTickets.delete(chId);
        }
      }, 2000);
    } else {
      // set pending note
      const expiresAt = Math.floor(now / 1000) + 30;
      pendingTicketClose.set(chId, { userId: interaction.user.id, ts: now });
      await interaction.reply({
        embeds: [buildTicketCloseConfirmEmbed("Kliknij przycisk jeszcze raz", expiresAt)],
        flags: [MessageFlags.Ephemeral],
      });
      // schedule expiry
      setTimeout(() => pendingTicketClose.delete(chId), 30_000);
    }
    return;
  }

  // Redeem code (ticket modal)
  if (customId.startsWith("ticket_code_")) {
    const parts = customId.split("_");
    const ticketChannelId = parts[2];
    const ticketUserId = parts[3];

    if (interaction.user.id !== ticketUserId) {
      await interaction.reply({
        content: "> `❗` × Brak wymaganych uprawnień.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const modal = new ModalBuilder()
      .setCustomId(`modal_redeem_code_${interaction.channel.id}`)
      .setTitle("Wpisz kod rabatowy");

    const codeInput = new TextInputBuilder()
      .setCustomId("discount_code")
      .setLabel("Wpisz kod który wygrałeś w Wylosuj nagrodę")
      .setStyle(TextInputStyle.Short)
      .setPlaceholder("np. ABC123XYZ0Q")
      .setRequired(true)
      .setMinLength(10)
      .setMaxLength(12);

    modal.addComponents(new ActionRowBuilder().addComponents(codeInput));
    await interaction.showModal(modal);
    return;
  }

  // Ticket settings button - ONLY admin/seller can use
  if (customId.startsWith("ticket_settings_")) {
    const channel = interaction.channel;
    if (!isTicketChannel(channel)) {
      await interaction.reply({
        content: "> `❌` × **Ta funkcja** działa jedynie na **ticketach**!",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    // Only administrator or seller can use settings
    if (!isAdminOrSeller(interaction.member)) {
      await interaction.reply({
        content: "> `❗` × Brak wymaganych uprawnień.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    // build embed (left stripe + header like screenshot)
    const settingsEmbed = new EmbedBuilder()
      .setColor(COLOR_BLUE)
      .setDescription("⚙️ × **Wybierz akcję z menu poniżej:**");

    // select menu with placeholder like the screenshot
    const select = new StringSelectMenuBuilder()
      .setCustomId(`ticket_settings_select_${channel.id}`)
      .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
      .addOptions([
        {
          label: "Dodaj osobę",
          value: "add",
          description: "Dodaj użytkownika do ticketu",
        },
        {
          label: "Zmień nazwę kanału",
          value: "rename",
          description: "Zmień nazwę tego ticketu",
        },
        {
          label: "Usuń osobę",
          value: "remove",
          description: "Usuń dostęp użytkownika z ticketu",
        },
      ]);

    const row = new ActionRowBuilder().addComponents(select);

    await interaction.reply({
      embeds: [settingsEmbed],
      components: [row],
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // Claiming a ticket via button - ONLY admin or seller
  // Ticket claim/unclaim -> wspólna logika (tak samo jak /przejmij i /odprzejmij)
  if (customId.startsWith("ticket_claim_")) {
    const channelId = customId.replace("ticket_claim_", "");
    await ticketClaimCommon(interaction, channelId);
    return;
  }
  if (customId.startsWith("ticket_unclaim_")) {
    const parts = customId.split("_");
    const channelId = parts[2];
    const expectedClaimer = parts[3] || "";

    const modalId = expectedClaimer ? `modal_odprzejmij_${expectedClaimer}` : "modal_odprzejmij";
    const modal = new ModalBuilder()
      .setCustomId(modalId)
      .setTitle("Zwalnianie ticketu");
    const powInput = new TextInputBuilder()
      .setCustomId("powod_odprzejmij")
      .setLabel("Dlaczego chcesz zwolnić ticket?")
      .setStyle(2)
      .setPlaceholder("Przykład: Brak odpowiedzi")
      .setRequired(false);
    modal.addComponents(new ActionRowBuilder().addComponents(powInput));
    await interaction.showModal(modal);
    return;
  }

  // Przycisk dodawania rozliczenia na kanale rozliczeń
  if (customId === "rozliczenie_dodaj_btn") {
    const isOwner = interaction.user.id === interaction.guild?.ownerId;
    const requiredRoleId = "1350786945944391733";
    const hasRole = interaction.member?.roles?.cache?.has(requiredRoleId);
    const SUSPENDED_ROLE_ID = "1537090439239442483";
    const isSuspended = interaction.member?.roles?.cache?.has(SUSPENDED_ROLE_ID);
    const isAdmin = interaction.member?.permissions?.has(PermissionFlagsBits.Administrator);

    if (!isAdmin && isSuspended) {
      await interaction.reply({
        content: "> `🔴` × Twoje konto sprzedawcy jest zawieszone i nie możesz dodawać rozliczeń.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    if (!isAdmin && !isOwner && !hasRole) {
      await interaction.reply({
        content: "> `❗` × Nie posiadasz roli sprzedawcy.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const modal = new ModalBuilder()
      .setCustomId("modal_rozliczenie_dodaj")
      .setTitle("Dodaj rozliczenie sprzedaży");

    const kwotaInput = new TextInputBuilder()
      .setCustomId("kwota_sprzedazy")
      .setLabel("Kwota sprzedaży w PLN")
      .setStyle(TextInputStyle.Short)
      .setPlaceholder("Przykład: 50")
      .setRequired(true)
      .setMinLength(1)
      .setMaxLength(10);

    modal.addComponents(new ActionRowBuilder().addComponents(kwotaInput));
    await interaction.showModal(modal);
    return;
  }

  // Seller choice for adding data
  if (customId === "seller_data_add_choice") {
    const container = new ContainerBuilder()
      .setAccentColor(COLOR_BLUE)
      .addTextDisplayComponents(
        new TextDisplayBuilder().setContent("> `📝` × Wybierz jakie dane chcesz skonfigurować:")
      )
      .addSeparatorComponents(new SeparatorBuilder().setDivider(true));

    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId("seller_data_edit_main")
        .setLabel("💳︲BLIK/PRZELEW")
        .setStyle(ButtonStyle.Secondary),
      new ButtonBuilder()
        .setCustomId("seller_data_edit_extra")
        .setLabel("🌐︲PAYPAL/LTC/MYPSC")
        .setStyle(ButtonStyle.Secondary)
    );

    container.addActionRowComponents(row);
    appendBrandFooterToContainer(container, interaction.guildId);

    await interaction.reply({
      components: [container],
      flags: MessageFlags.Ephemeral | MessageFlags.IsComponentsV2
    });
    return;
  }

  if (customId === "seller_data_view") {
    const profile = getSellerPaymentProfile(interaction.guildId, interaction.user.id);
    if (!sellerPaymentProfileHasData(profile)) {
      await interaction.reply({
        content: "> `❌` × Nie masz skonfigurowanych żadnych danych.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const container = new ContainerBuilder()
      .setAccentColor(COLOR_BLUE)
      .addTextDisplayComponents(
        new TextDisplayBuilder().setContent(
          "```\n" +
          "💳 New Shop × TWOJE DANE PŁATNOŚCI\n" +
          "```"
        )
      )
      .addSeparatorComponents(new SeparatorBuilder().setDivider(true))
      .addTextDisplayComponents(
        new TextDisplayBuilder().setContent(
          `> \`👤\` × **Odbiorca:** ${formatSellerPaymentValue(profile.recipient)}\n` +
          `> \`📱\` × **Nr. telefonu:** ${formatSellerPaymentValue(profile.phone)}\n` +
          `> \`🧾\` × **Tytuł przelewu:** ${formatSellerPaymentValue(profile.transferTitle)}\n` +
          `> \`✉️\` × **PayPal:** ${formatSellerPaymentValue(profile.paypalEmail)}\n` +
          `> \`👝\` × **Portfel LTC:** ${formatSellerPaymentValue(profile.ltcWallet)}\n` +
          `> \`🌐\` × **MyPSC:** ${formatSellerPaymentValue(profile.mypscEmail)}`
        )
      );

    appendBrandFooterToContainer(container, interaction.guildId);

    await interaction.reply({
      components: [container],
      flags: MessageFlags.Ephemeral | MessageFlags.IsComponentsV2
    });
    return;
  }

  // --- NEW: Seller Payment View in Ticket ---
  if (customId.startsWith("ticket_view_payment_")) {
    const sellerId = customId.replace("ticket_view_payment_", "");
    const profile = getSellerPaymentProfile(interaction.guildId, sellerId);

    if (!sellerPaymentProfileHasData(profile)) {
      await interaction.reply({
        content: "> `❌` × Sprzedawca nie skonfigurował jeszcze swoich danych.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const ticketId = interaction.channelId;
    const ticketData = ticketOwners.get(ticketId);
    const method = String(ticketData?.paymentMethod || "").toLowerCase();

    const lines = [];
    const addLine = (emoji, label, value) => {
      if (value && value !== "`Brak`" && value !== "Brak") {
        lines.push(`> ${emoji} × **${label}:** ${value}`);
      }
    };

    if (method.includes("blik") || method.includes("przelew") || !method) {
      addLine("👤", "Odbiorca", formatSellerPaymentValue(profile.recipient));
      addLine("📱", "Nr. telefonu", formatSellerPaymentValue(profile.phone));
      addLine("🧾", "Tytuł przelewu", formatSellerPaymentValue(profile.transferTitle));
    }
    if (method === "paypal" || !method) {
      addLine("✉️", "PayPal", formatSellerPaymentValue(profile.paypalEmail));
    }
    if (method === "ltc" || !method) {
      addLine("👝", "Portfel LTC", formatSellerPaymentValue(profile.ltcWallet));
    }
    if (method === "mypsc" || !method) {
      addLine("🌐", "MyPSC", formatSellerPaymentValue(profile.mypscEmail));
    }

    if (lines.length === 0) {
      addLine("👤", "Odbiorca", formatSellerPaymentValue(profile.recipient));
      addLine("📱", "Nr. telefonu", formatSellerPaymentValue(profile.phone));
      addLine("🧾", "Tytuł przelewu", formatSellerPaymentValue(profile.transferTitle));
      addLine("✉️", "PayPal", formatSellerPaymentValue(profile.paypalEmail));
      addLine("👝", "Portfel LTC", formatSellerPaymentValue(profile.ltcWallet));
      addLine("🌐", "MyPSC", formatSellerPaymentValue(profile.mypscEmail));
    }

    const container = new ContainerBuilder()
      .setAccentColor(COLOR_BLUE)
      .addTextDisplayComponents(
        new TextDisplayBuilder().setContent("```\n💳 New Shop × DANE DO PŁATNOŚCI\n```")
      )
      .addSeparatorComponents(new SeparatorBuilder().setDivider(true))
      .addTextDisplayComponents(
        new TextDisplayBuilder().setContent(lines.join("\n"))
      );

    if (interaction.user.id === sellerId || isAdminOrSeller(interaction.member)) {
      const row = new ActionRowBuilder().addComponents(
        new ButtonBuilder()
          .setCustomId("seller_data_edit_main")
          .setLabel("💳︲Zmień (BLIK)")
          .setStyle(ButtonStyle.Secondary),
        new ButtonBuilder()
          .setCustomId("seller_data_edit_extra")
          .setLabel("🌐︲Zmień (PP/LTC)")
          .setStyle(ButtonStyle.Secondary)
      );
      container.addActionRowComponents(row);
    }

    appendBrandFooterToContainer(container, interaction.guildId);

    await interaction.reply({
      components: [container],
      flags: MessageFlags.Ephemeral | MessageFlags.IsComponentsV2,
    });
    return;
  }
}

async function handleSlashCommand(interaction) {
  const { commandName } = interaction;

  // Rola administratora/helpera nie daje dostępu do prywatnych komend właściciela.
  if (!canUseSlashCommand(interaction)) {
    await interaction.reply({
      content: "> `❌` × Nie masz uprawnień do tej komendy.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  switch (commandName) {
    case "panel-wyslij":
      await handlePanelWyslijCommand(interaction);
      break;
    case "znizka": {
      if (!isTicketChannel(interaction.channel)) {
        await interaction.reply({
          content: "> `❌` × **Ta komenda** działa jedynie na **ticketach**!",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }
      const rawCode = interaction.options.getString("kod") || "";
      await processDiscountCodeRedemption(interaction, rawCode);
      break;
    }
    case "stworz-kod": {
      if (!isAdminOrSeller(interaction.member)) {
        await interaction.reply({
          content: "> `❗` × Brak wymaganych uprawnień.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      const znizka = interaction.options.getInteger("znizka");
      const customCodeRaw = interaction.options.getString("kod");
      const days = interaction.options.getInteger("dni") || 30;

      let code = customCodeRaw
        ? normalizeCodeInput(customCodeRaw)
        : generateCode();

      if (!code || code.length < 3) {
        await interaction.reply({
          content: "> `❌` × Kod jest za krótki. Podaj co najmniej 3 znaki.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      const expiresAt = Date.now() + (days * 86400 * 1000);

      const payload = {
        oderId: interaction.user.id,
        discount: znizka,
        expiresAt: expiresAt,
        created: Date.now(),
        type: "discount",
        used: false,
      };

      activeCodes.set(code, payload);
      await db.saveActiveCode(code, payload).catch(() => null);
      scheduleSavePersistentState(true);

      const expiryTimestamp = Math.floor(expiresAt / 1000);

      const dmPayload = buildCodeDeliveryDmPayload({
        code,
        rewardLine: `> \`💸\` × **Otrzymałeś:** \`-${znizka}%\``,
        expiryTimestamp,
        instructionText: PURCHASE_CODE_USAGE_TEXT,
        guildId: interaction.guild?.id || null,
      });

      let dmSent = true;
      try {
        await interaction.user.send(dmPayload);
      } catch (_err) {
        dmSent = false;
      }

      if (dmSent) {
        await interaction.reply({
          content: "> `✅` × **Kod wygenerowany i wysłany na Twoje PV!**",
          flags: [MessageFlags.Ephemeral],
        });
      } else {
        await interaction.reply({
          ...dmPayload,
          flags: [MessageFlags.Ephemeral],
        });
      }
      break;
    }
    case "ustawienia": {
      const channel = interaction.channel;
      if (!isTicketChannel(channel)) {
        await interaction.reply({
          content: "> `❌` × **Ta komenda** działa jedynie na **ticketach**!",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      if (!isAdminOrSeller(interaction.member)) {
        await interaction.reply({
          content: "> `❗` × Brak wymaganych uprawnień.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      const settingsEmbed = new EmbedBuilder()
        .setColor(COLOR_BLUE)
        .setDescription("⚙️ × **Wybierz akcję z menu poniżej:**");

      const select = new StringSelectMenuBuilder()
        .setCustomId(`ticket_settings_select_${channel.id}`)
        .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
        .addOptions([
          {
            label: "Dodaj osobę",
            value: "add",
            description: "Dodaj użytkownika do ticketu",
          },
          {
            label: "Usuń osobę",
            value: "remove",
            description: "Usuń użytkownika z ticketu",
          },
          {
            label: "Zmień nazwę",
            value: "rename",
            description: "Zmień nazwę kanału ticketu",
          },
        ]);

      const row = new ActionRowBuilder().addComponents(select);
      await interaction.reply({
        embeds: [settingsEmbed],
        components: [row],
        flags: [MessageFlags.Ephemeral],
      });
      break;
    }
    case "warn":
      await handleOstrzezenieCommand(interaction);
      break;
    case "ostrzezenie-usun":
    case "unwarn":
      await handleOstrzezenieUsunCommand(interaction);
      break;
    case "ostrzezenia":
    case "warns":
      await handleOstrzezeniaListCommand(interaction);
      break;
    case "drop":
      await interaction.reply({
        content:
          `> \`ℹ️\` × Ta komenda została wyłączona.\n` +
          `> \`🎁\` × Wejdź na kanał <#${FREE_KASA_CHANNEL_ID}> i kliknij przycisk \`Losuj nagrodę\`.`,
        flags: [MessageFlags.Ephemeral],
      });
      break;
    case "zaproszenia":
      await handleAdminZaproszeniaCommand(interaction);
      break;
    case "cennik":
      await handleCennikCommand(interaction);
      break;
    case "help":
      await handleHelpCommand(interaction);
      break;
    case "opiniekanal":
      await handleOpinieKanalCommand(interaction);
      break;
    case "ticket":
      await handleTicketCommand(interaction);
      break;
    case "ticket-zakoncz":
      await handleTicketZakonczCommand(interaction);
      break;
    case "anonim":
      await handleAnonimCommand(interaction);
      break;
    case "wydane":
      await handleWydaneCommand(interaction);
      break;
    case "topwydane":
    case "top-wydane":
      await handleTopWydaneCommand(interaction);
      break;
    case "wydaneshop":
    case "wydatkishop":
      await handleShopTotalsCommand(interaction);
      break;
    case "wydanestats":
      await handleWydaneStatsCommand(interaction);
      break;
    case "usunzakup":
      await handleUsunZakupCommand(interaction);
      break;
    case "znajdz-ticket":
      await handleZnajdzTicketCommand(interaction);
      break;
    case "zamknij-z-powodem":
      await handleZamknijZPowodemCommand(interaction);
      break;
    case "legit-check-ustaw":
      await handleLegitRepUstawCommand(interaction);
      break;
    case "wykres-legit-test":
    case "dzienne-lc-ustaw":
      await handleDailyLegitChartTestCommand(interaction);
      break;
    case "zamknij":
      await handleCloseTicketCommand(interaction);
      break;
    case "dodaj":
      await handleAddUserToTicketCommand(interaction);
      break;
    case "opinia":
      await handleOpinionCommand(interaction);
      break;
    case "wyczysc":
      await handleWyczyscKanalCommand(interaction);
      break;
    case "resetlc":
      await handleResetLCCommand(interaction);
      break;
    case "autolc":
      await handleAutoLcTestCommand(interaction);
      break;
    case "autolc-status":
      await handleAutoLcStatusCommand(interaction);
      break;
    case "autolc-timer":
      await handleAutoLcTimerCommand(interaction);
      break;
    case "zco":
      await handleZresetujCzasCommand(interaction);
      break;
    case "przejmij":
      await handleAdminPrzejmij(interaction);
      break;
    case "odprzejmij":
      await handleAdminOdprzejmij(interaction);
      break;
    case "autoprzejmij":
      await handleAutoPrzejmijCommand(interaction);
      break;
    case "zacznijliczycwlasicicielowi":
      await handleOwnerInviteCountingCommand(interaction);
      break;
    case "autoverify":
      await handleAutoVerifyCommand(interaction);
      break;
    case "embed":
      await handleSendMessageCommand(interaction);
      break;
    case "embed-2":
      await handleEmbedTestCommand(interaction);
      break;
    case "sprawdz-embed-2":
      await handleSprawdzEmbedTestCommand(interaction);
      break;
    case "ustaw-tickety-sprzedawcy":
      await handleUstawTicketySprzedawcyCommand(interaction);
      break;
    case "przelicz-tickety-sprzedawcy":
      await handlePrzeliczTicketySprzedawcyCommand(interaction);
      break;
    case "zaaktualizuj-film":
      await handleZaaktualizujFilmCommand(interaction);
      break;
    case "aktualizacja-embed":
      await handleAktualizacjaEmbedCommand(interaction);
      break;
    case "mody":
      await handleModyCommand(interaction);
      break;
    case "sprawdz-zaproszenia":
      await handleSprawdzZaproszeniaCommand(interaction);
      break;
    case "sprawdz-kogo-zaprosil":
      await handleSprawdzKogoZaprosilCommand(interaction);
      break;
    case "utworz-konkurs":
      await handleDodajKonkursCommand(interaction);
      break;
    case "rozliczenie":
      await handleRozliczenieCommand(interaction);
      break;
    case "rozliczeniazaplacil":
    case "rozliczeniezaplacil":
      await handleRozliczenieZaplacilCommand(interaction);
      break;

    case "rozliczenieprowizja":
    case "rozliczenie-prowizja":
      await handleRozliczenieProwizjaCommand(interaction);
      break;
    case "rozliczeniezakoncz":
      await handleRozliczenieZakonczCommand(interaction);
      break;
    case "test-pv":
      await handleTestPvCommand(interaction);
      break;
    case "test-legit-check-wzor":
      await handleTestLegitCheckWzorCommand(interaction);
      break;
    case "test-legit-check-sukces":
      await handleTestLegitCheckSukcesCommand(interaction);
      break;
    case "test-rozliczenia-ping":
      await handleTestRozliczeniaPingCommand(interaction);
      break;
    case "test-pingi":
      await handleTestPingiCommand(interaction);
      break;
    case "statusbota":
      await handleStatusBotaCommand(interaction);
      break;
    case "rozliczenieustaw":
      await handleRozliczenieUstawCommand(interaction);
      break;
    case "wezwij":
    case "wezwij-osobe":
      await handleWezwijCommand(interaction);
      break;
    case "zaproszenia-edytuj":
      await handleZaprosieniaStatsCommand(interaction);
      break;
    case "stworzkonkurs":
      await handleDodajKonkursCommand(interaction);
      break;
    case "end-giveaways":
      await handleEndGiveawaysCommand(interaction);
      break;
  }
}

// Handler dla komendy /rozliczenie
async function handleRozliczenieCommand(interaction) {
  // Sprawdź czy właściciel lub ma odpowiednią rolę
  const isOwner = interaction.user.id === interaction.guild.ownerId;
  const requiredRoleId = "1350786945944391733";
  const hasRole = interaction.member.roles.cache.has(requiredRoleId);

  if (!isOwner && !hasRole) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral]
    });
    return;
  }

  // Sprawdź czy komenda jest używana na właściwym kanale
  if (interaction.channelId !== ROZLICZENIA_CHANNEL_ID) {
    await interaction.reply({
      content: `❌ Ta komenda może być użyta tylko na kanale rozliczeń! <#${ROZLICZENIA_CHANNEL_ID}>`,
      flags: [MessageFlags.Ephemeral]
    });
    return;
  }

  const kwota = interaction.options.getInteger("kwota");
  const userId = interaction.user.id;

  if (!weeklySales.has(userId)) {
    weeklySales.set(userId, {
      amount: 0,
      lastUpdate: Date.now(),
      paid: false,
      paidAt: null,
      guildId: interaction.guild.id,
    });
  }

  const userData = weeklySales.get(userId);
  if (userData.paid) {
    userData.amount = 0;
    userData.paid = false;
    userData.paidAt = null;
  }
  userData.amount += kwota;
  userData.lastUpdate = Date.now();
  userData.guildId = interaction.guild.id;
  weeklySales.set(userId, userData);

  // Zapisz weekly sales do Supabase
  await db.saveWeeklySale(
    userId,
    userData.amount,
    interaction.guild.id,
    userData.paid || false,
    userData.paidAt || null,
    userData.lastUpdate,
  );
  scheduleSavePersistentState(true);
  console.log(`[rozliczenie] Użytkownik ${userId} dodał rozliczenie: ${kwota} zł, suma tygodniowa: ${userData.amount} zł`);

  const klientEmojiCmd = findGuildEmojiByName(interaction.guildId, "klient");
  const userEmojiStrCmd = klientEmojiCmd ? toGuildEmojiMarkup(klientEmojiCmd) : "👤";

  const embed = new EmbedBuilder()
    .setColor(COLOR_BLUE)
    .setTitle("\`💱\` Rozliczenie dodane")
    .setDescription(
      `> ${userEmojiStrCmd} **Użytkownik:** <@${userId}>\n` +
      `> \`✅\` × **Dodano sprzedaż:** ${kwota.toLocaleString("pl-PL")} zł\n` +
      `> \`📊\` × **Suma tygodniowa:** ${userData.amount.toLocaleString("pl-PL")} zł`,
    );

  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] }).catch(() => null);

  await interaction.channel.send({ embeds: [embed] }).catch(() => null);
  await interaction.deleteReply().catch(() => null);
  console.log(`Użytkownik ${userId} dodał rozliczenie: ${kwota} zł`);

  setTimeout(sendRozliczeniaMessage, 1000);
  setTimeout(() => sendRozliczeniaStatusReport(interaction.guild), 1000);
}

// Pomocnicze funkcje do cyklu rozliczeń
let rozliczeniaReportMessageId = null;
let isSundayResetTriggered = false;

async function sendRozliczeniaStatusReport(guild, forceNewMessage = false) {
  try {
    const logsChannel = await client.channels.fetch(ROZLICZENIA_LOGS_CHANNEL_ID).catch(() => null);
    if (!logsChannel) return;

    const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);

    const activeSalesCount = Array.from(weeklySales.values()).filter((data) => data.amount > 0).length;
    if (activeSalesCount === 0) {
      const timerLine = `> \`⏳\` × **Kolejne rozliczenia zaczynają się:** <t:${getNextSundayTimestamp()}:R>`;

      container.addTextDisplayComponents(new TextDisplayBuilder().setContent("# `📊` STATYSTYKI ROZLICZEŃ"));
      container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
      container.addTextDisplayComponents(new TextDisplayBuilder().setContent("> `📈` × **Bieżące podsumowanie sprzedaży w tym tygodniu:**"));
      container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
      container.addTextDisplayComponents(new TextDisplayBuilder().setContent("> `ℹ️` × Brak zarejestrowanych sprzedaży w tym tygodniu."));
      container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
      container.addTextDisplayComponents(new TextDisplayBuilder().setContent(timerLine));
      appendBrandFooterToContainer(container, guild?.id);

      if (!rozliczeniaReportMessageId) {
        const fetchedMsgs = await logsChannel.messages.fetch({ limit: 25 }).catch(() => null);
        if (fetchedMsgs && fetchedMsgs.size > 0) {
          const found = fetchedMsgs.find((m) =>
            m.author.id === client.user.id &&
            (m.content.includes("STATYSTYKI ROZLICZEŃ") ||
             m.content.includes("ROZLICZENIA TYGODNIOWE") ||
             JSON.stringify(m.toJSON()).includes("STATYSTYKI ROZLICZEŃ") ||
             JSON.stringify(m.toJSON()).includes("ROZLICZENIA TYGODNIOWE"))
          );
          if (found) rozliczeniaReportMessageId = found.id;
        }
      }

      let existingMsg = null;
      if (rozliczeniaReportMessageId) {
        existingMsg = await logsChannel.messages.fetch(rozliczeniaReportMessageId).catch(() => null);
      }

      if (existingMsg) {
        await existingMsg.edit({
          components: [container],
          flags: MessageFlags.IsComponentsV2,
        }).catch(() => null);
      } else {
        const sentMsg = await logsChannel.send({
          components: [container],
          flags: MessageFlags.IsComponentsV2,
        });
        if (sentMsg) rozliczeniaReportMessageId = sentMsg.id;
      }
      return;
    }

    // AWARYJNIE/PO RESECIE BOTA: Jeśli brak ID w pamięci, poszukaj wiadomości na kanale
    if (!rozliczeniaReportMessageId) {
      const fetchedMsgs = await logsChannel.messages.fetch({ limit: 25 }).catch(() => null);
      if (fetchedMsgs && fetchedMsgs.size > 0) {
        const found = fetchedMsgs.find((m) =>
          m.author.id === client.user.id &&
          (m.content.includes("STATYSTYKI ROZLICZEŃ") ||
           m.content.includes("ROZLICZENIA TYGODNIOWE") ||
           JSON.stringify(m.toJSON()).includes("STATYSTYKI ROZLICZEŃ") ||
           JSON.stringify(m.toJSON()).includes("ROZLICZENIA TYGODNIOWE"))
        );
        if (found) {
          rozliczeniaReportMessageId = found.id;
          console.log(`[rozliczenia] Odszukano istniejącą wiadomość raportu (ID: ${found.id}) na kanale rozliczeń.`);
        }
      }
    }

    const klientEmoji = findGuildEmojiByName(guild?.id, "klient");
    const userEmojiStr = klientEmoji ? toGuildEmojiMarkup(klientEmoji) : "👤";

    const { dayOfWeek, hour } = getPolandTime();

    const hasUnpaidSales = Array.from(weeklySales.values()).some((data) => data.amount > 0 && !data.paid);
    if (!hasUnpaidSales) {
      isSundayResetTriggered = false;
    }
    const isSundayMode = (dayOfWeek === 0 ? hasUnpaidSales : false) || (isSundayResetTriggered && hasUnpaidSales);

    const sortedSales = Array.from(weeklySales.entries())
      .filter(([_, data]) => data.amount > 0)
      .sort((a, b) => b[1].amount - a[1].amount);

    if (isSundayMode) {
      container.addTextDisplayComponents(new TextDisplayBuilder().setContent("# `📊` ROZLICZENIA TYGODNIOWE"));
      container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
      container.addTextDisplayComponents(new TextDisplayBuilder().setContent(
        "> `⏳` × **Termin na zapłacenie do godziny 20:00**\n" +
        "> `📱` × **Przelew na numer:** `880 260 392`"
      ));
      container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

      const salesListText = sortedSales.map(([userId, data]) => {
        const userRate = getUserCommissionRate(userId);
        const prowizja = (data.amount * userRate).toFixed(2);
        const percentStr = `${Math.round(userRate * 100)}%`;
        const statusEmoji = data.paid ? "`✅`" : "`❌`";
        return `> ${statusEmoji} ${userEmojiStr} <@${userId}>: **${data.amount.toLocaleString("pl-PL")} zł** (prowizja ${percentStr}: **${prowizja} zł**)`;
      }).join("\n");

      container.addTextDisplayComponents(new TextDisplayBuilder().setContent(salesListText));
    } else {
      const isSunday = dayOfWeek === 0;
      const isBefore20 = hour < 20;
      const timerLine = (isSunday && isBefore20 && hasUnpaidSales)
        ? `> \`⏳\` × **Rozliczenia są w toku! Czas na wpłatę:** <t:${getTodaySunday20Timestamp()}:R>`
        : `> \`⏳\` × **Kolejne rozliczenia zaczynają się:** <t:${getNextSundayTimestamp()}:R>`;

      container.addTextDisplayComponents(new TextDisplayBuilder().setContent("# `📊` STATYSTYKI ROZLICZEŃ"));
      container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
      container.addTextDisplayComponents(new TextDisplayBuilder().setContent("> `📈` × **Bieżące podsumowanie sprzedaży w tym tygodniu:**"));
      container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

      const salesListText = sortedSales.map(([userId, data]) => {
        const userRate = getUserCommissionRate(userId);
        const prowizja = (data.amount * userRate).toFixed(2);
        const percentStr = `${Math.round(userRate * 100)}%`;
        return `> ${userEmojiStr} <@${userId}>: **${data.amount.toLocaleString("pl-PL")} zł** (prowizja ${percentStr}: **${prowizja} zł**)`;
      }).join("\n");

      container.addTextDisplayComponents(new TextDisplayBuilder().setContent(salesListText));
      container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
      container.addTextDisplayComponents(new TextDisplayBuilder().setContent(timerLine));
    }

    appendBrandFooterToContainer(container, guild?.id);

    if (forceNewMessage && rozliczeniaReportMessageId) {
      await logsChannel.messages.delete(rozliczeniaReportMessageId).catch(() => null);
      rozliczeniaReportMessageId = null;
    }

    let existingMsg = null;
    if (rozliczeniaReportMessageId) {
      existingMsg = await logsChannel.messages.fetch(rozliczeniaReportMessageId).catch(() => null);
    }

    if (existingMsg) {
      await existingMsg.edit({
        components: [container],
        flags: MessageFlags.IsComponentsV2,
      }).catch(() => null);
    } else {
      const sentMsg = await logsChannel.send({
        components: [container],
        flags: MessageFlags.IsComponentsV2,
      });
      if (sentMsg) rozliczeniaReportMessageId = sentMsg.id;
    }
  } catch (err) {
    console.error("Błąd wysyłania/edycji raportu rozliczeń:", err);
  }
}

async function executeSundayReset00(guild) {
  if (!guild) return 0;
  isSundayResetTriggered = true;
  const LIMIT_ROLE_IDS = [
    "1541553994000891984", // NO LIMIT
    "1541553589833695393", // limit 400
    "1449448860517798061", // limit 200
    "1449448686156255333", // limit 100
    "1449448702925209651", // limit 50
    "1449448705563557918", // limit 20
  ];
  let count = 0;

  for (const [userId, data] of weeklySales) {
    if (data.amount > 0 && !data.paid) {
      const member = await guild.members.fetch(userId).catch(() => null);
      if (member) {
        const key = `${guild.id}:${userId}`;
        const userLimitRoles = LIMIT_ROLE_IDS.filter((roleId) => member.roles.cache.has(roleId));
        if (userLimitRoles.length > 0) {
          sellerSavedLimitRoles.set(key, userLimitRoles);
        }
        for (const roleId of LIMIT_ROLE_IDS) {
          if (member.roles.cache.has(roleId)) {
            await member.roles.remove(roleId, "Cotygodniowe rozliczenia 00:00 - zabranie limitów do czasu zapłaty").catch(() => null);
          }
        }
        count++;
      }
    }
  }
  scheduleSavePersistentState(true);
  await sendRozliczeniaStatusReport(guild, true);
  return count;
}

async function executeSundayDeadline20(guild) {
  if (!guild) return 0;
  const SUSPENDED_ROLE_ID = "1537090439239442483";
  let count = 0;

  for (const [userId, data] of weeklySales) {
    if (data.amount > 0 && !data.paid) {
      const member = await guild.members.fetch(userId).catch(() => null);
      if (member) {
        if (!member.roles.cache.has(SUSPENDED_ROLE_ID)) {
          await member.roles.add(SUSPENDED_ROLE_ID, "Przekroczenie terminu rozliczeń 20:00 - zawieszenie").catch(() => null);
        }
        count++;
      }
    }
  }
  scheduleSavePersistentState(true);
  await sendRozliczeniaStatusReport(guild, true);
  return count;
}

// Handler dla komendy /rozliczeniazaplacil oraz /rozliczeniezaplacil
async function handleRozliczenieZaplacilCommand(interaction) {
  const isOwner = interaction.user.id === interaction.guild?.ownerId;
  const isAdmin = interaction.member?.permissions?.has(PermissionFlagsBits.Administrator) ||
                  interaction.member?.permissions?.has(PermissionFlagsBits.ManageChannels);

  if (!isAdmin && !isOwner) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral]
    });
    return;
  }

  const targetUser = interaction.options.getUser("uzytkownik");
  const userId = targetUser.id;

  if (!weeklySales.has(userId)) {
    await interaction.reply({
      content: `> \`❌\` × Użytkownik <@${userId}> nie posiada rozliczeń w tym tygodniu!`,
      flags: [MessageFlags.Ephemeral]
    });
    return;
  }

  const userData = weeklySales.get(userId);
  const prowizja = (userData.amount * ROZLICZENIA_PROWIZJA).toFixed(2);

  userData.paid = true;
  userData.paidAt = Date.now();
  userData.lastUpdate = Date.now();
  userData.guildId = interaction.guild.id;
  weeklySales.set(userId, userData);

  await db.saveWeeklySale(
    userId,
    userData.amount,
    interaction.guild.id,
    true,
    userData.paidAt,
    userData.lastUpdate,
  );

  // Odzyskaj rangi limitów i usuń rolę zawieszony
  const key = `${interaction.guild.id}:${userId}`;
  const member = await interaction.guild.members.fetch(userId).catch(() => null);
  const SUSPENDED_ROLE_ID = "1537090439239442483";
  const DEFAULT_LIMIT_ROLE_ID = "1449448705563557918";

  if (member) {
    if (member.roles.cache.has(SUSPENDED_ROLE_ID)) {
      await member.roles.remove(SUSPENDED_ROLE_ID, "Rozliczenie zapłacone - usunięcie zawieszenia").catch(() => null);
    }
    const savedRoles = sellerSavedLimitRoles.get(key);
    if (Array.isArray(savedRoles) && savedRoles.length > 0) {
      for (const roleId of savedRoles) {
        if (!member.roles.cache.has(roleId)) {
          await member.roles.add(roleId, "Rozliczenie zapłacone - przywrócenie limity").catch(() => null);
        }
      }
      sellerSavedLimitRoles.delete(key);
    } else {
      if (!member.roles.cache.has(DEFAULT_LIMIT_ROLE_ID)) {
        await member.roles.add(DEFAULT_LIMIT_ROLE_ID, "Rozliczenie zapłacone - domyślny limit 20").catch(() => null);
      }
    }
  }

  // Gdy wszyscy użytkownicy opłacili rozliczenia, natychmiast resetuj statystyki na nowy tydzień
  const hasSales = Array.from(weeklySales.values()).some((data) => data.amount > 0);
  const allPaid = hasSales && Array.from(weeklySales.values()).every((data) => data.paid || data.amount === 0);
  if (allPaid) {
    console.log("[rozliczenie] Wszyscy użytkownicy zapłacili. Resetuję statystyki na nowy tydzień...");
    weeklySales.clear();
    isSundayResetTriggered = false;
    await db.resetWeeklySales().catch((e) => console.error("Błąd resetWeeklySales:", e));
  }

  scheduleSavePersistentState(true);

  const embed = new EmbedBuilder()
    .setColor(COLOR_BLUE)
    .setTitle("`✅` Rozliczenie oznaczone jako zapłacone")
    .setDescription(
      `> 👤 **Użytkownik:** <@${userId}>\n` +
      `> \`✅\` × **Status:** Rozliczenie zapłacone (zmieniono na \`✅\`)\n` +
      `> \`📊\` × **Suma sprzedaży:** ${userData.amount.toLocaleString("pl-PL")} zł\n` +
      `> \`💸\` × **Prowizja 10%:** ${prowizja} zł\n` +
      `> \`🔓\` × **Uprawnienia:** Przywrócono dostęp do limitów i zdjęto ewentualne zawieszenie.`
    );

  await interaction.reply({
    embeds: [embed],
    flags: [MessageFlags.Ephemeral],
  });
  console.log(`[rozliczenie] Admin ${interaction.user.id} oznaczył rozliczenie ${userId} jako zapłacone`);

  await sendRozliczeniaStatusReport(interaction.guild);
  setTimeout(sendRozliczeniaMessage, 1000);
}

// Handler dla komendy /rozliczenieprowizja
async function handleRozliczenieProwizjaCommand(interaction) {
  const isOwner = interaction.user.id === interaction.guild?.ownerId;
  const isAdmin = interaction.member?.permissions?.has(PermissionFlagsBits.Administrator) ||
                  interaction.member?.permissions?.has(PermissionFlagsBits.ManageChannels);

  if (!isAdmin && !isOwner) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral]
    });
    return;
  }

  const prowizjaVal = interaction.options.getInteger("prowizja");
  const targetUser = interaction.options.getUser("uzytkownik");
  const newRate = prowizjaVal / 100;

  if (targetUser) {
    sellerCommissionRates.set(targetUser.id, newRate);
    scheduleSavePersistentState(true);
    await interaction.reply({
      content: `> \`✅\` × Ustawiono indywidualną prowizję **${prowizjaVal}%** dla użytkownika <@${targetUser.id}>.`,
      flags: [MessageFlags.Ephemeral]
    });
  } else {
    globalCommissionRate = newRate;
    scheduleSavePersistentState(true);
    await interaction.reply({
      content: `> \`✅\` × Ustawiono domyślną prowizję globalną **${prowizjaVal}%** dla wszystkich.`,
      flags: [MessageFlags.Ephemeral]
    });
  }

  await sendRozliczeniaStatusReport(interaction.guild);
}

// Handler dla komendy /test-rozliczenia-ping
async function handleTestRozliczeniaPingCommand(interaction) {
  if (!isAdminOrSeller(interaction.member)) {
    await interaction.reply({
      content: "> `‼️` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const targetSeller = interaction.options.getUser("sprzedawca");
  if (!targetSeller) {
    await interaction.reply({
      content: "> `❌` × Wybierz sprzedawcę.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  if (targetSeller.id === "1305200545979437129") {
    await interaction.reply({
      content: "> `ℹ️` × Użytkownik <@1305200545979437129> to właściciel — jest wykluczony z powiadomień o rozliczeniach.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });

  try {
    const rozliczeniaChannel = await client.channels.fetch("1449162620807675935").catch(() => null);
    if (!rozliczeniaChannel || !rozliczeniaChannel.isTextBased()) {
      await interaction.editReply({
        content: "> `❌` × Nie udało się odnaleźć kanału rozliczeń (1449162620807675935).",
      });
      return;
    }

    const pingMsg = await rozliczeniaChannel.send({
      content: `<@${targetSeller.id}>`,
      allowedMentions: { users: [targetSeller.id] },
    }).catch(() => null);

    if (pingMsg && pingMsg.deletable) {
      setTimeout(() => {
        pingMsg.delete().catch(() => null);
      }, 5000);
    }

    await interaction.editReply({
      content: `> \`✅\` × **Wysłano testowy ping** dla <@${targetSeller.id}> na kanał <#1449162620807675935> (zostanie usunięty po 5s).`,
    });
  } catch (err) {
    console.error("Błąd w handleTestRozliczeniaPingCommand:", err);
    await interaction.editReply({
      content: "> `❌` × Błąd podczas testowania pinga.",
    });
  }
}

// Handler dla komendy /test-pingi
async function handleTestPingiCommand(interaction) {
  if (!isAdminOrSeller(interaction.member)) {
    await interaction.reply({
      content: "> `‼️` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });

  const targetUser = interaction.options.getUser("uzytkownik") || interaction.user;
  const shouldSend = interaction.options.getBoolean("wyslij") || false;
  const targetId = targetUser.id;

  const hasReactedCzyLegit = await hasUserReactedInCzyLegit(targetId, interaction.guild);
  const hasGivenOpinion = await hasUserPublishedOpinion(targetId, interaction.guild);

  const CZY_LEGIT_CH_ID = "1350446732365926494";
  const OPINIE_CH_ID = "1449783959306375198";
  const LEGIT_REP_CH_ID = "1449840030947217529";

  const descriptionLines = [
    `### 🧪 Test logiki oznaczeń dla <@${targetId}> (\`${targetUser.tag || targetUser.username}\`)\n`,
    `> **🧐 × czy-legit** (<#${CZY_LEGIT_CH_ID}>):`,
    hasReactedCzyLegit
      ? `> \`✅\` **Reakcja wykryta** ➔ Ping zostanie **POMINIĘTY** (brak oznaczenia)`
      : `> \`❌\` **Brak reakcji** ➔ Użytkownik **ZOSTANIE OZNACZONY**`,
    ``,
    `> **⭐ × opinie-klientow** (<#${OPINIE_CH_ID}>):`,
    hasGivenOpinion
      ? `> \`✅\` **Opinia wykryta** ➔ Ping zostanie **POMINIĘTY** (brak oznaczenia)`
      : `> \`❌\` **Brak opinii** ➔ Użytkownik **ZOSTANIE OZNACZONY**`,
    ``,
    `> **📅 × legit-checki** (<#${LEGIT_REP_CH_ID}>):`,
    `> \`📢\` **Kanał zakupowy** ➔ Zawsze oznaczany (do wystawienia +rep)`,
  ];

  let sendResult = "";
  if (shouldSend) {
    const channelsToPing = [LEGIT_REP_CH_ID];
    if (!hasReactedCzyLegit) channelsToPing.push(CZY_LEGIT_CH_ID);
    if (!hasGivenOpinion) channelsToPing.push(OPINIE_CH_ID);

    const sentChannels = [];
    for (const chId of channelsToPing) {
      const ch = await interaction.guild.channels.fetch(chId).catch(() => null);
      if (ch && ch.isTextBased()) {
        const pingMessage = await ch.send({
          content: `<@${targetId}>`,
          allowedMentions: { users: [targetId] },
        }).catch(() => null);

        if (pingMessage) {
          sentChannels.push(`<#${chId}>`);
          setTimeout(() => {
            pingMessage.delete().catch(() => null);
          }, LEGIT_REP_PING_DELETE_DELAY_MS);
        }
      }
    }
    sendResult = `\n\n> \`📨\` **Wysłano testowe pingi (usunięcie po 4s) na:** ${sentChannels.join(", ") || "brak"}`;
  }

  const embed = new EmbedBuilder()
    .setColor(COLOR_BLUE)
    .setTitle("Wynik weryfikacji oznaczeń (/ticket-zakoncz)")
    .setDescription(descriptionLines.join("\n") + sendResult)
    .setFooter({ text: "Test logiki /ticket-zakoncz • New Shop" })
    .setTimestamp();

  await interaction.editReply({ embeds: [embed] });
}

// Handler dla komendy /rozliczeniezakoncz
async function handleRozliczenieZakonczCommand(interaction) {
  // Sprawdź czy właściciel
  if (interaction.user.id !== interaction.guild.ownerId) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral]
    });
    return;
  }

  try {
    const logsChannel = await client.channels.fetch(ROZLICZENIA_LOGS_CHANNEL_ID);
    if (!logsChannel) {
      await interaction.reply({
        content: "> `❌` × **Nie znaleziono** kanału **rozliczeń**!",
        flags: [MessageFlags.Ephemeral]
      });
      return;
    }

    if (weeklySales.size === 0) {
      await interaction.reply({
        content: "> `❌` × **Brak** rozliczeń w tym **tygodniu**!",
        flags: [MessageFlags.Ephemeral]
      });
      return;
    }

    // Zbuduj raport jako embed
    let totalSales = 0;
    let reportLines = [];

    for (const [userId, data] of weeklySales) {
      const prowizja = data.amount * ROZLICZENIA_PROWIZJA;
      // Pobierz nazwę użytkownika zamiast pingować
      const user = client.users.cache.get(userId);
      const userName = user ? `<@${userId}>` : `<@${userId}>`;

      reportLines.push(`${userName} Do zapłaty ${prowizja.toFixed(2)}zł`);
      totalSales += data.amount;
    }

    const totalProwizja = (totalSales * ROZLICZENIA_PROWIZJA).toFixed(2);

    const reportEmbed = new EmbedBuilder()
      .setColor(COLOR_BLUE)
      .setTitle("\`📊\` ROZLICZENIA TYGODNIOWE")
      .setDescription(
        reportLines.join('\n') + '\n\n' +
        `> \`📱\` **Przelew na numer:** 880 260 392\n` +
        `> \`⏳\` **Termin płatności:** do 20:00 dnia dzisiejszego\n` +
        `> \`🚫\` **Od teraz do czasu zapłaty nie macie dostępu do ticketów**`
      )
      .setTimestamp()
      .setFooter(getBrandFooterBuilderObject());

    const sentMessage = await logsChannel.send({ embeds: [reportEmbed] });

    // Wyślij osobną wiadomość z pingami osób do zapłaty
    if (weeklySales.size > 0) {
      const pings = [];
      for (const [userId, data] of weeklySales) {
        pings.push(`<@${userId}>`);
      }

      const pingMessage = await logsChannel.send({
        content: `**Osoby do zapłaty prowizji:** ${pings.join(' ')}`
      });

      // Usuń wiadomość z pingami po 5 sekundach
      setTimeout(() => {
        pingMessage.delete().catch(err => console.log('Nie udało się usunąć wiadomości z pingami:', err));
      }, 5000);
    }

    // Zapisz dane przed resetem dla embeda
    const liczbaOsob = weeklySales.size;
    const totalSalesValue = totalSales;
    const totalProwizjaValue = totalProwizja;

    // Resetuj dane po wysłaniu raportu - TYLKO rozliczenia, NIE zaproszenia!
    weeklySales.clear();
    console.log("Ręcznie zresetowano rozliczenia po /rozliczeniezakoncz");

    // Resetuj też w Supabase dla aktualnego tygodnia
    try {
      const resetOk = await db.resetWeeklySales();
      if (!resetOk) {
        console.error("[Supabase] Nie udało się zresetować weekly_sales dla aktualnego tygodnia");
      } else {
        console.log("[Supabase] Zresetowano weekly_sales po /rozliczeniezakoncz");
      }
    } catch (err) {
      console.error("Błąd podczas resetowania rozliczeń w Supabase:", err);
    }
    scheduleSavePersistentState(true);

    // UWAGA: NIE resetujemy zaproszeń - są one przechowywane w Supabase osobno!
    console.log("🔒 ZAPROSZENIA ZACHOWANE - nie resetowane!");

    const embed = new EmbedBuilder()
      .setColor(COLOR_BLUE)
      .setTitle("✅ Podsumowanie wysłane i zresetowano")
      .setDescription(
        `> \`✅\` × **Wysłano podsumowanie** na kanał <#${ROZLICZENIA_LOGS_CHANNEL_ID}>\n` +
        `> \`🔄\` × **Zresetowano statystyki** na nowy tydzień\n` +
        `> \`📊\` × **Liczba osób:** ${liczbaOsob}\n` +
        `> \`💰\` × **Łączna sprzedaż:** ${totalSalesValue.toLocaleString("pl-PL")} zł\n` +
        `> \`💸\` × **Łączna prowizja:** ${parseFloat(totalProwizjaValue).toFixed(2)} zł`
      )
      .setTimestamp();

    // Odśwież raport na kanale rozliczeń oraz panel z przyciskiem
    await sendRozliczeniaStatusReport(interaction.guild);
    setTimeout(sendRozliczeniaMessage, 1000);

    await interaction.reply({ embeds: [embed], flags: [MessageFlags.Ephemeral] });
    console.log(`Właściciel ${interaction.user.id} wygenerował podsumowanie rozliczeń`);
  } catch (err) {
    console.error("Błąd generowania podsumowania:", err);
    await interaction.reply({
      content: "> `❌` × **Wystąpił** błąd podczas generowania **podsumowania**!",
      flags: [MessageFlags.Ephemeral]
    });
  }
}

// Handler dla komendy /statusbota
async function handleStatusBotaCommand(interaction) {
  // Sprawdź czy właściciel
  if (interaction.user.id !== interaction.guild.ownerId) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral]
    });
    return;
  }

  try {
    const status = await checkBotStatus();

    const embed = new EmbedBuilder()
      .setColor(status.statusColor)
      .setTitle("📊 Status Bota")
      .setDescription(`**Status:** ${status.status}`)
      .addFields(
        { name: "⏱ Uptime", value: status.uptime, inline: true },
        { name: "📡 Ping", value: `${status.ping}ms (avg: ${status.avgPing}ms)`, inline: true },
        { name: "🔢 Błędy", value: status.errorCount.toString(), inline: true },
        { name: "🌐 Serwery", value: status.guilds.toString(), inline: true },
        { name: "👥 Użytkownicy", value: status.users.toString(), inline: true },
        { name: "💬 Kanały", value: status.channels.toString(), inline: true }
      )
      .setTimestamp()
      .setFooter(getBrandFooterBuilderObject());

    await interaction.reply({ embeds: [embed] });
  } catch (err) {
    console.error("Błąd komendy /statusbota:", err);
    await interaction.reply({
      content: "> `❌` × **Wystąpił** błąd podczas pobierania statusu **bota**!",
      flags: [MessageFlags.Ephemeral]
    });
  }
}

// Handler dla komendy /rozliczenieustaw
async function handleRozliczenieUstawCommand(interaction) {
  // Sprawdź czy właściciel
  if (interaction.user.id !== interaction.guild.ownerId) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral]
    });
    return;
  }

  const targetUser = interaction.options.getUser("uzytkownik");
  const akcja = interaction.options.getString("akcja");
  const kwota = interaction.options.getInteger("kwota");
  const userId = targetUser.id;

  // Inicjalizuj użytkownika jeśli nie istnieje
  if (!weeklySales.has(userId)) {
    weeklySales.set(userId, {
      amount: 0,
      lastUpdate: Date.now(),
      paid: false,
      paidAt: null,
      guildId: interaction.guild.id,
    });
  }

  const userData = weeklySales.get(userId);

  if (akcja === "dodaj") {
    userData.amount += kwota;
  } else if (akcja === "odejmij") {
    userData.amount = Math.max(0, userData.amount - kwota);
  } else if (akcja === "ustaw") {
    userData.amount = kwota;
  }

  if (userData.amount <= 0) {
    userData.amount = 0;
    weeklySales.delete(userId);
    await db.saveWeeklySale(
      userId,
      0,
      interaction.guild.id,
      false,
      null,
      Date.now()
    ).catch(() => null);
  } else {
    userData.lastUpdate = Date.now();
    userData.guildId = interaction.guild.id;
    weeklySales.set(userId, userData);

    await db.saveWeeklySale(
      userId,
      userData.amount,
      interaction.guild.id,
      userData.paid || false,
      userData.paidAt || null,
      userData.lastUpdate,
    ).catch(() => null);
  }

  // Zapisz stan po zmianie rozliczenia
  scheduleSavePersistentState(true);

  const userRate = getUserCommissionRate(userId);
  const prowizja = userData.amount * userRate;
  const percentStr = `${Math.round(userRate * 100)}%`;
  const zmiana = kwota;
  const znakZmiany = akcja === "dodaj" ? "+" : akcja === "odejmij" ? "-" : "";

  const embed = new EmbedBuilder()
    .setColor(COLOR_GREEN)
    .setTitle("`✅` Rozliczenie zaktualizowane")
    .setDescription(
      `> 👤 **Użytkownik:** <@${userId}>\n` +
      `> \`🔄\` × **Akcja:** ${akcja.charAt(0).toUpperCase() + akcja.slice(1)}\n` +
      `> \`💰\` × **Zmiana:** ${znakZmiany}${zmiana.toLocaleString("pl-PL")} zł\n` +
      `> \`📊\` × **Nowa suma:** ${userData.amount.toLocaleString("pl-PL")} zł\n` +
      `> \`💸\` × **Prowizja (${percentStr}):** ${prowizja.toFixed(2)} zł`
    );

  await interaction.reply({ embeds: [embed], flags: [MessageFlags.Ephemeral] });
  await sendRozliczeniaStatusReport(interaction.guild);
  setTimeout(sendRozliczeniaMessage, 1000);
  console.log(`Właściciel zaktualizował rozliczenie dla ${userId}: ${akcja} ${kwota} zł`);
}

async function handleAdminPrzejmij(interaction) {
  // Sprawdź uprawnienia przed sprawdzaniem kanału
  if (!isAdminOrSeller(interaction.member)) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const channel = interaction.channel;
  if (!isTicketChannel(channel)) {
    await interaction.reply({
      content: "> `❌` × **Użyj** komendy w kanale **ticketu**.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }
  await ticketClaimCommon(interaction, channel.id); // quiz odpali się w środku
}

function getPurchaseTicketCategoryIdsForGuild(guild) {
  const guildCats = ticketCategories.get(guild.id) || {};
  const purchaseCategoryIds = new Set();

  for (const [key, value] of Object.entries(guildCats)) {
    if (key.startsWith("zakup-") && value) {
      purchaseCategoryIds.add(String(value));
    }
  }

  if (purchaseCategoryIds.size === 0) {
    for (const ch of guild.channels.cache.values()) {
      if (
        ch.type === ChannelType.GuildCategory &&
        ch.name &&
        ch.name.toLowerCase().includes("zakup")
      ) {
        purchaseCategoryIds.add(String(ch.id));
      }
    }
  }

  return purchaseCategoryIds;
}

function isPurchaseTicketLabel(label = "") {
  const normalized = String(label || "").toUpperCase();
  return normalized === "ZAKUP";
}

function isOwnerInviteCountingEnabled(guildId) {
  return ownerInviteCountingSettings.get(String(guildId)) === true;
}

function isOwnerOnlyPurchaseTicket(channel, ticketMeta = null) {
  if (ticketMeta?.ownerOnlyPurchase) return true;

  const label = String(ticketMeta?.ticketTypeLabel || "").toUpperCase();
  if (
    ["ZAKUP BOTÓW", "ZAKUP BOTA", "ZAKUP AUTORYNKU", "ZAKUP AUTO RYNKU", "ZAKUP MODÓW", "ZAKUP MODA"].includes(label)
  ) {
    return true;
  }

  const topic = String(channel?.topic || "").toLowerCase();
  if (topic.includes("zakup botów") || topic.includes("zakup bota") || topic.includes("zakup autorynku") || topic.includes("zakup moda")) {
    return true;
  }

  const normalizedName = String(channel?.name || "").toLowerCase();
  return /-(boty|bot|autorynek|mod|mody)$/.test(normalizedName);
}

function getPurchaseStaffRoleIdsForCategory(categoryId, guild) {
  const key = guild && ticketCategoryManager.keyFor(guild, categoryId);
  const thresholds = ["zakup-0-20", "zakup-20-50", "zakup-50-100", "zakup-100-200", "zakup-200-400", "zakup-400-999"];
  if (thresholds.includes(key)) return PURCHASE_STAFF_ROLE_IDS.slice(thresholds.indexOf(key));
  const normalized = String(categoryId || "");
  switch (normalized) {
    case "1449526840942268526":
      return [...PURCHASE_STAFF_ROLE_IDS];
    case "1449526958508474409":
      return PURCHASE_STAFF_ROLE_IDS.slice(1);
    case "1449451716129984595":
      return PURCHASE_STAFF_ROLE_IDS.slice(2);
    case "1449452354201190485":
      return PURCHASE_STAFF_ROLE_IDS.slice(3);
    default:
      return [...PURCHASE_STAFF_ROLE_IDS];
  }
}

async function syncPurchaseTicketSellerVisibility(
  guild,
  channel,
  ownerId,
  hideStaff,
) {
  if (!guild || !channel || channel.type !== ChannelType.GuildText) return false;

  const allowedRoleIds = getPurchaseStaffRoleIdsForCategory(channel.parentId, guild);
  const hiddenRoleIds = Array.from(
    new Set([BASE_SELLER_ROLE_ID, ...PURCHASE_STAFF_ROLE_IDS]),
  );
  if (!hiddenRoleIds.length) return false;

  if (ownerId) {
    await channel.permissionOverwrites.edit(ownerId, {
      ViewChannel: true,
      SendMessages: true,
      ReadMessageHistory: true,
    }).catch(() => null);
  }

  for (const roleId of hiddenRoleIds) {
    if (hideStaff) {
      await channel.permissionOverwrites.edit(roleId, {
        ViewChannel: false,
        SendMessages: false,
        ReadMessageHistory: false,
      }).catch(() => null);
      continue;
    }

    if (allowedRoleIds.includes(roleId)) {
      await channel.permissionOverwrites.edit(roleId, {
        ViewChannel: true,
        SendMessages: true,
        ReadMessageHistory: true,
      }).catch(() => null);
      continue;
    }

    await channel.permissionOverwrites.delete(roleId).catch(() => null);
  }

  return true;
}

async function runAutoPrzejmijSweep(guild, ownerId, ownerName, targetChannelId = null) {
  const purchaseCategoryIds = getPurchaseTicketCategoryIdsForGuild(guild);
  const hideStaff = Boolean(autoPrzejmijSettings.get(guild.id)?.enabled);

  const stats = {
    changedCount: 0,
    skippedNonPurchase: 0,
    staleRemoved: 0,
    errorCount: 0,
    changedChannels: [],
    missingPurchaseCategories: purchaseCategoryIds.size === 0,
    mode: hideStaff ? "ukryte" : "przywrocone",
  };

  if (stats.missingPurchaseCategories) return stats;

  for (const [channelId] of ticketOwners.entries()) {
    if (targetChannelId && channelId !== targetChannelId) continue;

    let channel = guild.channels.cache.get(channelId) || null;
    if (!channel) channel = await client.channels.fetch(channelId).catch(() => null);

    if (!channel) {
      ticketOwners.delete(channelId);
      stats.staleRemoved += 1;
      continue;
    }

    if (
      !channel.guild ||
      channel.guild.id !== guild.id ||
      channel.type !== ChannelType.GuildText
    ) {
      continue;
    }

    const parentId = channel.parentId ? String(channel.parentId) : "";
    const ticketMeta = ticketOwners.get(channel.id) || null;
    const ticketLabel = guessTicketTypeLabel(channel, ticketMeta);
    if (isOwnerOnlyPurchaseTicket(channel, ticketMeta)) {
      stats.skippedNonPurchase += 1;
      continue;
    }
    if (!purchaseCategoryIds.has(parentId) || !isPurchaseTicketLabel(ticketLabel)) {
      stats.skippedNonPurchase += 1;
      continue;
    }

    const synced = await syncPurchaseTicketSellerVisibility(
      guild,
      channel,
      ownerId,
      hideStaff,
    ).catch(() => false);

    if (synced) {
      stats.changedCount += 1;
      stats.changedChannels.push(`<#${channel.id}>`);
    } else {
      stats.errorCount += 1;
    }
  }

  if (stats.staleRemoved > 0) scheduleSavePersistentState();
  return stats;
}

function formatAutoPrzejmijSummary(stats, statusLine) {
  const lines = [];
  if (statusLine) lines.push(statusLine);

  if (stats.missingPurchaseCategories) {
    lines.push("> `❌` × Nie znalazlem kategorii ticketow zakupowych.");
    return lines.join("\n");
  }

  lines.push(`> \`✅\` × Tickety zakupowe ${stats.mode}: **${stats.changedCount}**.`);
  lines.push(`> \`⏭️\` × Pominiete nie-zakupowe: **${stats.skippedNonPurchase}**.`);

  if (stats.staleRemoved > 0) {
    lines.push(`> \`🧹\` × Usuniete nieaktualne wpisy: **${stats.staleRemoved}**.`);
  }
  if (stats.errorCount > 0) {
    lines.push(`> \`⚠️\` × Bledy podczas zmiany widocznosci: **${stats.errorCount}**.`);
  }
  if (stats.changedChannels.length > 0) {
    const preview = stats.changedChannels.slice(0, 10).join(", ");
    const more =
      stats.changedChannels.length > 10
        ? ` (+${stats.changedChannels.length - 10} wiecej)`
        : "";
    lines.push(`> \`📌\` × Zmienione kanaly: ${preview}${more}`);
  }
  return lines.join("\n");
}

async function maybeAutoPrzejmijNewTicket(guild, channelId) {
  const cfg = autoPrzejmijSettings.get(guild.id);
  if (!cfg || !cfg.enabled) return;

  if (cfg.ownerId !== guild.ownerId) {
    autoPrzejmijSettings.delete(guild.id);
    scheduleSavePersistentState();
    return;
  }

  const ownerMember = await guild.members.fetch(cfg.ownerId).catch(() => null);
  const ownerName = ownerMember?.displayName || cfg.ownerName || "Wlasciciel";

  if (cfg.ownerName !== ownerName) {
    cfg.ownerName = ownerName;
    autoPrzejmijSettings.set(guild.id, cfg);
    scheduleSavePersistentState();
  }

  await runAutoPrzejmijSweep(guild, cfg.ownerId, ownerName, channelId).catch(
    (err) => console.error("[autoprzejmij] Zmiana widocznosci nowego ticketa nieudana:", err),
  );
}

async function handleAutoPrzejmijCommand(interaction) {
  const guild = interaction.guild;
  if (!guild) {
    await interaction.reply({
      content: "> `❌` × Ta komenda dziala tylko na serwerze.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  if (interaction.user.id !== guild.ownerId) {
    await interaction.reply({
      content: "> `❌` × Tej komendy moze uzyc tylko wlasciciel serwera.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const modeSel = interaction.options.getString("status", true);
  const guildId = guild.id;

  if (modeSel === "wylacz") {
    const ownerName =
      interaction.member?.displayName ||
      interaction.user.globalName ||
      interaction.user.username;
    autoPrzejmijSettings.delete(guildId);
    scheduleSavePersistentState();
    const stats = await runAutoPrzejmijSweep(
      guild,
      interaction.user.id,
      ownerName,
      null,
    );
    await interaction.reply({
      content: formatAutoPrzejmijSummary(
        stats,
        "> `✅` × Przywróciłem normalną widoczność ticketów zakupowych dla sprzedawców.",
      ),
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const pick = generateClaimQuiz();
  const modalId = `autoprzejmij_quiz_${guildId}_${interaction.user.id}_${Date.now()}`;

  pendingAutoPrzejmijQuiz.set(modalId, {
    guildId,
    userId: interaction.user.id,
    ownerId: interaction.user.id,
    ownerName:
      interaction.member?.displayName ||
      interaction.user.globalName ||
      interaction.user.username,
    answer: pick.a,
  });

  const modal = new ModalBuilder()
    .setCustomId(modalId)
    .setTitle("Weryfikacja trybu ticketów");
  const input = new TextInputBuilder()
    .setCustomId("autoprzejmij_answer")
    .setLabel(pick.q)
    .setStyle(TextInputStyle.Short)
    .setRequired(true)
    .setMaxLength(5);
  modal.addComponents(new ActionRowBuilder().addComponents(input));

  await interaction.showModal(modal).catch(async () => {
    pendingAutoPrzejmijQuiz.delete(modalId);
    await interaction.reply({
      content: "> `❌` × Nie udalo sie otworzyc captcha. Sprobuj ponownie.",
      flags: [MessageFlags.Ephemeral],
    }).catch(() => null);
  });
}

async function handlePanelKalkulatorCommand(interaction) {
  if (!interaction.guild) {
    await interaction.reply({
      content: "> `❌` × **Ta komenda** działa tylko na **serwerze**.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  if (interaction.user.id !== interaction.guild.ownerId) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);
  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      "```\n" +
      "🧮 New Shop × Kalkulator\n" +
      "```"
    )
  );
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      "> <a:arrowwhite:1491476759290449984> × **Oblicz w szybki i prosty sposób ile otrzymasz lub ile musisz dać aby dostać określoną ilość __waluty__**"
    )
  );

  const typeSelect = new StringSelectMenuBuilder()
    .setCustomId("kalkulator_typ")
    .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
    .setMinValues(1)
    .setMaxValues(1)
    .addOptions(KALKULATOR_MODE_OPTIONS);

  container.addActionRowComponents(new ActionRowBuilder().addComponents(typeSelect));
  appendBrandFooterToContainer(container, interaction.guildId);

  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });
  await interaction.channel.send({
    components: [container],
    flags: MessageFlags.IsComponentsV2
  });
  await interaction.editReply({
    content: "> `✅` × **Panel** kalkulatora został wysłany na ten **kanał**.",
  });
}

function sanitizeOptionsEmojis(options) {
  if (!options) return [];
  return options.map((opt) => {
    let emoji = opt.emoji;
    if (emoji && emoji.id) {
      const hasEmoji = client.emojis.cache.has(emoji.id);
      if (!hasEmoji) {
        emoji = undefined;
      }
    }
    return {
      ...opt,
      emoji,
    };
  });
}

function buildKalkulatorModal(typ, detectedServer = null) {
  const isOtrzymam = typ === "otrzymam";
  const timestamp = Date.now();
  const modal = new ModalBuilder()
    .setCustomId(isOtrzymam ? `modal_ile_otrzymam_${timestamp}` : `modal_ile_musze_dac_${timestamp}`)
    .setTitle("Kalkulator");

  const valueInput = new TextInputBuilder()
    .setCustomId(isOtrzymam ? `kwota_${timestamp}` : `waluta_${timestamp}`)
    .setPlaceholder(isOtrzymam ? "Przykład: 50zł" : "Przykład: 125k")
    .setStyle(TextInputStyle.Short)
    .setRequired(true);

  const serverSelect = new StringSelectMenuBuilder()
    .setCustomId(`kalkulator_server_${timestamp}`)
    .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
    .setRequired(true)
    .setMinValues(1)
    .setMaxValues(1)
    .addOptions(
      sanitizeOptionsEmojis(
        KALKULATOR_SERVER_OPTIONS.map((opt) => ({
          ...opt,
          default: !!(detectedServer && opt.value === detectedServer.calcValue),
        }))
      )
    );

  const paymentSelect = new StringSelectMenuBuilder()
    .setCustomId(`kalkulator_payment_${timestamp}`)
    .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
    .setRequired(true)
    .setMinValues(1)
    .setMaxValues(1)
    .addOptions(sanitizeOptionsEmojis(KALKULATOR_PAYMENT_OPTIONS));

  modal.addLabelComponents(
    new LabelBuilder()
      .setLabel("Na jakim serwerze?")
      .setStringSelectMenuComponent(serverSelect),
    new LabelBuilder()
      .setLabel("Forma płatności")
      .setStringSelectMenuComponent(paymentSelect),
    new LabelBuilder()
      .setLabel(isOtrzymam ? "Kwota (PLN)" : "Ilość waluty")
      .setTextInputComponent(valueInput),
  );

  return modal;
}

function getKalkulatorRateDescription(rate, serverRaw) {
  const server = (serverRaw || "").toString().trim().toUpperCase().replace(/\s+/g, "_");
  const isSky = (server === "MINESTAR_SKY" || server === "MINESTAR_SKYPVP" || server === "MINESTAR_SKY_PVP");
  let rateStr;
  if (rate >= 1000000000) {
    const val = rate / 1000000000;
    rateStr = val.toFixed(1).replace(".", ",").replace(",0", "") + "mld";
  } else if (rate >= 1000000) {
    const val = rate / 1000000;
    rateStr = val.toFixed(1).replace(".", ",").replace(",0", "") + "M";
  } else if (rate >= 1000) {
    const val = rate / 1000;
    rateStr = val.toFixed(1).replace(".", ",").replace(",0", "") + "k";
  } else rateStr = formatRateShort(rate);

  const unitStr = isSky ? " odłamki" : "";
  let desc = `Obliczone z cennika **${rateStr}${unitStr}** za **1zł**`;
  if (server === "ANARCHIA_LIFESTEAL" && rate === 8500) {
    desc += " (zakup powyżej **50**zł)";
  } else if (server === "MINESTAR_LF" && rate === 400) {
    desc += " (zakup powyżej **50**zł)";
  }
  return desc;
}

function buildKalkulatorResultMessage({ typ, kwota, waluta, tryb, metoda }) {
  if (!tryb || !metoda) {
    return {
      error: "> `❌` × **Proszę** wybrać zarówno serwer jak i metodę **płatności**.",
    };
  }

  const minPurchase = getMinPurchasePln(metoda);
  const serverUpper = (tryb || "").toString().toUpperCase();
  const isSky = serverUpper.includes("SKY") || serverUpper.includes("MINESTAR_SKY");
  const currencyUnit = isSky ? " odłamków" : " $";

  if (typ === "otrzymam") {
    if (kwota < minPurchase) {
      return {
        error: `> \`❌\` × **Minimalne zakupy** dla ${metoda} to **${minPurchase}zł**.`,
      };
    }

    const { fee, feeLabel } = calculateFeePln(kwota, metoda);
    const effectivePln = kwota - fee;
    const rate = getRateForPlnAmount(kwota, tryb);
    const calculatedWaluta = round2(effectivePln * rate);
    const kwotaZl = Math.trunc(Number(kwota) || 0);
    const walutaShort = formatShortWaluta(calculatedWaluta);

    return {
      message: `> \`🔢\` × **Płacąc nam ${kwotaZl}zł (${metoda} prowizja: ${feeLabel}) otrzymasz:** \`${walutaShort}\` **(${formatRateShort(calculatedWaluta)}${currencyUnit})**\n> \`💵\` × ${getKalkulatorRateDescription(rate, tryb)}`,
    };
  }

  const server = (tryb || "").toString().toUpperCase();
  let rate;
  if (server === "MINESTAR_SKY" || server === "MINESTAR_SKYPVP") {
    rate = MINESTAR_SKY_RATE;
  } else if (server === "ANARCHIA_BOXPVP") {
    rate = ANARCHIA_BOXPVP_RATE;
  } else if (server === "ANARCHIA_LIFESTEAL") {
    rate = getAnarchiaLifestealRateForWaluta(waluta, metoda);
  } else if (server === "MINESTAR_LF") {
    rate = getMinestarLfRateForWaluta(waluta, metoda);
  } else if (server === "DONUT_SMP") {
    rate = DONUT_SMP_RATE;
  } else {
    rate = ANARCHIA_LIFESTEAL_RATE;
  }

  const baseRaw = waluta / rate;
  const basePln = round2(baseRaw);
  const { fee, feeLabel } = calculateFeePln(basePln, metoda);
  const totalPln = round2(basePln + fee);
  const totalZl = Math.trunc(Number(totalPln) || 0);

  if (totalZl < minPurchase) {
    return {
      error: `> \`❌\` × **Minimalne zakupy** dla ${metoda} to **${minPurchase}zł**.`,
    };
  }

  const walutaInt = Math.floor(Number(waluta) || 0);
  const walutaShort = formatShortWaluta(walutaInt);

  return {
    message: `> \`🔢\` × **Aby otrzymać:** \`${walutaShort}\` **(${walutaInt}${currencyUnit})** **musisz zapłacić ${totalZl}zł (${metoda} prowizja: ${feeLabel})**\n> \`💵\` × ${getKalkulatorRateDescription(rate, tryb)}`,
  };
}

async function handleAdminOdprzejmij(interaction) {
  if (!isAdminOrSeller(interaction.member)) {
    await interaction.reply({ content: "> `❗` × Brak wymaganych uprawnień.", flags: [MessageFlags.Ephemeral] });
    return;
  }
  if (!isTicketChannel(interaction.channel)) {
    await interaction.reply({ content: "> `❌` × **Użyj** komendy w kanale **ticketu**.", flags: [MessageFlags.Ephemeral] });
    return;
  }

  const modal = new ModalBuilder()
    .setCustomId("modal_odprzejmij")
    .setTitle("Zwalnianie ticketu");
  const powInput = new TextInputBuilder()
    .setCustomId("powod_odprzejmij")
    .setLabel("Dlaczego chcesz zwolnić ticket?")
    .setStyle(2)
    .setRequired(false);
  modal.addComponents(new ActionRowBuilder().addComponents(powInput));
  await interaction.showModal(modal);
}

function replaceEmbedAliasTokens(text = "") {
  const arrowEmoji = "<a:arrowwhite:1491476759290449984>";
  const alertEmoji = "<a:alert:1474431227972026469>";
  const alertEmoji2 = "<a:alertownik2:1477688955221835807>";
  const minecraftEmoji2 = "<a:minecraft2:1480590181944791122>";
  const ironLoveEmoji = "<a:iron_love:1480590229697069210>";
  const starEmoji = "<:star:1474431260133691567>";

  return (text || "")
    .replace(/:strzałka:/gi, arrowEmoji)
    .replace(/:arrowwhite:/gi, arrowEmoji)
    .replace(/:alertownik:/gi, alertEmoji)
    .replace(/:alertownik2:/gi, alertEmoji2)
    .replace(/:minecraft2:/gi, minecraftEmoji2)
    .replace(/:iron_love:/gi, ironLoveEmoji)
    .replace(/:startownik:/gi, starEmoji);
}

function extractEmbedPingTokens(text = "") {
  const pingRegex = /<@!?\d+>|<@&\d+>|@everyone|@here/g;
  const matches = text.match(pingRegex) || [];
  const unique = [];

  for (const match of matches) {
    if (!unique.includes(match)) {
      unique.push(match);
    }
  }

  const cleaned = text
    .replace(pingRegex, "")
    .replace(/[ \t]{2,}/g, " ")
    .replace(/\n[ \t]+/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();

  return {
    pingContent: unique.join(" "),
    cleanedContent: cleaned,
  };
}

function collectEmbedMediaFromMessage(message) {
  const mediaUrls = [];
  const fileUrls = [];

  for (const attachment of message.attachments.values()) {
    const contentType = (attachment.contentType || "").toLowerCase();
    const name = (attachment.name || "").toLowerCase();
    const isMedia =
      contentType.startsWith("image/") ||
      contentType.startsWith("video/") ||
      /\.(png|jpe?g|gif|webp|bmp|mp4|mov|webm|m4v)$/i.test(name);

    if (isMedia) {
      mediaUrls.push(attachment.url);
    } else {
      fileUrls.push(attachment.url);
    }
  }

  return { mediaUrls, fileUrls };
}

function splitEmbedBodyIntoSections(text = "") {
  const lines = (text || "").split(/\r?\n/);
  const parts = [];
  let buffer = [];

  const flushBuffer = () => {
    const content = buffer.join("\n").trim();
    if (content) {
      parts.push({ type: "text", content });
    }
    buffer = [];
  };

  for (const line of lines) {
    if (line.trim() === "--") {
      flushBuffer();

      if (parts.length && parts[parts.length - 1].type !== "separator") {
        parts.push({ type: "separator" });
      }
      continue;
    }

    buffer.push(line);
  }

  flushBuffer();

  while (parts[0]?.type === "separator") {
    parts.shift();
  }

  while (parts[parts.length - 1]?.type === "separator") {
    parts.pop();
  }

  return parts;
}

function buildSendMessageCardPayload({
  bodyText,
  mediaUrls,
  includeDate,
  fileUrls,
  guildId,
}) {
  const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);
  const trimmedBody = (bodyText || "").trim();
  const bodyParts = splitEmbedBodyIntoSections(trimmedBody);

  if (bodyParts.length) {
    for (const part of bodyParts) {
      if (part.type === "separator") {
        container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
        continue;
      }

      container.addTextDisplayComponents(
        new TextDisplayBuilder().setContent(part.content),
      );
    }
  }

  if (mediaUrls.length) {
    container.addMediaGalleryComponents(
      new MediaGalleryBuilder().addItems(
        mediaUrls.map((url) => new MediaGalleryItemBuilder().setURL(url)),
      ),
    );
  }

  if (false && includeDate) {
    if (bodyParts.length || mediaUrls.length) {
      container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
    }

    container.addTextDisplayComponents(
      new TextDisplayBuilder().setContent(
        `-# Wysłano <t:${Math.floor(Date.now() / 1000)}:f>`,
      ),
    );
  }

  if (!container.components.length) {
    container.addTextDisplayComponents(
      new TextDisplayBuilder().setContent("-# (brak treści)"),
    );
  }

  appendBrandFooterToContainer(container, guildId);

  return {
    components: [container],
    files: fileUrls.length ? fileUrls : undefined,
    flags: MessageFlags.IsComponentsV2,
  };
}

async function handleSendMessageCommand(interaction) {
  if (interaction.user.id !== interaction.guild.ownerId) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const targetChannel =
    interaction.options.getChannel("kanal") || interaction.channel;
  const dateMode = interaction.options.getString("data") || "bezdaty";
  const pingMode = interaction.options.getString("pingi") || "bezpingu";
  const includeDate = dateMode === "zdata";

  if (!targetChannel || targetChannel.type !== ChannelType.GuildText) {
    await interaction.reply({
      content: "> `❌` × **Wybierz** poprawny kanał tekstowy **docelowy**.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  try {
    await interaction.reply({
      content:
        "✉️ Napisz w tym kanale w ciągu 2 minut wiadomość, którą mam wysłać.\n" +
        `Docelowy kanał: <#${targetChannel.id}>\n` +
        `Tryb daty: \`${dateMode}\`\n` +
        `Tryb pingów: \`${pingMode}\`\n\n` +
        "Możesz używać markdownu Discorda jak `###`, `**tekst**`, `-# tekst`, wysłać GIF/filmik/obraz i wpisać `anuluj`, aby przerwać.",
      flags: [MessageFlags.Ephemeral],
    });
  } catch (e) {
    console.error("handleSendMessageCommand: reply failed", e);
    return;
  }

  const collectChannel = interaction.channel;
  if (!collectChannel || !collectChannel.createMessageCollector) {
    await interaction.followUp({
      content:
        "❌ Nie mogę uruchomić kolektora w tym kanale. Spróbuj ponownie.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const filter = (m) => m.author.id === interaction.user.id && !m.author.bot;
  const collector = collectChannel.createMessageCollector({
    filter,
    time: 120_000,
    max: 1,
  });

  collector.on("collect", async (msg) => {
    const contentRaw = (msg.content || "").trim();
    await ensureEmbedTestEmojiCache(interaction.guild.id);
    const contentWithAliases = replaceNamedGuildEmojis(
      replaceEmbedAliasTokens(contentRaw),
      interaction.guild.id,
    );

    if (contentWithAliases.toLowerCase() === "anuluj") {
      try {
        await interaction.followUp({
          content: "> `❌` × **Anulowano** wysyłanie wiadomości.",
          flags: [MessageFlags.Ephemeral],
        });
      } catch (e) { }
      collector.stop("cancelled");
      return;
    }

    const { pingContent, cleanedContent } = extractEmbedPingTokens(
      contentWithAliases,
    );
    const { mediaUrls, fileUrls } = collectEmbedMediaFromMessage(msg);
    const finalBodyText =
      pingMode === "zpingiem" ? cleanedContent : contentWithAliases;

    try {
      const sendOptions = buildSendMessageCardPayload({
        bodyText: finalBodyText,
        mediaUrls,
        includeDate,
        fileUrls,
        guildId: interaction.guildId,
      });

      if (pingMode === "zpingiem" && pingContent) {
        await targetChannel.send({
          content: pingContent,
          allowedMentions: { parse: ["users", "roles", "everyone"] },
        });
      }

      await targetChannel.send(sendOptions);

      await interaction.followUp({
        content: `✅ Wiadomość została wysłana do <#${targetChannel.id}>.`,
        flags: [MessageFlags.Ephemeral],
      });
    } catch (err) {
      console.error("handleSendMessageCommand: send failed", err);
      try {
        await interaction.followUp({
          content:
            "❌ Nie udało się wysłać wiadomości. Sprawdź kanał, załączniki i format treści.",
          flags: [MessageFlags.Ephemeral],
        });
      } catch (e) { }
    }
  });

  collector.on("end", async (collected, reason) => {
    if (reason === "time" && collected.size === 0) {
      try {
        await interaction.followUp({
          content:
            "⌛ Nie otrzymałem wiadomości w wyznaczonym czasie. Użyj ponownie /embed aby spróbować jeszcze raz.",
          flags: [MessageFlags.Ephemeral],
        });
      } catch (e) { }
    }
  });
}

async function handleModyCommand(interaction) {
  if (!interaction.guild) {
    await interaction.reply({
      content: "> `❌` × **Ta komenda** działa tylko na **serwerze**.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // Owner-only
  if (interaction.user.id !== interaction.guild.ownerId) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const targetChannel =
    interaction.options.getChannel("kanal") || interaction.channel;

  if (!targetChannel || targetChannel.type !== ChannelType.GuildText) {
    await interaction.reply({
      content: "> `❌` × **Wybierz** poprawny kanał tekstowy **docelowy**.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  try {
    await interaction.reply({
      content:
        "✉️ Napisz w tym kanale (w ciągu 2 minut) wiadomość, którą mam wysłać z przyciskiem **Nagrania modów**.\n" +
        `Docelowy kanał: <#${targetChannel.id}>\n\n` +
        "Możesz wysłać tekst, obraz/GIF i animowane emoji. Wpisz `anuluj`, aby przerwać.",
      flags: [MessageFlags.Ephemeral],
    });
  } catch (e) {
    console.error("handleModyCommand: reply failed", e);
    return;
  }

  const collectChannel = interaction.channel;
  if (!collectChannel || !collectChannel.createMessageCollector) {
    await interaction.followUp({
      content: "❌ Nie mogę uruchomić kolektora w tym kanale. Spróbuj ponownie.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const filter = (m) => m.author.id === interaction.user.id && !m.author.bot;
  const collector = collectChannel.createMessageCollector({
    filter,
    time: 120_000,
    max: 1,
  });

  collector.on("collect", async (msg) => {
    const contentRaw = (msg.content || "").trim();
    const arrowEmoji = "<a:arrowwhite:1491476759290449984>";
    const alertEmoji = "<a:alert:1474431227972026469>";
    const alertEmoji2 = "<a:alertownik2:1477688955221835807>";
    const minecraftEmoji2 = "<a:minecraft2:1480590181944791122>";
    const ironLoveEmoji = "<a:iron_love:1480590229697069210>";
    const starEmoji = "<:star:1474431260133691567>";
    const content = contentRaw
      .replace(/:strzałka:/gi, arrowEmoji)
      .replace(/:arrowwhite:/gi, arrowEmoji)
      .replace(/:alertownik:/gi, alertEmoji)
      .replace(/:alertownik2:/gi, alertEmoji2)
      .replace(/:minecraft2:/gi, minecraftEmoji2)
      .replace(/:iron_love:/gi, ironLoveEmoji)
      .replace(/:startownik:/gi, starEmoji);

    if (content.toLowerCase() === "anuluj") {
      try {
        await interaction.followUp({
          content: "> `❌` × **Anulowano** wysyłanie wiadomości.",
          flags: [MessageFlags.Ephemeral],
        });
      } catch (e) { }
      collector.stop("cancelled");
      return;
    }

    const files = [];
    let imageAttachment = null;
    for (const att of msg.attachments.values()) {
      if (att.contentType && att.contentType.startsWith("image/")) {
        imageAttachment = att.url;
      } else {
        files.push(att.url);
      }
    }

    const sendEmbed = new EmbedBuilder()
      .setColor(COLOR_BLUE)
      .setDescription(
        (content || "`(brak treści)`").replace(/<@!?\d+>|@everyone|@here/g, ""),
      )
      .setTimestamp();

    if (imageAttachment) {
      sendEmbed.setImage(imageAttachment);
    }

    const videosButton = new ButtonBuilder()
      .setCustomId(`mody_videos_${Date.now()}`)
      .setLabel("Nagrania modów")
      .setEmoji("📸")
      .setStyle(ButtonStyle.Secondary);
    const buyModButton = new ButtonBuilder()
      .setCustomId(`mody_buy_${Date.now()}`)
      .setLabel("Zakup moda")
      .setEmoji({ id: "1477662159029796865", name: "java" })
      .setStyle(ButtonStyle.Secondary);
    const row = new ActionRowBuilder().addComponents(videosButton, buyModButton);

    try {
      const sendOptions = {
        embeds: [sendEmbed],
        components: [row],
        files: files.length ? files : undefined,
      };

      const pings = content.match(/<@!?\d+>|@everyone|@here/g);
      if (pings && pings.length > 0) {
        await targetChannel.send({ content: pings.join(" ") });
      }

      await targetChannel.send(sendOptions);

      await interaction.followUp({
        content: `✅ Wiadomość z przyciskiem modów została wysłana do <#${targetChannel.id}>.`,
        flags: [MessageFlags.Ephemeral],
      });
    } catch (err) {
      console.error("handleModyCommand: send failed", err);
      try {
        await interaction.followUp({
          content:
            "❌ Nie udało się wysłać wiadomości (sprawdź uprawnienia bota do wysyłania wiadomości/załączników).",
          flags: [MessageFlags.Ephemeral],
        });
      } catch (e) { }
    }
  });

  collector.on("end", async (collected, reason) => {
    if (reason === "time" && collected.size === 0) {
      try {
        await interaction.followUp({
          content:
            "⌛ Nie otrzymałem wiadomości w wyznaczonym czasie. Użyj ponownie /mody, aby spróbować jeszcze raz.",
          flags: [MessageFlags.Ephemeral],
        });
      } catch (e) { }
    }
  });
}

async function handleDropCommand(interaction) {
  const user = interaction.user;
  const guildId = interaction.guildId;

  // Now require guild and configured drop channel
  if (!guildId) {
    await interaction.reply({
      content: "> `❌` × **Ta komenda** działa tylko na **serwerze**!",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const dropChannelId = dropChannels.get(guildId);
  if (!dropChannelId) {
    await interaction.reply({
      content:
        "❌ Kanał drop nie został ustawiony. Administrator może ustawić go manualnie lub utworzyć kanał o nazwie domyślnej.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  if (interaction.channelId !== dropChannelId) {
    await interaction.reply({
      content: `> \`❌\` × Użyj tej **komendy** na kanale <#${dropChannelId}>`,
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // Enforce per-user cooldown for /drop (24h)
  const lastDrop = dropCooldowns.get(user.id) || 0;
  const now = Date.now();
  if (now - lastDrop < DROP_COOLDOWN_MS) {
    const remaining = DROP_COOLDOWN_MS - (now - lastDrop);
    await interaction.reply({
      content: `> \`❌\` × Możesz użyć komendy </drop:1464015494876102748> ponownie za \`${humanizeMs(remaining)}\``,
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // reduce drop chances (smaller chance to win)
  const chance = Math.random() * 100;

  let result;
  // Lower probabilities (smaller chance to win)
  if (chance < 0.5) {
    result = { win: true, discount: 10 };
  } else if (chance < 5) {
    result = { win: true, discount: 5 };
  } else {
    result = { win: false };
  }

  // Register use (start cooldown) regardless of win/lose
  dropCooldowns.set(user.id, Date.now());

  // we'll need the channel object to manage the instruction message after replying
  const channel = interaction.channel;

  if (result.win) {
    const code = generateCode();
    const expiryTime = Date.now() + 86400000;
    const expiryTimestamp = Math.floor(expiryTime / 1000);

    activeCodes.set(code, {
      oderId: user.id,
      discount: result.discount,
      expiresAt: expiryTime,
      created: Date.now(),
      type: "discount",
    });

    // Zapisz do Supabase
    await db.saveActiveCode(code, {
      oderId: user.id,
      discount: result.discount,
      expiresAt: expiryTime,
      created: Date.now(),
      type: "discount"
    });

    scheduleSavePersistentState();

    setTimeout(() => {
      activeCodes.delete(code);
      db.deleteActiveCode(code);
      scheduleSavePersistentState();
    }, 86400000);

    const winEmbed = new EmbedBuilder()
      .setColor(0xd4af37) // yellow for win
      .setDescription(
        "```\n" +
        "🎀 New Shop × DROP\n" +
        "```\n" +
        `\`👤\` × **Użytkownik:** ${user}\n` +
        `\`🎉\` × **Gratulacje! Udało ci się wylosować -${result.discount}% na zakupy w naszym sklepie!**\n` +
        `\`⏰\` × **Zniżka wygasa:** <t:${expiryTimestamp}:R>\n\n` +
        `📩 **Sprawdź prywatne wiadomości po kod!**`,
      )
      .setTimestamp();

    const dmEmbed = new EmbedBuilder()
      .setColor(0xd4af37)
      .setTitle("`🔑` Twój kod rabatowy")
      .setDescription(
        "```\n" +
        code +
        "\n```\n" +
        `> \`💸\` × **Otrzymałeś:** \`-${result.discount}%\`\n` +
        `> \`🕑\` × **Kod wygaśnie za:** <t:${expiryTimestamp}:R> \n\n` +
        `${PURCHASE_CODE_USAGE_TEXT}`,
      )
      .setTimestamp();

    try {
      await user.send({ embeds: [dmEmbed] });
      await interaction.reply({ embeds: [winEmbed] });
    } catch (error) {
      const winEmbedWithCode = new EmbedBuilder()
        .setColor(COLOR_YELLOW)
        .setDescription(
          "```\n" +
          "🎀 New Shop × DROP\n" +
          "```\n" +
          `\`👤\` × **Użytkownik:** ${user}\n` +
          `\`🎉\` × **Gratulacje! Udało ci się wylosować -${result.discount}% na zakupy w sklepie!**\n` +
          `\`🔑\` × **Twój kod:** ||\`${code}\`|| (kliknij aby odkryć)\n` +
          `\`⏰\` × **Zniżka wygasa:** <t:${expiryTimestamp}:R>`,
        )
        .setTimestamp();
      await interaction.reply({ embeds: [winEmbedWithCode], flags: [MessageFlags.Ephemeral] });
    }
  } else {
    const loseEmbed = new EmbedBuilder()
      .setColor(COLOR_GRAY) // gray for lose
      .setDescription(
        "```\n" +
        "🎀 New Shop × DROP\n" +
        "```\n" +
        `\`👤\` × **Użytkownik:** ${user}\n` +
        `\`😢\` × **Niestety, tym razem nie udało się! Spróbuj ponownie później...**`,
      )
      .setTimestamp();

    await interaction.reply({ embeds: [loseEmbed] });
  }

  // Manage drop instruction message: delete previous and send a fresh one so it moves to the bottom
  try {
    if (channel && channel.id) {
      // delete previous instruction if present
      const prevInstrId = lastDropInstruction.get(channel.id);
      if (prevInstrId) {
        try {
          const prevMsg = await channel.messages
            .fetch(prevInstrId)
            .catch(() => null);
          if (prevMsg && prevMsg.deletable) {
            await prevMsg.delete().catch(() => null);
          }
        } catch (err) {
          // ignore
        }
        lastDropInstruction.delete(channel.id);
      }

      // send new instruction embed
      const instructionDropEmbed = new EmbedBuilder()
        .setColor(COLOR_YELLOW)
        .setDescription(
          "`🎁` × Użyj **komendy** </drop:1464015494876102748>, aby wylosować zniżkę na zakupy!",
        );

      try {
        const sent = await channel.send({ embeds: [instructionDropEmbed] });
        lastDropInstruction.set(channel.id, sent.id);
      } catch (err) {
        // ignore (no perms)
      }
    }
  } catch (e) {
    console.error("Błąd zarządzania instrukcją drop:", e);
  }
}

async function handleOpinieKanalCommand(interaction) {
  const channel = interaction.options.getChannel("kanal");
  const guildId = interaction.guildId;
  if (!guildId) {
    await interaction.reply({
      content: "> `❌` × **Ta komenda** działa tylko na **serwerze**!",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  opinieChannels.set(guildId, channel.id);
  await interaction.reply({
    content: `✅ Kanał opinii ustawiony na <#${channel.id}>`,
    flags: [MessageFlags.Ephemeral],
  });
  console.log(`Kanał opinii ustawiony na ${channel.id} dla serwera ${guildId}`);
}

function getSellerPaymentProfileKey(guildId, userId) {
  return `${guildId}:${userId}`;
}

function getSellerPaymentProfile(guildId, userId) {
  return sellerPaymentProfiles.get(getSellerPaymentProfileKey(guildId, userId)) || null;
}

function normalizeSellerPaymentProfile(profile = {}) {
  return {
    phone: String(profile.phone || "").trim().slice(0, 80),
    transferTitle: String(profile.transferTitle || "").trim().slice(0, 120),
    recipient: String(profile.recipient || "").trim().slice(0, 120),
    paypalEmail: String(profile.paypalEmail || "").trim().toLowerCase().slice(0, 120),
    ltcWallet: String(profile.ltcWallet || "").trim().slice(0, 180),
    mypscEmail: String(profile.mypscEmail || "").trim().toLowerCase().slice(0, 120),
    updatedAt: Number(profile.updatedAt || Date.now()),
  };
}

function sellerPaymentProfileHasData(profile) {
  return !!(
    profile &&
    (String(profile.phone || "").trim() ||
      String(profile.transferTitle || "").trim() ||
      String(profile.recipient || "").trim() ||
      String(profile.paypalEmail || "").trim() ||
      String(profile.ltcWallet || "").trim() ||
      String(profile.mypscEmail || "").trim())
  );
}

function formatSellerPaymentValue(value) {
  const text = String(value || "").trim();
  return text ? `\`${text.replace(/`/g, "'")}\`` : "`Brak`";
}

function isPurchaseTicketForPaymentData(channel, ticketData = null) {
  const label = String(ticketData?.ticketTypeLabel || "").toUpperCase();
  if (label.startsWith("SPRZ")) return false;

  // Domyślnie traktuj jako ticket zakupowy, chyba że wyraźnie oznaczony jako sprzedaż
  return true;
}

function buildSellerPaymentPanelPayload(guildId) {
  const container = new ContainerBuilder()
    .setAccentColor(COLOR_BLUE)
    .addTextDisplayComponents(
      new TextDisplayBuilder().setContent(
        "```\n" +
        "💳 New Shop × DANE SPRZEDAWCY\n" +
        "```"
      )
    )
    .addSeparatorComponents(new SeparatorBuilder().setDivider(true))
    .addTextDisplayComponents(
      new TextDisplayBuilder().setContent(
        "> <a:arrowwhite:1491476759290449984> × Ustaw swoje dane, aby klient wiedział od razu co ma robić po przejęciu ticketa."
      )
    )
    .addSeparatorComponents(new SeparatorBuilder().setDivider(true));

  const row = new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId("seller_data_add_choice")
      .setLabel("📝︲Dodaj dane")
      .setStyle(ButtonStyle.Secondary),
    new ButtonBuilder()
      .setCustomId("seller_data_view")
      .setLabel("🔍︲Moje Dane")
      .setStyle(ButtonStyle.Secondary),
    new ButtonBuilder()
      .setCustomId("seller_data_clear")
      .setLabel("🧹︲Wyczyść")
      .setStyle(ButtonStyle.Secondary)
  );

  container.addActionRowComponents(row);
  appendBrandFooterToContainer(container, guildId);

  return { components: [container], flags: MessageFlags.IsComponentsV2 };
}

function buildSellerPaymentDataModalMain(interaction) {
  const current = getSellerPaymentProfile(interaction.guildId, interaction.user.id) || {};
  const modal = new ModalBuilder()
    .setCustomId("seller_data_modal")
    .setTitle("Ustaw dane");

  const phoneInput = new TextInputBuilder()
    .setCustomId("phone")
    .setLabel("Numer telefonu")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(80)
    .setPlaceholder("Przykład: 123 456 789");

  const transferTitleInput = new TextInputBuilder()
    .setCustomId("transfer_title")
    .setLabel("Tytuł przelewu")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(120)
    .setPlaceholder("Przykład: Zakup itemów mc");

  const recipientInput = new TextInputBuilder()
    .setCustomId("recipient")
    .setLabel("Odbiorca przelewu")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(120)
    .setPlaceholder("Przykład: Imię Nazwisko");

  setTextInputValueIfPresent(phoneInput, current.phone || "");
  setTextInputValueIfPresent(transferTitleInput, current.transferTitle || "");
  setTextInputValueIfPresent(recipientInput, current.recipient || "");

  modal.addComponents(
    new ActionRowBuilder().addComponents(phoneInput),
    new ActionRowBuilder().addComponents(transferTitleInput),
    new ActionRowBuilder().addComponents(recipientInput)
  );

  return modal;
}

function buildSellerPaymentDataModalExtra(interaction) {
  const current = getSellerPaymentProfile(interaction.guildId, interaction.user.id) || {};
  const modal = new ModalBuilder()
    .setCustomId("seller_data_modal")
    .setTitle("Ustaw dane");

  const paypalEmailInput = new TextInputBuilder()
    .setCustomId("paypal_email")
    .setLabel("PayPal (E-mail)")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(120)
    .setPlaceholder("Przykład: paypal@example.com");

  const ltcWalletInput = new TextInputBuilder()
    .setCustomId("ltc_wallet")
    .setLabel("Portfel LTC")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(180)
    .setPlaceholder("Przykład: Lhq9...");

  const mypscEmailInput = new TextInputBuilder()
    .setCustomId("mypsc_email")
    .setLabel("MyPSC (Konto / E-mail)")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(120)
    .setPlaceholder("Przykład: mypsc@example.com");

  setTextInputValueIfPresent(paypalEmailInput, current.paypalEmail || "");
  setTextInputValueIfPresent(ltcWalletInput, current.ltcWallet || "");
  setTextInputValueIfPresent(mypscEmailInput, current.mypscEmail || "");

  modal.addComponents(
    new ActionRowBuilder().addComponents(paypalEmailInput),
    new ActionRowBuilder().addComponents(ltcWalletInput),
    new ActionRowBuilder().addComponents(mypscEmailInput)
  );

  return modal;
}

async function handlePanelDaneCommand(interaction) {
  if (!interaction.guild) {
    await interaction.reply({
      content: "> `❌` × **Ta komenda** działa tylko na **serwerze**.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  if (!interaction.member?.permissions?.has(PermissionFlagsBits.ManageChannels)) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  await interaction.channel.send(buildSellerPaymentPanelPayload(interaction.guildId));
  await interaction.reply({
    content: "> `✅` × Panel danych sprzedawcy został wysłany.",
    flags: [MessageFlags.Ephemeral],
  });
}

async function handleOstrzezenieCommand(interaction) {
  try {
    // Defer natychmiast – Discord ma 3s limit, a my robimy wiele async operacji
    await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });

    const SELLER_ROLE_ID = "1350786945944391733";
    const isAdmin = interaction.member?.permissions?.has(PermissionFlagsBits.Administrator);
    const isSeller = interaction.member?.roles?.cache?.has(SELLER_ROLE_ID);
    if (!isAdmin && !isSeller) {
      await interaction.editReply({
        content: "> `❌` × Nie masz uprawnień do wystawiania ostrzeżeń.",
      });
      return;
    }

    const targetUser = interaction.options.getUser("sprzedawca");
    const powod = interaction.options.getString("powod");

    if (!targetUser) {
      await interaction.editReply({
        content: "> `❌` × Nie wybrano sprzedawcy.",
      });
      return;
    }

    const key = `${interaction.guildId}:${targetUser.id}`;
    const previousCount = sellerWarnings.get(key) || 0;

    if (previousCount >= 3) {
      await interaction.editReply({
        content: `> \`❌\` × Użytkownik <@${targetUser.id}> posiada już maksymalną liczbę ostrzeżeń (**3/3**).`,
      });
      return;
    }

    const currentCount = Math.min(3, previousCount + 1);
    sellerWarnings.set(key, currentCount);
    scheduleSavePersistentState(true);

    let headerTitle = `OSTRZEŻENIE ${currentCount}/3`;
    if (currentCount === 2) headerTitle = `OSTRZEŻENIE ${currentCount}/3 ❗️`;

    const container = new ContainerBuilder()
      .setAccentColor(COLOR_BLUE)
      .addTextDisplayComponents(
        new TextDisplayBuilder().setContent(
          `## ${headerTitle}\n` +
          `> Powód: ${powod}`
        )
      );

    appendBrandFooterToContainer(container, interaction.guildId);

    let warnChannel = interaction.guild?.channels.cache.get(OSTRZEZENIA_CHANNEL_ID);
    if (!warnChannel && interaction.guild) {
      warnChannel = await interaction.guild.channels.fetch(OSTRZEZENIA_CHANNEL_ID).catch(() => null);
    }
    if (!warnChannel) warnChannel = interaction.channel;

    // Ping osobno (poza embedem), potem container
    const pingMsg = await warnChannel.send({ content: `<@${targetUser.id}>` }).catch(() => null);
    const embedMsg = await warnChannel.send({
      components: [container],
      flags: MessageFlags.IsComponentsV2,
    }).catch(() => null);

    const warnMsgs = sellerWarnMessages.get(key) || [];
    warnMsgs.push({
      pingMessageId: pingMsg?.id,
      embedMessageId: embedMsg?.id,
      channelId: warnChannel.id,
    });
    sellerWarnMessages.set(key, warnMsgs);

    // Jeśli sprzedawca osiągnął 3/3 – odbierz mu wszystkie role limitu zakupu i nadaj rolę zawieszony
    if (currentCount >= 3 && interaction.guild) {
  const LIMIT_ROLE_IDS = [
    "1541553994000891984", // NO LIMIT
    "1541553589833695393", // limit 400
    "1449448860517798061", // limit 200
    "1449448686156255333", // limit 100
    "1449448702925209651", // limit 50
    "1449448705563557918", // limit 20
  ];
      const SUSPENDED_ROLE_ID = "1537090439239442483";
      const member = await interaction.guild.members.fetch(targetUser.id).catch(() => null);
      if (member) {
        // Zapamiętaj role limitów, które użytkownik aktualnie posiada (zanim je odbierzemy)
        const userLimitRoles = LIMIT_ROLE_IDS.filter((roleId) => member.roles.cache.has(roleId));
        if (userLimitRoles.length > 0) {
          sellerSavedLimitRoles.set(key, userLimitRoles);
        }

        for (const roleId of LIMIT_ROLE_IDS) {
          if (member.roles.cache.has(roleId)) {
            await member.roles.remove(roleId, "3/3 ostrzeżeń – utrata limitu zakupu").catch((e) =>
              console.error(`Błąd usuwania roli limitu ${roleId}:`, e)
            );
          }
        }
        // Nadaj rolę zawieszony
        if (!member.roles.cache.has(SUSPENDED_ROLE_ID)) {
          await member.roles.add(SUSPENDED_ROLE_ID, "3/3 ostrzeżeń – zawieszenie").catch((e) =>
            console.error("Błąd nadawania roli zawieszony:", e)
          );
        }
      }
    }

    await interaction.editReply({
      content: `> \`✅\` × Pomyślnie wystawiono ostrzeżenie **${currentCount}/3** dla użytkownika <@${targetUser.id}> na kanale <#${warnChannel.id}>.`,
    });
  } catch (err) {
    console.error("Błąd w handleOstrzezenieCommand:", err);
    if (interaction.deferred) {
      await interaction.editReply({
        content: "> `❌` × Wystąpił błąd podczas wystawiania ostrzeżenia.",
      }).catch(() => null);
    } else if (!interaction.replied) {
      await interaction.reply({
        content: "> `❌` × Wystąpił błąd podczas wystawiania ostrzeżenia.",
        flags: [MessageFlags.Ephemeral],
      }).catch(() => null);
    }
  }
}

async function handleOstrzezenieUsunCommand(interaction) {
  try {
    await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });

    const SELLER_ROLE_ID = "1350786945944391733";
    const isAdmin = interaction.member?.permissions?.has(PermissionFlagsBits.Administrator);
    const isSeller = interaction.member?.roles?.cache?.has(SELLER_ROLE_ID);
    if (!isAdmin && !isSeller) {
      await interaction.editReply({
        content: "> `❌` × Nie masz uprawnień do usuwania ostrzeżeń.",
      });
      return;
    }

    const targetUser = interaction.options.getUser("sprzedawca");
    if (!targetUser) {
      await interaction.editReply({
        content: "> `❌` × Nie wybrano sprzedawcy.",
      });
      return;
    }

    const key = `${interaction.guildId}:${targetUser.id}`;
    const previousCount = sellerWarnings.get(key) || 0;

    if (previousCount <= 0) {
      await interaction.editReply({
        content: `> \`❌\` × Użytkownik <@${targetUser.id}> nie posiada żadnych aktywnych ostrzeżeń.`,
      });
      return;
    }

    const currentValidCount = Math.min(3, previousCount);
    const newCount = currentValidCount - 1;

    if (newCount > 0) {
      sellerWarnings.set(key, newCount);
    } else {
      sellerWarnings.delete(key);
    }

    // Jeśli wcześniej miał 3/3 (zawieszony), a teraz spadł poniżej 3 (np. do 2/3):
    if (previousCount >= 3 && newCount < 3 && interaction.guild) {
      const SUSPENDED_ROLE_ID = "1537090439239442483";
      const member = await interaction.guild.members.fetch(targetUser.id).catch(() => null);
      if (member) {
        // Usuń rolę zawieszony
        if (member.roles.cache.has(SUSPENDED_ROLE_ID)) {
          await member.roles.remove(SUSPENDED_ROLE_ID, "Obniżenie liczby ostrzeżeń poniżej 3/3").catch((e) =>
            console.error("Błąd usuwania roli zawieszony:", e)
          );
        }

        // Przywróć zapamiętane role limitów
        const savedRoles = sellerSavedLimitRoles.get(key);
        if (Array.isArray(savedRoles) && savedRoles.length > 0) {
          for (const roleId of savedRoles) {
            await member.roles.add(roleId, "Przywrócenie limitu po usunięciu ostrzeżenia").catch((e) =>
              console.error(`Błąd przywracania roli limitu ${roleId}:`, e)
            );
          }
          sellerSavedLimitRoles.delete(key);
        } else {
          // Jeśli z jakiegoś powodu nie było zapisanych ról w pamięci, przywracamy domyślną rolę limitu 20
          const DEFAULT_LIMIT_ROLE_ID = "1449448705563557918";
          await member.roles.add(DEFAULT_LIMIT_ROLE_ID, "Domyślne przywrócenie limitu po usunięciu ostrzeżenia").catch(() => null);
        }
      }
    }

    scheduleSavePersistentState(true);

    // Usuń wiadomość z ostrzeżeniem z kanału #ostrzeżenia
    let warnChannel = interaction.guild?.channels.cache.get(OSTRZEZENIA_CHANNEL_ID);
    if (!warnChannel && interaction.guild) {
      warnChannel = await interaction.guild.channels.fetch(OSTRZEZENIA_CHANNEL_ID).catch(() => null);
    }

    if (warnChannel) {
      const warnMsgs = sellerWarnMessages.get(key) || [];
      if (warnMsgs.length > 0) {
        const lastWarn = warnMsgs.pop();
        if (warnMsgs.length > 0) {
          sellerWarnMessages.set(key, warnMsgs);
        } else {
          sellerWarnMessages.delete(key);
        }
        if (lastWarn.pingMessageId) {
          await warnChannel.messages.delete(lastWarn.pingMessageId).catch(() => null);
        }
        if (lastWarn.embedMessageId) {
          await warnChannel.messages.delete(lastWarn.embedMessageId).catch(() => null);
        }
      } else {
        // Fallback: szukamy w ostatnich 50 wiadomościach kanału warnChannel jeśli wiadomość wysłana przed dodaniem shiptrackingu
        const recentMsgs = await warnChannel.messages.fetch({ limit: 50 }).catch(() => null);
        if (recentMsgs && recentMsgs.size > 0) {
          const sorted = Array.from(recentMsgs.values()).sort((a, b) => b.createdTimestamp - a.createdTimestamp);
          for (let i = 0; i < sorted.length; i++) {
            const msg = sorted[i];
            if (msg.content.includes(targetUser.id)) {
              await msg.delete().catch(() => null);
              const adjacentMsg = sorted[i - 1] || sorted[i + 1];
              if (adjacentMsg && adjacentMsg.author?.id === interaction.client.user.id) {
                await adjacentMsg.delete().catch(() => null);
              }
              break;
            }
          }
        }
      }
    }

    await interaction.editReply({
      content: `> \`✅\` × Usunięto 1 ostrzeżenie użytkownikowi <@${targetUser.id}>. Aktualna liczba: **${newCount}/3**.`,
    });
  } catch (err) {
    console.error("Błąd w handleOstrzezenieUsunCommand:", err);
    if (interaction.deferred) {
      await interaction.editReply({
        content: "> `❌` × Wystąpił błąd podczas usuwania ostrzeżenia.",
      }).catch(() => null);
    } else if (!interaction.replied) {
      await interaction.reply({
        content: "> `❌` × Wystąpił błąd podczas usuwania ostrzeżenia.",
        flags: [MessageFlags.Ephemeral],
      }).catch(() => null);
    }
  }
}

async function handleOstrzezeniaListCommand(interaction) {
  try {
    const targetUser = interaction.options.getUser("sprzedawca") || interaction.user;
    const key = `${interaction.guildId}:${targetUser.id}`;
    const count = sellerWarnings.get(key) || 0;

    const container = new ContainerBuilder()
      .setAccentColor(COLOR_BLUE)
      .addTextDisplayComponents(
        new TextDisplayBuilder().setContent(
          "```\n" +
          "⚠️ New Shop × OSTRZEŻENIA SPRZEDAWCY\n" +
          "```"
        )
      )
      .addSeparatorComponents(new SeparatorBuilder().setDivider(true))
      .addTextDisplayComponents(
        new TextDisplayBuilder().setContent(
          `> <a:arrowwhite:1491476759290449984> × **Sprzedawca:** <@${targetUser.id}>\n` +
          `> <a:arrowwhite:1491476759290449984> × **Liczba ostrzeżeń:** **${count}/3**`
        )
      );

    appendBrandFooterToContainer(container, interaction.guildId);

    await interaction.reply({
      components: [container],
      flags: MessageFlags.Ephemeral | MessageFlags.IsComponentsV2,
    });
  } catch (err) {
    console.error("Błąd w handleOstrzezeniaListCommand:", err);
    if (!interaction.replied && !interaction.deferred) {
      await interaction.reply({
        content: "> `❌` × Wystąpił błąd podczas pobierania listy ostrzeżeń.",
        flags: [MessageFlags.Ephemeral],
      }).catch(() => null);
    }
  }
}

async function sendSellerPaymentProfileToTicket(channel, guildId, sellerId, ticketData = null) {
  if (!isPurchaseTicketForPaymentData(channel, ticketData)) return;

  const profile = getSellerPaymentProfile(guildId, sellerId);
  if (!sellerPaymentProfileHasData(profile)) return;

  const method = String(ticketData?.paymentMethod || "").toLowerCase();

  const lines = [];
  const addLine = (emoji, label, value) => {
    if (value && value !== "`Brak`" && value !== "Brak") {
      lines.push(`> ${emoji} × **${label}:** ${formatSellerPaymentValue(value)}`);
    }
  };

  lines.push("> ### <a:jump_dirt:1480590181944791122> × **Dane do płatności**");

  if (method === "kod_blik" || method === "kod blik") {
    return;
  } else if (method.includes("blik") || method.includes("przelew")) {
    addLine("`📱`", "Telefon", profile.phone);
    addLine("`🧾`", "Tytuł przelewu", profile.transferTitle);
    addLine("`👤`", "Odbiorca", profile.recipient);
  } else if (method === "paypal") {
    addLine("`✉️`", "PayPal", profile.paypalEmail);
  } else if (method === "ltc") {
    addLine("`👝`", "Portfel LTC", profile.ltcWallet);
  } else if (method === "mypsc") {
    addLine("`🌐`", "MyPSC", profile.mypscEmail);
  } else if (method === "psc" || method === "psc_bez_paragonu" || method === "kod_blik" || method === "kod blik") {
    // PSC / KOD BLIK - brak danych bankowych, instrukcja osobno
    return;
  } else {
    addLine("`📱`", "Telefon", profile.phone);
    addLine("`🧾`", "Tytuł przelewu", profile.transferTitle);
    addLine("`👤`", "Odbiorca", profile.recipient);
    addLine("`✉️`", "PayPal", profile.paypalEmail);
    addLine("`👝`", "Portfel LTC", profile.ltcWallet);
    addLine("`🌐`", "MyPSC", profile.mypscEmail);
  }

  if (lines.length === 1) return;

  const embed = new EmbedBuilder()
    .setColor(COLOR_BLUE)
    .setDescription(lines.join("\n"));

  await channel.send({ embeds: [embed] }).catch(() => null);
}

async function handlePanelWeryfikacjaCommand(interaction) {
  const guildId = interaction.guildId;
  if (!guildId) {
    await interaction.reply({
      content: "> `❌` × **Ta komenda** działa tylko na **serwerze**!",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // Sprawdź czy właściciel
  if (interaction.user.id !== interaction.guild.ownerId) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const roleId = "1425935544273338532";
  const button = new ButtonBuilder()
    .setStyle(ButtonStyle.Link)
    .setLabel("︲Zweryfikuj się")
    .setEmoji({ id: "1537166632613322792", name: "YES", animated: true })
    .setURL(
      "https://discord.com/oauth2/authorize?client_id=1449397101032112139&redirect_uri=https%3A%2F%2Frestorecord.com%2Fapi%2Fcallback&response_type=code&scope=identify+guilds.join&state=1350446732365926491&prompt=none",
    );

  const row = new ActionRowBuilder().addComponents(button);
  const verificationContainer = new ContainerBuilder()
    .setAccentColor(COLOR_BLUE)
    .addTextDisplayComponents(
      new TextDisplayBuilder().setContent(
        "```\n" +
        "🛒 New Shop × WERYFIKACJA\n" +
        "```",
      ),
    )
    .addSeparatorComponents(new SeparatorBuilder().setDivider(true))
    .addTextDisplayComponents(
      new TextDisplayBuilder().setContent(
        "> <a:arrowwhite:1491476759290449984> Weryfikacja pozwala **przywrócić cię na serwer** po __**t3rmie**__.\n" +
        "> <a:arrowwhite:1491476759290449984> **Nie będziemy zapraszać** żadnych osób na **inne serwery!**",
      ),
    )
    .addSeparatorComponents(new SeparatorBuilder().setDivider(true))
    .addActionRowComponents(row);

  appendBrandFooterToContainer(verificationContainer, guildId);

  try {
    // Defer reply na początku, aby uniknąć Unknown interaction
    await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });

    const sendOptions = {
      components: [verificationContainer],
      flags: MessageFlags.IsComponentsV2,
      allowedMentions: { roles: [roleId] },
    };


    await interaction.channel.send(sendOptions);

    await interaction.editReply({
      content: "> `✅` × **Panel** weryfikacji wysłany na ten **kanał**.",
    });
    console.log(
      `Wysłano panel weryfikacji na kanale ${interaction.channelId} (serwer ${guildId})`,
    );
  } catch (err) {
    console.error("Błąd wysyłania panelu weryfikacji:", err);
    try {
      if (interaction.replied || interaction.deferred) {
        await interaction.editReply({
          content:
            "❌ Nie udało się wysłać panelu weryfikacji (sprawdź uprawnienia lub ścieżkę do pliku).",
        });
      } else {
        await interaction.reply({
          content:
            "❌ Nie udało się wysłać panelu weryfikacji (sprawdź uprawnienia lub ścieżkę do pliku).",
          flags: [MessageFlags.Ephemeral],
        });
      }
    } catch (e) {
      // ignore
    }
  }
}

async function searchLogiTicketChannel(guild, query, searchType = "kod") {
  try {
    const logCh = await getLogiTicketChannel(guild);
    if (!logCh) return searchType === "kod" ? null : [];

    const fetchPromise = logCh.messages.fetch({ limit: 100 }).catch(() => null);
    const timeoutPromise = new Promise((resolve) => setTimeout(() => resolve(null), 1500));
    const messages = await Promise.race([fetchPromise, timeoutPromise]);

    if (!messages || !messages.size) return searchType === "kod" ? null : [];

    if (searchType === "kod") {
      const qLower = String(query).toLowerCase().trim();
      for (const msg of messages.values()) {
        for (const embed of (msg.embeds || [])) {
          const footerText = String(embed.footer?.text || "").toLowerCase();
          const channelField = String(embed.fields?.find((f) => f.name === "Kanał")?.value || "").toLowerCase();
          const typeField = String(embed.fields?.find((f) => f.name === "Typ")?.value || "");
          const statusField = String(embed.fields?.find((f) => f.name === "Status")?.value || "");
          const ownerField = String(embed.fields?.find((f) => f.name === "Klient")?.value || "");
          const claimedField = String(embed.fields?.find((f) => f.name === "Obsługa")?.value || "");
          const infoField = String(embed.fields?.find((f) => f.name === "Informacje")?.value || "");

          if (footerText.includes(qLower) || channelField.includes(qLower)) {
            const shortMatch = footerText.match(/kod id:\s*([a-z0-9_-]+)/i);
            const shortId = shortMatch ? shortMatch[1] : (generateTicketShortCode(query) || query);
            const ownerMatch = ownerField.match(/<@!?(\d+)>/);
            const ownerId = ownerMatch ? ownerMatch[1] : null;
            const historyField = embed.fields?.find((f) => f.name === "Historia")?.value || "";
            const historyLines = historyField ? historyField.split("\n").filter(Boolean) : [];
            const txtAtt = msg.attachments?.find((a) => a.name.endsWith(".txt")) || msg.attachments?.first();
            const attachmentUrl = txtAtt?.url || null;

            return {
              shortId,
              channelId: query,
              ownerId,
              type: typeField.replace(/`/g, ""),
              status: statusField.replace(/`/g, ""),
              openedAt: msg.createdTimestamp,
              closedAt: statusField.includes("ZAMKNIĘTY") ? msg.createdTimestamp : null,
              closeReason: infoField.includes("Powód:") ? infoField.split("Powód:")[1]?.split("\n")[0]?.trim() : "brak",
              claimedBy: claimedField.match(/<@!?(\d+)>/)?.[1] || null,
              formInfo: infoField.includes("Informacje z formularza") ? infoField : null,
              history: historyLines,
              attachmentUrl,
            };
          }
        }
      }
      return null;
    }

    if (searchType === "user") {
      const results = [];
      const userId = String(query);
      for (const msg of messages.values()) {
        for (const embed of (msg.embeds || [])) {
          const ownerField = String(embed.fields?.find((f) => f.name === "Klient")?.value || "");
          if (ownerField.includes(userId)) {
            const footerText = String(embed.footer?.text || "");
            const shortMatch = footerText.match(/kod id:\s*([a-z0-9_-]+)/i);
            const shortId = shortMatch ? shortMatch[1] : generateTicketShortCode(msg.id);
            const typeField = String(embed.fields?.find((f) => f.name === "Typ")?.value || "NIEZNANY");
            const statusField = String(embed.fields?.find((f) => f.name === "Status")?.value || "ZAMKNIĘTY");

            results.push({
              shortId,
              ownerId: userId,
              type: typeField.replace(/`/g, ""),
              status: statusField.replace(/`/g, ""),
              openedAt: msg.createdTimestamp,
            });
            break;
          }
        }
      }
      return results;
    }
  } catch (err) {
    console.error("searchLogiTicketChannel error:", err);
  }
  return searchType === "kod" ? null : [];
}

async function handleZnajdzTicketCommand(interaction) {
  try {
    await interaction.deferReply({ flags: [MessageFlags.Ephemeral] }).catch(() => null);

    if (!isAdminOrSeller(interaction.member)) {
      await interaction.editReply({
        content: "> `‼️` × Brak wymaganych uprawnień.",
      });
      return;
    }

    const kodInput = (interaction.options.getString("kod") || "").trim();
    const targetUser = interaction.options.getUser("uzytkownik");

    if (!kodInput && !targetUser) {
      await interaction.editReply({
        content: "> `❌` × Musisz podać **kod ID ticketu** lub **oznaczyć gracza**.",
      });
      return;
    }

    const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);
    const arrowEmoji = '<a:arrowwhite:1491476759290449984>';

    if (kodInput) {
      let ticketRecord = ticketHistoryStore.get(kodInput);

      if (!ticketRecord) {
        for (const rec of ticketHistoryStore.values()) {
          if (rec.shortId === kodInput || rec.channelId === kodInput) {
            ticketRecord = rec;
            break;
          }
        }
      }

      if (!ticketRecord && interaction.guild) {
        ticketRecord = await searchLogiTicketChannel(interaction.guild, kodInput, "kod");
      }

      if (!ticketRecord) {
        await interaction.editReply({
          content: `> \`❌\` × Nie znaleziono ticketu o ID lub kodzie: \`${kodInput}\`.`,
        });
        return;
      }

      container.addTextDisplayComponents(
        new TextDisplayBuilder().setContent(
          "```\n" +
          "🔍 New Shop × SZCZEGÓŁY TICKETU\n" +
          "```"
        )
      );
      container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

      const openedTimeStr = ticketRecord.openedAt ? `<t:${Math.floor(ticketRecord.openedAt / 1000)}:F>` : "Brak danych";
      const closedTimeStr = ticketRecord.closedAt ? `<t:${Math.floor(ticketRecord.closedAt / 1000)}:F>` : "Nadal otwarty";

      container.addTextDisplayComponents(
        new TextDisplayBuilder().setContent(
          `> ${arrowEmoji} × **Kod ID:** \`${ticketRecord.shortId}\`\n` +
          `> ${arrowEmoji} × **Klient:** ${ticketRecord.ownerId ? `<@${ticketRecord.ownerId}> (\`${ticketRecord.ownerId}\`)` : "Nieznany"}\n` +
          `> ${arrowEmoji} × **Typ ticketu:** \`${ticketRecord.type || "NIEZNANY"}\`\n` +
          `> ${arrowEmoji} × **Status:** \`${ticketRecord.status || "NIEZNANY"}\`\n` +
          `> ${arrowEmoji} × **Data utworzenia:** ${openedTimeStr}\n` +
          `> ${arrowEmoji} × **Data zamknięcia:** ${closedTimeStr}\n` +
          `> ${arrowEmoji} × **Powód zamknięcia:** \`${ticketRecord.closeReason || "brak"}\`\n` +
          `> ${arrowEmoji} × **Obsługa:** ${ticketRecord.claimedBy ? `<@${ticketRecord.claimedBy}>` : "Brak"}`
        )
      );

      if (ticketRecord.formInfo) {
        container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
        container.addTextDisplayComponents(
          new TextDisplayBuilder().setContent(
            `### \`📝\` × **Formularz:**\n${ticketRecord.formInfo}`
          )
        );
      }

      if (ticketRecord.history && ticketRecord.history.length > 0) {
        container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
        container.addTextDisplayComponents(
          new TextDisplayBuilder().setContent(
            `### \`📜\` × **Historia zdarzeń:**\n` +
            ticketRecord.history.map((h) => `> ${h}`).join("\n")
          )
        );
      }

      if (ticketRecord.transcriptText || ticketRecord.attachmentUrl) {
        container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

        let previewText = "";
        if (ticketRecord.transcriptText) {
          const rawParts = ticketRecord.transcriptText.split("--- MESSAGES ---");
          const msgBody = rawParts.length > 1 ? rawParts[1].trim() : rawParts[0].trim();
          const lines = msgBody.split("\n\n").filter(Boolean);
          const lastLines = lines.slice(-8);
          previewText = lastLines.join("\n\n");
        }

        if (previewText) {
          if (previewText.length > 1500) previewText = previewText.slice(0, 1500) + "\n...";
          container.addTextDisplayComponents(
            new TextDisplayBuilder().setContent(
              `### \`💬\` × **Zapis rozmowy (Podgląd):**\n` +
              "```text\n" +
              previewText + "\n" +
              "```"
            )
          );
        } else {
          container.addTextDisplayComponents(
            new TextDisplayBuilder().setContent(
              `### \`📁\` × **Transkrypt wiadomości:**\n` +
              `> \`📄\` × **Pełny plik transkryptu jest dostępny do pobrania poniżej.**`
            )
          );
        }

        if (ticketRecord.attachmentUrl) {
          const btnTranscript = new ButtonBuilder()
            .setLabel("📄 Otwórz pełny transkrypt (.txt)")
            .setStyle(ButtonStyle.Link)
            .setURL(ticketRecord.attachmentUrl);

          container.addActionRowComponents(
            new ActionRowBuilder().addComponents(btnTranscript)
          );
        }
      }

      appendBrandFooterToContainer(container, interaction.guildId);

      await interaction.editReply({
        components: [container],
        flags: MessageFlags.IsComponentsV2,
      });
      return;
    }

    if (targetUser) {
      const userTickets = [];
      const seenShortIds = new Set();

      for (const rec of ticketHistoryStore.values()) {
        if (rec.ownerId === targetUser.id && rec.shortId && !seenShortIds.has(rec.shortId)) {
          seenShortIds.add(rec.shortId);
          userTickets.push(rec);
        }
      }

      if (userTickets.length === 0 && interaction.guild) {
        const channelResults = await searchLogiTicketChannel(interaction.guild, targetUser.id, "user");
        for (const rec of channelResults) {
          if (rec.shortId && !seenShortIds.has(rec.shortId)) {
            seenShortIds.add(rec.shortId);
            userTickets.push(rec);
          }
        }
      }

      if (userTickets.length === 0) {
        await interaction.editReply({
          content: `> \`❌\` × Gracz <@${targetUser.id}> nie posiada zarejestrowanych ticketów.`,
        });
        return;
      }

      userTickets.sort((a, b) => (b.openedAt || 0) - (a.openedAt || 0));

      container.addTextDisplayComponents(
        new TextDisplayBuilder().setContent(
          "```\n" +
          "🔍 New Shop × TICKETY GRACZA\n" +
          "```"
        )
      );
      container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

      const lines = userTickets.slice(0, 15).map((t) => {
        const dateStr = t.openedAt ? `<t:${Math.floor(t.openedAt / 1000)}:R>` : "dawno";
        return `> ${arrowEmoji} × **Kod:** \`${t.shortId}\` | **Typ:** \`${t.type || "Inny"}\` | **Status:** \`${t.status || "Zamknięty"}\` (${dateStr})`;
      });

      container.addTextDisplayComponents(
        new TextDisplayBuilder().setContent(
          `### \`👤\` × **Tickety gracza** <@${targetUser.id}> (Łącznie: \`${userTickets.length}\`):\n` +
          lines.join("\n")
        )
      );

      appendBrandFooterToContainer(container, interaction.guildId);

      await interaction.editReply({
        components: [container],
        flags: MessageFlags.IsComponentsV2,
      });
    }
  } catch (err) {
    console.error("Error in handleZnajdzTicketCommand:", err);
    await interaction.editReply({
      content: "> `❌` × Wystąpił błąd podczas pobierania informacji o ticketach.",
    }).catch(() => null);
  }
}

async function handleTicketCommand(interaction) {
  const botName = client.user?.username || "NEWSHOP";

  const embed = new EmbedBuilder()
    .setColor(COLOR_BLUE)
    .setDescription(
      "```\n" +
      "🛒 New Shop × TICKET\n" +
      "```\n" +
      `📦 × Wybierz odpowiednią kategorię, aby utworzyć ticketa!`,
    );

  const selectMenu = new StringSelectMenuBuilder()
    .setCustomId("ticket_category")
    .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
    .addOptions([
      {
        label: "ᴢᴀᴋᴜᴘ ɪᴛᴇᴍóᴡ",
        value: "zakup",
        description: "Kliknij, aby kupić itemy!",
        emoji: "🛒",
      },
      {
        label: "ꜱᴘʀᴢᴇᴅᴀż",
        value: "sprzedaz",
        description: "Kliknij, aby sprzedać przedmioty!",
        emoji: { id: "1476700165082710178", name: "kasa_2" },
      },
      {
        label: "ᴢᴀᴋᴜᴘ ᴀᴜᴛᴏʀsᴋɪᴇɢᴏ ᴍᴏᴅᴀ",
        value: "zakup_moda",
        description: "Kliknij, aby kupić autorskiego moda!",
        emoji: { id: "1480590181944791122", name: "autorynek" },
      },
      {
        label: "ᴢᴀᴋᴜᴘ ʙᴏᴛóᴡ",
        value: "zakup_autorynku",
        description: "Kliknij, aby kupić najlepsze boty!",
        emoji: { id: "1480590181944791122", name: "autorynek" },
      },
      {
        label: "ᴏᴅʙɪᴇʀᴢ ɴᴀɢʀᴏᴅᴇ",
        value: "odbior",
        description: "Kliknij, aby odebrać nagrodę, którą zdobyłeś!",
        emoji: { id: "1480590229697069210", name: "nagroda" },
      },
      {
        label: "ᴘʏᴛᴀɴɪᴇ / ᴘᴏᴍᴏᴄ",
        value: "inne",
        description: "Kliknij, aby zadać pytanie lub otrzymać pomoc!",
        emoji: { id: "1477688955221835807", name: "pytanie", animated: true },
      },
    ]);

  const row = new ActionRowBuilder().addComponents(selectMenu);

  await interaction.reply({
    embeds: [embed],
    components: [row],
    flags: [MessageFlags.Ephemeral],
  });
}

function getDiscordMessageUrl(guildId, channelId, messageId = null) {
  if (!guildId || !channelId) return "https://discord.com/channels/@me";
  if (messageId) return `https://discord.com/channels/${guildId}/${channelId}/${messageId}`;
  return `https://discord.com/channels/${guildId}/${channelId}`;
}

function findEmbedTestPaymentsChannel(guild) {
  if (!guild) return null;

  const normalize = (s = "") =>
    s
      .toString()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^a-z0-9 ]/gi, "")
      .trim()
      .toLowerCase();

  return (
    guild.channels.cache.find(
      (channel) =>
        channel.type === ChannelType.GuildText &&
        (normalize(channel.name).includes("platnosci") ||
          normalize(channel.name).includes("platnosc")),
    ) || null
  );
}

const EMBED_TEST_COLOR_OPTIONS = [
  {
    value: "blue",
    label: "Niebieski",
    description: "Domyślny styl New Shop",
    emoji: "🔵",
    color: COLOR_BLUE,
  },
  {
    value: "cyan",
    label: "Cyan",
    description: "Jasny chłodny akcent",
    emoji: "🩵",
    color: 0x3cc8ff,
  },
  {
    value: "green",
    label: "Zielony",
    description: "Miękki zielony akcent",
    emoji: "🟢",
    color: 0x57f287,
  },
  {
    value: "yellow",
    label: "Żółty",
    description: "Mocniejszy jasny styl",
    emoji: "🟡",
    color: 0xfee75c,
  },
  {
    value: "orange",
    label: "Pomarańczowy",
    description: "Ciepły pomarańczowy akcent",
    emoji: "🟠",
    color: 0xffa543,
  },
  {
    value: "red",
    label: "Czerwony",
    description: "Mocny kontrastowy styl",
    emoji: "🔴",
    color: 0xed4245,
  },
  {
    value: "pink",
    label: "Różowy",
    description: "Jaśniejszy neonowy wariant",
    emoji: "🩷",
    color: 0xeb459e,
  },
  {
    value: "purple",
    label: "Fioletowy",
    description: "Delikatny ciemniejszy akcent",
    emoji: "🟣",
    color: 0x9b59b6,
  },
  {
    value: "gray",
    label: "Szary",
    description: "Bardziej stonowany wygląd",
    emoji: "⚫",
    color: 0x4f545c,
  },
];

const EMBED_TEST_PRIMARY_BUTTON_ACTION_OPTIONS = [
  {
    value: "zakup",
    label: "Zakup itemów",
    description: "Otwiera formularz zakupu itemów",
    emoji: "🛒",
  },
  {
    value: "zakup_autorynku",
    label: "Zakup botów",
    description: "Otwiera formularz zakupu botów",
    emoji: "🤖",
  },
  {
    value: "zakup_moda",
    label: "Zakup moda",
    description: "Otwiera formularz zakupu moda",
    emoji: "🧩",
  },
  {
    value: "sprzedaz",
    label: "Sprzedaż",
    description: "Otwiera formularz sprzedaży",
    emoji: "💸",
  },
  {
    value: "odbior",
    label: "Odbierz nagrodę",
    description: "Otwiera odbiór nagrody",
    emoji: "🎁",
  },
  {
    value: "inne",
    label: "Pomoc",
    description: "Otwiera formularz pomocy",
    emoji: "❓",
  },
  {
    value: "panel",
    label: "Panel kategorii",
    description: "Pokazuje cały panel ticketów",
    emoji: "📩",
  },
  {
    value: "regulamin",
    label: "Regulamin",
    description: "Otwiera przeglądarkę regulaminu",
    emoji: "📜",
  },
  {
    value: "nagrania",
    label: "Nagrania",
    description: "Wyświetla nagrania modów",
    emoji: "🎥",
  },
  {
    value: "link",
    label: "Link zewnętrzny",
    description: "Otwiera link w przeglądarce",
    emoji: "🔗",
  },
  {
    value: "kalkulator",
    label: "Kalkulator",
    description: "Otwiera kalkulator waluty",
    emoji: "🧮",
  },
  {
    value: "sprawdz_bonusy",
    label: "Sprawdź bonusy",
    description: "Pokazuje wydaną kwotę i aktualny bonus gracza",
    emoji: "💵",
  },
  {
    value: "zostansprzedawca",
    label: "Zostań sprzedawcą",
    description: "Otwiera wybór limitu sprzedawcy i ceny pakietu",
    emoji: "🤝",
  },
];

const EMBED_TEST_SPECIAL_EMOJI_MARKUP = {
  newshop: NEWSHOP_EMOJI_MARKUP,
  gg: "<:anarchia_gg:1469444521308852324>",
  kasa: "<:kasa_2:1476700165082710178>",
  kasa_2: "<:kasa_2:1476700165082710178>",
  strzalka: "<a:arrowwhite:1491476759290449984>",
  "strzałka": "<a:arrowwhite:1491476759290449984>",
  arrowwhite: "<a:arrowwhite:1491476759290449984>",
};

function getEmbedTestColorDef(value) {
  return (
    EMBED_TEST_COLOR_OPTIONS.find((option) => option.value === value) ||
    EMBED_TEST_COLOR_OPTIONS[0]
  );
}

function getEmbedTestPrimaryButtonActionDef(value) {
  return (
    EMBED_TEST_PRIMARY_BUTTON_ACTION_OPTIONS.find(
      (option) => option.value === value,
    ) || EMBED_TEST_PRIMARY_BUTTON_ACTION_OPTIONS[0]
  );
}

function parseEmbedTestPrimaryButtonActionInput(input, fallback = "zakup") {
  const normalized = (input || "")
    .toString()
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");

  if (isHttpUrl(input)) {
    return { value: "link", label: "Link", url: input };
  }

  if (!normalized) {
    return getEmbedTestPrimaryButtonActionDef(fallback);
  }

  if (normalized === "link" || normalized === "url") {
    return { value: "link", label: "Link" };
  }

  const directMatch = EMBED_TEST_PRIMARY_BUTTON_ACTION_OPTIONS.find(
    (option) => option.value === normalized,
  );
  if (directMatch) return directMatch;

  if (["zostan sprzedawca", "zostan_sprzedawca"].includes(normalized.replace(/ł/g, "l"))) {
    return getEmbedTestPrimaryButtonActionDef("zostansprzedawca");
  }

  if (
    normalized === "zakup itemow" ||
    normalized === "zakup itemy" ||
    normalized === "itemy" ||
    normalized === "item" ||
    normalized === "zakup"
  ) {
    return getEmbedTestPrimaryButtonActionDef("zakup");
  }

  if (
    normalized === "zakup botów" ||
    normalized === "zakup botow" ||
    normalized === "boty" ||
    normalized === "bot" ||
    normalized === "zakup autorynku" ||
    normalized === "autorynek" ||
    normalized === "auto rynek"
  ) {
    return getEmbedTestPrimaryButtonActionDef("zakup_autorynku");
  }

  if (
    normalized === "zakup autorskiego moda" ||
    normalized === "zakup moda" ||
    normalized === "mod" ||
    normalized === "mody" ||
    normalized === "moda"
  ) {
    return getEmbedTestPrimaryButtonActionDef("zakup_moda");
  }

  if (normalized === "sprzedaz" || normalized === "sprzedaz itemow") {
    return getEmbedTestPrimaryButtonActionDef("sprzedaz");
  }

  if (
    normalized === "nagroda" ||
    normalized === "nagroda za zaproszenia" ||
    normalized === "odbior"
  ) {
    return getEmbedTestPrimaryButtonActionDef("odbior");
  }

  if (
    normalized === "pomoc" ||
    normalized === "pytanie" ||
    normalized === "pytanie / pomoc" ||
    normalized === "inne"
  ) {
    return getEmbedTestPrimaryButtonActionDef("inne");
  }

  if (
    normalized === "panel" ||
    normalized === "panel kategorii" ||
    normalized === "kategorie"
  ) {
    return getEmbedTestPrimaryButtonActionDef("panel");
  }

  if (
    normalized === "regulamin" ||
    normalized === "zasady" ||
    normalized === "rules"
  ) {
    return getEmbedTestPrimaryButtonActionDef("regulamin");
  }

  if (
    normalized === "kalkulator" ||
    normalized === "calc" ||
    normalized === "oblicz"
  ) {
    return getEmbedTestPrimaryButtonActionDef("kalkulator");
  }

  if (
    normalized === "sprawdz_bonusy" ||
    normalized === "sprawdz bonusy" ||
    normalized === "bonusy" ||
    normalized === "bonus"
  ) {
    return getEmbedTestPrimaryButtonActionDef("sprawdz_bonusy");
  }

  return null;
}

function getEmbedTestSpecialEmojiMarkup(token) {
  const normalized = (token || "")
    .toString()
    .trim()
    .replace(/^:/, "")
    .replace(/:$/, "")
    .toLowerCase();

  return EMBED_TEST_SPECIAL_EMOJI_MARKUP[normalized] || null;
}

async function ensureEmbedTestEmojiCache(guildId) {
  if (!guildId) return;

  const lastFetch = embedTestEmojiCacheReady.get(guildId) || 0;
  if (Date.now() - lastFetch < 60_000) {
    return;
  }

  const guild = client.guilds.cache.get(guildId);
  if (!guild) return;

  try {
    await guild.emojis.fetch();
    embedTestEmojiCacheReady.set(guildId, Date.now());
  } catch (error) {
    console.error("embedtest emoji fetch failed:", error);
  }
}

function findGuildEmojiByName(guildId, emojiName) {
  if (!guildId || !emojiName) return null;

  const guild = client.guilds.cache.get(guildId);
  if (!guild) return null;

  const normalized = emojiName.toLowerCase();
  return (
    guild.emojis.cache.find((emoji) => emoji.name?.toLowerCase() === normalized) ||
    client.emojis?.cache?.find(
      (emoji) => emoji.name?.toLowerCase() === normalized,
    ) ||
    null
  );
}

function toGuildEmojiMarkup(emoji) {
  if (!emoji) return "";
  return `<${emoji.animated ? "a" : ""}:${emoji.name}:${emoji.id}>`;
}

function replaceNamedGuildEmojis(text, guildId) {
  const source = (text || "").toString();
  if (!source) return "";

  const preserved = [];
  const masked = source.replace(/<a?:[a-zA-Z0-9_]+:\d+>/g, (match) => {
    const token = `__EMBEDTEST_EMOJI_${preserved.length}__`;
    preserved.push({ token, markup: match });
    return token;
  });

  const replaced = masked.replace(/:([^:\s]+):/g, (match, name) => {
    const specialEmojiMarkup = getEmbedTestSpecialEmojiMarkup(name);
    if (specialEmojiMarkup) return specialEmojiMarkup;

    const emoji = findGuildEmojiByName(guildId, name);
    return emoji ? toGuildEmojiMarkup(emoji) : match;
  });

  return preserved.reduce(
    (content, item) => content.replace(item.token, item.markup),
    replaced,
  );
}

function setTextInputValueIfPresent(input, value) {
  if (typeof value === "string" && value.length > 0) {
    input.setValue(value);
  }

  return input;
}

function parseButtonEmojiInput(input, guildId) {
  const value = (input || "").trim();
  if (!value) return null;

  const specialEmojiMarkup = getEmbedTestSpecialEmojiMarkup(value);
  if (specialEmojiMarkup) {
    input = specialEmojiMarkup;
  }

  const normalizedValue = (input || "").trim();

  const customEmojiMatch = normalizedValue.match(/^<(a?):([a-zA-Z0-9_]+):(\d+)>$/);
  if (customEmojiMatch) {
    const [, animatedFlag, name, id] = customEmojiMatch;
    return {
      id,
      name,
      animated: animatedFlag === "a",
    };
  }

  const customEmojiByNameMatch = normalizedValue.match(/^:([a-zA-Z0-9_]+):$/);
  if (customEmojiByNameMatch) {
    const emoji = findGuildEmojiByName(guildId, customEmojiByNameMatch[1]);
    return emoji
      ? { id: emoji.id, name: emoji.name, animated: emoji.animated }
      : null;
  }

  if (/^[a-zA-Z0-9_]+$/.test(normalizedValue)) {
    const emoji = findGuildEmojiByName(guildId, normalizedValue);
    if (emoji) {
      return { id: emoji.id, name: emoji.name, animated: emoji.animated };
    }
  }

  return { name: normalizedValue };
}

function buildEmbedTestSectionParts(title, body, guildId) {
  const parts = [];

  if (title) {
    parts.push({
      type: "text",
      content: `**${replaceNamedGuildEmojis(title, guildId)}**`,
    });
  }

  const normalizedBody = replaceNamedGuildEmojis(body || "", guildId);
  if (normalizedBody) {
    parts.push(...splitEmbedBodyIntoSections(normalizedBody));
  }

  return parts;
}

function appendEmbedTestSectionToContainer(
  container,
  sectionParts,
  addLeadingSeparator = false,
) {
  if (!Array.isArray(sectionParts) || !sectionParts.length) {
    return false;
  }

  let dividerAdded = false;
  if (addLeadingSeparator) {
    container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
    dividerAdded = true;
  }

  let hasVisibleContent = false;
  let hasAnyComponent = false;

  for (const part of sectionParts) {
    if (!part) continue;

    if (part.type === "separator") {
      // Dodaj separator jeśli mamy już jakąś treść LUB jeśli to początek sekcji (ale nie podwójnie)
      if (!dividerAdded || hasVisibleContent) {
        container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
        dividerAdded = true;
        hasAnyComponent = true;
      }
      continue;
    }

    if (part.type === "text" && part.content) {
      container.addTextDisplayComponents(
        new TextDisplayBuilder().setContent(part.content),
      );
      hasVisibleContent = true;
      hasAnyComponent = true;
      dividerAdded = false;
    }
  }

  return hasAnyComponent;
}

function isEmbedTestPublishTarget(channel) {
  return (
    !!channel &&
    typeof channel.isSendable === "function" &&
    channel.isSendable() &&
    !(typeof channel.isDMBased === "function" && channel.isDMBased())
  );
}

function parseEmbedTestChannelInput(input) {
  const value = (input || "").trim();
  if (!value) return null;

  const mentionMatch = value.match(/^<#(\d+)>$/);
  if (mentionMatch) return mentionMatch[1];

  const idMatch = value.match(/^(\d{5,})$/);
  if (idMatch) return idMatch[1];

  return null;
}

function getPendingEmbedTestPublishKey(guildId, userId) {
  return `${guildId}:${userId}`;
}

function normalizeEmbedTestChannelLookup(value) {
  return (value || "")
    .toString()
    .trim()
    .replace(/^#/, "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function resolveEmbedTestPublishTargetFromMessage(message) {
  if (!message.guild) return null;

  const mentionedChannel = message.mentions?.channels?.first() || null;
  if (isEmbedTestPublishTarget(mentionedChannel)) {
    return mentionedChannel;
  }

  const channelId = parseEmbedTestChannelInput(message.content);
  if (channelId) {
    const byId = message.guild.channels.cache.get(channelId) || null;
    if (isEmbedTestPublishTarget(byId)) {
      return byId;
    }
  }

  const lookup = normalizeEmbedTestChannelLookup(message.content);
  if (!lookup) return null;

  return (
    message.guild.channels.cache.find((channel) => {
      if (!isEmbedTestPublishTarget(channel)) return false;
      return normalizeEmbedTestChannelLookup(channel.name) === lookup;
    }) || null
  );
}

function normalizeEmbedTestAttachment(attachment) {
  if (!attachment?.url) return null;

  const contentType = (attachment.contentType || "").toLowerCase();
  const name = (attachment.name || "").toLowerCase();
  const isMedia =
    contentType.startsWith("image/") ||
    contentType.startsWith("video/") ||
    /\.(png|jpe?g|gif|webp|bmp|mp4|mov|webm|m4v)$/i.test(name);

  if (!isMedia) return null;

  return {
    url: attachment.url,
    name: attachment.name || null,
    contentType: attachment.contentType || null,
  };
}

function sanitizeEmbedTestMediaFilename(name = "embedtest-media.mp4") {
  const raw = String(name || "embedtest-media.mp4").trim();
  const extMatch = raw.match(/\.([a-z0-9]{2,5})$/i);
  const ext = extMatch ? extMatch[0].toLowerCase() : ".mp4";
  const base = raw
    .replace(/\.[a-z0-9]{2,5}$/i, "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-zA-Z0-9_-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 48);

  return `${base || "embedtest_media"}${ext}`;
}

function createEmbedTestMediaFileFromAttachment(attachment) {
  const normalized = normalizeEmbedTestAttachment(attachment);
  if (!normalized) return null;

  const name = sanitizeEmbedTestMediaFilename(normalized.name || "embedtest-media.mp4");
  return {
    url: normalized.url,
    name,
    contentType: normalized.contentType || null,
  };
}

function getEmbedTestMediaFilesFromMessage(message) {
  if (!message?.attachments?.size) return [];

  const files = [];
  for (const attachment of message.attachments.values()) {
    const mediaFile = createEmbedTestMediaFileFromAttachment(attachment);
    if (mediaFile) files.push(mediaFile);
  }
  return files;
}

function applyEmbedTestMediaFilesToState(state, mediaFiles = []) {
  const validFiles = Array.isArray(mediaFiles)
    ? mediaFiles.filter((file) => file?.url && file?.name)
    : [];

  state.mediaFiles = validFiles;
  state.mediaUrls = validFiles.map((file) => `attachment://${file.name}`);
}

function getEmbedTestPayloadFiles(state) {
  return Array.isArray(state?.mediaFiles)
    ? state.mediaFiles
      .filter((file) => file?.url && file?.name)
      .map((file) => ({ attachment: file.url, name: file.name }))
    : [];
}

function isRegulationEmbedState(state) {
  return state?.variant === "regulamin";
}

function normalizeRegulationPage(page) {
  return {
    title: String(page?.title || ""),
    body: String(page?.body || ""),
  };
}

function createDefaultRegulationPages() {
  return [
    {
      title: "> # 1. __Postanowienia ogólne__ 📜",
      body:
        "> :strzałka: Korzystanie z naszych usług oznacza **akceptację zasad** obowiązujących na serwerze.\n" +
        "> :strzałka: Zakupy dotyczą m.in. serwerów takich jak: **Anarchia, DonutSMP, PvkMC** oraz innych wskazanych przez administrację.\n" +
        "> :strzałka: Administracja zastrzega sobie prawo do **zmiany regulaminu** w każdym momencie.\n" +
        "> :strzałka: **Nieznajomość zasad** nie zwalnia z ich przestrzegania.",
    },
    {
      title: "> # 2. __Transakcje__ 🛒",
      body:
        "> :strzałka: Obsługujemy płatności przez **BLIK, Paysafecard, PayPal, Revolut oraz krypto**.\n" +
        "> :strzałka: Każdą wpłatę wykonuj **dokładnie według wskazówek administracji**, inaczej może nie zostać zaliczona.\n" +
        "> :strzałka: Wykrycie środków z **nielegalnego pochodzenia** skutkuje **cofnięciem transakcji** i **blokadą konta**.",
    },
    {
      title: "> # 3. Zachowanie użytkownika 👤",
      body:
        "> :strzałka: Próby **oszustwa**, wprowadzania w błąd lub **brak szacunku wobec administracji** mogą skutkować **odmową realizacji transakcji** oraz **blokadą konta**.\n" +
        "> :strzałka: W takich przypadkach administracja może **zatrzymać środki** oraz **ukarać użytkownika**.",
    },
    {
      title: "> # 4. Zwroty :kasa_3:",
      body:
        "> :strzałka: Po dokonaniu zakupu **środki nie podlegają zwrotowi**.",
    },
    {
      title: "> # 5. Wymogi nagród za zaproszenia :gift:",
      body:
        "> :strzałka: **Multikonta, konta AFK oraz puste profile** nie są zaliczane każde zaproszenie jest **weryfikowane**.\n" +
        "> :strzałka: **Zaproszona osoba musi być zweryfikowana** (posiadać rangę **Klient**).\n" +
        "> :strzałka: Zaproszona osoba musi przebywać na serwerze **minimum 24h** oraz mieć konto discord **co najmniej 2 miesiące**.\n" +
        "> :strzałka: **Zakaz oszustw, spamu i sztucznego nabijania** grozi **brakiem nagrody lub banem**.",
    },
  ];
}

function getLegacyRegulationPages(state) {
  return [
    {
      title: state?.cashSectionTitle || "",
      body: state?.cashBody || "",
    },
    {
      title: state?.itemsSectionTitle || "",
      body: state?.itemsBody || "",
    },
    {
      title: state?.extraSectionTitle || "",
      body: state?.extraSectionBody || "",
    },
    {
      title: state?.extraSectionTwoTitle || "",
      body: state?.extraSectionTwoBody || "",
    },
  ]
    .map((page) => normalizeRegulationPage(page))
    .filter(
      (page) => String(page.title || "").trim() || String(page.body || "").trim(),
    );
}

function getRawRegulationPages(state, fallbackPages = null) {
  if (Array.isArray(state?.pages) && state.pages.length) {
    return state.pages.map((page) => normalizeRegulationPage(page));
  }

  const legacyPages = getLegacyRegulationPages(state);
  if (legacyPages.length) {
    return legacyPages;
  }

  if (Array.isArray(fallbackPages) && fallbackPages.length) {
    return fallbackPages.map((page) => normalizeRegulationPage(page));
  }

  return [
    {
      title: "Regulamin",
      body: "-# Ten regulamin nie został jeszcze uzupełniony.",
    },
  ];
}

function setRegulationPagesOnState(state, pages) {
  const normalizedPages =
    Array.isArray(pages) && pages.length
      ? pages.map((page) => normalizeRegulationPage(page))
      : [{ title: "", body: "" }];
  const [first = {}, second = {}, third = {}, fourth = {}] = normalizedPages;

  state.pages = normalizedPages.map(p => ({
    ...p,
    body: sanitizeBranding(p.body)
  }));
  state.cashSectionTitle = first.title || "";
  state.cashBody = sanitizeBranding(first.body || "");
  state.itemsSectionTitle = second.title || "";
  state.itemsBody = sanitizeBranding(second.body || "");
  state.extraSectionTitle = third.title || "";
  state.extraSectionBody = sanitizeBranding(third.body || "");
  state.extraSectionTwoTitle = fourth.title || "";
  state.extraSectionTwoBody = sanitizeBranding(fourth.body || "");
  return state;
}

function cloneRegulationPanelState(state, overrides = {}) {
  const colorKey = state?.accentColorKey || "yellow";
  const colorDef = getEmbedTestColorDef(colorKey);
  const cloned = {
    ownerId: state?.ownerId || null,
    guildId: state?.guildId || null,
    channelId: state?.channelId || null,
    messageId: state?.messageId || null,
    variant: "regulamin",
    persistPanel: !!state?.persistPanel,
    accentColorKey: colorKey,
    accentColor: Number(state?.accentColor || colorDef.color),
    headerBadge: String(state?.headerBadge || "📜"),
    headerNote: String(
      state?.headerNote ||
      "• Kliknij **przycisk poniżej**, aby wyświetlić regulamin.",
    ),
    title: String(state?.title || "NEW SHOP × REGULAMIN"),
    cashSectionTitle: String(state?.cashSectionTitle || ""),
    cashBody: String(state?.cashBody || ""),
    itemsSectionTitle: String(state?.itemsSectionTitle || ""),
    itemsBody: String(state?.itemsBody || ""),
    extraSectionTitle: String(state?.extraSectionTitle || ""),
    extraSectionBody: String(state?.extraSectionBody || ""),
    extraSectionTwoTitle: String(state?.extraSectionTwoTitle || ""),
    extraSectionTwoBody: String(state?.extraSectionTwoBody || ""),
    buttonOneLabel: String(state?.buttonOneLabel || "Zobacz regulamin"),
    buttonOneEmoji: String(state?.buttonOneEmoji || "📜"),
    buttonOneAction: "regulamin",
    buttonOneUrl: "",
    buttonTwoLabel: String(state?.buttonTwoLabel || ""),
    buttonTwoEmoji: String(state?.buttonTwoEmoji || ""),
    buttonTwoUrl: String(state?.buttonTwoUrl || ""),
    mediaUrls: Array.isArray(state?.mediaUrls)
      ? state.mediaUrls.filter((url) => typeof url === "string" && url.trim())
      : [],
    pages: Array.isArray(state?.pages)
      ? state.pages.map((page) => normalizeRegulationPage(page))
      : [],
    ...overrides,
  };

  return setRegulationPagesOnState(
    cloned,
    getRawRegulationPages(cloned, createDefaultRegulationPages()),
  );
}

function createDefaultRegulaminState(
  guild,
  targetChannel,
  ownerId,
  mediaAttachment = null,
) {
  const baseState = createDefaultEmbedTestState(
    guild,
    targetChannel,
    ownerId,
    mediaAttachment,
  );

  return cloneRegulationPanelState(baseState, {
    ownerId,
    guildId: guild.id,
    channelId: targetChannel.id,
    messageId: null,
    persistPanel: true,
    accentColorKey: "yellow",
    accentColor: getEmbedTestColorDef("yellow").color,
    headerBadge: "📜",
    headerNote: "• Kliknij **przycisk poniżej**, aby wyświetlić regulamin.",
    title: "NEW SHOP × REGULAMIN",
    pages: createDefaultRegulationPages(),
    buttonOneLabel: "Zobacz regulamin",
    buttonOneEmoji: "📜",
    buttonTwoLabel: "",
    buttonTwoEmoji: "",
    buttonTwoUrl: "",
  });
}

function getRegulationPanelPages(state) {
  return getRawRegulationPages(state);
}

function getRegulationPanelStateByMessageId(messageId) {
  if (!messageId) return null;

  const persistedState = regulationPanels.get(messageId);
  if (persistedState) {
    return cloneRegulationPanelState(persistedState, { messageId });
  }

  const editableState = embedTestStates.get(messageId);
  if (editableState && isRegulationEmbedState(editableState)) {
    return editableState;
  }

  return null;
}

function buildRegulationPanelMessagePayload(state, skipFooter = false) {
  const buttons = [];
  const mediaUrls = Array.isArray(state.mediaUrls)
    ? state.mediaUrls.filter((url) => typeof url === "string" && url.trim())
    : [];
  const buttonOneEmoji = parseButtonEmojiInput(
    state.buttonOneEmoji,
    state.guildId,
  );
  const buttonTwoEmoji = parseButtonEmojiInput(
    state.buttonTwoEmoji,
    state.guildId,
  );

  const headingParts = [];
  if (state.headerBadge) {
    headingParts.push(replaceNamedGuildEmojis(state.headerBadge, state.guildId));
  }
  if (state.title) {
    headingParts.push(replaceNamedGuildEmojis(state.title, state.guildId));
  }

  const container = new ContainerBuilder().setAccentColor(
    state.accentColor || COLOR_BLUE,
  );

  if (headingParts.length) {
    container.addTextDisplayComponents(
      new TextDisplayBuilder().setContent(headingParts.join(" ")),
    );
  }

  if (state.headerNote) {
    if (headingParts.length) {
      container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
    }

    container.addTextDisplayComponents(
      new TextDisplayBuilder().setContent(
        replaceNamedGuildEmojis(state.headerNote, state.guildId),
      ),
    );
  }

  if (mediaUrls.length) {
    if (headingParts.length || state.headerNote) {
      container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
    }

    container.addMediaGalleryComponents(
      new MediaGalleryBuilder().addItems(
        mediaUrls.map((url) => new MediaGalleryItemBuilder().setURL(url)),
      ),
    );
  }

  if (state.buttonOneLabel) {
    const button = new ButtonBuilder()
      .setLabel(state.buttonOneLabel)
      .setStyle(ButtonStyle.Secondary)
      .setCustomId("embedtest_buy_open_regulamin");

    if (buttonOneEmoji) {
      button.setEmoji(buttonOneEmoji);
    }

    buttons.push(button);
  }

  if (state.buttonTwoLabel && isHttpUrl(state.buttonTwoUrl)) {
    const button = new ButtonBuilder()
      .setLabel(state.buttonTwoLabel)
      .setStyle(ButtonStyle.Link)
      .setURL(state.buttonTwoUrl);

    if (buttonTwoEmoji) {
      button.setEmoji(buttonTwoEmoji);
    }

    buttons.push(button);
  }

  if (buttons.length) {
    if (container.components.length > 0) {
      container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
    }

    container.addActionRowComponents(
      new ActionRowBuilder().addComponents(...buttons),
    );
  }

  if (!container.components.length) {
    container.addTextDisplayComponents(
      new TextDisplayBuilder().setContent("-# Pusty panel regulaminu"),
    );
  }

  if (!skipFooter) {
    appendBrandFooterToContainer(container, state.guildId);
  }

  return {
    components: [container],
    files: getEmbedTestPayloadFiles(state),
    flags: MessageFlags.IsComponentsV2,
  };
}

function buildRegulationViewerPayload(state, panelMessageId, pageIndex = 0) {
  const pages = getRegulationPanelPages(state);
  const safeIndex = Math.max(
    0,
    Math.min(Number(pageIndex) || 0, pages.length - 1),
  );
  const page = pages[safeIndex] || pages[0];
  const pageTitle = replaceNamedGuildEmojis(
    page.title || state.title || "REGULAMIN",
    state.guildId,
  ).trim();
  const pageBody = replaceNamedGuildEmojis(
    page.body || "-# Ta strona regulaminu jest pusta.",
    state.guildId,
  ).trim();
  const container = new ContainerBuilder().setAccentColor(
    state.accentColor || COLOR_BLUE,
  );

  if (pageTitle) {
    container.addTextDisplayComponents(
      new TextDisplayBuilder().setContent(pageTitle),
    );
  }

  if (pageBody) {
    if (container.components.length) {
      container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
    }
    container.addTextDisplayComponents(
      new TextDisplayBuilder().setContent(pageBody),
    );
  }

  if (pages.length > 1) {
    if (container.components.length) {
      container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
    }
    container.addActionRowComponents(
      new ActionRowBuilder().addComponents(
        new ButtonBuilder()
          .setCustomId(
            `regulamin_page_${panelMessageId}_${Math.max(0, safeIndex - 1)}`,
          )
          .setStyle(ButtonStyle.Secondary)
          .setLabel("<")
          .setDisabled(safeIndex === 0),
        new ButtonBuilder()
          .setCustomId(`regulamin_page_info_${panelMessageId}_${safeIndex}`)
          .setStyle(ButtonStyle.Secondary)
          .setLabel(`${safeIndex + 1}/${pages.length}`)
          .setDisabled(true),
        new ButtonBuilder()
          .setCustomId(
            `regulamin_page_${panelMessageId}_${Math.min(
              pages.length - 1,
              safeIndex + 1,
            )}`,
          )
          .setStyle(ButtonStyle.Secondary)
          .setLabel(">")
          .setDisabled(safeIndex === pages.length - 1),
      ),
    );
  }

  // Stopka pozostaje częścią tego samego kontenera. Helper dodaje przed nią
  // separator, dzięki czemu regulamin ma identyczny układ jak pozostałe panele.
  appendBrandFooterToContainer(container, state.guildId);

  return {
    components: [container],
    flags: MessageFlags.IsComponentsV2,
  };
}

function buildRegulationPagesEditorPayload(state, pageIndex = 0) {
  const pages = getRegulationPanelPages(state);
  const safeIndex = Math.max(
    0,
    Math.min(Number(pageIndex) || 0, pages.length - 1),
  );
  const page = pages[safeIndex] || pages[0] || { title: "", body: "" };
  const titlePreview = replaceNamedGuildEmojis(
    page.title || `Strona ${safeIndex + 1}`,
    state.guildId,
  );
  const bodyPreview = replaceNamedGuildEmojis(
    page.body || "-# Ta strona jest jeszcze pusta.",
    state.guildId,
  );

  const embed = new EmbedBuilder()
    .setColor(COLOR_BLUE)
    .setTitle("New Shop × Strony regulaminu")
    .setDescription(
      `Edytujesz stronę **${safeIndex + 1}/${pages.length}**.\nKliknij przycisk niżej, żeby ją zmienić albo dodać kolejną.`,
    )
    .addFields(
      {
        name: "Tytuł strony",
        value: titlePreview.slice(0, 1024) || "-# Brak tytułu",
      },
      {
        name: "Treść strony",
        value:
          bodyPreview.length > 1024
            ? `${bodyPreview.slice(0, 1021)}...`
            : bodyPreview,
      },
    );

  return {
    embeds: [embed],
    components: [
      new ActionRowBuilder().addComponents(
        new ButtonBuilder()
          .setCustomId(`regulamin_editor_prev_${state.messageId}_${safeIndex}`)
          .setLabel("<")
          .setStyle(ButtonStyle.Secondary)
          .setDisabled(safeIndex === 0),
        new ButtonBuilder()
          .setCustomId(`regulamin_editor_info_${state.messageId}_${safeIndex}`)
          .setLabel(`${safeIndex + 1}/${pages.length}`)
          .setStyle(ButtonStyle.Secondary)
          .setDisabled(true),
        new ButtonBuilder()
          .setCustomId(`regulamin_editor_next_${state.messageId}_${safeIndex}`)
          .setLabel(">")
          .setStyle(ButtonStyle.Secondary)
          .setDisabled(safeIndex === pages.length - 1),
        new ButtonBuilder()
          .setCustomId(`regulamin_editor_edit_${state.messageId}_${safeIndex}`)
          .setLabel("Edytuj")
          .setStyle(ButtonStyle.Primary),
        new ButtonBuilder()
          .setCustomId(`regulamin_editor_add_${state.messageId}_${safeIndex}`)
          .setLabel("Dodaj stronę")
          .setStyle(ButtonStyle.Success),
      ),
      new ActionRowBuilder().addComponents(
        new ButtonBuilder()
          .setCustomId(`regulamin_editor_delete_${state.messageId}_${safeIndex}`)
          .setLabel("Usuń stronę")
          .setStyle(ButtonStyle.Danger)
          .setDisabled(pages.length <= 1),
      ),
    ],
  };
}

function buildRegulationPageModal(state, pageIndex = 0) {
  const pages = getRegulationPanelPages(state);
  const safeIndex = Math.max(
    0,
    Math.min(Number(pageIndex) || 0, pages.length - 1),
  );
  const page = pages[safeIndex] || { title: "", body: "" };
  const modal = new ModalBuilder()
    .setCustomId(`regulamin_modal_page_${state.messageId}_${safeIndex}`)
    .setTitle(`Edytuj stronę ${safeIndex + 1}`);

  const titleInput = new TextInputBuilder()
    .setCustomId("page_title")
    .setLabel(`Tytuł strony ${safeIndex + 1}`)
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(120)
    .setPlaceholder("Np. 5. Reklamacje");

  const bodyInput = new TextInputBuilder()
    .setCustomId("page_body")
    .setLabel(`Treść strony ${safeIndex + 1}`)
    .setStyle(TextInputStyle.Paragraph)
    .setRequired(false)
    .setMaxLength(4000)
    .setPlaceholder("Wpisz całą treść tej strony regulaminu.");

  setTextInputValueIfPresent(titleInput, page.title);
  setTextInputValueIfPresent(bodyInput, page.body);

  modal.addComponents(
    new ActionRowBuilder().addComponents(titleInput),
    new ActionRowBuilder().addComponents(bodyInput),
  );

  return modal;
}

function createDefaultEmbedTestState(
  guild,
  targetChannel,
  ownerId,
  mediaAttachment = null,
) {
  const paymentsChannel = findEmbedTestPaymentsChannel(guild);
  const buyUrl = getDiscordMessageUrl(guild.id, targetChannel.id);
  const paymentsUrl = getDiscordMessageUrl(
    guild.id,
    paymentsChannel?.id || targetChannel.id,
  );
  const normalizedMediaAttachment = normalizeEmbedTestAttachment(mediaAttachment);
  const mediaFile = mediaAttachment
    ? createEmbedTestMediaFileFromAttachment(mediaAttachment)
    : null;

  return {
    ownerId,
    guildId: guild.id,
    channelId: targetChannel.id,
    messageId: null,
    accentColorKey: "blue",
    accentColor: COLOR_BLUE,
    headerBadge: "<:anarchia_gg:1469444521308852324>",
    headerNote: "",
    title: "ANARCHIA LF - CENNIK :jump_dirt:",
    cashSectionTitle: "WALUTA SERWEROWA:",
    cashBody:
      "-# zakupiona kasa wysyłana jest na /gift\n" +
      "### :arrowwhite: :kasa_2:  `8,2k$ ➜ 1 ZŁ`\n\n" +
      "### :arrowwhite: :kasa_2:  `8,5k$ ➜ 1 ZŁ` (powyżej 50zł)",
    itemsSectionTitle: "ITEMY:",
    itemsBody:
      "-# Każdy item przeliczany jest z cennika u góry np. Item o wartości 1MLN = 122zł",
    extraSectionTitle: "",
    extraSectionBody: "",
    extraSectionTwoTitle: "",
    extraSectionTwoBody: "",
    buttonOneLabel: "Kup teraz",
    buttonOneEmoji: "🛒",
    buttonOneAction: "zakup",
    buttonOneUrl: buyUrl,
    buttonTwoLabel: "Płatności",
    buttonTwoEmoji: "💳",
    buttonTwoAction: "link",
    buttonTwoUrl: paymentsUrl,
    buttonThreeLabel: "",
    buttonThreeEmoji: "",
    buttonThreeAction: "kalkulator",
    buttonThreeUrl: null,
    mediaUrls: mediaFile
      ? [`attachment://${mediaFile.name}`]
      : normalizedMediaAttachment
        ? [normalizedMediaAttachment.url]
        : [],
    mediaFiles: mediaFile ? [mediaFile] : [],
  };
}

function buildEmbedTestMessagePayload(state, skipFooter = false) {
  if (isRegulationEmbedState(state)) {
    return buildRegulationPanelMessagePayload(state, skipFooter);
  }

  const buttons = [];
  const headerLines = [];
  const payloadFiles = getEmbedTestPayloadFiles(state);
  const attachedFileNames = new Set(payloadFiles.map((f) => f.name));

  const rawMediaUrls = Array.isArray(state.mediaUrls)
    ? state.mediaUrls.filter((url) => typeof url === "string" && url.trim())
    : [];

  const mediaUrls = rawMediaUrls
    .map((url) => {
      if (url.startsWith("attachment://")) {
        const filename = url.replace("attachment://", "");
        if (attachedFileNames.has(filename)) {
          return url;
        }
        const matchFile = state.mediaFiles?.find(
          (f) => f.name === filename || f.name.includes(filename),
        );
        if (matchFile?.url && matchFile.url.startsWith("http")) {
          return matchFile.url;
        }
        return null;
      }
      return url;
    })
    .filter(Boolean);
  const buttonOneEmoji = parseButtonEmojiInput(
    state.buttonOneEmoji,
    state.guildId,
  );
  const buttonTwoEmoji = parseButtonEmojiInput(
    state.buttonTwoEmoji,
    state.guildId,
  );
  const buttonThreeEmoji = parseButtonEmojiInput(
    state.buttonThreeEmoji,
    state.guildId,
  );
  const cashSectionParts = buildEmbedTestSectionParts(
    state.cashSectionTitle,
    state.cashBody,
    state.guildId,
  );
  const itemsSectionParts = buildEmbedTestSectionParts(
    state.itemsSectionTitle,
    state.itemsBody,
    state.guildId,
  );
  const extraSectionParts = buildEmbedTestSectionParts(
    state.extraSectionTitle,
    state.extraSectionBody,
    state.guildId,
  );
  const extraSectionTwoParts = buildEmbedTestSectionParts(
    state.extraSectionTwoTitle,
    state.extraSectionTwoBody,
    state.guildId,
  );

  const headingParts = [];
  if (state.headerBadge) {
    headingParts.push(replaceNamedGuildEmojis(state.headerBadge, state.guildId));
  }
  if (state.title) {
    headingParts.push(replaceNamedGuildEmojis(state.title, state.guildId));
  }
  if (headingParts.length) {
    headerLines.push(`## ${headingParts.join(" ")}`);
  }

  if (state.headerNote) {
    headerLines.push(replaceNamedGuildEmojis(state.headerNote, state.guildId));
  }

  const createBtn = (label, emoji, action, url) => {
    if (!label) return null;
    if (action === "link" && isHttpUrl(url)) {
      const b = new ButtonBuilder()
        .setLabel(label)
        .setStyle(ButtonStyle.Link)
        .setURL(url);
      if (emoji) b.setEmoji(emoji);
      return b;
    }
    const b = new ButtonBuilder()
      .setLabel(label)
      .setStyle(ButtonStyle.Secondary)
      .setCustomId(`embedtest_buy_open_${action || "zakup"}`);
    if (emoji) b.setEmoji(emoji);
    return b;
  };

  const btn1 = createBtn(state.buttonOneLabel, buttonOneEmoji, state.buttonOneAction, state.buttonOneUrl);
  if (btn1) buttons.push(btn1);

  const btn2 = createBtn(state.buttonTwoLabel, buttonTwoEmoji, state.buttonTwoAction, state.buttonTwoUrl);
  if (btn2) buttons.push(btn2);

  const btn3 = createBtn(state.buttonThreeLabel, buttonThreeEmoji, state.buttonThreeAction, state.buttonThreeUrl);
  if (btn3) buttons.push(btn3);

  const container = new ContainerBuilder().setAccentColor(
    state.accentColor || COLOR_BLUE,
  );

  if (headerLines.length) {
    container.addTextDisplayComponents(
      new TextDisplayBuilder().setContent(headerLines.join("\n")),
    );
  }

  const hasCashSection = appendEmbedTestSectionToContainer(
    container,
    cashSectionParts,
    headerLines.length > 0,
  );

  const hasItemsSection = appendEmbedTestSectionToContainer(
    container,
    itemsSectionParts,
    headerLines.length > 0 || hasCashSection,
  );

  const hasExtraSection = appendEmbedTestSectionToContainer(
    container,
    extraSectionParts,
    headerLines.length > 0 || hasCashSection || hasItemsSection,
  );

  const hasExtraSectionTwo = appendEmbedTestSectionToContainer(
    container,
    extraSectionTwoParts,
    headerLines.length > 0 ||
    hasCashSection ||
    hasItemsSection ||
    hasExtraSection,
  );

  if (mediaUrls.length) {
    if (
      headerLines.length ||
      hasCashSection ||
      hasItemsSection ||
      hasExtraSection ||
      hasExtraSectionTwo
    ) {
      container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
    }

    container.addMediaGalleryComponents(
      new MediaGalleryBuilder().addItems(
        mediaUrls.map((url) => new MediaGalleryItemBuilder().setURL(url)),
      ),
    );
  }

  if (buttons.length) {
    if (container.components.length > 0) {
      container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
    }
    container.addActionRowComponents(
      new ActionRowBuilder().addComponents(...buttons),
    );
  }

  if (state.isModyPanel) {
    container.addActionRowComponents(
      new ActionRowBuilder().addComponents(
        new ButtonBuilder()
          .setCustomId(`mody_videos_${Date.now()}`)
          .setLabel("Nagrania modów")
          .setEmoji("📸")
          .setStyle(ButtonStyle.Secondary),
        new ButtonBuilder()
          .setCustomId(`mody_buy_${Date.now()}`)
          .setLabel("Zakup moda")
          .setEmoji({ id: "1477662159029796865", name: "java" })
          .setStyle(ButtonStyle.Secondary)
      )
    );
  }

  if (!container.components.length) {
    container.addTextDisplayComponents(
      new TextDisplayBuilder().setContent("-# Pusty embed testowy"),
    );
  }

  if (!skipFooter) {
    appendBrandFooterToContainer(container, state.guildId);
  }

  return {
    components: [container],
    files: getEmbedTestPayloadFiles(state),
    flags: MessageFlags.IsComponentsV2,
  };
}

function buildEmbedTestControls(state) {
  const isRegulation = isRegulationEmbedState(state);
  const currentColor = getEmbedTestColorDef(state.accentColorKey);
  const colorSelect = new StringSelectMenuBuilder()
    .setCustomId(`embedtest_color_${state.messageId}`)
    .setPlaceholder(
      `${isRegulation ? "Kolor panelu" : "Kolor embeda"}: ${currentColor.label}`,
    )
    .setMinValues(1)
    .setMaxValues(1)
    .addOptions(
      EMBED_TEST_COLOR_OPTIONS.map((option) => ({
        label: option.label,
        value: option.value,
        description: option.description,
        emoji: option.emoji,
        default: option.value === state.accentColorKey,
      })),
    );

  const rows = [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(`embedtest_edit_header_${state.messageId}`)
        .setLabel("Edytuj górę")
        .setStyle(ButtonStyle.Secondary),
      new ButtonBuilder()
        .setCustomId(`embedtest_edit_content_${state.messageId}`)
        .setLabel(isRegulation ? "Edytuj strony" : "Edytuj treść")
        .setStyle(ButtonStyle.Secondary),
      new ButtonBuilder()
        .setCustomId(`embedtest_edit_buttons_${state.messageId}`)
        .setLabel(isRegulation ? "Przyciski" : "Edytuj przyciski")
        .setStyle(ButtonStyle.Secondary),
      new ButtonBuilder()
        .setCustomId(`embedtest_edit_emojis_${state.messageId}`)
        .setLabel("Emoji")
        .setStyle(ButtonStyle.Secondary),
      new ButtonBuilder()
        .setCustomId(`embedtest_publish_start_${state.messageId}`)
        .setLabel(isRegulation ? "Opublikuj" : "Zakończ")
        .setStyle(ButtonStyle.Success),
    ),
  ];

  if (!isRegulation) {
    rows.push(
      new ActionRowBuilder().addComponents(
        new ButtonBuilder()
          .setCustomId(`embedtest_edit_content_extra_${state.messageId}`)
          .setLabel("Treść 2")
          .setStyle(ButtonStyle.Secondary),
      ),
    );
  }

  rows.push(new ActionRowBuilder().addComponents(colorSelect));
  return rows;
}

function buildEmbedTestControlPayload(state, statusLine) {
  const isRegulation = isRegulationEmbedState(state);
  const jumpUrl = getDiscordMessageUrl(
    state.guildId,
    state.channelId,
    state.messageId,
  );

  const embed = new EmbedBuilder()
    .setColor(COLOR_BLUE)
    .setDescription(
      "```\n" +
      "🧪 New Shop × EMBED TEST\n" +
      "```\n" +
      `> \`✅\` × ${statusLine}\n` +
      `> \`🔗\` × [Otwórz wiadomość](${jumpUrl})\n` +
      "> `🛠️` × Edytuj go przyciskami poniżej\n" +
      "> `🎨` × Kolor zmienisz z menu pod spodem",
    );

  if (isRegulation) {
    embed.setDescription(
      "```\n" +
      "📜 New Shop × REGULAMIN\n" +
      "```\n" +
      `> \`✅\` × ${statusLine}\n` +
      `> \`🔗\` × [Otwórz wiadomość](${jumpUrl})\n` +
      "> `🛠️` × Edytuj panel i strony przyciskami poniżej\n" +
      "> `🎨` × Kolor panelu zmienisz z menu pod spodem",
    );
  }

  return {
    embeds: [embed],
    components: buildEmbedTestControls(state),
  };
}

function buildEmbedTestPublishPrompt(state) {
  const isRegulation = isRegulationEmbedState(state);
  const embed = new EmbedBuilder()
    .setColor(COLOR_BLUE)
    .setDescription(
      "```\n" +
      "📤 New Shop × PUBLIKACJA\n" +
      "```\n" +
      "> `📍` × Wyślij teraz na czacie kanał docelowy\n" +
      "> `✍️` × Przykład: `#‼️×〢anarchia-lf` albo ID kanału\n" +
      "> `⏳` × Masz `2 min` na wysłanie kanału",
    );

  if (isRegulation) {
    embed.setDescription(
      "```\n" +
      "📤 New Shop × PUBLIKACJA REGULAMINU\n" +
      "```\n" +
      "> `📍` × Wyślij teraz na czacie kanał docelowy\n" +
      "> `✍️` × Przykład: `#regulamin` albo ID kanału\n" +
      "> `⏳` × Masz `2 min` na wysłanie kanału",
    );
  }

  return {
    embeds: [embed],
    components: [],
    flags: [MessageFlags.Ephemeral],
  };
}

function buildEmbedTestHeaderModal(state) {
  const isRegulation = isRegulationEmbedState(state);
  const modal = new ModalBuilder()
    .setCustomId(`embedtest_modal_header_${state.messageId}`)
    .setTitle("Edytuj górę embeda");

  const badgeInput = new TextInputBuilder()
    .setCustomId("header_badge")
    .setLabel("Nagłówek górny")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(150)
    .setPlaceholder("np. <:anarchialf:123456789> NEW SHOP × CENNIK");

  const noteInput = new TextInputBuilder()
    .setCustomId("header_note")
    .setLabel("Mały opis pod nagłówkiem")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(180)
    .setPlaceholder("-# Krótki dopisek pod tytułem");
  const titleInput = new TextInputBuilder()
    .setCustomId("panel_title")
    .setLabel("Tytuł panelu")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(120)
    .setPlaceholder("np. NEW SHOP × REGULAMIN");

  if (isRegulation) {
    modal.setTitle("Edytuj górę panelu");
    noteInput
      .setLabel("Opis panelu pod nagłówkiem")
      .setPlaceholder("Krótka instrukcja pod tytułem panelu");
  }

  setTextInputValueIfPresent(badgeInput, state.headerBadge);
  setTextInputValueIfPresent(noteInput, state.headerNote || "");
  if (isRegulation) {
    setTextInputValueIfPresent(titleInput, state.title || "");
  }

  const components = [
    new ActionRowBuilder().addComponents(badgeInput),
    new ActionRowBuilder().addComponents(noteInput),
  ];
  if (isRegulation) {
    components.splice(1, 0, new ActionRowBuilder().addComponents(titleInput));
  }

  modal.addComponents(...components);

  return modal;
}

function buildEmbedTestContentModal(state) {
  const isRegulation = isRegulationEmbedState(state);
  const modal = new ModalBuilder()
    .setCustomId(`embedtest_modal_content_${state.messageId}`)
    .setTitle("Edytuj embed testowy");

  const titleInput = new TextInputBuilder()
    .setCustomId("title")
    .setLabel("Tytuł")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(120)
    .setPlaceholder("np. ANARCHIA LF");

  const cashTitleInput = new TextInputBuilder()
    .setCustomId("cash_section_title")
    .setLabel("Nagłówek sekcji 1")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(80)
    .setPlaceholder("np. KASA");

  const cashBodyInput = new TextInputBuilder()
    .setCustomId("cash_body")
    .setLabel("Treść sekcji 1")
    .setStyle(TextInputStyle.Paragraph)
    .setRequired(false)
    .setMaxLength(1000)
    .setPlaceholder("Możesz używać **pogrubień**, -# opisu i -- separatora");

  const itemsTitleInput = new TextInputBuilder()
    .setCustomId("items_section_title")
    .setLabel("Nagłówek sekcji 2")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(80)
    .setPlaceholder("np. ITEMY");

  const itemsBodyInput = new TextInputBuilder()
    .setCustomId("items_body")
    .setLabel("Treść sekcji 2")
    .setStyle(TextInputStyle.Paragraph)
    .setRequired(false)
    .setMaxLength(1000)
    .setPlaceholder("Wpisz opis, pusty enter lub osobną linię -- na kreskę");

  if (isRegulation) {
    modal.setTitle("Edytuj strony 1-2");
    titleInput.setLabel("Tytuł panelu");
    cashTitleInput.setLabel("Tytuł strony 1");
    cashBodyInput.setLabel("Treść strony 1");
    itemsTitleInput.setLabel("Tytuł strony 2");
    itemsBodyInput.setLabel("Treść strony 2");
  }

  setTextInputValueIfPresent(titleInput, state.title);
  setTextInputValueIfPresent(cashTitleInput, state.cashSectionTitle);
  setTextInputValueIfPresent(cashBodyInput, state.cashBody);
  setTextInputValueIfPresent(itemsTitleInput, state.itemsSectionTitle);
  setTextInputValueIfPresent(itemsBodyInput, state.itemsBody);

  modal.addComponents(
    new ActionRowBuilder().addComponents(titleInput),
    new ActionRowBuilder().addComponents(cashTitleInput),
    new ActionRowBuilder().addComponents(cashBodyInput),
    new ActionRowBuilder().addComponents(itemsTitleInput),
    new ActionRowBuilder().addComponents(itemsBodyInput),
  );

  return modal;
}

function buildEmbedTestExtraContentModal(state) {
  const isRegulation = isRegulationEmbedState(state);
  const modal = new ModalBuilder()
    .setCustomId(`embedtest_modal_content_extra_${state.messageId}`)
    .setTitle("Dodatkowe sekcje");

  const extraTitleInput = new TextInputBuilder()
    .setCustomId("extra_section_title")
    .setLabel("Nagłówek sekcji 3")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(80)
    .setPlaceholder("np. SKUPUJEMY TAKŻE");

  const extraBodyInput = new TextInputBuilder()
    .setCustomId("extra_section_body")
    .setLabel("Treść sekcji 3")
    .setStyle(TextInputStyle.Paragraph)
    .setRequired(false)
    .setMaxLength(1000)
    .setPlaceholder("Tu też działa pusty enter i osobna linia --");

  const extraTwoTitleInput = new TextInputBuilder()
    .setCustomId("extra_section_two_title")
    .setLabel("Nagłówek sekcji 4")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(80)
    .setPlaceholder("np. INFO");

  const extraTwoBodyInput = new TextInputBuilder()
    .setCustomId("extra_section_two_body")
    .setLabel("Treść sekcji 4")
    .setStyle(TextInputStyle.Paragraph)
    .setRequired(false)
    .setMaxLength(1000)
    .setPlaceholder("Możesz robić kolejne bloki i separatory");

  if (isRegulation) {
    modal.setTitle("Edytuj strony 3-4");
    extraTitleInput.setLabel("Tytuł strony 3");
    extraBodyInput.setLabel("Treść strony 3");
    extraTwoTitleInput.setLabel("Tytuł strony 4");
    extraTwoBodyInput.setLabel("Treść strony 4");
  }

  setTextInputValueIfPresent(extraTitleInput, state.extraSectionTitle);
  setTextInputValueIfPresent(extraBodyInput, state.extraSectionBody);
  setTextInputValueIfPresent(extraTwoTitleInput, state.extraSectionTwoTitle);
  setTextInputValueIfPresent(extraTwoBodyInput, state.extraSectionTwoBody);

  modal.addComponents(
    new ActionRowBuilder().addComponents(extraTitleInput),
    new ActionRowBuilder().addComponents(extraBodyInput),
    new ActionRowBuilder().addComponents(extraTwoTitleInput),
    new ActionRowBuilder().addComponents(extraTwoBodyInput),
  );

  return modal;
}

function buildEmbedTestButtonsModal(state) {
  const isRegulation = isRegulationEmbedState(state);
  const modal = new ModalBuilder()
    .setCustomId(`embedtest_modal_buttons_${state.messageId}`)
    .setTitle("Edytuj przyciski");

  const buttonOneLabelInput = new TextInputBuilder()
    .setCustomId("button_one_label")
    .setLabel("Nazwa przycisku 1 (Kup teraz)")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(80)
    .setPlaceholder("np. Kup teraz");

  const buttonTwoLabelInput = new TextInputBuilder()
    .setCustomId("button_two_label")
    .setLabel("Nazwa przycisku 2")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(80)
    .setPlaceholder("np. Płatności / Kalkulator");

  const buttonTwoActionInput = new TextInputBuilder()
    .setCustomId("button_two_action")
    .setLabel("Akcja / Link 2")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(400)
    .setPlaceholder("zostansprzedawca / kalkulator / zakup / https://...");

  const buttonThreeLabelInput = new TextInputBuilder()
    .setCustomId("button_three_label")
    .setLabel("Nazwa przycisku 3")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(80)
    .setPlaceholder("np. Kalkulator / Płatności");

  const buttonThreeActionInput = new TextInputBuilder()
    .setCustomId("button_three_action")
    .setLabel("Akcja / Link 3")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(400)
    .setPlaceholder("zostansprzedawca / kalkulator / zakup / https://...");

  if (isRegulation) {
    modal.setTitle("Edytuj przyciski panelu");
  }

  setTextInputValueIfPresent(buttonOneLabelInput, state.buttonOneLabel);
  setTextInputValueIfPresent(buttonTwoLabelInput, state.buttonTwoLabel);
  setTextInputValueIfPresent(buttonTwoActionInput, state.buttonTwoUrl || state.buttonTwoAction);
  setTextInputValueIfPresent(buttonThreeLabelInput, state.buttonThreeLabel);
  setTextInputValueIfPresent(buttonThreeActionInput, state.buttonThreeUrl || state.buttonThreeAction);

  modal.addComponents(
    new ActionRowBuilder().addComponents(buttonOneLabelInput),
    new ActionRowBuilder().addComponents(buttonTwoLabelInput),
    new ActionRowBuilder().addComponents(buttonTwoActionInput),
    new ActionRowBuilder().addComponents(buttonThreeLabelInput),
    new ActionRowBuilder().addComponents(buttonThreeActionInput),
  );

  return modal;
}

function buildEmbedTestEmojisModal(state) {
  const modal = new ModalBuilder()
    .setCustomId(`embedtest_modal_emojis_${state.messageId}`)
    .setTitle("Edytuj emoji");

  const buttonOneEmojiInput = new TextInputBuilder()
    .setCustomId("button_one_emoji")
    .setLabel("Emoji przycisku 1")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(80)
    .setPlaceholder("np. 💸 lub <:anarchialf:123456789>");

  const buttonTwoEmojiInput = new TextInputBuilder()
    .setCustomId("button_two_emoji")
    .setLabel("Emoji przycisku 2")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(80)
    .setPlaceholder("np. 💳 lub <:donutsmp:123456789>");

  const buttonThreeEmojiInput = new TextInputBuilder()
    .setCustomId("button_three_emoji")
    .setLabel("Emoji przycisku 3")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(80)
    .setPlaceholder("np. 🧮 lub 🔢");

  setTextInputValueIfPresent(buttonOneEmojiInput, state.buttonOneEmoji || "");
  setTextInputValueIfPresent(buttonTwoEmojiInput, state.buttonTwoEmoji || "");
  setTextInputValueIfPresent(buttonThreeEmojiInput, state.buttonThreeEmoji || "");

  modal.addComponents(
    new ActionRowBuilder().addComponents(buttonOneEmojiInput),
    new ActionRowBuilder().addComponents(buttonTwoEmojiInput),
    new ActionRowBuilder().addComponents(buttonThreeEmojiInput),
  );

  return modal;
}

async function updateEmbedTestMessage(state) {
  await ensureEmbedTestEmojiCache(state.guildId);

  const guild = client.guilds.cache.get(state.guildId) || null;
  if (!guild) throw new Error("Nie znaleziono serwera w pamięci bota.");

  const channel = await guild.channels.fetch(state.channelId).catch(() => null);
  if (!channel || !channel.isTextBased()) throw new Error("Nie znaleziono kanału.");

  const message = await channel.messages.fetch(state.messageId).catch(() => null);
  if (!message) throw new Error("Nie znaleziono wiadomości na kanale.");

  const payload = buildEmbedTestMessagePayload(state);
  payload.embeds = [];

  try {
    await message.edit(payload);
  } catch (editErr) {
    console.error("[EmbedTest] Pierwotny message.edit nie powiódł się, próba bez embeds:", editErr);
    delete payload.embeds;
    await message.edit(payload);
  }

  if (isRegulationEmbedState(state) && state.persistPanel) {
    regulationPanels.set(
      state.messageId,
      cloneRegulationPanelState(state, { persistPanel: true }),
    );
    scheduleSavePersistentState(true);
  }

  return true;
}

async function sendEmbedTestToTargetChannel(state, targetChannel) {
  await ensureEmbedTestEmojiCache(state.guildId);

  if (!isEmbedTestPublishTarget(targetChannel)) {
    return null;
  }

  const sentMessage = await targetChannel.send(buildEmbedTestMessagePayload(state));
  delete state.mediaFiles;

  if (isRegulationEmbedState(state)) {
    regulationPanels.set(
      sentMessage.id,
      cloneRegulationPanelState(state, {
        messageId: sentMessage.id,
        channelId: targetChannel.id,
        guildId: targetChannel.guild?.id || state.guildId,
        persistPanel: true,
      }),
    );
    scheduleSavePersistentState(true);
  }

  if (!isRegulationEmbedState(state)) {
    embedTestStates.delete(state.messageId);
  }
  pendingEmbedTestPublish.delete(
    getPendingEmbedTestPublishKey(state.guildId, state.ownerId),
  );
  return sentMessage;
}

async function publishEmbedTestToChannel(interaction, state, targetChannel) {
  if (!isEmbedTestPublishTarget(targetChannel)) {
    await interaction.reply({
      content: "> `❌` × Wybierz poprawny kanał, na który bot może wysłać wiadomość.",
      flags: [MessageFlags.Ephemeral],
    });
    return false;
  }

  try {
    const sentMessage = await sendEmbedTestToTargetChannel(state, targetChannel);
    if (sentMessage) {
      // Zapisujemy stan dla wysłanej wiadomości, aby można było ją później aktualizować
      embedTestStates.set(sentMessage.id, {
        ...state,
        messageId: sentMessage.id,
        channelId: targetChannel.id,
      });
      scheduleSavePersistentState(true);
    }
    if (!sentMessage) {
      await interaction.reply({
        content: "> `❌` × Wybierz poprawny kanał, na który bot może wysłać wiadomość.",
        flags: [MessageFlags.Ephemeral],
      });
      return false;
    }

    const payload = {
      embeds: [
        new EmbedBuilder().setColor(COLOR_BLUE).setDescription(
          "```\n" +
          "✅ New Shop × GOTOWE\n" +
          "```\n" +
          `> \`📤\` × Wysłałem gotową wersję do <#${targetChannel.id}>\n` +
          `> \`🔗\` × [Otwórz wiadomość](${getDiscordMessageUrl(
            interaction.guildId,
            targetChannel.id,
            sentMessage.id,
          )})`,
        ),
      ],
      components: [],
    };

    if (typeof interaction.update === "function" && interaction.isMessageComponent()) {
      await interaction.update(payload);
    } else {
      await interaction.reply({
        ...payload,
        flags: [MessageFlags.Ephemeral],
      });
    }

    return true;
  } catch (error) {
    console.error("embedtest publish failed:", error);
    await interaction.reply({
      content:
        "> `❌` × Nie udało się wysłać gotowej wersji do wybranego kanału. Sprawdź uprawnienia bota.",
      flags: [MessageFlags.Ephemeral],
    });
    return false;
  }
}

async function handleEmbedTestCommand(interaction) {
  if (!interaction.guild) {
    await interaction.reply({
      content: "> `❌` × **Ta komenda** działa tylko na **serwerze**.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  if (interaction.user.id !== interaction.guild.ownerId) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const targetChannel =
    interaction.options.getChannel("kanal") || interaction.channel;
  const mediaAttachment = interaction.options.getAttachment("filmik");

  if (!targetChannel || targetChannel.type !== ChannelType.GuildText) {
    await interaction.reply({
      content: "> `❌` × **Wybierz** poprawny kanał tekstowy.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  if (mediaAttachment && !normalizeEmbedTestAttachment(mediaAttachment)) {
    await interaction.reply({
      content:
        "> `❌` × Załącznik w `/embed-2` musi być filmikiem, gifem albo obrazem.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const state = createDefaultEmbedTestState(
    interaction.guild,
    targetChannel,
    interaction.user.id,
    mediaAttachment,
  );

  try {
    await ensureEmbedTestEmojiCache(interaction.guild.id);
    const sent = await targetChannel.send(buildEmbedTestMessagePayload(state));
    delete state.mediaFiles;
    state.messageId = sent.id;
    embedTestStates.set(sent.id, state);

    await interaction.reply({
      ...buildEmbedTestControlPayload(
        state,
        `Wysłałem testowy embed do <#${targetChannel.id}>`,
      ),
      flags: [MessageFlags.Ephemeral],
    });
  } catch (err) {
    console.error("handleEmbedTestCommand error:", err);
    await interaction.reply({
      content:
        "> `❌` × Nie udało się wysłać testowego embeda. Sprawdź uprawnienia bota do kanału.",
      flags: [MessageFlags.Ephemeral],
    });
  }
}

async function handleRegulaminWyslijCommand(interaction) {
  if (!interaction.guild) {
    await interaction.reply({
      content: "> `❌` × **Ta komenda** działa tylko na **serwerze**.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  if (interaction.user.id !== interaction.guild.ownerId) {
    await interaction.reply({
      content: "> `‼️` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const targetChannel =
    interaction.options.getChannel("kanal") || interaction.channel;
  const mediaAttachment = interaction.options.getAttachment("obrazek");

  if (!targetChannel || targetChannel.type !== ChannelType.GuildText) {
    await interaction.reply({
      content: "> `❌` × **Wybierz** poprawny kanał tekstowy.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  if (mediaAttachment && !normalizeEmbedTestAttachment(mediaAttachment)) {
    await interaction.reply({
      content:
        "> `❌` × Załącznik w `/sprawdz-embed-2` musi być obrazem, gifem albo video.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const state = createDefaultRegulaminState(
    interaction.guild,
    targetChannel,
    interaction.user.id,
    mediaAttachment,
  );

  try {
    await ensureEmbedTestEmojiCache(interaction.guild.id);
    const sent = await targetChannel.send(buildEmbedTestMessagePayload(state));
    delete state.mediaFiles;
    state.messageId = sent.id;
    state.persistPanel = true;
    embedTestStates.set(sent.id, state);
    regulationPanels.set(
      sent.id,
      cloneRegulationPanelState(state, {
        messageId: sent.id,
        channelId: targetChannel.id,
        guildId: interaction.guild.id,
        persistPanel: true,
      }),
    );
    scheduleSavePersistentState(true);

    await interaction.reply({
      ...buildEmbedTestControlPayload(
        state,
        `Wysłałem panel regulaminu do <#${targetChannel.id}>`,
      ),
      flags: [MessageFlags.Ephemeral],
    });
  } catch (error) {
    console.error("handleRegulaminWyslijCommand error:", error);
    await interaction.reply({
      content:
        "> `❌` × Nie udało się wysłać panelu regulaminu. Sprawdź uprawnienia bota do kanału.",
      flags: [MessageFlags.Ephemeral],
    });
  }
}

async function openRegulationPanelViewer(
  interaction,
  panelMessageId,
  pageIndex = 0,
  useUpdate = false,
) {
  const state = getRegulationPanelStateByMessageId(panelMessageId);

  if (!state) {
    const payload = {
      content:
        "> `❌` × Nie mogę już otworzyć tego regulaminu. Wyślij panel jeszcze raz.",
      flags: [MessageFlags.Ephemeral],
    };

    if (useUpdate && typeof interaction.update === "function") {
      await interaction.update({
        embeds: [],
        components: [],
        content:
          "> `❌` × Nie mogę już otworzyć tego regulaminu. Wyślij panel jeszcze raz.",
      });
      return;
    }

    await interaction.reply(payload);
    return;
  }

  const payload = buildRegulationViewerPayload(state, panelMessageId, pageIndex);
  if (useUpdate && typeof interaction.update === "function") {
    await interaction.update(payload);
    return;
  }

  await interaction.reply({
    ...payload,
    flags: MessageFlags.Ephemeral | MessageFlags.IsComponentsV2,
  });
}

function getSerializableMessageComponent(component) {
  if (!component) return null;
  return typeof component.toJSON === "function" ? component.toJSON() : component;
}

function collectEmbedTestMessageData(node, collector) {
  if (!node || typeof node !== "object") return;

  if (
    collector.accentColor === null &&
    typeof node.accent_color === "number"
  ) {
    collector.accentColor = node.accent_color;
  }

  const isSeparatorNode =
    node.type === 14 ||
    (typeof node.divider === "boolean" &&
      !("content" in node) &&
      !("label" in node));

  if (isSeparatorNode) {
    collector.sequence.push({ type: "separator" });
  }

  if (typeof node.content === "string" && node.content.trim()) {
    const sanitized = sanitizeBranding(node.content);
    if (sanitized) {
      collector.texts.push(sanitized);
      collector.sequence.push({ type: "text", content: sanitized });
    }
  }

  if (typeof node.label === "string" && (node.custom_id || node.url)) {
    collector.buttons.push({
      label: node.label,
      customId: node.custom_id || "",
      url: node.url || "",
      emoji: node.emoji || null,
    });
  }

  if (Array.isArray(node.items)) {
    for (const item of node.items) {
      const media = item?.media || item;
      const url = media?.url || item?.url || null;
      if (url) {
        collector.mediaUrls.push(url);
      }
    }
  }

  if (Array.isArray(node.components)) {
    for (const child of node.components) {
      collectEmbedTestMessageData(child, collector);
    }
  }
}

function formatEmbedTestButtonEmojiValue(emojiData) {
  if (!emojiData) return "";
  if (emojiData.id && emojiData.name) {
    return `<${emojiData.animated ? "a" : ""}:${emojiData.name}:${emojiData.id}>`;
  }
  return emojiData.name || "";
}

function resolveEmbedTestColorKeyFromValue(colorValue) {
  if (!Number.isFinite(Number(colorValue))) {
    return EMBED_TEST_COLOR_OPTIONS[0].value;
  }

  const numericColor = Number(colorValue);
  const exactMatch = EMBED_TEST_COLOR_OPTIONS.find(
    (option) => option.color === numericColor,
  );
  if (exactMatch) return exactMatch.value;

  let bestOption = EMBED_TEST_COLOR_OPTIONS[0];
  let bestDistance = Number.POSITIVE_INFINITY;

  for (const option of EMBED_TEST_COLOR_OPTIONS) {
    const distance = Math.abs(option.color - numericColor);
    if (distance < bestDistance) {
      bestDistance = distance;
      bestOption = option;
    }
  }

  return bestOption.value;
}

function splitEmbedTestHeadingParts(content = "") {
  const lines = String(content || "").split(/\r?\n/);
  const headingLine = (lines.shift() || "").replace(/^##\s*/, "").trim();
  const headerNote = lines.join("\n").trim();

  let headerBadge = "";
  let title = headingLine;

  const markupMatch = headingLine.match(/^(<a?:[A-Za-z0-9_]+:\d+>)\s+(.+)$/);
  if (markupMatch) {
    headerBadge = markupMatch[1];
    title = markupMatch[2];
    return { headerBadge, title, headerNote };
  }

  const shortcodeMatch = headingLine.match(/^(:[A-Za-z0-9_]+:)\s+(.+)$/);
  if (shortcodeMatch) {
    headerBadge = shortcodeMatch[1];
    title = shortcodeMatch[2];
  }

  return { headerBadge, title, headerNote };
}

function isEmbedTestSectionTitleBlock(content = "") {
  const trimmed = String(content || "").trim();
  if (!trimmed || trimmed.includes("\n")) return null;

  const titleMatch = trimmed.match(/^\*\*(.+?)\*\*$/s);
  return titleMatch ? titleMatch[1] : null;
}

function tokenizeEmbedTestSectionContent(content = "") {
  const lines = String(content || "").split(/\r?\n/);
  const tokens = [];
  let buffer = [];

  const flushBuffer = () => {
    const joined = buffer.join("\n").trim();
    if (joined) {
      tokens.push({ type: "text", content: joined });
    }
    buffer = [];
  };

  for (const line of lines) {
    const trimmed = line.trim();

    if (trimmed === "--") {
      flushBuffer();
      tokens.push({ type: "separator" });
      continue;
    }

    const inlineTitle = isEmbedTestSectionTitleBlock(trimmed);
    if (inlineTitle) {
      flushBuffer();
      tokens.push({ type: "title", title: inlineTitle });
      continue;
    }

    buffer.push(line);
  }

  flushBuffer();
  return tokens;
}

function joinEmbedTestSectionBodyParts(parts = []) {
  const normalized = [];

  for (const part of parts) {
    if (part === "__SEPARATOR__") {
      if (
        normalized.length &&
        normalized[normalized.length - 1] !== "__SEPARATOR__"
      ) {
        normalized.push(part);
      }
      continue;
    }

    const trimmed = String(part || "").trim();
    if (trimmed) {
      normalized.push(trimmed);
    }
  }

  while (normalized[0] === "__SEPARATOR__") {
    normalized.shift();
  }

  while (normalized[normalized.length - 1] === "__SEPARATOR__") {
    normalized.pop();
  }

  return normalized
    .map((part) => (part === "__SEPARATOR__" ? "--" : part))
    .join("\n\n")
    .trim();
}

function appendSerializedSectionToBody(targetSection, section) {
  if (!targetSection || !section) return;

  const serializedParts = [];
  if (section.title) {
    serializedParts.push(`**${section.title}**`);
  }
  if (section.body) {
    serializedParts.push(section.body);
  }

  const serializedSection = serializedParts.join("\n\n").trim();
  if (!serializedSection) return;

  if (targetSection.body) {
    targetSection.body += "\n\n--\n\n";
  }
  targetSection.body += serializedSection;
}

function reconstructEmbedTestStateFromMessage(message, ownerId) {
  if (!message?.guild || !message.channel) return null;

  const collector = {
    accentColor: null,
    texts: [],
    sequence: [],
    buttons: [],
    mediaUrls: [],
  };

  const componentJson = Array.isArray(message.components)
    ? message.components.map(getSerializableMessageComponent).filter(Boolean)
    : [];

  for (const item of componentJson) {
    collectEmbedTestMessageData(item, collector);
  }

  const state = createDefaultEmbedTestState(
    message.guild,
    message.channel,
    ownerId,
    null,
  );

  state.messageId = message.id;
  state.ownerId = ownerId;
  state.guildId = message.guild.id;
  state.channelId = message.channel.id;

  if (collector.accentColor !== null) {
    const accentColorKey = resolveEmbedTestColorKeyFromValue(collector.accentColor);
    const colorDef = getEmbedTestColorDef(accentColorKey);
    state.accentColorKey = accentColorKey;
    state.accentColor = colorDef.color;
  }

  if (collector.mediaUrls.length) {
    state.mediaUrls = [...new Set(collector.mediaUrls)];
  }

  const messageMediaFiles = getEmbedTestMediaFilesFromMessage(message);
  if (messageMediaFiles.length) {
    applyEmbedTestMediaFilesToState(state, messageMediaFiles);
  }

  const embedTestButtons = collector.buttons.filter(b =>
    String(b.customId || "").startsWith("embedtest_buy_open_") || b.url
  );

  if (embedTestButtons[0]) {
    const b = embedTestButtons[0];
    state.buttonOneLabel = b.label || state.buttonOneLabel;
    state.buttonOneEmoji = formatEmbedTestButtonEmojiValue(b.emoji);
    if (b.url) {
      state.buttonOneAction = "link";
      state.buttonOneUrl = b.url;
    } else {
      const match = String(b.customId).match(/^embedtest_buy_open(?:_(.+))?$/);
      state.buttonOneAction = match?.[1] || "zakup";
      state.buttonOneUrl = null;
    }
  }

  if (embedTestButtons[1]) {
    const b = embedTestButtons[1];
    state.buttonTwoLabel = b.label || state.buttonTwoLabel;
    state.buttonTwoEmoji = formatEmbedTestButtonEmojiValue(b.emoji);
    if (b.url) {
      state.buttonTwoAction = "link";
      state.buttonTwoUrl = b.url;
    } else {
      const match = String(b.customId).match(/^embedtest_buy_open(?:_(.+))?$/);
      state.buttonTwoAction = match?.[1] || "zakup";
      state.buttonTwoUrl = null;
    }
  }

  if (embedTestButtons[2]) {
    const b = embedTestButtons[2];
    state.buttonThreeLabel = b.label || state.buttonThreeLabel;
    state.buttonThreeEmoji = formatEmbedTestButtonEmojiValue(b.emoji);
    if (b.url) {
      state.buttonThreeAction = "link";
      state.buttonThreeUrl = b.url;
    } else {
      const match = String(b.customId).match(/^embedtest_buy_open(?:_(.+))?$/);
      state.buttonThreeAction = match?.[1] || "zakup";
      state.buttonThreeUrl = null;
    }
  }

  const isRegulationPanel = state.buttonOneAction === "regulamin";

  const sequence = [];
  for (const token of collector.sequence) {
    if (!token) continue;

    if (token.type === "separator") {
      sequence.push(token);
      continue;
    }

    if (token.type === "text") {
      sequence.push(...tokenizeEmbedTestSectionContent(token.content));
    }
  }

  if (sequence.length && sequence[0]?.type === "text") {
    const firstTextBlock = String(sequence[0].content || "").trim();
    if (firstTextBlock.startsWith("## ") || isRegulationPanel) {
      const heading = splitEmbedTestHeadingParts(sequence.shift().content);
      state.headerBadge = heading.headerBadge || state.headerBadge;
      state.title = heading.title || state.title;
      state.headerNote = heading.headerNote || state.headerNote;

      if (isRegulationPanel && sequence[0]?.type === "text") {
        const possibleHeaderNote = String(sequence[0].content || "").trim();
        const inlineTitle = isEmbedTestSectionTitleBlock(possibleHeaderNote);
        if (
          possibleHeaderNote &&
          !inlineTitle &&
          !possibleHeaderNote.startsWith("## ")
        ) {
          state.headerNote = possibleHeaderNote;
          sequence.shift();
        }
      }
    }
  }

  const sections = [];
  let currentSection = null;

  const pushCurrentSection = () => {
    if (!currentSection) return;
    currentSection.body = joinEmbedTestSectionBodyParts(currentSection.bodyParts);
    delete currentSection.bodyParts;
    if (currentSection.title || currentSection.body) {
      sections.push(currentSection);
    }
    currentSection = null;
  };

  const getNextTextToken = (startIndex) => {
    for (let i = startIndex; i < sequence.length; i += 1) {
      if (sequence[i]?.type === "text" && String(sequence[i].content || "").trim()) {
        return sequence[i];
      }
    }
    return null;
  };

  for (let index = 0; index < sequence.length; index += 1) {
    const token = sequence[index];
    if (!token) continue;

    if (token.type === "separator") {
      const nextTextToken = getNextTextToken(index + 1);
      const nextTitle =
        nextTextToken?.type === "title"
          ? nextTextToken.title
          : nextTextToken?.type === "text"
            ? isEmbedTestSectionTitleBlock(nextTextToken.content)
            : null;

      if (
        nextTitle &&
        currentSection &&
        (currentSection.title || currentSection.bodyParts.length)
      ) {
        pushCurrentSection();
        continue;
      }

      if (currentSection && currentSection.bodyParts.length) {
        currentSection.bodyParts.push("__SEPARATOR__");
      }
      continue;
    }

    if (token.type === "title") {
      pushCurrentSection();
      currentSection = {
        title: token.title,
        bodyParts: [],
      };
      continue;
    }

    const block = token.content;
    const trimmed = String(block || "").trim();
    if (!trimmed) continue;

    if (!currentSection) {
      currentSection = {
        title: "",
        bodyParts: [],
      };
    }

    currentSection.bodyParts.push(block);
  }

  pushCurrentSection();

  if (isRegulationPanel && sections.length) {
    setRegulationPagesOnState(
      state,
      sections.map((section) => ({
        title: section?.title || "",
        body: section?.body || "",
      })),
    );
  }

  const cashSection = sections[0] || null;
  const itemsSection = sections[1] || null;
  const extraSection = sections[2] || null;
  const extraSectionTwo = sections[3]
    ? {
      title: sections[3].title || "",
      body: sections[3].body || "",
    }
    : null;

  if (sections.length > 4 && extraSectionTwo) {
    for (const overflowSection of sections.slice(4)) {
      appendSerializedSectionToBody(extraSectionTwo, overflowSection);
    }
  }

  state.cashSectionTitle = "";
  state.cashBody = "";
  state.itemsSectionTitle = "";
  state.itemsBody = "";
  state.extraSectionTitle = "";
  state.extraSectionBody = "";
  state.extraSectionTwoTitle = "";
  state.extraSectionTwoBody = "";

  if (sections.length === 0) {
    // nothing to do
  } else if (sections.length === 1) {
    // Single section — simple case
    state.cashSectionTitle = sections[0].title || "";
    state.cashBody = sections[0].body || "";
  } else {
    // Multiple sections — check if they have distinct titles
    // If they do, keep them separated (user intentionally structured it)
    // If they don't have titles, merge all into cashBody to avoid ghost sections
    const allHaveTitles = sections.every(s => s.title);
    if (allHaveTitles && sections.length <= 4) {
      if (sections[0]) { state.cashSectionTitle = sections[0].title || ""; state.cashBody = sections[0].body || ""; }
      if (sections[1]) { state.itemsSectionTitle = sections[1].title || ""; state.itemsBody = sections[1].body || ""; }
      if (sections[2]) { state.extraSectionTitle = sections[2].title || ""; state.extraSectionBody = sections[2].body || ""; }
      if (sections[3]) { state.extraSectionTwoTitle = sections[3].title || ""; state.extraSectionTwoBody = sections[3].body || ""; }
    } else {
      // Merge all sections into cashBody with separators
      const merged = sections
        .map(s => [s.title ? `### ${s.title}` : null, s.body].filter(Boolean).join("\n"))
        .join("\n\n--\n\n");
      state.cashSectionTitle = sections[0]?.title || "";
      state.cashBody = merged;
    }
  }

  // Usunięto starą logikę secondaryButton na rzecz zunifikowanej powyżej

  if (isRegulationPanel) {
    return cloneRegulationPanelState(state, {
      ownerId,
      guildId: message.guild.id,
      channelId: message.channel.id,
      messageId: message.id,
      persistPanel: true,
    });
  }

  return state;
}

function getPanelComponentDump(message) {
  const componentJson = Array.isArray(message?.components)
    ? message.components.map(getSerializableMessageComponent).filter(Boolean)
    : [];
  return JSON.stringify(componentJson);
}

function isRegulationPanelMessage(message) {
  return getPanelComponentDump(message).includes("embedtest_buy_open_regulamin");
}

function isLegacyModyPanelMessage(message) {
  const componentDump = getPanelComponentDump(message);
  return (
    message?.author?.id === client.user?.id &&
    message.embeds?.length > 0 &&
    componentDump.includes("mody_videos_") &&
    componentDump.includes("mody_buy_")
  );
}

async function findLatestEmbedTestMessage(channel) {
  if (!channel?.isTextBased?.()) return null;

  try {
    const fetched = await channel.messages.fetch({ limit: 100 });
    for (const message of fetched.values()) {
      if (message.author?.id !== client.user?.id) continue;

      if (regulationPanels.has(message.id) || embedTestStates.has(message.id)) {
        return message;
      }

      const componentDump = getPanelComponentDump(message);

      if (
        componentDump.includes("embedtest_") ||
        componentDump.includes("embedtest_buy_open_") ||
        componentDump.includes("\"Zakup\"") ||
        componentDump.includes("\"Metody płatności\"") ||
        componentDump.includes("\"Kup teraz\"") ||
        componentDump.includes("\"Płatności\"") ||
        componentDump.includes("\"Zobacz regulamin\"") ||
        (message.flags?.has && message.flags.has(MessageFlags.IsComponentsV2)) ||
        (message.embeds && message.embeds.length > 0)
      ) {
        return message;
      }
    }
  } catch (_error) {
    return null;
  }

  return null;
}

async function findLatestLegacyModyPanelMessage(channel) {
  if (!channel?.isTextBased?.()) return null;

  try {
    const fetched = await channel.messages.fetch({ limit: 100 });
    for (const message of fetched.values()) {
      if (isLegacyModyPanelMessage(message)) {
        return message;
      }
    }
  } catch (_error) {
    return null;
  }

  return null;
}

async function handleSprawdzEmbedTestCommand(interaction) {
  if (!interaction.guild) {
    await interaction.reply({
      content: "> `❌` × **Ta komenda** działa tylko na **serwerze**.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  if (!isAdminOrSeller(interaction.member)) {
    await interaction.reply({
      content: "> `‼️` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const targetChannel =
    interaction.options.getChannel("kanal") || interaction.channel;

  if (!targetChannel || targetChannel.type !== ChannelType.GuildText) {
    await interaction.reply({
      content: "> `❌` × **Wybierz** poprawny kanał tekstowy.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const foundMessage = await findLatestEmbedTestMessage(targetChannel);

  if (!foundMessage) {
    await interaction.reply({
      content:
        "> `❌` × Nie znalazłem na tym kanale żadnego aktywnego embeda testowego od bota.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const liveState = embedTestStates.get(foundMessage.id) || null;
  const storedRegulationState = regulationPanels.has(foundMessage.id)
    ? cloneRegulationPanelState(regulationPanels.get(foundMessage.id), {
      ownerId: interaction.user.id,
      guildId: interaction.guild.id,
      channelId: targetChannel.id,
      messageId: foundMessage.id,
      persistPanel: true,
    })
    : null;

  if (!liveState && !storedRegulationState && isRegulationPanelMessage(foundMessage)) {
    await interaction.reply({
      content:
        "> `❌` × Znalazłem panel regulaminu, ale bot nie ma zapisanego stanu jego stron. Podepnij go ponownie przez `/sprawdz-embed-2` albo otwórz aktywną sesję edycji.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const reconstructedState =
    liveState || storedRegulationState
      ? null
      : reconstructEmbedTestStateFromMessage(foundMessage, interaction.user.id);
  const existingState = liveState || storedRegulationState || reconstructedState || null;

  if (!existingState) {
    await interaction.reply({
      content:
        "> `❌` × Znalazłem wiadomość, ale nie udało mi się podpiąć jej pod edytor.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  existingState.messageId = foundMessage.id;
  existingState.ownerId = interaction.user.id;
  existingState.guildId = interaction.guild.id;
  existingState.channelId = targetChannel.id;
  embedTestStates.set(foundMessage.id, existingState);

  await interaction.reply({
    ...buildEmbedTestControlPayload(
      existingState,
      isRegulationEmbedState(existingState)
        ? `Podpiąłem istniejący panel regulaminu z <#${targetChannel.id}>`
        : `Podpiąłem istniejący embed testowy z <#${targetChannel.id}>`,
    ),
    flags: [MessageFlags.Ephemeral],
  });
}

function getEditableEmbedTestStateFromMessage(message, ownerId, targetChannel) {
  const liveState = embedTestStates.get(message.id) || null;
  const storedRegulationState = regulationPanels.has(message.id)
    ? cloneRegulationPanelState(regulationPanels.get(message.id), {
      ownerId,
      guildId: message.guild.id,
      channelId: targetChannel.id,
      messageId: message.id,
      persistPanel: true,
    })
    : null;

  if (!liveState && !storedRegulationState && isRegulationPanelMessage(message)) {
    return null;
  }

  const reconstructedState =
    liveState || storedRegulationState
      ? null
      : reconstructEmbedTestStateFromMessage(message, ownerId);

  const state = liveState || storedRegulationState || reconstructedState || null;
  if (!state) return null;

  state.messageId = message.id;
  state.ownerId = ownerId;
  state.guildId = message.guild.id;
  state.channelId = targetChannel.id;
  return state;
}

async function findEmbedTestStateForOwnerCommand(interaction, targetChannel) {
  const foundMessage = await findLatestEmbedTestMessage(targetChannel);
  if (!foundMessage) {
    return { message: null, state: null };
  }

  const state = getEditableEmbedTestStateFromMessage(
    foundMessage,
    interaction.user.id,
    targetChannel,
  );

  return { message: foundMessage, state };
}

async function resendLegacyModyPanelMessage(message, targetChannel) {
  const embeds = message.embeds.map((embed) => EmbedBuilder.from(embed));
  const components = message.components
    .map((component) =>
      typeof component.toJSON === "function"
        ? component.toJSON()
        : getSerializableMessageComponent(component),
    )
    .filter(Boolean);
  const files = message.attachments?.size
    ? message.attachments.map((attachment) => ({
      attachment: attachment.url,
      name: attachment.name || "zalacznik",
    }))
    : [];

  const sent = await targetChannel.send({
    content: message.content || undefined,
    embeds,
    components,
    files: files.length ? files : undefined,
    allowedMentions: { parse: [] },
  });

  await message.delete().catch(() => null);
  return sent;
}

async function handleZaaktualizujFilmCommand(interaction) {
  if (!interaction.guild) {
    await interaction.reply({
      content: "> `❌` × **Ta komenda** działa tylko na **serwerze**.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  if (!isAdminOrSeller(interaction.member)) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const targetChannel =
    interaction.options.getChannel("kanal") || interaction.channel;

  if (!targetChannel || targetChannel.type !== ChannelType.GuildText) {
    await interaction.reply({
      content: "> `❌` × **Wybierz** poprawny kanał tekstowy.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const { message, state } = await findEmbedTestStateForOwnerCommand(
    interaction,
    targetChannel,
  );

  if (!message || !state) {
    await interaction.reply({
      content:
        "> `❌` × Nie znalazłem aktywnego embedtestu na tym kanale albo nie da się go odtworzyć.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  await interaction.reply({
    content:
      `> \`🎬\` × Znalazłem embedtest: ${getDiscordMessageUrl(
        interaction.guildId,
        targetChannel.id,
        message.id,
      )}\n` +
      "> `📎` × Wyślij teraz **jeden filmik / gif / obraz** na tym kanale. Podmienię nim stary plik w embedzie.",
    flags: [MessageFlags.Ephemeral],
  });

  const filter = (msg) =>
    msg.author?.id === interaction.user.id &&
    msg.channelId === interaction.channelId &&
    msg.attachments?.size > 0;

  const collected = await interaction.channel
    .awaitMessages({ filter, max: 1, time: 120_000, errors: ["time"] })
    .catch(() => null);

  const uploadMessage = collected?.first?.() || null;
  const attachment = uploadMessage?.attachments?.find((att) =>
    !!normalizeEmbedTestAttachment(att),
  );

  if (!attachment) {
    await interaction.followUp({
      content: "> `❌` × Nie dostałem poprawnego filmu, gifa ani obrazu w ciągu 2 minut.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const mediaFile = createEmbedTestMediaFileFromAttachment(attachment);
  applyEmbedTestMediaFilesToState(state, [mediaFile]);
  embedTestStates.set(message.id, state);

  const payload = buildEmbedTestMessagePayload(state);
  await message.edit({ ...payload, attachments: [] });
  delete state.mediaFiles;

  await interaction.followUp({
    content:
      "> `✅` × Podmieniłem film/obraz w embedteście. Nie usuwaj wiadomości z wysłanym plikiem, dopóki Discord nie przetworzy załącznika.",
    flags: [MessageFlags.Ephemeral],
  });
}

async function handleAktualizacjaEmbedCommand(interaction) {
  if (!interaction.guild) {
    await interaction.reply({
      content: "> `❌` × **Ta komenda** działa tylko na **serwerze**.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  if (interaction.user.id !== interaction.guild.ownerId) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const targetChannel =
    interaction.options.getChannel("kanal") || interaction.channel;

  if (!targetChannel || targetChannel.type !== ChannelType.GuildText) {
    await interaction.reply({
      content: "> `❌` × **Wybierz** poprawny kanał tekstowy.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // 1. Szukamy celu (Target): najpierw embedtest, potem jakikolwiek ostatni embed bota na kanale
  let targetMessage = await findLatestEmbedTestMessage(targetChannel);
  if (!targetMessage) {
    try {
      const fetched = await targetChannel.messages.fetch({ limit: 50 });
      targetMessage = fetched.find(m => m.author.id === client.user.id && (m.embeds.length > 0 || m.flags.has(MessageFlags.IsComponentsV2))) || null;
    } catch (e) { }
  }

  if (!targetMessage) {
    await interaction.reply({
      content: "> `❌` × Nie znalazłem żadnego embeda bota do zaktualizowania na tym kanale.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // 2. Szukamy źródła (Source): stan skojarzony z wiadomością LUB importujemy ze starej wiadomości
  let state = embedTestStates.get(targetMessage.id) || regulationPanels.get(targetMessage.id);

  if (state) {
    state.headerNote = sanitizeBranding(state.headerNote);
    state.cashBody = sanitizeBranding(state.cashBody);
    state.itemsBody = sanitizeBranding(state.itemsBody);
    state.extraSectionBody = sanitizeBranding(state.extraSectionBody);
    state.extraSectionTwoBody = sanitizeBranding(state.extraSectionTwoBody);
    if (Array.isArray(state.pages)) {
      state.pages = state.pages.map(p => ({
        ...p,
        body: sanitizeBranding(p.body)
      }));
    }
  }

  if (!state) {
    // Próba importu z istniejącego embeda
    if (targetMessage.flags.has(MessageFlags.IsComponentsV2)) {
      state = reconstructEmbedTestStateFromMessage(targetMessage, interaction.user.id);
      if (state) {
        state.headerNote = sanitizeBranding(state.headerNote);
        state.cashBody = sanitizeBranding(state.cashBody);
        state.itemsBody = sanitizeBranding(state.itemsBody);
        state.extraSectionBody = sanitizeBranding(state.extraSectionBody);
        state.extraSectionTwoBody = sanitizeBranding(state.extraSectionTwoBody);
      }
    } else {
      const embed = targetMessage.embeds[0];
      if (embed) {
        state = {
          guildId: interaction.guildId,
          ownerId: interaction.user.id,
          title: embed.title || "",
          // Importujemy opis do sekcji cashBody, bo description nie jest bezpośrednio renderowane
          // Czyścimy branding, aby nie było duplikatów stopki
          cashBody: sanitizeBranding(embed.description || ""),
          accentColor: embed.color || COLOR_BLUE,
          thumbnail: embed.thumbnail?.url || null,
          image: embed.image?.url || null,
          footer: sanitizeBranding(embed.footer?.text || ""),
          footerIcon: embed.footer?.iconURL || null,
          messageId: targetMessage.id,
          channelId: targetChannel.id
        };
      }
    }
  }

  // Jeśli nadal brak stanu, szukamy najnowszego aktywnego draftu użytkownika (fallback)
  if (!state) {
    let latestDraft = null;
    for (const s of embedTestStates.values()) {
      if (s.ownerId === interaction.user.id) {
        latestDraft = s;
      }
    }
    if (latestDraft) {
      state = { ...latestDraft };
    }
  }

  if (!state) {
    await interaction.reply({
      content: "> `❌` × Nie znalazłem zapisanego stanu dla tej wiadomości ani treści do zaimportowania. \n> `💡` × Jeśli chcesz nadpisać ten panel nową treścią, użyj najpierw `/embed-2`, stwórz podgląd, a potem tutaj `/aktualizacja-embed` (ale upewnij się, że masz tylko jeden aktywny podgląd).",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // Zachowaj media ze starej wiadomości jeśli istnieją
  const mediaFiles = getEmbedTestMediaFilesFromMessage(targetMessage);
  if (mediaFiles.length) {
    applyEmbedTestMediaFilesToState(state, mediaFiles);
  }

  // 3. Budujemy payload z wybranego stanu (bez stopki na razie, dodamy ją na końcu)
  state.isModyPanel = targetChannel.name.toLowerCase().includes("mody");
  const payload = buildEmbedTestMessagePayload(state, true);

  // 4. Przenosimy funkcjonalne przyciski ze starej wiadomości DO ŚRODKA embeda
  const originalButtons = [];
  if (targetMessage.components && targetMessage.components.length > 0) {
    targetMessage.components.forEach(row => {
      const comps = row.components || (row.type === 1 ? row.components : [row]);
      comps.forEach(comp => {
        if (comp.type === 2 || comp.type === "BUTTON") {
          const cid = comp.customId || "";
          if (!cid.startsWith("embedtest_")) {
            originalButtons.push(ButtonBuilder.from(comp));
          }
        }
      });
    });
  }

  if (originalButtons.length > 0) {
    const container = payload.components[0];
    if (container && typeof container.addActionRowComponents === "function") {
      for (let i = 0; i < originalButtons.length; i += 5) {
        const chunk = originalButtons.slice(i, i + 5);
        container.addActionRowComponents(new ActionRowBuilder().addComponents(...chunk));
      }
    }
  }

  // DODAJ STOPKĘ NA SAMYM KOŃCU (Zawsze pod wszystkimi przyciskami)
  const container = payload.components[0];
  if (container) {
    appendBrandFooterToContainer(container, state.guildId);
  }

  // 5. Wyślij nowe, usuń stare, zaktualizuj stan
  const sent = await targetChannel.send(payload);
  delete state.mediaFiles;

  state.messageId = sent.id;
  state.channelId = targetChannel.id;
  embedTestStates.delete(targetMessage.id);
  embedTestStates.set(sent.id, state);

  if (isRegulationEmbedState(state) || regulationPanels.has(targetMessage.id)) {
    regulationPanels.delete(targetMessage.id);
    regulationPanels.set(
      sent.id,
      cloneRegulationPanelState(state, {
        messageId: sent.id,
        channelId: targetChannel.id,
        guildId: interaction.guild.id,
        persistPanel: true,
      }),
    );
    scheduleSavePersistentState(true);
  }

  await targetMessage.delete().catch(() => null);

  await interaction.reply({
    content: `> \`✅\` × Zaktualizowałem embed na wzór embedtest (przyciski przeniesione do środka): ${getDiscordMessageUrl(interaction.guildId, targetChannel.id, sent.id)}`,
    flags: [MessageFlags.Ephemeral],
  });
}

const PANEL_FONT_MAP = {
  a: "ᴀ", b: "ʙ", c: "ᴄ", d: "ᴅ", e: "ᴇ", f: "ꜰ", g: "ɢ", h: "ʜ", i: "ɪ", j: "ᴊ", k: "ᴋ", l: "ʟ", m: "ᴍ", n: "ɴ", o: "ᴏ", p: "ᴘ", q: "ǫ", r: "ʀ", s: "ꜱ", t: "ᴛ", u: "ᴜ", v: "ᴠ", w: "ᴡ", x: "x", y: "ʏ", z: "ᴢ",
  A: "ᴀ", B: "ʙ", C: "ᴄ", D: "ᴅ", E: "ᴇ", F: "ꜰ", G: "ɢ", H: "ʜ", I: "ɪ", J: "ᴊ", K: "ᴋ", L: "ʟ", M: "ᴍ", N: "ɴ", O: "ᴏ", P: "ᴘ", Q: "ǫ", R: "ʀ", S: "ꜱ", T: "ᴛ", U: "ᴜ", V: "ᴠ", W: "ᴡ", X: "x", Y: "ʏ", Z: "ᴢ",
  ł: "ᴌ", Ł: "ᴌ", ś: "ś", Ś: "ś", ć: "ć", Ć: "ć", ń: "ń", Ń: "ń", ó: "ó", Ó: "ó", ź: "ź", Ź: "ź", ż: "ż", Ż: "ż", ą: "ą", Ą: "ą", ę: "ę", Ę: "ę"
};

function toPanelFont(text = "") {
  return String(text)
    .split("")
    .map((char) => PANEL_FONT_MAP[char] || char)
    .join("");
}

const TEST_PANEL_CATEGORY_OPTIONS = [
  {
    label: "Kupno itemów",
    value: "zakup",
    description: "Testowy formularz zakupu itemów",
  },
];

const TICKET_OTHER_OPTION_EMOJI = {
  id: "1491446746239336448",
  name: "question",
};

const TICKET_OTHER_SERVER_OPTION = {
  label: toPanelFont("INNE"),
  value: "inne",
  description: "Inny serwer",
  emoji: TICKET_OTHER_OPTION_EMOJI,
};

const TICKET_OTHER_PAYMENT_OPTION = {
  label: toPanelFont("INNE"),
  value: "inne",
  description: "Inna forma płatności",
  emoji: TICKET_OTHER_OPTION_EMOJI,
};

const TICKET_OTHER_PAYOUT_OPTION = {
  label: toPanelFont("INNE"),
  value: "inne",
  description: "Inna forma wypłaty",
  emoji: TICKET_OTHER_OPTION_EMOJI,
};

const SHOP_SERVER_OPTION_DEFS = [
  {
    label: "Anarchia LF",
    testValue: "anarchia_lf",
    calcValue: "ANARCHIA_LIFESTEAL",
    description: "Tryb Anarchia LifeSteal na Anarchii",
    channelSlug: "anarchia-lf",
    emoji: { id: "1469444521308852324", name: "ANARCHIA_GG" },
  },
  {
    label: "Anarchia BoxPvP",
    testValue: "anarchia_boxpvp",
    calcValue: "ANARCHIA_BOXPVP",
    description: "Tryb BoxPvP na Anarchii",
    channelSlug: "anarchia-boxpvp",
    emoji: { id: "1469444521308852324", name: "ANARCHIA_GG" },
  },
  {
    label: "MineStar LF",
    testValue: "minestar_lf",
    calcValue: "MINESTAR_LF",
    description: "Tryb LifeSteal na MineStar",
    channelSlug: "minestar-lf",
    emoji: { id: "1515321503770607777", name: "minestarlf" },
  },
  {
    label: "MineStar SkyPvP",
    testValue: "minestar_skypvp",
    calcValue: "MINESTAR_SKY",
    description: "Tryb SkyPvP na MineStar",
    channelSlug: "minestar-skypvp",
    emoji: { id: "1515321503770607777", name: "minestarlf" },
  },
  {
    label: "Donut SMP",
    testValue: "donut_smp",
    calcValue: "DONUT_SMP",
    description: "Tryb SMP na Donut",
    channelSlug: "donut-smp",
    emoji: { id: "1489578418432381059", name: "donutsmp" },
  },
];

const SHOP_PAYMENT_OPTION_DEFS = [
  {
    label: "BLIK",
    testValue: "blik",
    calcValue: "BLIK",
    description: "Szybki przelew BLIK (0% prowizji)",
    channelSlug: "blik",
    emoji: { id: "1537186968222433361", name: "blik_2" },
  },
  {
    label: "Kod BLIK",
    testValue: "kod_blik",
    calcValue: "Kod BLIK",
    description: "Kod BLIK (10% prowizji)",
    channelSlug: "kod-blik",
    emoji: { id: "1537186968222433361", name: "blik_2" },
  },
  {
    label: "PSC",
    testValue: "psc",
    calcValue: "PSC",
    description: "Paysafecard (10% prowizji)",
    channelSlug: "psc",
    emoji: { id: "1469107238676467940", name: "PSC" },
  },
  {
    label: "PSC bez paragonu",
    testValue: "psc_bez_paragonu",
    calcValue: "PSC bez paragonu",
    description: "Paysafecard (20% prowizji)",
    channelSlug: "psc-bez-paragonu",
    emoji: { id: "1469107238676467940", name: "PSC" },
  },
  {
    label: "MYPSC",
    testValue: "mypsc",
    calcValue: "MYPSC",
    description: "MYPSC (20% lub min 10zł)",
    channelSlug: "mypsc",
    emoji: { id: "1469107199350669473", name: "MYPSC" },
  },
  {
    label: "PayPal",
    testValue: "paypal",
    calcValue: "PayPal",
    description: "PayPal (10% prowizji)",
    channelSlug: "paypal",
    emoji: { id: "1449354427755659444", name: "PAYPAL" },
  },
  {
    label: "LTC",
    testValue: "ltc",
    calcValue: "LTC",
    description: "Litecoin (10% prowizji)",
    channelSlug: "ltc",
    emoji: { id: "1449186363101548677", name: "LTC" },
  },
];

const AUTORYNEK_EXTRA_PAYMENT_OPTION_DEFS = [
  {
    label: "Zaproszenia",
    testValue: "zaproszenia",
    description: "Płatność zaproszeniami",
    channelSlug: "zaproszenia",
    emoji: "📩",
  },
  {
    label: "Waluta Serwerowa 300k$",
    testValue: "waluta_serwerowa_300k",
    description: "Płatność walutą serwerową 300k$",
    channelSlug: "waluta-serwerowa-300k",
    emoji: { id: "1476700165082710178", name: "kasa_2" },
  },
];

const KALKULATOR_MODE_OPTIONS = [
  {
    label: toPanelFont("Ile otrzymam"),
    value: "otrzymam",
    description: "Podasz kwotę w PLN i zobaczysz ile waluty dostaniesz",
    emoji: { id: "1476700165082710178", name: "kasa_2" },
  },
  {
    label: toPanelFont("Ile muszę dać"),
    value: "muszedac",
    description: "Podasz ilość waluty i zobaczysz, ile musisz za nią zapłacić",
    emoji: { id: "1476700165082710178", name: "kasa_2" },
  },
];

const PANEL_CATEGORY_OPTIONS = [
  {
    label: "ᴢᴀᴋᴜᴘ ɪᴛᴇᴍóᴡ",
    value: "zakup",
    description: "Kliknij, aby kupić itemy!",
    emoji: "🛒",
  },
  {
    label: "ꜱᴘʀᴢᴇᴅᴀż",
    value: "sprzedaz",
    description: "Kliknij, aby sprzedać przedmioty!",
    emoji: { id: "1476700165082710178", name: "kasa_2" },
  },
  {
    label: "ᴢᴀᴋᴜᴘ ᴍᴏᴅᴀ",
    value: "zakup_moda",
    description: "Kliknij, aby kupić autorskiego moda!",
    emoji: { id: "1480590181944791122", name: "autorynek" },
  },
  {
    label: "ᴢᴀᴋᴜᴘ ʙᴏᴛóᴡ",
    value: "zakup_autorynku",
    description: "Kliknij, aby kupić najlepsze boty!",
    emoji: { id: "1480590181944791122", name: "autorynek" },
  },
  {
    label: "ᴏᴅʙɪᴇʀᴢ ɴᴀɢʀᴏᴅᴇ",
    value: "odbior",
    description: "Kliknij, aby odebrać nagrodę, którą zdobyłeś!",
    emoji: { id: "1480590229697069210", name: "nagroda" },
  },
  {
    label: "ᴘᴏᴍᴏᴄ",
    value: "inne",
    description: "Kliknij, aby zadać pytanie lub otrzymać pomoc!",
    emoji: { id: "1477688955221835807", name: "pytanie", animated: true },
  },
];



const TEST_PANEL_SERVER_OPTIONS = [
  ...SHOP_SERVER_OPTION_DEFS.map((option) => ({
    label: toPanelFont(option.label),
    value: option.testValue,
    description: option.description,
    emoji: option.emoji,
  })),
  TICKET_OTHER_SERVER_OPTION,
];

const TEST_PANEL_PAYMENT_OPTIONS = [
  ...SHOP_PAYMENT_OPTION_DEFS.map((option) => ({
    label: toPanelFont(option.label),
    value: option.testValue,
    description: option.description,
    emoji: option.emoji,
  })),
  TICKET_OTHER_PAYMENT_OPTION,
];

const KALKULATOR_SERVER_OPTIONS = SHOP_SERVER_OPTION_DEFS.map((option) => ({
  label: toPanelFont(option.label),
  value: option.calcValue,
  description: option.description,
  emoji: option.emoji,
}));

const KALKULATOR_PAYMENT_OPTIONS = SHOP_PAYMENT_OPTION_DEFS.map((option) => ({
  label: toPanelFont(option.label),
  value: option.calcValue,
  description: option.description,
  emoji: option.emoji,
}));

const SIMPLE_PAYMENT_OPTIONS = [
  ...SHOP_PAYMENT_OPTION_DEFS.map((option) => ({
    label: toPanelFont(option.label),
    value: option.testValue,
    description: `Płatność ${option.label}`,
    emoji: option.emoji,
  })),
  TICKET_OTHER_PAYMENT_OPTION,
];

const AUTORYNEK_PAYMENT_OPTIONS = [
  ...SIMPLE_PAYMENT_OPTIONS,
  ...AUTORYNEK_EXTRA_PAYMENT_OPTION_DEFS.map((option) => ({
    label: toPanelFont(option.label),
    value: option.testValue,
    description: option.description,
    emoji: option.emoji,
  })),
];

const PAYOUT_OPTIONS = [
  ...SHOP_PAYMENT_OPTION_DEFS.map((option) => ({
    label: toPanelFont(option.label),
    value: option.testValue,
    description: `Wypłata ${option.label}`,
    emoji: option.emoji,
  })),
  TICKET_OTHER_PAYOUT_OPTION,
];

const MOD_COUNT_OPTIONS = [
  { label: "1 mod", value: "1", description: "Kupisz 1 mod" },
  { label: "2 mody", value: "2", description: "Kupisz 2 mody" },
  { label: "3 mody", value: "3", description: "Kupisz 3 mody" },
  { label: "4 mody", value: "4", description: "Kupisz 4 mody" },
];

const MAX_PURCHASE_PLN = 10_000;

function getTestPanelOptionLabel(options, value) {
  return options.find((option) => option.value === value)?.label || null;
}

function getShopServerOptionDef(value) {
  if (String(value || "").toLowerCase() === "inne") {
    return {
      label: "INNE",
      testValue: "inne",
      calcValue: "INNE",
      description: "Inny serwer",
      channelSlug: "inne",
      emoji: TICKET_OTHER_OPTION_EMOJI,
    };
  }
  return (
    SHOP_SERVER_OPTION_DEFS.find(
      (option) => option.testValue === value || option.calcValue === value,
    ) || null
  );
}

function getShopPaymentOptionDef(value) {
  if (String(value || "").toLowerCase() === "inne") {
    return {
      label: "INNE",
      testValue: "inne",
      calcValue: "INNE",
      description: "Inna forma płatności",
      channelSlug: "inne",
      emoji: TICKET_OTHER_OPTION_EMOJI,
    };
  }
  return (
    SHOP_PAYMENT_OPTION_DEFS.find(
      (option) => option.testValue === value || option.calcValue === value,
    ) || null
  );
}

function getAutorynekPaymentOptionDef(value) {
  return (
    AUTORYNEK_EXTRA_PAYMENT_OPTION_DEFS.find(
      (option) => option.testValue === value,
    ) || getShopPaymentOptionDef(value)
  );
}

function getShopServerLabel(value) {
  return getShopServerOptionDef(value)?.label || value;
}

function getShopPaymentLabel(value) {
  return getShopPaymentOptionDef(value)?.label || value;
}

function getAutorynekPaymentLabel(value) {
  return getAutorynekPaymentOptionDef(value)?.label || value;
}

function sanitizeTicketChannelNamePart(value) {
  return (
    (value || "")
      .toString()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 80) || "ticket"
  );
}

function getTicketBuyerSlug(member, user) {
  return sanitizeTicketChannelNamePart(
    member?.displayName || user?.globalName || user?.username || "ticket",
  );
}

function formatTicketAmountPart(amount) {
  const parsed = Number(amount);
  if (!Number.isFinite(parsed) || parsed <= 0) return "0zl";

  const normalized = Number.isInteger(parsed)
    ? String(parsed)
    : parsed.toFixed(2).replace(/\.?0+$/, "").replace(".", "-");

  return `${normalized}zl`;
}

function buildPurchaseTicketChannelName(member, user, paymentValue, serverValue) {
  const paymentDef = getShopPaymentOptionDef(paymentValue);
  const paymentSlug =
    paymentDef?.channelSlug || sanitizeTicketChannelNamePart(paymentValue);
  const serverDef = getShopServerOptionDef(serverValue);
  const serverSlug =
    serverDef?.channelSlug || sanitizeTicketChannelNamePart(serverValue || "serwer");

  return `${serverSlug}-${paymentSlug}`.slice(0, 100);
}

function buildSpecialPurchaseTicketChannelName(member, user, suffix, modName, paymentValue) {
  // suffix = "mody" or "mod", modName = actual mod name, paymentValue = payment method
  if (modName && paymentValue) {
    const paymentDef = getShopPaymentOptionDef(paymentValue);
    const paymentSlug =
      paymentDef?.channelSlug || sanitizeTicketChannelNamePart(paymentValue);
    const modSlug = sanitizeTicketChannelNamePart(modName);
    return `${modSlug}-${paymentSlug}`.slice(0, 100);
  }
  // fallback for boty etc.
  const normalizedSuffix = sanitizeTicketChannelNamePart(suffix);
  return normalizedSuffix.slice(0, 100);
}

function isModernPurchaseTicketChannelName(name) {
  const normalized = (name || "").toString().toLowerCase();
  if (!normalized) return false;

  const isPaymentName = SHOP_SERVER_OPTION_DEFS.some((serverOption) =>
    SHOP_PAYMENT_OPTION_DEFS.some(
      (paymentOption) =>
        normalized === `${serverOption.channelSlug}-${paymentOption.channelSlug}`,
    ),
  );

  if (isPaymentName) return true;

  const allPaymentSlugs = [
    ...SHOP_PAYMENT_OPTION_DEFS.map((option) => option.channelSlug),
    ...AUTORYNEK_EXTRA_PAYMENT_OPTION_DEFS.map((option) => option.channelSlug),
  ];

  if (
    allPaymentSlugs.some(
      (paymentSlug) =>
        normalized.endsWith(`-${paymentSlug}`) &&
        normalized.length > paymentSlug.length + 1,
    )
  ) {
    return true;
  }

  if (/(?:^|.*-)(boty|bot|autorynek|mod|mody)$/.test(normalized)) {
    return true;
  }

  return SHOP_SERVER_OPTION_DEFS.some((serverOption) => {
    if (!normalized.startsWith(`${serverOption.channelSlug}-`)) return false;

    const suffix = normalized.slice(serverOption.channelSlug.length + 1);
    return /^\d+(?:-\d+)?zl$/.test(suffix);
  });
}

function getModalTextInputValueSafe(interaction, customId) {
  try {
    const field = interaction.fields.fields.find(f => f.customId.startsWith(customId));
    if (field) return field.value;
    return interaction.fields.getTextInputValue(customId);
  } catch {
    return null;
  }
}

function getModalStringSelectValueSafe(interaction, customId) {
  try {
    const field = interaction.fields.fields.find(f => f.customId.startsWith(customId));
    if (field && field.values) return field.values[0] || null;
    return interaction.fields.getStringSelectValues(customId)?.[0] || null;
  } catch {
    return null;
  }
}

function buildOpinionRatingSelect(customId) {
  return new StringSelectMenuBuilder()
    .setCustomId(customId)
    .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
    .setRequired(true)
    .setMinValues(1)
    .setMaxValues(1)
    .addOptions(OPINION_RATING_OPTIONS);
}

function parseOpinionRatingValue(raw) {
  if (raw == null) return null;
  const text = String(raw).trim();
  const number = Number.parseInt(text.replace(/[^0-9]/g, ""), 10);
  if (Number.isFinite(number) && number >= 1 && number <= 5) return number;

  const starCount = (text.match(/\u2B50|★|🌟/gu) || []).length;
  return starCount >= 1 && starCount <= 5 ? starCount : null;
}

function getOpinionRatingValue(interaction, customId) {
  const selected = getModalStringSelectValueSafe(interaction, customId);
  if (selected) return parseOpinionRatingValue(selected) || 5;
  return parseOpinionRatingValue(getModalTextInputValueSafe(interaction, customId)) || 5;
}

function formatOpinionStars(value) {
  const count = Math.max(1, Math.min(5, Math.trunc(Number(value) || 1)));
  const emptyCount = 5 - count;
  return `\`${OPINION_STAR.repeat(count)}${OPINION_NO_STAR.repeat(emptyCount)}\``;
}

function sanitizeInlineCodeText(value, fallback = "-") {
  const clean = String(value ?? "")
    .replace(/`/g, "ʼ")
    .replace(/[\r\n]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return clean || fallback;
}

function formatInlineCodeText(value, fallback = "-") {
  return `\`${sanitizeInlineCodeText(value, fallback)}\``;
}

function getSafeTicketDisplayName(member, user) {
  return sanitizeInlineCodeText(
    member?.displayName || user?.globalName || user?.username,
    user?.username || "Użytkownik",
  );
}

function formatOpinionText(value) {
  return formatInlineCodeText(value);
}

function normalizeOpinionModerationText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/ł/g, "l")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function isSuspiciousUnverifiedOpinion(ratings, text) {
  const hasOneStar = ratings.some((rating) => Number(rating) === 1);
  const normalizedText = normalizeOpinionModerationText(text);
  const suspiciousWords = [
    "scam",
    "scammer",
    "oszust",
    "oszustwo",
    "oszukali",
    "oszukal",
    "oszukala",
    "oszukuje",
    "zlodziej",
    "zlodzieje",
  ];
  const hasSuspiciousText = suspiciousWords.some((word) =>
    normalizedText.split(" ").some((part) => part === word || part.startsWith(word)),
  );
  return hasOneStar || hasSuspiciousText;
}

async function getVerifiedBuyerStatus(userId, guildId) {
  const purchaseResult = await db.supabase
    .from("user_purchases")
    .select("id, price, server, created_at")
    .eq("user_id", userId)
    .eq("guild_id", guildId || "default")
    .eq("type", "zakup")
    .limit(1);

  if (purchaseResult.error) {
    console.error("[opinia-filter] Nie udało się sprawdzić historii zakupów:", purchaseResult.error.message);
    return null;
  }

  const purchase = Array.isArray(purchaseResult.data) ? purchaseResult.data[0] : null;
  if (purchase) {
    console.log(`[opinia-filter] Zweryfikowany zakup użytkownika ${userId}: ${purchase.id}, ${purchase.price} PLN, ${purchase.server}.`);
    return true;
  }

  console.log(`[opinia-filter] Brak zakończonego zakupu dla użytkownika ${userId}.`);
  return false;
}

async function shouldRejectUnverifiedOpinion(userId, guildId, ratings, text) {
  if (!isSuspiciousUnverifiedOpinion(ratings, text)) return false;
  const buyerStatus = await getVerifiedBuyerStatus(userId, guildId);
  // Podejrzana opinia przechodzi wyłącznie po jednoznacznym potwierdzeniu zakupu.
  // Brak wpisu lub błąd bazy nie może przepuścić fałszywej opinii.
  return buyerStatus !== true;
}

async function moderatePublishedOpinionMessage(message) {
  if (!isOpinionChannel(message?.channel) || !isPublishedOpinionMessage(message)) return false;

  const description = String(message.embeds?.[0]?.description || "");
  const authorMatch = description.match(/<@!?(\d+)>/);
  const authorId = authorMatch?.[1];
  if (!authorId) {
    console.warn(`[opinia-filter] Nie odczytano autora opinii ${message.id}; pozostawiam wiadomość do ręcznej kontroli.`);
    return false;
  }

  const containsOneStar = description.includes(`${OPINION_STAR}${OPINION_NO_STAR.repeat(4)}`);
  const ratings = containsOneStar ? [1] : [5];
  if (!isSuspiciousUnverifiedOpinion(ratings, description)) return false;

  const buyerStatus = await getVerifiedBuyerStatus(authorId, message.guildId);
  if (buyerStatus === true) return false;

  await message.delete();
  console.log(`[opinia-filter] Usunięto opublikowaną podejrzaną opinię ${message.id} użytkownika ${authorId} bez potwierdzonego zakupu.`);
  return true;
}

function buildOpinionModal() {
  const trescInput = new TextInputBuilder()
    .setCustomId("tresc_opinii")
    .setStyle(TextInputStyle.Paragraph)
    .setRequired(true)
    .setMaxLength(900)
    .setValue(OPINION_DEFAULT_TEXT)
    .setPlaceholder(OPINION_DEFAULT_TEXT);

  return new ModalBuilder()
    .setCustomId("modal_wystaw_opinie")
    .setTitle(`⭐ NEW SHOP - Opinia`)
    .addLabelComponents(
      new LabelBuilder()
        .setLabel("Czas oczekiwania")
        .setStringSelectMenuComponent(buildOpinionRatingSelect("czas_oczekiwania")),
      new LabelBuilder()
        .setLabel("Przebieg transakcji")
        .setStringSelectMenuComponent(buildOpinionRatingSelect("przebieg_transakcji")),
      new LabelBuilder()
        .setLabel("Cena produktu")
        .setStringSelectMenuComponent(buildOpinionRatingSelect("realizacja_wymiany")),
      new LabelBuilder()
        .setLabel("Opinia")
        .setTextInputComponent(trescInput),
    );
}

function buildOpinionButton() {
  return new ButtonBuilder()
    .setCustomId("btn_wystaw_opinie")
    .setLabel("︲Wystaw opinię")
    .setEmoji(OPINION_BTN_STAR)
    .setStyle(ButtonStyle.Secondary);
}

function buildOpinionInstructionPayload() {
  const container = new ContainerBuilder().setAccentColor(0xffd700);
  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      "`📊` × Kliknij w przycisk na dole, aby podzielić się **opinią** o **naszym serwerze**!"
    )
  );

  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

  container.addActionRowComponents(
    new ActionRowBuilder().addComponents(buildOpinionButton()),
  );

  appendBrandFooterToContainer(container, null);

  return {
    components: [container],
    flags: MessageFlags.IsComponentsV2,
  };
}

function buildZaproszeniaInstructionPayload() {
  const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);
  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      "`✉️` × Kliknij w przycisk na dole, aby sprawdzić swoje **zaproszenia** na serwerze!"
    )
  );

  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

  const btn = new ButtonBuilder()
    .setCustomId("btn_sprawdz_zaproszenia")
    .setLabel("📩︲Sprawdź zaproszenia")
    .setStyle(ButtonStyle.Secondary);

  container.addActionRowComponents(
    new ActionRowBuilder().addComponents(btn)
  );

  appendBrandFooterToContainer(container, null);

  return {
    components: [container],
    flags: MessageFlags.IsComponentsV2,
  };
}

let isEnsuringInvitePanel = false;

async function ensureInvitePanel(channel) {
  if (!channel || channel.id !== SPRAWDZ_ZAPROSZENIA_CHANNEL_ID) return;
  if (isEnsuringInvitePanel) return;
  isEnsuringInvitePanel = true;
  try {
    const messages = await channel.messages.fetch({ limit: 100 }).catch(() => null);
    if (!messages) {
      isEnsuringInvitePanel = false;
      return;
    }

    const panelMessages = [];

    messages.forEach(msg => {
      const componentsStr = JSON.stringify(msg.components || {});
      const isPanel = msg.author.id === client.user.id && componentsStr.includes("btn_sprawdz_zaproszenia");
      if (isPanel) {
        panelMessages.push(msg);
      }
    });

    let activePanel = null;
    if (panelMessages.length > 0) {
      activePanel = panelMessages[0]; // Trzymamy najnowszy panel
      for (let i = 1; i < panelMessages.length; i++) {
        await panelMessages[i].delete().catch(() => null);
      }
    }

    if (!activePanel) {
      const payload = buildZaproszeniaInstructionPayload();
      const sent = await channel.send(payload);
      lastInviteInstruction.set(channel.id, sent.id);
      scheduleSavePersistentState();
      console.log(`[invites] Wysłano nowy panel zaproszeń: ${sent.id} na kanał #${channel.name || channel.id}`);
    } else {
      lastInviteInstruction.set(channel.id, activePanel.id);
      console.log(`[invites] Znaleziono aktywny panel zaproszeń: ${activePanel.id}`);
    }
  } catch (e) {
    console.error("[invites] Błąd w ensureInvitePanel:", e?.message || e);
  } finally {
    isEnsuringInvitePanel = false;
  }
}

async function handlePanelZaproszenCommand(interaction, targetChannel = null) {
  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });
  try {
    const zapCh = targetChannel ||
      interaction.guild.channels.cache.get(SPRAWDZ_ZAPROSZENIA_CHANNEL_ID) ||
      (await client.channels.fetch(SPRAWDZ_ZAPROSZENIA_CHANNEL_ID).catch((e) => {
        console.error("[panel-zaproszen] Błąd pobierania kanału:", e?.message || e);
        return null;
      }));

    if (!zapCh) {
      await interaction.editReply({
        content: `> \`❌\` × Nie mogę znaleźć kanału <#${SPRAWDZ_ZAPROSZENIA_CHANNEL_ID}> (${SPRAWDZ_ZAPROSZENIA_CHANNEL_ID}). Upewnij się, że bot ma dostęp do tego kanału!`
      });
      return;
    }

    // Usuń stare panele jeśli istnieją na kanale
    const messages = await zapCh.messages.fetch({ limit: 50 }).catch(() => null);
    if (messages) {
      for (const msg of messages.values()) {
        const comp = JSON.stringify(msg.components || {});
        if (msg.author.id === client.user.id && comp.includes("btn_sprawdz_zaproszenia")) {
          await msg.delete().catch(() => null);
        }
      }
    }

    const payload = buildZaproszeniaInstructionPayload();
    const sent = await zapCh.send(payload);
    lastInviteInstruction.set(zapCh.id, sent.id);
    scheduleSavePersistentState();

    await interaction.editReply({
      content: `> \`✅\` × Panel zaproszeń został pomyślnie wysłany na kanał <#${zapCh.id}>!`
    });
  } catch (err) {
    console.error("Błąd w handlePanelZaproszenCommand:", err);
    await interaction.editReply({
      content: `> \`❌\` × Błąd podczas wysyłania panelu: \`${err?.message || err}\`\n> Sprawdź, czy bot ma uprawnienia \`Wysyłanie wiadomości\` oraz \`Wyświetlanie kanału\` na kanale <#${SPRAWDZ_ZAPROSZENIA_CHANNEL_ID}>.`
    });
  }
}

function buildTicketPanelPayload() {
  const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);
  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      "```\n" +
      "🛒 New Shop × TICKET\n" +
      "```"
    )
  );
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      "> <a:arrowwhite:1491476759290449984> × Wybierz odpowiednią kategorię, aby utworzyć ticketa!"
    )
  );

  const selectMenu = new StringSelectMenuBuilder()
    .setCustomId("ticket_category")
    .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
    .addOptions(PANEL_CATEGORY_OPTIONS);

  container.addActionRowComponents(new ActionRowBuilder().addComponents(selectMenu));
  // Przywrócono stopkę dla panelu ticketów zgodnie z prośbą
  appendBrandFooterToContainer(container, null);

  return {
    components: [container],
    flags: MessageFlags.IsComponentsV2
  };
}

async function sendTicketPanel(interaction) {
  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });
  await interaction.channel.send(buildTicketPanelPayload());
  await interaction.editReply({
    content: "> `✅` × **Panel** ticketów wysłany!",
  });
}

async function handlePanelWyslijCommand(interaction) {
  const respond = (content) => interaction.deferred || interaction.replied
    ? interaction.editReply({ content })
    : interaction.reply({ content, flags: [MessageFlags.Ephemeral] });
  if (!interaction.guild || !interaction.channel?.isTextBased() || typeof interaction.channel.send !== "function") {
    await respond("> `❌` × Wybierz kanał tekstowy na serwerze.");
    return;
  }
  const isOwner = interaction.user.id === interaction.guild.ownerId;
  if (!isOwner && !interaction.member?.permissions?.has(PermissionFlagsBits.ManageChannels)) {
    await respond("> `❗` × Wymagane uprawnienie: Zarządzanie kanałami.");
    return;
  }
  const selected = SENDABLE_PANELS.find((panel) => panel.value === interaction.options.getString("panel", true));
  if (!selected) {
    await respond("> `❌` × Nieznany panel. Wybierz panel z listy komendy.");
    return;
  }
  if (selected.category !== interaction.options.getString("kategoria")) {
    await respond("> `❌` × Wybierz panel z wybranej kategorii. Po zmianie kategorii wybierz panel ponownie.");
    return;
  }
  try {
    if (selected.handler) {
      await selected.handler(interaction);
      return;
    }
    await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });
    if (selected.send) await selected.send(interaction);
    else await interaction.channel.send({ ...selected.payload(interaction), allowedMentions: { parse: [] } });
    await respond(`> \`✅\` × Wysłano panel **${selected.name}** na <#${interaction.channelId}>.`);
  } catch (error) {
    console.error("[panel-wyslij] Nie udało się wysłać panelu:", error);
    await respond("> `❌` × Nie udało się wysłać panelu. Sprawdź uprawnienia bota do kanału.");
  }
}

function cleanImportedPanelComponent(component) {
  const fields = ["type", "accent_color", "spoiler", "content", "spacing", "divider",
    "custom_id", "style", "label", "emoji", "url", "disabled", "placeholder",
    "min_values", "max_values", "options"];
  const result = Object.fromEntries(fields.filter((key) => component[key] !== undefined)
    .map((key) => [key, component[key]]));
  if (component.components) result.components = component.components.map(cleanImportedPanelComponent);
  if (component.items) result.items = component.items.map((item) => ({
    media: { url: item.media.url }, ...(item.description ? { description: item.description } : {}), spoiler: !!item.spoiler,
  }));
  return result;
}

async function sendImportedPanel(interaction, file) {
  const preset = JSON.parse(fs.readFileSync(path.join(__dirname, "panel-presets", `${file}.json`), "utf8"));
  if (preset.format !== "newshop-panel" || preset.version !== 1 || !preset.state) throw new Error("Niepoprawny zapis panelu");
  const components = preset.message.components.map(cleanImportedPanelComponent);
  // Refresh signed media links from the original Discord message when available.
  if (JSON.stringify(components).includes('"media"')) {
    const sourceChannel = await interaction.guild.channels.fetch(preset.source.channelId).catch(() => null);
    const sourceMessage = await sourceChannel?.messages?.fetch(preset.source.messageId).catch(() => null);
    if (sourceMessage) {
      const freshUrls = new Map();
      const collect = (node) => {
        if (node.media?.url) freshUrls.set(new URL(node.media.url).pathname, node.media.url);
        for (const child of node.components || []) collect(child);
        for (const item of node.items || []) collect(item);
      };
      for (const component of sourceMessage.components) collect(getSerializableMessageComponent(component));
      const refresh = (node) => {
        if (node.media?.url) node.media.url = freshUrls.get(new URL(node.media.url).pathname) || node.media.url;
        for (const child of node.components || []) refresh(child);
        for (const item of node.items || []) refresh(item);
      };
      components.forEach(refresh);
    }
  }
  const state = { ...JSON.parse(JSON.stringify(preset.state)),
    ownerId: interaction.user.id, guildId: interaction.guildId, channelId: interaction.channelId,
    messageId: null, persistPanel: true };
  const mediaUrls = [];
  const rememberMedia = (node) => {
    if (node.media?.url) mediaUrls.push(node.media.url);
    for (const child of node.components || []) rememberMedia(child);
    for (const item of node.items || []) rememberMedia(item);
  };
  components.forEach(rememberMedia);
  state.mediaUrls = mediaUrls;
  const sent = await interaction.channel.send({
    components, flags: MessageFlags.IsComponentsV2, allowedMentions: { parse: [] },
  });
  state.messageId = sent.id;
  embedTestStates.set(sent.id, state);
  if (isRegulationEmbedState(state)) regulationPanels.set(sent.id, cloneRegulationPanelState(state));
  scheduleSavePersistentState(true);
}

async function sendSavedRegulationPanel(interaction) {
  const candidates = [...regulationPanels.entries()]
    .filter(([, state]) => state.guildId === interaction.guildId)
    .sort(([leftId, left], [rightId, right]) => {
      const localDifference = Number(right.channelId === interaction.channelId) - Number(left.channelId === interaction.channelId);
      if (localDifference) return localDifference;
      return BigInt(rightId) > BigInt(leftId) ? 1 : BigInt(rightId) < BigInt(leftId) ? -1 : 0;
    });
  const sourceState = candidates.length ? getRegulationPanelStateByMessageId(candidates[0][0]) : null;
  const state = sourceState
    ? cloneRegulationPanelState(sourceState, {
      ownerId: interaction.user.id, guildId: interaction.guildId,
      channelId: interaction.channelId, messageId: null, persistPanel: true,
    })
    : createDefaultRegulaminState(interaction.guild, interaction.channel, interaction.user.id, null);
  await ensureEmbedTestEmojiCache(interaction.guildId);
  const sent = await interaction.channel.send({ ...buildEmbedTestMessagePayload(state), allowedMentions: { parse: [] } });
  delete state.mediaFiles;
  state.messageId = sent.id;
  state.persistPanel = true;
  embedTestStates.set(sent.id, state);
  regulationPanels.set(sent.id, cloneRegulationPanelState(state));
  scheduleSavePersistentState(true);
}

async function showTestPanelZakupModal(interaction) {
  const detectedServer = await detectServerFromContext(interaction);
  await showZakupModalV2(interaction, detectedServer);
}

async function showZakupModalV2(interaction, detectedServer = null) {
  const timestamp = Date.now();

  const serverSelect = new StringSelectMenuBuilder()
    .setCustomId(`zakup_server_${timestamp}`)
    .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
    .setRequired(true)
    .setMinValues(1)
    .setMaxValues(1)
    .addOptions(
      sanitizeOptionsEmojis(
        TEST_PANEL_SERVER_OPTIONS.map((opt) => ({
          ...opt,
          default: !!(detectedServer && opt.value === detectedServer.testValue),
        }))
      )
    );

  const paymentSelect = new StringSelectMenuBuilder()
    .setCustomId(`zakup_payment_${timestamp}`)
    .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
    .setRequired(true)
    .setMinValues(1)
    .setMaxValues(1)
    .addOptions(sanitizeOptionsEmojis(TEST_PANEL_PAYMENT_OPTIONS));

  const amountInput = new TextInputBuilder()
    .setCustomId(`kwota_${timestamp}`)
    .setStyle(TextInputStyle.Short)
    .setPlaceholder("Przykład: 20zł")
    .setRequired(true);

  const modal = new ModalBuilder()
    .setCustomId(`modal_zakup_${timestamp}`)
    .setTitle("Zakup itemów")
    .addLabelComponents(
      new LabelBuilder()
        .setLabel("Na jakim serwerze?")
        .setStringSelectMenuComponent(serverSelect),
      new LabelBuilder()
        .setLabel("Forma płatności")
        .setStringSelectMenuComponent(paymentSelect),
      new LabelBuilder()
        .setLabel("Kwota (PLN)")
        .setTextInputComponent(amountInput),
    );

  await interaction.showModal(modal);
}

async function handleOwnerInviteCountingCommand(interaction) {
  const guild = interaction.guild;
  if (!guild) {
    await interaction.reply({
      content: "> `❌` × Ta komenda działa tylko na serwerze.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  if (interaction.user.id !== guild.ownerId) {
    await interaction.reply({
      content: "> `❌` × Tej komendy może użyć tylko właściciel serwera.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const status = interaction.options.getString("status", true);
  const enabled = status === "on";
  ownerInviteCountingSettings.set(guild.id, enabled);
  scheduleSavePersistentState(true);

  await interaction.reply({
    content: enabled
      ? "> `✅` × Od teraz zaproszenia właściciela są liczone jak u zwykłego użytkownika."
      : "> `✅` × Wyłączyłem liczenie zaproszeń właścicielowi.",
    flags: [MessageFlags.Ephemeral],
  });
}

async function handleAutoVerifyCommand(interaction) {
  const guild = interaction.guild;
  if (!guild) {
    await interaction.reply({
      content: "> `❌` × Ta komenda działa tylko na serwerze.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  if (interaction.user.id !== guild.ownerId) {
    await interaction.reply({
      content: "> `❌` × Tej komendy może użyć tylko właściciel serwera.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const status = interaction.options.getString("status", true);
  const enabled = status === "wlacz";
  autoVerifySettings.set(guild.id, enabled);
  scheduleSavePersistentState(true);

  await interaction.reply({
    content: enabled
      ? "> `✅` × Automatyczna weryfikacja została **włączona**. Nowi użytkownicy będą automatycznie otrzymywać rolę klient."
      : "> `✅` × Automatyczna weryfikacja została **wyłączona**.",
    flags: [MessageFlags.Ephemeral],
  });
}

async function handleTestPanelCommand(interaction) {
  if (interaction.user.id !== interaction.guild.ownerId) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  await sendTicketPanel(interaction);
}

async function handleTicketPanelCommand(interaction) {
  // Sprawdź czy właściciel
  if (interaction.user.id !== interaction.guild.ownerId) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }
  await sendTicketPanel(interaction);
}

async function handlePanelKlientaCommand(interaction) {
  try {
    if (!interaction.deferred && !interaction.replied) {
      await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });
    }

    const SELLER_ROLE_ID = "1350786945944391733";
    const isAdmin = interaction.member?.permissions?.has(PermissionFlagsBits.Administrator);
    const isSeller = interaction.member?.roles?.cache?.has(SELLER_ROLE_ID);
    if (!isAdmin && !isSeller) {
      await interaction.editReply({ content: "> `❗` × Brak wymaganych uprawnień." });
      return;
    }

    await interaction.channel.send(buildPanelKlientaPayload(interaction.guildId));

    await interaction.editReply({
      content: "> `✅` × **Panel klienta** został wysłany na ten **kanał**."
    });
  } catch (err) {
    console.error("Błąd w handlePanelKlientaCommand:", err);
    const payload = { content: "> `❌` × Wystąpił błąd podczas wysyłania panelu klienta." };
    if (interaction.deferred || interaction.replied) await interaction.editReply(payload).catch(() => null);
    else await interaction.reply({ ...payload, flags: [MessageFlags.Ephemeral] }).catch(() => null);
  }
}

function buildPanelKlientaPayload(guildId) {
  const spentEmoji = findGuildEmojiByName(guildId, "kasa_3");
  const codesEmoji = findGuildEmojiByName(guildId, "shop");
  const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);
  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      "```\n" +
      "🛒 New Shop × PANEL KLIENTA\n" +
      "```"
    )
  );
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      "> <a:arrowwhite:1491476759290449984> × Wybierz jedną z opcji poniżej."
    )
  );

  const clientSelect = new StringSelectMenuBuilder()
    .setCustomId("panel_klienta_select")
    .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
    .setMinValues(1)
    .setMaxValues(1)
    .addOptions(
      {
        label: toPanelFont("Sprawdź ile wydałeś"),
        value: "panel_klienta_spent",
        description: "Wyświetla sumę Twoich pieniędzy oraz rangę",
        emoji: spentEmoji
          ? { id: spentEmoji.id, name: spentEmoji.name, animated: spentEmoji.animated }
          : "💵"
      },
      {
        label: toPanelFont("Sprawdź aktywne kody"),
        value: "panel_klienta_codes",
        description: "Wyświetla aktywne kody przypisane do Twojego konta",
        emoji: codesEmoji
          ? { id: codesEmoji.id, name: codesEmoji.name, animated: codesEmoji.animated }
          : "🎁"
      }
    );

  container.addActionRowComponents(
    new ActionRowBuilder().addComponents(clientSelect)
  );

  appendBrandFooterToContainer(container, null);

  return {
    components: [container],
    flags: MessageFlags.IsComponentsV2
  };
}

async function handlePanelKlientaSpent(interaction) {
  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });

  try {
    const guild = interaction.guild;
    const userId = interaction.user.id;
    
    if (!guild) {
      await interaction.editReply({ content: "> `❌` Ta komenda działa jedynie na serwerach." });
      return;
    }

    const spent = await db.getUserSpent(userId, guild.id);

    const roleTiers = [
      { name: "Klient 200+", min: 200, roleId: "1521924538265243738" },
      { name: "Klient 500+", min: 500, roleId: "1458145139938562058" },
      { name: "Klient 1000+", min: 1000, roleId: "1521924656792080384" },
      { name: "Klient 2000+", min: 2000, roleId: "1521924963190177924" }
    ];

    let nextTier = null;
    for (let i = 0; i < roleTiers.length; i++) {
      if (spent < roleTiers[i].min) {
        nextTier = roleTiers[i];
        break;
      }
    }

    let currentTier = null;
    for (const tier of roleTiers) {
      if (spent >= tier.min) {
        currentTier = tier;
      }
    }
    const ownedRoles = currentTier ? [`**${currentTier.name}**`] : [];

    let line1 = `<a:arrowwhite:1491476759290449984> ×  Aktualnie __nie posiadasz__ żadnej __rangi__. `;
    if (ownedRoles.length > 0) {
      line1 = `<a:arrowwhite:1491476759290449984> ×  **Posiadasz** __rangę__ ${ownedRoles.join(", ")}.`;
    }

    const line2 = `<a:arrowwhite:1491476759290449984> ×  Łącznie __wydałeś__ **${spent.toFixed(0)} PLN** w naszym sklepie.`;

    let line3 = "";
    if (nextTier) {
      line3 = `<a:arrowwhite:1491476759290449984> ×  Do __następnej rangi__ (**${nextTier.name}**) brakuje Ci **${(nextTier.min - spent).toFixed(0)} PLN**.`;
    } else {
      line3 = `<a:arrowwhite:1491476759290449984> ×  Osiągnąłeś już __najwyższą rangę__ bonusową!`;
    }

    const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);
    container.addTextDisplayComponents(
      new TextDisplayBuilder().setContent(
        "```\n" +
        "💵 New Shop × TWOJE STATYSTYKI\n" +
        "```"
      )
    );
    container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

    container.addTextDisplayComponents(
      new TextDisplayBuilder().setContent(
        `> ${line1}\n` +
        `> ${line2}\n` +
        `> ${line3}`
      )
    );
    appendBrandFooterToContainer(container, guild.id);

    await interaction.editReply({
      components: [container],
      flags: MessageFlags.IsComponentsV2 | MessageFlags.Ephemeral,
    });
  } catch (err) {
    console.error("Błąd w handlePanelKlientaSpent:", err);
    await interaction.editReply({ content: "> `❌` Wystąpił błąd podczas sprawdzania Twoich wydatków." });
  }
}

function getClientCodeType(codeData) {
  if (codeData.type === "invite_cash" || codeData.type === "invite_reward") {
    return "Nagroda za zaproszenia";
  }
  if (codeData.type === "free_kasa_reward") {
    return Number(codeData.rewardAmount || 0) > 0
      ? "Nagroda pieniężna"
      : "Nagroda z Free Kasa";
  }
  if (codeData.type === "discount" || Number(codeData.discount || 0) > 0) {
    return "Kod zniżkowy";
  }
  return "Kod nagrody";
}

function getClientCodeReward(codeData) {
  if (codeData.rewardText) return String(codeData.rewardText);
  if (codeData.reward) return String(codeData.reward);

  const discount = Number(codeData.discount || 0);
  if (discount > 0) return `-${discount}% na zakupy w sklepie`;

  const rewardAmount = Number(codeData.rewardAmount || 0);
  if (rewardAmount > 0) {
    return `${rewardAmount.toLocaleString("pl-PL")}$ na Anarchia LF`;
  }

  return "Nagroda do odebrania";
}

async function getClientActiveCodes(userId) {
  const databaseCodes = await db.getActiveCodes().catch((error) => {
    console.error("Błąd odświeżania aktywnych kodów panelu klienta:", error);
    return [];
  });

  for (const storedCodeData of databaseCodes) {
    const normalizedCode = normalizeCodeInput(storedCodeData?.code);
    if (!normalizedCode) continue;

    const cached = activeCodes.get(normalizedCode) || {};
    activeCodes.set(normalizedCode, {
      ...cached,
      oderId: storedCodeData.user_id || cached.oderId,
      discount: Number(storedCodeData.discount || cached.discount || 0),
      expiresAt: storedCodeData.expires_at
        ? new Date(storedCodeData.expires_at).getTime()
        : Number(cached.expiresAt || 0),
      used: !!storedCodeData.used,
      reward: storedCodeData.reward || cached.reward,
      rewardAmount: Number(storedCodeData.reward_amount || cached.rewardAmount || 0),
      rewardText: storedCodeData.reward_text || cached.rewardText,
      type: storedCodeData.type || cached.type,
    });
  }

  const now = Date.now();
  return Array.from(activeCodes.entries())
    .filter(([, codeData]) =>
      String(codeData?.oderId || "") === String(userId) &&
      !codeData?.used &&
      Number(codeData?.expiresAt || 0) > now
    )
    .sort(([, first], [, second]) =>
      Number(first.expiresAt || 0) - Number(second.expiresAt || 0)
    );
}

async function handlePanelKlientaActiveCodes(interaction, pageIndex = 0) {
  const isUpdate = interaction.customId.startsWith("panel_klienta_codes_") ||
    interaction.customId.startsWith("panel_klienta_history_");
  if (isUpdate) {
    await interaction.deferUpdate();
  } else {
    await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });
  }

  try {
    const guild = interaction.guild;
    const userId = interaction.user.id;
    
    if (!guild) {
      const errPayload = { content: "> `❌` Ta komenda działa jedynie na serwerach.", flags: [MessageFlags.Ephemeral] };
      if (isUpdate) await interaction.followUp(errPayload);
      else await interaction.editReply(errPayload);
      return;
    }

    const userCodes = await getClientActiveCodes(userId);

    if (userCodes.length === 0) {
      const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);
      container.addTextDisplayComponents(
        new TextDisplayBuilder().setContent(
          "```\n" +
          "🎁 New Shop × AKTYWNE KODY\n" +
          "```\n" +
          "> <a:arrowwhite:1491476759290449984> × Aktualnie __nie posiadasz__ **żadnych aktywnych kodów**.\n" +
          `> <a:arrowwhite:1491476759290449984> × Zdobyć je możesz __na kanale__ <#${FREE_KASA_CHANNEL_ID}> lub <#${REWARDS_CHANNEL_ID}>.`
        )
      );
      appendBrandFooterToContainer(container, guild.id);

      const emptyPayload = {
        components: [container],
        flags: MessageFlags.IsComponentsV2 | MessageFlags.Ephemeral
      };
      await interaction.editReply(emptyPayload);
      return;
    }

    const itemsPerPage = 5;
    const totalPages = Math.ceil(userCodes.length / itemsPerPage);
    const safePageIndex = Math.max(0, Math.min(pageIndex, totalPages - 1));

    const start = safePageIndex * itemsPerPage;
    const end = Math.min(start + itemsPerPage, userCodes.length);
    const pageCodes = userCodes.slice(start, end);

    const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);
    container.addTextDisplayComponents(
        new TextDisplayBuilder().setContent(
          "```\n" +
          "🎁 New Shop × AKTYWNE KODY\n" +
          "```",
      ),
    );

    for (const [code, codeData] of pageCodes) {
      const expiryTimestamp = Math.floor(Number(codeData.expiresAt) / 1000);
      container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
      container.addTextDisplayComponents(
        new TextDisplayBuilder().setContent(
          `> \`🎁\` × **Kod:** \`${code}\`\n` +
          `> \`🏷️\` × **Rodzaj:** ${getClientCodeType(codeData)}\n` +
          `> \`🎁\` × **Nagroda:** __**${getClientCodeReward(codeData)}**__\n` +
          `> \`🕒\` × **Wygasa:** <t:${expiryTimestamp}:R>`
        )
      );
    }

    if (totalPages > 1) {
      container.addActionRowComponents(
        new ActionRowBuilder().addComponents(
          new ButtonBuilder()
            .setCustomId(`panel_klienta_codes_${safePageIndex - 1}`)
            .setStyle(ButtonStyle.Secondary)
            .setLabel("<")
            .setDisabled(safePageIndex === 0),
          new ButtonBuilder()
            .setCustomId(`panel_klienta_codes_info`)
            .setStyle(ButtonStyle.Secondary)
            .setLabel(`${safePageIndex + 1}/${totalPages}`)
            .setDisabled(true),
          new ButtonBuilder()
            .setCustomId(`panel_klienta_codes_${safePageIndex + 1}`)
            .setStyle(ButtonStyle.Secondary)
            .setLabel(">")
            .setDisabled(safePageIndex === totalPages - 1)
        )
      );
    }

    appendBrandFooterToContainer(container, guild.id);

    const payload = {
      components: [container],
      flags: MessageFlags.IsComponentsV2 | MessageFlags.Ephemeral
    };

    await interaction.editReply(payload);
  } catch (err) {
    console.error("Błąd w handlePanelKlientaActiveCodes:", err);
    const errPayload = { content: "> `❌` Wystąpił błąd podczas pobierania aktywnych kodów.", flags: [MessageFlags.Ephemeral] };
    if (isUpdate) await interaction.followUp(errPayload);
    else await interaction.editReply(errPayload);
  }
}

function buildTicketCloseConfirmEmbed(actionLabel, expiresAt = Math.floor(Date.now() / 1000) + 30) {
  return new EmbedBuilder()
    .setColor(COLOR_BLUE)
    .setDescription(
      `> \`⚠️\` × ${actionLabel}\n` +
      `> \`⏳\` × Czas na potwierdzenie kończy się: <t:${expiresAt}:R>`,
    );
}

async function handleCloseTicketCommand(interaction) {
  // Sprawdź uprawnienia przed sprawdzaniem kanału
  if (!isAdminOrSeller(interaction.member)) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const channel = interaction.channel;

  if (!isTicketChannel(channel)) {
    await interaction.reply({
      content: "> `❌` × Ta **komenda** działa jedynie na **ticketach**!",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const chId = channel.id;
  const now = Date.now();
  const pending = pendingTicketClose.get(chId);

  if (
    pending &&
    pending.userId === interaction.user.id &&
    now - pending.ts < 30_000
  ) {
    pendingTicketClose.delete(chId);
    // remove ticketOwners entry immediately
    const ticketMeta = ticketOwners.get(chId) || null;
    await commitRewardTicketClaim(chId).catch(() => null);
    ticketOwners.delete(chId);
    scheduleSavePersistentState();

    await interaction.reply({
      embeds: [
        new EmbedBuilder()
          .setColor(COLOR_BLUE)
          .setDescription("> \`ℹ️\` × **Ticket zostanie zamknięty w ciągu 5 sekund...**")
      ]
    });

    try {
      await archiveTicketOnClose(
        channel,
        interaction.user.id,
        ticketMeta,
        { closeMethod: "Komenda /zamknij" },
      ).catch((e) => console.error("archiveTicketOnClose error:", e));
    } catch (e) {
      console.error("Błąd archiwizacji ticketu (command):", e);
    }

    setTimeout(async () => {
      try {
        await channel.delete();
      } catch (error) {
        console.error("Błąd zamykania ticketu:", error);
      }
    }, 2000);
  } else {
    const expiresAt = Math.floor(now / 1000) + 30;
    pendingTicketClose.set(chId, { userId: interaction.user.id, ts: now });
    await interaction.reply({
      embeds: [buildTicketCloseConfirmEmbed("Użyj `/zamknij` jeszcze raz", expiresAt)],
      flags: [MessageFlags.Ephemeral],
    });
    setTimeout(() => pendingTicketClose.delete(chId), 30_000);
  }
}

async function handleAddUserToTicketCommand(interaction) {
  if (!isGuildOwner(interaction) && !isActiveSeller(interaction)) {
    await interaction.reply({
      content: "> `❗` × Tylko właściciel lub sprzedawca może użyć tej komendy.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const channel = interaction.channel;
  if (!isTicketChannel(channel)) {
    await interaction.reply({
      content: "> `❌` × Ta komenda działa jedynie na ticketach!",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const targetUser = interaction.options.getUser("osoba", true);
  if (targetUser.bot) {
    await interaction.reply({
      content: "> `❌` × Nie można dodać bota jako uczestnika ticketu.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const ticketData = ticketOwners.get(channel.id) || null;

  try {
    await channel.permissionOverwrites.edit(targetUser.id, {
      ViewChannel: true,
      SendMessages: true,
      ReadMessageHistory: true,
    });

    await sendTicketLogEntry(interaction.guild, {
      title: "Dodano użytkownika do ticketu",
      icon: "👥",
      color: COLOR_BLUE,
      summary: "Do ticketu został dodany dodatkowy użytkownik komendą /dodaj.",
      ticketChannel: channel,
      ownerId: ticketData?.userId || null,
      actorId: interaction.user.id,
      claimedById: ticketData?.claimedBy || null,
      ticketMeta: ticketData,
      statusLabel: ticketData?.claimedBy ? "PRZEJĘTY" : "OTWARTY",
      detailLines: [`Dodano użytkownika: <@${targetUser.id}>`],
    }).catch((error) => console.error("ticket /dodaj log error:", error));

    await interaction.reply({
      content: `> \`✅\` × Dodano <@${targetUser.id}> do ticketu.`,
      allowedMentions: { users: [] },
      flags: [MessageFlags.Ephemeral],
    });
  } catch (error) {
    console.error("Błąd komendy /dodaj:", error);
    await interaction.reply({
      content: "> `❌` × Nie udało się dodać użytkownika do ticketu.",
      flags: [MessageFlags.Ephemeral],
    });
  }
}

// ----------------- /ticket-zakoncz handler -----------------
async function handleTicketZakonczCommand(interaction) {
  // Sprawdź czy właściciel lub sprzedawca
  const isOwner = interaction.user.id === interaction.guild.ownerId;
  const SELLER_ROLE_ID = "1350786945944391733";
  const hasSellerRole = interaction.member.roles.cache.has(SELLER_ROLE_ID);

  if (!isOwner && !hasSellerRole) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const channel = interaction.channel;
  const ticketData = ticketOwners.get(channel.id) || null;

  // Sprawdź czy komenda jest używana w tickecie
  if (!isTicketChannel(channel)) {
    await interaction.reply({
      content: "> `❌` × Ta **komenda** działa jedynie na **ticketach**!",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // Pobierz parametry
  const typ = interaction.options.getString("typ");
  const cena = interaction.options.getInteger("ile", true);
  const formattedCena = `${cena} PLN`;
  const serwer = (interaction.options.getString("serwer") || "").trim();

  const ticketOwnerId = await resolveTicketOwnerId(channel);

  if (!ticketOwnerId) {
    await interaction.reply({
      content: "> `❌` × **Nie udało się** zidentyfikować właściciela ticketu.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // Automatycznie dodaj kwotę do wydatków klienta (ticketOwnerId) w Supabase, jeśli to zakup
  if (typ === "zakup") {
    const amount = parsePLN(formattedCena);
    if (amount > 0) {
      await db.addUserSpent(ticketOwnerId, amount, interaction.guildId || "default").catch((err) =>
        console.error("Error automatically adding user spent on ticket-zakoncz:", err)
      );
      // Zapisz transakcję do historii zakupów
      // Synchronizacja ról zakupowych klienta
      await syncUserSpentRoles(interaction.guild, ticketOwnerId).catch((err) =>
        console.error("Error syncing roles on ticket-zakoncz:", err)
      );
    }
  }

  // Zachowaj w historii obrotu zarówno zakupy, jak i sprzedaże.
  if (typ === "zakup" || typ === "sprzedaż" || typ === "sprzedaz") {
    await db.addUserPurchase(
      ticketOwnerId,
      cena,
      serwer || "Brak",
      typ,
      interaction.guildId || "default",
    ).catch((err) => console.error("Error saving shop transaction history:", err));
  }

  const legitRepChannelId = "1449840030947217529";
  let thankLine = "Dziękujemy za zakup w naszym sklepie";
  const typLower = typ.toLowerCase();
  if (typLower === "sprzedaż") {
    thankLine = "Dziękujemy za sprzedaż w naszym sklepie";
  } else if (typLower === "wręczył nagrodę") {
    thankLine = "Nagroda została nadana";
  }

  const verb = typ.toUpperCase();
  const repMessage = `+rep @${interaction.user.username} ${verb} ${formattedCena}${serwer ? ` ${serwer}` : ""}`;

  const payload = buildPendingLegitCheckPayload(
    ticketOwnerId,
    thankLine,
    legitRepChannelId,
    repMessage,
    interaction.guild?.id || null
  );

  // Ephemeral potwierdzenie dla sprzedawcy
  await interaction.reply({
    content: "`✅` × Poprawnie użyto komendy ticket zakończ.",
    flags: [MessageFlags.Ephemeral],
  });

  // Wyślij ping osobno nad embedem
  const pingMsg = await interaction.channel.send({
    content: `<@${ticketOwnerId}>`,
    allowedMentions: { users: [ticketOwnerId] },
  }).catch(() => null);

  // Wyślij embed + wzór na ticket
  const sentEmbedMsg = await interaction.channel.send(payload).catch(() => null);

  const sentRepMsg = await interaction.channel.send({
    content: repMessage,
  }).catch(() => null);

  // Oznacz właściciela ticketu na kanałach do opinii/repa i usuń ping po chwili
  try {
    const channelsToPing = [legitRepChannelId];

    const hasReactedCzyLegit = await hasUserReactedInCzyLegit(ticketOwnerId, interaction.guild);
    if (!hasReactedCzyLegit) {
      channelsToPing.push("1350446732365926494"); // czy-legit
    } else {
      console.log(`[ticket-zakoncz] Pomijam ping dla ${ticketOwnerId} na czy-legit (użytkownik ma już reakcję).`);
    }

    const hasGivenOpinion = await hasUserPublishedOpinion(ticketOwnerId, interaction.guild);
    if (!hasGivenOpinion) {
      channelsToPing.push("1449783959306375198"); // opinie klientów
    } else {
      console.log(`[ticket-zakoncz] Pomijam ping dla ${ticketOwnerId} na opinie-klientow (użytkownik wystawił już opinię).`);
    }

    for (const chId of channelsToPing) {
      const ch = await interaction.guild.channels.fetch(chId).catch(() => null);
      if (ch && ch.isTextBased()) {
        const pingMessage = await ch.send({
          content: `<@${ticketOwnerId}>`,
          allowedMentions: { users: [ticketOwnerId] },
        }).catch(() => null);

        if (pingMessage) {
          setTimeout(() => {
            pingMessage.delete().catch(() => null);
          }, LEGIT_REP_PING_DELETE_DELAY_MS);
        }
      }
    }
  } catch (err) {
    console.error("Nie udało się wysłać pingów:", err);
  }

  // Zapisz informację o oczekiwaniu na +rep dla tego ticketu
  pendingTicketClose.set(channel.id, {
    userId: ticketOwnerId, // właściciel ticketu musi wysłać +rep
    commandUserId: interaction.user.id, // osoba która użyła komendy
    commandUsername: interaction.user.username, // nick osoby
    typ: typ,
    co: formattedCena,
    serwer: serwer,
    awaitingRep: true,
    legitRepChannelId,
    embedMessageId: sentEmbedMsg?.id || null,
    repTextMessageId: sentRepMsg?.id || null,
    repMessage: repMessage,
    ts: Date.now()
  });

  if (sentEmbedMsg?.id) {
    ticketRepMessages.set(sentEmbedMsg.id, repMessage);
  }
  ticketRepMessages.set(channel.id, repMessage);

  // Przenieś ticket do kategorii zrealizowanej
  try {
    await ticketCategoryManager.move(channel, "zrealizowane");
  } catch (err) {
    console.error("Nie udało się przenieść ticketu do kategorii zrealizowanej:", err);
  }

  await sendTicketLogEntry(interaction.guild, {
    title: "Ticket oczekuje na +rep",
    icon: "🟠",
    color: COLOR_YELLOW,
    summary: "Ticket został oznaczony jako zrealizowany i czeka na legit check od klienta.",
    ticketChannel: channel,
    ownerId: ticketOwnerId,
    actorId: interaction.user.id,
    claimedById: ticketData?.claimedBy || null,
    ticketMeta: ticketData,
    statusLabel: "OCZEKUJE NA +REP",
    detailLines: [
      `Typ transakcji: ${typ}`,
      `Co: ${formattedCena}`,
      `Serwer: ${serwer}`,
      `Kanał legit-check: <#${legitRepChannelId}>`,
      `Wzór: ${repMessage}`,
    ],
  }).catch((err) => console.error("ticket-zakoncz log error:", err));

  console.log(`Ticket ${channel.id} oczekuje na +rep od użytkownika ${ticketOwnerId} (komenda użyta przez ${interaction.user.username})`);
}

// ----------------- /anonim handler -----------------
async function handleAnonimCommand(interaction) {
  const isOwner = interaction.user.id === interaction.guild.ownerId;
  const SELLER_ROLE_ID = "1350786945944391733";
  const hasSellerRole = interaction.member.roles.cache.has(SELLER_ROLE_ID);

  if (!isOwner && !hasSellerRole) {
    await interaction.reply({
      content: "> `❌` Brak uprawnień do użycia komendy /anonim.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const channel = interaction.channel;
  if (!isTicketChannel(channel)) {
    await interaction.reply({
      content: "> `❌` Ta **komenda** działa jedynie na **ticketach**!",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const ticketData = pendingTicketClose.get(channel.id);
  if (!ticketData || !ticketData.awaitingRep) {
    await interaction.reply({
      content: "> `❌` Brak oczekującego legit checka! Najpierw użyj komendy **/ticket-zakoncz**.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  if (ticketData.processing) {
    await interaction.reply({
      content: "> `⏳` Ten ticket jest już w trakcie zamykania...",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // Lock immediately using processing flag to prevent concurrency
  ticketData.processing = true;

  await interaction.reply({
    content: "> `⏳` Przetwarzanie anonimowego legit checka i zamykanie ticketu...",
    flags: [MessageFlags.Ephemeral],
  });

  try {
    await closeTicketAnonymously(channel, interaction.guild, interaction.user.id);
  } catch (error) {
    // Re-enable if closing failed
    ticketData.processing = false;
    console.error("Blad komendy /anonim:", error);
    await interaction.followUp({
      content: `> \`❌\` Wystąpił błąd: ${error.message}`,
      flags: [MessageFlags.Ephemeral]
    }).catch(() => null);
  }
}

// ----------------- /zamknij-z-powodem handler -----------------
async function handleZamknijZPowodemCommand(interaction) {
  const channel = interaction.channel;

  // Sprawdź czy komenda jest używana w tickecie
  if (!isTicketChannel(channel)) {
    await interaction.reply({
      content: "> `❌` × Ta **komenda** działa jedynie na **ticketach**!",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // Sprawdź czy właściciel
  if (interaction.user.id !== interaction.guild.ownerId) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // Pobierz powód
  const powodPreset = interaction.options.getString("powod");
  const powodCustom = (interaction.options.getString("powod_custom") || "").trim();
  const powod = powodCustom || powodPreset;

  const ticketOwnerId = await resolveTicketOwnerId(channel);

  if (!ticketOwnerId) {
    await interaction.reply({
      content: "> `❌` × **Nie udało się** zidentyfikować właściciela ticketu.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  try {
    const ticketMeta = ticketOwners.get(channel.id) || null;

    // Wyślij DM do właściciela ticketu
    const ticketOwner = await client.users.fetch(ticketOwnerId).catch(() => null);
    if (ticketOwner) {
      const payload = buildTicketClosedDmPayload({
        powod,
        channelId: channel.id,
        closedTimestamp: Math.floor(Date.now() / 1000),
        guildId: interaction.guild?.id || null,
      });
      await ticketOwner.send(payload).catch(() => null);
    }

    // Wyślij potwierdzenie na kanał (publicznie)
    await interaction.reply({
      content: `> \`✅\` × Ticket zamknięty z powodem: **${powod}**`,
      flags: [MessageFlags.Ephemeral],
    });

    // Zamknij ticket po 2 sekundach
    setTimeout(async () => {
      try {
        await archiveTicketOnClose(
          channel,
          interaction.user.id,
          ticketMeta,
          {
            closeMethod: "Komenda /zamknij-z-powodem",
            reason: powod,
          },
        ).catch((e) => console.error("archiveTicketOnClose error (reason):", e));

        await channel.delete(`Ticket zamknięty przez właściciela z powodem: ${powod}`);
        await commitRewardTicketClaim(channel.id).catch(() => null);
        ticketOwners.delete(channel.id);
        pendingTicketClose.delete(channel.id);

        console.log(`Ticket ${channel.id} został zamknięty przez właściciela z powodem: ${powod}`);
      } catch (closeErr) {
        console.error(`Błąd zamykania ticketu ${channel.id}:`, closeErr);
      }
    }, 2000);

  } catch (error) {
    console.error("Błąd podczas zamykania ticketu z powodem:", error);
    await interaction.reply({
      content: "> `❌` × **Wystąpił** błąd podczas zamykania ticketu.",
      flags: [MessageFlags.Ephemeral],
    });
  }
}

function buildPendingLegitCheckPayload(ticketOwnerId, thankLine, legitRepChannelId, repMessage, guildId) {
  const arrowEmoji = '<a:arrowwhite:1491476759290449984>';
  const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);
  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      "```\n" +
      "✅ New Shop × WYSTAW LEGIT CHECK\n" +
      "```"
    )
  );
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      `> ${arrowEmoji} **${thankLine}**`
    )
  );
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      `> ${arrowEmoji} **Aby zamknąć ticket wyślij legit checka na kanał**\n<#${legitRepChannelId}>`
    )
  );
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      `> \`📋\` **Wzór do skopiowania:**\n\`${repMessage}\``
    )
  );
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

  const anonBtn = new ButtonBuilder()
    .setCustomId("ticket_anon_close")
    .setLabel("︲Anonimowo")
    .setStyle(ButtonStyle.Secondary);

  const customEmoji = findGuildEmojiByName(guildId, "legit_shield") || findGuildEmojiByName(guildId, "anonim");
  if (customEmoji) {
    anonBtn.setEmoji({ id: customEmoji.id, name: customEmoji.name, animated: customEmoji.animated });
  } else {
    anonBtn.setEmoji({ id: "1521899996914647321", name: "legit_shield", animated: true });
  }

  const yesEmoji = findGuildEmojiByName(guildId, "YES") || findGuildEmojiByName(guildId, "yes") || findGuildEmojiByName(guildId, "TAK") || findGuildEmojiByName(guildId, "tak");
  const copyBtn = new ButtonBuilder()
    .setCustomId("ticket_copy_rep")
    .setLabel("︲Skopiuj wzór")
    .setStyle(ButtonStyle.Secondary);

  if (yesEmoji) {
    copyBtn.setEmoji({ id: yesEmoji.id, name: yesEmoji.name, animated: yesEmoji.animated });
  } else {
    copyBtn.setEmoji("✅");
  }

  container.addActionRowComponents(new ActionRowBuilder().addComponents(anonBtn, copyBtn));

  appendBrandFooterToContainer(container, guildId);

  return {
    components: [container],
    flags: MessageFlags.IsComponentsV2,
  };
}

async function replaceLegitCheckEmbedWithSuccess(channel, ticketData) {
  try {
    if (ticketData?.repTextMessageId) {
      const repMsg = await channel.messages.fetch(ticketData.repTextMessageId).catch(() => null);
      if (repMsg && repMsg.deletable) {
        await repMsg.delete().catch(() => null);
      }
    }

    let thankText = "Dziękujemy za zakup i zapraszamy ponownie!";
    const typLower = String(ticketData?.typ || "").toLowerCase();
    if (typLower === "sprzedaż" || typLower === "sprzedaz") {
      thankText = "Dziękujemy za sprzedaż i zapraszamy ponownie!";
    } else if (typLower === "wręczył nagrodę" || typLower === "wreczyl nagrode") {
      thankText = "Nagroda została nadana i zapraszamy ponownie!";
    }

    const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);
    container.addTextDisplayComponents(
      new TextDisplayBuilder().setContent(
        "```\n" +
        "✅ New Shop × LEGIT CHECK\n" +
        "```"
      )
    );
    container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
    container.addTextDisplayComponents(
      new TextDisplayBuilder().setContent(
        "> `📝` × **Pomyślnie wystawiono legit checka.**\n" +
        `> \`❤️\` × **${thankText}**`
      )
    );
    container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

    const shopEmoji = findGuildEmojiByName(channel.guild?.id, "shop");
    const kasaEmoji = findGuildEmojiByName(channel.guild?.id, "kasa_3");

    const btnPanelKlienta = new ButtonBuilder()
      .setLabel("︲Panel klienta")
      .setStyle(ButtonStyle.Link)
      .setURL("https://discord.com/channels/1350446732365926491/1523806115177959590");

    if (shopEmoji) {
      btnPanelKlienta.setEmoji({ id: shopEmoji.id, name: shopEmoji.name, animated: shopEmoji.animated });
    } else {
      btnPanelKlienta.setEmoji("🛒");
    }

    const btnBonusyKlientow = new ButtonBuilder()
      .setLabel("︲Bonusy klientów")
      .setStyle(ButtonStyle.Link)
      .setURL("https://discord.com/channels/1350446732365926491/1521896517055807601");

    if (kasaEmoji) {
      btnBonusyKlientow.setEmoji({ id: kasaEmoji.id, name: kasaEmoji.name, animated: kasaEmoji.animated });
    } else {
      btnBonusyKlientow.setEmoji("💵");
    }

    container.addActionRowComponents(
      new ActionRowBuilder().addComponents(btnPanelKlienta, btnBonusyKlientow)
    );

    appendBrandFooterToContainer(container, channel.guild?.id || null);

    let targetMsg = null;
    if (ticketData?.embedMessageId) {
      targetMsg = await channel.messages.fetch(ticketData.embedMessageId).catch(() => null);
    }

    if (!targetMsg && channel.messages) {
      const recent = await channel.messages.fetch({ limit: 10 }).catch(() => null);
      if (recent) {
        targetMsg = recent.find((m) =>
          m.author.id === client.user.id &&
          m.components && m.components.length > 0
        ) || null;
      }
    }

    if (targetMsg) {
      await targetMsg.edit({
        content: null,
        embeds: [],
        components: [container],
        files: [],
        flags: MessageFlags.IsComponentsV2,
      }).catch((err) => console.error("[replaceLegitCheckEmbedWithSuccess] edit error:", err));
    } else {
      await channel.send({
        components: [container],
        flags: MessageFlags.IsComponentsV2,
      }).catch((err) => console.error("[replaceLegitCheckEmbedWithSuccess] send error:", err));
    }
  } catch (err) {
    console.error("[LegitCheck] Błąd podmieniania embeda:", err);
  }
}

async function handleTestLegitCheckSukcesCommand(interaction) {
  if (!isAdminOrSeller(interaction.member)) {
    await interaction.reply({
      content: "> `‼️` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const channel = interaction.channel;
  if (!channel) return;

  const ticketData = pendingTicketClose.get(channel.id) || {
    typ: "zakup",
    co: "50 PLN",
    serwer: "ANARCHIA LIFESTEAL"
  };

  await interaction.reply({
    content: "> `🧪` × Testowe podmienienie embeda na sukces...",
    flags: [MessageFlags.Ephemeral],
  });

  await replaceLegitCheckEmbedWithSuccess(channel, ticketData);
}

async function handleTestLegitCheckWzorCommand(interaction) {
  if (!isAdminOrSeller(interaction.member)) {
    await interaction.reply({
      content: "> `‼️` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });

  const legitRepChannelId = "1449840030947217529";
  const typ = interaction.options.getString("typ") || "zakup";
  const ile = interaction.options.getInteger("ile") || 50;
  const serwer = (interaction.options.getString("serwer") || "ANARCHIA LIFESTEAL").trim();
  const formattedCena = `${ile} PLN`;

  let thankLine = "Dziękujemy za zakup w naszym sklepie";
  const typLower = typ.toLowerCase();
  if (typLower === "sprzedaż" || typLower === "sprzedaz") {
    thankLine = "Dziękujemy za sprzedaż w naszym sklepie";
  } else if (typLower === "wręczył nagrodę" || typLower === "wreczyl nagrode") {
    thankLine = "Nagroda została nadana";
  }

  const verb = typ.toUpperCase();
  const repMessage = `+rep @${interaction.user.username} ${verb} ${formattedCena}${serwer ? ` ${serwer}` : ""}`;

  try {
    const payload = buildPendingLegitCheckPayload(
      interaction.user.id,
      thankLine,
      legitRepChannelId,
      repMessage,
      interaction.guild?.id || null
    );

    const pingMsg = await interaction.channel.send({
      content: `<@${interaction.user.id}>`,
      allowedMentions: { users: [interaction.user.id] },
    }).catch(() => null);

    const sentEmbedMsg = await interaction.channel.send(payload);

    const sentRepMsg = await interaction.channel.send({
      content: repMessage,
    }).catch(() => null);

    pendingTicketClose.set(interaction.channelId, {
      userId: interaction.user.id,
      commandUserId: interaction.user.id,
      commandUsername: interaction.user.username,
      typ: typ,
      co: formattedCena,
      serwer: serwer,
      awaitingRep: true,
      legitRepChannelId,
      embedMessageId: sentEmbedMsg?.id || null,
      repTextMessageId: sentRepMsg?.id || null,
      repMessage: repMessage,
      pingMessageId: pingMsg?.id || null,
      isTest: true,
      ts: Date.now()
    });

    if (sentEmbedMsg?.id) {
      ticketRepMessages.set(sentEmbedMsg.id, repMessage);
    }
    ticketRepMessages.set(interaction.channelId, repMessage);

    await interaction.editReply({
      content: "> `🧪` × Wysłano testowy panel oczekiwania na legit checka ze wzorem.",
    });
  } catch (err) {
    console.error("[handleTestLegitCheckWzorCommand] Błąd:", err);
    await interaction.editReply({
      content: `> \`❌\` × Błąd wysyłania panelu: **${err.message}**`,
    });
  }
}

async function handleTestPvCommand(interaction) {
  if (!isAdminOrSeller(interaction.member)) {
    await interaction.reply({
      content: "> `‼️` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });

  const typ = interaction.options.getString("typ");
  const user = interaction.user;
  const expiryTimestamp = Math.floor(Date.now() / 1000) + 7 * 24 * 3600; // za 7 dni

  const payloadsToSend = [];

  if (typ === "wygrana-znizka" || typ === "wszystkie") {
    payloadsToSend.push(
      buildCodeDeliveryDmPayload({
        code: "TEST-DISCOUNT-10",
        rewardLine: "> `💸` × **Otrzymałeś:** `-10%`",
        expiryTimestamp,
        instructionText: PURCHASE_CODE_USAGE_TEXT,
        guildId: interaction.guild?.id || null,
      })
    );
  }

  if (typ === "wygrana-kasa" || typ === "wszystkie") {
    payloadsToSend.push(
      buildCodeDeliveryDmPayload({
        code: "TEST-REWARD-50K",
        rewardLine: "> `💸` × **Wygrałeś:** `50k$ na Anarchia LIFESTEAL`",
        expiryTimestamp,
        instructionText: REWARD_CODE_USAGE_TEXT,
        guildId: interaction.guild?.id || null,
      })
    );
  }

  if (typ === "wygrana-przedmiot" || typ === "wszystkie") {
    payloadsToSend.push(
      buildCodeDeliveryDmPayload({
        code: "TEST-ITEM-MIECZ",
        rewardLine: "> `💸` × **Wygrałeś:** `Anarchiczny miecz na Anarchia LF`",
        expiryTimestamp,
        instructionText: REWARD_CODE_USAGE_TEXT,
        guildId: interaction.guild?.id || null,
      })
    );
  }

  if (typ === "wygrana-zaproszenia" || typ === "wszystkie") {
    payloadsToSend.push(
      buildCodeDeliveryDmPayload({
        code: "TEST-INVITE-90K",
        rewardLine: "> `💸` × **Wygrałeś:** `90k$ na Anarchia LF`",
        expiryTimestamp,
        instructionText: REWARD_CODE_USAGE_TEXT,
        guildId: interaction.guild?.id || null,
      })
    );
  }

  if (typ === "wezwanie" || typ === "wszystkie") {
    payloadsToSend.push(
      buildWezwijDmPayload({
        isOwner: true,
        channelLink: "https://discord.com/channels/1350446732365926491/1449453853731983484",
        channelId: "1449453853731983484",
        guildId: interaction.guild?.id || null,
      })
    );
  }

  if (typ === "auto-odpowiedz" || typ === "wszystkie") {
    payloadsToSend.push(
      buildDmAutoReplyPayload(interaction.guild?.id || null)
    );
  }

  if (typ === "zamkniecie-ticketu" || typ === "wszystkie") {
    payloadsToSend.push(
      buildTicketClosedDmPayload({
        powod: "Brak odpowiedzi",
        channelId: "1449453853731983484",
        closedTimestamp: Math.floor(Date.now() / 1000),
        guildId: interaction.guild?.id || null,
      })
    );
  }

  if (typ === "weryfikacja" || typ === "wszystkie") {
    const vContainer = new ContainerBuilder().setAccentColor(COLOR_BLUE);
    vContainer.addTextDisplayComponents(
      new TextDisplayBuilder().setContent(
        "```\n" +
        "🛒 New Shop × WERYFIKACJA\n" +
        "```"
      )
    );
    vContainer.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
    vContainer.addTextDisplayComponents(
      new TextDisplayBuilder().setContent(
        "`✨` Gratulacje!\n\n" +
        "`📝` Pomyślnie przeszedłeś weryfikacje na naszym serwerze discord życzymy udanych zakupów!"
      )
    );
    vContainer.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
    appendBrandFooterToContainer(vContainer, interaction.guild?.id || null);

    payloadsToSend.push({
      components: [vContainer],
      flags: MessageFlags.IsComponentsV2,
    });
  }

  try {
    for (const payload of payloadsToSend) {
      await user.send(payload);
    }

    await interaction.editReply({
      content: `> \`✅\` × **Pomyślnie wysłano testowe wiadomości PV (${payloadsToSend.length}) na Twoje prywatne wiadomości!** Sprawdź swoje PV.`,
    });
  } catch (err) {
    console.error("Błąd w handleTestPvCommand (DM blocked):", err);
    await interaction.editReply({
      content: "> `❌` × **Nie udało się wysłać prywatnej wiadomości!** Upewnij się, że masz włączone wiadomości prywatne (DM) z członkami serwera w ustawieniach prywatności Discorda.",
    });
  }
}

// ===== Auto Legit Check (3-5 legit checków dziennie o losowych godzinach) =====
const AUTO_LC_SELLER_ID = "1305200545979437129"; // @Kolczasty
const AUTO_LC_BLACKOUT_START_HOUR = 0;   // od 00:00 nie wystawiamy
const AUTO_LC_BLACKOUT_END_HOUR = 7;     // do 07:00 (włącznie) przerwa nocna

// Kwoty okrągłe z wagami (częstość w realnych wpisach: 10 najczęstsze itd.)
const AUTO_LC_ROUND_AMOUNTS = [
  10, 10, 10, 10, 10,
  20, 20, 20, 20,
  30, 30, 30, 30,
  50, 50, 50, 50,
  5, 5, 100, 100,
];
let autoLcEnabled = true;
let autoLcTimer = null;
let autoLcNextFireAt = 0;
let autoLcDailyQueue = [];

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function pickAutoLcAmount() {
  if (Math.random() < 0.85) {
    return AUTO_LC_ROUND_AMOUNTS[randomInt(0, AUTO_LC_ROUND_AMOUNTS.length - 1)];
  }
  return randomInt(5, 100);
}

function pickAutoLcServer() {
  return Math.random() < 0.95 ? "ANARCHIA LIFESTEAL" : "MINESTAR SKYPVP";
}

function pickAutoLcVerb() {
  return Math.random() < 0.9 ? "ZAKUP" : "SPRZEDAŻ";
}

function isAutoLcInBlackout(now = new Date()) {
  const hour = getWarsawDateParts(now).hour;
  if (AUTO_LC_BLACKOUT_START_HOUR <= AUTO_LC_BLACKOUT_END_HOUR) {
    return hour >= AUTO_LC_BLACKOUT_START_HOUR && hour <= AUTO_LC_BLACKOUT_END_HOUR;
  }
  return hour >= AUTO_LC_BLACKOUT_START_HOUR || hour <= AUTO_LC_BLACKOUT_END_HOUR;
}

function getWarsawDateString(date = new Date()) {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Warsaw",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date);
}

function getWarsawEpochForDate(dateStr, hour, minute, seconds = 0) {
  const baseUtc = Date.parse(`${dateStr}T${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:${String(seconds).padStart(2, "0")}Z`);
  for (let offsetHours = -4; offsetHours <= 4; offsetHours++) {
    const testTs = baseUtc + offsetHours * 3600000;
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone: "Europe/Warsaw",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hourCycle: "h23",
    }).formatToParts(new Date(testTs));
    const v = (t) => parts.find((p) => p.type === t)?.value;
    if (
      `${v("year")}-${v("month")}-${v("day")}` === dateStr &&
      Number(v("hour")) === hour &&
      Number(v("minute")) === minute &&
      Number(v("second")) === seconds
    ) {
      return testTs;
    }
  }
  return null;
}

// Generuje losowo od 3 do 5 legit checków w ciągu dnia (w godz. 08:00 - 22:45)
function generateDailyAutoLcTimes(dateStr) {
  const count = randomInt(3, 5); // 3 do 5 LC na dzień
  const startMin = 8 * 60; // 08:00 (poza nocną przerwą 00:00-07:00)
  const endMin = 22 * 60 + 45; // 22:45
  const totalMin = endMin - startMin;
  const slotSize = totalMin / count;

  const times = [];
  for (let i = 0; i < count; i++) {
    const slotStart = startMin + i * slotSize;
    const slotEnd = startMin + (i + 1) * slotSize;
    const pickMin = Math.floor(slotStart + 5 + Math.random() * (slotEnd - slotStart - 10));
    const hour = Math.floor(pickMin / 60);
    const minute = pickMin % 60;
    const sec = Math.floor(Math.random() * 60);
    const epoch = getWarsawEpochForDate(dateStr, hour, minute, sec);
    if (epoch) times.push(epoch);
  }
  times.sort((a, b) => a - b);
  return times;
}

function getNextScheduledAutoLcTimestamp() {
  const now = Date.now();
  autoLcDailyQueue = autoLcDailyQueue.filter((ts) => ts > now + 1000);
  if (autoLcDailyQueue.length > 0) {
    return autoLcDailyQueue[0];
  }
  const todayStr = getWarsawDateString(new Date());
  const todayTimes = generateDailyAutoLcTimes(todayStr).filter((ts) => ts > now + 1000);
  if (todayTimes.length > 0) {
    autoLcDailyQueue = todayTimes;
    return autoLcDailyQueue[0];
  }
  const tomorrow = new Date(now + 24 * 60 * 60 * 1000);
  const tomorrowStr = getWarsawDateString(tomorrow);
  autoLcDailyQueue = generateDailyAutoLcTimes(tomorrowStr);
  return autoLcDailyQueue[0] || null;
}

async function postAutoLegitCheck() {
  try {
    const repChannel = await client.channels.fetch(REP_CHANNEL_ID).catch(() => null);
    if (!repChannel) {
      console.error("[auto-lc] Nie znaleziono kanału legit-check.");
      return;
    }

    const amount = pickAutoLcAmount();
    const verb = pickAutoLcVerb();
    const server = pickAutoLcServer();
    const repText = `+rep <@${AUTO_LC_SELLER_ID}> ${verb} ${amount} PLN ${server}`;

    console.log(`[auto-lc] Wystawiam automatyczny legit check: ${repText}`);
    const sentAnonRep = await sendAnonRep(repChannel, repText);

    if (legitRepHistoryInitialized && sentAnonRep?.id) {
      legitRepMessageIds.add(sentAnonRep.id);
      legitRepCount = legitRepMessageIds.size;
    } else {
      legitRepCount++;
    }

    const dailyTransactionAmount = isDailyLegitMoneyTransaction(verb.toLowerCase()) ? amount : 0;
    recordDailyLegitRep("anonimowy", dailyTransactionAmount);
    scheduleRepChannelRename(repChannel, legitRepCount).catch(() => null);
    scheduleSavePersistentState();

    // Tak jak przy anonimowym LC z ticketu: prześlij nowy panel "LEGIT CHECKI"
    // na dół kanału i usuń stary (żeby wiadomości wyglądały jak od prawdziwego gracza).
    await sendLegitCheckInfoMessage(repChannel).catch((error) =>
      console.error("[auto-lc] Błąd wysyłania panelu legit-check:", error),
    );

    console.log(`[auto-lc] +rep wystawione przez bota, licznik: ${legitRepCount}`);
  } catch (err) {
    console.error("[auto-lc] Błąd wystawiania auto legit checka:", err);
  }
}

function scheduleRandomAutoLegitCheck() {
  if (autoLcTimer) {
    clearTimeout(autoLcTimer);
    autoLcTimer = null;
  }

  if (!autoLcEnabled) {
    console.log("[auto-lc] Timer auto LC jest wyłączony (autoLcEnabled = false), pomijam planowanie.");
    return;
  }

  const now = Date.now();
  let nextTargetTs = (autoLcNextFireAt > now + 1000) ? autoLcNextFireAt : getNextScheduledAutoLcTimestamp();
  if (!nextTargetTs) {
    console.error("[auto-lc] Nie udało się wygenerować kolejnego terminu LC.");
    return;
  }
  autoLcNextFireAt = nextTargetTs;
  scheduleSavePersistentState();

  const delay = Math.max(1000, nextTargetTs - now);
  const hours = (delay / 3600000).toFixed(1);
  console.log(`[auto-lc] Następny automatyczny legit check za ${hours} h (${new Date(autoLcNextFireAt).toLocaleString("pl-PL", { timeZone: "Europe/Warsaw" })}).`);
  autoLcTimer = setTimeout(async () => {
    autoLcNextFireAt = 0;
    // Zaplanuj kolejny
    scheduleRandomAutoLegitCheck();
    try {
      await postAutoLegitCheck();
    } catch (err) {
      console.error("[auto-lc] Błąd w callbacku timera:", err);
    }
  }, delay);
}

async function closeTicketAnonymously(channel, guild, executorId) {
  const ticketData = pendingTicketClose.get(channel.id);
  if (!ticketData || !ticketData.awaitingRep) {
    throw new Error("Brak oczekującego legit checka! Najpierw użyj komendy **/ticket-zakoncz**.");
  }

  // Jeśli to tylko test komendą /test-legit-check-wzor, tylko podmień embed bez wysyłania prawdziwego repa i bez zamykania kanału
  if (ticketData.isTest) {
    await replaceLegitCheckEmbedWithSuccess(channel, ticketData);
    pendingTicketClose.delete(channel.id);
    return;
  }

  const repChannel = await client.channels.fetch(ticketData.legitRepChannelId).catch(() => null);
  if (!repChannel) {
    throw new Error("Nie można znaleźć kanału w bazie (legit checki).");
  }

  let verb = "wystawił/a";
  if (ticketData.typ === "zakup") verb = "ZAKUP";
  else if (ticketData.typ === "sprzedaz" || ticketData.typ === "sprzedaż") verb = "SPRZEDAŻ";
  else if (ticketData.typ === "wreczyl nagrode" || ticketData.typ === "wręczył nagrodę") verb = "WRĘCZYŁ NAGRODĘ";

  let simulatedRepText = `+rep <@${ticketData.commandUserId}> ${verb} ${ticketData.co}`;
  if (ticketData.serwer) {
    simulatedRepText += ` ${ticketData.serwer}`;
  }

  // (Wydatki zostały już zaktualizowane automatycznie przy uruchomieniu komendy /ticket-zakoncz)

  // Send via webhook as "Anonimowy LC" if possible
  const sentAnonRep = await sendAnonRep(repChannel, simulatedRepText);

  if (legitRepHistoryInitialized && sentAnonRep?.id) {
    legitRepMessageIds.add(sentAnonRep.id);
    legitRepCount = legitRepMessageIds.size;
  } else {
    legitRepCount++;
  }
  const dailyTransactionAmount = isDailyLegitMoneyTransaction(ticketData.typ)
    ? parsePLN(ticketData.co)
    : 0;
  recordDailyLegitRep("anonimowy", dailyTransactionAmount);
  console.log(`[anonim] +rep wystawione przez bota, licznik: ${legitRepCount}`);

  if (ticketData.commandUserId) {
    const current = sellerTicketCounts.get(ticketData.commandUserId) || 0;
    sellerTicketCounts.set(ticketData.commandUserId, current + 1);
  }

  if (String(ticketData.typ || "").toLowerCase() === "zakup" && String(ticketData.commandUserId) !== "1305200545979437129") {
    try {
      const rozliczeniaChannel = await client.channels.fetch("1449162620807675935").catch(() => null);
      if (rozliczeniaChannel && rozliczeniaChannel.isTextBased()) {
        const pingMsg = await rozliczeniaChannel.send({
          content: `<@${ticketData.commandUserId}>`,
          allowedMentions: { users: [ticketData.commandUserId] },
        }).catch((err) => console.error("[rozliczenia-ping] Błąd wysyłania powiadomienia:", err));

        if (pingMsg && pingMsg.deletable) {
          setTimeout(() => {
            pingMsg.delete().catch(() => null);
          }, 5000);
        }
      }
    } catch (rozliczenieErr) {
      console.error("[rozliczenia-ping] Błąd:", rozliczenieErr);
    }
  }

  scheduleRepChannelRename(repChannel, legitRepCount).catch(() => null);
  scheduleSavePersistentState();

  await sendLegitCheckInfoMessage(repChannel).catch((error) =>
    console.error("[legit-check] Błąd panelu po anonimowym checku:", error),
  );

  // Podmień embed na sukces i usuń stary wzór + przycisk
  await replaceLegitCheckEmbedWithSuccess(channel, ticketData);

  const ticketMeta = ticketOwners.get(channel.id) || null;

  setTimeout(async () => {
    try {
      await archiveTicketOnClose(channel, executorId, ticketMeta, {
        closeMethod: "Automatyczne zamknięcie po anonimowym legit checku",
      }).catch(() => null);

      await channel.delete('Ticket zamknięty po anonimowym legit checku').catch(() => null);
      pendingTicketClose.delete(channel.id);
      await commitRewardTicketClaim(channel.id).catch(() => null);
      ticketOwners.delete(channel.id);
    } catch (closeErr) {
      console.error("Błąd zamykania kanału w closeTicketAnonymously:", closeErr);
    }
  }, 10000);
}


async function sendLegitCheckInfoMessage(channel) {
  const footerUserId = "1305200545979437129";
  const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);
  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      "```\n" +
      "✅ New Shop × LEGIT CHECKI\n" +
      "```",
    ),
  );
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      "### `🗒️` × Jak napisać:\n" +
      "> `+rep @sprzedawca [ZAKUP/SPRZEDAŻ] [ILE] PLN [SERWER]`",
    ),
  );
  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent(
      "### `📝` × Przykład:\n" +
      `> **+rep** <@${footerUserId}> **ZAKUP 50 PLN ANARCHIA LIFESTEAL**`,
    ),
  );
  appendBrandFooterToContainer(container, channel.guildId);

  // Znajdź wszystkie stare panele wysłane przez bota (niezależnie od restartów)
  const oldPanels = [];
  try {
    const recent = await channel.messages.fetch({ limit: 30 }).catch(() => null);
    if (recent) {
      for (const msg of recent.values()) {
        // Tylko wiadomości bota posiadające komponenty/embed z instrukcją (nigdy zwykłe wiadomości +rep)
        if (msg.author?.id === client.user.id && (msg.components?.length > 0 || msg.embeds?.length > 0)) {
          const rawStr = JSON.stringify(msg.components || []) + " " + JSON.stringify(msg.embeds || []);
          if (rawStr.includes("Jak napisać") || rawStr.includes("LEGIT CHECKI")) {
            oldPanels.push(msg);
          }
        }
      }
    }
  } catch (err) {
    console.error("[legit-panel] Błąd pobierania starych paneli:", err);
  }

  const previousId = repLastInfoMessage.get(channel.id);
  if (previousId && !oldPanels.some((m) => m.id === previousId)) {
    const prev = await channel.messages.fetch(previousId).catch(() => null);
    if (prev) oldPanels.push(prev);
  }

  const newInfoMessage = await channel.send({
    components: [container],
    flags: MessageFlags.IsComponentsV2,
    allowedMentions: { parse: [] },
  });
  repLastInfoMessage.set(channel.id, newInfoMessage.id);
  scheduleSavePersistentState(true);

  // Usuń wszystkie stare panele, aby zawsze na dole był tylko 1 aktualny
  for (const oldMsg of oldPanels) {
    if (oldMsg.id !== newInfoMessage.id && oldMsg.deletable) {
      await oldMsg.delete().catch(() => null);
    }
  }
  return newInfoMessage;
}

// Helper to send anonymous rep using Webhooks
async function sendAnonRep(channel, content) {
  // Anonymous and manually posted legit checks use the same format rules.
  if (!parseLegitRepContent(content)) {
    throw new Error("Nieprawidłowy wzór anonimowego legit checka.");
  }
  try {
    let webhooks = await channel.fetchWebhooks().catch(() => null);
    let webhook = webhooks ? webhooks.find(w => w.owner?.id === client.user.id) : null;
    if (!webhook && channel.guild.members.me.permissions.has(PermissionFlagsBits.ManageWebhooks)) {
      webhook = await channel.createWebhook({
        name: 'Anonimowy LC',
        avatar: client.user.displayAvatarURL(),
        reason: 'Anonimowe legit checki'
      }).catch(() => null);
    }
    if (webhook) {
      return await webhook.send({
        content: content,
        username: 'Anonimowy LC',
        avatarURL: client.user.displayAvatarURL(),
        wait: true,
      });
    }
  } catch (err) {
    console.error("Failed to send anon rep via webhook:", err);
  }
  return await channel.send({ content: content });
}

// Helper to parse PLN amount from a string
function parsePLN(str) {
  if (!str) return 0;
  const match = str.replace(/\s/g, '').match(/(\d+(?:[.,]\d+)?)/);
  if (match) {
    return parseFloat(match[1].replace(',', '.'));
  }
  return 0;
}

const COMMAND_EXTERNAL_TIMEOUT_MS = 6000;

async function runCommandExternal(
  operation,
  label,
  attempts = 2,
  timeoutMs = COMMAND_EXTERNAL_TIMEOUT_MS,
) {
  let lastError = null;

  for (let attempt = 1; attempt <= attempts; attempt++) {
    let timeoutId;
    const controller = new AbortController();
    try {
      const operationPromise = Promise.resolve().then(() => operation(controller.signal));
      const timeoutPromise = new Promise((_, reject) => {
        timeoutId = setTimeout(() => {
          controller.abort();
          const error = new Error(`${label}: timeout po ${timeoutMs}ms`);
          error.code = "COMMAND_EXTERNAL_TIMEOUT";
          reject(error);
        }, timeoutMs);
      });

      return await Promise.race([operationPromise, timeoutPromise]);
    } catch (error) {
      lastError = error;
      console.error(
        `[COMMAND] ${label} nie powiodło się (próba ${attempt}/${attempts}):`,
        error?.message || error,
      );
      if (attempt < attempts) {
        await new Promise((resolve) => setTimeout(resolve, 350));
      }
    } finally {
      if (timeoutId) clearTimeout(timeoutId);
    }
  }

  throw lastError || new Error(`${label}: nieznany błąd`);
}

// ----------------- /wydane handler -----------------
async function handleWydaneCommand(interaction) {
  const targetUser = interaction.options.getUser("gracz") || interaction.user;
  const isSelf = targetUser.id === interaction.user.id;

  if (!isSelf) {
    const isOwner = interaction.user.id === interaction.guild.ownerId;
    const SELLER_ROLE_ID = "1350786945944391733";
    const hasSellerRole = interaction.member.roles.cache.has(SELLER_ROLE_ID);
    if (!isOwner && !hasSellerRole) {
      await interaction.reply({
        content: "> `❌` × Brak uprawnień do sprawdzania wydanej kwoty innych graczy.",
        flags: [MessageFlags.Ephemeral]
      });
      return;
    }
  }

  try {
    console.log(
      `[/wydane] Start user=${interaction.user.id} target=${targetUser.id} guild=${interaction.guildId}`,
    );
    const spent = await runCommandExternal(
      (signal) => db.getUserSpent(targetUser.id, interaction.guildId || "default", { signal }),
      "/wydane: odczyt Supabase",
      1,
      2000,
    );
    console.log(`[/wydane] Supabase zakończone target=${targetUser.id} spent=${spent}`);
    
    const embed = new EmbedBuilder()
      .setColor(COLOR_BLUE)
      .setDescription(
        "```\n" +
        "✅ New Shop × STATYSTYKI ZAKUPÓW\n" +
        "```\n" +
        `<a:arrowwhite:1491476759290449984> Użytkownik: <@${targetUser.id}>\n` +
        `<a:arrowwhite:1491476759290449984> Łączna kwota wydana w sklepie: **${spent.toFixed(2)} PLN**`
      );

    await interaction.reply({ embeds: [embed], flags: [MessageFlags.Ephemeral] });
    console.log(`[/wydane] Odpowiedź wysłana target=${targetUser.id}`);
  } catch (err) {
    console.error("Błąd w komendzie /wydane:", err);
    const errorPayload = {
      content: "> `❌` Nie udało się teraz pobrać danych z bazy. Spróbuj ponownie za chwilę.",
      flags: [MessageFlags.Ephemeral],
    };
    const responsePromise = interaction.deferred || interaction.replied
      ? interaction.editReply(errorPayload)
      : interaction.reply(errorPayload);
    await responsePromise.catch((replyError) => {
      console.error("[/wydane] Nie udało się zakończyć odroczonej odpowiedzi:", replyError);
    });
  }
}

// ----------------- /topwydane handler -----------------
async function handleTopWydaneCommand(interaction) {
  if (!isGuildOwner(interaction)) {
    await interaction.reply({ content: "> `❌` × Ta komenda jest dostępna tylko dla właściciela serwera.", flags: [MessageFlags.Ephemeral] });
    return;
  }
  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });

  try {
    const topList = await db.getTopSpenders(10, interaction.guildId || "default");
    
    if (topList.length === 0) {
      await interaction.editReply({
        content: "> `ℹ️` × Brak danych o wydatkach graczy w tym sklepie."
      });
      return;
    }

    let description = "```\n" +
      "✅ New Shop × TOP SPENDERS (NAJWIĘCEJ WYDANE)\n" +
      "```\n";

    for (let i = 0; i < topList.length; i++) {
      const entry = topList[i];
      const amount = Number(entry.amount) || 0;
      description += `${i + 1}. <@${entry.user_id}> – **${amount.toFixed(2)} PLN**\n`;
    }

    const embed = new EmbedBuilder()
      .setColor(COLOR_BLUE)
      .setDescription(description);

    await interaction.editReply({ embeds: [embed] });
  } catch (err) {
    console.error("Błąd w komendzie /topwydane:", err);
    await interaction.editReply({ content: "> `❌` Wystąpił błąd podczas pobierania danych o top spenders." });
  }
}

async function handleShopTotalsCommand(interaction) {
  if (!isAdminOrSeller(interaction.member)) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });

  try {
    const totals = await getLegitChannelTotals();
    const formatPln = (value) =>
      Number(value || 0).toLocaleString("pl-PL", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      });

    const embed = new EmbedBuilder()
      .setColor(COLOR_BLUE)
      .setDescription(
        "```\n" +
        "💰 New Shop × WYDATKI CAŁEGO SHOPA\n" +
        "```\n" +
        `> \`✅\` × **Wszystkie legit checki:** ${totals.legitCount}\n` +
        `> \`🛒\` × **Zakupy:** ${totals.purchaseCount} transakcji — ${formatPln(totals.purchaseAmount)} PLN\n` +
        `> \`📦\` × **Sprzedaże:** ${totals.saleCount} transakcji — ${formatPln(totals.saleAmount)} PLN\n` +
        `> \`📊\` × **Łączny obrót:** ${formatPln(totals.transactionAmount)} PLN`,
      )
      .setTimestamp()
      .setBrandFooter();

    await interaction.editReply({ embeds: [embed] });
  } catch (error) {
    console.error("Błąd komendy /wydatkishop:", error);
    await interaction.editReply({
      content: "> `❌` × Nie udało się pobrać łącznych statystyk sklepu.",
    });
  }
}

async function getLegitChannelTotals() {
  const channel = await client.channels.fetch(REP_CHANNEL_ID).catch(() => null);
  if (!channel?.isTextBased?.()) {
    throw new Error(`Nie znaleziono kanału legit-check ${REP_CHANNEL_ID}.`);
  }

  let before;
  const totals = {
    legitCount: 0,
    purchaseCount: 0,
    purchaseAmount: 0,
    saleCount: 0,
    saleAmount: 0,
    transactionAmount: 0,
  };

  while (true) {
    const options = { limit: 100 };
    if (before) options.before = before;
    const messages = await channel.messages.fetch(options);
    if (!messages.size) break;

    for (const message of messages.values()) {
      if (!isLegitRepMessage(message)) continue;
      totals.legitCount += 1;

      const normalized = String(message.content || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "");
      const transaction = normalized.match(
        /\b(ZAKUP|SPRZEDAZ)\s+(\d+(?:[.,]\d+)?)\s*PLN\b/i,
      );
      if (!transaction) continue;

      const type = transaction[1].toUpperCase();
      const amount = Number(transaction[2].replace(",", ".")) || 0;
      if (type === "ZAKUP") {
        totals.purchaseCount += 1;
        totals.purchaseAmount += amount;
      } else {
        totals.saleCount += 1;
        totals.saleAmount += amount;
      }
    }

    before = messages.last()?.id;
    if (messages.size < 100 || !before) break;
  }

  totals.transactionAmount = totals.purchaseAmount + totals.saleAmount;
  return totals;
}

// ----------------- /usunzakup handler -----------------
async function handleUsunZakupCommand(interaction) {
  const isOwner = interaction.user.id === interaction.guild.ownerId;
  const SELLER_ROLE_ID = "1350786945944391733";
  const hasSellerRole = interaction.member.roles.cache.has(SELLER_ROLE_ID);

  if (!isOwner && !hasSellerRole) {
    return interaction.reply({
      content: "> `❌` × Nie masz uprawnień do użycia tej komendy.",
      flags: [MessageFlags.Ephemeral]
    });
  }

  const targetUser = interaction.options.getUser("gracz");
  const guildId = interaction.guildId || "default";

  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });

  try {
    const purchases = await db.getUserPurchases(targetUser.id, guildId);

    if (!purchases || purchases.length === 0) {
      const embed = new EmbedBuilder()
        .setColor(COLOR_BLUE)
        .setDescription(
          "```\n" +
          "❌ New Shop × USUŃ ZAKUP\n" +
          "```\n" +
          `> ❌ × Użytkownik <@${targetUser.id}> nie ma żadnych zakupów w historii.`
        )
        .setBrandFooter();
      return interaction.editReply({ embeds: [embed] });
    }

    await sendUsunZakupPage(interaction, targetUser, purchases, 0);
  } catch (err) {
    console.error("Błąd w komendzie /usunzakup:", err);
    await interaction.editReply({ content: "> `❌` Wystąpił błąd podczas odczytu zakupów." });
  }
}

async function sendUsunZakupPage(interactionOrMsg, targetUser, purchases, pageIndex) {
  const itemsPerPage = 5;
  const totalPages = Math.ceil(purchases.length / itemsPerPage);
  const safePage = Math.max(0, Math.min(pageIndex, totalPages - 1));
  const start = safePage * itemsPerPage;
  const end = Math.min(start + itemsPerPage, purchases.length);
  const pagePurchases = purchases.slice(start, end);

  const descriptionParts = [
    "```\n" +
    "🗑️ New Shop × USUŃ ZAKUP\n" +
    "```\n" +
    `> \`👤\` × **Gracz:** <@${targetUser.id}>\n` +
    `> \`📄\` × **Ilość zakupów:** ${purchases.length}\n`
  ];

  for (const p of pagePurchases) {
    const ts = Math.floor(new Date(p.created_at).getTime() / 1000);
    descriptionParts.push(
      `> <:arrow:1491439525025095681> \`[${p.id}]\` <t:${ts}:d> <t:${ts}:t> — **${p.price} PLN** — ${p.server || "Brak"}`
    );
  }

  const row = new ActionRowBuilder();

  if (safePage > 0) {
    row.addComponents(
      new ButtonBuilder()
        .setCustomId(`usunzakup_page_${targetUser.id}_${safePage - 1}`)
        .setLabel("◀ Poprzednia")
        .setStyle(ButtonStyle.Secondary)
    );
  }

  if (safePage < totalPages - 1) {
    row.addComponents(
      new ButtonBuilder()
        .setCustomId(`usunzakup_page_${targetUser.id}_${safePage + 1}`)
        .setLabel("Następna ▶")
        .setStyle(ButtonStyle.Secondary)
    );
  }

  // Dodaj przycisk usuwania dla każdego zakupu na stronie
  for (const p of pagePurchases) {
    row.addComponents(
      new ButtonBuilder()
        .setCustomId(`usunzakup_del_${targetUser.id}_${p.id}`)
        .setLabel(`Usuń #${p.id}`)
        .setStyle(ButtonStyle.Danger)
    );
  }

  const embed = new EmbedBuilder()
    .setColor(COLOR_BLUE)
    .setDescription(descriptionParts.join("\n"))
    .setBrandFooter();

  const msgData = { embeds: [embed], components: row.components.length > 0 ? [row] : [] };

  if (interactionOrMsg.isButton && interactionOrMsg.isButton()) {
    await interactionOrMsg.update(msgData);
  } else {
    await interactionOrMsg.editReply(msgData);
  }
}

// Handler dla przycisków /usunzakup
async function handleUsunZakupButton(interaction) {
  const customId = interaction.customId;
  const parts = customId.split("_");
  // Format: usunzakup_page_USERID_PAGE lub usunzakup_del_USERID_PURCHASEID
  const action = parts[1];
  const targetUserId = parts[2];
  const guildId = interaction.guildId || "default";

  if (!interaction.user.id === interaction.guild.ownerId) {
    return interaction.reply({
      content: "> `❌` × Tylko właściciel serwera może usuwać zakupy.",
      flags: [MessageFlags.Ephemeral]
    });
  }

  try {
    const targetUser = await client.users.fetch(targetUserId);

    if (action === "page") {
      const pageIndex = parseInt(parts[3], 10);
      const purchases = await db.getUserPurchases(targetUserId, guildId);
      if (!purchases || purchases.length === 0) {
        const embed = new EmbedBuilder()
          .setColor(COLOR_BLUE)
          .setDescription(
            "```\n" +
            "❌ New Shop × USUŃ ZAKUP\n" +
            "```\n" +
            `> ❌ × Użytkownik <@${targetUserId}> nie ma już żadnych zakupów.`
          )
          .setBrandFooter();
        return interaction.update({ embeds: [embed], components: [] });
      }
      await sendUsunZakupPage(interaction, targetUser, purchases, pageIndex);
    } else if (action === "del") {
      const purchaseId = parts[3];
      const purchases = await db.getUserPurchases(targetUserId, guildId);
      const purchase = purchases.find((p) => String(p.id) === purchaseId);

      if (!purchase) {
        return interaction.reply({
          content: `> ❌ Zakup #${purchaseId} już nie istnieje.`,
          flags: [MessageFlags.Ephemeral]
        });
      }

      const success = await db.deleteUserPurchase(purchaseId, guildId);
      if (!success) {
        return interaction.reply({
          content: "> ❌ Błąd podczas usuwania zakupu.",
          flags: [MessageFlags.Ephemeral]
        });
      }

      const amount = Number(purchase.price) || 0;
      if (amount > 0) {
        const currentSpent = await db.getUserSpent(targetUserId, guildId);
        const newAmount = Math.max(0, currentSpent - amount);
        await db.setUserSpent(targetUserId, newAmount, guildId);
        await syncUserSpentRoles(interaction.guild, targetUserId).catch(() => null);
      }

      await interaction.deferUpdate();

      // Odśwież listę
      const updatedPurchases = await db.getUserPurchases(targetUserId, guildId);
      if (!updatedPurchases || updatedPurchases.length === 0) {
        const embed = new EmbedBuilder()
          .setColor(COLOR_BLUE)
          .setDescription(
            "```\n" +
            "✅ New Shop × USUNIĘTO ZAKUP\n" +
            "```\n" +
            `> ✅ Usunięto zakup #${purchaseId} (<t:${Math.floor(new Date(purchase.created_at).getTime() / 1000)}:d>) użytkownika <@${targetUserId}>.\n` +
            `> Użytkownik nie ma już więcej zakupów.`
          )
          .setBrandFooter();
        await interaction.editReply({ embeds: [embed], components: [] });
      } else {
        await sendUsunZakupPage(interaction, await client.users.fetch(targetUserId), updatedPurchases, 0);
      }
    }
  } catch (err) {
    console.error("Błąd w przycisku /usunzakup:", err);
  }
}

// ----------------- /wydanestats handler -----------------
async function handleWydaneStatsCommand(interaction) {
  const isOwner = interaction.user.id === interaction.guild.ownerId;
  const SELLER_ROLE_ID = "1350786945944391733";
  const hasSellerRole = interaction.member.roles.cache.has(SELLER_ROLE_ID);

  if (!isOwner && !hasSellerRole) {
    await interaction.reply({
      content: "> `❌` × Brak uprawnień do zarządzania wydatkami.",
      flags: [MessageFlags.Ephemeral]
    });
    return;
  }

  const subcommand = interaction.options.getSubcommand();
  const targetUser = interaction.options.getUser("gracz");
  const amount = interaction.options.getInteger("kwota") || 0;
  const guildId = interaction.guildId || "default";

  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });

  try {
    if (subcommand === "dodaj") {
      await db.addUserSpent(targetUser.id, amount, guildId);
      await syncUserSpentRoles(interaction.guild, targetUser.id).catch(() => null);
      const newSpent = await db.getUserSpent(targetUser.id, guildId);
      
      const embed = new EmbedBuilder()
        .setColor(COLOR_BLUE)
        .setDescription(
          "```\n" +
          "✅ New Shop × AKTUALIZACJA WYDATKÓW\n" +
          "```\n" +
          `<a:arrowwhite:1491476759290449984> Dodano **${amount} PLN** do wydatków użytkownika <@${targetUser.id}>.\n` +
          `<a:arrowwhite:1491476759290449984> Aktualna kwota wydana: **${newSpent.toFixed(2)} PLN**`
        );
      await interaction.editReply({ embeds: [embed] });
      
    } else if (subcommand === "odejmij") {
      await db.addUserSpent(targetUser.id, -amount, guildId);
      await syncUserSpentRoles(interaction.guild, targetUser.id).catch(() => null);
      const newSpent = await db.getUserSpent(targetUser.id, guildId);
      
      const embed = new EmbedBuilder()
        .setColor(COLOR_BLUE)
        .setDescription(
          "```\n" +
          "✅ New Shop × AKTUALIZACJA WYDATKÓW\n" +
          "```\n" +
          `<a:arrowwhite:1491476759290449984> Odjęto **${amount} PLN** z wydatków użytkownika <@${targetUser.id}>.\n` +
          `<a:arrowwhite:1491476759290449984> Aktualna kwota wydana: **${newSpent.toFixed(2)} PLN**`
        );
      await interaction.editReply({ embeds: [embed] });
      
    } else if (subcommand === "ustaw") {
      await db.setUserSpent(targetUser.id, amount, guildId);
      await syncUserSpentRoles(interaction.guild, targetUser.id).catch(() => null);
      
      const embed = new EmbedBuilder()
        .setColor(COLOR_BLUE)
        .setDescription(
          "```\n" +
          "✅ New Shop × USTAWIENIE WYDATKÓW\n" +
          "```\n" +
          `<a:arrowwhite:1491476759290449984> Ustawiono kwotę wydatków użytkownika <@${targetUser.id}> na **${amount.toFixed(2)} PLN**.`
        );
      await interaction.editReply({ embeds: [embed] });
      
    } else if (subcommand === "usun") {
      const [spentDeleted, purchasesDeleted] = await Promise.all([
        db.deleteUserSpent(targetUser.id, guildId),
        db.deleteUserPurchases(targetUser.id, guildId, "zakup"),
      ]);
      if (!spentDeleted || !purchasesDeleted) {
        throw new Error("Nie udało się całkowicie usunąć wydatków i historii zakupów.");
      }
      await syncUserSpentRoles(interaction.guild, targetUser.id).catch(() => null);
      
      const embed = new EmbedBuilder()
        .setColor(COLOR_BLUE)
        .setDescription(
          "```\n" +
          "✅ New Shop × RESET WYDATKÓW\n" +
          "```\n" +
          `<a:arrowwhite:1491476759290449984> Zresetowano/usunięto wydatki użytkownika <@${targetUser.id}> (0.00 PLN).`
        );
      await interaction.editReply({ embeds: [embed] });
    }
  } catch (err) {
    console.error("Błąd w komendzie /wydanestats:", err);
    await interaction.editReply({ content: "> `❌` Wystąpił błąd podczas zapisywania zmian w bazie danych." });
  }
}

// ----------------- syncUserSpentRoles & sprawdz_bonusy utility -----------------
async function syncUserSpentRoles(guild, userId) {
  if (!guild) return;
  
  const member = await guild.members.fetch(userId).catch(() => null);
  if (!member) return;

  const spent = await db.getUserSpent(userId, guild.id);

  const roleTiers = [
    { min: 200, roleId: "1521924538265243738" },
    { min: 500, roleId: "1458145139938562058" },
    { min: 1000, roleId: "1521924656792080384" },
    { min: 2000, roleId: "1521924963190177924" }
  ];

  // Role pieniężne są wyłącznie wirtualne (zapisywane w bazie, bez nadawania rang na Discordzie)
  // Usuwamy wszystkie role pieniężne jeśli użytkownik je posiada
  for (const tier of roleTiers) {
    if (member.roles.cache.has(tier.roleId)) {
      await member.roles.remove(tier.roleId).catch((err) =>
        console.error(`[Spent Roles] Nie udało się usunąć roli ${tier.roleId} dla ${userId}:`, err)
      );
    }
  }
}

async function handleSprawdzBonusyButton(interaction) {
  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });

  const guild = interaction.guild;
  const userId = interaction.user.id;
  if (!guild) {
    await interaction.editReply({ content: "> `❌` Ta komenda działa jedynie na serwerach." });
    return;
  }

  let responsePayload;
  try {
    const spent = await db.getUserSpent(userId, guild.id);

    const roleTiers = [
      { name: "Klient 200+", min: 200, roleId: "1521924538265243738" },
      { name: "Klient 500+", min: 500, roleId: "1458145139938562058" },
      { name: "Klient 1000+", min: 1000, roleId: "1521924656792080384" },
      { name: "Klient 2000+", min: 2000, roleId: "1521924963190177924" }
    ];

    let nextTier = null;
    for (let i = 0; i < roleTiers.length; i++) {
      if (spent < roleTiers[i].min) {
        nextTier = roleTiers[i];
        break;
      }
    }

    let currentTier = null;
    for (const tier of roleTiers) {
      if (spent >= tier.min) {
        currentTier = tier;
      }
    }
    const ownedRoles = currentTier ? [`**${currentTier.name}**`] : [];

    let line1 = `<a:arrowwhite:1491476759290449984> ×  Aktualnie __nie posiadasz__ żadnej __rangi__. `;
    if (ownedRoles.length > 0) {
      line1 = `<a:arrowwhite:1491476759290449984> ×  **Posiadasz** __rangę__ ${ownedRoles.join(", ")}.`;
    }

    const line2 = `<a:arrowwhite:1491476759290449984> ×  Łącznie __wydałeś__ **${spent.toFixed(0)} PLN** w naszym sklepie.`;

    let line3 = "";
    if (nextTier) {
      line3 = `<a:arrowwhite:1491476759290449984> ×  Do __następnej rangi__ (**${nextTier.name}**) brakuje Ci **${(nextTier.min - spent).toFixed(0)} PLN**.`;
    } else {
      line3 = `<a:arrowwhite:1491476759290449984> ×  Osiągnąłeś już __najwyższą rangę__ bonusową!`;
    }

    const descContent = [
      "```",
      "💵 New Shop × TWOJE BONUSY",
      "```",
      `> ${line1}`,
      `> ${line2}`,
      `> ${line3}`,
    ].join("\n");

    const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);
    container.addTextDisplayComponents(
      new TextDisplayBuilder().setContent(descContent)
    );
    appendBrandFooterToContainer(container, guild.id);

    responsePayload = {
      components: [container],
      // Ephemeral zostało już ustawione przez deferReply.
      flags: MessageFlags.IsComponentsV2,
    };
  } catch (err) {
    console.error("Błąd w handleSprawdzBonusyButton:", err);
    responsePayload = { content: "> `❌` Wystąpił błąd podczas sprawdzania Twoich bonusów." };
  }

  // Dokładnie jedna próba końcowej odpowiedzi. Błąd sieci/API przekazujemy do
  // wspólnej obsługi zamiast natychmiast wysyłać drugi editReply.
  await interaction.editReply(responsePayload);
}

// ----------------- /legit-check-ustaw handler -----------------
async function handleDailyLegitChartTestCommand(interaction) {
  if (!interaction.memberPermissions?.has(PermissionFlagsBits.ManageChannels)) {
    await interaction.reply({
      content: "> `❌` × Nie masz uprawnień do testowania wykresu.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });
  rollDailyLegitStatsIfNeeded();

  const action = interaction.options.getString("akcja", true);
  const amount = interaction.options.getInteger("ile") || 1;
  const selectedHour = interaction.options.getInteger("godzina");
  const testAmountPln = interaction.options.getInteger("kwota") || 0;
  const hour = selectedHour ?? getWarsawDateParts().hour;
  let resultText;

  if (action === "dodaj") {
    dailyLegitStats.hourly[hour] += amount;
    dailyLegitStats.total += amount;
    dailyLegitStats.totalPln = Math.round((dailyLegitStats.totalPln + testAmountPln) * 100) / 100;
    resultText = `Dodano **${amount}** testowych legit checków do godziny **${String(hour).padStart(2, "0")}:00** i **${testAmountPln.toFixed(2)} PLN**.`;
  } else if (action === "odejmij") {
    const removed = Math.min(amount, dailyLegitStats.hourly[hour]);
    dailyLegitStats.hourly[hour] -= removed;
    dailyLegitStats.total = Math.max(0, dailyLegitStats.total - removed);
    dailyLegitStats.totalPln = Math.max(0, Math.round((dailyLegitStats.totalPln - testAmountPln) * 100) / 100);
    resultText = `Odjęto **${removed}** testowych legit checków z godziny **${String(hour).padStart(2, "0")}:00** i **${testAmountPln.toFixed(2)} PLN**.`;
  } else if (action === "wyzeruj") {
    const messageId = dailyLegitStats.messageId;
    dailyLegitStats = createEmptyDailyLegitStats();
    dailyLegitStats.messageId = messageId;
    resultText = "Wyzerowano testowy wynik na dzisiejszym wykresie.";
  } else {
    resultText = "Odświeżono obraz wykresu bez zmiany wyniku.";
  }

  dailyLegitStats.updatedAt = new Date().toISOString();
  scheduleSavePersistentState(true);
  await queueDailyLegitChartPublish({ forceChannelRename: action === "wyzeruj" });

  await interaction.editReply({
    content: `> \`✅\` × ${resultText}\n> Dzisiejszy wynik: **${dailyLegitStats.total}** legit checków i **${dailyLegitStats.totalPln.toFixed(2)} PLN**. Główny licznik nie został zmieniony.`,
  });
}

async function handleUstawTicketySprzedawcyCommand(interaction) {
  if (!isAdminOrSeller(interaction.member)) {
    await interaction.reply({
      content: "> `‼️` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const user = interaction.options.getUser("uzytkownik");
  const ilosc = interaction.options.getInteger("ilosc");

  if (!user || ilosc < 0) {
    await interaction.reply({
      content: "> `❌` × Podaj poprawnego użytkownika i dodatnią liczbę.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  sellerTicketCounts.set(user.id, ilosc);
  scheduleSavePersistentState(true);

  await interaction.reply({
    content: `> \`✅\` × **Pomyślnie ustawiono** liczbę ticketów dla <@${user.id}> na **${ilosc}** (${formatTicketsPlural(ilosc)}).`,
    flags: [MessageFlags.Ephemeral],
  });
}

async function handlePrzeliczTicketySprzedawcyCommand(interaction) {
  if (!isAdminOrSeller(interaction.member)) {
    await interaction.reply({
      content: "> `‼️` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });
  const user = interaction.options.getUser("uzytkownik");
  const count = await getSellerTicketCount(user.id, true);

  await interaction.editReply({
    content: `> \`✅\` × **Przeliczono z kanału +rep**: użytkownik <@${user.id}> ma łącznie **${count}** ${formatTicketsPlural(count)}.`,
  });
}

async function handleLegitRepUstawCommand(interaction) {
  try {
    console.log("[/legit-check-ustaw] start", {
      user: interaction.user?.id,
      guild: interaction.guild?.id,
    });

    // ensure we acknowledge the interaction to avoid "application did not respond"
    if (!interaction.deferred && !interaction.replied) {
      await interaction.deferReply({ ephemeral: true });
    }

    // Sprawdź czy właściciel
    if (interaction.user.id !== interaction.guild.ownerId) {
      const payload = { content: "> `❗` × Brak wymaganych uprawnień.", flags: [MessageFlags.Ephemeral] };
      if (interaction.deferred || interaction.replied) await interaction.editReply(payload);
      else await interaction.reply(payload);
      return;
    }

    const ile = interaction.options.getInteger("ile");

    if (ile < 0 || ile > 9999) {
      const payload = { content: "> `❌` × **Podaj** liczbę od 0 do 9999.", flags: [MessageFlags.Ephemeral] };
      if (interaction.deferred || interaction.replied) await interaction.editReply(payload);
      else await interaction.reply(payload);
      return;
    }

    // Zaktualizuj licznik
    legitRepCount = ile;

    // Zmień nazwę kanału
    const channelId = "1449840030947217529";
    const channel = await client.channels.fetch(channelId).catch((err) => {
      console.error("legit-check-ustaw fetch channel error", err);
      return null;
    });

    if (!channel) {
      const payload = { content: "> `❌` × **Nie znaleziono** kanału legit-check.", flags: [MessageFlags.Ephemeral] };
      if (interaction.deferred || interaction.replied) await interaction.editReply(payload);
      else await interaction.reply(payload);
      return;
    }

    const newName = `✅×〢legit-checki➔${ile}`;
    await channel.setName(newName);

    // Wyślij informacyjną wiadomość
    const successPayload = {
      content: `Legit checki: ${ile}`,
      flags: [MessageFlags.Ephemeral],
    };
    if (interaction.deferred || interaction.replied) await interaction.editReply(successPayload);
    else await interaction.reply(successPayload);

    // Zapisz stan
    scheduleSavePersistentState();

    console.log(`Nazwa kanału legit-check zmieniona na: ${newName} przez ${interaction.user.tag}`);
  } catch (error) {
    console.error("Błąd podczas ustawiania legit-check (outer catch):", error);
    const payload = { content: "> `❌` × **Wystąpił** błąd podczas zmiany nazwy kanału.", flags: [MessageFlags.Ephemeral] };
    if (interaction.deferred || interaction.replied) await interaction.editReply(payload);
    else await interaction.reply(payload);
  }
}

// ─────────────────────────────────────────────
// Helper: format a raw rate number into display string (e.g. 8500 → "8,5k")
function formatRateShort(rate) {
  if (rate >= 1_000_000) {
    const v = rate / 1_000_000;
    return v.toFixed(1).replace(".", ",").replace(",0", "") + "M";
  } else if (rate >= 1_000) {
    const v = rate / 1_000;
    return v.toFixed(1).replace(".", ",").replace(",0", "") + "k";
  }
  const rounded = Math.round((Number(rate) + Number.EPSILON) * 1_000_000) / 1_000_000;
  return String(rounded).replace(".", ",");
}

// Helper: replace every occurrence of oldRate (number & formatted) in cashBody
function replaceCashBodyRate(cashBody, oldRate, newRate) {
  let result = cashBody;
  // Replace formatted short string  (e.g. "8,5k" or "8,2k")
  const oldShort = formatRateShort(oldRate);
  const newShort = formatRateShort(newRate);
  // Escape special regex chars in the short string
  const escapedShort = oldShort.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  result = result.replace(new RegExp(escapedShort, "g"), newShort);
  // Also replace raw number (e.g. 8500) surrounded by non-digit chars
  const escapedNum = String(oldRate).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  result = result.replace(new RegExp(`(?<![0-9])${escapedNum}(?![0-9])`, "g"), String(newRate));
  return result;
}

// ----------------- /cennik handler -----------------
async function handleCennikCommand(interaction) {
  try {
    if (!interaction.deferred && !interaction.replied) {
      await interaction.deferReply({ ephemeral: true });
    }

    // Auth: admin, seller, or helper
    const SELLER_ROLE_ID = "1350786945944391733";
    const HELPER_ROLE_ID = "1519069239254974475";
    const isAdmin = interaction.member?.permissions?.has(PermissionFlagsBits.Administrator);
    const isSeller = interaction.member?.roles?.cache?.has(SELLER_ROLE_ID);
    const isHelper = interaction.member?.roles?.cache?.has(HELPER_ROLE_ID);
    if (!isAdmin && !isSeller && !isHelper) {
      await interaction.editReply({ content: "> `❗` × Brak wymaganych uprawnień.", flags: [MessageFlags.Ephemeral] });
      return;
    }

    const serwer = interaction.options.getString("serwer");
    const stawka = interaction.options.getNumber("stawka");

    // ── VIEW mode ───────────────────────────────────────
    if (!serwer || stawka === null || stawka === undefined) {
      const lines = [
        `> \`💰\` × **Aktualny cennik kalkulatora:**`,
        `> `,
        `> **Anarchia LF:**`,
        `>  • Normalna: \`${formatRateShort(ANARCHIA_LIFESTEAL_RATE)}$ → 1 zł\``,
        `>  • Hurtowa (≥${ANARCHIA_LIFESTEAL_BULK_THRESHOLD_PLN}zł): \`${formatRateShort(ANARCHIA_LIFESTEAL_BULK_RATE)}$ → 1 zł\``,
        `>  • Próg: \`${ANARCHIA_LIFESTEAL_BULK_THRESHOLD_PLN} zł\``,
        `> `,
        `> **Anarchia BoxPvP:**`,
        `>  • \`${formatRateShort(ANARCHIA_BOXPVP_RATE)}$ → 1 zł\``,
        `> `,
        `> **MineStar LF:**`,
        `>  • Normalna: \`${formatRateShort(MINESTAR_LF_RATE)}$ → 1 zł\``,
        `>  • Hurtowa (≥${MINESTAR_LF_BULK_THRESHOLD_PLN}zł): \`${formatRateShort(MINESTAR_LF_BULK_RATE)}$ → 1 zł\``,
        `>  • Próg: \`${MINESTAR_LF_BULK_THRESHOLD_PLN} zł\``,
        `> `,
        `> **MineStar SkyPvP:**`,
        `>  • \`${formatRateShort(MINESTAR_SKY_RATE)} odłamki → 1 zł\``,
        `> `,
        `> **Donut SMP:**`,
        `>  • \`${formatRateShort(DONUT_SMP_RATE)}$ → 1 zł\``,
        `> `,
        `> *Użyj \`/cennik serwer:... stawka:...\` aby zmienić stawkę.*`,
      ];
      await interaction.editReply({ content: lines.join("\n") });
      return;
    }

    // ── SET mode ────────────────────────────────────────
    // Map choice → variable name and label
    const RATE_MAP = {
      anarchia_lf_normal:    { get: () => ANARCHIA_LIFESTEAL_RATE,              set: (v) => { ANARCHIA_LIFESTEAL_RATE = v; },              label: "Anarchia LF - Normalna" },
      anarchia_lf_bulk:      { get: () => ANARCHIA_LIFESTEAL_BULK_RATE,          set: (v) => { ANARCHIA_LIFESTEAL_BULK_RATE = v; },          label: "Anarchia LF - Hurtowa" },
      anarchia_lf_threshold: { get: () => ANARCHIA_LIFESTEAL_BULK_THRESHOLD_PLN, set: (v) => { ANARCHIA_LIFESTEAL_BULK_THRESHOLD_PLN = v; }, label: "Anarchia LF - Próg (zł)" },
      anarchia_boxpvp:       { get: () => ANARCHIA_BOXPVP_RATE,                  set: (v) => { ANARCHIA_BOXPVP_RATE = v; },                  label: "Anarchia BoxPvP" },
      minestar_lf_normal:    { get: () => MINESTAR_LF_RATE,                      set: (v) => { MINESTAR_LF_RATE = v; },                      label: "MineStar LF - Normalna" },
      minestar_lf_bulk:      { get: () => MINESTAR_LF_BULK_RATE,                 set: (v) => { MINESTAR_LF_BULK_RATE = v; },                 label: "MineStar LF - Hurtowa" },
      minestar_lf_threshold: { get: () => MINESTAR_LF_BULK_THRESHOLD_PLN,        set: (v) => { MINESTAR_LF_BULK_THRESHOLD_PLN = v; },        label: "MineStar LF - Próg (zł)" },
      minestar_skypvp:       { get: () => MINESTAR_SKY_RATE,                     set: (v) => { MINESTAR_SKY_RATE = v; },                     label: "MineStar SkyPvP" },
      donut_smp:             { get: () => DONUT_SMP_RATE,                        set: (v) => { DONUT_SMP_RATE = v; },                        label: "Donut SMP" },
    };

    const entry = RATE_MAP[serwer];
    if (!entry) {
      await interaction.editReply({ content: "> `❌` × Nieznany serwer.", flags: [MessageFlags.Ephemeral] });
      return;
    }

    if (!Number.isFinite(stawka) || stawka <= 0) {
      await interaction.editReply({ content: "> `❌` × Stawka musi być liczbą większą od zera.", flags: [MessageFlags.Ephemeral] });
      return;
    }

    const oldRate = entry.get();
    const newRate = stawka;
    entry.set(newRate);

    // Persist immediately
    scheduleSavePersistentState(true);

    // ── Refresh all active cennik embeds ─────────────────
    let updatedPanels = 0;
    for (const [messageId, panelState] of regulationPanels.entries()) {
      try {
        if (!panelState.cashBody) continue;
        const updatedCashBody = replaceCashBodyRate(panelState.cashBody, oldRate, newRate);
        if (updatedCashBody === panelState.cashBody) continue; // nothing changed

        panelState.cashBody = updatedCashBody;
        // Also sync the clone stored in the map
        regulationPanels.set(messageId, { ...panelState, cashBody: updatedCashBody });

        // Edit the Discord message
        const guild = client.guilds.cache.get(panelState.guildId);
        if (!guild) continue;
        const channel = await guild.channels.fetch(panelState.channelId).catch(() => null);
        if (!channel || !channel.isTextBased()) continue;
        const message = await channel.messages.fetch(messageId).catch(() => null);
        if (!message) continue;
        await message.edit(buildEmbedTestMessagePayload(panelState)).catch((err) => {
          console.error("[cennik] Błąd edycji wiadomości:", err);
        });
        updatedPanels++;
      } catch (err) {
        console.error("[cennik] Błąd przy odświeżaniu panelu:", err);
      }
    }

    scheduleSavePersistentState(true);

    const reply = [
      `> \`✅\` × **Cennik zaktualizowany!**`,
      `> Serwer: \`${entry.label}\``,
      `> Stara stawka: \`${formatRateShort(oldRate)}\``,
      `> Nowa stawka: \`${formatRateShort(newRate)}\``,
      updatedPanels > 0 ? `> Zaktualizowano **${updatedPanels}** panel(i) na Discord.` : `> Brak aktywnych paneli do zaktualizowania.`,
    ].join("\n");

    await interaction.editReply({ content: reply });
    console.log(`[/cennik] ${interaction.user.tag} zmienił ${entry.label}: ${oldRate} → ${newRate}`);

  } catch (err) {
    console.error("[/cennik] Błąd:", err);
    const payload = { content: "> `❌` × Wystąpił błąd podczas zmiany cennika.", flags: [MessageFlags.Ephemeral] };
    if (interaction.deferred || interaction.replied) await interaction.editReply(payload);
    else await interaction.reply(payload);
  }
}

// ----------------- /sprawdz-kogo-zaprosil handler -----------------
async function handleAdminZaproszeniaCommand(interaction) {
  if (interaction.user.id !== interaction.guild.ownerId) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień (Tylko właściciel).",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const targetUser = interaction.options.getUser("nick");
  const targetId = targetUser.id;
  const guild = interaction.guild;

  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });

  try {
    let allInvites = [];
    try {
      console.log(
        `[/zaproszenia] Start user=${interaction.user.id} target=${targetId} guild=${guild.id}`,
      );
      const { data, error } = await runCommandExternal(
        (signal) => db.supabase
          .from("invites")
          .select("*")
          .eq("guild_id", guild.id)
          .eq("inviter_id", targetId)
          .abortSignal(signal),
        "/zaproszenia: odczyt Supabase",
        1,
      );

      if (!error && data) {
        allInvites = data;
      }
    } catch (e) {
      // Statystyki zaproszeń są także utrzymywane w pamięci bota, więc
      // awaria Supabase nie może zatrzymać całej komendy.
      console.error("Supabase fail in zaproszenia command; używam pamięci:", e);
    }

    const inMemoryInvited = new Set();
    for (const [key, storedInvite] of inviterOfMember.entries()) {
      const inviterId = getStoredInviterId(storedInvite);
      if (inviterId === targetId && key.startsWith(`${guild.id}:`)) {
        inMemoryInvited.add(key.split(":")[1]);
      }
    }
    for (const [key, storedInvite] of leaveRecords.entries()) {
      const inviterId = getStoredInviterId(storedInvite);
      if (inviterId === targetId && key.startsWith(`${guild.id}:`)) {
        inMemoryInvited.add(key.split(":")[1]);
      }
    }

    const allUserIds = new Set(allInvites.map(i => i.invited_user_id));
    for (const id of inMemoryInvited) allUserIds.add(id);

    // Patrzymy "do tyłu" używając Discord Invites API
    const invites = await runCommandExternal(
      () => guild.invites.fetch(),
      "/zaproszenia: Discord Invites API",
      1,
    ).catch((error) => {
      console.error("[/zaproszenia] Discord Invites API niedostępne; używam pamięci:", error);
      return new Map();
    });
    let totalUses = 0;
    invites.forEach(inv => {
      if (inv.inviter?.id === targetId) {
        totalUses += (inv.uses || 0);
      }
    });

    if (allUserIds.size === 0 && totalUses === 0) {
      await interaction.editReply({
        content: `> \`ℹ️\` × **Użytkownik** <@${targetId}> **nie zaprosił żadnych osób** (ani teraz, ani w przeszłości wg linków).`,
      });
      return;
    }

    const members = new Map();
    if (allUserIds.size > 0) {
      try {
        const userIdsArr = Array.from(allUserIds);
        for (let i = 0; i < userIdsArr.length; i += 100) {
          const chunk = userIdsArr.slice(i, i + 100);
          const fetchedChunk = await runCommandExternal(
            () => guild.members.fetch({ user: chunk }),
            "/zaproszenia: Discord Members API",
            1,
          ).catch(() => new Map());
          fetchedChunk.forEach(m => members.set(m.id, m));
        }
      } catch (fetchErr) {
        console.error("[zaproszenia] Error fetching specific members:", fetchErr);
      }
    }

    const normFn = (s = "") => (s).toString().toLowerCase().trim().normalize("NFD").replace(/[\u0300-\u036f]/g, "");
    const CLIENT_ROLE_ID =
      verificationRoles.get(guild.id) ||
      guild.roles.cache.find((r) => normFn(r.name).includes(normFn("klient")))?.id;

    const verified = [];
    const unverified = [];
    const left = [];

    for (const uid of allUserIds) {
      const mem = members.get(uid);
      if (mem) {
        if (CLIENT_ROLE_ID && mem.roles.cache.has(CLIENT_ROLE_ID)) {
          verified.push(uid);
        } else {
          unverified.push(uid);
        }
      } else {
        left.push(uid);
      }
    }

    // Inicjalizacja map i pobranie statystyk z pamięci bota
    if (!inviteCounts.has(guild.id)) inviteCounts.set(guild.id, new Map());
    if (!inviteLeaves.has(guild.id)) inviteLeaves.set(guild.id, new Map());
    if (!inviteFakeAccounts.has(guild.id)) inviteFakeAccounts.set(guild.id, new Map());
    if (!inviteBonusInvites.has(guild.id)) inviteBonusInvites.set(guild.id, new Map());

    const gMap = inviteCounts.get(guild.id);
    const lMap = inviteLeaves.get(guild.id);
    const fakeMap = inviteFakeAccounts.get(guild.id);
    const bonusMap = inviteBonusInvites.get(guild.id);

    const validInvites = gMap.get(targetId) || 0;
    const leftCount = lMap.get(targetId) || 0;
    const fakeCount = fakeMap.get(targetId) || 0;
    const bonusCount = bonusMap.get(targetId) || 0;
    const displayedInvites = validInvites + bonusCount;

    const embed = new EmbedBuilder()
      .setColor(COLOR_BLUE)
      .setTitle("📩 New Shop × SZCZEGÓŁOWE ZAPROSZENIA")
      .setDescription(
        `###  Podsumowanie dla <@${targetId}>\n` +
        `> \`⭐\` **Łączne zaproszenia:** \`${displayedInvites}\` \n` +
        `> ├ \`✅\` Prawdziwe: \`${validInvites}\`\n` +
        `> ├ \`🎁\` Dodatkowe: \`${bonusCount}\`\n` +
        `> ├ \`🚶\` Opuścili serwer: \`${leftCount}\`\n` +
        `> └ \`⚠️\` Konta < 2 mies. (fake): \`${fakeCount}\`\n` +
        `> \`🔢\` **Suma starych użyć linków:** \`${totalUses}\` użyć\n\n` +
        `###  Logi szczegółowe dołączeń\n` +
        `*Wykaz użytkowników przypisanych w bazie bota:*`
      )
      .addFields(
        {
          name: `\`✅\` Zweryfikowani (Klient) [${verified.length}]`,
          value: verified.length > 0 
            ? verified.slice(0, 30).map(u => `<@${u}>`).join(", ") + (verified.length > 30 ? "..." : "")
            : "*Brak*",
          inline: false
        },
        {
          name: `\`⏳\` Niezweryfikowani [${unverified.length}]`,
          value: unverified.length > 0 
            ? unverified.slice(0, 30).map(u => `<@${u}>`).join(", ") + (unverified.length > 30 ? "..." : "")
            : "*Brak*",
          inline: false
        },
        {
          name: `\`❌\` Wyszli z serwera [${left.length}]`,
          value: left.length > 0 
            ? left.slice(0, 30).map(u => `<@${u}>`).join(", ") + (left.length > 30 ? "..." : "")
            : "*Brak*",
          inline: false
        }
      );

    await interaction.editReply({
      embeds: [embed],
      allowedMentions: {
        users: Array.from(allUserIds).slice(0, 100),
        repliedUser: false,
      },
    });
    console.log(`[/zaproszenia] Odpowiedź wysłana target=${targetId}`);
  } catch (err) {
    console.error("Zaproszenia logs error:", err);
    await interaction.editReply({
      content: `> \`❌\` × Wystąpił błąd podczas pobierania zaproszeń: ${err.message}`,
    }).catch((replyError) => {
      console.error("[/zaproszenia] Nie udało się zakończyć odroczonej odpowiedzi:", replyError);
    });
  }
}

async function handleSprawdzKogoZaprosilCommand(interaction) {
  // Sprawdź czy właściciel
  if (interaction.user.id !== interaction.guild.ownerId) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const targetUser = interaction.options.getUser("kto");
  if (!targetUser) {
    await interaction.reply({
      content: "> `❌` × **Nie udało się** zidentyfikować użytkownika.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  try {
    const guild = interaction.guild;
    const targetUserId = targetUser.id;

    // Pobierz zaproszenia z Supabase
    const invitedUsers = await db.getInvitedUsersByInviter(guild.id, targetUserId);

    if (invitedUsers.length === 0) {
      await interaction.reply({
        content: `> \`ℹ️\` × **Użytkownik** <@${targetUserId}> **nie ma żadnych aktywnych zaproszeń**.`,
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    // Pobierz aktualnych członków serwera
    const guildMembers = await guild.members.fetch();
    const currentMemberIds = new Set(guildMembers.keys());

    // Filtruj tylko osoby które są nadal na serwerze
    let invitedList = [];

    for (const invitedUser of invitedUsers) {
      try {
        // Sprawdź czy użytkownik jest nadal na serwerze
        if (currentMemberIds.has(invitedUser.invited_user_id)) {
          const member = guildMembers.get(invitedUser.invited_user_id);

          // Sprawdź czy konto ma więcej niż 2 miesiące
          const accountAge = member.user.createdAt;
          const twoMonthsAgo = new Date(Date.now() - (60 * 24 * 60 * 60 * 1000)); // 60 dni

          if (accountAge && accountAge > twoMonthsAgo) {
            const joinedDate = invitedUser.created_at ?
              new Date(invitedUser.created_at).toLocaleDateString('pl-PL') :
              'Nieznana data';

            invitedList.push({
              user: member.user,
              date: joinedDate
            });
          }
        }
      } catch (err) {
        // Użytkownik opuścił serwer lub konto za młode - nie dodajemy do listy
        continue;
      }
    }

    // Usuń duplikaty z listy
    const uniqueInvites = [];
    const seenUsers = new Set();

    for (const item of invitedList) {
      if (item.user && !seenUsers.has(item.user.id)) {
        seenUsers.add(item.user.id);
        uniqueInvites.push(item);
      }
    }

    // Twórz embed
    const embed = new EmbedBuilder()
      .setColor(COLOR_BLUE)
      .setTitle("New Shop x Logi")
      .setDescription(`**Sprawdzasz:** <@${targetUserId}>\nUżytkownik zaprosił **${uniqueInvites.length}** osób`)
      .addFields({
        name: "--=--=--=--=LISTA=--=--=--=--=--=",
        value: uniqueInvites.length > 0
          ? uniqueInvites.map(item =>
            `@${item.user.username} (${item.date})`
          ).join('\n')
          : "Brak aktywnych zaproszeń na serwerze"
      })
      .setTimestamp();

    await interaction.reply({ embeds: [embed] });

  } catch (error) {
    console.error("Błąd podczas sprawdzania zaproszonych osób:", error);
    await interaction.reply({
      content: "> `❌` × **Wystąpił** błąd podczas sprawdzania zaproszeń.",
      flags: [MessageFlags.Ephemeral],
    });
  }
}

async function handleSelectMenu(interaction) {
  const embedTestPublishChannelMatch = interaction.customId.match(
    /^embedtest_publish_channel_(\d+)$/,
  );
  if (embedTestPublishChannelMatch) {
    const [, messageId] = embedTestPublishChannelMatch;
    const state = embedTestStates.get(messageId);

    if (!state) {
      await interaction.reply({
        content: "> `❌` × Ta sesja edycji wygasła. Użyj `/embed-2` ponownie.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    if (state.ownerId !== interaction.user.id) {
      await interaction.reply({
        content: "> `❗` × Tylko autor testu może zakończyć ten embed.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const targetChannelId = interaction.values[0];
    const targetChannel = await interaction.guild.channels
      .fetch(targetChannelId)
      .catch(() => null);
    await publishEmbedTestToChannel(interaction, state, targetChannel);
    return;
  }

  const embedTestColorMatch = interaction.customId.match(
    /^embedtest_color_(\d+)$/,
  );
  if (embedTestColorMatch) {
    const [, messageId] = embedTestColorMatch;
    const state = embedTestStates.get(messageId);

    if (!state) {
      await interaction.reply({
        content: "> `❌` × Ta sesja edycji wygasła. Użyj `/embed-2` ponownie.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    if (state.ownerId !== interaction.user.id) {
      await interaction.reply({
        content: "> `❗` × Tylko autor testu może edytować ten embed.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const selectedColor = getEmbedTestColorDef(interaction.values[0]);
    state.accentColorKey = selectedColor.value;
    state.accentColor = selectedColor.color;
    embedTestStates.set(messageId, state);

    const updated = await updateEmbedTestMessage(state);
    if (!updated) {
      embedTestStates.delete(messageId);
      await interaction.reply({
        content: "> `❌` × Nie udało się zaktualizować wiadomości. Użyj `/embed-2` ponownie.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    await interaction.update(
      buildEmbedTestControlPayload(
        state,
        isRegulationEmbedState(state)
          ? `Ustawiłem kolor panelu na ${selectedColor.label}`
          : `Ustawiłem kolor embeda na ${selectedColor.label}`,
      ),
    );
    return;
  }

  if (interaction.customId === "kalkulator_typ") {
    const selectedType = interaction.values[0];
    try {
      const detectedServer = await detectServerFromContext(interaction);
      await interaction.showModal(buildKalkulatorModal(selectedType, detectedServer));
    } catch (error) {
      console.error("kalkulator_typ showModal error:", error);
      if (!interaction.replied && !interaction.deferred) {
        await interaction.reply({
          content: "> `❌` × Nie udało się otworzyć formularza kalkulatora. Spróbuj ponownie.",
          flags: [MessageFlags.Ephemeral],
        }).catch(() => null);
      }
    }
    return;
  }
  if (interaction.customId === "panel_klienta_select") {
    const selectedValue = interaction.values[0];
    if (selectedValue === "panel_klienta_spent") {
      await handlePanelKlientaSpent(interaction);
    } else if (selectedValue === "panel_klienta_codes" || selectedValue === "panel_klienta_history") {
      await handlePanelKlientaActiveCodes(interaction, 0);
    }
    return;
  }

  // KALKULATOR select menu handlers
  if (interaction.customId === "kalkulator_tryb" || interaction.customId === "kalkulator_metoda") {
    await handleKalkulatorSelect(interaction);
    return;
  }

  if (interaction.customId === "testpanel_category") {
    const selectedCategory = interaction.values[0];

    if (selectedCategory !== "zakup") {
      await interaction.reply({
        content: "> `❌` × Ta kategoria testowa nie jest jeszcze dostępna.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    await showZakupModal(interaction);
    return;
  }

  // ticket category menu
  if (interaction.customId === "ticket_category") {
    const selectedCategory = interaction.values[0];

    switch (selectedCategory) {
      case "zakup":
        await showZakupModal(interaction);
        break;
      case "zakup_moda":
        await showModyZakupModal(interaction);
        break;
      case "zakup_autorynku":
        await showAutoRynekZakupModal(interaction);
        break;
      case "sprzedaz":
        await showSprzedazModal(interaction);
        break;
      case "odbior":
        await showOdbiorModal(interaction);
        break;
      case "inne":
        await showInneModal(interaction);
        break;
      default:
        await interaction.reply({
          content: "> `❌` × **Nie wybrano** żadnej z kategorii!",
          flags: [MessageFlags.Ephemeral],
        });
    }
    return;
  }

  // ticket settings select handler
  if (interaction.customId.startsWith("ticket_settings_select_")) {
    const channelId = interaction.customId.replace(
      "ticket_settings_select_",
      "",
    );
    const chosen = interaction.values[0];

    // handle chosen action: open modal accordingly
    if (chosen === "rename") {
      const modal = new ModalBuilder()
        .setCustomId(`modal_rename_${channelId}`)
        .setTitle("Zmień nazwę ticketu");

      const nameInput = new TextInputBuilder()
        .setCustomId("new_ticket_name")
        .setLabel("Nowa nazwa kanału (np. ticket-nick)")
        .setStyle(TextInputStyle.Short)
        .setPlaceholder("ticket-nick")
        .setRequired(true)
        .setMinLength(3)
        .setMaxLength(90);

      modal.addComponents(new ActionRowBuilder().addComponents(nameInput));
      await interaction.showModal(modal);
      return;
    }

    if (chosen === "add") {
      const modal = new ModalBuilder()
        .setCustomId(`modal_add_${channelId}`)
        .setTitle("Dodaj użytkownika do ticketu");

      const userInput = new TextInputBuilder()
        .setCustomId("user_to_add")
        .setLabel("Wpisz @mention lub ID użytkownika")
        .setStyle(TextInputStyle.Short)
        .setPlaceholder("@użytkownik lub ID")
        .setRequired(true);

      modal.addComponents(new ActionRowBuilder().addComponents(userInput));
      await interaction.showModal(modal);
      return;
    }

    if (chosen === "remove") {
      const modal = new ModalBuilder()
        .setCustomId(`modal_remove_${channelId}`)
        .setTitle("Usuń użytkownika z ticketu");

      const userInput = new TextInputBuilder()
        .setCustomId("user_to_remove")
        .setLabel("Wpisz @mention lub ID użytkownika")
        .setStyle(TextInputStyle.Short)
        .setPlaceholder("@użytkownik lub ID")
        .setRequired(true);

      modal.addComponents(new ActionRowBuilder().addComponents(userInput));
      await interaction.showModal(modal);
      return;
    }

    await interaction.reply({ content: "> `❌` × **Nieznana** akcja.", flags: [MessageFlags.Ephemeral] });
    return;
  }
}

async function showZakupModal(interaction, detectedServer = null) {
  await showZakupModalV2(interaction, detectedServer);
}

async function showModyZakupModal(interaction) {
  const timestamp = Date.now();
  const modNameInput = new TextInputBuilder()
    .setCustomId(`mod_name_${timestamp}`)
    .setPlaceholder("Przykład: Auto_Dripstone")
    .setStyle(TextInputStyle.Short)
    .setRequired(true)
    .setMaxLength(64);

  const paymentSelect = new StringSelectMenuBuilder()
    .setCustomId(`mod_payment_method_${timestamp}`)
    .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
    .setRequired(true)
    .setMinValues(1)
    .setMaxValues(1)
    .addOptions(sanitizeOptionsEmojis(SIMPLE_PAYMENT_OPTIONS));

  const modsCountInput = new TextInputBuilder()
    .setCustomId(`mods_count_${timestamp}`)
    .setPlaceholder("Podaj liczbę od 1 do 4")
    .setStyle(TextInputStyle.Short)
    .setRequired(true)
    .setMaxLength(1);

  const modal = new ModalBuilder()
    .setCustomId(`modal_mody_zakup_${timestamp}`)
    .setTitle("Zakup moda")
    .addLabelComponents(
      new LabelBuilder()
        .setLabel("Jakiego moda chcesz kupić?")
        .setTextInputComponent(modNameInput),
      new LabelBuilder()
        .setLabel("Forma płatności")
        .setStringSelectMenuComponent(paymentSelect),
      new LabelBuilder()
        .setLabel("Ile modów chcesz kupić?")
        .setTextInputComponent(modsCountInput),
    );

  await interaction.showModal(modal);
}

async function showAutoRynekZakupModal(interaction) {
  const timestamp = Date.now();
  const paymentSelect = new StringSelectMenuBuilder()
    .setCustomId(`autorynek_payment_method_${timestamp}`)
    .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
    .setRequired(true)
    .setMinValues(1)
    .setMaxValues(1)
    .addOptions(sanitizeOptionsEmojis(AUTORYNEK_PAYMENT_OPTIONS));

  const modal = new ModalBuilder()
    .setCustomId(`modal_autorynek_zakup_${timestamp}`)
    .setTitle("Zakup Botów")
    .addLabelComponents(
      new LabelBuilder()
        .setLabel("Forma płatności")
        .setStringSelectMenuComponent(paymentSelect),
    );

  await interaction.showModal(modal);
}

async function ticketClaimCommon(interaction, channelId, opts = {}) {
  const isBtn = typeof interaction.isButton === "function" && interaction.isButton();
  const skipQuiz = opts.skipQuiz === true;
  const bypassPermissionCheck = opts.bypassPermissionCheck === true;

  if (!bypassPermissionCheck && !isAdminOrSeller(interaction.member)) {
    if (!interaction.replied && !interaction.deferred) {
      await interaction.reply({
        content: "> `❗` × Brak wymaganych uprawnień.",
        flags: [MessageFlags.Ephemeral],
      });
    } else {
      await interaction.followUp({
        content: "> `❗` × Brak wymaganych uprawnień.",
        flags: [MessageFlags.Ephemeral],
      }).catch(() => null);
    }
    return { ok: false, reason: "permission" };
  }
  // Właściciel serwera nie musi przechodzić captchy
  const isOwner = interaction.guild && interaction.user.id === interaction.guild.ownerId;

  // quiz matematyczny przed przejęciem (przycisk + /przejmij) - pomijamy dla właściciela
  if (!skipQuiz && !isOwner) {
    const pick = generateClaimQuiz();
    const modalId = `claim_quiz_${channelId}_${interaction.user.id}_${Date.now()}`;
    pendingClaimQuiz.set(modalId, { channelId, userId: interaction.user.id, answer: pick.a });

    const modal = new ModalBuilder()
      .setCustomId(modalId)
      .setTitle("Weryfikacja przejęcia ticketu");
    const input = new TextInputBuilder()
      .setCustomId("claim_answer")
      .setLabel(pick.q)
      .setStyle(TextInputStyle.Short)
      .setRequired(true)
      .setMaxLength(5);
    modal.addComponents(new ActionRowBuilder().addComponents(input));
    await interaction.showModal(modal).catch(() => null);
    return { ok: false, reason: "quiz-required" };
  }

  // Blokada wyścigu – tylko jeden sprzedawca może przejąć naraz
  if (claimingInProgress.has(channelId)) {
    const failMsg = "> `❌` × Ten ticket jest właśnie przejmowany przez kogoś innego. Spróbuj za chwilę.";
    if (!interaction.replied && !interaction.deferred) {
      await interaction.reply({ content: failMsg, flags: [MessageFlags.Ephemeral] }).catch(() => null);
    } else {
      await interaction.followUp({ content: failMsg, flags: [MessageFlags.Ephemeral] }).catch(() => null);
    }
    return { ok: false, reason: "claiming-in-progress" };
  }
  claimingInProgress.add(channelId);

  try {
    // szybka odpowiedź, żeby Discord nie wyświetlał błędu interakcji (po quizie)
    if (!interaction.replied && !interaction.deferred) {
      if (isBtn) {
        await interaction.deferUpdate().catch(() => null);
      } else {
        await interaction.deferReply({ flags: [MessageFlags.Ephemeral] }).catch(() => null);
      }
    }

    const replyEphemeral = async (text) => {
      // jeśli interakcja nie została jeszcze potwierdzona, użyj reply()
      if (!interaction.replied && !interaction.deferred) {
        await interaction
          .reply({ content: text, flags: [MessageFlags.Ephemeral] })
          .catch(() => null);
        return;
      }
      if (isBtn) {
        await interaction.followUp({ content: text, flags: [MessageFlags.Ephemeral] }).catch(() => null);
      } else {
        await interaction.editReply({ content: text }).catch(() => null);
      }
    };

    const ticketData = ticketOwners.get(channelId) || {
      claimedBy: null,
      locked: false,
      userId: null,
      ticketMessageId: null,
      originalCategoryId: null, // Zapisz oryginalną kategorię
    };

    if (ticketData.locked) {
      await replyEphemeral(
        "> `❌` × Ten ticket został zablokowany do przejmowania.",
      );
      return { ok: false, reason: "locked" };
    }

    if (ticketData && ticketData.claimedBy) {
      await replyEphemeral(
        `> \`❌\` × Ticket został już przejęty przez <@${ticketData.claimedBy}>.`,
      );
      return { ok: false, reason: "already-claimed", claimedBy: ticketData.claimedBy };
    }

    const ch = await client.channels.fetch(channelId).catch(() => null);
    if (!ch) {
      await replyEphemeral("> `❌` × Nie mogę znaleźć tego kanału.");
      return { ok: false, reason: "channel-not-found" };
    }

    const claimerId = interaction.user.id;

    // Zapisz oryginalną kategorię przed przeniesieniem
    if (!ticketData.originalCategoryId) {
      ticketData.originalCategoryId = ch.parentId;
    }
    ticketData.originalCategoryKey ||= ticketCategoryManager.keyFor(interaction.guild, ticketData.originalCategoryId);

    // Przenieś do kategorii TICKETY PRZEJĘTE
    await ticketCategoryManager.move(ch, "przejete");
    console.log(`Przeniesiono ticket ${channelId} do kategorii TICKETY PRZEJĘTE`);

    // Ustaw uprawnienia dla osoby przejmującej + właściciela ticketu
    const permissionOverwrites = [
      {
        id: claimerId,
        allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory]
      },
      {
        id: interaction.guild.roles.everyone,
        deny: [PermissionFlagsBits.ViewChannel] // @everyone nie widzi gdy ktoś przejmie
      }
    ];

    // Dodaj właściciela ticketu do uprawnień
    if (ticketData && ticketData.userId) {
      permissionOverwrites.push({
        id: ticketData.userId,
        allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory]
      });
    }

    const helperRoleId = "1519069239254974475";
    if (ticketData && ticketData.ticketTypeLabel === "PYTANIE") {
      permissionOverwrites.push({
        id: helperRoleId,
        allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory]
      });
    } else {
      permissionOverwrites.push({
        id: helperRoleId,
        deny: [PermissionFlagsBits.ViewChannel]
      });
    }

    await ch.permissionOverwrites.set(permissionOverwrites);

    // Usuń limity kategorii dla kanału
    const limitCategories = [
      "1449448705563557918", // limit 20
      "1449448702925209651", // limit 50
      "1449448686156255333", // limit 100
      "1449448860517798061"  // limit 200
    ];

    for (const categoryId of limitCategories) {
      const category = await client.channels.fetch(categoryId).catch(() => null);
      if (category && category.type === ChannelType.GuildCategory) {
        await category.permissionOverwrites.edit(ch.id, {
          ViewChannel: false,
          SendMessages: false,
          ReadMessageHistory: false
        }).catch(() => null);
      }
    }

    ticketData.claimedBy = claimerId;
    ticketOwners.set(channelId, ticketData);
    scheduleSavePersistentState();

    if (ticketData && ticketData.ticketMessageId) {
      await editTicketMessageButtons(ch, ticketData.ticketMessageId, claimerId).catch(() => null);
    }

    const publicClaimerLabel =
      (typeof opts.publicClaimerLabel === "string" && opts.publicClaimerLabel.trim()) ||
      `<@${claimerId}>`;

    const sellerCount = await getSellerTicketCount(claimerId);

    const publicEmbed = new EmbedBuilder()
      .setColor(COLOR_BLUE)
      .setDescription(
        `> \`✅\` × Ticket został przejęty przez: ${publicClaimerLabel}\n` +
        `> <a:card:1537186328867770488> × Wykonał on łącznie: __**${sellerCount}**__ ${formatTicketsPlural(sellerCount)}`
      );

    if (ticketData.lastUnclaimMsgId) {
      const msgToDelete = await ch.messages.fetch(ticketData.lastUnclaimMsgId).catch(() => null);
      if (msgToDelete) {
        await msgToDelete.delete().catch(() => null);
      }
      ticketData.lastUnclaimMsgId = null;
    }

    try {
      const sendPayload = { embeds: [publicEmbed] };
      if (ticketData && ticketData.userId) {
        sendPayload.content = `<@${ticketData.userId}>`;
        sendPayload.allowedMentions = { users: [ticketData.userId] };
      }
      const sent = await ch.send(sendPayload).catch(() => null);
      if (sent && sent.id) {
        ticketData.lastClaimMsgId = sent.id;
        ticketOwners.set(channelId, ticketData);
        scheduleSavePersistentState();
      }
    } catch {
      // ignore
    }

    try {
      if (ticketData.ticketTypeLabel === "ZAKUP") {
        await sendSellerPaymentProfileToTicket(ch, interaction.guildId, claimerId, ticketData);

        const method = String(ticketData?.paymentMethod || "").toLowerCase();
        if (method === "psc" || method === "psc_bez_paragonu" || method === "kod_blik" || method === "kod blik") {
          let desc = "> `ℹ️` × **Informacje**\n";
          if (method === "psc") {
            desc += "> `🛒` × Wyślij **zdjęcie paragonu** oraz **kod psc**.";
          } else if (method === "psc_bez_paragonu") {
            desc += "> `🛒` × Wyślij **kod psc**.";
          } else {
            desc += "> `🛒` × Wyślij **KOD BLIK**.";
          }
          const pscEmbed = new EmbedBuilder()
            .setColor(COLOR_BLUE)
            .setDescription(desc);
          await ch.send({ embeds: [pscEmbed] }).catch(() => null);
        }
      }
    } catch (e) {
      console.error("Error sending payment profile:", e);
    }

    await sendTicketLogEntry(interaction.guild, {
      title: "Ticket przejęty",
      icon: "🟢",
      color: 0x57f287,
      summary: null,
      ticketChannel: ch,
      ownerId: ticketData.userId,
      actorId: interaction.user.id,
      claimedById: claimerId,
      ticketMeta: ticketData,
      statusLabel: "PRZEJĘTY",
      detailLines: [],
    }).catch((err) => console.error("ticket claim log error:", err));

    if (!isBtn) {
      await interaction.deleteReply().catch(() => null);
    }
    return { ok: true, reason: "claimed", channelId, claimedBy: claimerId };
  } catch (err) {
    console.error("Błąd przy przejmowaniu ticketu:", err);
    await replyEphemeral("❌ Wystąpił błąd podczas przejmowania ticketu.");
    return { ok: false, reason: "error", channelId };
  } finally {
    claimingInProgress.delete(channelId);
  }
}

function getPingRolesForTicketType(ticketType) {
  const SELLER_PING_ROLE_ID = "1350786945944391733";
  if (ticketType === "inne") {
    return ["1519069239254974475"]; // helper
  }
  return [SELLER_PING_ROLE_ID];
}

function getLimitRolePermissions(ticketType, categoryId, categories = {}) {
  const R20 = "1449448705563557918";
  const R50 = "1449448702925209651";
  const R100 = "1449448686156255333";
  const R200 = "1449448860517798061";
  const R400 = "1541553589833695393";
  const R_NOLIMIT = "1541553994000891984";

  const allLimitRoles = [R20, R50, R100, R200, R400, R_NOLIMIT];

  let allowed = [];
  const helpCategoryId = categories["inne"];
  if (ticketType === "inne" || categoryId === "1449527585271976131" || (helpCategoryId && categoryId === helpCategoryId)) {
    // Tickets pomoc/innen są prywatne: sprzedawcy nie mogą ich widzieć.
    allowed = [];
  } else if (ticketType === "zakup-0-20" || categoryId === categories["zakup-0-20"] || categoryId === "1449526840942268526") {
    allowed = [R20, R50, R100, R200, R400, R_NOLIMIT];
  } else if (ticketType === "zakup-20-50" || categoryId === categories["zakup-20-50"] || categoryId === "1449526958508474409") {
    allowed = [R50, R100, R200, R400, R_NOLIMIT];
  } else if (ticketType === "zakup-50-100" || categoryId === categories["zakup-50-100"] || categoryId === "1449451716129984595") {
    allowed = [R100, R200, R400, R_NOLIMIT];
  } else if (ticketType === "zakup-100-200" || categoryId === categories["zakup-100-200"] || categoryId === "1449452354201190485") {
    allowed = [R200, R400, R_NOLIMIT];
  } else if (ticketType === "zakup-200-400" || categoryId === categories["zakup-200-400"]) {
    allowed = [R400, R_NOLIMIT];
  } else if (ticketType === "zakup-400-999" || categoryId === categories["zakup-400-999"]) {
    allowed = [R_NOLIMIT];
  } else if (ticketType === "sprzedaz" || categoryId === categories["sprzedaz"] || categoryId === "1449455848043708426") {
    allowed = [R20, R50, R100, R200, R400, R_NOLIMIT];
  } else {
    allowed = [R20, R50, R100, R200, R400, R_NOLIMIT];
  }

  const overwrites = [];
  for (const roleId of allLimitRoles) {
    if (allowed.includes(roleId)) {
      overwrites.push({
        id: roleId,
        allow: [
          PermissionsBitField.Flags.ViewChannel,
          PermissionsBitField.Flags.SendMessages,
          PermissionsBitField.Flags.ReadMessageHistory,
        ],
      });
    } else {
      overwrites.push({
        id: roleId,
        deny: [PermissionsBitField.Flags.ViewChannel],
      });
    }
  }

  return overwrites;
}

async function ticketUnclaimCommon(interaction, channelId, expectedClaimer = null, reason = "Brak podanego powodu") {
  const isBtn = typeof interaction.isButton === "function" && interaction.isButton();

  if (!isAdminOrSeller(interaction.member)) {
    if (!interaction.replied && !interaction.deferred) {
      await interaction.reply({
        content: "> `❗` × Brak wymaganych uprawnień.",
        flags: [MessageFlags.Ephemeral],
      });
    } else {
      await interaction.followUp({
        content: "> `❗` × Brak wymaganych uprawnień.",
        flags: [MessageFlags.Ephemeral],
      }).catch(() => null);
    }
    return;
  }

  if (!interaction.replied && !interaction.deferred) {
    if (isBtn) {
      await interaction.deferUpdate().catch(() => null);
    } else {
      await interaction.deferReply({ flags: [MessageFlags.Ephemeral] }).catch(() => null);
    }
  }

  const replyEphemeral = async (text) => {
    if (isBtn) {
      await interaction.followUp({ content: text, flags: [MessageFlags.Ephemeral] }).catch(() => null);
    } else {
      await interaction.editReply({ content: text }).catch(() => null);
    }
  };

  const ticketData = ticketOwners.get(channelId) || {
    claimedBy: null,
    userId: null,
    ticketMessageId: null,
    originalCategoryId: null, // Dodaj oryginalną kategorię
  };

  const ch = await client.channels.fetch(channelId).catch(() => null);
  if (!ch) {
    await replyEphemeral("❌ Nie mogę znaleźć tego kanału.");
    return;
  }

  if (!ticketData.claimedBy) {
    await replyEphemeral("ℹ️ Ten ticket nie jest przejęty.");
    return;
  }

  if (
    expectedClaimer &&
    expectedClaimer !== interaction.user.id &&
    !isAdminOrSeller(interaction.member)
  ) {
    await replyEphemeral(
      "> `❗` Brak wymaganych uprawnień.",
    );
    return;
  }

  if (!interaction.replied && !interaction.deferred) {
    if (isBtn) {
      await interaction.deferUpdate().catch(() => null);
    } else {
      await interaction.deferReply({ flags: [MessageFlags.Ephemeral] }).catch(() => null);
    }
  }

  try {
    const releaserId = interaction.user.id;
    const previousClaimerId = ticketData.claimedBy || null;

    // Odtwórz kategorię po zwolnieniu ticketu, nawet jeśli wcześniej została usunięta.
    const originalKey = ticketData.originalCategoryKey || ticketCategoryManager.keyFor(interaction.guild, ticketData.originalCategoryId);
    if (originalKey) {
      ticketData.originalCategoryId = await ticketCategoryManager.move(ch, originalKey);
    } else if (ticketData.originalCategoryId) {
      const originalCategory = await client.channels.fetch(ticketData.originalCategoryId).catch(() => null);

      if (originalCategory) {
        await ch.setParent(ticketData.originalCategoryId, { lockPermissions: false }).catch((err) => {
          console.error("Błąd przywracania oryginalnej kategorii:", err);
        });
        console.log(`Przywrócono ticket ${channelId} do oryginalnej kategorii ${ticketData.originalCategoryId}`);
      } else {
        console.error("Nie znaleziono oryginalnej kategorii:", ticketData.originalCategoryId);
      }
    }

    // Przywróć uprawnienia w zależności od oryginalnej kategorii.
    // Dla starszych ticketów bez zapisanego parenta rozpoznaj prywatny ticket
    // po etykiecie PYTANIE, aby nie przywrócić dostępu sprzedawcom.
    {
      const categoryId = ticketData.originalCategoryId || null;
      const categories = ticketCategories.get(interaction.guildId) || {};
      const configuredTicketType = Object.entries(categories).find(
        ([, configuredCategoryId]) => String(configuredCategoryId) === String(categoryId),
      )?.[0] || null;
      const isHelpTicket =
        configuredTicketType === "inne" ||
        ticketData.ticketTypeLabel === "PYTANIE" ||
        categoryId === "1449527585271976131" ||
        (categories["inne"] && categoryId === categories["inne"]);
      if (categoryId || isHelpTicket) {
        const ticketType = configuredTicketType || (isHelpTicket ? "inne" : null);
        const limitOverwrites = getLimitRolePermissions(ticketType, categoryId, categories);
        const overwritesToSet = [
          { id: interaction.guild.roles.everyone, deny: [PermissionsBitField.Flags.ViewChannel] },
          ...limitOverwrites,
        ];
        if (isHelpTicket) {
          // Oprócz rang limitów zablokuj też bazową rangę sprzedawcy.
          overwritesToSet.push({ id: BASE_SELLER_ROLE_ID, deny: [PermissionsBitField.Flags.ViewChannel] });
          overwritesToSet.push({ id: "1519069239254974475", allow: [PermissionsBitField.Flags.ViewChannel, PermissionsBitField.Flags.SendMessages, PermissionsBitField.Flags.ReadMessageHistory] });
          if (interaction.guild.ownerId) {
            overwritesToSet.push({ id: interaction.guild.ownerId, allow: [PermissionsBitField.Flags.ViewChannel, PermissionsBitField.Flags.SendMessages, PermissionsBitField.Flags.ReadMessageHistory] });
          }
        } else {
          overwritesToSet.push({ id: "1519069239254974475", deny: [PermissionsBitField.Flags.ViewChannel] });
        }
        await ch.permissionOverwrites.set(overwritesToSet);
      }
    }

    // Przywróć dostęp właścicielowi ticketu - zawsze musi widzieć
    if (ticketData && ticketData.userId) {
      await ch.permissionOverwrites.edit(ticketData.userId, {
        ViewChannel: true,
        SendMessages: true,
        ReadMessageHistory: true,
      }).catch(() => null);
    }

    // Usuń uprawnienia osoby przejmującej
    if (ticketData.claimedBy) {
      await ch.permissionOverwrites.delete(ticketData.claimedBy).catch(() => null);
    }

    ticketData.claimedBy = null;
    ticketOwners.set(channelId, ticketData);
    scheduleSavePersistentState();

    if (ticketData.ticketMessageId) {
      await editTicketMessageButtons(ch, ticketData.ticketMessageId, null).catch(() => null);
    }

    // log do logi-ticket + backup wiadomości przed czyszczeniem
    try {
      const logCh = await getLogiTicketChannel(interaction.guild);
      // backup wiadomości przed usunięciem
      let backupAttachment = null;
      try {
        const messages = await ch.messages.fetch({ limit: 100 }).catch(() => null);
        if (messages && messages.size) {
          const sorted = Array.from(messages.values()).sort((a, b) => a.createdTimestamp - b.createdTimestamp);
          const lines = sorted.map((m) => {
            const ts = new Date(m.createdTimestamp).toISOString();
            const author = `${m.author.tag} (${m.author.id})`;
            const content = (m.content || "").replace(/\n/g, " ");
            const attachments = m.attachments?.size ? ` [załączniki: ${Array.from(m.attachments.values()).map((a) => a.url).join(", ")}]` : "";
            return `[${ts}] ${author}: ${content}${attachments}`;
          });
          const buf = Buffer.from(lines.join("\n"), "utf8");
          backupAttachment = new AttachmentBuilder(buf, { name: `ticket_${channelId}_history.txt` });
        }
      } catch (e) {
        console.error("Backup messages before unclaim failed:", e);
      }

      if (logCh) {
        await sendTicketLogEntry(interaction.guild, {
          title: "Ticket zwolniony",
          icon: "🟡",
          color: COLOR_YELLOW,
          summary: null,
          ticketChannel: ch,
          ownerId: ticketData.userId,
          actorId: interaction.user.id,
          claimedById: previousClaimerId,
          ticketMeta: ticketData,
          statusLabel: "OTWARTY",
          reason: reason || null,
          detailLines: [],
          files: backupAttachment ? [backupAttachment] : [],
        }).catch(() => null);
      }
    } catch (e) {
      console.error("Log unclaim failed:", e);
    }

    // wyczyść historię kanału od czasu przejęcia do teraz (zostawiając samą wiadomość o przejęciu)
    try {
      let claimMsg = null;
      if (ticketData.lastClaimMsgId) {
        claimMsg = await ch.messages.fetch(ticketData.lastClaimMsgId).catch(() => null);
      }

      const msgs = await ch.messages.fetch({ limit: 100 }).catch(() => null);
      if (msgs && msgs.size) {
        const toDelete = msgs.filter((m) => {
          if (claimMsg && m.id === claimMsg.id) return true;
          if (m.id === interaction.message?.id) return false;
          if (claimMsg) return m.createdTimestamp >= claimMsg.createdTimestamp;
          return true;
        });
        if (toDelete.size) {
          await ch.bulkDelete(toDelete, true).catch(() => null);
        }
      }
    } catch (e) {
      console.error("Nie udało się wyczyścić historii kanału po odprzejęciu:", e);
    }

    let description = `> \`🔓\` × Ticket został zwolniony przez: <@${interaction.user.id}>`;
    if (reason && reason.trim()) {
      description += `\n> Powód: **${reason}**`;
    }
    const publicEmbed = new EmbedBuilder()
      .setColor(COLOR_BLUE)
      .setDescription(description);
    const isHelpTicket = ticketData.ticketTypeLabel === "PYTANIE";
    const notifyRoleId = isHelpTicket ? "1519069239254974475" : BASE_SELLER_ROLE_ID;
    const sentUnclaim = await ch.send({
      content: `<@&${notifyRoleId}>`,
      embeds: [publicEmbed],
      allowedMentions: { roles: [notifyRoleId] },
    }).catch(() => null);
    if (sentUnclaim && sentUnclaim.id) {
      ticketData.lastUnclaimMsgId = sentUnclaim.id;
      ticketOwners.set(channelId, ticketData);
      scheduleSavePersistentState();
    }
    if (!isBtn) {
      await interaction.editReply({ content: "> `✅` × Pomyślnie zwolniono ticket.", flags: [MessageFlags.Ephemeral] }).catch(() => null);
    }
  } catch (err) {
    console.error("Błąd przy unclaim:", err);
    await replyEphemeral("> \`❌` Wystąpił błąd podczas odprzejmowania ticketu.");
  }
}

const SELLER_LIMIT_PACKAGES = Object.freeze([
  { value: "limit_20", label: "Limit 20", price: 60 },
  { value: "limit_50", label: "Limit 50", price: 150 },
  { value: "limit_100", label: "Limit 100", price: 250 },
  { value: "limit_200", label: "Limit 200", price: 400 },
  { value: "limit_400", label: "Limit 400", price: 600 },
  { value: "no_limit", label: "NO LIMIT", price: 800 },
]);

async function showSellerLimitModal(interaction) {
  const timestamp = Date.now();
  const modal = new ModalBuilder()
    .setCustomId(`modal_seller_limit_${timestamp}`)
    .setTitle("Zostań sprzedawcą")
    .addLabelComponents(
      new LabelBuilder()
        .setLabel("Jaki limit sprzedawcy chcesz zakupić?")
        .setStringSelectMenuComponent(
          new StringSelectMenuBuilder()
            .setCustomId(`seller_limit_${timestamp}`)
            .setPlaceholder("Wybierz limit")
            .setRequired(true)
            .setMinValues(1)
            .setMaxValues(1)
            .addOptions(SELLER_LIMIT_PACKAGES.map((pack) => ({
              label: pack.label,
              description: `Cena: ${pack.price}zł`,
              value: pack.value,
            }))),
        ),
    );
  await interaction.showModal(modal);
}

async function showSprzedazModal(interaction) {
  const timestamp = Date.now();
  const modal = new ModalBuilder()
    .setCustomId(`modal_sprzedaz_${timestamp}`)
    .setTitle("Sprzedaż")
    .addLabelComponents(
      new LabelBuilder()
        .setLabel("Co chcesz sprzedać?")
        .setTextInputComponent(
          new TextInputBuilder()
            .setCustomId(`co_sprzedac_${timestamp}`)
            .setPlaceholder("Przykład: 100k$")
            .setStyle(TextInputStyle.Short)
            .setRequired(true)
        ),
      new LabelBuilder()
        .setLabel("Na jakim serwerze?")
        .setStringSelectMenuComponent(
          new StringSelectMenuBuilder()
            .setCustomId(`sprzedaz_server_${timestamp}`)
            .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
            .setRequired(true)
            .setMinValues(1)
            .setMaxValues(1)
            .addOptions(TEST_PANEL_SERVER_OPTIONS)
        ),
      new LabelBuilder()
        .setLabel("Forma wypłaty")
        .setStringSelectMenuComponent(
          new StringSelectMenuBuilder()
            .setCustomId(`sprzedaz_payout_${timestamp}`)
            .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
            .setRequired(true)
            .setMinValues(1)
            .setMaxValues(1)
            .addOptions(PAYOUT_OPTIONS)
        ),
    );

  await interaction.showModal(modal);
}

async function findExistingOpenTicketForUser(guild, userId) {
  for (const [channelId, ticketData] of ticketOwners.entries()) {
    if (ticketData?.userId !== userId) continue;
    const existingChannel = await guild.channels.fetch(channelId).catch(() => null);
    if (existingChannel) {
      return channelId;
    }
    ticketOwners.delete(channelId);
    rewardTicketClaims.delete(channelId);
    scheduleSavePersistentState();
  }
  return null;
}

function buildRewardClaimSummary(availability) {
  const rewardLines = [];

  if (availability.inviteMilestones.length) {
    for (const milestone of availability.inviteMilestones) {
      rewardLines.push(
        `> <a:arrowwhite:1491476759290449984> × **Zaproszenia:** \`${milestone.label}\` za próg \`${milestone.threshold}\` zaproszeń`,
      );
    }
  }

  if (availability.freeKasaCashToClaim > 0) {
    rewardLines.push(
      `> <a:arrowwhite:1491476759290449984> × **FREE KASA do odebrania teraz:** \`${formatRewardCashAmount(availability.freeKasaCashToClaim)}\``,
    );
  }

  if (availability.freeKasaSwordCount > 0) {
    rewardLines.push(
      `> <a:arrowwhite:1491476759290449984> × **Przedmioty z FREE KASA:** \`${availability.freeKasaSwordCount}x Anarchiczny miecz\``,
    );
  }

  const historyLines = buildFreeKasaHistoryLines(availability.userId, 6).map(
    (line) => `> ${line}`,
  );

  const infoLines = [];
  if (availability.freeKasaCashToClaim > 0 || availability.freeKasaSwordCount > 0) {
    infoLines.push(
      "> <a:arrowwhite:1491476759290449984> × **Wyślij screeny wiadomości z FREE KASA potwierdzające te wygrane.**",
    );
  }

  return [
    ...rewardLines,
    "",
    "### ・ `📚` × Historia FREE KASA:",
    ...historyLines,
    "",
    ...infoLines,
  ]
    .filter((line) => line !== null && line !== undefined)
    .join("\n");
}

async function openRewardClaimTicket(interaction) {
  const guild = interaction.guild;
  const user = interaction.user;
  const categories = ticketCategories.get(guild.id) || {};

  const existingTicketId = await findExistingOpenTicketForUser(guild, user.id);
  if (existingTicketId) {
    await interaction.reply({
      content:
        `> \`❌\` × **Masz już otwarty** ticket: <#${existingTicketId}>\n` +
        "> `ℹ️` × Zamknij go, zanim otworzysz nowy.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const availability = getRewardClaimAvailability(guild.id, user.id);
  availability.userId = user.id;

  if (!availability.hasAnyClaim) {
    const missingInviteLine = availability.nextInviteMilestone
      ? `> \`📨\` × Do kolejnej nagrody z zaproszeń brakuje Ci \`${Math.max(
        0,
        availability.nextInviteMilestone.threshold - availability.displayedInvites,
      )}\` zaproszeń.`
      : "> `📨` × Wszystkie aktualne nagrody z zaproszeń masz już odebrane.";

    await interaction.reply({
      content:
        "> `❌` × Nie masz jeszcze nic do odebrania.\n" +
        `${missingInviteLine}\n` +
        `> \`🎁\` × Jeśli wygrałeś nagrodę w FREE KASA, wpisz kod w formularzu tej kategorii.\n` +
        `> \`📨\` × Nagrody z zaproszeń odbierzesz tutaj automatycznie po osiągnięciu progu.`,
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const ticketType = "odbior-nagrody";
  const ticketTypeLabel = "NAGRODA";
  const formInfo = buildRewardClaimSummary(availability);
  let parentToUse = REWARDS_CATEGORY_ID || categories["odbior-nagrody"] || null;
  if (!parentToUse) {
    const foundCat = guild.channels.cache.find(
      (c) =>
        c.type === ChannelType.GuildCategory &&
        c.name &&
        c.name.toLowerCase().includes("odbior"),
    );
    if (foundCat) parentToUse = foundCat.id;
  }

  const createOptions = {
    name: `ticket-${user.username}`,
    type: ChannelType.GuildText,
    permissionOverwrites: [
      {
        id: guild.id,
        deny: [PermissionsBitField.Flags.ViewChannel],
      },
      {
        id: user.id,
        allow: [
          PermissionsBitField.Flags.ViewChannel,
          PermissionsBitField.Flags.SendMessages,
          PermissionsBitField.Flags.ReadMessageHistory,
        ],
      },
      {
        id: "1519069239254974475",
        deny: [PermissionsBitField.Flags.ViewChannel],
      },
    ],
  };
  if (parentToUse) createOptions.parent = parentToUse;

  const channel = await ticketCategoryManager.create(guild, ticketType, createOptions);

  const headerText = `\`\`\`text\n🛒 NEW SHOP × ${ticketTypeLabel}\n\`\`\``;
  const bodyText =
    `### ・ \`👤\` × **Informacje o kliencie:**\n` +
    `> <a:arrowwhite:1491476759290449984> × **Ping:** <@${user.id}>\n` +
    `> <a:arrowwhite:1491476759290449984> × **Nick:** ${formatInlineCodeText(getSafeTicketDisplayName(interaction.member, user))}\n` +
    `> <a:arrowwhite:1491476759290449984> × **ID:** \`${user.id}\`\n` +
    `### ・ \`📋\` × **Informacje z formularza:**\n` +
    `${formInfo}`;

  const closeButton = new ButtonBuilder()
    .setCustomId(`ticket_close_${channel.id}`)
    .setLabel("︲Zamknij")
    .setStyle(ButtonStyle.Secondary)
    .setEmoji({ id: "1537166621527908382", name: "NO", animated: true });
  const settingsButton = new ButtonBuilder()
    .setCustomId(`ticket_settings_${channel.id}`)
    .setLabel("⚙️︲Ustawienia")
    .setStyle(ButtonStyle.Secondary);
  const claimButton = new ButtonBuilder()
    .setCustomId(`ticket_claim_${channel.id}`)
    .setLabel("🔒︲Przejmij")
    .setStyle(ButtonStyle.Secondary);

  const buttonRow = new ActionRowBuilder().addComponents(
    closeButton,
    settingsButton,
    claimButton,
  );

  const payload = buildTicketContainerPayload({
    headerText,
    bodyText,
    buttonRow,
    guildId: interaction.guild?.id,
  });

  {
    const _pingRoles = getPingRolesForTicketType(ticketType);
    if (_pingRoles.length > 0) {
      await channel.send({
        content: _pingRoles.map(id => `<@&${id}>`).join(" "),
        allowedMentions: { roles: _pingRoles },
      }).catch(() => null);
    }
  }

  const sentMsg = await channel.send(payload);

  ticketOwners.set(channel.id, {
    claimedBy: null,
    userId: user.id,
    ticketMessageId: sentMsg.id,
    locked: false,
    ticketTypeLabel,
    formInfo,
    openedAt: Date.now(),
  });

  rewardTicketClaims.set(channel.id, {
    guildId: guild.id,
    userId: user.id,
    inviteMilestones: availability.inviteMilestones.map((milestone) => milestone.threshold),
    freeKasaCashToClaim: availability.freeKasaCashToClaim,
    freeKasaSwordCount: availability.freeKasaSwordCount,
    createdAt: Date.now(),
  });
  scheduleSavePersistentState(true);

  await logTicketCreation(guild, channel, {
    openerId: user.id,
    ticketTypeLabel,
    formInfo,
    ticketMessageId: sentMsg.id,
  });

  await interaction.reply({
    content: `> \`✅\` × Ticket został stworzony: <#${channel.id}>`,
    flags: [MessageFlags.Ephemeral],
  });
}

async function commitRewardTicketClaim(channelId) {
  const claimData = rewardTicketClaims.get(channelId);
  if (!claimData) return;

  try {
    if (claimData.guildId && claimData.userId && Array.isArray(claimData.inviteMilestones)) {
      const claimedLevels = getClaimedInviteRewardLevels(claimData.guildId, claimData.userId);
      for (const milestone of claimData.inviteMilestones) {
        claimedLevels.add(String(milestone));
      }
    }

    if (claimData.userId) {
      const state = getFreeKasaRewardProgress(claimData.userId);
      state.cashBalance = Math.max(
        0,
        Number(state.cashBalance || 0) - Number(claimData.freeKasaCashToClaim || 0),
      );
      state.pendingSwords = Math.max(
        0,
        Number(state.pendingSwords || 0) - Number(claimData.freeKasaSwordCount || 0),
      );
      freeKasaRewardProgress.set(claimData.userId, state);
    }
  } finally {
    rewardTicketClaims.delete(channelId);
    scheduleSavePersistentState(true);
  }
}

async function showOdbiorModal(interaction) {
  const timestamp = Date.now();
  const modal = new ModalBuilder()
    .setCustomId(`modal_odbior_${timestamp}`)
    .setTitle("Odbierz nagrodę");

  const codeInput = new TextInputBuilder()
    .setCustomId(`reward_code_${timestamp}`)
    .setLabel("Kod nagrody")
    .setStyle(TextInputStyle.Short)
    .setRequired(true)
    .setMaxLength(64)
    .setPlaceholder("123XYZABCQWERTY");

  modal.addComponents(new ActionRowBuilder().addComponents(codeInput));
  await interaction.showModal(modal);
}

async function showInneModal(interaction) {
  const timestamp = Date.now();
  const modal = new ModalBuilder()
    .setCustomId(`modal_inne_${timestamp}`)
    .setTitle("Pomoc");

  const sprawaInput = new TextInputBuilder()
    .setCustomId(`sprawa_${timestamp}`)
    .setLabel("W jakiej sprawie robisz ticketa?")
    .setStyle(TextInputStyle.Paragraph)
    .setMaxLength(256)
    .setRequired(true);

  modal.addComponents(new ActionRowBuilder().addComponents(sprawaInput));

  await interaction.showModal(modal);
}

function getBaseCustomId(customId) {
  if (!customId) return "";
  const prefixes = [
    "modal_wystaw_opinie",
    "modal_ile_otrzymam",
    "modal_ile_musze_dac",
    "modal_testpanel_purchase",
    "modal_zakup",
    "modal_mody_zakup",
    "modal_autorynek_zakup",
    "modal_sprzedaz",
    "modal_seller_limit",
    "modal_odbior",
    "modal_inne"
  ];
  for (const prefix of prefixes) {
    if (customId.startsWith(prefix)) {
      return prefix;
    }
  }
  return customId;
}

async function handleModalSubmit(interaction) {
  const guildId = interaction.guildId;
  if (!guildId || !interaction.guild) return;

  const rawCid = interaction.customId || "";
  const cid = getBaseCustomId(rawCid);

  // --- DODAJ ROZLICZENIE (PRZYCISK) ---
  if (rawCid === "modal_rozliczenie_dodaj") {
    if (interaction.replied || interaction.deferred) return;
    const kwotaRaw = interaction.fields.getTextInputValue("kwota_sprzedazy");
    const kwota = parseInt(kwotaRaw.replace(/[^0-9]/g, ""), 10);

    if (isNaN(kwota) || kwota <= 0) {
      await interaction.reply({
        content: "> `❌` × Podano nieprawidłową kwotę. Wpisz samą liczbę dodatnią (Przykład: 50).",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const userId = interaction.user.id;
    if (!weeklySales.has(userId)) {
      weeklySales.set(userId, {
        amount: 0,
        lastUpdate: Date.now(),
        paid: false,
        paidAt: null,
        guildId: interaction.guild.id,
      });
    }

    const userData = weeklySales.get(userId);
    if (userData.paid) {
      userData.amount = 0;
      userData.paid = false;
      userData.paidAt = null;
    }
    userData.amount += kwota;
    userData.lastUpdate = Date.now();
    userData.guildId = interaction.guild.id;
    weeklySales.set(userId, userData);

    await db.saveWeeklySale(
      userId,
      userData.amount,
      interaction.guild.id,
      userData.paid || false,
      userData.paidAt || null,
      userData.lastUpdate,
    );
    scheduleSavePersistentState(true);

    const prowizja = (userData.amount * ROZLICZENIA_PROWIZJA).toFixed(2);

    const klientEmojiModal = findGuildEmojiByName(interaction.guildId, "klient");
    const userEmojiStrModal = klientEmojiModal ? toGuildEmojiMarkup(klientEmojiModal) : "👤";

    const embed = new EmbedBuilder()
      .setColor(COLOR_BLUE)
      .setTitle("`💱` Rozliczenie dodane")
      .setDescription(
        `> ${userEmojiStrModal} **Użytkownik:** <@${userId}>\n` +
        `> \`✅\` × **Dodano sprzedaż:** ${kwota.toLocaleString("pl-PL")} zł\n` +
        `> \`📊\` × **Suma tygodniowa:** ${userData.amount.toLocaleString("pl-PL")} zł`
      );

    await interaction.deferReply({ flags: [MessageFlags.Ephemeral] }).catch(() => null);

    await interaction.channel.send({
      embeds: [embed],
    }).catch(() => null);

    await interaction.deleteReply().catch(() => null);

    setTimeout(sendRozliczeniaMessage, 1000);
    setTimeout(() => sendRozliczeniaStatusReport(interaction.guild), 1000);
    return;
  }

  if (cid === "modal_odprzejmij") {
    const reason = getModalTextInputValueSafe(interaction, "powod_odprzejmij");
    const expectedClaimer = rawCid.split("_")[2] || null;
    await ticketUnclaimCommon(interaction, interaction.channelId || interaction.channel?.id, expectedClaimer, reason);
    return;
  }

  if (cid === "modal_wystaw_opinie") {
    const czas = getOpinionRatingValue(interaction, "czas_oczekiwania");
    const przebieg = getOpinionRatingValue(interaction, "przebieg_transakcji");
    const realizacja = getOpinionRatingValue(interaction, "realizacja_wymiany");
    const tresc = getModalTextInputValueSafe(interaction, "tresc_opinii") || "";

    if (!czas || !przebieg || !realizacja) {
      await interaction.reply({
        content: "> `❌` × Wybierz ocenę w każdej rozwijanej kategorii.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    if (await shouldRejectUnverifiedOpinion(
      interaction.user.id,
      guildId,
      [czas, przebieg, realizacja],
      tresc,
    )) {
      await interaction.reply({
        content: "> `❌` × Twoja opinia nie została opublikowana, dokonaj pierwszego zakupu i spróbuj ponownie.",
        flags: [MessageFlags.Ephemeral],
      });
      console.log(`[opinia-filter] Odrzucono podejrzaną opinię użytkownika ${interaction.user.id} bez historii zakupów.`);
      return;
    }

    // Simulate /opinia command logic with the new modal fields
    const normalize = (s = "") =>
      s
        .toString()
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .replace(/[^a-z0-9 _-]/gi, "")
        .trim()
        .toLowerCase();

    let allowedChannelId = opinieChannels.get(guildId);
    if (!allowedChannelId) {
      const found = interaction.guild.channels.cache.find(
        (c) =>
          c.type === ChannelType.GuildText &&
          (c.name === "⭐-×┃opinie-klientow" ||
            normalize(c.name).includes("opinie") ||
            normalize(c.name).includes("opinie-klientow")),
      );
      if (found) {
        allowedChannelId = found.id;
        opinieChannels.set(guildId, found.id);
      }
    }

    if (!allowedChannelId) {
      await interaction.reply({
        content: `> \`❌\` × Nie znaleziono kanału opinii.`,
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    // Oznaczamy użycie cooldown
    opinionCooldowns.set(interaction.user.id, Date.now());

    const safeTresc = formatOpinionText(tresc);

    const description = [
      "```",
      "✅ New Shop × OPINIA",
      "```",
      `> \`👤\` **× Twórca opinii:** <@${interaction.user.id}>`,
      `> \`📝\` **× Treść:** ${safeTresc}`,
      "",
      `> \`⏳\` **× Czas oczekiwania:** ${formatOpinionStars(czas)}`,
      `> \`📋\` **× Jakość produktu:** ${formatOpinionStars(przebieg)}`,
      `> \`💸\` **× Cena produktu:** ${formatOpinionStars(realizacja)}`,
    ].join("\n");

    const opinionEmbed = new EmbedBuilder()
      .setColor(COLOR_BLUE)
      .setDescription(description)
      .setThumbnail(interaction.user.displayAvatarURL({ dynamic: true, size: 128 }));

    try {
      const targetChannel = interaction.guild.channels.cache.get(allowedChannelId) || await interaction.guild.channels.fetch(allowedChannelId);

      let botWebhook = null;
      try {
        const webhooks = await targetChannel.fetchWebhooks();
        botWebhook = webhooks.find((w) => w.owner?.id === client.user.id && w.name === "ZAKUP_ITy_OPINIE");
      } catch (e) { }

      if (!botWebhook) {
        botWebhook = await targetChannel.createWebhook({
          name: "ZAKUP_ITy_OPINIE",
          avatar: client.user.displayAvatarURL({ dynamic: true }),
        });
      }

      await botWebhook.send({
        content: "",
        embeds: [opinionEmbed],
        username: interaction.member?.displayName || interaction.user.globalName || interaction.user.username,
        avatarURL: interaction.user.displayAvatarURL({ dynamic: true, size: 128 }),
      });

      let instrMsg = null;
      const lastId = lastOpinionInstruction.get(allowedChannelId);
      if (lastId) {
        instrMsg = await targetChannel.messages.fetch(lastId).catch(() => null);
      }

      if (!instrMsg) {
        const found = await findBotMessageWithEmbed(
          targetChannel,
          (emb) => typeof emb.description === "string" && (emb.description.includes("Kliknij w przycisk na dole, aby podzielić się opinią") || emb.description.includes("Użyj **komendy**"))
        );
        if (found) instrMsg = found;
      }

      if (instrMsg) {
        await instrMsg.delete().catch(() => null);
        lastOpinionInstruction.delete(allowedChannelId);
      }

      const sent = await targetChannel.send(buildOpinionInstructionPayload());
      lastOpinionInstruction.set(allowedChannelId, sent.id);

      opinionAuthorUserIds.add(interaction.user.id);
      await interaction.reply({
        content: "> `✅` × **Twoja opinia** została opublikowana.",
        flags: [MessageFlags.Ephemeral],
      });
    } catch (e) {
      console.error(e);
      await interaction.reply({
        content: "> `❌` × Błąd podczas wysyłania opinii.",
        flags: [MessageFlags.Ephemeral],
      }).catch(() => null);
    }
    return;
  }

  if (cid === "seller_data_modal") {
    if (!isAdminOrSeller(interaction.member)) {
      await interaction.reply({
        content: "> `❗` × Ten formularz jest tylko dla sprzedawców.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const key = getSellerPaymentProfileKey(guildId, interaction.user.id);
    const existing = getSellerPaymentProfile(guildId, interaction.user.id) || {};

    const getField = (name) => {
      try {
        return interaction.fields.getTextInputValue(name);
      } catch {
        return null;
      }
    };

    const phoneRaw = getField("phone");
    const phone = phoneRaw ? phoneRaw.replace(/\s+/g, "").replace(/-/g, "") : null;
    const transferTitle = getField("transfer_title");
    const recipient = getField("recipient");
    const paypalEmail = getField("paypal_email");
    const ltcWallet = getField("ltc_wallet");
    const mypscEmail = getField("mypsc_email");

    const errors = [];
    if (phone && !PHONE_REGEX.test(phone)) {
      errors.push("Nieprawidłowy format numeru telefonu.");
    }
    if (paypalEmail && !EMAIL_REGEX.test(paypalEmail.trim())) {
      errors.push("Nieprawidłowy e-mail PayPal.");
    }
    if (ltcWallet && !LTC_REGEX.test(ltcWallet.trim())) {
      errors.push("Nieprawidłowy adres portfela LTC.");
    }
    if (mypscEmail && !EMAIL_REGEX.test(mypscEmail.trim())) {
      errors.push("Nieprawidłowy e-mail MyPSC.");
    }

    if (errors.length > 0) {
      await interaction.reply({
        content: `> \`❌\` × **Błąd walidacji danych:**\n${errors.map((e) => `> • ${e}`).join("\n")}`,
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const profile = normalizeSellerPaymentProfile({
      ...existing,
      phone: phone !== null ? phone : existing.phone,
      transferTitle: transferTitle !== null ? transferTitle : existing.transferTitle,
      recipient: recipient !== null ? recipient : existing.recipient,
      paypalEmail: paypalEmail !== null ? paypalEmail : existing.paypalEmail,
      ltcWallet: ltcWallet !== null ? ltcWallet : existing.ltcWallet,
      mypscEmail: mypscEmail !== null ? mypscEmail : existing.mypscEmail,
      updatedAt: Date.now(),
    });

    if (sellerPaymentProfileHasData(profile)) {
      sellerPaymentProfiles.set(key, profile);
      scheduleSavePersistentState(true);

      await interaction.reply({
        content: "> `✅` × Pomyślnie zapisano dane płatności.",
        flags: [MessageFlags.Ephemeral],
      });
    } else {
      sellerPaymentProfiles.delete(key);
      scheduleSavePersistentState(true);

      await interaction.reply({
        content: "> `🧹` × Formularz był pusty – pomyślnie wyczyszczono dane płatności.",
        flags: [MessageFlags.Ephemeral],
      });
    }
    return;
  }

  // --- ODPRZEJMIJ MODAL ---
  if (cid.startsWith("modal_odprzejmij")) {
    const reason = getModalTextInputValueSafe(interaction, "powod_odprzejmij");
    const expectedClaimer = cid.split("_")[2] || null;
    await ticketUnclaimCommon(interaction, interaction.channelId || interaction.channel?.id, expectedClaimer, reason);
    return;
  }

  const embedTestHeaderMatch = cid.match(/^embedtest_modal_header_(\d+)$/);
  if (embedTestHeaderMatch) {
    const [, messageId] = embedTestHeaderMatch;
    const state = embedTestStates.get(messageId);

    if (!state) {
      await interaction.reply({
        content: "> `❌` × Ta sesja edycji wygasła. Użyj `/embed-2` ponownie.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    if (state.ownerId !== interaction.user.id) {
      await interaction.reply({
        content: "> `❗` × Tylko autor testu może edytować ten embed.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    state.headerBadge = interaction.fields
      .getTextInputValue("header_badge")
      .trim();
    state.headerNote = interaction.fields
      .getTextInputValue("header_note")
      .trim();
    if (
      isRegulationEmbedState(state) &&
      interaction.fields.fields.get("panel_title")
    ) {
      state.title = interaction.fields.getTextInputValue("panel_title").trim();
    }
    embedTestStates.set(messageId, state);

    const updated = await updateEmbedTestMessage(state);
    if (!updated) {
      embedTestStates.delete(messageId);
      await interaction.reply({
        content: "> `❌` × Nie udało się zaktualizować wiadomości. Użyj `/embed-2` ponownie.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    await interaction.reply({
      ...buildEmbedTestControlPayload(
        state,
        isRegulationEmbedState(state)
          ? "Zaktualizowałem górę panelu regulaminu"
          : "Zaktualizowałem górę embeda",
      ),
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const regulationPageModalMatch = cid.match(/^regulamin_modal_page_(\d+)_(\d+)$/);
  if (regulationPageModalMatch) {
    const [, messageId, rawPageIndex] = regulationPageModalMatch;
    const state = embedTestStates.get(messageId);

    if (!state || !isRegulationEmbedState(state)) {
      await interaction.reply({
        content: "> `❌` × Ta sesja edycji wygasła. Użyj `/sprawdz-embed-2` ponownie.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    if (state.ownerId !== interaction.user.id) {
      await interaction.reply({
        content: "> `❗` × Tylko autor panelu może edytować ten regulamin.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const pages = getRegulationPanelPages(state).map((page) =>
      normalizeRegulationPage(page),
    );
    const safeIndex = Math.max(
      0,
      Math.min(Number(rawPageIndex) || 0, pages.length - 1),
    );

    pages[safeIndex] = {
      title: interaction.fields.getTextInputValue("page_title").trim(),
      body: interaction.fields.getTextInputValue("page_body").trim(),
    };

    setRegulationPagesOnState(state, pages);
    embedTestStates.set(messageId, state);

    const updated = await updateEmbedTestMessage(state);
    if (!updated) {
      embedTestStates.delete(messageId);
      await interaction.reply({
        content:
          "> `❌` × Nie udało się zaktualizować panelu regulaminu. Użyj `/sprawdz-embed-2` ponownie.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    await interaction.reply({
      ...buildRegulationPagesEditorPayload(state, safeIndex),
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const embedTestEmojisMatch = cid.match(/^embedtest_modal_emojis_(\d+)$/);
  if (embedTestEmojisMatch) {
    try {
      const [, messageId] = embedTestEmojisMatch;
      let state = embedTestStates.get(messageId);

      if (!state) {
        const channel = interaction.channel;
        const msg = await channel?.messages?.fetch(messageId).catch(() => null);
        if (msg) {
          state = reconstructEmbedTestStateFromMessage(msg, interaction.user.id);
        }
      }

      if (!state) {
        await interaction.reply({
          content: "> `❌` × Ta sesja edycji wygasła. Użyj `/sprawdz-embed-2` ponownie.",
          flags: [MessageFlags.Ephemeral],
        }).catch(() => null);
        return;
      }

      if (!isAdminOrSeller(interaction.member) && state.ownerId !== interaction.user.id) {
        await interaction.reply({
          content: "> `❗` × Brak wymaganych uprawnień.",
          flags: [MessageFlags.Ephemeral],
        }).catch(() => null);
        return;
      }

      const b1Emoji = getModalTextInputValueSafe(interaction, "button_one_emoji");
      const b2Emoji = getModalTextInputValueSafe(interaction, "button_two_emoji");
      const b3Emoji = getModalTextInputValueSafe(interaction, "button_three_emoji");

      if (b1Emoji !== null) state.buttonOneEmoji = b1Emoji.trim();
      if (b2Emoji !== null) state.buttonTwoEmoji = b2Emoji.trim();
      if (b3Emoji !== null) state.buttonThreeEmoji = b3Emoji.trim();

      embedTestStates.set(messageId, state);

      const updated = await updateEmbedTestMessage(state);
      if (!updated) {
        embedTestStates.delete(messageId);
        await interaction.reply({
          content: "> `❌` × Nie udało się zaktualizować wiadomości. Użyj `/sprawdz-embed-2` ponownie.",
          flags: [MessageFlags.Ephemeral],
        }).catch(() => null);
        return;
      }

      await interaction.reply({
        ...buildEmbedTestControlPayload(
          state,
          isRegulationEmbedState(state)
            ? "Zaktualizowałem emoji panelu regulaminu"
            : "Zaktualizowałem emoji embeda",
        ),
        flags: [MessageFlags.Ephemeral],
      }).catch(() => null);
    } catch (err) {
      console.error("Błąd embedtest_modal_emojis:", err);
      await interaction.reply({
        content: "> `❌` × Wystąpił błąd podczas edycji emoji.",
        flags: [MessageFlags.Ephemeral],
      }).catch(() => null);
    }
    return;
  }

  const embedTestContentMatch = cid.match(/^embedtest_modal_content_(\d+)$/);
  if (embedTestContentMatch) {
    try {
      const [, messageId] = embedTestContentMatch;
      let state = embedTestStates.get(messageId);

      if (!state) {
        const channel = interaction.channel;
        const msg = await channel?.messages?.fetch(messageId).catch(() => null);
        if (msg) {
          state = reconstructEmbedTestStateFromMessage(msg, interaction.user.id);
        }
      }

      if (!state) {
        await interaction.reply({
          content: "> `❌` × Ta sesja edycji wygasła. Użyj `/sprawdz-embed-2` ponownie.",
          flags: [MessageFlags.Ephemeral],
        }).catch(() => null);
        return;
      }

      if (!isAdminOrSeller(interaction.member) && state.ownerId !== interaction.user.id) {
        await interaction.reply({
          content: "> `❗` × Brak wymaganych uprawnień do edycji tego embeda.",
          flags: [MessageFlags.Ephemeral],
        }).catch(() => null);
        return;
      }

      const titleVal = getModalTextInputValueSafe(interaction, "title");
      const cashTitleVal = getModalTextInputValueSafe(interaction, "cash_section_title");
      const cashBodyVal = getModalTextInputValueSafe(interaction, "cash_body");
      const itemsTitleVal = getModalTextInputValueSafe(interaction, "items_section_title");
      const itemsBodyVal = getModalTextInputValueSafe(interaction, "items_body");

      if (titleVal !== null) state.title = titleVal.trim();

      if (isRegulationEmbedState(state)) {
        const pages = getRegulationPanelPages(state).map((page) =>
          normalizeRegulationPage(page),
        );
        pages[0] = {
          title: (cashTitleVal || "").trim(),
          body: (cashBodyVal || "").trim(),
        };
        pages[1] = {
          title: (itemsTitleVal || "").trim(),
          body: (itemsBodyVal || "").trim(),
        };
        setRegulationPagesOnState(state, pages);
      } else {
        if (cashTitleVal !== null) state.cashSectionTitle = cashTitleVal.trim();
        if (cashBodyVal !== null) state.cashBody = cashBodyVal.trim();
        if (itemsTitleVal !== null) state.itemsSectionTitle = itemsTitleVal.trim();
        if (itemsBodyVal !== null) state.itemsBody = itemsBodyVal.trim();
      }
      embedTestStates.set(messageId, state);

      const updated = await updateEmbedTestMessage(state);
      if (!updated) {
        embedTestStates.delete(messageId);
        await interaction.reply({
          content: "> `❌` × Nie udało się zaktualizować wiadomości. Użyj `/sprawdz-embed-2` ponownie.",
          flags: [MessageFlags.Ephemeral],
        }).catch(() => null);
        return;
      }

      await interaction.reply({
        ...buildEmbedTestControlPayload(
          state,
          isRegulationEmbedState(state)
            ? "Zaktualizowałem strony regulaminu"
            : "Zaktualizowałem treść embeda",
        ),
        flags: [MessageFlags.Ephemeral],
      }).catch(() => null);
    } catch (err) {
      console.error("Błąd zapisu embedtest_modal_content:", err);
      const errMsg = err?.message || String(err);
      await interaction.reply({
        content: `> \`❌\` × **Błąd zapisywania:** \`${errMsg}\``,
        flags: [MessageFlags.Ephemeral],
      }).catch(() => null);
    }
    return;
  }

  const embedTestExtraContentMatch = cid.match(
    /^embedtest_modal_content_extra_(\d+)$/,
  );
  if (embedTestExtraContentMatch) {
    const [, messageId] = embedTestExtraContentMatch;
    const state = embedTestStates.get(messageId);

    if (!state) {
      await interaction.reply({
        content: "> `❌` × Ta sesja edycji wygasła. Użyj `/embed-2` ponownie.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    if (state.ownerId !== interaction.user.id) {
      await interaction.reply({
        content: "> `❗` × Tylko autor testu może edytować ten embed.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    if (isRegulationEmbedState(state)) {
      const pages = getRegulationPanelPages(state).map((page) =>
        normalizeRegulationPage(page),
      );
      pages[2] = {
        title: interaction.fields.getTextInputValue("extra_section_title").trim(),
        body: interaction.fields.getTextInputValue("extra_section_body").trim(),
      };
      pages[3] = {
        title: interaction.fields
          .getTextInputValue("extra_section_two_title")
          .trim(),
        body: interaction.fields.getTextInputValue("extra_section_two_body").trim(),
      };
      setRegulationPagesOnState(state, pages);
    } else {
      state.extraSectionTitle = interaction.fields
        .getTextInputValue("extra_section_title")
        .trim();
      state.extraSectionBody = interaction.fields
        .getTextInputValue("extra_section_body")
        .trim();
      state.extraSectionTwoTitle = interaction.fields
        .getTextInputValue("extra_section_two_title")
        .trim();
      state.extraSectionTwoBody = interaction.fields
        .getTextInputValue("extra_section_two_body")
        .trim();
    }
    embedTestStates.set(messageId, state);

    const updated = await updateEmbedTestMessage(state);
    if (!updated) {
      embedTestStates.delete(messageId);
      await interaction.reply({
        content: "> `❌` × Nie udało się zaktualizować wiadomości. Użyj `/embed-2` ponownie.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    await interaction.reply({
      ...buildEmbedTestControlPayload(
        state,
        isRegulationEmbedState(state)
          ? "Zaktualizowałem strony 3-4 regulaminu"
          : "Zaktualizowałem dodatkowe sekcje embeda",
      ),
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const embedTestButtonsMatch = cid.match(/^embedtest_modal_buttons_(\d+)$/);
  if (embedTestButtonsMatch) {
    const [, messageId] = embedTestButtonsMatch;
    const state = embedTestStates.get(messageId);

    if (!state) {
      await interaction.reply({
        content: "> `❌` × Ta sesja edycji wygasła. Użyj `/embed-2` ponownie.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    if (state.ownerId !== interaction.user.id) {
      await interaction.reply({
        content: "> `❗` × Tylko autor testu może edytować ten embed.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const buttonTwoActionInput = interaction.fields.getTextInputValue("button_two_action").trim();
    let buttonThreeActionInput = "";
    try {
      buttonThreeActionInput = interaction.fields.getTextInputValue("button_three_action").trim();
    } catch (_) {}

    const buttonOneLabel = interaction.fields.getTextInputValue("button_one_label").trim();
    const buttonTwoLabel = interaction.fields.getTextInputValue("button_two_label").trim();

    // Przycisk 1 — jeśli brak nazwy, usuń przycisk
    if (!buttonOneLabel) {
      state.buttonOneLabel = "";
      state.buttonOneAction = "";
      state.buttonOneUrl = null;
    } else {
      state.buttonOneLabel = buttonOneLabel;
      state.buttonOneAction = state.buttonOneAction || "zakup";
      state.buttonOneUrl = null;
    }

    // Przycisk 2 — jeśli brak nazwy, usuń przycisk
    if (!buttonTwoLabel) {
      state.buttonTwoLabel = "";
      state.buttonTwoAction = "";
      state.buttonTwoUrl = null;
    } else {
      const parsedAction2 = parseEmbedTestPrimaryButtonActionInput(buttonTwoActionInput, state.buttonTwoAction);
      state.buttonTwoLabel = buttonTwoLabel;
      state.buttonTwoAction = parsedAction2.value;
      state.buttonTwoUrl = parsedAction2.url || null;

      if (state.buttonTwoAction === "link" && !state.buttonTwoUrl) {
        await interaction.reply({ content: "> `❌` × Podaj poprawny URL dla przycisku 2 (np. https://...).", flags: [MessageFlags.Ephemeral] });
        return;
      }
    }

    let buttonThreeLabel = "";
    try {
      buttonThreeLabel = interaction.fields.getTextInputValue("button_three_label").trim();
    } catch (_) {}

    // Przycisk 3 — jeśli brak nazwy, usuń przycisk
    if (!buttonThreeLabel) {
      state.buttonThreeLabel = "";
      state.buttonThreeAction = "";
      state.buttonThreeUrl = null;
    } else {
      const parsedAction3 = parseEmbedTestPrimaryButtonActionInput(buttonThreeActionInput, state.buttonThreeAction);
      state.buttonThreeLabel = buttonThreeLabel;
      state.buttonThreeAction = parsedAction3.value;
      state.buttonThreeUrl = parsedAction3.url || null;

      if (state.buttonThreeAction === "link" && !state.buttonThreeUrl) {
        const guild = interaction.guild || client.guilds.cache.get(state.guildId);
        const paymentsChannel = guild ? findEmbedTestPaymentsChannel(guild) : null;
        const defaultPaymentsUrl = guild ? getDiscordMessageUrl(guild.id, paymentsChannel?.id || state.channelId) : "";
        state.buttonThreeUrl = defaultPaymentsUrl;
        
        if (!state.buttonThreeUrl) {
          await interaction.reply({ content: "> `❌` × Podaj poprawny URL dla przycisku 3 (np. https://...).", flags: [MessageFlags.Ephemeral] });
          return;
        }
      }
    }

    embedTestStates.set(messageId, state);

    const updated = await updateEmbedTestMessage(state);
    if (!updated) {
      embedTestStates.delete(messageId);
      await interaction.reply({
        content: "> `❌` × Nie udało się zaktualizować wiadomości. Użyj `/embed-2` ponownie.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    await interaction.reply({
      ...buildEmbedTestControlPayload(state, "Zaktualizowałem przyciski embeda"),
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // quiz do przejęcia ticketu
  if (cid.startsWith("claim_quiz_")) {
    const data = pendingClaimQuiz.get(cid);
    if (!data || data.userId !== interaction.user.id) {
      await interaction.reply({ content: "> `❌` × Ta weryfikacja wygasła. Kliknij **Przejmij** ponownie.", flags: [MessageFlags.Ephemeral] }).catch(() => null);
      return;
    }
    const answer = (interaction.fields.getTextInputValue("claim_answer") || "").trim();
    if (answer.toLowerCase() !== data.answer.toLowerCase()) {
      await interaction.reply({ content: "> `❌` × Zła odpowiedź. Spróbuj ponownie.", flags: [MessageFlags.Ephemeral] }).catch(() => null);
      pendingClaimQuiz.delete(cid);
      return;
    }
    pendingClaimQuiz.delete(cid);
    await ticketClaimCommon(interaction, data.channelId, { skipQuiz: true });
    return;
  }

  // captcha do wlaczenia /autoprzejmij
  if (cid.startsWith("autoprzejmij_quiz_")) {
    const data = pendingAutoPrzejmijQuiz.get(cid);
    if (!data || data.userId !== interaction.user.id) {
      await interaction.reply({
        content: "> `❌` × Ta captcha wygasla. Uzyj /autoprzejmij ponownie.",
        flags: [MessageFlags.Ephemeral],
      }).catch(() => null);
      return;
    }

    const answer = (interaction.fields.getTextInputValue("autoprzejmij_answer") || "").trim();
    if (answer.toLowerCase() !== data.answer.toLowerCase()) {
      pendingAutoPrzejmijQuiz.delete(cid);
      await interaction.reply({
        content: "> `❌` × Zla odpowiedz captcha. Sprobuj ponownie.",
        flags: [MessageFlags.Ephemeral],
      }).catch(() => null);
      return;
    }

    pendingAutoPrzejmijQuiz.delete(cid);
    autoPrzejmijSettings.set(data.guildId, {
      enabled: true,
      ownerId: data.ownerId,
      ownerName: data.ownerName,
      enabledAt: Date.now(),
    });
    scheduleSavePersistentState();

    const stats = await runAutoPrzejmijSweep(
      interaction.guild,
      data.ownerId,
      data.ownerName,
      null,
    );

    await interaction.reply({
      content: formatAutoPrzejmijSummary(
        stats,
        "> `✅` × Od teraz tylko właściciel widzi tickety zakupowe.",
      ),
      flags: [MessageFlags.Ephemeral],
    }).catch(() => null);
    return;
  }

  const botName = client.user?.username || "NEWSHOP";

  // NEW: konkurs create modal
  if (interaction.customId === "konkurs_create_modal") {
    await handleKonkursCreateModal(interaction);
    return;
  }
  // KALKULATOR: ile otrzymam?
  if (cid === "modal_ile_otrzymam") {
    try {
      const kwotaStr = getModalTextInputValueSafe(interaction, "kwota") || "";
      const kwota = parseFloat(kwotaStr.replace(",", "."));
      const selectedServer =
        getModalStringSelectValueSafe(interaction, "kalkulator_server") || "";
      const selectedPayment =
        getModalStringSelectValueSafe(interaction, "kalkulator_payment") || "";

      if (isNaN(kwota) || kwota <= 0) {
        await interaction.reply({
          content: "> `❌` × Podaj **poprawną** kwotę w PLN.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      if (kwota > MAX_PURCHASE_PLN) {
        await interaction.reply({
          content: "> `❌` × Maksymalna kwota w kalkulatorze to **10 000zł**.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      if (selectedServer && selectedPayment) {
        const result = buildKalkulatorResultMessage({
          typ: "otrzymam",
          kwota,
          tryb: selectedServer,
          metoda: selectedPayment,
        });

        await interaction.reply({
          content: result.error || result.message,
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      // Fallback dla starszych wiadomości kalkulatora
      const userId = interaction.user.id;
      kalkulatorData.set(userId, { kwota, typ: "otrzymam" });

      const trybSelect = new StringSelectMenuBuilder()
        .setCustomId("kalkulator_tryb")
        .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
        .addOptions(KALKULATOR_SERVER_OPTIONS);

      const metodaSelect = new StringSelectMenuBuilder()
        .setCustomId("kalkulator_metoda")
        .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
        .addOptions(KALKULATOR_PAYMENT_OPTIONS);

      const embed = new EmbedBuilder()
        .setColor(COLOR_BLUE)
        .setDescription(
          "```\n" +
          "🔢 New Shop × Obliczanie\n" +
          "```\n" +
          `> \`💵\` × **Wybrana kwota:** \`${kwota.toFixed(2)}zł\`\n> \`❗\` × Wybierz serwer i metodę płatności __poniżej:__`);

      await interaction.reply({
        embeds: [embed],
        components: [
          new ActionRowBuilder().addComponents(trybSelect),
          new ActionRowBuilder().addComponents(metodaSelect)
        ],
        flags: [MessageFlags.Ephemeral]
      });
    } catch (error) {
      console.error("Błąd w modal_ile_otrzymam:", error);
      await interaction.reply({
        content: "> `❌` × **Wystąpił** błąd podczas przetwarzania. Spróbuj **ponownie**.",
        flags: [MessageFlags.Ephemeral]
      });
    }
    return;
  }

  // KALKULATOR: ile muszę dać?
  if (cid === "modal_ile_musze_dac") {
    try {
      const walutaStr = getModalTextInputValueSafe(interaction, "waluta") || "";
      const waluta = parseShortNumber(walutaStr);
      const selectedServer =
        getModalStringSelectValueSafe(interaction, "kalkulator_server") || "";
      const selectedPayment =
        getModalStringSelectValueSafe(interaction, "kalkulator_payment") || "";

      if (!waluta || waluta <= 0 || waluta > 999_000_000) {
        await interaction.reply({
          content: "> `❌` × Podaj **poprawną** ilość waluty (1–999 000 000, możesz użyć k/m).",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      if (selectedServer && selectedPayment) {
        const result = buildKalkulatorResultMessage({
          typ: "muszedac",
          waluta,
          tryb: selectedServer,
          metoda: selectedPayment,
        });

        await interaction.reply({
          content: result.error || result.message,
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      // Fallback dla starszych wiadomości kalkulatora
      const userId = interaction.user.id;
      kalkulatorData.set(userId, { waluta, typ: "muszedac" });

      const trybSelect = new StringSelectMenuBuilder()
        .setCustomId("kalkulator_tryb")
        .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
        .addOptions(KALKULATOR_SERVER_OPTIONS);

      const metodaSelect = new StringSelectMenuBuilder()
        .setCustomId("kalkulator_metoda")
        .setPlaceholder(DEFAULT_SELECT_EMPTY_PLACEHOLDER)
        .addOptions(KALKULATOR_PAYMENT_OPTIONS);

      const embed = new EmbedBuilder()
        .setColor(COLOR_BLUE)
        .setDescription(
          "```\n" +
          "🔢 New Shop × Obliczanie\n" +
          "```\n" +
          `> \`💲\` × **Wybrana ilość waluty:** \`${formatShortWaluta(waluta)}\`\n> \`❗\` × Wybierz serwer i metodę płatności __poniżej:__`);

      await interaction.reply({
        embeds: [embed],
        components: [
          new ActionRowBuilder().addComponents(trybSelect),
          new ActionRowBuilder().addComponents(metodaSelect)
        ],
        flags: [MessageFlags.Ephemeral]
      });
    } catch (error) {
      console.error("Błąd w modal_ile_musze_dac:", error);
      await interaction.reply({
        content: "> \`❌\` **Wystąpił błąd podczas przetwarzania. Spróbuj ponownie.**",
        flags: [MessageFlags.Ephemeral]
      });
    }
    return;
  }

  // NEW: konkurs join modal
  if (interaction.customId.startsWith("konkurs_join_modal_")) {
    const msgId = interaction.customId.replace("konkurs_join_modal_", "");
    await handleKonkursJoinModal(interaction, msgId);
    return;
  }

  // NEW: verification modal handling
  if (interaction.customId.startsWith("modal_verify_")) {
    const modalId = interaction.customId;
    const record = pendingVerifications.get(modalId);

    if (!record) {
      await interaction.reply({
        content:
          "> \`❌\` **Nie mogę znaleźć zapisanego zadania weryfikacji (spróbuj ponownie).**",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    if (record.userId !== interaction.user.id) {
      await interaction.reply({
        content:
          "> \`❌\` **Tylko użytkownik, który kliknął przycisk, może rozwiązać tę zagadkę.**",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const entered = interaction.fields
      .getTextInputValue("verify_answer")
      .trim();
    const numeric = parseInt(entered.replace(/[^0-9\-]/g, ""), 10);

    if (Number.isNaN(numeric)) {
      await interaction.reply({
        content: "\`❌\` **Nieprawidłowa odpowiedź (powinna być liczbą).**",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    if (numeric !== record.answer) {
      await interaction.reply({
        content: "> \`❌\` × **Źle! Nieprawidłowy wynik. Spróbuj jeszcze raz.**",
        flags: [MessageFlags.Ephemeral],
      });
      // remove record so they can request a new puzzle
      pendingVerifications.delete(modalId);
      return;
    }

    // correct answer
    pendingVerifications.delete(modalId);

    let roleId = record.roleId;
    const guild = interaction.guild;

    // if no roleId recorded, try to find dynamically in guild and cache it
    if (!roleId && guild) {
      const normalize = (s = "") =>
        s
          .toString()
          .normalize("NFD")
          .replace(/[\u0300-\u036f]/g, "")
          .replace(/[^a-z0-9 ]/gi, "")
          .trim()
          .toLowerCase();

      let role =
        guild.roles.cache.find(
          (r) => r.name === DEFAULT_NAMES.verificationRoleName,
        ) ||
        guild.roles.cache.find((r) =>
          normalize(r.name).includes(normalize("klient")),
        );

      if (role) {
        roleId = role.id;
        verificationRoles.set(guild.id, roleId);
        scheduleSavePersistentState();
        console.log(
          `Dynamicznie ustawiono rolę weryfikacji dla guild ${guild.id}: ${role.name} (${roleId})`,
        );
      } else {
        console.log(
          `Nie znaleziono roli weryfikacji w guild ${guild.id} podczas nadawania roli.`,
        );
      }
    }

    if (!roleId) {
      await interaction.reply({
        content:
          "✅ Poprawnie! Niestety rola weryfikacji nie została znaleziona. Skontaktuj się z administracją.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    try {
      // give role
      const member = await guild.members.fetch(interaction.user.id);
      await member.roles.add(roleId, "Przejście weryfikacji");

      // prepare DM embed (as requested)
      const dmEmbed = new EmbedBuilder()
        .setColor(COLOR_BLUE)
        .setDescription(
          "```\n" +
          "🛒 New Shop × WERYFIKACJA\n" +
          "```\n" +
          "`✨` Gratulacje!\n\n" +
          "`📝` Pomyślnie przeszedłeś weryfikacje na naszym serwerze discord życzymy udanych zakupów!",
        )
        .setTimestamp();

      // send DM to user
      try {
        await interaction.user.send({ embeds: [dmEmbed] });
        // ephemeral confirmation (not public)
        await interaction.reply({
          content: "> \`✅\` × Zostałeś pomyślnie zweryfikowany",
          flags: [MessageFlags.Ephemeral],
        });
      } catch (dmError) {
        console.error("Nie udało się wysłać DM po weryfikacji:", dmError);
        await interaction.reply({
          content: "> \`✅\` × Zostałeś pomyślnie zweryfikowany",
          flags: [MessageFlags.Ephemeral],
        });
      }

      console.log(
        `Użytkownik ${interaction.user.username} przeszedł weryfikację na serwerze ${guild.id}`,
      );
    } catch (error) {
      console.error("Błąd przy nadawaniu roli po weryfikacji:", error);
      await interaction.reply({
        content: "> \`❌\` **Wystąpił błąd przy nadawaniu roli.**",
        flags: [MessageFlags.Ephemeral],
      });
    }
    return;
  }

  // redeem code modal handling (used in tickets)
  if (interaction.customId.startsWith("modal_redeem_code_")) {
    const { code: enteredCode, codeData } = await getActiveCodeData(
      interaction.fields.getTextInputValue("discount_code"),
    );

    if (!codeData) {
      await interaction.reply({
        content:
          "❌ **Nieprawidłowy kod!**",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    // Sprawdź typ kodu
    if (
      codeData.type === "invite_cash" ||
      codeData.type === "invite_reward" ||
      codeData.type === "free_kasa_reward"
    ) {
      await interaction.reply({
        content:
          "❌ Ten kod odbierzesz tylko w kategorii 'Odbierz nagrodę' w TicketPanel.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    if (codeData.used) {
      await interaction.reply({
        content: "> `❌` × **Kod** został już wykorzystany!",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    if (Date.now() > codeData.expiresAt) {
      activeCodes.delete(enteredCode);
      await db.deleteActiveCode(enteredCode);
      scheduleSavePersistentState();
      await interaction.reply({
        content: "> `❌` × **Kod** wygasł!",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    codeData.used = true;
    activeCodes.delete(enteredCode);
    await db.deleteActiveCode(enteredCode);

    // Aktualizuj w Supabase
    await db.updateActiveCode(enteredCode, { used: true });

    scheduleSavePersistentState();

    await interaction.reply(
      buildDiscountRedemptionPayload({
        code: enteredCode,
        discount: codeData.discount,
        guildId: interaction.guildId || null,
      }),
    );
    console.log(
      `Użytkownik ${interaction.user.username} odebrał kod rabatowy ${enteredCode} (-${codeData.discount}%)`,
    );
    return;
  }

  // Ticket settings modals: rename/add/remove
  if (interaction.customId.startsWith("modal_rename_")) {
    const chId = interaction.customId.replace("modal_rename_", "");
    const newName = interaction.fields
      .getTextInputValue("new_ticket_name")
      .trim();
    const channel = await interaction.guild.channels
      .fetch(chId)
      .catch(() => null);
    if (!channel) {
      await interaction.reply({
        content: "> `❌` × **Błąd** z próbą odnalezienia **kanału**.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }
    const data = ticketOwners.get(chId) || {
      claimedBy: null,
      ticketMessageId: null,
    };
    const claimer = data.claimedBy;

    if (!isAdminOrSeller(interaction.member)) {
      await interaction.reply({
        content: "> `❗` × Brak wymaganych uprawnień.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }
    if (
      claimer &&
      claimer !== interaction.user.id &&
      !isAdminOrSeller(interaction.member)
    ) {
      await interaction.reply({
        content: "> `❗` × Brak wymaganych uprawnień.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }
    try {
      const oldName = channel.name;
      await channel.setName(newName);

      await sendTicketLogEntry(interaction.guild, {
        title: "Zmieniono nazwę ticketu",
        icon: "📝",
        color: COLOR_BLUE,
        summary: "Nazwa ticketu została zmieniona przez obsługę.",
        ticketChannel: channel,
        ownerId: data.userId || null,
        actorId: interaction.user.id,
        claimedById: data.claimedBy || null,
        ticketMeta: data,
        statusLabel: data.claimedBy ? "PRZEJĘTY" : "OTWARTY",
        detailLines: [
          `Stara nazwa: ${oldName}`,
          `Nowa nazwa: ${newName}`,
        ],
      }).catch((err) => console.error("ticket rename log error:", err));

      await interaction.reply({
        content: `✅ Zmieniono nazwę ticketu na \`${newName}\`.`,
        flags: [MessageFlags.Ephemeral],
      });
    } catch (err) {
      console.error("Błąd zmiany nazwy ticketu:", err);
      await interaction.reply({
        content: "> `❌` × **Nie udało się** zmienić nazwy **ticketu**.",
        flags: [MessageFlags.Ephemeral],
      });
    }
    return;
  }

  if (interaction.customId.startsWith("modal_add_")) {
    const chId = interaction.customId.replace("modal_add_", "");
    const userInput = interaction.fields
      .getTextInputValue("user_to_add")
      .trim();
    const channel = await interaction.guild.channels
      .fetch(chId)
      .catch(() => null);
    if (!channel) {
      await interaction.reply({
        content: "> `❌` × **Kanał** nie znaleziony.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }
    const data = ticketOwners.get(chId) || { claimedBy: null };
    const claimer = data.claimedBy;

    if (!isAdminOrSeller(interaction.member)) {
      await interaction.reply({
        content: "> `❗` × Brak wymaganych uprawnień.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }
    if (
      claimer &&
      claimer !== interaction.user.id &&
      !isAdminOrSeller(interaction.member)
    ) {
      await interaction.reply({
        content: "> `❗` × Brak wymaganych uprawnień.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    // parse mention or id
    const match =
      userInput.match(/<@!?(\d+)>/) || userInput.match(/(\d{17,20})/);
    if (!match) {
      await interaction.reply({
        content: "> `❌` × **Nieprawidłowy** format użytkownika. Podaj **@mention** lub **ID**.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }
    const userIdToAdd = match[1];
    try {
      await channel.permissionOverwrites.edit(userIdToAdd, {
        ViewChannel: true,
        SendMessages: true,
        ReadMessageHistory: true,
      });

      await sendTicketLogEntry(interaction.guild, {
        title: "Dodano użytkownika do ticketu",
        icon: "👥",
        color: COLOR_BLUE,
        summary: "Do ticketu został dodany dodatkowy użytkownik.",
        ticketChannel: channel,
        ownerId: data.userId || null,
        actorId: interaction.user.id,
        claimedById: data.claimedBy || null,
        ticketMeta: data,
        statusLabel: data.claimedBy ? "PRZEJĘTY" : "OTWARTY",
        detailLines: [`Dodano użytkownika: <@${userIdToAdd}>`],
      }).catch((err) => console.error("ticket add-user log error:", err));

      await interaction.reply({
        content: `✅ Dodano <@${userIdToAdd}> do ticketu.`,
        flags: [MessageFlags.Ephemeral],
      });
    } catch (err) {
      console.error("Błąd dodawania użytkownika do ticketu:", err);
      await interaction.reply({
        content: "> `❌` × **Nie udało się** dodać użytkownika (sprawdź uprawnienia).",
        flags: [MessageFlags.Ephemeral],
      });
    }
    return;
  }

  if (interaction.customId.startsWith("modal_remove_")) {
    const chId = interaction.customId.replace("modal_remove_", "");
    const userInput = interaction.fields
      .getTextInputValue("user_to_remove")
      .trim();
    const channel = await interaction.guild.channels
      .fetch(chId)
      .catch(() => null);
    if (!channel) {
      await interaction.reply({
        content: "> `❌` × **Kanał** nie znaleziony.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }
    const data = ticketOwners.get(chId) || { claimedBy: null };
    const claimer = data.claimedBy;

    if (!isAdminOrSeller(interaction.member)) {
      await interaction.reply({
        content: "> `❗` × Brak wymaganych uprawnień.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }
    if (
      claimer &&
      claimer !== interaction.user.id &&
      !isAdminOrSeller(interaction.member)
    ) {
      await interaction.reply({
        content: "> `❗` × Brak wymaganych uprawnień.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const match =
      userInput.match(/<@!?(\d+)>/) || userInput.match(/(\d{17,20})/);
    if (!match) {
      await interaction.reply({
        content: "> `❌` × **Nieprawidłowy** format użytkownika. Podaj **@mention** lub **ID**.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }
    const userIdToRemove = match[1];
    try {
      await channel.permissionOverwrites
        .delete(userIdToRemove)
        .catch(() => null);

      await sendTicketLogEntry(interaction.guild, {
        title: "Usunięto użytkownika z ticketu",
        icon: "➖",
        color: COLOR_YELLOW,
        summary: "Z ticketu usunięto dodatkowego użytkownika.",
        ticketChannel: channel,
        ownerId: data.userId || null,
        actorId: interaction.user.id,
        claimedById: data.claimedBy || null,
        ticketMeta: data,
        statusLabel: data.claimedBy ? "PRZEJĘTY" : "OTWARTY",
        detailLines: [`Usunięto użytkownika: <@${userIdToRemove}>`],
      }).catch((err) => console.error("ticket remove-user log error:", err));

      await interaction.reply({
        content: `✅ Usunięto <@${userIdToRemove}> z ticketu.`,
        flags: [MessageFlags.Ephemeral],
      });
    } catch (err) {
      console.error("Błąd usuwania użytkownika z ticketu:", err);
      await interaction.reply({
        content: "> `❌` × **Nie udało się** usunąć użytkownika (sprawdź uprawnienia).",
        flags: [MessageFlags.Ephemeral],
      });
    }
    return;
  }

  // Ticket modal flows follow...
  const ticketNumber = getNextTicketNumber(guildId);
  const categories = ticketCategories.get(guildId) || {};
  const user = interaction.user;

  let categoryId;
  let ticketType;
  let ticketTypeLabel;
  let formInfo;
  let ticketTopic;
  let forceOwnerOnlyVisibility = false;
  let preferredChannelName = null;
  let paymentMethod = null;

  switch (cid) {
    case "modal_testpanel_purchase":
    case "modal_zakup": {
      const itemToBuy =
        (getModalTextInputValueSafe(interaction, "co_kupic") || "").trim();
      const selectedServer =
        getModalStringSelectValueSafe(interaction, "zakup_server") ||
        getModalStringSelectValueSafe(interaction, "testpanel_purchase_server") ||
        getModalTextInputValueSafe(interaction, "serwer") ||
        "";
      const selectedPayment =
        getModalStringSelectValueSafe(interaction, "zakup_payment") ||
        getModalStringSelectValueSafe(interaction, "testpanel_purchase_payment") ||
        getModalTextInputValueSafe(interaction, "platnosc") ||
        "";
      const kwotaRaw = getModalTextInputValueSafe(interaction, "kwota") || "";
      let kwotaNum = parseFloat(
        kwotaRaw.replace(/[^0-9,.\-]/g, "").replace(/,/g, "."),
      );

      if (Number.isNaN(kwotaNum)) {
        await interaction.reply({
          content: "> `❌` × Podaj kwotę jako liczbę, np. `20` lub `20.5`.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      if (!Number.isFinite(kwotaNum) || kwotaNum < 0) kwotaNum = 0;

      if (kwotaNum < 5) {
        await interaction.reply({
          content: "> `❌` × Minimalna kwota zakupu to **5zł**.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      if (kwotaNum > MAX_PURCHASE_PLN) {
        await interaction.reply({
          content: "> `❌` × Maksymalna kwota zakupu to **10 000zł**.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      if (!selectedServer) {
        await interaction.reply({
          content: "> `❌` × Wybierz serwer przed wysłaniem formularza.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      if (!selectedPayment) {
        await interaction.reply({
          content: "> `❌` × Wybierz formę płatności przed wysłaniem formularza.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      // Pytanie "Co chcesz kupić?" zostało usunięte na prośbę użytkownika

      if (kwotaNum <= 20) {
        categoryId = categories["zakup-0-20"];
        ticketType = "zakup-0-20";
      } else if (kwotaNum <= 50) {
        categoryId = categories["zakup-20-50"];
        ticketType = "zakup-20-50";
      } else if (kwotaNum <= 100) {
        categoryId = categories["zakup-50-100"];
        ticketType = "zakup-50-100";
      } else if (kwotaNum <= 200) {
        categoryId = categories["zakup-100-200"];
        ticketType = "zakup-100-200";
      } else if (kwotaNum <= 400) {
        categoryId = categories["zakup-200-400"];
        ticketType = "zakup-200-400";
      } else {
        categoryId = categories["zakup-400-999"];
        ticketType = "zakup-400-999";
      }

      const serverLabel = getShopServerLabel(selectedServer);
      const paymentLabel = getShopPaymentLabel(selectedPayment);

      ticketTypeLabel = "ZAKUP";
      ticketTopic = `Zakup itemów na serwerze: ${serverLabel} (${kwotaNum}zł)`;
      if (ticketTopic.length > 1024) ticketTopic = ticketTopic.slice(0, 1024);
      preferredChannelName = buildPurchaseTicketChannelName(
        interaction.member,
        user,
        selectedPayment,
        selectedServer,
      );

      paymentMethod = selectedPayment;
      formInfo =
        `> <a:arrowwhite:1491476759290449984> × **Serwer:** \`${serverLabel}\`\n` +
        `> <a:arrowwhite:1491476759290449984> × **Kwota:** \`${kwotaNum}zł\`\n` +
        `> <a:arrowwhite:1491476759290449984> × **Forma płatności:** \`${paymentLabel}\``;

      const currentTicketData = {
        claimedBy: null,
        userId: user.id,
        locked: false,
        ticketTypeLabel,
        formInfo,
        paymentMethod: selectedPayment, // Przechowujemy forme platnosci
        openedAt: Date.now(),
      };
      // To bedzie uzyte pozniej przy zapisywaniu do ticketOwners w ticket creation logic
      // Ale musimy sie upewnic, ze to trafi do ticketOwners.
      // Widze ze ticketOwners.set jest na koncu dlugiego bloku.
      // Musze znalezc gdzie modal_zakup zapisuje do ticketOwners.
      break;
    }
    case "modal_mody_zakup": {
      const modName = (getModalTextInputValueSafe(interaction, "mod_name") || "").trim();
      const paymentMethodRaw =
        getModalStringSelectValueSafe(interaction, "mod_payment_method") ||
        getModalTextInputValueSafe(interaction, "payment_method") ||
        "";
      const modsCountRaw =
        getModalStringSelectValueSafe(interaction, "mods_count_select") ||
        getModalTextInputValueSafe(interaction, "mods_count") ||
        "";

      if (!modName) {
        await interaction.reply({
          content: "> `❌` × Podaj nazwę moda, którego chcesz kupić.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      if (!paymentMethodRaw) {
        await interaction.reply({
          content: "> `❌` × Wybierz formę płatności.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      if (!/^\d+$/.test(modsCountRaw)) {
        await interaction.reply({
          content: "> `❌` × Liczba modów musi być liczbą od **1** do **4**.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      const modsCount = parseInt(modsCountRaw, 10);
      if (modsCount < 1 || modsCount > 4) {
        await interaction.reply({
          content: "> `❌` × Możesz kupić jednorazowo od **1** do **4** modów.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      const totalPrice = modsCount * 20;
      categoryId =
        interaction.guild.channels.cache.has(PRIVATE_SPECIAL_PURCHASE_CATEGORY_ID)
          ? PRIVATE_SPECIAL_PURCHASE_CATEGORY_ID
          : categories["zakup-20-50"];
      ticketType = "zakup-mody";

      ticketTypeLabel = "ZAKUP MODÓW";
      forceOwnerOnlyVisibility = true;
      preferredChannelName = buildSpecialPurchaseTicketChannelName(
        interaction.member,
        user,
        modsCount > 1 ? "mody" : "mod",
        modName,
        paymentMethodRaw,
      );
      ticketTopic = `Zakup moda: ${modName} (${modsCount} szt.)`;
      if (ticketTopic.length > 1024) ticketTopic = ticketTopic.slice(0, 1024);
      const paymentMethodLabel = getAutorynekPaymentLabel(paymentMethodRaw);

      paymentMethod = paymentMethodRaw;
      formInfo = `> <a:arrowwhite:1491476759290449984> × **Mod:** \`${modName}\`\n` +
        `> <a:arrowwhite:1491476759290449984> × **Forma płatności:** \`${paymentMethodLabel}\`\n` +
        `> <a:arrowwhite:1491476759290449984> × **Ilość modów:** \`${modsCount}\`\n` +
        `> <a:arrowwhite:1491476759290449984> × **Łączna kwota:** \`${totalPrice}zł\``;
      break;
    }
    case "modal_autorynek_zakup": {
      const paymentMethodRaw =
        getModalStringSelectValueSafe(interaction, "autorynek_payment_method") ||
        getModalTextInputValueSafe(interaction, "payment_method") ||
        "";
      if (!paymentMethodRaw) {
        await interaction.reply({
          content: "> `❌` × Wybierz formę płatności.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      categoryId =
        interaction.guild.channels.cache.has(PRIVATE_SPECIAL_PURCHASE_CATEGORY_ID)
          ? PRIVATE_SPECIAL_PURCHASE_CATEGORY_ID
          : categories["zakup-20-50"];
      ticketType = "zakup-autorynku";
      ticketTypeLabel = "ZAKUP BOTÓW";
      forceOwnerOnlyVisibility = true;
      preferredChannelName = buildSpecialPurchaseTicketChannelName(
        interaction.member,
        user,
        "boty",
      );
      
      let priceLabel = "30zł";
      if (paymentMethodRaw === "waluta_serwerowa_300k") {
        priceLabel = "300k$";
      } else if (paymentMethodRaw === "zaproszenia") {
        priceLabel = "Zaproszenia";
      }

      ticketTopic = `Zakup botów (${priceLabel})`;
      if (ticketTopic.length > 1024) ticketTopic = ticketTopic.slice(0, 1024);
      const paymentMethodLabel = getAutorynekPaymentLabel(paymentMethodRaw);

      paymentMethod = paymentMethodRaw;
      formInfo =
        `> <a:arrowwhite:1491476759290449984> × **Cena:** \`${priceLabel}\`\n` +
        `> <a:arrowwhite:1491476759290449984> × **Forma płatności:** \`${paymentMethodLabel}\``;
      break;
    }
    case "modal_sprzedaz": {
      const co = getModalTextInputValueSafe(interaction, "co_sprzedac") || "";
      const serwerRaw =
        getModalStringSelectValueSafe(interaction, "sprzedaz_server") ||
        getModalTextInputValueSafe(interaction, "serwer") ||
        "";
      const payoutRaw =
        getModalStringSelectValueSafe(interaction, "sprzedaz_payout") ||
        getModalTextInputValueSafe(interaction, "payout_method") ||
        getModalTextInputValueSafe(interaction, "platnosc") ||
        "";
      const coTrimmed = co.trim();

      if (!serwerRaw) {
        await interaction.reply({
          content: "> `❌` × Wybierz serwer przed wysłaniem formularza.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      if (!payoutRaw) {
        await interaction.reply({
          content: "> `❌` × Wybierz formę wypłaty przed wysłaniem formularza.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      if (!coTrimmed) {
        await interaction.reply({
          content: "> `❌` × Opisz, co chcesz sprzedać.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      categoryId = categories["sprzedaz"];
      ticketType = "sprzedaz";
      ticketTypeLabel = "SPRZEDAŻ";
      const serwer = getShopServerLabel(serwerRaw);
      const payoutMethod = getShopPaymentLabel(payoutRaw);

      ticketTopic = `Sprzedaż na serwerze: ${serwer}`;
      if (ticketTopic.length > 1024) ticketTopic = ticketTopic.slice(0, 1024);

      {
        const serwerDef = getShopServerOptionDef(serwerRaw);
        const serwerSlug = serwerDef?.channelSlug || sanitizeTicketChannelNamePart(serwerRaw);
        const payoutDef = getShopPaymentOptionDef(payoutRaw);
        const payoutSlug = payoutDef?.channelSlug || sanitizeTicketChannelNamePart(payoutRaw);
        preferredChannelName = `${serwerSlug}-${payoutSlug}`.slice(0, 100);
      }

      formInfo =
        `> <a:arrowwhite:1491476759290449984> × **Co chce sprzedać:** \`${coTrimmed}\`\n` +
        `> <a:arrowwhite:1491476759290449984> × **Serwer:** \`${serwer}\`\n` +
        `> <a:arrowwhite:1491476759290449984> × **Forma wypłaty:** \`${payoutMethod}\``;
      break;
    }
    case "modal_odbior": {
      const enteredCodeRaw =
        getModalTextInputValueSafe(interaction, "reward_code") || "";
      const { code: enteredCode, codeData } =
        await getActiveCodeData(enteredCodeRaw);

      if (!enteredCode) {
        await interaction.reply({
          content: "> `❌` × Wpisz kod nagrody przed wysłaniem formularza.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      if (!codeData) {
        await interaction.reply({
          content: "> `❌` × Ten kod jest nieprawidłowy.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      // Sprawdź czy to kod na nagrodę
      if (
        codeData.type !== "invite_cash" &&
        codeData.type !== "invite_reward" &&
        codeData.type !== "free_kasa_reward"
      ) {
        await interaction.reply({
          content:
            "> `❌` × Ten kod nie jest kodem nagrody do odbioru w tej kategorii.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      if (codeData.used) {
        await interaction.reply({
          content: "> `❌` × Ten kod został już wykorzystany.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      if (Date.now() > (codeData.expiresAt || 0)) {
        activeCodes.delete(enteredCode);
        scheduleSavePersistentState();
        await interaction.reply({
          content: "> `❌` × Ten kod wygasł.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      // Sprawdź czy kod należy do użytkownika
      if (String(codeData.oderId) !== String(interaction.user.id)) {
        await interaction.reply({
          content:
            "> `❌` × Ten kod nie należy do Ciebie. Może go odebrać tylko osoba, która dostała go na PV.",
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }

      // Oznacz kod jako użyty
      codeData.used = true;
      activeCodes.delete(enteredCode);
      await db.deleteActiveCode(enteredCode);
      scheduleSavePersistentState();

      // Stwórz ticket typu ODBIÓR NAGRODY
      const ticketNumber = getNextTicketNumber(interaction.guildId);
      const categories = ticketCategories.get(interaction.guildId) || {};
      const user = interaction.user;

      const categoryId = REWARDS_CATEGORY_ID;
      const ticketTypeLabel = "NAGRODA";

      const expiryTs = codeData.expiresAt
        ? Math.floor(codeData.expiresAt / 1000)
        : null;
      const expiryLine = "";

      const formInfo = `> <a:arrowwhite:1491476759290449984> × **Kod:** \`${enteredCode}\`\n> <a:arrowwhite:1491476759290449984> × **Nagroda:** \`${codeData.rewardText || codeData.reward || INVITE_REWARD_TEXT || "70k$"}\``;

      try {
        let parentToUse = categoryId;
        if (!parentToUse || !interaction.guild.channels.cache.has(parentToUse)) {
          const foundCat = interaction.guild.channels.cache.find(
            (c) =>
              c.type === ChannelType.GuildCategory &&
              c.name &&
              c.name.toLowerCase().includes("odbior"),
          );
          if (foundCat) parentToUse = foundCat.id;
          else parentToUse = null;
        }

        let rawReward = String(codeData.rewardText || codeData.reward || "nagroda");
        rawReward = rawReward
          .replace(/na\s+anarchia\.gg/gi, "anarchia-lf")
          .replace(/anarchia\.gg/gi, "anarchia-lf")
          .replace(/anarchia-gg/gi, "anarchia-lf");
        const rewardSlug = sanitizeTicketChannelNamePart(rawReward);
        const createOptions = {
          name: `${rewardSlug}`.slice(0, 100),
          type: ChannelType.GuildText,
          permissionOverwrites: [
            {
              id: interaction.guild.id,
              deny: [PermissionsBitField.Flags.ViewChannel],
            },
            {
              id: user.id,
              allow: [
                PermissionsBitField.Flags.ViewChannel,
                PermissionsBitField.Flags.SendMessages,
                PermissionsBitField.Flags.ReadMessageHistory,
              ],
            },
            {
              id: "1519069239254974475",
              deny: [PermissionsBitField.Flags.ViewChannel],
            },
          ],
        };
        if (parentToUse) createOptions.parent = parentToUse;

        // Specjalna obsługa dla kategorii "inne" - dodaj uprawnienia dla właściciela
        if (parentToUse && parentToUse === categories["inne"]) {
          createOptions.permissionOverwrites.push(
            { id: interaction.guild.ownerId, allow: [PermissionsBitField.Flags.ViewChannel, PermissionsBitField.Flags.SendMessages, PermissionsBitField.Flags.ReadMessageHistory] } // właściciel serwera
          );
        }

        const channel = await ticketCategoryManager.create(interaction.guild, "odbior-nagrody", createOptions);

        const headerText = `\`\`\`text\n🛒 NEW SHOP × ${ticketTypeLabel}\n\`\`\``;
        const bodyText =
          `### ・ \`👤\` × **Informacje o kliencie:**\n` +
          `> <a:arrowwhite:1491476759290449984> × **Ping:** <@${user.id}>\n` +
          `> <a:arrowwhite:1491476759290449984> × **Nick:** ${formatInlineCodeText(getSafeTicketDisplayName(interaction.member, user))}\n` +
          `> <a:arrowwhite:1491476759290449984> × **ID:** \`${user.id}\`\n` +
          `### ・ \`📋\` × **Informacje z formularza:**\n` +
          `${formInfo}`;

        const closeButton = new ButtonBuilder()
          .setCustomId(`ticket_close_${channel.id}`)
          .setLabel("︲Zamknij")
          .setStyle(ButtonStyle.Secondary)
          .setEmoji({ id: "1537166621527908382", name: "NO", animated: true });
        const settingsButton = new ButtonBuilder()
          .setCustomId(`ticket_settings_${channel.id}`)
          .setLabel("⚙️︲Ustawienia")
          .setStyle(ButtonStyle.Secondary);
        const claimButton = new ButtonBuilder()
          .setCustomId(`ticket_claim_${channel.id}`)
          .setLabel("🔒︲Przejmij")
          .setStyle(ButtonStyle.Secondary);

        const buttonRow = new ActionRowBuilder().addComponents(
          closeButton,
          settingsButton,
          claimButton,
        );

        const payload = buildTicketContainerPayload({
          headerText,
          bodyText,
          buttonRow,
          guildId: interaction.guild?.id,
        });

        {
          const _pingRoles = getPingRolesForTicketType(ticketType);
          if (_pingRoles.length > 0) {
            await channel.send({
              content: _pingRoles.map(id => `<@&${id}>`).join(" "),
              allowedMentions: { roles: _pingRoles },
            }).catch(() => null);
          }
        }

        const sentMsg = await channel.send(payload);

        ticketOwners.set(channel.id, {
          claimedBy: null,
          userId: user.id,
          ticketMessageId: sentMsg.id,
          locked: false,
          ticketTypeLabel,
          formInfo,
          // Nie każdy modal ma pole tekstowe `payment_method` (część używa
          // select menu albo innego customId). Wartość została już bezpiecznie
          // odczytana w odpowiedniej gałęzi switcha.
          paymentMethod: paymentMethod || null,
          openedAt: Date.now(),
        });
        if (isRewardTicketLabel(ticketTypeLabel)) {
          rewardTicketClaims.set(channel.id, {
            guildId: interaction.guild.id,
            userId: user.id,
            createdAt: Date.now(),
          });
        }
        scheduleSavePersistentState();

        await logTicketCreation(interaction.guild, channel, {
          openerId: user.id,
          ticketTypeLabel,
          formInfo,
          ticketChannelId: channel.id,
          ticketMessageId: sentMsg.id,
        }).catch(() => { });

        await interaction.reply({
          content: `> \`✅\` × Ticket został stworzony: <#${channel.id}>`,
          flags: [MessageFlags.Ephemeral],
        });
      } catch (err) {
        console.error("Błąd tworzenia ticketu (odbior):", err);
        await interaction.reply({
          content: "> `❌` × **Wystąpił** błąd podczas tworzenia **ticketa**.",
          flags: [MessageFlags.Ephemeral],
        });
      }
      break;
    }
    case "modal_seller_limit":
    case "modal_inne": {
      const sprawa = getModalTextInputValueSafe(interaction, "sprawa") || "";
      let selectedPackage = null;
      if (cid === "modal_seller_limit") {
        const selected = getModalStringSelectValueSafe(interaction, "seller_limit");
        selectedPackage = SELLER_LIMIT_PACKAGES.find((entry) => entry.value === selected);
        if (!selectedPackage) {
          await interaction.reply({
            content: "> `❌` × Wybierz dostępny pakiet limitu sprzedawcy.",
            flags: [MessageFlags.Ephemeral],
          });
          return;
        }
      }

      categoryId = categories["inne"];
      ticketType = "inne";
      ticketTypeLabel = "PYTANIE";
      formInfo = `> <a:arrowwhite:1491476759290449984> × **Sprawa:** ${formatInlineCodeText(sprawa)}`;
      preferredChannelName = `inne-${sanitizeTicketChannelNamePart(
        interaction.member?.displayName || user?.globalName || user?.username || "nick"
      )}`.slice(0, 100);
      if (selectedPackage) {
        formInfo = `> <a:arrowwhite:1491476759290449984> × **Wybrany limit:** ${formatInlineCodeText(selectedPackage.label)}\n` +
          `> <a:arrowwhite:1491476759290449984> × **Koszt:** ${formatInlineCodeText(`${selectedPackage.price}zł`)}`;
      }
      break;
    }
    default:
      break;
  }

  if (creatingTicketUsers.has(user.id)) {
    if (!interaction.deferred && !interaction.replied) {
      await interaction.reply({
        content: "> `⏳` × **Trwa już tworzenie Twojego ticketu...** Proszę czekać.",
        flags: [MessageFlags.Ephemeral],
      }).catch(() => null);
    }
    return;
  }
  creatingTicketUsers.add(user.id);

  try {
    // ENFORCE: Ticket limit per user
    // Allow up to 2 tickets ONLY if the user is creating a purchase ticket ("zakup")
    // and their existing open ticket is a non-purchase ticket (nagroda, pomoc/inne, sprzedaz).
    const isNewTicketPurchase = typeof ticketType === "string" && ticketType.startsWith("zakup");
    const userOpenTickets = [];

    for (const [chanId, tData] of ticketOwners.entries()) {
      if (tData && tData.userId === user.id) {
        const existingChannel = await interaction.guild.channels
          .fetch(chanId)
          .catch(() => null);
        if (existingChannel) {
          userOpenTickets.push({ chanId, tData, channel: existingChannel });
        } else {
          ticketOwners.delete(chanId);
          scheduleSavePersistentState();
        }
      }
    }

    if (userOpenTickets.length >= 2) {
      const links = userOpenTickets.map((t) => `<#${t.chanId}>`).join(", ");
      await interaction.reply({
        content:
          `> \`❌\` × **Osiągnąłeś maksymalny limit** (2 tickety): ${links}\n` +
          "> `ℹ️` × Zamknij jeden z nich, zanim otworzysz nowy.",
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    if (userOpenTickets.length === 1) {
      const existing = userOpenTickets[0];
      const existingLabel = String(existing.tData?.ticketTypeLabel || "").toUpperCase();
      const existingChanName = String(existing.channel?.name || "").toLowerCase();

      const isExistingReward =
        existingLabel.includes("NAGROD") ||
        existingChanName.includes("nagrod") ||
        rewardTicketClaims.has(existing.chanId);

      const isExistingHelp =
        existingLabel.includes("PYTANIE") ||
        existingLabel.includes("POMOC") ||
        existingChanName.startsWith("inne-");

      const isExistingSell =
        existingLabel.includes("SPRZEDA") ||
        existingChanName.startsWith("sprzedaz-") ||
        existingChanName.includes("sprzeda");

      // Pozwalamy na stworzenie drugiego ticketu TYLKO wtedy, gdy nowy to zakup,
      // a istniejący to nagroda, pomoc/inne lub sprzedaż
      const canCreateSecondTicket =
        isNewTicketPurchase && (isExistingReward || isExistingHelp || isExistingSell);

      if (!canCreateSecondTicket) {
        await interaction.reply({
          content: `> \`❌\` × **Masz już otwarty** ticket: <#${existing.chanId}>`,
          flags: [MessageFlags.Ephemeral],
        });
        return;
      }
    }

    // Kategorię dobiera i odtwarza manager; uprawnienia liczymy z typu ticketu.
    const parentToUse = categoryId || ticketType;

    // create channel with or without parent
    const createOptions = {
      name: preferredChannelName || `ticket-${user.username}`,
      type: ChannelType.GuildText,
      permissionOverwrites: [
        {
          id: interaction.guild.id,
          deny: [PermissionsBitField.Flags.ViewChannel], // @everyone nie widzi ticketów
        },
        {
          id: interaction.user.id,
          allow: [
            PermissionsBitField.Flags.ViewChannel,
            PermissionsBitField.Flags.SendMessages,
            PermissionsBitField.Flags.ReadMessageHistory,
          ],
        },
      ],
    };

    const helperRoleId = "1519069239254974475";
    if (ticketType === "inne") {
      createOptions.permissionOverwrites.push({
        id: helperRoleId,
        allow: [
          PermissionsBitField.Flags.ViewChannel,
          PermissionsBitField.Flags.SendMessages,
          PermissionsBitField.Flags.ReadMessageHistory,
        ],
      });
    } else {
      createOptions.permissionOverwrites.push({
        id: helperRoleId,
        deny: [PermissionsBitField.Flags.ViewChannel],
      });
    }

    if (
      forceOwnerOnlyVisibility &&
      interaction.guild.ownerId &&
      interaction.guild.ownerId !== interaction.user.id
    ) {
      createOptions.permissionOverwrites.push({
        id: interaction.guild.ownerId,
        allow: [
          PermissionsBitField.Flags.ViewChannel,
          PermissionsBitField.Flags.SendMessages,
          PermissionsBitField.Flags.ReadMessageHistory,
        ],
      });
    }

    if (ticketType === "sprzedaz") {
      // Sprzedawcy nie mogą widzieć ticketów ze sprzedażą kasy (BASE_SELLER_ROLE_ID = "1350786945944391733")
      createOptions.permissionOverwrites.push({
        id: BASE_SELLER_ROLE_ID,
        deny: [PermissionsBitField.Flags.ViewChannel],
      });
    }

    // Dodaj rangi limitów w zależności od kategorii
    if (parentToUse && !forceOwnerOnlyVisibility) {
      const categoryId = parentToUse;
      const categories = ticketCategories.get(interaction.guildId) || {};

      const autoClaimCfg = autoPrzejmijSettings.get(interaction.guildId);
      const hasAutoClaim = autoClaimCfg && autoClaimCfg.enabled && autoClaimCfg.ownerId;
      const isPurchaseTicket = ticketType && (ticketType.startsWith("zakup-") || ticketType === "zakup" || ticketTypeLabel === "ZAKUP" || ticketTypeLabel === "ZAKUP AUTORYNKU");

      if (hasAutoClaim && isPurchaseTicket) {
        // Jeśli jest włączone autoprzejmowanie dla ticketów zakupowych, od razu nadaj uprawnienie
        // tylko osobie przejmującej i ukryj przed wszystkimi innymi rangami limitów.
        createOptions.permissionOverwrites.push(
          { id: "1449448705563557918", deny: [PermissionsBitField.Flags.ViewChannel] }, // limit 20
          { id: "1449448702925209651", deny: [PermissionsBitField.Flags.ViewChannel] }, // limit 50
          { id: "1449448686156255333", deny: [PermissionsBitField.Flags.ViewChannel] }, // limit 100
          { id: "1449448860517798061", deny: [PermissionsBitField.Flags.ViewChannel] }, // limit 200
          { id: "1541553589833695393", deny: [PermissionsBitField.Flags.ViewChannel] }, // limit 400
          { id: "1541553994000891984", deny: [PermissionsBitField.Flags.ViewChannel] }, // NO LIMIT
          {
            id: autoClaimCfg.ownerId,
            allow: [
              PermissionsBitField.Flags.ViewChannel,
              PermissionsBitField.Flags.SendMessages,
              PermissionsBitField.Flags.ReadMessageHistory,
            ]
          }
        );
      } else {
        if (categoryId === "1449527585271976131" || categoryId === categories["inne"]) {
          createOptions.permissionOverwrites.push(
            { id: interaction.guild.ownerId, allow: [PermissionsBitField.Flags.ViewChannel, PermissionsBitField.Flags.SendMessages, PermissionsBitField.Flags.ReadMessageHistory] }
          );
        } else {
          const limitOverwrites = getLimitRolePermissions(ticketType, null);
          createOptions.permissionOverwrites.push(...limitOverwrites);
        }
      }
    }
    // Usuwamy dodawanie opisu kanału (topic)
    if (parentToUse && interaction.guild.channels.cache.has(parentToUse)) {
      createOptions.parent = parentToUse;
    }

    await interaction.deferReply({ flags: [MessageFlags.Ephemeral] }).catch(() => null);

    const channel = await ticketCategoryManager.create(interaction.guild, ticketType, createOptions);

    const isPurchaseTicket = ticketType && (ticketType.startsWith("zakup-") || ticketType === "zakup" || ticketTypeLabel === "ZAKUP" || ticketTypeLabel === "ZAKUP AUTORYNKU");
    const autoClaimCfg = autoPrzejmijSettings.get(interaction.guildId);
    const hasAutoClaim = autoClaimCfg && autoClaimCfg.enabled && autoClaimCfg.ownerId;

    // Zawsze jawnie ustaw uprawnienia, aby Discord nie dziedziczył niechcianych ról z kategorii
    await channel.permissionOverwrites
      .set(createOptions.permissionOverwrites)
      .catch(() => null);

    const headerLabel = ticketType === "inne" ? "INNE" : ticketTypeLabel;
    const headerText = `\`\`\`text\n🛒 NEW SHOP × ${headerLabel}\n\`\`\``;
    const bodyText =
      `### ・ \`👤\` × **Informacje o kliencie:**\n` +
      `> <a:arrowwhite:1491476759290449984> × **Ping:** <@${user.id}>\n` +
      `> <a:arrowwhite:1491476759290449984> × **Nick:** ${formatInlineCodeText(getSafeTicketDisplayName(interaction.member, user))}\n` +
      `> <a:arrowwhite:1491476759290449984> × **ID:** \`${user.id}\`\n` +
      `### ・ \`📋\` × **Informacje z formularza:**\n` +
      `${formInfo}`;

    // Build buttons: Close (animated :NO: emoji), Settings, Claim toggle button
    const closeButton = new ButtonBuilder()
      .setCustomId(`ticket_close_${channel.id}`)
      .setLabel("︲Zamknij")
      .setStyle(ButtonStyle.Secondary)
      .setEmoji({ id: "1537166621527908382", name: "NO", animated: true });

    const settingsButton = new ButtonBuilder()
      .setCustomId(`ticket_settings_${channel.id}`)
      .setLabel("⚙️︲Ustawienia")
      .setStyle(ButtonStyle.Secondary);

    const claimButton = new ButtonBuilder()
      .setCustomId(`ticket_claim_${channel.id}`)
      .setLabel("🔒︲Przejmij")
      .setStyle(ButtonStyle.Secondary);

    const buttons = [closeButton, settingsButton, claimButton];

    const buttonRow = new ActionRowBuilder().addComponents(...buttons);

    const payload = buildTicketContainerPayload({
      headerText,
      bodyText,
      buttonRow,
      guildId: interaction.guild?.id,
    });

    {
      const sellerPurchaseNotifyUserId = cid === "modal_seller_limit" ? "1305200545979437129" : null;
      const _pingRoles = sellerPurchaseNotifyUserId ? [] : getPingRolesForTicketType(ticketType);
      if (sellerPurchaseNotifyUserId) {
        await channel.send({
          content: `<@${sellerPurchaseNotifyUserId}>`,
          allowedMentions: { parse: [], users: [sellerPurchaseNotifyUserId] },
        }).catch(() => null);
      } else if (_pingRoles.length > 0) {
        await channel.send({
          content: _pingRoles.map(id => `<@&${id}>`).join(" "),
          allowedMentions: { roles: _pingRoles },
        }).catch(() => null);
      }
    }

    const sentMsg = await channel.send(payload);

    ticketOwners.set(channel.id, {
      claimedBy: null,
      userId: user.id,
      ticketMessageId: sentMsg.id,
      locked: false,
      ticketTypeLabel,
      ownerOnlyPurchase: forceOwnerOnlyVisibility,
      formInfo,
      paymentMethod,
      openedAt: Date.now(),
    });
    scheduleSavePersistentState();

    // LOG: ticket creation in logi-ticket channel (if exists)
    try {
      await logTicketCreation(interaction.guild, channel, {
        openerId: user.id,
        ticketTypeLabel,
        formInfo,
        ticketChannelId: channel.id,
        ticketMessageId: sentMsg.id,
      }).catch((e) => console.error("logTicketCreation error:", e));
    } catch (e) {
      console.error("Błąd logowania utworzenia ticketu:", e);
    }

    if (interaction.deferred || interaction.replied) {
      await interaction.editReply({
        content: `> \`✅\` × Ticket został stworzony: <#${channel.id}>`,
      }).catch(() => null);
    } else {
      await interaction.reply({
        content: `> \`✅\` × Ticket został stworzony: <#${channel.id}>`,
        flags: [MessageFlags.Ephemeral],
      }).catch(() => null);
    }

    if (ticketTypeLabel === "ZAKUP" && !forceOwnerOnlyVisibility) {
      await maybeAutoPrzejmijNewTicket(interaction.guild, channel.id).catch((err) =>
        console.error("[autoprzejmij] Auto-claim po utworzeniu ticketa nieudany:", err),
      );
    }
  } catch (error) {
    if (error?.code === 10062 || error?.message?.includes("Unknown interaction")) {
      return;
    }
    console.error("Błąd tworzenia ticketu:", error);
    try {
      if (interaction.deferred || interaction.replied) {
        await interaction.editReply({
          content: "> `❌` × **Wystąpił** błąd podczas tworzenia **ticketu**.",
        }).catch(() => null);
      } else {
        await interaction.reply({
          content: "> `❌` × **Wystąpił** błąd podczas tworzenia **ticketu**.",
          flags: [MessageFlags.Ephemeral],
        }).catch(() => null);
      }
    } catch (_) {}
  } finally {
    creatingTicketUsers.delete(user.id);
  }
}

const dmAutoReplyCooldowns = new Map();
const DM_AUTO_REPLY_COOLDOWN_MS = 10 * 60 * 1000; // 10 minut
const mentionCooldowns = new Map();

// message create handler: enforce channel restrictions and keep existing legitcheck behavior
client.on(Events.MessageCreate, async (message) => {
  if (message.author.bot) return;

  // Obsługa wiadomości prywatnych (PV) z cooldownem 10 minut
  if (!message.guild) {
    const now = Date.now();
    const lastSent = dmAutoReplyCooldowns.get(message.author.id) || 0;
    if (now - lastSent < DM_AUTO_REPLY_COOLDOWN_MS) {
      return;
    }

    dmAutoReplyCooldowns.set(message.author.id, now);
    const payload = buildDmAutoReplyPayload();
    await message.reply(payload).catch(() => null);
    return;
  }

  // Obsługa oznaczenia (mention) bota na serwerze z cooldownem i stanami ostrzeżeń
  const botMentionRegex = new RegExp(`<@!?${client.user.id}>`);
  if (message.mentions.users.has(client.user.id) && botMentionRegex.test(message.content)) {
    // Ignoruj na kanale legit-rep
    if (message.channelId === "1449840030947217529") return;

    const now = Date.now();
    const userCooldown = mentionCooldowns.get(message.author.id) || { lastTime: 0, state: "idle" };

    if (userCooldown.state === "warned" && now - userCooldown.lastTime < 120000) {
      userCooldown.state = "cooldown";
      userCooldown.lastTime = now;
      mentionCooldowns.set(message.author.id, userCooldown);
      await message.reply({
        content: "Czy możesz przestać mnie oznaczać!?"
      }).catch(() => null);
    } else if (userCooldown.state === "cooldown" && now - userCooldown.lastTime < 120000) {
      // Ignoruj (cooldown aktywny)
    } else {
      // Stan idle lub cooldown minął
      userCooldown.state = "warned";
      userCooldown.lastTime = now;
      mentionCooldowns.set(message.author.id, userCooldown);
      await message.reply({
        content: "Cześć! Jeśli chcesz coś kupić, oznacz sprzedawcę zamiast mnie."
      }).catch(() => null);
    }
    return;
  }

  // --- NOWA LOGIKA: PING SPRZEDAWCY PO 5 MIN OD 1 WIADOMOŚCI KLIENTA ---
  const channelId = message.channel?.id;
  if (channelId) {
    const ticketData = ticketOwners.get(channelId);
    if (ticketData && ticketData.userId === message.author?.id && !ticketData.claimedBy && !ticketData.firstMessageReceived) {
      ticketData.firstMessageReceived = true;
      ticketOwners.set(channelId, ticketData);

      const type = ticketData.ticketTypeLabel;
      if (type === "ZAKUP" || type === "SPRZEDAŻ" || type === "ZAKUP AUTORYNKU" || type === "ZAKUP MODÓW") {
        const targetChannel = message.channel;
        setTimeout(async () => {
          if (!targetChannel) return;
          const currentTicketData = ticketOwners.get(channelId);
          if (currentTicketData && !currentTicketData.claimedBy) {
            try {
              await targetChannel.send("<@&1350786945944391733>").catch(() => null);
            } catch (err) {
              console.error("Błąd pingu po 5 min od pierwszej wiadomości:", err);
            }
          }
        }, 5 * 60 * 1000);
      }
    }
  }
  // ----------------------------------------------------------------------

  if (
    message.guild &&
    message.channel?.id === FREE_KASA_CHANNEL_ID &&
    !message.interactionMetadata
  ) {
    await message.delete().catch(() => null);
    await refreshFreeKasaInstruction(message.channel).catch(() => null);
    return;
  }

  if (
    message.guild &&
    message.channel?.id === SPRAWDZ_ZAPROSZENIA_CHANNEL_ID &&
    message.author.id !== client.user.id
  ) {
    await message.delete().catch(() => null);
    await ensureInvitePanel(message.channel).catch(() => null);
  }

  if (message.guild) {
    const pendingKey = getPendingEmbedTestPublishKey(
      message.guild.id,
      message.author.id,
    );
    const pending = pendingEmbedTestPublish.get(pendingKey);

    if (pending) {
      if (pending.expiresAt <= Date.now()) {
        pendingEmbedTestPublish.delete(pendingKey);
      } else if (pending.sourceChannelId !== message.channelId) {
        // czekamy tylko na wiadomość w tym samym kanale, w którym kliknięto Zakończ
      } else {
        const state = embedTestStates.get(pending.messageId);
        if (!state || state.ownerId !== message.author.id) {
          pendingEmbedTestPublish.delete(pendingKey);
        } else {
          const targetChannel = resolveEmbedTestPublishTargetFromMessage(message);

          if (!targetChannel) {
            const warn = await message.reply({
              content:
                "> `❌` × Nie znalazłem tego kanału. Wyślij `#kanał` albo ID kanału.",
            }).catch(() => null);

            if (warn) {
              setTimeout(() => warn.delete().catch(() => null), 7_000);
            }
            return;
          }

          try {
            const sentMessage = await sendEmbedTestToTargetChannel(
              state,
              targetChannel,
            );

            if (!sentMessage) {
              const warn = await message.reply({
                content:
                  "> `❌` × Nie mogę wysłać tam wiadomości. Wybierz inny kanał.",
              }).catch(() => null);
              if (warn) {
                setTimeout(() => warn.delete().catch(() => null), 7_000);
              }
              return;
            }

            await message.delete().catch(() => null);

            const confirm = await message.channel.send({
              content:
                `> \`✅\` × Wysłałem gotową wersję do <#${targetChannel.id}>\n` +
                `> \`🔗\` × ${getDiscordMessageUrl(
                  message.guild.id,
                  targetChannel.id,
                  sentMessage.id,
                )}`,
            }).catch(() => null);

            if (confirm) {
              setTimeout(() => confirm.delete().catch(() => null), 10_000);
            }
            return;
          } catch (error) {
            console.error("embedtest publish by message failed:", error);
            const warn = await message.reply({
              content:
                "> `❌` × Nie udało się opublikować embeda. Sprawdź uprawnienia bota.",
            }).catch(() => null);
            if (warn) {
              setTimeout(() => warn.delete().catch(() => null), 8_000);
            }
            return;
          }
        }
      }
    }
  }

  // ANTI-DISCORD-INVITE: delete invite links and timeout user for 30 minutes
  try {
    const content = message.content || "";
    const isGuildOwner = Boolean(message.guild && message.author.id === message.guild.ownerId);
    const inviteRegex =
      /(https?:\/\/)?(www\.)?(discord\.gg|discord(?:app)?\.com\/invite)\/[^\s/]+/i;
    if (!isGuildOwner && inviteRegex.test(content)) {
      // delete message first
      try {
        await message.delete().catch(() => null);
      } catch (e) {
        // ignore
      }
      // attempt to timeout the member for 30 minutes (1800 seconds = 30 minutes)
      try {
        const member = message.member;
        if (member && typeof member.timeout === "function") {
          const ms = 30 * 60 * 1000;
          await member
            .timeout(ms, "Wysłanie linku Discord invite/discord.gg")
            .catch(() => null);
        } else if (member && member.manageable) {
          // fallback: try to add a muted role named 'Muted' (best-effort)
          const guild = message.guild;
          let mutedRole = guild.roles.cache.find(
            (r) => r.name.toLowerCase() === "muted",
          );
          if (!mutedRole) {
            try {
              mutedRole = await guild.roles
                .create({ name: "Muted", permissions: [] })
                .catch(() => null);
            } catch (e) {
              mutedRole = null;
            }
          }
          if (mutedRole) {
            await member.roles.add(mutedRole).catch(() => null);
            // schedule removal in 30 minutes
            setTimeout(
              () => {
                guild.members
                  .fetch(member.id)
                  .then((m) => {
                    m.roles.remove(mutedRole).catch(() => null);
                  })
                  .catch(() => null);
              },
              30 * 60 * 1000,
            );
          }
        }
      } catch (err) {
        console.error("Nie udało się dać muta/timeout po wysłaniu linka:", err);
      }

      // notify channel briefly
      try {
        const warn = await message.channel.send({
          content: `<@${message.author.id}>`,
          embeds: [
            new EmbedBuilder()
              .setColor(COLOR_RED)
              .setDescription(
                "• `❗` __**Wysyłanie linków Discord jest zabronione otrzymujesz mute na 30 minut**__",
              ),
          ],
        });
        setTimeout(() => warn.delete().catch(() => null), 6_000);
      } catch (e) {
        // ignore
      }
      return;
    }
  } catch (e) {
    console.error("Błąd podczas sprawdzania linków zaproszeń:", e);
  }

  // ANTI-MASS-PING: delete message and timeout user for 1 hour if 5+ pings in one message
  try {
    const content = message.content || "";
    const isGuildOwner = Boolean(message.guild && message.author.id === message.guild.ownerId);
    // Catch all types of mentions: @user, @!user, @here, @everyone, and role mentions
    const mentionRegex = /<@!?(\d+)>|@here|@everyone|<@&(\d+)>/g;
    const mentions = content.match(mentionRegex) || [];

    if (!isGuildOwner && mentions.length >= 5) {
      // delete message first
      try {
        await message.delete();
      } catch (e) {
        // ignore
      }

      // attempt to timeout the member for 1 hour (3600 seconds)
      try {
        const member = message.member;
        const guild = message.guild;

        if (member && typeof member.timeout === "function") {
          const ms = 60 * 60 * 1000; // 1 hour
          await member.timeout(ms, "Masowy ping - 5+ oznaczeń w jednej wiadomości");
        } else {
          // fallback: try to add a muted role named 'Muted' (best-effort)
          let mutedRole = guild.roles.cache.find(
            (r) => r.name.toLowerCase() === "muted",
          );
          if (!mutedRole) {
            try {
              mutedRole = await guild.roles.create({
                name: "Muted",
                permissions: [],
                reason: "Rola dla masowego pingowania"
              });
            } catch (e) {
              mutedRole = null;
            }
          }

          if (mutedRole) {
            await member.roles.add(mutedRole, "Masowy ping - 5+ oznaczeń");

            // schedule removal in 1 hour
            setTimeout(async () => {
              try {
                const guildMember = await guild.members.fetch(member.id).catch(() => null);
                if (guildMember) {
                  await guildMember.roles.remove(mutedRole, "Automatyczne usunięcie mute po 1h");
                }
              } catch (e) {
                // ignore
              }
            }, 60 * 60 * 1000);
          }
        }
      } catch (err) {
        console.error("Nie udało się dać muta/timeout po masowym pingu:", err);
      }

      // notify channel briefly
      try {
        const warn = await message.channel.send({
          content: `<@${message.author.id}>`,
          embeds: [
            new EmbedBuilder()
              .setColor(COLOR_RED)
              .setDescription(
                "• `❗`  **__Masowy ping jest niedozwolony otrzymujesz mute na 1 godzine__**",
              ),
          ],
        });
        setTimeout(() => warn.delete().catch(() => null), 6_000);
      } catch (e) {
        // ignore
      }
      return;
    }
  } catch (e) {
    console.error("Błąd podczas sprawdzania masowych pingów:", e);
  }

  // Invalid-channel embeds (customized)
  const opinInvalidEmbed = new EmbedBuilder()
    .setColor(COLOR_RED)
    .setDescription(
      `• \`❗\` __**Na tym kanale można wystawiać tylko opinie!**__`,
    );

  try {
    const guildId = message.guildId;
    if (guildId) {
      const opinieChannelId = opinieChannels.get(guildId);
      if (opinieChannelId && message.channel.id === opinieChannelId) {
        if (!message.author.bot) {
          await message.delete().catch(() => null);
          return;
        }
      }

      const zapCh = message.guild
        ? message.guild.channels.cache.find(
          (c) =>
            c.type === ChannelType.GuildText &&
            (c.name === "❓-×┃sprawdz-zapro" ||
              c.name.includes("sprawdz-zapro") ||
              c.name.includes("sprawdz-zaproszenia")),
        )
        : null;

      if (zapCh && message.channel.id === zapCh.id) {
        if (!message.author.bot) {
          await message.delete().catch(() => null);
          return;
        }
      }
    }
  } catch (e) {
    console.error("Błąd przy egzekwowaniu reguł kanałów opinia/zaproszenia:", e);
  }

  // Enforce zaproszenia-check-only channel rule:
  try {
    const content = (message.content || "").trim();
    const zapCh = message.guild
      ? message.guild.channels.cache.find(
        (c) =>
          c.type === ChannelType.GuildText &&
          (c.name === "❓-×┃sprawdz-zapro" ||
            c.name.includes("sprawdz-zapro") ||
            c.name.includes("sprawdz-zaproszenia")),
      )
      : null;

    if (zapCh && message.channel.id === zapCh.id) {
      // allow only if typed command starts with /sprawdz-zaproszenia
      if (!content.toLowerCase().startsWith("/sprawdz-zaproszenia")) {
        try {
          await message.delete().catch(() => null);
        } catch (e) { }
        return;
      } else {
        // typed the command - allow (but delete to reduce clutter)
        try {
          await message.delete().catch(() => null);
        } catch (e) { }
        return;
      }
    }
  } catch (e) {
    console.error("Błąd przy egzekwowaniu reguły kanału zaproszenia:", e);
  }

  // If any message is sent in the specific legitcheck-rep channel
  if (
    message.channel &&
    message.channel.id === REP_CHANNEL_ID &&
    !message.author.bot
  ) {
    console.log(`[+rep] Otrzymano wiadomość na kanale legit-check: ${message.content} od ${message.author.tag}`);
    try {
      // ignore empty messages or slash-like content
      if (!message.content || message.content.trim().length === 0) return;
      if (message.content.trim().startsWith("/")) return;

      const channel = message.channel;
      const messageContent = message.content.trim();
      const now = Date.now();
      const COOLDOWN_MS = 15 * 60 * 1000; // 15 minut

      // Cooldown dla autora (15 min po poprawnym +rep)
      const lastRepTs = legitRepCooldown.get(message.author.id);
      if (lastRepTs && now - lastRepTs < COOLDOWN_MS) {
        const remaining = COOLDOWN_MS - (now - lastRepTs);
        await message.delete().catch(() => null);
        const cooldownEmbed = new EmbedBuilder()
          .setColor(COLOR_BLUE)
          .setDescription(
            "```\n" +
            "✅ New Shop × LEGIT CHECK\n" +
            "```\n" +
            `<a:arrowwhite:1491476759290449984> **__Stop!__**\n` +
            `<a:arrowwhite:1491476759290449984> Możesz wystawić następnego **legit checka** za \`${humanizeMs(remaining)}\`!`
          )
          .setTimestamp();
        message.author.send({ embeds: [cooldownEmbed] }).catch(() => null);
        return;
      }

      // Format jest zamknięty: po nazwie jednego z obsługiwanych serwerów
      // nie może znaleźć się żaden komentarz ani dodatkowy tekst.
      const parsedRep = parseLegitRepContent(messageContent);
      const hasMention = Boolean(parsedRep?.seller);
      const isValidRep = Boolean(parsedRep);

      console.log(`[+rep] Otrzymano wiadomość: "${messageContent}" | hasMention=${hasMention} | valid=${isValidRep}`);

      if (!hasMention) {
        try {
          await message.delete();
          const warningEmbed = new EmbedBuilder()
            .setColor(COLOR_RED)
            .setDescription(`• \`❗\` × __**Stosuj się do wzoru legit checka!**__`);
          const warnMsg = await channel.send({ content: `<@${message.author.id}>`, embeds: [warningEmbed] });
          setTimeout(
            () => warnMsg.delete().catch(() => null),
            LEGIT_REP_WARNING_DELETE_DELAY_MS,
          );
        } catch (err) {
          console.error("Błąd usuwania nieoznaczonego legit-check:", err);
        }
        return;
      }

      if (!isValidRep) {
        try {
          await message.delete();
          const warningEmbed = new EmbedBuilder()
            .setColor(COLOR_RED)
            .setDescription(
              `• \`❗\` × __**Stosuj się do wzoru legit checka!**__`,
            );

          const warnMsg = await channel.send({ content: `<@${message.author.id}>`, embeds: [warningEmbed] });
          setTimeout(
            () => warnMsg.delete().catch(() => null),
            LEGIT_REP_WARNING_DELETE_DELAY_MS,
          );
        } catch (err) {
          console.error("Błąd usuwania nieprawidłowego legit-check:", err);
        }
        return;
      }

      // Sprawdź czy użytkownik ma ticket oczekujący na +rep
      console.log(`[+rep] pendingTicketClose size: ${pendingTicketClose.size}`);
      for (const [chId, d] of pendingTicketClose.entries()) {
        console.log(`[+rep] ticket ${chId}: userId=${d.userId}, awaitingRep=${d.awaitingRep}, msgAuthor=${message.author.id}`);
      }
      const pendingTicketEntry = Array.from(pendingTicketClose.entries()).find(
        ([channelId, data]) => {
          return (
            String(data.userId) === String(message.author.id) &&
            data.awaitingRep === true &&
            channel.id === data.legitRepChannelId &&
            legitRepMatchesPendingTicket(parsedRep, message, data)
          );
        }
      );
      const hasPendingTicket = Boolean(pendingTicketEntry);

      if (!hasPendingTicket) {
        await message.delete().catch(() => null);
        try {
          const embed = new EmbedBuilder()
            .setColor(COLOR_BLUE)
            .setDescription(
              "> \`❌\` × Brak ticketów **oczekujących** na legit check."
            );
          await message.author.send({ embeds: [embed] });
        } catch {}
        return;
      }

      // Valid +rep message - licznik odpowiada faktycznym wiadomościom na kanale.
      if (legitRepHistoryInitialized) {
        legitRepMessageIds.add(message.id);
        legitRepCount = legitRepMessageIds.size;
      } else {
        legitRepCount++;
      }
      const pendingTicketData = pendingTicketEntry?.[1];
      const creditedSellerId = String(pendingTicketData?.commandUserId || "");
      if (creditedSellerId) {
        const currentSellerCount = sellerTicketCounts.get(creditedSellerId) || 0;
        sellerTicketCounts.set(creditedSellerId, currentSellerCount + 1);
        scheduleSavePersistentState();
      }
      const dailyTransactionAmount = isDailyLegitMoneyTransaction(pendingTicketData?.typ)
        ? parsePLN(pendingTicketData.co)
        : 0;
      recordDailyLegitRep("wiadomość", dailyTransactionAmount);
      legitRepCooldown.set(message.author.id, now);
      console.log(`+rep otrzymany! Licznik: ${legitRepCount}`);

      // Sprawdź czy istnieje ticket oczekujący na +rep od tego użytkownika
      try {
        const senderId = message.author.id; // ID osoby która wysłała +rep
        console.log(`[+rep] Sprawdzam tickety oczekujące na +rep od użytkownika ${senderId}`);

        // Przeszukaj wszystkie tickety oczekujące na +rep
        for (const [ticketChannelId, ticketData] of pendingTicketClose.entries()) {
          console.log(`[+rep] Sprawdzam ticket ${ticketChannelId}: awaitingRep=${ticketData.awaitingRep}, userId=${ticketData.userId}`);
          if (
            ticketData.awaitingRep &&
            String(ticketData.userId) === String(senderId) &&
            channel.id === ticketData.legitRepChannelId &&
            legitRepMatchesPendingTicket(parsedRep, message, ticketData)
          ) {
            // Sprawdź czy w wiadomości +rep jest wzmianka o sprzedawcy/używającym komendę
            const expectedUsername = ticketData.commandUsername;
            const expectedId = ticketData.commandUserId;
            const msgContent = message.content.trim();

            const mentionMatchesSeller = message.mentions.users.has(expectedId);
            const usernameIncluded =
              parsedRep.seller.toLocaleLowerCase("pl-PL") ===
              `@${expectedUsername}`.toLocaleLowerCase("pl-PL");

            if (mentionMatchesSeller || usernameIncluded) {
              console.log(`Znaleziono ticket ${ticketChannelId} - twórca ticketu ${senderId} wysłał +rep dla ${expectedUsername}`);
              
              if ((String(ticketData.typ || "").toLowerCase() === "zakup" || parsedRep?.verb === "ZAKUP") && String(expectedId) !== "1305200545979437129") {
                try {
                  const rozliczeniaChannel = await client.channels.fetch("1449162620807675935").catch(() => null);
                  if (rozliczeniaChannel && rozliczeniaChannel.isTextBased()) {
                    const pingMsg = await rozliczeniaChannel.send({
                      content: `<@${expectedId}>`,
                      allowedMentions: { users: [expectedId] },
                    }).catch((err) => console.error("[rozliczenia-ping] Błąd wysyłania powiadomienia:", err));

                    if (pingMsg && pingMsg.deletable) {
                      setTimeout(() => {
                        pingMsg.delete().catch(() => null);
                      }, 5000);
                    }
                  }
                } catch (rozliczenieErr) {
                  console.error("[rozliczenia-ping] Błąd:", rozliczenieErr);
                }
              }

              // (Wydatki zostały już zaktualizowane automatycznie przy uruchomieniu komendy /ticket-zakoncz)

              const ticketChannel = await client.channels.fetch(ticketChannelId).catch(() => null);
              if (ticketChannel) {
                try {
                  // Podmień embed na sukces i usuń stary wzór + przycisk
                  await replaceLegitCheckEmbedWithSuccess(ticketChannel, ticketData);

                  const ticketMeta = ticketOwners.get(ticketChannelId) || null;

                  setTimeout(async () => {
                    try {
                      await archiveTicketOnClose(
                        ticketChannel,
                        message.author.id,
                        ticketMeta,
                        {
                          closeMethod: "Automatyczne zamknięcie po +rep",
                        },
                      ).catch((e) => console.error("archiveTicketOnClose error (+rep):", e));
                      await ticketChannel.delete('Ticket zamknięty po otrzymaniu +rep').catch(() => null);
                      pendingTicketClose.delete(ticketChannelId);
                      await commitRewardTicketClaim(ticketChannelId).catch(() => null);
                      ticketOwners.delete(ticketChannelId);
                      console.log(`Ticket ${ticketChannelId} został zamknięty po +rep`);
                    } catch (closeErr) {
                      console.error(`Błąd zamykania ticketu ${ticketChannelId}:`, closeErr);
                    }
                  }, 10000);
                } catch (err) {
                  console.error("Błąd podczas przetwarzania zamknięcia ticketu (+rep):", err);
                }
              }
            }
          }
        }
      } catch (ticketErr) {
        console.error("Błąd sprawdzania ticketów oczekujących na +rep:", ticketErr);
      }

      // Use scheduled rename (respect cooldown)
      scheduleRepChannelRename(channel, legitRepCount).catch(() => null);
      scheduleSavePersistentState();

      // cooldown per user for info embed
      const last = infoCooldowns.get(message.author.id) || 0;
      if (Date.now() - last < INFO_EMBED_COOLDOWN_MS) {
        console.log(`Cooldown dla ${message.author.username}, pomijam embed`);
        return;
      }
      infoCooldowns.set(message.author.id, Date.now());
      console.log(`Wysyłam embed dla ${message.author.username}`);

      await sendLegitCheckInfoMessage(channel).catch((error) =>
        console.error("Błąd wysyłania panelu legit-check:", error),
      );
    } catch (err) {
      console.error("Błąd wysyłania info embed na legitcheck-rep:", err);
    }
  }

  if (message.content.toLowerCase().trim() === "legit") {
    // legacy: no legit flows for now
    return;
  }

  if (message.content === "!ping") {
    message.reply("Pong!");
  }
});

// ----------------- OPINIA handler (updated to match provided layout + delete & re-send instruction so it moves to bottom) -----------------

async function handleOpinionCommand(interaction) {
  const guildId = interaction.guildId;
  if (!guildId || !interaction.guild) {
    await interaction.reply({
      content: "> `❌` × **Ta komenda** działa tylko na **serwerze**!",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // Enforce per-user cooldown for /opinia (30 minutes)
  const lastUsed = opinionCooldowns.get(interaction.user.id) || 0;
  if (Date.now() - lastUsed < OPINION_COOLDOWN_MS) {
    const remaining = OPINION_COOLDOWN_MS - (Date.now() - lastUsed);
    await interaction.reply({
      content: `> \`❌\` × Możesz użyć komendy </opinia:1464015495392133321> ponownie za \`${humanizeMs(remaining)}\``,
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const normalize = (s = "") =>
    s
      .toString()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^a-z0-9 _-]/gi, "")
      .trim()
      .toLowerCase();

  let allowedChannelId = opinieChannels.get(guildId);
  if (!allowedChannelId) {
    const found = interaction.guild.channels.cache.find(
      (c) =>
        c.type === ChannelType.GuildText &&
        (c.name === "⭐-×┃opinie-klientow" ||
          normalize(c.name).includes("opinie") ||
          normalize(c.name).includes("opinie-klientow")),
    );
    if (found) {
      allowedChannelId = found.id;
      opinieChannels.set(guildId, found.id);
    }
  }

  if (!allowedChannelId || interaction.channelId !== allowedChannelId) {
    await interaction.reply({
      content: `> \`❌\` × Użyj tej **komendy** na kanale <#${allowedChannelId || "⭐-×┃opinie-klientow"}>.`,
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // Pobranie opcji
  const czas = interaction.options.getInteger("czas_oczekiwania");
  const jakosc = interaction.options.getInteger("jakosc_produktu");
  const cena = interaction.options.getInteger("cena_produktu");
  const tresc = interaction.options.getString("tresc_opinii");

  if (await shouldRejectUnverifiedOpinion(
    interaction.user.id,
    guildId,
    [czas, jakosc, cena],
    tresc,
  )) {
    await interaction.reply({
      content: "> `❌` × Twoja opinia nie została opublikowana, dokonaj pierwszego zakupu i spróbuj ponownie.",
      flags: [MessageFlags.Ephemeral],
    });
    console.log(`[opinia-filter] Odrzucono podejrzaną opinię użytkownika ${interaction.user.id} bez historii zakupów.`);
    return;
  }

  // mark cooldown (successful invocation)
  opinionCooldowns.set(interaction.user.id, Date.now());

  const starsInline = (n) => {
    return formatOpinionStars(n);
  };

  const safeTresc = formatOpinionText(tresc);

  // Budujemy opis jako pojedynczy string
  const description = [
    "```",
    "✅ New Shop × OPINIA",
    "```",
    `> \`👤\` **× Twórca opinii:** <@${interaction.user.id}>`,
    `> \`📝\` **× Treść:** ${safeTresc}`,
    "",
    `> \`⌛\` **× Czas oczekiwania:** ${starsInline(czas)}`,
    `> \`📋\` **× Jakość produktu:** ${starsInline(jakosc)}`,
    `> \`💸\` **× Cena produktu:** ${starsInline(cena)}`,
  ].join("\n");

  // Tworzymy embed z poprawnym description
  const opinionEmbed = new EmbedBuilder()
    .setColor(COLOR_BLUE)
    .setDescription(description)
    .setThumbnail(
      interaction.user.displayAvatarURL({ dynamic: true, size: 128 }),
    );
  try {
    const channel = interaction.channel;

    // Spróbuj użyć webhooka do wysłania opinii z nazwą równą displayName użytkownika
    // (wygląda jakby wysłał użytkownik — ale to nadal webhook)
    let botWebhook = null;
    try {
      const webhooks = await channel.fetchWebhooks();
      botWebhook = webhooks.find(
        (w) => w.owner?.id === client.user.id && w.name === "ZAKUP_ITy_OPINIE",
      );
    } catch (e) {
      botWebhook = null;
    }

    if (!botWebhook) {
      try {
        botWebhook = await channel.createWebhook({
          name: "ZAKUP_ITy_OPINIE",
          avatar: client.user.displayAvatarURL({ dynamic: true }),
          reason: "Webhook do publikowania opinii",
        });
      } catch (createErr) {
        botWebhook = null;
      }
    }

    if (botWebhook) {
      const displayName =
        interaction.member?.displayName || interaction.user.username;
      await botWebhook.send({
        username: displayName,
        avatarURL: interaction.user.displayAvatarURL({ dynamic: true }),
        embeds: [opinionEmbed],
        wait: true,
      });
    } else {
      await channel.send({ embeds: [opinionEmbed] });
    }

    // Delete previous instruction message (if exists) so the new one will be posted BELOW the just-sent opinion
    const channelId = channel.id;
    let instrMsg = null;

    if (lastOpinionInstruction.has(channelId)) {
      instrMsg = await channel.messages
        .fetch(lastOpinionInstruction.get(channelId))
        .catch(() => null);
      if (!instrMsg) lastOpinionInstruction.delete(channelId);
    }

    if (!instrMsg) {
      // try to find in recent messages one with the same description (old instruction leftover)
      const found = await findBotMessageWithEmbed(
        channel,
        (emb) =>
          typeof emb.description === "string" &&
          (emb.description.includes(
            "Kliknij w przycisk na dole, aby podzielić się opinią",
          ) ||
            emb.description.includes("Użyj **komendy** `/opinia`") || emb.description.includes("Użyj **komendy** </opinia")),
      );
      if (found) instrMsg = found;
    }

    if (instrMsg) {
      try {
        if (instrMsg.deletable) {
          await instrMsg.delete().catch(() => null);
        }
      } catch (e) {
        // ignore
      }
      lastOpinionInstruction.delete(channelId);
    }

    // Send a fresh instruction message (so it will be at the bottom)
    try {
      const sent = await channel.send(buildOpinionInstructionPayload());
      lastOpinionInstruction.set(channelId, sent.id);
    } catch (e) {
      // ignore (maybe no perms)
    }

    opinionAuthorUserIds.add(interaction.user.id);
    await interaction.reply({
      content: "> `✅` × **Twoja opinia** została opublikowana.",
      flags: [MessageFlags.Ephemeral],
    });
  } catch (err) {
    console.error("Błąd publikacji opinii:", err);
    try {
      await interaction.reply({
        content: "> `❌` × **Wystąpił** błąd podczas publikacji **opinii**.",
        flags: [MessageFlags.Ephemeral],
      });
    } catch (e) {
      // ignore
    }
  }
}
// ---------------------------------------------------

// Helper sleep
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/*
  NEW: /wyczysckanal handler
  - tryb: "wszystko" -> usuwa jak najwięcej wiadomości (pomija pinned)
  - tryb: "ilosc" -> usuwa określoną ilość (1-100)
  Notes:
  - Bulk delete nie usuwa wiadomości starszych niż 14 dni; w tym przypadku pojedyncze usuwanie jest używane jako fallback (może być wolne).
  - Command requires ManageMessages permission by default (set in command registration) but we double-check at runtime.
*/
async function handleWyczyscKanalCommand(interaction) {
  const guildId = interaction.guildId;
  const channel = interaction.channel;

  if (!guildId || !interaction.guild) {
    await interaction.reply({
      content: "> `❌` × **Ta komenda** działa tylko na **serwerze**!",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // Defer to avoid timeout and allow multiple replies
  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] }).catch(() => null);

  // only text channels
  if (
    !channel ||
    (channel.type !== ChannelType.GuildText &&
      channel.type !== ChannelType.GuildVoice &&
      channel.type !== ChannelType.GuildAnnouncement &&
      channel.type !== ChannelType.GuildForum &&
      channel.type !== ChannelType.GuildStageVoice &&
      channel.type !== ChannelType.GuildCategory)
  ) {
    // simpler: require GuildText
    if (channel.type !== ChannelType.GuildText) {
      try {
        await interaction.editReply({
          content:
            "❌ Ta komenda działa tylko na zwykłych kanałach tekstowych (nie w prywatnych wiadomościach).",
        });
      } catch (e) {
        // ignore
      }
      return;
    }
  }

  const mode = interaction.options.getString("tryb");
  const amount = interaction.options.getInteger("ilosc") || 0;

  try {
    if (mode === "ilosc") {
      // validate amount
      if (amount <= 0 || amount > 100) {
        try {
          await interaction.editReply({
            content: "> `❌` × **Podaj** poprawną ilość wiadomości do usunięcia (1-100).",
          });
        } catch (e) {
          // ignore
        }
        return;
      }

      // Use bulkDelete with filterOld = true to avoid error on >14days messages
      const deleted = await channel.bulkDelete(amount, true);
      const deletedCount = deleted.size || 0;

      try {
        await interaction.editReply({
          content: `✅ Usunięto ${deletedCount} wiadomości z tego kanału.`,
        });
      } catch (e) {
        // ignore
      }
      return;
    }

    if (mode === "wszystko") {
      try {
        await interaction.editReply({
          content:
            "🧹 Rozpoczynam czyszczenie kanału. To może potrwać (usuwam wszystkie nie-przypięte wiadomości)...",
        });
      } catch (e) {
        // ignore
      }

      let totalDeleted = 0;
      // loop fetching up to 100 messages and deleting them until none left (or stuck)
      while (true) {
        // fetch up to 100 messages
        const fetched = await channel.messages.fetch({ limit: 100 });
        if (!fetched || fetched.size === 0) break;

        // filter out pinned messages
        const toDelete = fetched.filter((m) => !m.pinned);

        if (toDelete.size === 0) {
          // nothing to delete in this batch (all pinned) -> stop
          break;
        }

        try {
          // bulkDelete with filterOld true to avoid errors on >14d
          const deleted = await channel.bulkDelete(toDelete, true);
          const count = deleted.size || 0;
          totalDeleted += count;

          // If some messages couldn't be bulk-deleted because older than 14 days,
          // bulkDelete will just skip them when filterOld = true, so handle leftovers manually.
          // Collect leftovers (those not deleted and not pinned) and delete individually.
          const remaining = toDelete.filter((m) => !deleted.has(m.id));
          if (remaining.size > 0) {
            for (const m of remaining.values()) {
              try {
                await m.delete().catch(() => null);
                totalDeleted++;
                // small delay to avoid rate limits
                await sleep(200);
              } catch (err) {
                // ignore single deletion errors
              }
            }
          }
        } catch (err) {
          // fallback: if bulkDelete fails for any reason, delete individually
          console.warn(
            "bulkDelete nie powiodło się, przechodzę do indywidualnego usuwania:",
            err,
          );
          for (const m of toDelete.values()) {
            try {
              await m.delete().catch(() => null);
              totalDeleted++;
              await sleep(200);
            } catch (e) {
              // ignore
            }
          }
        }

        // small pause to be polite with rate limits
        await sleep(500);

        // try next batch
      }

      await interaction.editReply({
        content: `✅ Czyszczenie zakończone. Usunięto około ${totalDeleted} wiadomości. (Pamiętaj: wiadomości przypięte zostały zachowane, a wiadomości starsze niż 14 dni mogły być usunięte indywidualnie lub pominięte).`,
      });
      return;
    }

    try {
      await interaction.editReply({
        content: "> `❌` × **Nieznany** tryb. Wybierz '**wszystko**' lub '**ilosc**'.",
      });
    } catch (e) {
      // ignore
    }
  } catch (error) {
    console.error("Błąd wyczyszczenia kanału:", error);
    try {
      await interaction.editReply({
        content: "> `❌` × **Wystąpił** błąd podczas czyszczenia **kanału**.",
      });
    } catch (e) {
      // ignore
    }
  }
}

/*
  NEW: schedule and perform rep channel rename while respecting cooldown
  - If immediate rename allowed (cooldown passed), perform now.
  - Otherwise schedule a single delayed rename to occur when cooldown ends.
  - pendingRename prevents multiple overlapping scheduled renames.
*/
async function scheduleRepChannelRename(channel, count) {
  if (!channel || typeof channel.setName !== "function") return;

  latestRepRenameCount = Math.max(0, Number(count) || 0);
  const newName = `✅×〢legit-checki➔${count}`;
  const now = Date.now();
  const since = now - lastChannelRename;
  const remaining = Math.max(0, CHANNEL_RENAME_COOLDOWN - since);

  if (remaining === 0 && !pendingRename) {
    // do it now
    pendingRename = true;
    try {
      await channel.setName(newName);
      lastChannelRename = Date.now();
      console.log(`Zmieniono nazwę kanału na: ${newName}`);
    } catch (err) {
      console.error("Błąd zmiany nazwy kanału (natychmiastowa próba):", err);
    } finally {
      pendingRename = false;
      if (latestRepRenameCount !== count) {
        scheduleRepChannelRename(channel, latestRepRenameCount).catch(() => null);
      }
    }
  } else {
    // schedule once (if not already scheduled)
    if (pendingRename) {
      // already scheduled — we won't schedule another to avoid piling many timeouts.
      console.log(
        `Zmiana nazwy kanału już zaplanowana. Nowa nazwa zostanie ustawiona przy najbliższej okazji: ${newName}`,
      );
      return;
    }

    pendingRename = true;
    const when = lastChannelRename + CHANNEL_RENAME_COOLDOWN;
    const delay = Math.max(0, when - now) + 1000; // add small safety buffer
    console.log(`Planuję zmianę nazwy kanału na ${newName} za ${delay} ms`);

    setTimeout(async () => {
      const scheduledCount = latestRepRenameCount;
      const scheduledName = `✅×〢legit-checki➔${scheduledCount}`;
      try {
        await channel.setName(scheduledName);
        lastChannelRename = Date.now();
        console.log(`Zaplanowana zmiana nazwy wykonana: ${scheduledName}`);
      } catch (err) {
        console.error("Błąd zmiany nazwy kanału (zaplanowana próba):", err);
      } finally {
        pendingRename = false;
        if (latestRepRenameCount !== scheduledCount) {
          scheduleRepChannelRename(channel, latestRepRenameCount).catch(() => null);
        }
      }
    }, delay);
  }
}

/*
  NEW: /autolc handler - natychmiast wystawia automatyczny legit check
*/
async function handleAutoLcTestCommand(interaction) {
  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });
  try {
    await postAutoLegitCheck();
    await interaction.editReply({
      content: `> \`✅\` × **Auto LC:** wystawiono automatyczny legit check na kanale <#${REP_CHANNEL_ID}>. Licznik: **${legitRepCount}**`,
    });
  } catch (err) {
    console.error("[autolc] Błąd:", err);
    await interaction.editReply({
      content: `> \`❌\` × Błąd: ${err.message}`,
    });
  }
}

/*
  NEW: /autolc-timer handler - start/stop/status timera auto legit checka
*/
async function handleAutoLcTimerCommand(interaction) {
  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });
  const action = interaction.options.getString("akcja");

  if (action === "start") {
    autoLcEnabled = true;
    scheduleRandomAutoLegitCheck();
    const saved = await saveStateToSupabase(buildPersistentStateData());
    if (!saved) {
      scheduleSavePersistentState(true);
      await interaction.editReply({ content: "> `⚠️` × Timer działa, ale zapis w bazie nie powiódł się. Ponowię zapis; ustawienie po restarcie nie jest jeszcze potwierdzone." });
      return;
    }
    const nextTime = autoLcNextFireAt > 0 ? new Date(autoLcNextFireAt) : null;
    const nextTimeStr = nextTime ? nextTime.toLocaleString("pl-PL", { timeZone: "Europe/Warsaw" }) : "wkrótce";
    await interaction.editReply({
      content: `> \`✅\` × **Timer auto LC:** włączony (zapisano w bazie – pozostanie włączony po restarcie bota).\n> **Następny legit check:** \`${nextTimeStr}\` *(losowo 3–5 LC dziennie w godz. 08:00–23:00, bez nocy 00:00–07:00)*`,
    });
    return;
  }

  if (action === "stop") {
    autoLcEnabled = false;
    if (autoLcTimer) {
      clearTimeout(autoLcTimer);
      autoLcTimer = null;
    }
    autoLcNextFireAt = 0;
    const saved = await saveStateToSupabase(buildPersistentStateData());
    if (!saved) {
      scheduleSavePersistentState(true);
      await interaction.editReply({ content: "> `⚠️` × Timer został zatrzymany, ale zapis w bazie nie powiódł się. Ponowię zapis; ustawienie po restarcie nie jest jeszcze potwierdzone." });
      return;
    }
    await interaction.editReply({
      content: "> `⏹️` × **Timer auto LC:** zatrzymany (zapisano w bazie – pozostanie zatrzymany po restarcie bota). Żaden automatyczny legit check nie zostanie wystawiony, dopóki nie włączysz go ponownie (`/autolc-timer akcja:start`).",
    });
    return;
  }

  // status
  const now = new Date();
  const nextTime = autoLcNextFireAt > 0 ? new Date(autoLcNextFireAt) : null;
  await interaction.editReply({
    content:
      "```\n" +
      "[Auto LC - timer]\n" +
      `Status: ${autoLcEnabled ? (autoLcTimer ? "AKTYWNY" : "WŁĄCZONY") : "ZATRZYMANY"}\n` +
      `Zapis w bazie: ${autoLcEnabled ? "WŁĄCZONY" : "WYŁĄCZONY"}\n` +
      `Następny: ${nextTime ? nextTime.toLocaleString("pl-PL", { timeZone: "Europe/Warsaw" }) : "BRAK"}\n` +
      "```",
  });
}

/*
  NEW: /autolc-status handler - pokazuje stan harmonogramu auto legit checka
*/
async function handleAutoLcStatusCommand(interaction) {
  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });
  const now = new Date();
  const nextTime = autoLcNextFireAt > 0 ? new Date(autoLcNextFireAt) : null;
  const blackout = isAutoLcInBlackout(now) ? "TAK (teraz przerwa nocna)" : "nie";
  const sampleAmounts = [];
  for (let i = 0; i < 6; i++) sampleAmounts.push(pickAutoLcAmount());
  await interaction.editReply({
    content:
      "```\n" +
      "[Auto LC - status]\n" +
      `Licznik legit checków: ${legitRepCount}\n` +
      `Timer włączony w bazie: ${autoLcEnabled ? "TAK" : "NIE"}\n` +
      `Timer aktywny w pamięci: ${autoLcTimer ? "TAK" : "NIE"}\n` +
      `W czarnej strefie (00:00-07:00): ${blackout}\n` +
      `Następny auto LC: ${nextTime ? nextTime.toLocaleString("pl-PL", { timeZone: "Europe/Warsaw" }) : "BRAK (timer nie uruchomiony)"}\n` +
      `Tryb: 3–5 legit checków dziennie (w godz. 08:00–23:00)\n` +
      `Serwery: 95% ANARCHIA LIFESTEAL, 5% MINESTAR SKYPVP\n` +
      `Kwoty (przykłady losowania): ${sampleAmounts.join(", ")}\n` +
      "```",
  });
}

async function handleResetLCCommand(interaction) {
  // ensure command used in guild
  if (!interaction.guild) {
    try {
      await interaction.reply({
        content: "> `❌` × **Ta komenda** działa tylko na **serwerze**!",
        flags: [MessageFlags.Ephemeral],
      });
    } catch (e) {
      console.error("Nie udało się odpowiedzieć (brak guild):", e);
    }
    return;
  }

  // Sprawdź czy właściciel
  if (interaction.user.id !== interaction.guild.ownerId) {
    try {
      await interaction.reply({
        content: "> `❗` × Brak wymaganych uprawnień.",
        flags: [MessageFlags.Ephemeral],
      });
    } catch (e) {
      console.error("Nie udało się odpowiedzieć o braku uprawnień:", e);
    }
    return;
  }

  // Defer reply to avoid "App is not responding" while we perform work
  try {
    await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });
  } catch (e) {
    console.warn("Nie udało się deferReply (może już odpowiedziano):", e);
  }

  console.log(
    `[resetlc] Użytkownik ${interaction.user.tag} (${interaction.user.id}) żąda resetu licznika.`,
  );

  // reset counter
  legitRepCount = 0;
  scheduleSavePersistentState();

  try {
    const channel = await client.channels
      .fetch(REP_CHANNEL_ID)
      .catch(() => null);
    if (!channel) {
      console.warn(
        `[resetlc] Nie znaleziono kanału o ID ${REP_CHANNEL_ID} lub bot nie ma do niego dostępu.`,
      );
      await interaction.editReply({
        content:
          "✅ Licznik został zresetowany lokalnie, ale nie udało się znaleźć kanału z licznikiem (sprawdź REP_CHANNEL_ID i uprawnienia bota).",
      });
      return;
    }

    // Try immediate rename if cooldown allows, otherwise schedule
    const now = Date.now();
    const since = now - lastChannelRename;
    const remaining = Math.max(0, CHANNEL_RENAME_COOLDOWN - since);

    if (remaining === 0 && !pendingRename) {
      try {
        // attempt immediate rename (may fail if missing ManageChannels)
        await channel.setName(`✅×〢legit-checki➔${legitRepCount}`);
        lastChannelRename = Date.now();
        pendingRename = false;
        console.log(`[resetlc] Kanał ${channel.id} zaktualizowany do 0.`);
        await interaction.editReply({
          content:
            "✅ Licznik legitchecków został zresetowany do 0, nazwa kanału została zaktualizowana.",
        });
        return;
      } catch (err) {
        console.error(
          "[resetlc] Błąd przy natychmiastowej zmianie nazwy kanału:",
          err,
        );
        // fallback to scheduling
        await scheduleRepChannelRename(channel, legitRepCount);
        await interaction.editReply({
          content:
            "✅ Licznik został zresetowany do 0. Nie udało się natychmiast zaktualizować nazwy kanału — zmiana została zaplanowana.",
        });
        return;
      }
    } else {
      // schedule rename respecting cooldown
      await scheduleRepChannelRename(channel, legitRepCount);
      await interaction.editReply({
        content:
          "✅ Licznik został zresetowany do 0. Nazwa kanału zostanie zaktualizowana za kilka minut (szanujemy cooldown Discorda).",
      });
      return;
    }
  } catch (err) {
    console.error("[resetlc] Błąd podczas resetowania licznika:", err);
    try {
      await interaction.editReply({
        content: "> `❌` × **Wystąpił** błąd podczas resetowania **licznika**.",
      });
    } catch (e) {
      console.error("Nie udało się wysłać editReply po błędzie:", e);
    }
  }
}

/*
  NEW: /zresetujczasoczekiwania handler
  - Admin-only command that clears cooldowns for /drop and /opinia (and internal info).
*/
async function handleZresetujCzasCommand(interaction) {
  if (!interaction.guild) {
    await interaction.reply({
      content: "> `❌` × **Ta komenda** działa tylko na **serwerze**!",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // Sprawdź czy właściciel
  if (interaction.user.id !== interaction.guild.ownerId) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  try {
    const what = interaction.options.getString("co");
    const targetUser = interaction.options.getUser("kto") || interaction.user;
    const targetId = targetUser.id;
    const targets = [];
    if (what === "opinia" || what === "all") {
      targets.push("/opinia");
      opinionCooldowns.delete(targetId);
    }
    if (what === "zaproszenia" || what === "all") {
      targets.push("/sprawdz-zaproszenia");
      sprawdzZaproszeniaCooldowns.delete(targetId);
    }
    if (what === "rep" || what === "all") {
      targets.push("+rep");
      legitRepCooldown.delete(targetId);
    }
    if (what === "free-kasa" || what === "all") {
      targets.push("Wylosuj nagrodę");
      freeKasaCooldowns.delete(targetId);
    }

    infoCooldowns.delete(targetId); // reset internal info cooldown for target

    await interaction.reply({
      content: `✅ Zresetowano czas oczekiwania (${targets.join(', ') || 'brak'}) dla <@${targetId}>.`,
      flags: [MessageFlags.Ephemeral],
    });
    console.log(`[zco] ${interaction.user.tag} zresetował cooldowny: ${targets.join(', ')} dla ${targetUser.tag}`);
  } catch (err) {
    console.error("[zco] Błąd:", err);
    await interaction.reply({
      content: "> `❌` × **Wystąpił** błąd podczas resetowania czasów **oczekiwania**.",
      flags: [MessageFlags.Ephemeral],
    });
  }
}

// ----------------- Welcome message system + Invite tracking & protections -----------------
client.on(Events.GuildMemberAdd, async (member) => {
  try {
    // --- Robust invite detection ---
    let inviterId = null;
    let countThisInvite = false;
    let isFakeAccount = false;
    let usedVanityCode = null;
    let selfInviteDetected = false;
    let invalidInviterDetected = false;

    try {
      // jeśli ten użytkownik wcześniej opuścił i mieliśmy to zapisane -> usuń "leave" (kompensacja)
      const memberKey = `${member.guild.id}:${member.id}`;
      if (leaveRecords.has(memberKey)) {
        try {
          const prevInviter = leaveRecords.get(memberKey);
          if (prevInviter) {
            if (!inviteLeaves.has(member.guild.id))
              inviteLeaves.set(member.guild.id, new Map());
            const lMap = inviteLeaves.get(member.guild.id);
            const prevLeft = lMap.get(prevInviter) || 0;
            lMap.set(prevInviter, Math.max(0, prevLeft - 1));
            inviteLeaves.set(member.guild.id, lMap);
            scheduleSavePersistentState();
          }
        } catch (e) {
          console.warn("Error compensating leave on rejoin:", e);
        } finally {
          leaveRecords.delete(memberKey);
          scheduleSavePersistentState();
        }
      }

      // fetch current invites with a few retries because Discord often updates uses with delay
      const prevMap = new Map(guildInvites.get(member.guild.id) || new Map());
      let latestInviteMap = null;

      for (let attempt = 0; attempt < 3 && !inviterId; attempt++) {
        const currentInvites = await member.guild.invites.fetch().catch(() => null);

        if (currentInvites) {
          const newMap = new Map();
          const increasedInvites = [];

          for (const inv of currentInvites.values()) {
            newMap.set(inv.code, inv.uses || 0);
          }

          latestInviteMap = newMap;

          for (const inv of currentInvites.values()) {
            const prevUses = prevMap.get(inv.code) || 0;
            const nowUses = inv.uses || 0;
            const diff = nowUses - prevUses;

            if (diff > 0) {
              increasedInvites.push({ invite: inv, diff });
            }
          }

          if (increasedInvites.length === 1) {
            inviterId = increasedInvites[0].invite.inviter
              ? increasedInvites[0].invite.inviter.id
              : null;
            countThisInvite = true;
          } else if (increasedInvites.length > 1) {
            increasedInvites.sort(
              (a, b) =>
                b.diff - a.diff ||
                (b.invite.uses || 0) - (a.invite.uses || 0),
            );
            inviterId = increasedInvites[0].invite.inviter
              ? increasedInvites[0].invite.inviter.id
              : null;
            countThisInvite = true;
            console.log(
              `[invites] Wykryto kilka rosnących invite'ów dla ${member.user.tag}; używam ${increasedInvites[0].invite.code}.`,
            );
          }
        } else if (attempt === 0) {
          console.warn(
            `[invites] Nie udało się pobrać invite'ów dla guild ${member.guild.id} — sprawdź uprawnienia bota (MANAGE_GUILD).`,
          );
        }

        if (!inviterId && attempt < 2) {
          await sleep(1250);
        }
      }

      if (latestInviteMap) {
        guildInvites.set(member.guild.id, latestInviteMap);
      }

      const previousVanityUses = guildVanityUses.has(member.guild.id)
        ? guildVanityUses.get(member.guild.id)
        : null;
      const currentVanityData = await fetchGuildVanityDataSafe(member.guild);
      const currentVanityUses =
        typeof currentVanityData?.uses === "number"
          ? currentVanityData.uses
          : null;
      const currentVanityCode =
        typeof currentVanityData?.code === "string" &&
          currentVanityData.code.trim()
          ? currentVanityData.code.trim()
          : null;

      if (
        !inviterId &&
        previousVanityUses !== null &&
        typeof currentVanityUses === "number" &&
        currentVanityUses > previousVanityUses
      ) {
        usedVanityCode = currentVanityCode || "newshop";
        console.log(
          `[invites] Wykryto wejście przez vanity URL ${usedVanityCode} dla guild ${member.guild.id}.`,
        );
      }

      if (typeof currentVanityUses === "number") {
        guildVanityUses.set(member.guild.id, currentVanityUses);
      }

      if (!inviterId && !usedVanityCode) {
        const deletedInviteFallback = consumeRecentDeletedInvite(member.guild.id);
        if (deletedInviteFallback?.inviterId) {
          inviterId = deletedInviteFallback.inviterId;
          countThisInvite = true;
          console.log(
            `[invites] Użyto fallbacku po usuniętym invicie ${deletedInviteFallback.code} dla ${member.user.tag}.`,
          );
        }
      }

      if (inviterId && inviterId === member.id) {
        console.log(
          `[invites] Pomijam self-invite dla ${member.user.tag} (${member.id}).`,
        );
        selfInviteDetected = true;
        inviterId = null;
        countThisInvite = false;
      }

      if (inviterId && !/^\d{17,20}$/.test(String(inviterId))) {
        console.log(
          `[invites] Pomijam nieprawidłowe ID zapraszającego (${inviterId}) dla ${member.user.tag}.`,
        );
        invalidInviterDetected = true;
        inviterId = null;
        countThisInvite = false;
      }
    } catch (e) {
      console.error("Błąd podczas wykrywania invite:", e);
    }

    // Simple fake-account detection (~2 months)
    try {
      const ACCOUNT_AGE_THRESHOLD_MS = 60 * 24 * 60 * 60 * 1000;
      const accountAgeMs =
        Date.now() - (member.user.createdTimestamp || Date.now());
      isFakeAccount = accountAgeMs < ACCOUNT_AGE_THRESHOLD_MS;

      // Debug: loguj wiek konta
      const accountAgeDays = Math.floor(accountAgeMs / (24 * 60 * 60 * 1000));
      console.log(`[invite] Konto ${member.user.tag} (${member.id}) ma ${accountAgeDays} dni. Fake: ${isFakeAccount}`);
    } catch (e) {
      isFakeAccount = false;
    }

    // Rate-limit per inviter to avoid abuse (only if we detected inviter)
    if (inviterId && countThisInvite) {
      if (!inviterRateLimit.has(member.guild.id))
        inviterRateLimit.set(member.guild.id, new Map());
      const rateMap = inviterRateLimit.get(member.guild.id);
      if (!rateMap.has(inviterId)) rateMap.set(inviterId, []);
      const timestamps = rateMap.get(inviterId);

      const cutoff = Date.now() - INVITER_RATE_LIMIT_WINDOW_MS;
      const recent = timestamps.filter((t) => t > cutoff);
      recent.push(Date.now());
      rateMap.set(inviterId, recent);
      inviterRateLimit.set(member.guild.id, rateMap);
      scheduleSavePersistentState();

      if (recent.length > INVITER_RATE_LIMIT_MAX) {
        // too many invites in the window -> mark as not counted
        countThisInvite = false;
        console.log(
          `[invites][ratelimit] Nie dodaję zaproszenia dla ${inviterId} - przekroczono limit w oknie.`,
        );
      }
    }

    // If we detected an inviter (even if not counted due to rate-limit, inviterId may be present)
    let fakeMap = null;
    const ownerId = member.guild.ownerId;
    const countOwnerInvites = isOwnerInviteCountingEnabled(member.guild.id);

    if (inviterId) {
      // Ensure all maps exist
      if (!inviteCounts.has(member.guild.id))
        inviteCounts.set(member.guild.id, new Map());
      if (!inviteRewards.has(member.guild.id))
        inviteRewards.set(member.guild.id, new Map());
      if (!inviteRewardsGiven.has(member.guild.id))
        inviteRewardsGiven.set(member.guild.id, new Map());
      if (!inviteLeaves.has(member.guild.id))
        inviteLeaves.set(member.guild.id, new Map());
      if (!inviteTotalJoined.has(member.guild.id))
        inviteTotalJoined.set(member.guild.id, new Map());
      if (!inviteFakeAccounts.has(member.guild.id))
        inviteFakeAccounts.set(member.guild.id, new Map());
      if (!inviteBonusInvites.has(member.guild.id))
        inviteBonusInvites.set(member.guild.id, new Map());

      const gMap = inviteCounts.get(member.guild.id); // prawdziwe zaproszenia
      const totalMap = inviteTotalJoined.get(member.guild.id); // wszystkie joiny
      fakeMap = inviteFakeAccounts.get(member.guild.id); // fake

      // Always increment totalJoined (wszystkie dołączenia przypisane do zapraszającego)
      const prevTotal = totalMap.get(inviterId) || 0;
      totalMap.set(inviterId, prevTotal + 1);
      inviteTotalJoined.set(member.guild.id, totalMap);
      scheduleSavePersistentState();

      // Liczymy zaproszenia tylko jeśli nie jest właścicielem, chyba że właściciel włączył tę opcję
      let previousValidInvites = gMap.get(inviterId) || 0;
      let currentValidInvites = previousValidInvites;
      if (countThisInvite && (inviterId !== ownerId || countOwnerInvites)) {
        if (!isFakeAccount) {
          const prev = gMap.get(inviterId) || 0;
          previousValidInvites = prev;
          currentValidInvites = prev + 1;
          gMap.set(inviterId, currentValidInvites);
          inviteCounts.set(member.guild.id, gMap);
          scheduleSavePersistentState(true); // Natychmiastowy zapis
        }
      }

      // --- Nagrody za zaproszenia ---
      await deliverPendingInviteRewardCodes(member.guild, inviterId).catch((error) =>
        console.error("[invites] Błąd wysyłania zaległych kodów za zaproszenia:", error),
      );
      const crossedInviteRewardThreshold = INVITE_REWARD_MILESTONES.some(
        (milestone) =>
          previousValidInvites < milestone.threshold &&
          currentValidInvites >= milestone.threshold,
      );
      if (
        crossedInviteRewardThreshold ||
        (countThisInvite && !isFakeAccount && currentValidInvites >= INVITE_REWARD_THRESHOLD)
      ) {
        queueInviteRewardDeliveryRetryBurst(member.guild.id, inviterId);
      }
    }

    // Jeśli konto jest fake (< 4 mies.), dodajemy tylko do licznika fake
    if (isFakeAccount && inviterId) {
      if (!inviteFakeAccounts.has(member.guild.id))
        inviteFakeAccounts.set(member.guild.id, new Map());
      const fakeMapLocal = fakeMap || inviteFakeAccounts.get(member.guild.id);
      const prevFake = fakeMapLocal.get(inviterId) || 0;
      fakeMapLocal.set(inviterId, prevFake + 1);
      inviteFakeAccounts.set(member.guild.id, fakeMapLocal);
      scheduleSavePersistentState();
    }

    // store who invited this member (and whether it was counted)
    const memberKey = `${member.guild.id}:${member.id}`;
    inviterOfMember.set(memberKey, {
      inviterId,
      counted: !!(
        inviterId &&
        countThisInvite &&
        !isFakeAccount &&
        (inviterId !== ownerId || countOwnerInvites)
      ),
      isFake: !!isFakeAccount,
      isVanity: !!usedVanityCode,
      vanityCode: usedVanityCode || null,
    });

    if (inviterId) {
      db.supabase
        .from("invites")
        .upsert(
          {
            guild_id: member.guild.id,
            inviter_id: inviterId,
            invited_user_id: member.id,
            status: "joined",
            counted: !!(
              inviterId &&
              countThisInvite &&
              !isFakeAccount &&
              (inviterId !== ownerId || countOwnerInvites)
            ),
            is_fake: !!isFakeAccount,
            updated_at: new Date().toISOString(),
          },
          { onConflict: "guild_id,invited_user_id" },
        )
        .then(({ error }) => {
          if (error) console.warn("[invites] Nie udało się zapisać szczegółu invite:", error.message || error);
        })
        .catch((error) =>
          console.warn("[invites] Błąd zapisu szczegółu invite:", error?.message || error),
        );
    }

    // persist join/invite state
    scheduleSavePersistentState(true); // Natychmiastowy zapis

    // Powiadomienie na kanale zaproszeń kto kogo dodał
    const zapChannelId = "1449159392388972554";
    const zapChannel = member.guild.channels.cache.get(zapChannelId);

    if (zapChannel) {
      const gMap = inviteCounts.get(member.guild.id) || new Map();
      const hasValidInviterId =
        typeof inviterId === "string" && /^\d{17,20}$/.test(inviterId);
      const currentInvites = hasValidInviterId ? gMap.get(inviterId) || 0 : 0;
      const inviteWord = getInviteWord(currentInvites);

      try {
        let message;
        if (usedVanityCode) {
          message = isFakeAccount
            ? `> \`✉️\` × <@${member.id}> dołączył używając linku **${usedVanityCode}**. (konto ma mniej niż 2 mies.)`
            : `> \`✉️\` × <@${member.id}> dołączył używając linku **${usedVanityCode}**.`;
        } else if (selfInviteDetected) {
          message = `> \`✉️\` × <@${member.id}> dołączył swoim własnym linkiem. Zaproszenie nie zostało zaliczone.`;
        } else if (invalidInviterDetected || !hasValidInviterId) {
          message = isFakeAccount
            ? `> \`✉️\` × <@${member.id}> dołączył używając linku **newshop**. (konto ma mniej niż 2 mies.)`
            : `> \`✉️\` × <@${member.id}> dołączył używając linku **newshop**.`;
        } else if (inviterId === ownerId && !countOwnerInvites) {
          // Zaproszenie przez właściciela - nie liczymy zaproszeń
          message = `> \`✉️\` × <@${inviterId}> zaprosił <@${member.id}> (został zaproszony przez właściciela)`;
        } else {
          // Normalne zaproszenie
          message = isFakeAccount
            ? `> \`✉️\` × <@${inviterId}> zaprosił <@${member.id}> i ma teraz **${currentInvites}** ${inviteWord}! (konto ma mniej niż 2 mies.)`
            : `> \`✉️\` × <@${inviterId}> zaprosił <@${member.id}> i ma teraz **${currentInvites}** ${inviteWord}!`;
        }

        if (!message) {
          message = isFakeAccount
            ? `> \`✉️\` × <@${member.id}> dołączył używając linku **newshop**. (konto ma mniej niż 2 mies.)`
            : `> \`✉️\` × <@${member.id}> dołączył używając linku **newshop**.`;
        }
        await zapChannel.send(message);
      } catch (e) { }
    }

    // Send welcome embed (no inviter details here)
    // Ensure channels are in cache before searching
    await member.guild.channels.fetch().catch(() => null);
    const ch =
      member.guild.channels.cache.find(
        (c) =>
          c.type === ChannelType.GuildText &&
          (c.name === "👋-×┃lobby" || c.name.toLowerCase().includes("lobby")),
      ) || null;

    console.log(`[lobby] Szukam kanału lobby. Znaleziono: ${ch ? ch.name : "brak"}. SystemChannel: ${member.guild.systemChannel?.name || "brak"}`);

    if (ch || member.guild.systemChannel) {
      const targetCh = ch || member.guild.systemChannel;

      const avatarUrl = member.displayAvatarURL({ extension: "png", forceStatic: false, size: 256 })
        || member.user.displayAvatarURL({ extension: "png", size: 256 });

      const welcomeEmbed = new EmbedBuilder()
        .setColor(COLOR_BLUE)
        .setDescription(
          "```\n👋 New Shop × LOBBY\n```\n" +
          `> \`😎\` **Witaj \`${member.user.username}\` na __NEW SHOP!__**\n` +
          `> \`🧑‍🤝‍🧑\` **Jesteś \`${member.guild.memberCount}\` osobą na naszym serwerze!**\n` +
          `> \`✨\` **Liczymy, że zostaniesz z nami na dłużej!**`
        )
        .setBrandFooter();

      if (avatarUrl) {
        welcomeEmbed.setThumbnail(avatarUrl);
      }

      await targetCh.send({
        content: `<@${member.id}>`,
        embeds: [welcomeEmbed],
      }).catch((err) => console.error("[lobby] Błąd wysyłania powitania:", err));
    } else {
      console.warn(`[lobby] Nie znaleziono kanału lobby ani systemChannel dla guild ${member.guild.id}`);
    }
  } catch (err) {
    console.error("Błąd wysyłania powitania / invite tracking:", err);
  }
});

// decrement inviter count on leave if we tracked who invited them
client.on(Events.GuildMemberRemove, async (member) => {
  try {
    const key = `${member.guild.id}:${member.id}`;
    const stored = inviterOfMember.get(key);
    if (!stored) return;

    // backward-compat: jeżeli stary format (string), zamieniamy na obiekt
    let inviterId, counted, wasFake, vanityCode;
    if (typeof stored === "string") {
      inviterId = stored;
      counted = true; // zakładamy, że wcześniej był liczony
      wasFake = false;
      vanityCode = null;
    } else {
      inviterId = stored.inviterId;
      counted = !!stored.counted;
      wasFake = !!stored.isFake;
      vanityCode =
        typeof stored.vanityCode === "string" && stored.vanityCode.trim()
          ? stored.vanityCode.trim()
          : null;
    }

    if (!inviterId && !vanityCode) {
      inviterOfMember.delete(key);
      return;
    }

    // decrement inviteCounts for inviter (if present AND if this invite was counted)
    if (!inviteCounts.has(member.guild.id))
      inviteCounts.set(member.guild.id, new Map());
    const gMap = inviteCounts.get(member.guild.id);
    const ownerId = member.guild.ownerId;
    const countOwnerInvites = isOwnerInviteCountingEnabled(member.guild.id);

    // Odejmujemy zaproszenia tylko jeśli nie jest właścicielem, chyba że opcja liczenia właścicielowi jest włączona
    if (counted && inviterId && (inviterId !== ownerId || countOwnerInvites)) {
      const prev = gMap.get(inviterId) || 0;
      const newCount = Math.max(0, prev - 1);
      gMap.set(inviterId, newCount);
      inviteCounts.set(member.guild.id, gMap);
      scheduleSavePersistentState(true); // Natychmiastowy zapis
    }

    if (inviterId) {
      // decrement totalJoined (since we incremented it on join unconditionally)
      if (!inviteTotalJoined.has(member.guild.id))
        inviteTotalJoined.set(member.guild.id, new Map());
      const totalMap = inviteTotalJoined.get(member.guild.id);
      const prevTotal = totalMap.get(inviterId) || 0;
      totalMap.set(inviterId, Math.max(0, prevTotal - 1));

      // If it was marked as fake on join, decrement fake counter
      if (wasFake) {
        if (!inviteFakeAccounts.has(member.guild.id))
          inviteFakeAccounts.set(member.guild.id, new Map());
        const fMap = inviteFakeAccounts.get(member.guild.id);
        const prevFake = fMap.get(inviterId) || 0;
        fMap.set(inviterId, Math.max(0, prevFake - 1));
      }

      // increment leaves count
      if (!inviteLeaves.has(member.guild.id))
        inviteLeaves.set(member.guild.id, new Map());
      const lMap = inviteLeaves.get(member.guild.id);
      const prevLeft = lMap.get(inviterId) || 0;
      lMap.set(inviterId, prevLeft + 1);
      inviteLeaves.set(member.guild.id, lMap);

      // Zapisz do leaveRecords na wypadek powrotu
      leaveRecords.set(key, inviterId);

      db.supabase
        .from("invites")
        .update({
          status: "left",
          updated_at: new Date().toISOString(),
        })
        .eq("guild_id", member.guild.id)
        .eq("invited_user_id", member.id)
        .then(({ error }) => {
          if (error) console.warn("[invites] Nie udało się oznaczyć wyjścia:", error.message || error);
        })
        .catch((error) =>
          console.warn("[invites] Błąd oznaczania wyjścia:", error?.message || error),
        );
    }

    // remove mapping
    inviterOfMember.delete(key);

    // persist invite + leave stan
    scheduleSavePersistentState();

    // notify zaproszenia channel
    const zapCh =
      member.guild.channels.cache.find(
        (c) =>
          c.type === ChannelType.GuildText &&
          (c.name === "📨-×┃zaproszenia" ||
            c.name.toLowerCase().includes("zaproszen") ||
            c.name.toLowerCase().includes("zaproszenia")),
      ) || null;

    if (zapCh) {
      // compute newCount for message (inviteCounts after possible decrement)
      const currentCount = gMap.get(inviterId) || 0;
      const inviteWord = getInviteWord(currentCount);

      try {
        let message;
        if (vanityCode) {
          message = `> \`✉️\` × <@${member.id}> opuścił serwer. Dołączył używając linku **${vanityCode}**.`;
        } else if (inviterId === ownerId && !countOwnerInvites) {
          // Opuszczenie przez zaproszenie właściciela - nie odejmowaliśmy zaproszeń
          message = `> \`✉️\` × <@${member.id}> opuścił serwer. (Był zaproszony przez właściciela)`;
        } else {
          // Normalne opuszczenie
          message = `> \`✉️\` × <@${member.id}> opuścił serwer. Był zaproszony przez <@${inviterId}> który ma teraz **${currentCount}** ${inviteWord}.`;
        }
        await zapCh.send(message);
      } catch (e) { }
    }

    if (vanityCode) {
      console.log(
        `Użytkownik ${member.id} opuścił serwer po wejściu przez vanity URL ${vanityCode}.`,
      );
    } else {
      console.log(
        `Odejmuję zaproszenie od ${inviterId} po leave (counted=${counted}, wasFake=${wasFake}).`,
      );
    }
  } catch (err) {
    console.error("Błąd przy obsłudze odejścia członka:", err);
  }
});

// ----------------- /sprawdz-zaproszenia command handler -----------------
async function handleSprawdzZaproszeniaCommand(interaction) {
  // Najpierw sprawdzamy warunki bez defer
  if (!interaction.guild) {
    await interaction.reply({
      content: "> `❌` × **Tylko** na **serwerze**.",
      flags: [MessageFlags.Ephemeral]
    });
    return;
  }

  if (interaction.channelId !== SPRAWDZ_ZAPROSZENIA_CHANNEL_ID) {
    await interaction.reply({
      content: `> \`❌\` × Użyj tej **komendy** na kanale <#${SPRAWDZ_ZAPROSZENIA_CHANNEL_ID}>.`,
      flags: [MessageFlags.Ephemeral]
    });
    return;
  }

  // cooldown 5s
  const SPRAWDZ_ZAPROSZENIA_COOLDOWN_MS = 5_000;
  const nowTs = Date.now();
  const lastTs = sprawdzZaproszeniaCooldowns.get(interaction.user.id) || 0;
  if (nowTs - lastTs < SPRAWDZ_ZAPROSZENIA_COOLDOWN_MS) {
    const remain = Math.ceil((SPRAWDZ_ZAPROSZENIA_COOLDOWN_MS - (nowTs - lastTs)) / 1000);
    await interaction.reply({
      content: `> \`❌\` × Możesz sprawdzić swoje zaproszenia ponownie za \`${remain}s\` `,
      flags: [MessageFlags.Ephemeral]
    });
    return;
  }
  sprawdzZaproszeniaCooldowns.set(interaction.user.id, nowTs);

  if (!interaction.deferred && !interaction.replied) {
    await interaction.deferReply({
      flags: [MessageFlags.Ephemeral],
    });
  }

  // ===== SPRAWDZ-ZAPROSZENIA – PEŁNY SCRIPT =====

  const guildId = interaction.guild.id;

  // Inicjalizacja map
  if (!inviteCounts.has(guildId)) inviteCounts.set(guildId, new Map());
  if (!inviteRewards.has(guildId)) inviteRewards.set(guildId, new Map());
  if (!inviteRewardsGiven.has(guildId)) inviteRewardsGiven.set(guildId, new Map());
  if (!inviteLeaves.has(guildId)) inviteLeaves.set(guildId, new Map());
  if (!inviteTotalJoined.has(guildId)) inviteTotalJoined.set(guildId, new Map());
  if (!inviteFakeAccounts.has(guildId)) inviteFakeAccounts.set(guildId, new Map());
  if (!inviteBonusInvites.has(guildId)) inviteBonusInvites.set(guildId, new Map());

  // Mapy gildii
  const gMap = inviteCounts.get(guildId);
  const totalMap = inviteTotalJoined.get(guildId);
  const fakeMap = inviteFakeAccounts.get(guildId);
  const lMap = inviteLeaves.get(guildId);
  const bonusMap = inviteBonusInvites.get(guildId);

  // Dane użytkownika
  const userId = interaction.user.id;
  const validInvites = inviteCounterValue(gMap.get(userId));
  const left = inviteCounterValue(lMap.get(userId));
  const fake = inviteCounterValue(fakeMap.get(userId));
  const bonus = inviteCounterValue(bonusMap.get(userId));

  const pendingInviteRewardDelivery = await deliverPendingInviteRewardCodes(
    interaction.guild,
    userId,
  ).catch((error) => {
    console.error("[invites] Błąd dosyłania kodu przy /sprawdz-zaproszenia:", error);
    return { deliveredCount: 0, deliveredLabels: [], blocked: false };
  });

  // Zaproszenia wyświetlane (z bonusem)
  const displayedInvites = validInvites + bonus;
  const inviteWord = getInviteWord(displayedInvites);
  const availableInviteRewards = getAvailableInviteRewardMilestones(guildId, userId);
  const nextInviteReward = getNextInviteRewardMilestone(guildId, userId);
  const hasPreviousInviteReward = getClaimedInviteRewardLevels(guildId, userId).size > 0
    || getIssuedInviteRewardLevels(guildId, userId).size > 0;
  const rewardStatusLine = availableInviteRewards.length
    ? `> \`🎁\` × **Masz do odbioru:** \`${availableInviteRewards.map((reward) => reward.label).join(", ")}\`\n`
    : nextInviteReward
      ? `> \`💸\` × **${hasPreviousInviteReward ? "Brakuje Ci do następnej nagrody:" : "Brakuje Ci do nagrody:"}** \`${Math.max(0, nextInviteReward.threshold - displayedInvites)}\`\n`
      : "> `❗` × **Odebrałeś już wszystkie nagrody za zaproszenia.**\n";

  const description =
      "```\n" +
      "📩 New Shop × ZAPROSZENIA\n" +
      "```\n" +
      `> \`👤\` × <@${userId}> **posiadasz:** \`${displayedInvites}\` **${inviteWord}**!\n` +
      `${rewardStatusLine}\n` +
      `> \`👥\` × **Prawdziwe osoby które dołączyły:** \`${validInvites}\`\n` +
      `> \`🚶\` × **Osoby które opuściły serwer:** \`${left}\`\n` +
      `> \`⚠️\` × **Niespełniające kryteriów (< konto 2 mies.):** \`${fake}\`\n` +
      `> \`🎁\` × **Dodatkowe zaproszenia:** \`${bonus}\``;

  try {
    const content =
      pendingInviteRewardDelivery.deliveredCount > 0
        ? `> \`✅\` × Informacje o twoich **zaproszeniach** zostały załadowane.\n> \`📩\` × Kod za nagrodę został wysłany na PV: \`${pendingInviteRewardDelivery.deliveredLabels.join(", ")}\`.`
        : pendingInviteRewardDelivery.blocked
          ? "> `❌` × Nie mogłem wysłać kodu na PV. Włącz wiadomości prywatne i użyj komendy ponownie."
          : null;

    const container = new ContainerBuilder().setAccentColor(COLOR_BLUE);
    container.addTextDisplayComponents(new TextDisplayBuilder().setContent(description));
    if (content) {
      container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
      container.addTextDisplayComponents(new TextDisplayBuilder().setContent(content));
    }
    appendBrandFooterToContainer(container, guildId);
    await interaction.editReply({
      content: null,
      embeds: [],
      components: [container],
      flags: MessageFlags.IsComponentsV2 | MessageFlags.Ephemeral,
      allowedMentions: { parse: [] },
    });
  } catch (err) {
    console.error("Błąd przy odpowiedzi sprawdz-zaproszenia:", err);
  }

  if (interaction.channel) {
    ensureInvitePanel(interaction.channel).catch(() => null);
  }
}

// ---------------------------------------------------
// Nowa komenda: /zaproszenia-edytuj
async function handleZaprosieniaStatsCommand(interaction) {
  const respond = (payload) => {
    if (!interaction.deferred && !interaction.replied) return interaction.reply(payload);
    const { flags, ...editPayload } = payload;
    return interaction.editReply(editPayload);
  };
  if (!interaction.guild) {
    await respond({
      content: "> `❌` × **Ta komenda** działa tylko na **serwerze**.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // Sprawdź czy właściciel
  if (interaction.user.id !== interaction.guild.ownerId) {
    await respond({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  if (!interaction.deferred && !interaction.replied) await interaction.deferReply({ flags: [MessageFlags.Ephemeral] });
  const guildId = interaction.guild.id;
  let subcommand = null;

  try {
    subcommand = interaction.options.getSubcommand(false);
  } catch {
    subcommand = null;
  }
  subcommand = subcommand || interaction.options.getString("akcja");

  if (subcommand === "nagroda-kwota") {
    const prog = interaction.options.getInteger("prog");
    const kwota = interaction.options.getInteger("kwota");
    if (![5, 10].includes(prog) || !Number.isSafeInteger(kwota) || kwota < 1) {
      await respond({ content: "> `❌` × Przy zmianie nagrody wybierz `prog` (5 lub 10) i podaj `kwota` w $.", flags: [MessageFlags.Ephemeral] });
      return;
    }

    const milestone = INVITE_REWARD_MILESTONES.find(m => m.threshold === prog);
    if (!milestone) {
      await respond({
        content: `> \`❌\` × Nie znaleziono progu zaproszeń: \`${prog}\`.`,
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }

    const oldAmount = milestone.amount;
    milestone.amount = kwota;
    milestone.label = `${Math.round(kwota / 1000)}k$`;

    syncInviteRewardThresholds();
    scheduleSavePersistentState(true);

    await respond({
      content: `> \`✅\` × Pomyślnie zmieniono kwotę nagrody za próg **${prog}** zaproszeń:\n` +
               `> \`💵\` × Stara kwota: **${formatRewardCashAmount(oldAmount)}**\n` +
               `> \`💰\` × Nowa kwota: **${formatRewardCashAmount(kwota)}** (wyświetlana jako \`${milestone.label}\`)`,
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  if (subcommand === "usunblokade" || subcommand === "odblokuj-nagrody") {
    const targetUser = interaction.options.getUser(subcommand === "usunblokade" ? "kto" : "osoba");
    if (!targetUser) {
      await respond({ content: "> `❌` × Wybierz `osoba`, której chcesz odblokować ponowny odbiór nagród.", flags: [MessageFlags.Ephemeral] });
      return;
    }

    if (!inviteRewardsGiven.has(guildId)) inviteRewardsGiven.set(guildId, new Map());
    if (!claimedInviteRewardMilestones.has(guildId)) {
      claimedInviteRewardMilestones.set(guildId, new Map());
    }
    if (!inviteRewardLevels.has(guildId)) {
      inviteRewardLevels.set(guildId, new Map());
    }

    inviteRewardsGiven.get(guildId).delete(targetUser.id);
    claimedInviteRewardMilestones.get(guildId).delete(targetUser.id);
    inviteRewardLevels.get(guildId).delete(targetUser.id);

    const codesToDelete = [];
    for (const [code, codeData] of activeCodes.entries()) {
      if (
        String(codeData?.oderId || "") === String(targetUser.id) &&
        (codeData?.type === "invite_cash" || codeData?.type === "invite_reward")
      ) {
        codesToDelete.push(code);
      }
    }

    for (const code of codesToDelete) {
      activeCodes.delete(code);
      await db.deleteActiveCode(code).catch(() => null);
    }

    scheduleSavePersistentState(true);

    await respond({
      content:
        `> \`✅\` × Usunąłem blokadę nagród za zaproszenia dla <@${targetUser.id}>.\n` +
        "> `🎁` × Ta osoba może ponownie odebrać nagrody za próg `5` i `10` zaproszeń.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const legacyEdit = subcommand === "edytuj";
  const user = interaction.options.getUser(legacyEdit ? "komu" : "osoba");
  if (!user) {
    await respond({ content: "> `❌` × Wskaż osobę, której statystyki chcesz zmienić.", flags: [MessageFlags.Ephemeral] });
    return;
  }
  if (subcommand === "pokaz") {
    const valid = inviteCounterValue(inviteCounts.get(guildId)?.get(user.id));
    const bonus = inviteCounterValue(inviteBonusInvites.get(guildId)?.get(user.id));
    await respond({ content:
      `> \`👤\` × Statystyki <@${user.id}>\n` +
      `> \`✅\` × Prawdziwe: \`${valid}\`\n` +
      `> \`🎁\` × Dodatkowe: \`${bonus}\`\n` +
      `> \`⭐\` × Suma do nagród: \`${valid + bonus}\` (prawdziwe + dodatkowe)\n` +
      `> \`🚶\` × Opuszczone: \`${inviteCounterValue(inviteLeaves.get(guildId)?.get(user.id))}\`\n` +
      `> \`⚠️\` × Konta młodsze niż 2 miesiące: \`${inviteCounterValue(inviteFakeAccounts.get(guildId)?.get(user.id))}\``,
      flags: [MessageFlags.Ephemeral] });
    return;
  }
  const categoryRaw = (interaction.options.getString(legacyEdit ? "kategoria" : "licznik") || "").toLowerCase();
  const action = legacyEdit ? (interaction.options.getString("akcja") || "").toLowerCase()
    : subcommand === "wyzeruj" ? "wyczysc" : subcommand;
  const number = action === "wyczysc" ? 0 : interaction.options.getInteger("liczba");
  if (!["dodaj", "odejmij", "ustaw", "wyczysc"].includes(action)
      || !Number.isSafeInteger(number) || number < (action === "dodaj" || action === "odejmij" ? 1 : 0)
      || number > 1000000) {
    await respond({ content: "> `❌` × Podaj poprawną liczbę. Dodaj/odejmij: 1–1000000; ustaw: 0–1000000.", flags: [MessageFlags.Ephemeral] });
    return;
  }

  // normalize category aliases
  let category = null;
  if (["prawdziwe", "prawdziwy", "prawdzi"].includes(categoryRaw))
    category = "prawdziwe";
  else if (
    ["opuszczone", "opuśćone", "opuszcone", "left", "lefts"].includes(
      categoryRaw,
    )
  )
    category = "opuszczone";
  else if (
    [
      "mniej4mies",
      "mniej2mies",
      "mniejniż4mies",
      "mniej_niz_4mies",
      "mniej",
      "mniej4",
    ].includes(categoryRaw)
  )
    category = "mniej4mies";
  else if (["dodatkowe", "dodatkowa", "bonus", "bonusy"].includes(categoryRaw))
    category = "dodatkowe";

  if (!category) {
    await respond({
      content: "> ❌ × **Nieznana** kategoria. Wybierz: `prawdziwe`, `opuszczone`, `mniej2mies`, `dodatkowe`.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // ensure maps exist
  if (!inviteCounts.has(guildId)) inviteCounts.set(guildId, new Map());
  if (!inviteLeaves.has(guildId)) inviteLeaves.set(guildId, new Map());
  if (!inviteFakeAccounts.has(guildId))
    inviteFakeAccounts.set(guildId, new Map());
  if (!inviteBonusInvites.has(guildId))
    inviteBonusInvites.set(guildId, new Map());
  if (!inviteRewards.has(guildId)) inviteRewards.set(guildId, new Map());
  if (!inviteRewardsGiven.has(guildId))
    inviteRewardsGiven.set(guildId, new Map());

  let targetMap;
  let prettyName;
  switch (category) {
    case "prawdziwe":
      targetMap = inviteCounts.get(guildId);
      prettyName = "Prawdziwe (policzone) zaproszenia";
      break;
    case "opuszczone":
      targetMap = inviteLeaves.get(guildId);
      prettyName = "Osoby, które opuściły serwer";
      break;
    case "mniej4mies":
      targetMap = inviteFakeAccounts.get(guildId);
      prettyName = "Niespełniające kryteriów (konto młodsze niż 2 miesiące)";
      break;
    case "dodatkowe":
      targetMap = inviteBonusInvites.get(guildId);
      prettyName = "Dodatkowe zaproszenia";
      break;
    default:
      targetMap = inviteCounts.get(guildId);
      prettyName = category;
  }

  const previousDisplayedInvites = getInviteDisplayCount(guildId, user.id);
  const prev = inviteCounterValue(targetMap.get(user.id));
  let newVal = prev;

  if (action === "dodaj") {
    newVal = prev + number;
  } else if (action === "odejmij") {
    newVal = Math.max(0, prev - number);
  } else if (action === "ustaw") {
    newVal = Math.max(0, number);
  } else if (action === "wyczysc" || action === "czysc" || action === "reset") {
    newVal = 0;
  } else {
    await respond({
      content:
        "❌ Nieznana akcja. Wybierz: `dodaj`, `odejmij`, `ustaw`, `wyczysc`.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // Nagrody za zaproszenia są teraz odbierane bez kodów,
  // dopiero przy wejściu w kategorię "Odbierz nagrodę".

  // finally set the (possibly adjusted) value
  targetMap.set(user.id, newVal);
  const statsSaved = await saveStateToSupabase(buildPersistentStateData());
  if (!statsSaved) scheduleSavePersistentState(true);

  const newDisplayedInvites = getInviteDisplayCount(guildId, user.id);
  const crossedInviteRewardThresholdByEdit = INVITE_REWARD_MILESTONES.some(
    (milestone) =>
      previousDisplayedInvites < milestone.threshold &&
      newDisplayedInvites >= milestone.threshold,
  );

  let pendingInviteRewardDelivery = {
    deliveredCount: 0,
    deliveredLabels: [],
    blocked: false,
  };

  if (
    ["prawdziwe", "dodatkowe"].includes(category) &&
    newDisplayedInvites > previousDisplayedInvites &&
    newDisplayedInvites >= INVITE_REWARD_THRESHOLD
  ) {
    pendingInviteRewardDelivery = await deliverPendingInviteRewardCodes(
      interaction.guild,
      user.id,
    ).catch((error) => {
      console.error("[invites] Błąd wysyłania kodu po /zaproszenia-edytuj edytuj:", error);
      return { deliveredCount: 0, deliveredLabels: [], blocked: false };
    });

    if (crossedInviteRewardThresholdByEdit) {
      queueInviteRewardDeliveryRetryBurst(guildId, user.id);
    }
  }

  await respond({
    content:
      `✅ Zaktualizowano **${prettyName}** dla <@${user.id}>: \`${prev}\` → \`${newVal}\`.\n` +
      `> \`⭐\` × Suma do nagród: \`${newDisplayedInvites}\` (prawdziwe + dodatkowe).` +
      (statsSaved ? "" : "\n> `⚠️` × Zmiana działa teraz, ale zapis w bazie się nie powiódł. Bot ponowi zapis; sprawdź licznik przed restartem.") +
      (
        pendingInviteRewardDelivery.deliveredCount > 0
          ? `\n> \`📩\` × Wysłałem na PV kod za nagrodę: \`${pendingInviteRewardDelivery.deliveredLabels.join(", ")}\`.`
          : pendingInviteRewardDelivery.blocked
            ? "\n> `❌` × Nie udało się wysłać kodu na PV. Niech użytkownik włączy wiadomości prywatne."
            : ""
      ),
    flags: [MessageFlags.Ephemeral],
  });
}

// ---------------------------------------------------
// Pomoc
async function handleHelpCommand(interaction) {
  try {
    const embed = new EmbedBuilder()
      .setColor(COLOR_BLUE)
      .setTitle("`📋` × Spis komend")
      .setDescription(
        [
          "**`Komendy ogólne:`**",
          "> `📩` × </sprawdz-zaproszenia:1464015495932940398> Sprawdź swoje zaproszenia",
          "> `⭐` × </opinia:1464015495392133321> Podziel się opinią o naszym sklepie",
          "> `📋` × </help:1464015495392133316> Pokaż tę wiadomość",
        ].join("\n"),
      );

    await interaction.reply({
      embeds: [embed],
      flags: [MessageFlags.Ephemeral],
    });
  } catch (err) {
    console.error("handleHelpCommand error:", err);
    try {
      await interaction.reply({
        content: "> `❌` × **Błąd** podczas wyświetlania **pomocy**.",
        flags: [MessageFlags.Ephemeral],
      });
    } catch (_error) { }
  }
}

// Parser czasu: 1h = 1 godzina, 1d = 1 dzień, 1m = 1 minuta, 1s = 1 sekunda
function parseTimeString(timeStr) {
  if (!timeStr || typeof timeStr !== "string") return null;
  const trimmed = timeStr.trim().toLowerCase();
  const match = trimmed.match(/^(\d+)([hdms])$/);
  if (!match) return null;
  const value = parseInt(match[1], 10);
  const unit = match[2];
  if (isNaN(value) || value <= 0) return null;

  switch (unit) {
    case "s":
      return value * 1000; // sekundy -> ms
    case "m":
      return value * 60 * 1000; // minuty -> ms
    case "h":
      return value * 60 * 60 * 1000; // godziny -> ms
    case "d":
      return value * 24 * 60 * 60 * 1000; // dni -> ms
    default:
      return null;
  }
}

// --- Pomocnicze: formatowanie pozostałego czasu ---
function formatTimeDelta(ms) {
  const timestamp = Math.floor((Date.now() + ms) / 1000);
  return `<t:${timestamp}:R>`;
}

// --- Pomocnicze: formatowanie czasu blokady ---
function formatBlockTime(remainingMs) {
  const totalSeconds = Math.floor(remainingMs / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if (hours > 0) {
    return `${hours} godzin ${minutes} minut ${seconds} sekund`;
  } else if (minutes > 0) {
    return `${minutes} minut ${seconds} sekund`;
  } else {
    return `${seconds} sekund`;
  }
}

// --- Pomocnicze: poprawna forma liczby osób ---
function getPersonForm(count) {
  if (count === 1) return "osoba";
  if (
    count % 10 >= 2 &&
    count % 10 <= 4 &&
    (count % 100 < 10 || count % 100 >= 20)
  ) {
    return "osoby";
  }
  return "osób";
}

// --- Pomocnicze: losowanie zwycięzców ---
function pickRandom(arr, n) {
  if (!arr || !arr.length) return [];
  const a = arr.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a.slice(0, n);
}

// ----------------- /dodajkonkurs handler (poprawiona wersja) -----------------
async function handleDodajKonkursCommand(interaction) {
  if (!interaction.guild) {
    await interaction.reply({
      content: "> `❌` × **Tylko** na **serwerze**.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }
  // Sprawdź czy właściciel
  if (interaction.user.id !== interaction.guild.ownerId) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // Modal: tylko nagroda (jako tytuł), czas, zwycięzcy i wymagane zaproszenia
  const modal = new ModalBuilder()
    .setCustomId("konkurs_create_modal")
    .setTitle("Utwórz konkurs");

  const prizeInput = new TextInputBuilder()
    .setCustomId("konkurs_nagroda")
    .setLabel("Nagroda (to będzie tytuł konkursu)")
    .setStyle(TextInputStyle.Short)
    .setRequired(true)
    .setMaxLength(200);

  const timeInput = new TextInputBuilder()
    .setCustomId("konkurs_czas")
    .setLabel("Czas trwania (np. 1h, 2d, 30m, 60s)")
    .setStyle(TextInputStyle.Short)
    .setRequired(true)
    .setPlaceholder("h = godzina, m = minuta, d = dzień, s = sekunda")
    .setMaxLength(10);

  const winnersInput = new TextInputBuilder()
    .setCustomId("konkurs_zwyciezcy")
    .setLabel("Liczba zwycięzców")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setPlaceholder("1")
    .setMaxLength(3);

  const invitesReqInput = new TextInputBuilder()
    .setCustomId("konkurs_wymagania_zaproszenia")
    .setLabel("Wymagane zaproszenia (opcjonalnie)")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setPlaceholder("2")
    .setMaxLength(5);

  modal.addComponents(
    new ActionRowBuilder().addComponents(prizeInput),
    new ActionRowBuilder().addComponents(timeInput),
    new ActionRowBuilder().addComponents(winnersInput),
    new ActionRowBuilder().addComponents(invitesReqInput),
  );

  await interaction.showModal(modal);
}

async function handleKonkursCreateModal(interaction) {
  const prize = interaction.fields.getTextInputValue("konkurs_nagroda");
  const timeStr = interaction.fields.getTextInputValue("konkurs_czas");
  const winnersStr =
    interaction.fields.getTextInputValue("konkurs_zwyciezcy") || "1";
  const invitesReqStr =
    interaction.fields.getTextInputValue("konkurs_wymagania_zaproszenia") || "";

  const timeMs = parseTimeString(timeStr);
  if (!timeMs) {
    await interaction.reply({
      content:
        "❌ Nieprawidłowy format czasu. Użyj np. `1h`, `2d`, `30m`, `60s`",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const winnersCount = Math.max(1, parseInt(winnersStr, 10) || 1);
  const invitesRequired = invitesReqStr.trim()
    ? Math.max(0, parseInt(invitesReqStr.trim(), 10) || 0)
    : 0;

  let targetChannel = interaction.channel;
  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] }).catch(() => { });

  const endsAt = Date.now() + timeMs;
  const ts = Math.floor(endsAt / 1000);

  // Początkowy opis z wymaganiami zaproszeń jeśli są
  let description =
    `🎁 **•** Nagroda: **${prize}**\n\n` +
    `🕐 **•** Koniec konkursu: ${formatTimeDelta(timeMs)}\n` +
    `👑 **•** Liczba zwycięzców: **${winnersCount}**\n` +
    `👥 **•** Liczba uczestników: **0**`;

  if (invitesRequired > 0) {
    const inviteForm = getPersonForm(invitesRequired);
    description += `\n\n⚠️ Wymagane: dodać ${invitesRequired} ${inviteForm} na serwer`;
  }

  // Początkowy embed - 🎉 New Shop × KONKURS w czarnym kwadracie
  const embed = new EmbedBuilder()
    .setDescription(
      "```\n" +
      "🎉 New Shop × KONKURS\n" +
      "```\n" +
      description
    )
    .setColor(COLOR_BLUE)
    .setTimestamp();

  // Placeholder button (will be replaced with proper customId after message is sent)
  const joinBtn = new ButtonBuilder()
    .setCustomId("konkurs_join_pending")
    .setLabel("Weź udział (0)")
    .setStyle(ButtonStyle.Secondary)
    .setDisabled(false);

  let sent = null;

  // Dodaj GIF przy tworzeniu konkursu
  try {
    const gifPath = path.join(
      __dirname,
      "attached_assets",
      "standard (4).gif",
    );
    const attachment = new AttachmentBuilder(gifPath, { name: "konkurs_start.gif" });
    embed.setImage("attachment://konkurs_start.gif");

    const row = new ActionRowBuilder().addComponents(joinBtn);
    sent = await targetChannel.send({
      embeds: [embed],
      components: [row],
      files: [attachment]
    });
  } catch (err) {
    console.warn("Nie udało się załadować GIFa przy tworzeniu konkursu:", err);
    // Fallback: wyślij bez GIFa
    const row = new ActionRowBuilder().addComponents(joinBtn);
    sent = await targetChannel.send({
      embeds: [embed],
      components: [row]
    });
  }

  if (!sent) {
    try {
      await interaction.editReply({
        content: "> `❌` × **Nie udało się** utworzyć konkursu (nie wysłano wiadomości w **kanał**).",
      });
    } catch (e) {
      // ignore
    }
    return;
  }

  contests.set(sent.id, {
    channelId: targetChannel.id,
    endsAt,
    winnersCount,
    title: prize,
    prize,
    messageId: sent.id,
    createdBy: interaction.user.id,
    invitesRequired,
  });

  contestParticipants.set(sent.id, new Map());
  scheduleSavePersistentState();

  // ustawiamy poprawny id na przycisku już po wysłaniu
  const properCustomId = `konkurs_join_${sent.id}`;
  const joinButtonCorrect = new ButtonBuilder()
    .setCustomId(properCustomId)
    .setLabel("Weź udział (0)")
    .setStyle(ButtonStyle.Secondary)
    .setDisabled(false);

  const newRow = new ActionRowBuilder().addComponents(joinButtonCorrect);
  await sent.edit({ components: [newRow] }).catch(() => null);

  setTimeout(() => {
    endContestByMessageId(sent.id).catch((e) => console.error(e));
  }, timeMs);

  try {
    await interaction.editReply({
      content: `\`✅\` Konkurs opublikowany w <#${targetChannel.id}> i potrwa ${formatTimeDelta(timeMs)} (do <t:${ts}:R>)`,
    });
  } catch (err) {
    console.error("Błąd tworzenia konkursu:", err);
    try {
      await interaction.editReply({
        content: "> `❌` × **Nie udało się** utworzyć **konkursu**.",
      });
    } catch (e) {
      console.error("Nie udało się wysłać editReply po błędzie:", e);
    }
  }
}

// ----------------- /dodajkonkurs handler (poprawiona wersja) -----------------
async function handleDodajKonkursCommand(interaction) {
  if (!interaction.guild) {
    await interaction.reply({
      content: "> `❌` × **Tylko** na **serwerze**.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }
  // Sprawdź czy właściciel
  if (interaction.user.id !== interaction.guild.ownerId) {
    await interaction.reply({
      content: "> `❗` × Brak wymaganych uprawnień.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // Modal: tylko nagroda (jako tytuł), czas, zwycięzcy i wymagane zaproszenia
  const modal = new ModalBuilder()
    .setCustomId("konkurs_create_modal")
    .setTitle("Utwórz konkurs");

  const prizeInput = new TextInputBuilder()
    .setCustomId("konkurs_nagroda")
    .setLabel("Nagroda (to będzie tytuł konkursu)")
    .setStyle(TextInputStyle.Short)
    .setRequired(true)
    .setMaxLength(200);

  const timeInput = new TextInputBuilder()
    .setCustomId("konkurs_czas")
    .setLabel("Czas trwania (np. 1h, 2d, 30m, 60s)")
    .setStyle(TextInputStyle.Short)
    .setRequired(true)
    .setPlaceholder("h = godzina, m = minuta, d = dzień, s = sekunda")
    .setMaxLength(10);

  const winnersInput = new TextInputBuilder()
    .setCustomId("konkurs_zwyciezcy")
    .setLabel("Liczba zwycięzców")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setPlaceholder("1")
    .setMaxLength(3);

  const invitesReqInput = new TextInputBuilder()
    .setCustomId("konkurs_wymagania_zaproszenia")
    .setLabel("Wymagane zaproszenia (opcjonalnie)")
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setPlaceholder("2")
    .setMaxLength(5);

  modal.addComponents(
    new ActionRowBuilder().addComponents(prizeInput),
    new ActionRowBuilder().addComponents(timeInput),
    new ActionRowBuilder().addComponents(winnersInput),
    new ActionRowBuilder().addComponents(invitesReqInput),
  );

  await interaction.showModal(modal);
}

async function handleKonkursCreateModal(interaction) {
  const prize = interaction.fields.getTextInputValue("konkurs_nagroda");
  const timeStr = interaction.fields.getTextInputValue("konkurs_czas");
  const winnersStr =
    interaction.fields.getTextInputValue("konkurs_zwyciezcy") || "1";
  const invitesReqStr =
    interaction.fields.getTextInputValue("konkurs_wymagania_zaproszenia") || "";

  const timeMs = parseTimeString(timeStr);
  if (!timeMs) {
    await interaction.reply({
      content:
        "❌ Nieprawidłowy format czasu. Użyj np. `1h`, `2d`, `30m`, `60s`",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const winnersCount = Math.max(1, parseInt(winnersStr, 10) || 1);
  const invitesRequired = invitesReqStr.trim()
    ? Math.max(0, parseInt(invitesReqStr.trim(), 10) || 0)
    : 0;

  let targetChannel = interaction.channel;
  await interaction.deferReply({ flags: [MessageFlags.Ephemeral] }).catch(() => { });

  const endsAt = Date.now() + timeMs;
  const ts = Math.floor(endsAt / 1000);

  // Początkowy opis z wymaganiami zaproszeń jeśli są
  let description =
    `🎁 **•** Nagroda: **${prize}**\n\n` +
    `🕐 **•** Koniec konkursu: ${formatTimeDelta(timeMs)}\n` +
    `👑 **•** Liczba zwycięzców: **${winnersCount}**\n` +
    `👥 **•** Liczba uczestników: **0**`;

  if (invitesRequired > 0) {
    const inviteForm = getPersonForm(invitesRequired);
    description += `\n\n \`❗\` **Wymagane: dodać ${invitesRequired} ${inviteForm} na serwer**`;
  }

  // Początkowy embed - 🎉 New Shop × KONKURS w czarnym kwadracie
  const embed = new EmbedBuilder()
    .setDescription(
      "```\n" +
      "🎉 New Shop × KONKURS\n" +
      "```\n" +
      description
    )
    .setColor(COLOR_BLUE)
    .setTimestamp();

  // Placeholder button (will be replaced with proper customId after message is sent)
  const joinBtn = new ButtonBuilder()
    .setCustomId("konkurs_join_pending")
    .setLabel("Weź udział (0)")
    .setStyle(ButtonStyle.Secondary)
    .setDisabled(false);

  let sent = null;

  // Dodaj GIF przy tworzeniu konkursu
  try {
    const gifPath = path.join(
      __dirname,
      "attached_assets",
      "standard (4).gif",
    );
    const attachment = new AttachmentBuilder(gifPath, { name: "konkurs_start.gif" });
    embed.setImage("attachment://konkurs_start.gif");

    const row = new ActionRowBuilder().addComponents(joinBtn);
    sent = await targetChannel.send({
      embeds: [embed],
      components: [row],
      files: [attachment]
    });
  } catch (err) {
    console.warn("Nie udało się załadować GIFa przy tworzeniu konkursu:", err);
    // Fallback: wyślij bez GIFa
    const row = new ActionRowBuilder().addComponents(joinBtn);
    sent = await targetChannel.send({
      embeds: [embed],
      components: [row]
    });
  }

  if (!sent) {
    try {
      await interaction.editReply({
        content: "> `❌` × **Nie udało się** utworzyć konkursu (nie wysłano wiadomości w **kanał**).",
      });
    } catch (e) {
      // ignore
    }
    return;
  }

  contests.set(sent.id, {
    channelId: targetChannel.id,
    endsAt,
    winnersCount,
    title: prize,
    prize,
    messageId: sent.id,
    createdBy: interaction.user.id,
    invitesRequired,
  });

  contestParticipants.set(sent.id, new Map());
  scheduleSavePersistentState();

  // ustawiamy poprawny id na przycisku już po wysłaniu
  const properCustomId = `konkurs_join_${sent.id}`;
  const joinButtonCorrect = new ButtonBuilder()
    .setCustomId(properCustomId)
    .setLabel("Weź udział (0)")
    .setStyle(ButtonStyle.Secondary)
    .setDisabled(false);

  const newRow = new ActionRowBuilder().addComponents(joinButtonCorrect);
  await sent.edit({ components: [newRow] }).catch(() => null);

  setTimeout(() => {
    endContestByMessageId(sent.id).catch((e) => console.error(e));
  }, timeMs);

  try {
    await interaction.editReply({
      content: `\`✅\` Konkurs opublikowany w <#${targetChannel.id}> i potrwa ${formatTimeDelta(timeMs)} (do <t:${ts}:R>)`,
    });
  } catch (err) {
    console.error("Błąd tworzenia konkursu:", err);
    try {
      await interaction.editReply({
        content: "> `❌` × **Nie udało się** utworzyć **konkursu**.",
      });
    } catch (e) {
      console.error("Nie udało się wysłać editReply po błędzie:", e);
    }
  }
}

async function handleKonkursJoinDirect(interaction, msgId) {
  const contest = contests.get(msgId);
  if (!contest) {
    await interaction.reply({
      embeds: [
        new EmbedBuilder()
          .setColor(COLOR_BLUE)
          .setDescription("> `❌` × **Konkurs** nie został znaleziony.")
          .setTimestamp(),
      ],
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }
  if (Date.now() >= contest.endsAt) {
    await interaction.reply({
      embeds: [
        new EmbedBuilder()
          .setColor(COLOR_BLUE)
          .setDescription("> `❌` × **Konkurs** już się zakończył.")
          .setTimestamp(),
      ],
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  if (contest.invitesRequired > 0) {
    const gMap = inviteCounts.get(interaction.guild.id) || new Map();
    const userInvites = gMap.get(interaction.user.id) || 0;
    if (userInvites < contest.invitesRequired) {
      await interaction.reply({
        embeds: [
          new EmbedBuilder()
            .setColor(COLOR_BLUE)
            .setDescription(
              `❌ Nie masz wystarczającej liczby zaproszeń. Wymagane: ${contest.invitesRequired}`,
            )
            .setTimestamp(),
        ],
        flags: [MessageFlags.Ephemeral],
      });
      return;
    }
  }

  let nick = "";

  let participantsMap = contestParticipants.get(msgId);
  if (!participantsMap) {
    participantsMap = new Map();
    contestParticipants.set(msgId, participantsMap);
  }

  const userId = interaction.user.id;
  if (participantsMap.has(userId)) {
    // Użytkownik już jest zapisany - pytaj czy chce opuścić
    const leaveBtn = new ButtonBuilder()
      .setCustomId(`konkurs_leave_${msgId}`)
      .setLabel("Opuść Konkurs")
      .setStyle(ButtonStyle.Danger);

    const cancelBtn = new ButtonBuilder()
      .setCustomId(`konkurs_cancel_leave_${msgId}`)
      .setLabel("Anuluj")
      .setStyle(ButtonStyle.Secondary);

    const row = new ActionRowBuilder().addComponents(leaveBtn, cancelBtn);

    const questionEmbed = new EmbedBuilder()
      .setColor(COLOR_BLUE)
      .setDescription("> \`❓\` × Już wziąłeś udział w tym konkursie! Czy chcesz go opuścić?");

    await interaction.reply({
      embeds: [questionEmbed],
      components: [row],
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  participantsMap.set(userId, nick);
  scheduleSavePersistentState();

  // Resetuj licznik wyjść gdy użytkownik ponownie dołącza do konkursu
  const userBlocks = contestLeaveBlocks.get(userId) || {};
  if (userBlocks[msgId]) {
    userBlocks[msgId].leaveCount = 0;
    userBlocks[msgId].blockedUntil = 0;
    contestLeaveBlocks.set(userId, userBlocks);
    scheduleSavePersistentState();
  }

  const participantsCount = participantsMap.size;

  // Aktualizuj wiadomość konkursu
  try {
    const ch = await client.channels.fetch(contest.channelId).catch(() => null);
    if (ch) {
      const origMsg = await ch.messages.fetch(msgId).catch(() => null);
      if (origMsg) {
        // Zaktualizuj opis
        let updatedDescription =
          `🎁 **•** Nagroda: **${contest.prize}**\n\n` +
          `🕐 **•** Koniec konkursu: ${formatTimeDelta(contest.endsAt - Date.now())}\n` +
          `👑 **•** Liczba zwycięzców: **${contest.winnersCount}**\n` +
          `👥 **•** Liczba uczestników: **${participantsCount}**`;



        if (contest.invitesRequired > 0) {
          const inviteForm = getPersonForm(contest.invitesRequired);
          updatedDescription += `\n\n⚠️ Wymagane: dodać ${contest.invitesRequired} ${inviteForm} na serwer`;
        }

        // Pobierz istniejący embed i zachowaj czarny kwadrat
        const existingEmbed = EmbedBuilder.from(origMsg.embeds[0]);
        const originalDescription = existingEmbed.data.description || '';

        // Wyodrębnij czarny kwadrat z oryginalnego opisu
        const blackBoxMatch = originalDescription.match(/```[\s\S]*?```/);
        const blackBox = blackBoxMatch ? blackBoxMatch[0] : '';

        // Połącz czarny kwadrat z nowym opisem
        const fullDescription = blackBox + '\n' + updatedDescription;
        existingEmbed.setDescription(fullDescription);

        // Zaktualizuj przycisk
        const joinButton = new ButtonBuilder()
          .setCustomId(`konkurs_join_${msgId}`)
          .setLabel(`Weź udział (${participantsCount})`)
          .setStyle(ButtonStyle.Secondary)
          .setDisabled(false);
        const row = new ActionRowBuilder().addComponents(joinButton);

        // Edytuj wiadomość - usuń stare załączniki i dodaj ten sam GIF ponownie
        try {
          const gifPath = path.join(
            __dirname,
            "attached_assets",
            "standard (4).gif",
          );
          const attachment = new AttachmentBuilder(gifPath, { name: "konkurs_start.gif" });
          existingEmbed.setImage("attachment://konkurs_start.gif");

          await origMsg.edit({
            embeds: [existingEmbed],
            components: [row],
            files: [attachment]
          }).catch(() => null);
        } catch (err) {
          console.warn("Nie udało się załadować GIFa przy edycji konkursu:", err);
          // Fallback: usuń załączniki bez GIFa
          await origMsg.edit({
            embeds: [existingEmbed],
            components: [row],
            attachments: []
          }).catch(() => null);
        }
      }
    }
  } catch (e) {
    console.warn("Nie udało się zaktualizować embed/btn konkursu:", e);
  }

  // Prosta odpowiedź dla nowego uczestnika
  const joinEmbed = new EmbedBuilder()
    .setColor(COLOR_BLUE)
    .setDescription("> \`✅\` × Poprawnie dołączyłeś do konkursu.");

  await interaction.reply({
    embeds: [joinEmbed],
    flags: [MessageFlags.Ephemeral],
  });
}

async function endContestByMessageId(messageId) {
  const meta = contests.get(messageId);
  if (!meta) return;
  const channel = await client.channels.fetch(meta.channelId).catch(() => null);
  if (!channel) return;

  const participantsMap = contestParticipants.get(messageId) || new Map();
  const participants = Array.from(participantsMap.entries());

  const winnersCount = Math.min(meta.winnersCount || 1, participants.length);
  const winners = pickRandom(participants, winnersCount);

  // logi-konkurs
  const logiKonkursChannelId = "1451666381937578004";
  let logChannel = null;
  try {
    logChannel = await channel.guild.channels
      .fetch(logiKonkursChannelId)
      .catch(() => null);
  } catch (e) {
    logChannel = null;
  }

  let winnersDetails = "";
  if (winners.length > 0) {
    winnersDetails = winners
      .map(
        ([userId, nick], i) =>
          `\`${i + 1}.\` <@${userId}> (MC: ${nick || "brak"})`,
      )
      .join("\n");
  } else {
    winnersDetails = "Brak zwycięzców";
  }

  const podsumowanieEmbed = new EmbedBuilder()
    .setColor(COLOR_BLUE)
    .setDescription(
      "```\n" +
      "🎉 Konkurs zakończony 🎉\n" +
      "```\n" +
      `**🎁 **•** Nagroda:** ${meta.prize}\n\n` +
      `**🏆 **•** Zwycięzcy:**\n${winnersDetails}`,
    )
    .setTimestamp();

  if (logChannel) {
    try {
      await logChannel.send({ embeds: [podsumowanieEmbed] });
    } catch (e) {
      console.warn("Nie udało się wysłać do logi-konkurs:", e);
    }
  }

  // Edytuj wiadomość konkursową — EMBED z wynikami + przycisk podsumowujący
  try {
    const origMsg = await channel.messages.fetch(messageId).catch(() => null);
    if (origMsg) {
      // embed końcowy
      const publicWinners =
        winners.length > 0
          ? winners.map(([userId]) => `<@${userId}>`).join("\n")
          : "Brak zwycięzców";

      const finalEmbed = new EmbedBuilder()
        .setColor(COLOR_BLUE)
        .setDescription(
          "```\n" +
          "🎉 Konkurs zakończony 🎉\n" +
          "```\n" +
          `**🎁 **•** Nagroda:** ${meta.prize}\n\n` +
          `**🏆 **•** Zwycięzcy:**\n${publicWinners}`,
        )
        .setTimestamp()
        .setImage("attachment://konkurs_end.gif");

      const personForm = getPersonForm(participants.length);
      let buttonLabel;
      if (participants.length === 1) {
        buttonLabel = `Wzięła udział 1 osoba`;
      } else if (
        participants.length % 10 >= 2 &&
        participants.length % 10 <= 4 &&
        (participants.length % 100 < 10 || participants.length % 100 >= 20)
      ) {
        buttonLabel = `Wzięły udział ${participants.length} ${personForm}`;
      } else {
        buttonLabel = `Wzięło udział ${participants.length} ${personForm}`;
      }

      const joinButton = new ButtonBuilder()
        .setCustomId(`konkurs_join_${messageId}`)
        .setLabel(buttonLabel)
        .setStyle(ButtonStyle.Secondary)
        .setDisabled(true);

      const row = new ActionRowBuilder().addComponents(joinButton);

      // Dodaj GIF na zakończenie konkursu
      try {
        const gifPath = path.join(
          __dirname,
          "attached_assets",
          "standard (3).gif",
        );
        const attachment = new AttachmentBuilder(gifPath, { name: "konkurs_end.gif" });
        await origMsg
          .edit({ embeds: [finalEmbed], components: [row], files: [attachment] })
          .catch(() => null);
      } catch (err) {
        console.warn("Nie udało się załadować GIFa na zakończenie konkursu:", err);
        try {
          finalEmbed.setImage(null);
        } catch (e) {
          // ignore
        }
        await origMsg
          .edit({ embeds: [finalEmbed], components: [row], attachments: [] })
          .catch(() => null);
      }
    }
  } catch (err) {
    console.warn("Nie udało się zedytować wiadomości konkursu na końcu:", err);
  }

  contests.delete(messageId);
  contestParticipants.delete(messageId);
  scheduleSavePersistentState();
}

// --- Obsługa /end-giveaways ---
async function handleEndGiveawaysCommand(interaction) {
  // Sprawdź czy właściciel serwera
  const isOwner = interaction.user.id === interaction.guild.ownerId;
  if (!isOwner) {
    await interaction.reply({
      content: "> `❌` × **Tylko właściciel serwera** może użyć tej komendy.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  if (!interaction.guild) {
    await interaction.reply({
      content: "> `❌` × **Tylko** na **serwerze**.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  const now = Date.now();
  const activeContests = Array.from(contests.entries()).filter(([_, meta]) => meta.endsAt > now);

  if (activeContests.length === 0) {
    await interaction.reply({
      content: "> `ℹ️` × **Brak aktywnych konkursów** do zakończenia.",
      flags: [MessageFlags.Ephemeral],
    });
    return;
  }

  // Zakończ wszystkie aktywne konkursy
  const endedContests = [];
  const failedContests = [];

  for (const [messageId, meta] of activeContests) {
    try {
      await endContestByMessageId(messageId);
      const timeLeft = meta.endsAt - now;
      endedContests.push({
        prize: meta.prize,
        timeLeft: humanizeMs(timeLeft),
        channelId: meta.channelId,
        messageId: messageId,
      });
    } catch (error) {
      console.error(`Błąd podczas kończenia konkursu ${messageId}:`, error);
      failedContests.push({
        prize: meta.prize,
        error: error.message,
      });
    }
  }

  // Stwórz embed z podsumowaniem
  const summaryEmbed = new EmbedBuilder()
    .setColor(endedContests.length > 0 ? COLOR_BLUE : COLOR_RED)
    .setTitle("🏁 Zakończono wszystkie konkursy")
    .setTimestamp()
    .setFooter(getBrandFooterBuilderObject());

  let description = "";

  if (endedContests.length > 0) {
    description += `## \`✅\` Pomyślnie zakończone konkursy (${endedContests.length}):\n\n`;
    endedContests.forEach((contest, index) => {
      description += `**${index + 1}. ${contest.prize}**\n`;
      description += `> ⏱️ Pozostało czasu: \`${contest.timeLeft}\`\n`;
      description += `> 📍 Kanał: <#${contest.channelId}>\n`;
      description += `> 🆔 ID wiadomości: \`${contest.messageId}\`\n\n`;
    });
  }

  if (failedContests.length > 0) {
    description += `## ❌ Nie udało się zakończyć (${failedContests.length}):\n\n`;
    failedContests.forEach((contest, index) => {
      description += `**${index + 1}. ${contest.prize}**\n`;
      description += `> 🚫 Błąd: \`${contest.error}\`\n\n`;
    });
  }

  summaryEmbed.setDescription(description);

  await interaction.reply({
    embeds: [summaryEmbed],
    flags: [MessageFlags.Ephemeral], // Tylko osoba wpisująca widzi odpowiedź
  });
}

// --- Obsługa opuszczenia konkursu ---
async function handleKonkursLeave(interaction, msgId) {
  const contest = contests.get(msgId);
  if (!contest) {
    await interaction.update({
      content: "> `❌` × **Konkurs** nie został znaleziony.",
      components: [],
    });
    return;
  }

  const userId = interaction.user.id;

  // Sprawdź blokadę opuszczania konkursu
  const userBlocks = contestLeaveBlocks.get(userId) || {};
  const contestBlock = userBlocks[msgId];

  if (contestBlock && contestBlock.blockedUntil > Date.now()) {
    const remainingTime = contestBlock.blockedUntil - Date.now();
    const timeString = formatBlockTime(remainingTime);

    await interaction.update({
      content: `> \`⏳\` × Musisz poczekać **${timeString}**, aby ponownie opuścić konkurs.`,
      components: [],
    });
    return;
  }

  let participantsMap = contestParticipants.get(msgId);
  if (!participantsMap) {
    await interaction.update({
      content: "> `❌` × **Nie bierzesz** udziału w tym **konkursie**.",
      components: [],
    });
    return;
  }

  if (!participantsMap.has(userId)) {
    await interaction.update({
      content: "> `❌` × **Nie bierzesz** udziału w tym **konkursie**.",
      components: [],
    });
    return;
  }

  // Zwiększ licznik wyjść i nałóż blokadę jeśli to drugie wyjście
  const currentLeaveCount = (contestBlock?.leaveCount || 0) + 1;

  if (currentLeaveCount >= 2) {
    // Nałóż blokadę 30 minut
    const blockedUntil = Date.now() + (30 * 60 * 1000); // 30 minut

    if (!userBlocks[msgId]) {
      userBlocks[msgId] = { leaveCount: 0, blockedUntil: 0 };
    }

    userBlocks[msgId].leaveCount = currentLeaveCount;
    userBlocks[msgId].blockedUntil = blockedUntil;

    contestLeaveBlocks.set(userId, userBlocks);
    scheduleSavePersistentState();
  } else {
    // Pierwsze wyjście - tylko zaktualizuj licznik
    if (!userBlocks[msgId]) {
      userBlocks[msgId] = { leaveCount: 0, blockedUntil: 0 };
    }

    userBlocks[msgId].leaveCount = currentLeaveCount;
    contestLeaveBlocks.set(userId, userBlocks);
    scheduleSavePersistentState();
  }

  // Usuwamy użytkownika z konkursu
  participantsMap.delete(userId);
  scheduleSavePersistentState();

  const participantsCount = participantsMap.size;

  // Aktualizujemy embed konkursu
  try {
    const ch = await client.channels.fetch(contest.channelId).catch(() => null);
    if (ch) {
      const origMsg = await ch.messages.fetch(msgId).catch(() => null);
      if (origMsg) {
        let updatedDescription =
          `🎁 **•** Nagroda: **${contest.prize}**\n\n` +
          `🕐 **•** Koniec konkursu: ${formatTimeDelta(contest.endsAt - Date.now())}\n` +
          `👑 **•** Liczba zwycięzców: **${contest.winnersCount}**\n` +
          `👥 **•** Liczba uczestników: **${participantsCount}**`;

        if (contest.invitesRequired > 0) {
          const inviteForm = getPersonForm(contest.invitesRequired);
          updatedDescription += `\n\n⚠️ Wymagane: dodać ${contest.invitesRequired} ${inviteForm} na serwer`;
        }

        // Pobierz istniejący embed i zachowaj czarny kwadrat
        const embed = origMsg.embeds[0]?.toJSON() || {};
        const originalDescription = embed.description || '';

        // Wyodrębnij czarny kwadrat z oryginalnego opisu
        const blackBoxMatch = originalDescription.match(/```[\s\S]*?```/);
        const blackBox = blackBoxMatch ? blackBoxMatch[0] : '';

        // Połącz czarny kwadrat z nowym opisem
        embed.description = blackBox + '\n' + updatedDescription;

        const joinButton = new ButtonBuilder()
          .setCustomId(`konkurs_join_${msgId}`)
          .setLabel(`Weź udział (${participantsCount})`)
          .setStyle(ButtonStyle.Secondary)
          .setDisabled(false);
        const row = new ActionRowBuilder().addComponents(joinButton);

        // Edytuj wiadomość - usuń stare załączniki i dodaj ten sam GIF ponownie
        try {
          const gifPath = path.join(
            __dirname,
            "attached_assets",
            "standard (4).gif",
          );
          const attachment = new AttachmentBuilder(gifPath, { name: "konkurs_start.gif" });
          embed.image = { url: "attachment://konkurs_start.gif" };

          await origMsg.edit({
            embeds: [embed],
            components: [row],
            files: [attachment]
          }).catch(() => null);
        } catch (err) {
          console.warn("Nie udało się załadować GIFa przy edycji konkursu (leave):", err);
          // Fallback: usuń załączniki bez GIFa
          await origMsg.edit({
            embeds: [embed],
            components: [row],
            attachments: []
          }).catch(() => null);
        }
      }
    }
  } catch (e) {
    console.warn("Nie udało się zaktualizować embed/btn konkursu:", e);
  }

  const leaveEmbed = new EmbedBuilder()
    .setColor(COLOR_BLUE)
    .setDescription("> \`🚪\` × Opuściłeś konkurs.");

  await interaction.update({
    embeds: [leaveEmbed],
    components: [],
  });
}

// --- Obsługa anulowania opuszczenia konkursu ---
async function handleKonkursCancelLeave(interaction, msgId) {
  const cancelEmbed = new EmbedBuilder()
    .setColor(COLOR_BLUE)
    .setDescription("> `📋` × Anulowano");

  await interaction.update({
    embeds: [cancelEmbed],
    components: [],
    content: "",
  });
}

// Modified: prefer fixed log channel ID 1450800337932783768 if accessible; otherwise fallback to channel name heuristics
async function getLogiTicketChannel(guild) {
  if (!guild) return null;
  // try the requested specific channel ID first (user requested)
  const forcedId = "1450800337932783768";
  try {
    const forced = await guild.channels.fetch(forcedId).catch(() => null);
    if (forced && forced.type === ChannelType.GuildText) return forced;
  } catch (e) {
    // ignore
  }

  // First try exact name 'logi-ticket', then contains or similar
  const ch =
    guild.channels.cache.find(
      (c) =>
        c.type === ChannelType.GuildText &&
        (c.name === "logi-ticket" ||
          c.name.toLowerCase().includes("logi-ticket") ||
          c.name.toLowerCase().includes("logi ticket") ||
          c.name.toLowerCase().includes("logi_ticket")),
    ) || null;
  return ch;
}

function truncateTicketLogValue(value, max = 1024) {
  const text = (value || "").toString().trim();
  if (!text) return "brak";
  if (text.length <= max) return text;
  return `${text.slice(0, Math.max(0, max - 3))}...`;
}

function formatTicketLogUser(userId) {
  if (!userId) return "brak";
  return `<@${userId}>\n\`${userId}\``;
}

function formatTicketLogChannel(ticketChannel) {
  if (!ticketChannel) return "brak";
  return `<#${ticketChannel.id}>\n\`${ticketChannel.name}\``;
}

function formatTicketLogCategory(ticketChannel) {
  if (!ticketChannel?.parentId) return "brak";

  const parent =
    ticketChannel.parent ||
    ticketChannel.guild?.channels?.cache?.get(ticketChannel.parentId) ||
    null;

  if (!parent) return `<#${ticketChannel.parentId}>`;
  return `<#${ticketChannel.parentId}>\n\`${parent.name}\``;
}

function formatTicketLogTimestamp(timestamp) {
  if (!timestamp) return "brak";
  const unix = Math.floor(timestamp / 1000);
  return `<t:${unix}:F>\n<t:${unix}:R>`;
}

function cleanTicketLogText(raw = "") {
  const lines = String(raw)
    .split("\n")
    .map((line) =>
      line
        .replace(/^>\s*/, "")
        .replace(/<a?:[A-Za-z0-9_~]+:\d+>\s*/g, "")
        .replace(/\*\*/g, "")
        .replace(/`/g, "")
        .replace(/\s+×\s+/g, " ")
        .trim(),
    )
    .filter(Boolean);

  return lines.length ? lines.join("\n") : "brak";
}

function guessTicketTypeLabel(ticketChannel, ticketMeta = null) {
  if (ticketMeta?.ticketTypeLabel) return ticketMeta.ticketTypeLabel;
  if (!ticketChannel?.guild) return "brak";

  if (ticketChannel.parentId && String(ticketChannel.parentId) === String(REWARDS_CATEGORY_ID)) {
    return "NAGRODA";
  }

  if (ticketChannel.parentId && (String(ticketChannel.parentId) === String(PRIVATE_SPECIAL_PURCHASE_CATEGORY_ID) || ticketCategoryManager.keyFor(ticketChannel.guild, ticketChannel.parentId) === "boty-mody")) {
    const normalizedName = String(ticketChannel.name || "").toLowerCase();
    const normalizedTopic = String(ticketChannel.topic || "").toLowerCase();
    if (
      normalizedName.endsWith("-boty") ||
      normalizedName.endsWith("-bot") ||
      normalizedTopic.includes("zakup botów") ||
      normalizedTopic.includes("zakup bota") ||
      normalizedName.endsWith("-autorynek") ||
      normalizedTopic.includes("zakup autorynku")
    ) {
      return "ZAKUP BOTÓW";
    }
    if (
      normalizedName.endsWith("-mod") ||
      normalizedName.endsWith("-mody") ||
      normalizedTopic.includes("zakup moda")
    ) {
      return "ZAKUP MODÓW";
    }
  }

  const cats = ticketCategories.get(ticketChannel.guild.id) || {};
  const zakupCategoryIds = [
    cats["zakup-0-20"],
    cats["zakup-20-50"],
    cats["zakup-50-100"],
    cats["zakup-100-200"],
    cats["zakup-200-400"],
    cats["zakup-400-999"],
  ].filter(Boolean);

  if (zakupCategoryIds.includes(ticketChannel.parentId) || isModernPurchaseTicketChannelName(ticketChannel.name)) {
    return "ZAKUP";
  }
  if (ticketChannel.parentId === cats["sprzedaz"]) return "SPRZEDAŻ";
  if (ticketChannel.parentId === cats["inne"]) return "PYTANIE / POMOC";
  if (ticketChannel.parentId === cats["odbior-nagrody"]) return "NAGRODA";

  return "TICKET";
}

function buildTicketLogDetailsValue({ formInfo = "", detailLines = [] } = {}) {
  const chunks = [];
  const cleanedFormInfo = cleanTicketLogText(formInfo);
  if (cleanedFormInfo !== "brak") chunks.push(cleanedFormInfo);

  for (const line of detailLines) {
    if (!line) continue;
    chunks.push(`• ${line}`);
  }

  if (!chunks.length) return "brak";
  return truncateTicketLogValue(chunks.join("\n"), 1024);
}

async function findTicketLogCard(logCh, ticketId) {
  if (!ticketId) return null;

  const rememberedId = ticketLogCards.get(ticketId);
  if (rememberedId) {
    const remembered = await logCh.messages.fetch(rememberedId).catch(() => null);
    if (remembered) return remembered;
    ticketLogCards.delete(ticketId);
  }

  const recent = await logCh.messages.fetch({ limit: 100 }).catch(() => null);
  const existing = recent?.find((message) =>
    message.author?.id === client.user?.id &&
    message.embeds?.some((embed) =>
      String(embed.footer?.text || "").includes(`Ticket ID: ${ticketId}`),
    ),
  ) || null;

  if (existing) ticketLogCards.set(ticketId, existing.id);
  return existing;
}

function getTicketLogTimeline(existingMessage) {
  const field = existingMessage?.embeds?.[0]?.fields?.find(
    (item) => item.name === "Historia",
  );
  if (!field?.value) return [];
  return String(field.value).split("\n").filter(Boolean).slice(-7);
}

async function sendTicketLogEntry(guild, options = {}) {
  const logCh = await getLogiTicketChannel(guild);
  if (!logCh) return null;

  const ticketChannel = options.ticketChannel || null;
  const ticketMeta = options.ticketMeta || null;
  const ticketId = String(ticketChannel?.id || options.ticketId || "");
  const existingMessage = await findTicketLogCard(logCh, ticketId);
  const now = Date.now();
  const actorId = options.actorId || null;
  const title = options.title || "Akcja na tickecie";
  const icon = options.icon || "🎫";
  const eventKey = `${title}|${actorId || "system"}|${options.statusLabel || ""}|${options.reason || ""}`;
  const lastEvent = ticketLogEventDedupe.get(ticketId);
  const isDuplicate =
    lastEvent?.key === eventKey && now - Number(lastEvent.timestamp || 0) < 15_000;

  let timeline = getTicketLogTimeline(existingMessage);
  if (!isDuplicate) {
    timeline.push(
      `<t:${Math.floor(now / 1000)}:t> ${icon} **${truncateTicketLogValue(title, 80)}**` +
      (actorId ? ` — <@${actorId}>` : ""),
    );
    timeline = timeline.slice(-8);
    ticketLogEventDedupe.set(ticketId, { key: eventKey, timestamp: now });
  }

  const detailsValue = buildTicketLogDetailsValue({
    formInfo: options.formInfo,
    detailLines: options.detailLines,
  });
  const info = [];
  if (options.reason) info.push(`**Powód:** ${truncateTicketLogValue(options.reason, 700)}`);
  if (detailsValue !== "brak") info.push(detailsValue);
  if (typeof options.messageCount === "number") {
    info.push(`**Wiadomości:** \`${options.messageCount}\``);
  }
  if (options.participantsText) {
    info.push(`**Uczestnicy:** ${truncateTicketLogValue(options.participantsText, 700)}`);
  }

  const openedAt =
    options.openedAt ?? ticketMeta?.openedAt ?? ticketChannel?.createdTimestamp ?? null;
  const status = options.statusLabel || "BRAK STATUSU";
  const type = options.ticketTypeLabel || guessTicketTypeLabel(ticketChannel, ticketMeta);
  const ownerId = options.ownerId ?? ticketMeta?.userId ?? null;
  const claimedById = options.claimedById ?? ticketMeta?.claimedBy ?? null;

  const embed = new EmbedBuilder()
    .setColor(options.color ?? COLOR_BLUE)
    .setAuthor({
      name: "New Shop • Centrum ticketu",
      iconURL: guild.iconURL?.({ size: 128 }) || undefined,
    })
    .setTitle(`${icon} ${title}`)
    .setDescription(
      truncateTicketLogValue(
        options.summary ||
          `Aktualny stan ticketu ${ticketChannel ? `<#${ticketChannel.id}>` : `\`${ticketId}\``}.`,
        4096,
      ),
    )
    .addFields(
      { name: "Status", value: `\`${status}\``, inline: true },
      { name: "Typ", value: `\`${type}\``, inline: true },
      {
        name: "Kanał",
        value: ticketChannel ? `<#${ticketChannel.id}>\n\`${ticketChannel.name}\`` : `\`${ticketId}\``,
        inline: true,
      },
      { name: "Klient", value: formatTicketLogUser(ownerId), inline: true },
      { name: "Obsługa", value: formatTicketLogUser(claimedById), inline: true },
      { name: "Ostatnia akcja", value: formatTicketLogUser(actorId), inline: true },
    );

  if (info.length) {
    embed.addFields({
      name: "Informacje",
      value: truncateTicketLogValue(info.join("\n"), 1024),
      inline: false,
    });
  }
  if (timeline.length) {
    embed.addFields({
      name: "Historia",
      value: truncateTicketLogValue(timeline.join("\n"), 1024),
      inline: false,
    });
  }

  const shortTicketId = generateTicketShortCode(ticketId);
  if (ticketId) {
    const existingRec = ticketHistoryStore.get(ticketId) || ticketHistoryStore.get(shortTicketId) || {};
    const updatedRec = {
      ...existingRec,
      shortId: shortTicketId,
      channelId: ticketId,
      channelName: ticketChannel?.name || existingRec.channelName || "unknown",
      ownerId: ownerId || existingRec.ownerId,
      type: type || existingRec.type,
      status: status || existingRec.status,
      openedAt: openedAt || existingRec.openedAt || now,
      closedAt: options.statusLabel === "ZAMKNIĘTY" ? now : existingRec.closedAt,
      closedBy: actorId || existingRec.closedBy,
      closeReason: options.reason || existingRec.closeReason,
      claimedBy: claimedById || existingRec.claimedBy,
      formInfo: options.formInfo || existingRec.formInfo,
      history: timeline.length ? timeline : existingRec.history || [],
    };

    if (options.files?.length && options.files[0]?.attachment) {
      const fileItem = options.files[0];
      if (Buffer.isBuffer(fileItem.attachment)) {
        updatedRec.transcriptText = fileItem.attachment.toString("utf-8");
      }
    }

    ticketHistoryStore.set(ticketId, updatedRec);
    ticketHistoryStore.set(shortTicketId, updatedRec);
  }

  embed
    .setFooter({
      text: `Kod ID: ${shortTicketId} • Ticket ID: ${ticketId || "brak"}` +
        (openedAt ? ` • utworzony ${new Date(openedAt).toLocaleDateString("pl-PL")}` : ""),
    })
    .setTimestamp(now);

  const payload = { embeds: [embed] };
  if (options.files?.length) payload.files = options.files;

  let logMessage = existingMessage;
  if (logMessage) {
    const edited = await logMessage.edit(payload).catch(() => null);
    if (!edited) logMessage = null;
  }
  if (!logMessage) {
    logMessage = await logCh.send(payload).catch(() => null);
  }

  if (logMessage && logMessage.attachments?.size) {
    const attUrl = logMessage.attachments.first()?.url;
    if (attUrl && ticketId) {
      const rec = ticketHistoryStore.get(ticketId) || ticketHistoryStore.get(shortTicketId);
      if (rec) rec.attachmentUrl = attUrl;
    }
  }

  if (ticketId && logMessage?.id) ticketLogCards.set(ticketId, logMessage.id);
  return logMessage;
}

async function logTicketCreation(guild, ticketChannel, details) {
  try {
    await sendTicketLogEntry(guild, {
      title: "Ticket utworzony",
      icon: "🟢",
      color: COLOR_BLUE,
      summary: "Nowy ticket został utworzony i czeka na obsługę.",
      ticketChannel,
      ownerId: details.openerId,
      actorId: details.openerId,
      claimedById: null,
      statusLabel: "OTWARTY",
      ticketTypeLabel: details.ticketTypeLabel,
      formInfo: details.formInfo,
      detailLines: [
        details.ticketMessageId
          ? `ID wiadomości startowej: ${details.ticketMessageId}`
          : null,
      ],
    });
  } catch (e) {
    console.error("logTicketCreation error:", e);
  }
}

async function archiveTicketOnClose(ticketChannel, closedById, ticketMeta, extra = {}) {
  try {
    const guild = ticketChannel.guild;
    const logCh = await getLogiTicketChannel(guild);
    if (!logCh) {
      console.warn("Brak kanału logi-ticket — pomijam logowanie ticketu.");
      return;
    }

    // Fetch all messages (up to 100)
    const fetched = await ticketChannel.messages
      .fetch({ limit: 100 })
      .catch(() => null);
    const messages = fetched ? Array.from(fetched.values()) : [];

    let beforeId = fetched && fetched.size ? fetched.last().id : null;
    while (beforeId) {
      const batch = await ticketChannel.messages
        .fetch({ limit: 100, before: beforeId })
        .catch(() => null);
      if (!batch || batch.size === 0) break;
      messages.push(...Array.from(batch.values()));
      beforeId = batch.size ? batch.last().id : null;
      if (batch.size < 100) break;
    }

    messages.sort((a, b) => a.createdTimestamp - b.createdTimestamp);

    const openerId = ticketMeta?.userId || null;
    const claimedById = ticketMeta?.claimedBy || null;

    const participantsSet = new Set();
    for (const m of messages) {
      if (m && m.author && m.author.id) participantsSet.add(m.author.id);
    }
    const participants = Array.from(participantsSet);
    const participantsPreview = participants.slice(0, 20);
    const participantsText = participantsPreview.length
      ? `${participantsPreview.map((id) => `<@${id}>`).join(" ")}${participants.length > participantsPreview.length ? ` (+${participants.length - participantsPreview.length})` : ""}`
      : "brak";

    // Build transcript
    const lines = messages.map((m) => {
      const time = new Date(m.createdTimestamp).toLocaleString("pl-PL");
      const authorTag = m.author ? m.author.tag : "unknown";
      const authorId = m.author ? m.author.id : "unknown";
      const content = m.content ? m.content : "";
      const attachmentUrls =
        m.attachments && m.attachments.size
          ? Array.from(m.attachments.values())
            .map((a) => a.url)
            .join(", ")
          : "";
      const attachments = attachmentUrls ? `\n[ATTACHMENTS: ${attachmentUrls}]` : "";
      return `${time}\n${authorTag} (${authorId})\n${content}${attachments}`;
    });

    let transcriptText =
      `Ticket: ${ticketChannel.name}\n` +
      `Channel ID: ${ticketChannel.id}\n` +
      `Close method: ${extra.closeMethod || "standard"}\n` +
      `Close reason: ${extra.reason || "brak"}\n` +
      `Closed by: ${closedById}\n` +
      `Opened by: ${openerId || "unknown"}\n` +
      `Claimed by: ${claimedById || "brak"}\n` +
      `Type: ${guessTicketTypeLabel(ticketChannel, ticketMeta)}\n` +
      `Messages: ${messages.length}\n` +
      `Participants: ${participants.join(", ") || "brak"}\n\n` +
      `--- MESSAGES ---\n\n` +
      lines.join("\n\n");

    const maxBytes = 7_500_000;
    let buffer = Buffer.from(transcriptText, "utf-8");
    if (buffer.length > maxBytes) {
      const ratio = maxBytes / buffer.length;
      const cutIndex = Math.max(0, Math.floor(transcriptText.length * ratio) - 50);
      transcriptText = `${transcriptText.slice(0, cutIndex)}\n\n[TRUNCATED]`;
      buffer = Buffer.from(transcriptText, "utf-8");
    }

    const fileName = `ticket-${ticketChannel.name.replace(/[^a-z0-9-_]/gi, "_")}-${Date.now()}.txt`;
    const attachment = new AttachmentBuilder(buffer, { name: fileName });

    await sendTicketLogEntry(guild, {
      title: "Ticket zamknięty",
      icon: "🔴",
      color: COLOR_RED,
      summary: null,
      ticketChannel,
      ownerId: openerId,
      actorId: closedById,
      claimedById,
      ticketMeta,
      ticketTypeLabel: guessTicketTypeLabel(ticketChannel, ticketMeta),
      statusLabel: "ZAMKNIĘTY",
      formInfo: ticketMeta?.formInfo,
      detailLines: [],
      reason: extra.reason || null,
      messageCount: messages.length,
      participantsText,
      files: [attachment],
    });
  } catch (e) {
    console.error("archiveTicketOnClose error:", e);
  }
}

// ---------------------------------------------------
// SYSTEM ROZLICZEN TYGODNIOWYCH
const ROZLICZENIA_CHANNEL_ID = "1449162620807675935";
const ROZLICZENIA_LOGS_CHANNEL_ID = "1457140136461730075";
const ROZLICZENIA_PROWIZJA = 0.10; // 10%

// Mapa na sumy sprzedaży w tygodniu
const weeklySales = new Map(); // userId -> { amount, lastUpdate }

// ID aktualnej wiadomości panelu rozliczeń (do usunięcia przed wysłaniem nowej)
let rozliczeniaPanelMessageId = null;

// Funkcja do wysyłania wiadomości o rozliczeniach
function buildRozliczeniaPanelPayload(guildId) {
  const container = new ContainerBuilder().setAccentColor(COLOR_GREEN);
  container.addTextDisplayComponents(
    new TextDisplayBuilder().setContent("`💱` × Dodaj **każdą sprzedaż** przyciskiem poniżej.")
  );
  container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));
  const kasaEmoji = findGuildEmojiByName(guildId, "kasa_3");
  const btn = new ButtonBuilder()
    .setCustomId("rozliczenie_dodaj_btn")
    .setLabel("︲Dodaj sprzedaż")
    .setStyle(ButtonStyle.Secondary);
  btn.setEmoji(kasaEmoji
    ? { id: kasaEmoji.id, name: kasaEmoji.name, animated: kasaEmoji.animated } : "💵");
  container.addActionRowComponents(new ActionRowBuilder().addComponents(btn));
  appendBrandFooterToContainer(container, guildId);
  return { components: [container], flags: MessageFlags.IsComponentsV2 };
}

async function sendRozliczeniaMessage(options = {}) {
  const force = typeof options === "object" && options?.force === true;
  try {
    const channel = await client.channels.fetch(ROZLICZENIA_CHANNEL_ID).catch(() => null);
    if (!channel) return;

    const fetched = await channel.messages.fetch({ limit: 5 }).catch(() => null);
    const lastMsg = fetched?.first();

    if (!force && lastMsg && lastMsg.author.id === client.user.id && JSON.stringify(lastMsg.toJSON()).includes("rozliczenie_dodaj_btn")) {
      rozliczeniaPanelMessageId = lastMsg.id;
      return;
    }

    // Usun stara wiadomosc panelu po zapisanym ID
    if (rozliczeniaPanelMessageId) {
      await channel.messages.delete(rozliczeniaPanelMessageId).catch(() => null);
      rozliczeniaPanelMessageId = null;
    }

    // Awaryjnie usun tez wszystkie stare panele bota z kanalu (np. po restarcie)
    if (fetched && fetched.size > 0) {
      const oldPanels = fetched.filter(msg =>
        msg.author.id === client.user.id &&
        msg.components && msg.components.length > 0 &&
        JSON.stringify(msg.toJSON()).includes("rozliczenie_dodaj_btn")
      );
      for (const [, msg] of oldPanels) {
        await msg.delete().catch(() => null);
      }
    }

    // Wyślij nową wiadomość w stylu ContainerBuilder
    const container = new ContainerBuilder().setAccentColor(COLOR_GREEN);
    container.addTextDisplayComponents(
      new TextDisplayBuilder().setContent(
        "`💱` × Dodaj **każdą sprzedaż** przyciskiem poniżej."
      )
    );

    container.addSeparatorComponents(new SeparatorBuilder().setDivider(true));

    const kasaEmoji = findGuildEmojiByName(channel.guild?.id, "kasa_3");
    const btn = new ButtonBuilder()
      .setCustomId("rozliczenie_dodaj_btn")
      .setLabel("︲Dodaj sprzedaż")
      .setStyle(ButtonStyle.Secondary);

    if (kasaEmoji) {
      btn.setEmoji({ id: kasaEmoji.id, name: kasaEmoji.name, animated: kasaEmoji.animated });
    } else {
      btn.setEmoji("💵");
    }

    container.addActionRowComponents(
      new ActionRowBuilder().addComponents(btn)
    );

    appendBrandFooterToContainer(container, channel.guild.id);

    const sentMsg = await channel.send({
      components: [container],
      flags: MessageFlags.IsComponentsV2,
    });
    rozliczeniaPanelMessageId = sentMsg.id;
    console.log("Wysłano nową wiadomość ContainerBuilder ROZLICZENIA TYGODNIOWE");
  } catch (err) {
    console.error("Błąd wysyłania wiadomości ROZLICZENIA TYGODNIOWE:", err);
  }
}

let lastReset00Trigger = "";
let lastDeadline20Trigger = "";

// Funkcja do sprawdzania i resetowania cotygodniowych rozliczeń
async function checkWeeklyReset() {
  const { dayOfWeek, hour, minute, dateKey } = getPolandTime();

  // Niedziela 00:00 - odebranie limitów dla osób z niezapłaconym rozliczeniem
  if (dayOfWeek === 0 && hour === 0 && lastReset00Trigger !== dateKey) {
    lastReset00Trigger = dateKey;
    console.log("[rozliczenia-timer] Niedziela 00:00 - zabieranie limitów dla osób bez zapłaty...");
    for (const guild of client.guilds.cache.values()) {
      await executeSundayReset00(guild).catch((e) => console.error("Błąd executeSundayReset00:", e));
    }
    setTimeout(sendRozliczeniaMessage, 1000);
  }

  // Niedziela 20:00 - nadanie roli ZAWIESZONY osobom które nie zapłaciły do 20:00
  if (dayOfWeek === 0 && hour >= 20 && lastDeadline20Trigger !== dateKey) {
    lastDeadline20Trigger = dateKey;
    console.log("[rozliczenia-timer] Niedziela 20:00 - nadawanie roli ZAWIESZONY dla osób bez zapłaty...");
    for (const guild of client.guilds.cache.values()) {
      await executeSundayDeadline20(guild).catch((e) => console.error("Błąd executeSundayDeadline20:", e));
    }
    setTimeout(sendRozliczeniaMessage, 1000);
  }

  // Niedziela - jeśli wszyscy zapłacili, zresetuj statystyki od razu
  const hasSales = Array.from(weeklySales.values()).some((data) => data.amount > 0);
  const allPaid = hasSales && Array.from(weeklySales.values()).every((data) => data.paid || data.amount === 0);
  if (dayOfWeek === 0 && allPaid) {
    console.log("[rozliczenia-timer] Niedziela - wszyscy użytkownicy zapłacili. Resetuję statystyki na nowy tydzień...");
    weeklySales.clear();
    isSundayResetTriggered = false;
    await db.resetWeeklySales().catch((e) => console.error("Błąd resetWeeklySales:", e));
    scheduleSavePersistentState(true);
    setTimeout(sendRozliczeniaMessage, 1000);
    for (const guild of client.guilds.cache.values()) {
      await sendRozliczeniaStatusReport(guild, true).catch(() => null);
    }
  }

  // Niedziela 23:59 - czyszczenie danych na nowy tydzień (fallback)
  if (dayOfWeek === 0 && hour === 23 && minute >= 55 && weeklySales.size > 0) {
    console.log("[rozliczenia-timer] Koniec tygodnia - reset danych rozliczeń na nowy tydzień...");
    weeklySales.clear();
    isSundayResetTriggered = false;
    await db.resetWeeklySales().catch((e) => console.error("Błąd resetWeeklySales:", e));
    scheduleSavePersistentState(true);
    setTimeout(sendRozliczeniaMessage, 1000);
  }
}

// Listener dla nowych wiadomości na kanale rozliczeń
client.on('messageCreate', async (message) => {
  // Ignoruj wiadomości od botów
  if (message.author.bot) return;

  // Sprawdź czy wiadomość jest na kanale rozliczeń
  if (message.channelId === ROZLICZENIA_CHANNEL_ID) {
    if (!message.content.startsWith('/rozliczenie')) {
      try {
        await message.delete().catch(() => null);
        await message.author.send({
          content: `> \`ℹ️\` × Na kanale <#${ROZLICZENIA_CHANNEL_ID}> użyj przycisku **Dodaj sprzedaż**, aby zarejestrować rozliczenie.`,
        }).catch(() => null);
      } catch (err) {
        console.error("Błąd usuwania wiadomości z kanału rozliczeń:", err);
      }
      return;
    }

    // Odśwież wiadomość ROZLICZENIA TYGODNIOWE
    setTimeout(sendRozliczeniaMessage, 1000); // Małe opóźnienie dla pewności
  }
});

// Uruchom sprawdzanie co 10 sekund (dla natychmiastowej precyzji o 00:00:00)
setInterval(checkWeeklyReset, 10 * 1000);

// Wysyłaj wiadomość o rozliczeniach i odświeżaj raport statusu
setInterval(async () => {
  await sendRozliczeniaMessage().catch(() => null);
  for (const guild of client.guilds.cache.values()) {
    await sendRozliczeniaStatusReport(guild).catch(() => null);
  }
}, 60 * 60 * 1000);

// Wyślij wiadomość, sprawdź reset i odśwież raport przy starcie bota
setTimeout(async () => {
  await checkWeeklyReset().catch(() => null);
  await sendRozliczeniaMessage().catch(() => null);
  for (const guild of client.guilds.cache.values()) {
    await sendRozliczeniaStatusReport(guild).catch(() => null);
  }
}, 5000);

// Monitoring is installed at process startup; /statusbota uses the same measurements.
async function checkBotStatus() { return monitoring.snapshot(); }

console.log("[DEBUG] Próba połączenia z Discord...");
console.log("[DEBUG] BOT_TOKEN exists:", !!process.env.BOT_TOKEN);
console.log("[DEBUG] BOT_TOKEN length:", process.env.BOT_TOKEN?.length || 0);

client.on("messageDelete", async (message) => {
  if (!message.guild || message.author?.bot) return;
  if (!isTicketChannel(message.channel)) return;
  const logCh = await getLogiTicketChannel(message.guild);
  if (!logCh) return;
  const content = message.content || "[Brak treści]";
  const attachments = message.attachments.map(a => a.url).join("\n") || "Brak załączników";
  const embed = new EmbedBuilder()
    .setColor(0xff0000)
    .setTitle("🗑️ Wiadomość usunięta w tickecie")
    .addFields(
      { name: "Autor", value: `${message.author.tag} (<@${message.author.id}>)` },
      { name: "Kanał", value: `<#${message.channel.id}>` },
      { name: "Treść", value: content.substring(0, 1024) },
      { name: "Załączniki", value: attachments.substring(0, 1024) }
    )
    .setTimestamp();

  const files = [];
  if (message.attachments.size > 0) {
    message.attachments.forEach(att => {
      files.push({ attachment: att.url, name: att.name || "zalacznik.png" });
    });
  }

  await logCh.send({ embeds: [embed], files }).catch(() => null);
});


client.on("messageCreate", async (message) => {
  if (!message.guild || message.author?.bot) return;
  if (!isTicketChannel(message.channel)) return;
  if (message.attachments.size === 0) return;
  const logCh = await getLogiTicketChannel(message.guild);
  if (!logCh) return;

  const files = [];
  message.attachments.forEach(att => {
    files.push({ attachment: att.url, name: att.name || "zalacznik.png" });
  });

  const embed = new EmbedBuilder()
    .setColor(0x00ff00)
    .setTitle("🖼️ Przesłano załącznik w tickecie")
    .addFields(
      { name: "Autor", value: `${message.author.tag} (<@${message.author.id}>)` },
      { name: "Kanał", value: `<#${message.channel.id}>` }
    )
    .setTimestamp();

  await logCh.send({ embeds: [embed], files }).catch(() => null);
});

// Prosta funkcja retry z backoffem i obsługą 429 + diagnostyka
async function loginWithRetry(maxRetries = 5) {
  for (let i = 0; i < maxRetries; i++) {
    let slowLoginWarning = null;
    let hardTimeoutId = null;
    try {
      const attempt = i + 1;
      console.log(`[LOGIN] Próba ${attempt}/${maxRetries}...`);
      lastDiscordLoginDiagnostic = {
        stage: 'connecting',
        attempt,
        timestamp: new Date().toISOString(),
      };

      slowLoginWarning = setTimeout(() => {
        console.warn(`[LOGIN] Logowanie trwa długo (>30s) — czekam na odpowiedź Discorda...`);
      }, 30000);

      const hardTimeout = new Promise((_, reject) => {
        hardTimeoutId = setTimeout(() => reject(new Error('LOGIN_HARD_TIMEOUT_90S')), 90000);
      });

      await Promise.race([
        client.login(process.env.BOT_TOKEN), hardTimeout]);

      console.log("[LOGIN] Sukces! Bot połączony z Discord.");
      lastDiscordLoginDiagnostic = {
        stage: 'connected',
        attempt,
        timestamp: new Date().toISOString(),
      };
      return true;
    } catch (err) {
      const retryAfterSeconds = Number(
        err?.data?.retry_after || err?.rawError?.retry_after || err?.retry_after || 0,
      );
      const is429 = err?.status === 429
        || err?.code === 429
        || retryAfterSeconds > 0
        || /429|rate.?limit/i.test(err?.message || "");
      const retryAfterHeader = retryAfterSeconds * 1000;
      const backoff = is429 ? Math.max(retryAfterHeader, 30000) : 10000 * (i + 1);
      lastDiscordLoginDiagnostic = {
        stage: 'retry_wait',
        attempt: i + 1,
        error_code: err?.code || null,
        http_status: err?.status || null,
        rate_limited: is429,
        retry_in_seconds: Math.round(backoff / 1000),
        timestamp: new Date().toISOString(),
      };

      console.error(`[LOGIN] Błąd próby ${i + 1}:`, err?.message || err);
      if (err?.code) console.error(`[LOGIN] err.code=${err.code}`);
      if (err?.status) console.error(`[LOGIN] err.status=${err.status}`);
      if (err?.data?.retry_after) console.error(`[LOGIN] retry_after=${err.data.retry_after}`);

      if (err?.name === 'DiscordAPIError' && err?.rawError) {
        console.error('[LOGIN] rawError:', err.rawError);
      }

      if (err?.message === 'LOGIN_HARD_TIMEOUT_90S') {
        client.destroy();
      }

      if (i < maxRetries - 1) {
        console.log(`[LOGIN] Czekam ${Math.round(backoff / 1000)}s przed kolejną próbą...`);
        await new Promise(resolve => setTimeout(resolve, backoff));
      }
    } finally {
      if (slowLoginWarning) clearTimeout(slowLoginWarning);
      if (hardTimeoutId) clearTimeout(hardTimeoutId);
    }
  }

  console.error("[LOGIN] Wszystkie próby nieudane!");
  lastDiscordLoginDiagnostic = {
    stage: 'series_failed',
    attempt: maxRetries,
    timestamp: new Date().toISOString(),
  };

  // Sprawdź połączenie sieciowe
  console.log("[NETWORK] Sprawdzam połączenie z Discord API...");
  try {
    const https = require('https');
    const req = https.request('https://discord.com/api/v10/gateway', (res) => {
      console.log(`[NETWORK] Discord API response: ${res.statusCode}`);
      if (res.statusCode === 200) {
        console.log("[NETWORK] Discord API jest dostępne - problem może być z WebSocket");
      } else {
        console.log(`[NETWORK] Discord API zwróciło: ${res.statusCode}`);
      }
    });
    req.on('error', (err) => {
      console.error("[NETWORK] Błąd połączenia z Discord API:", err.message);
    });
    req.setTimeout(5000, () => {
      console.error("[NETWORK] Timeout połączenia z Discord API");
      req.destroy();
    });
    req.end();
  } catch (err) {
    console.error("[NETWORK] Błąd sprawdzania połączenia:", err.message);
  }

  return false;
}

// Start login
async function startDiscordLoginLoop() {
  while (!client.isReady()) {
    const connected = await loginWithRetry();
    if (connected || client.isReady()) return;

    console.error("[LOGIN] Bot nadal offline. Kolejna seria prób za 5 minut.");
    await new Promise((resolve) => setTimeout(resolve, 5 * 60 * 1000));
  }
}

startDiscordLoginLoop().catch((error) => {
  console.error("[LOGIN] Nieoczekiwany błąd pętli logowania:", error);
});

function getVideoContentType(filePath) {
  const ext = path.extname(filePath || "").toLowerCase();
  if (ext === ".mp4") return "video/mp4";
  if (ext === ".mov") return "video/quicktime";
  if (ext === ".webm") return "video/webm";
  return "application/octet-stream";
}

app.get('/videos/:videoKey', (req, res) => {
  try {
    const videoKey = (req.params.videoKey || "").trim();
    const videoCfg = MODS_VIDEO_FILES.find((v) => v.key === videoKey);

    if (!videoCfg) {
      res.status(404).json({ error: "video_not_found" });
      return;
    }

    const localVideoPath = resolveLocalModsVideoPath(videoCfg);
    if (!localVideoPath) {
      res.status(404).json({ error: "video_file_missing" });
      return;
    }

    const stat = fs.statSync(localVideoPath);
    const totalSize = stat.size;
    const rangeHeader = req.headers.range;
    const contentType = getVideoContentType(localVideoPath);

    res.setHeader("Accept-Ranges", "bytes");
    res.setHeader("Cache-Control", "public, max-age=3600");
    res.setHeader("Content-Type", contentType);
    res.setHeader(
      "Content-Disposition",
      `inline; filename="${path.basename(localVideoPath)}"`,
    );

    if (!rangeHeader) {
      res.setHeader("Content-Length", totalSize);
      const stream = fs.createReadStream(localVideoPath);
      stream.on("error", (err) => {
        console.error("[VIDEO] Błąd streamu bez range:", err);
        if (!res.headersSent) {
          res.status(500).json({ error: "stream_error" });
        } else {
          res.end();
        }
      });
      stream.pipe(res);
      return;
    }

    const match = /^bytes=(\d*)-(\d*)$/.exec(rangeHeader);
    if (!match) {
      res.status(416).setHeader("Content-Range", `bytes */${totalSize}`);
      res.end();
      return;
    }

    let start = match[1] ? parseInt(match[1], 10) : 0;
    let end = match[2] ? parseInt(match[2], 10) : totalSize - 1;

    if (!Number.isFinite(start) || start < 0) start = 0;
    if (!Number.isFinite(end) || end >= totalSize) end = totalSize - 1;

    if (start > end || start >= totalSize) {
      res.status(416).setHeader("Content-Range", `bytes */${totalSize}`);
      res.end();
      return;
    }

    const chunkSize = end - start + 1;
    res.status(206);
    res.setHeader("Content-Range", `bytes ${start}-${end}/${totalSize}`);
    res.setHeader("Content-Length", chunkSize);

    const stream = fs.createReadStream(localVideoPath, { start, end });
    stream.on("error", (err) => {
      console.error("[VIDEO] Błąd streamu range:", err);
      if (!res.headersSent) {
        res.status(500).json({ error: "stream_error" });
      } else {
        res.end();
      }
    });
    stream.pipe(res);
  } catch (err) {
    console.error("[VIDEO] Błąd endpointu /videos/:videoKey:", err);
    res.status(500).json({ error: "internal_error" });
  }
});

// Health check endpoint
app.get('/', (req, res) => {
  const status = {
    status: 'alive',
    timestamp: new Date().toISOString(),
    discord_status: client.isReady() ? 'connected' : 'disconnected',
    uptime: client.uptime ? Math.floor(client.uptime / 1000) : 0,
    guilds: client.isReady() ? client.guilds.cache.size : 0,
    bot_tag: client.user ? client.user.tag : 'Not connected',
    ready: client.isReady()
  };

  // Sprawdź czy request chce JSON czy HTML
  if (req.headers.accept && req.headers.accept.includes('application/json')) {
    res.json(status, null, 2);
  } else {
    // Formatowanie HTML dla lepszej czytelności
    res.send(`
      <h1>🤖 Bot Status Monitor</h1>
      <pre>${JSON.stringify(status, null, 2)}</pre>
      <hr>
      <p><strong>Health Check:</strong> <a href="/health">/health</a></p>
      <p><strong>Timestamp:</strong> ${new Date().toLocaleString()}</p>
    `);
  }
});

app.get('/health', (req, res) => {
  const isHealthy = client.isReady();
  const status = {
    status: isHealthy ? 'healthy' : 'unhealthy',
    discord_connected: isHealthy,
    timestamp: new Date().toISOString(),
    uptime: client.uptime ? Math.floor(client.uptime / 1000) : 0,
    guilds: client.isReady() ? client.guilds.cache.size : 0
  };

  res.status(200).json(status, null, 2);
});
