const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

function getCurrentWeekStartString() {
  const now = new Date();
  const dayOfWeek = now.getDay();
  const diff = now.getDate() - dayOfWeek;
  const weekStart = new Date(now.setDate(diff));
  weekStart.setHours(0, 0, 0, 0);
  return weekStart.toISOString().split("T")[0];
}

// Weekly sales functions
async function saveWeeklySale(
  userId,
  amount,
  guildId = "default",
  paid = false,
  paidAt = null,
  lastUpdate = Date.now()
) {
  // Pobierz początek tygodnia (niedziela)
  const weekStart = getCurrentWeekStartString();
  
  const { error } = await supabase
    .from("weekly_sales")
    .upsert({ 
      user_id: userId, 
      guild_id: guildId,
      amount,
      paid,
      paid_at: paidAt ? new Date(paidAt).toISOString() : null,
      updated_at: new Date(lastUpdate).toISOString(),
      week_start: weekStart
    });
  if (error) console.error("[Supabase] Błąd zapisu weekly_sales:", error);
  else console.log(`[Supabase] Zapisano weekly_sales: ${guildId}/${userId} -> ${amount} (paid=${paid})`);
}

async function getWeeklySales(guildId = null) {
  // Pobierz początek aktualnego tygodnia (niedziela)
  const weekStartStr = getCurrentWeekStartString();
  
  let query = supabase
    .from("weekly_sales")
    .select("*")
    .eq("week_start", weekStartStr); // Tylko aktualny tydzień
    
  if (guildId && guildId !== "default") {
    query = query.eq("guild_id", guildId);
  }
  
  const { data, error } = await query;
  if (error) {
    console.error("[Supabase] Błąd odczytu weekly_sales:", error);
    return [];
  }
  return data;
}

async function resetWeeklySales(guildId = null) {
  const weekStartStr = getCurrentWeekStartString();

  let query = supabase
    .from("weekly_sales")
    .delete()
    .eq("week_start", weekStartStr);

  if (guildId && guildId !== "default") {
    query = query.eq("guild_id", guildId);
  }

  const { error } = await query;
  if (error) {
    console.error("[Supabase] BÅ‚Ä…d resetowania weekly_sales:", error);
    return false;
  }

  console.log(
    `[Supabase] Zresetowano weekly_sales dla tygodnia ${weekStartStr}${guildId && guildId !== "default" ? ` w guild ${guildId}` : ""}`
  );
  return true;
}

// Invite counts functions
async function saveInviteCount(guildId, userId, count) {
  const { error } = await supabase
    .from("invite_counts")
    .upsert({ 
      guild_id: guildId,
      user_id: userId,
      count,
      updated_at: new Date().toISOString()
    });
  if (error) console.error("[Supabase] Błąd zapisu invite_counts:", error);
  else console.log(`[Supabase] Zapisano invite_counts: ${guildId}/${userId} -> ${count}`);
}

async function getInviteCounts(guildId) {
  const { data, error } = await supabase
    .from("invite_counts")
    .select("*")
    .eq("guild_id", guildId);
  if (error) {
    console.error("[Supabase] Błąd odczytu invite_counts:", error);
    return [];
  }
  return data;
}

// Ticket functions
async function saveTicketOwner(channelId, ticketData) {
  const { error } = await supabase
    .from("ticket_owners")
    .upsert({ 
      channel_id: channelId,
      ...ticketData,
      updated_at: new Date().toISOString()
    });
  if (error) console.error("[Supabase] Błąd zapisu ticket_owners:", error);
  else console.log(`[Supabase] Zapisano ticket_owners: ${channelId}`);
}

async function getTicketOwners() {
  const { data, error } = await supabase.from("ticket_owners").select("*");
  if (error) {
    console.error("[Supabase] Błąd odczytu ticket_owners:", error);
    return [];
  }
  return data;
}

// Active codes functions
async function saveActiveCode(code, codeData) {
  const { error } = await supabase
    .from("active_codes")
    .upsert({ 
      code,
      ...codeData,
      updated_at: new Date().toISOString()
    });
  if (error) console.error("[Supabase] Błąd zapisu active_codes:", error);
  else console.log(`[Supabase] Zapisano active_codes: ${code}`);
}

async function getActiveCodes() {
  const { data, error } = await supabase.from("active_codes").select("*");
  if (error) {
    console.error("[Supabase] Błąd odczytu active_codes:", error);
    return [];
  }
  return data;
}

// Contest functions
async function saveContest(messageId, contestData) {
  const { error } = await supabase
    .from("contests")
    .upsert({ 
      message_id: messageId,
      ...contestData,
      updated_at: new Date().toISOString()
    });
  if (error) console.error("[Supabase] Błąd zapisu contests:", error);
  else console.log(`[Supabase] Zapisano contests: ${messageId}`);
}

async function getContests() {
  const { data, error } = await supabase.from("contests").select("*");
  if (error) {
    console.error("[Supabase] Błąd odczytu contests:", error);
    return [];
  }
  return data;
}

// Contest participants functions
async function saveContestParticipant(messageId, userId) {
  const { error } = await supabase
    .from("contest_participants")
    .upsert({ 
      message_id: messageId,
      user_id: userId,
      joined_at: new Date().toISOString()
    });
  if (error) console.error("[Supabase] Błąd zapisu contest_participants:", error);
  else console.log(`[Supabase] Zapisano contest_participants: ${messageId}/${userId}`);
}

async function getContestParticipants(messageId) {
  const { data, error } = await supabase
    .from("contest_participants")
    .select("*")
    .eq("message_id", messageId);
  if (error) {
    console.error("[Supabase] Błąd odczytu contest_participants:", error);
    return [];
  }
  return data;
}

// Active codes functions
async function saveActiveCode(code, codeData) {
  const { error } = await supabase
    .from("active_codes")
    .upsert({ 
      code,
      user_id: codeData.oderId || codeData.user_id,
      discount: codeData.discount || 0,
      expires_at: new Date(codeData.expiresAt).toISOString(),
      used: codeData.used || false,
      reward: codeData.reward,
      reward_amount: codeData.rewardAmount,
      reward_text: codeData.rewardText,
      type: codeData.type,
      updated_at: new Date().toISOString()
    });
  if (error) console.error("[Supabase] Błąd zapisu active_codes:", error);
  else console.log(`[Supabase] Zapisano active_code: ${code}`);
}

async function getActiveCodes() {
  const { data, error } = await supabase.from("active_codes").select("*");
  if (error) {
    console.error("[Supabase] Błąd odczytu active_codes:", error);
    return [];
  }
  return data;
}

async function getActiveCode(code) {
  const { data, error } = await supabase
    .from("active_codes")
    .select("*")
    .eq("code", code)
    .limit(1)
    .maybeSingle();
  if (error) {
    console.error("[Supabase] Błąd odczytu active_code:", error);
    return null;
  }
  return data || null;
}

async function updateActiveCode(code, updates) {
  const { error } = await supabase
    .from("active_codes")
    .update({ ...updates, updated_at: new Date().toISOString() })
    .eq("code", code);
  if (error) console.error("[Supabase] Błąd aktualizacji active_codes:", error);
  else console.log(`[Supabase] Zaktualizowano active_code: ${code}`);
}

async function deleteActiveCode(code) {
  const { error } = await supabase
    .from("active_codes")
    .delete()
    .eq("code", code);
  if (error) console.error("[Supabase] Błąd usuwania active_codes:", error);
  else console.log(`[Supabase] Usunięto active_code: ${code}`);
}

// Ticket owners functions
async function saveTicketOwner(channelId, ticketData) {
  const { error } = await supabase
    .from("ticket_owners")
    .upsert({ 
      channel_id: channelId,
      user_id: ticketData.userId,
      claimed_by: ticketData.claimedBy,
      locked: ticketData.locked || false,
      ticket_message_id: ticketData.ticketMessageId,
      updated_at: new Date().toISOString()
    });
  if (error) console.error("[Supabase] Błąd zapisu ticket_owners:", error);
  else console.log(`[Supabase] Zapisano ticket_owner: ${channelId}`);
}

async function getTicketOwners() {
  const { data, error } = await supabase.from("ticket_owners").select("*");
  if (error) {
    console.error("[Supabase] Błąd odczytu ticket_owners:", error);
    return {};
  }
  const result = {};
  data.forEach(item => {
    result[item.channel_id] = {
      claimedBy: item.claimed_by,
      userId: item.user_id,
      locked: item.locked,
      ticketMessageId: item.ticket_message_id
    };
  });
  return result;
}

async function deleteTicketOwner(channelId) {
  const { error } = await supabase
    .from("ticket_owners")
    .delete()
    .eq("channel_id", channelId);
  if (error) console.error("[Supabase] Błąd usuwania ticket_owners:", error);
  else console.log(`[Supabase] Usunięto ticket_owner: ${channelId}`);
}

// Invite counts functions
async function saveInviteCount(guildId, inviterId, count) {
  const now = new Date();
  const dayOfWeek = now.getDay(); // 0 = niedziela
  const diff = now.getDate() - dayOfWeek;
  const weekStart = new Date(now.setDate(diff));
  weekStart.setHours(0, 0, 0, 0);
  const weekStartStr = weekStart.toISOString().split('T')[0]; // YYYY-MM-DD
  
  const { error } = await supabase
    .from("invite_counts")
    .upsert({ 
      guild_id: guildId,
      inviter_id: inviterId,
      count: count,
      week_start: weekStartStr
    });
  if (error) console.error("[Supabase] Błąd zapisu invite_counts:", error);
  else console.log(`[Supabase] Zapisano invite_counts: ${guildId}/${inviterId} -> ${count}`);
}

async function getInviteCounts(guildId) {
  const now = new Date();
  const dayOfWeek = now.getDay(); // 0 = niedziela
  const diff = now.getDate() - dayOfWeek;
  const weekStart = new Date(now.setDate(diff));
  weekStart.setHours(0, 0, 0, 0);
  const weekStartStr = weekStart.toISOString().split('T')[0]; // YYYY-MM-DD
  
  const { data, error } = await supabase
    .from("invite_counts")
    .select("*")
    .eq("guild_id", guildId)
    .eq("week_start", weekStartStr);
  if (error) {
    console.error("[Supabase] Błąd odczytu invite_counts:", error);
    return {};
  }
  const result = {};
  data.forEach(item => {
    result[item.inviter_id] = item.count;
  });
  return result;
}

// Invite total joined functions
async function saveInviteTotalJoined(guildId, inviterId, count) {
  const now = new Date();
  const dayOfWeek = now.getDay(); // 0 = niedziela
  const diff = now.getDate() - dayOfWeek;
  const weekStart = new Date(now.setDate(diff));
  weekStart.setHours(0, 0, 0, 0);
  const weekStartStr = weekStart.toISOString().split('T')[0]; // YYYY-MM-DD
  
  const { error } = await supabase
    .from("invite_total_joined")
    .upsert({ 
      guild_id: guildId,
      inviter_id: inviterId,
      count: count,
      week_start: weekStartStr
    });
  if (error) console.error("[Supabase] Błąd zapisu invite_total_joined:", error);
  else console.log(`[Supabase] Zapisano invite_total_joined: ${guildId}/${inviterId} -> ${count}`);
}

async function getInviteTotalJoined(guildId) {
  const now = new Date();
  const dayOfWeek = now.getDay(); // 0 = niedziela
  const diff = now.getDate() - dayOfWeek;
  const weekStart = new Date(now.setDate(diff));
  weekStart.setHours(0, 0, 0, 0);
  const weekStartStr = weekStart.toISOString().split('T')[0]; // YYYY-MM-DD
  
  const { data, error } = await supabase
    .from("invite_total_joined")
    .select("*")
    .eq("guild_id", guildId)
    .eq("week_start", weekStartStr);
  if (error) {
    console.error("[Supabase] Błąd odczytu invite_total_joined:", error);
    return {};
  }
  const result = {};
  data.forEach(item => {
    result[item.inviter_id] = item.count;
  });
  return result;
}

// Invite fake accounts functions
async function saveInviteFakeAccounts(guildId, inviterId, count) {
  const now = new Date();
  const dayOfWeek = now.getDay(); // 0 = niedziela
  const diff = now.getDate() - dayOfWeek;
  const weekStart = new Date(now.setDate(diff));
  weekStart.setHours(0, 0, 0, 0);
  const weekStartStr = weekStart.toISOString().split('T')[0]; // YYYY-MM-DD
  
  const { error } = await supabase
    .from("invite_fake_accounts")
    .upsert({ 
      guild_id: guildId,
      inviter_id: inviterId,
      count: count,
      week_start: weekStartStr
    });
  if (error) console.error("[Supabase] Błąd zapisu invite_fake_accounts:", error);
  else console.log(`[Supabase] Zapisano invite_fake_accounts: ${guildId}/${inviterId} -> ${count}`);
}

async function getInviteFakeAccounts(guildId) {
  const now = new Date();
  const dayOfWeek = now.getDay(); // 0 = niedziela
  const diff = now.getDate() - dayOfWeek;
  const weekStart = new Date(now.setDate(diff));
  weekStart.setHours(0, 0, 0, 0);
  const weekStartStr = weekStart.toISOString().split('T')[0]; // YYYY-MM-DD
  
  const { data, error } = await supabase
    .from("invite_fake_accounts")
    .select("*")
    .eq("guild_id", guildId)
    .eq("week_start", weekStartStr);
  if (error) {
    console.error("[Supabase] Błąd odczytu invite_fake_accounts:", error);
    return {};
  }
  const result = {};
  data.forEach(item => {
    result[item.inviter_id] = item.count;
  });
  return result;
}

// Invite bonus invites functions
async function saveInviteBonusInvites(guildId, userId, bonus) {
  const { error } = await supabase
    .from("invite_bonus_invites")
    .upsert({ 
      guild_id: guildId,
      user_id: userId,
      bonus: bonus
    });
  if (error) console.error("[Supabase] Błąd zapisu invite_bonus_invites:", error);
  else console.log(`[Supabase] Zapisano invite_bonus_invites: ${guildId}/${userId} -> ${bonus}`);
}

async function getInviteBonusInvites(guildId) {
  const { data, error } = await supabase
    .from("invite_bonus_invites")
    .select("*")
    .eq("guild_id", guildId);
  if (error) {
    console.error("[Supabase] Błąd odczytu invite_bonus_invites:", error);
    return {};
  }
  const result = {};
  data.forEach(item => {
    result[item.user_id] = item.bonus;
  });
  return result;
}

// Invite rewards given functions
async function saveInviteRewardsGiven(guildId, userId, rewardsCount) {
  const { error } = await supabase
    .from("invite_rewards_given")
    .upsert({ 
      guild_id: guildId,
      user_id: userId,
      rewards_count: rewardsCount
    });
  if (error) console.error("[Supabase] Błąd zapisu invite_rewards_given:", error);
  else console.log(`[Supabase] Zapisano invite_rewards_given: ${guildId}/${userId} -> ${rewardsCount}`);
}

async function getInviteRewardsGiven(guildId) {
  const { data, error } = await supabase
    .from("invite_rewards_given")
    .select("*")
    .eq("guild_id", guildId);
  if (error) {
    console.error("[Supabase] Błąd odczytu invite_rewards_given:", error);
    return {};
  }
  const result = {};
  data.forEach(item => {
    result[item.user_id] = item.rewards_count;
  });
  return result;
}

// Invite reward levels functions
async function saveInviteRewardLevels(guildId, userId, levels) {
  const { error } = await supabase
    .from("invite_reward_levels")
    .upsert({ 
      guild_id: guildId,
      user_id: userId,
      reward_levels: levels
    });
  if (error) console.error("[Supabase] Błąd zapisu invite_reward_levels:", error);
  else console.log(`[Supabase] Zapisano invite_reward_levels: ${guildId}/${userId} -> ${JSON.stringify(levels)}`);
}

async function getInviteRewardLevels(guildId) {
  const { data, error } = await supabase
    .from("invite_reward_levels")
    .select("*")
    .eq("guild_id", guildId);
  if (error) {
    console.error("[Supabase] Błąd odczytu invite_reward_levels:", error);
    return {};
  }
  const result = {};
  data.forEach(item => {
    result[item.user_id] = new Set(item.reward_levels || []);
  });
  return result;
}

// Get invited users by inviter
async function getInvitedUsersByInviter(guildId, inviterId) {
  const { data, error } = await supabase
    .from("invites")
    .select("*")
    .eq("guild_id", guildId)
    .eq("inviter_id", inviterId)
    .eq("status", "joined"); // Tylko osoby które dołączyły
  
  if (error) {
    console.error("[Supabase] Błąd odczytu invites:", error);
    return [];
  }
  return data;
}

// User spent tracking functions
async function addUserSpent(userId, amount, guildId = "default") {
  // First, get the current spent amount
  const { data, error: fetchError } = await supabase
    .from("user_spent")
    .select("amount")
    .eq("user_id", userId)
    .eq("guild_id", guildId)
    .maybeSingle();

  let currentAmount = 0;
  if (data) {
    currentAmount = Number(data.amount) || 0;
  }

  const newAmount = currentAmount + amount;

  const { error } = await supabase
    .from("user_spent")
    .upsert({
      user_id: userId,
      guild_id: guildId,
      amount: newAmount,
      updated_at: new Date().toISOString()
    });

  if (error) {
    console.error("[Supabase] Błąd zapisu user_spent:", error);
  } else {
    console.log(`[Supabase] Zaktualizowano user_spent dla ${userId}: +${amount} PLN (Razem: ${newAmount} PLN)`);
  }
}

async function getUserSpent(userId, guildId = "default", options = {}) {
  let query = supabase
    .from("user_spent")
    .select("amount")
    .eq("user_id", userId)
    .eq("guild_id", guildId)
    .maybeSingle();

  if (options.signal) query = query.abortSignal(options.signal);
  const { data, error } = await query;

  if (error) {
    console.error("[Supabase] Błąd odczytu user_spent:", error);
    return 0;
  }
  return data ? Number(data.amount) || 0 : 0;
}

async function getTopSpenders(limit = 10, guildId = "default") {
  const { data, error } = await supabase
    .from("user_spent")
    .select("user_id, amount")
    .eq("guild_id", guildId)
    .order("amount", { ascending: false })
    .limit(limit);

  if (error) {
    console.error("[Supabase] Błąd odczytu top spenders:", error);
    return [];
  }
  return data || [];
}

async function setUserSpent(userId, amount, guildId = "default") {
  const { error } = await supabase
    .from("user_spent")
    .upsert({
      user_id: userId,
      guild_id: guildId,
      amount: amount,
      updated_at: new Date().toISOString()
    });

  if (error) {
    console.error("[Supabase] Błąd ustawiania user_spent:", error);
    return false;
  }
  return true;
}

async function deleteUserSpent(userId, guildId = "default") {
  const { error } = await supabase
    .from("user_spent")
    .delete()
    .eq("user_id", userId)
    .eq("guild_id", guildId);

  if (error) {
    console.error("[Supabase] Błąd usuwania user_spent:", error);
    return false;
  }
  return true;
}

async function addUserPurchase(userId, amount, server, type, guildId = "default") {
  const { error } = await supabase
    .from("user_purchases")
    .insert({
      user_id: userId,
      guild_id: guildId,
      price: amount,
      server: server,
      type: type,
      created_at: new Date().toISOString()
    });

  if (error) {
    console.error("[Supabase] Błąd zapisu user_purchases:", error);
    return false;
  }
  return true;
}

async function deleteUserPurchase(purchaseId, guildId = "default") {
  const { error } = await supabase
    .from("user_purchases")
    .delete()
    .eq("id", purchaseId)
    .eq("guild_id", guildId);

  if (error) {
    console.error("[Supabase] Błąd usuwania user_purchases:", error);
    return false;
  }
  return true;
}

async function deleteUserPurchases(userId, guildId = "default", type = "zakup") {
  const { error } = await supabase
    .from("user_purchases")
    .delete()
    .eq("user_id", userId)
    .eq("guild_id", guildId)
    .eq("type", type);

  if (error) {
    console.error("[Supabase] Błąd usuwania historii zakupów użytkownika:", error);
    return false;
  }
  return true;
}

async function getUserPurchases(userId, guildId = "default") {
  const { data, error } = await supabase
    .from("user_purchases")
    .select("*")
    .eq("user_id", userId)
    .eq("guild_id", guildId)
    .order("created_at", { ascending: false });

  if (error) {
    console.error("[Supabase] Błąd odczytu user_purchases:", error);
    return [];
  }
  return data || [];
}

async function getShopTotals(guildId = "default") {
  const pageSize = 1000;

  async function fetchAllRows(table, columns) {
    const rows = [];
    for (let from = 0; ; from += pageSize) {
      const { data, error } = await supabase
        .from(table)
        .select(columns)
        .eq("guild_id", guildId)
        .range(from, from + pageSize - 1);

      if (error) throw error;
      rows.push(...(data || []));
      if (!data || data.length < pageSize) break;
    }
    return rows;
  }

  const [spentRows, transactionRows] = await Promise.all([
    fetchAllRows("user_spent", "user_id,amount"),
    fetchAllRows("user_purchases", "user_id,price,type"),
  ]);

  const customerSpent = spentRows.reduce(
    (sum, row) => sum + (Number(row.amount) || 0),
    0,
  );
  const totals = {
    customerSpent,
    // user_spent jest źródłem prawdy dla aktualnych zakupów. Stara historia
    // user_purchases może zawierać wpisy sprzed resetów wydatków.
    purchaseAmount: customerSpent,
    saleAmount: 0,
    saleCount: 0,
  };

  for (const row of transactionRows) {
    const amount = Math.max(0, Number(row.price) || 0);
    const type = String(row.type || "").trim().toLowerCase();
    if (type === "sprzedaz" || type === "sprzedaż") {
      totals.saleAmount += amount;
      totals.saleCount += 1;
    }
  }

  totals.transactionAmount = totals.purchaseAmount + totals.saleAmount;
  return totals;
}

module.exports = {
  saveWeeklySale,
  getWeeklySales,
  resetWeeklySales,
  saveInviteCount,
  getInviteCounts,
  saveInviteTotalJoined,
  getInviteTotalJoined,
  saveInviteFakeAccounts,
  getInviteFakeAccounts,
  saveInviteBonusInvites,
  getInviteBonusInvites,
  saveInviteRewardsGiven,
  getInviteRewardsGiven,
  saveInviteRewardLevels,
  getInviteRewardLevels,
  saveTicketOwner,
  getTicketOwners,
  deleteTicketOwner,
  saveActiveCode,
  getActiveCode,
  getActiveCodes,
  updateActiveCode,
  deleteActiveCode,
  getInvitedUsersByInviter,
  saveContest,
  getContests,
  saveContestParticipant,
  getContestParticipants,
  addUserSpent,
  getUserSpent,
  getTopSpenders,
  setUserSpent,
  deleteUserSpent,
  addUserPurchase,
  getUserPurchases,
  deleteUserPurchase,
  deleteUserPurchases,
  getShopTotals,
  supabase
};
