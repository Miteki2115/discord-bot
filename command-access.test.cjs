const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const { SELLER_ROLE_ID, SELLER_COMMANDS, isGuildOwner, isActiveSeller, canUseSlashCommand, applyCommandVisibility } = require("./command-access.cjs");
const source = fs.readFileSync(require.resolve("./index.cjs"), "utf8");
const names = [...source.slice(source.indexOf("const commands = ["), source.indexOf("const rest = new REST")).matchAll(/^    \.setName\("([^"]+)"\)/gm)].map(m => m[1]);
function interaction(commandName, roles = [], owner = false, target = null) {
  return { commandName, guildId: "guild", guild: { ownerId: "owner" }, user: { id: owner ? "owner" : "member" },
    member: { roles: { cache: new Set(roles) }, permissions: { has: () => true } },
    options: { getUser: () => target } };
}

test("client and non-owner administrator can use only znizka", () => {
  assert(names.length > 50);
  for (const name of [...names, "zaproszenia-edytuj"]) {
    assert.equal(canUseSlashCommand(interaction(name)), name === "znizka", name);
    assert.equal(canUseSlashCommand(interaction(name, ["1519069239254974475"])), name === "znizka", `helper ${name}`);
  }
});
test("active seller has only listed commands; owner has every registered command", () => {
  for (const name of [...names, "zaproszenia-edytuj"]) {
    assert.equal(canUseSlashCommand(interaction(name, [SELLER_ROLE_ID])), name === "znizka" || SELLER_COMMANDS.has(name), name);
    assert.equal(canUseSlashCommand(interaction(name, [], true)), true, name);
  }
  for (const name of SELLER_COMMANDS) assert(names.includes(name), `Registered seller command ${name}`);
  assert(canUseSlashCommand(interaction("dodaj", [SELLER_ROLE_ID])));
});
test("suspended sellers and DMs are denied; owner exemption remains", () => {
  for (const name of SELLER_COMMANDS) assert.equal(canUseSlashCommand(interaction(name, [SELLER_ROLE_ID, "1537090439239442483"])), false);
  assert(canUseSlashCommand(interaction("znizka", ["1537090439239442483"])));
  assert(canUseSlashCommand(interaction("dodaj", ["1537090439239442483"], true)));
  const dm = interaction("znizka"); dm.guildId = null; dm.guild = null;
  assert.equal(canUseSlashCommand(dm), false);
});
test("wezwij target is owner-only, no target permits seller", () => {
  assert(canUseSlashCommand(interaction("wezwij", [SELLER_ROLE_ID])));
  assert.equal(canUseSlashCommand(interaction("wezwij", [SELLER_ROLE_ID], false, { id: "target" })), false);
  assert(canUseSlashCommand(interaction("wezwij", [], true, { id: "target" })));
});
test("default visibility exposes only znizka and disables DMs", () => {
  for (const name of names) {
    const command = applyCommandVisibility({ name });
    assert.equal(command.default_member_permissions, name === "znizka" ? null : "0");
    assert.equal(command.dm_permission, false);
  }
  assert(source.includes("].map(applyCommandVisibility)"));
  assert(source.includes("if (!canUseSlashCommand(interaction))"));
});
test("direct wezwij handler blocks target before channel edit or DM", async () => {
  const fn = source.slice(source.indexOf("async function handleWezwijCommand("), source.indexOf("async function handleWezwijCommand(") + source.slice(source.indexOf("async function handleWezwijCommand(")).indexOf("\nasync function ", 10));
  const context = vm.createContext({ isGuildOwner, isActiveSeller, ChannelType: { GuildText: 0 }, isTicketChannel: () => true, MessageFlags: { Ephemeral: 64 } });
  vm.runInContext(fn, context);
  const i = interaction("wezwij", [SELLER_ROLE_ID], false, { id: "target", send: () => assert.fail("Unexpected DM") });
  i.channel = { type: 0, permissionOverwrites: { edit: () => assert.fail("Unexpected channel access") } };
  let denied = false; i.reply = async () => { denied = true; };
  await context.handleWezwijCommand(i); assert(denied);
});
