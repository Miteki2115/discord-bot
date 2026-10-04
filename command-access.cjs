const SELLER_ROLE_ID = "1350786945944391733";
const SUSPENDED_ROLE_ID = "1537090439239442483";
const SELLER_COMMANDS = new Set([
  "ticket-zakoncz", "wydane", "ustawienia", "anonim",
  "zamknij-z-powodem", "zamknij", "dodaj", "przejmij", "odprzejmij",
  "znajdz-ticket", "rozliczenie", "wezwij", "help", "ostrzezenia", "warns",
]);

function isGuildOwner(interaction) {
  return Boolean(interaction.guild?.ownerId && interaction.user?.id === interaction.guild.ownerId);
}

function isActiveSeller(interaction) {
  const roles = interaction.member?.roles;
  const has = (id) => Boolean(roles?.cache?.has(id) || (Array.isArray(roles) && roles.includes(id)));
  return has(SELLER_ROLE_ID) && !has(SUSPENDED_ROLE_ID);
}

function canUseSlashCommand(interaction) {
  if (!interaction.guildId) return false;
  if (isGuildOwner(interaction)) return true;
  if (interaction.commandName === "znizka") return true;
  if (!SELLER_COMMANDS.has(interaction.commandName) || !isActiveSeller(interaction)) return false;
  if (interaction.commandName === "wezwij" && interaction.options.getUser("uzytkownik")) return false;
  return true;
}

function applyCommandVisibility(command) {
  return { ...command, default_member_permissions: command.name === "znizka" ? null : "0", dm_permission: false };
}

module.exports = { SELLER_ROLE_ID, SELLER_COMMANDS, isGuildOwner, isActiveSeller, canUseSlashCommand, applyCommandVisibility };
