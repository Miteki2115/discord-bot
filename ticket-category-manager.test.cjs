const test = require("node:test");
const assert = require("node:assert/strict");
const { createTicketCategoryManager, DEFINITIONS } = require("./ticket-category-manager.cjs");

function fixture() {
  const cache = new Map(), server = new Map(), categoryMap = new Map(), templates = new Map();
  const actions = []; let serial = 0, saveOk = true, fetchOk = true, failTicket = false;
  const guild = { id: "guild", members: { me: { id: "bot" } }, channels: {
    cache,
    fetch: async () => { if (!fetchOk) throw Error("fetch failed"); cache.clear(); for (const [id,c] of server) cache.set(id,c); return cache; },
    create: async options => {
      if (options.type === 0 && failTicket) throw Error("ticket failed");
      return add(`new-${++serial}`, options.name, options.type, options.parent, options);
    },
  } };
  function add(id, name, type = 4, parentId = null, options = {}) {
    const c = { id, name, type, parentId, guild, rawPosition: options.position ?? 5,
      permissionOverwrites: { cache: new Map((options.permissionOverwrites || []).map(o => [o.id, { ...o, type:o.type??0, allow:{bitfield:BigInt(typeof o.allow === "string" ? o.allow : 0)},deny:{bitfield:BigInt(typeof o.deny === "string" ? o.deny : 0)} }])) },
      delete: async () => { assert(![...server.values()].some(child => child.parentId === id)); actions.push(["delete",id]); server.delete(id); cache.delete(id); },
      setParent: async (id,opts) => { assert.equal(opts.lockPermissions,false); assert(server.has(id)); c.parentId=id; actions.push(["move",c.id,id]); },
    };
    server.set(id,c); cache.set(id,c); actions.push(["create",id,type]); return c;
  }
  const manager = createTicketCategoryManager({ categoryMap, templates, channelType:{GuildCategory:4}, flags:{ViewChannel:1024n,ManageChannels:16n}, persist:async()=>{actions.push(["save"]);return saveOk;} });
  return { guild, manager, add, actions, templates, categoryMap, server, cache, saveFail:()=>{saveOk=false;}, fetchFail:()=>{fetchOk=false;}, ticketFail:()=>{failTicket=true;} };
}

test("existing empty ticket categories removed, unrelated and occupied categories retained", async()=>{
  const f=fixture(); f.add("empty",DEFINITIONS["zakup-0-20"][0]); f.add("busy",DEFINITIONS.inne[0]); f.add("child","user-ticket",0,"busy"); f.add("other","Other category");
  await f.manager.cleanup(f.guild);
  assert(!f.server.has("empty"));assert(f.server.has("busy"));assert(f.server.has("other"));
  assert(f.actions.findIndex(a=>a[0]==="save")<f.actions.findIndex(a=>a[0]==="delete"));
});
test("creation lazily recreates category and preserves saved name, permissions and position", async()=>{
  const f=fixture(); f.add(DEFINITIONS["zakup-20-50"][1],"Custom shop category",4,null,{position:9,permissionOverwrites:[{id:"seller",type:0,allow:"1024",deny:"0"}]});
  await f.manager.cleanup(f.guild);
  const c=await f.manager.create(f.guild,"zakup-20-50",{name:"ticket",type:0,permissionOverwrites:[{id:"buyer",allow:"1024",deny:"0"}]});
  const parent=f.server.get(c.parentId); assert.equal(parent.name,"Custom shop category");assert.equal(parent.rawPosition,9);assert.equal(parent.permissionOverwrites.cache.get("seller").allow.bitfield,1024n);
  assert(c.permissionOverwrites.cache.has("buyer"));assert.equal(f.manager.keyFor(f.guild,c.parentId),"zakup-20-50");
});
test("concurrent ticket creation uses one category", async()=>{
  const f=fixture(); const tickets=await Promise.all([1,2,3].map(n=>f.manager.create(f.guild,"inne",{name:`ticket-${n}`,type:0})));
  assert.equal(new Set(tickets.map(c=>c.parentId)).size,1);assert.equal([...f.server.values()].filter(c=>c.type===4).length,1);
});
test("claim and release recreate removed categories with new IDs and retained access", async()=>{
  const f=fixture(); const c=await f.manager.create(f.guild,"zakup-50-100",{name:"ticket",type:0,permissionOverwrites:[{id:"buyer",allow:"1024",deny:"0"}]});
  const originalId=c.parentId; const key=f.manager.keyFor(f.guild,originalId);
  await f.manager.move(c,"przejete");assert(!f.server.has(originalId));const claimed=c.parentId;
  assert.equal(f.manager.keyFor(f.guild,originalId),key);
  await f.manager.move(c,key);assert(!f.server.has(claimed));assert.notEqual(c.parentId,originalId);assert.equal(f.manager.keyFor(f.guild,c.parentId),key);assert(c.permissionOverwrites.cache.has("buyer"));
});
test("fresh server fetch prevents deletion when cache misses a child", async()=>{
  const f=fixture(); f.add("category",DEFINITIONS.inne[0]);f.add("child","ticket",0,"category");f.cache.delete("child");
  await f.manager.cleanup(f.guild);assert(f.server.has("category"));
});
test("failed database save or failed fetch prevents category deletion", async()=>{
  const f=fixture();f.add("category",DEFINITIONS.inne[0]);f.saveFail();await f.manager.cleanup(f.guild);assert(f.server.has("category"));
  f.fetchFail();await assert.rejects(f.manager.cleanup(f.guild));assert(f.server.has("category"));
});
test("last deleted ticket removes parent; a voice channel still keeps its category", async()=>{
  const f=fixture();const c=await f.manager.create(f.guild,"sprzedaz",{name:"ticket",type:0});const id=c.parentId;
  await c.delete();await f.manager.cleanup(f.guild,id);assert(!f.server.has(id));
  f.add("other",DEFINITIONS.inne[0]);f.add("voice","voice",2,"other");await f.manager.cleanup(f.guild);assert(f.server.has("other"));
});
test("restart retains mappings/templates and special purchase aliases",async()=>{
  const f=fixture();const c=await f.manager.create(f.guild,"zakup-mody",{name:"ticket",type:0});assert.equal(f.manager.keyFor(f.guild,c.parentId),"boty-mody");
  const persisted=JSON.parse(JSON.stringify(Object.fromEntries(f.templates)));
  const map=JSON.parse(JSON.stringify(Object.fromEntries(f.categoryMap)));
  const restored=createTicketCategoryManager({categoryMap:new Map(Object.entries(map)),templates:new Map(Object.entries(persisted)),channelType:{GuildCategory:4},flags:{ViewChannel:1024n,ManageChannels:16n},persist:async()=>true});
  await c.delete();await restored.cleanup(f.guild,c.parentId);
  const next=await restored.create(f.guild,"zakup-autorynku",{name:"ticket",type:0});assert.equal(restored.keyFor(f.guild,next.parentId),"boty-mody");
});
test("failed ticket creation cleans up newly empty category",async()=>{
  const f=fixture();f.ticketFail();await assert.rejects(f.manager.create(f.guild,"inne",{name:"ticket",type:0}));assert.equal(f.server.size,0);
});
test("server collection, not stale cache, determines whether category exists or is empty", async()=>{
  const f=fixture();f.add("old",DEFINITIONS.inne[0]);f.add("gone","deleted child",0,"old");f.server.delete("gone");
  // discord.js fetch adds returned channels but does not purge old cache entries.
  f.guild.channels.fetch=async()=>new Map(f.server);
  await f.manager.cleanup(f.guild,"old");assert(!f.server.has("old"));
  const stale=f.add("stale",DEFINITIONS.inne[0]);f.server.delete(stale.id);
  const c=await f.manager.create(f.guild,"inne",{name:"ticket",type:0});
  assert.notEqual(c.parentId,"stale");assert(f.server.has(c.parentId));
});
test("creation racing cleanup never leaves an orphaned ticket",async()=>{
  const f=fixture();f.add("empty",DEFINITIONS.inne[0]);
  const [c]=await Promise.all([f.manager.create(f.guild,"inne",{name:"ticket",type:0}),f.manager.cleanup(f.guild)]);
  assert(f.server.has(c.parentId));
});
test("new category IDs preserve purchase seller limits and private help tickets",()=>{
  const fs=require("node:fs"),vm=require("node:vm");
  const source=fs.readFileSync(require.resolve("./index.cjs"),"utf8");
  function extract(name) { const start=source.indexOf(`function ${name}(`);return source.slice(start,start+source.slice(start).search(/\n}\r?\n/)+2); }
  const roles=["1449448705563557918","1449448702925209651","1449448686156255333","1449448860517798061","1541553589833695393","1541553994000891984"];
  const ctx=require("node:vm").createContext({PURCHASE_STAFF_ROLE_IDS:roles,PermissionsBitField:{Flags:{ViewChannel:1,SendMessages:2,ReadMessageHistory:4}},ticketCategoryManager:{keyFor:(_,id)=>id.slice(4)}});
  vm.runInContext(extract("getLimitRolePermissions")+"\n"+extract("getPurchaseStaffRoleIdsForCategory"),ctx);
  const keys=["zakup-0-20","zakup-20-50","zakup-50-100","zakup-100-200","zakup-200-400","zakup-400-999"];
  keys.forEach((key,index)=>{
    const overwrites=ctx.getLimitRolePermissions(key,null);
    assert.deepEqual(Array.from(overwrites.filter(o=>o.allow),o=>o.id),roles.slice(index));
    assert.deepEqual(Array.from(ctx.getPurchaseStaffRoleIdsForCategory(`new-${key}`,{})),roles.slice(index));
  });
  assert(ctx.getLimitRolePermissions("inne",null).every(o=>o.deny));
  assert(!source.includes("await interaction.guild.channels.create(createOptions)"));
  assert(!source.includes("await guild.channels.create(createOptions)"));
  assert(source.includes("ticketCategoryTemplates: Object.fromEntries(ticketCategoryTemplates)"));
  assert(source.includes("botStateData.ticketCategoryTemplates"));
});
