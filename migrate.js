const { createClient } = require("@supabase/supabase-js");

const OLD_URL = "https://wtrgebczyqumyyrnagus.supabase.co";
const OLD_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind0cmdlYmN6eXF1bXl5cm5hZ3VzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjgwNzQxMzcsImV4cCI6MjA4MzY1MDEzN30.DfoskyY-P5llIM3lXyEhHDBfqX2VoNIw1AADPcgHm1Q";

const NEW_URL = "https://esckymvfxkpagbhhivgu.supabase.co";
// Uzupełnij poniższy klucz swoim nowym kluczem Anon z panelu Supabase nowego projektu!
const NEW_KEY = "PLACE_YOUR_NEW_ANON_KEY_HERE"; 

const oldClient = createClient(OLD_URL, OLD_KEY);
const newClient = createClient(NEW_URL, NEW_KEY);

const TABLES = [
  "weekly_sales",
  "contest_participants",
  "contests",
  "invite_bonus_invites",
  "invite_counts",
  "invite_fake_accounts",
  "invite_reward_levels",
  "invite_rewards_given",
  "invite_total_joined",
  "invites",
  "ticket_owners"
];

async function runMigration() {
  if (NEW_KEY === "PLACE_YOUR_NEW_ANON_KEY_HERE" || !NEW_KEY) {
    console.error("❌ BŁĄD: Uzupełnij zmienną NEW_KEY w pliku migrate.js kluczem Anon public ze swojego nowego projektu Supabase!");
    return;
  }

  console.log("🚀 Rozpoczynanie migracji danych ze starej bazy do nowej...");

  for (const table of TABLES) {
    console.log(`\n📋 Migracja tabeli: ${table}...`);
    try {
      // 1. Fetch from old
      const { data, error: fetchError } = await oldClient.from(table).select("*");
      if (fetchError) {
        console.error(`❌ Błąd pobierania z tabeli ${table}:`, fetchError.message);
        continue;
      }
      
      if (!data || data.length === 0) {
        console.log(`ℹ️ Tabela ${table} jest pusta w starej bazie.`);
        continue;
      }

      console.log(`📥 Pobrano ${data.length} wierszy. Wprowadzanie do nowej bazy...`);

      // 2. Upsert to new
      const { error: insertError } = await newClient.from(table).upsert(data);
      if (insertError) {
        console.error(`❌ Błąd zapisu do nowej tabeli ${table}:`, insertError.message);
      } else {
        console.log(`✅ Pomyślnie zmigrowano tabelę ${table}!`);
      }
    } catch (err) {
      console.error(`❌ Wyjątek podczas migracji tabeli ${table}:`, err);
    }
  }

  console.log("\n🏁 Migracja zakończona!");
}

runMigration();
