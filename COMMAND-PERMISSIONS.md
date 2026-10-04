# Uprawnienia komend

Kod sprawdza właściciela serwera (guild.ownerId), a nie samą rangę Administrator.
Klient może używać tylko `/znizka`. Aktywny sprzedawca może dodatkowo używać:

`ticket-zakoncz`, `wydane`, `ustawienia`, `anonim`, `zamknij-z-powodem`,
`zamknij`, `dodaj`, `przejmij`, `odprzejmij`, `znajdz-ticket`, `rozliczenie`,
`wezwij`, `help`, `ostrzezenia`, `warns`.

Wszystkie pozostałe komendy są tylko dla właściciela, również oba rankingi
`top-wydane` i `topwydane`. Wyniki rankingów są prywatne.
Sprzedawca używa `/wezwij` bez argumentu; argument `uzytkownik` jest tylko
dla właściciela. Sprzedawca nadal może używać `/dodaj osoba:`.
Zawieszony sprzedawca traci dostęp do komend sprzedawcy.
Przyciski opinii i zaproszeń nadal działają dla klientów.

## Widoczność w Discordzie — krok właściciela

Po wdrożeniu wszystkie komendy poza `znizka` są domyślnie wyłączone dla
użytkowników bez Administratora. Aby pokazać je sprzedawcom, w ustawieniach
serwera → Integracje → NEW SHOP → Komendy przyznaj rolę Sprzedawca
(1350786945944391733) wyłącznie komendom z listy powyżej. Dla pozostałych
pozostaw brak dostępu dla @everyone i innych ról; usuń stare wyjątki ról
i użytkowników, jeśli były ustawione. Dla `znizka` pozostaw dostęp @everyone.
Nie włączaj Sprzedawcy dostępu do wszystkich komend całej integracji.

Token bota nie pozwala edytować wyjątków komend dla ról: API wymaga tokenu
OAuth2 użytkownika ze scope applications.commands.permissions.update.
Discord zawsze pokazuje komendy Administratorom; kod mimo to blokuje im
użycie prywatnych komend, jeżeli nie są właścicielem serwera.

Dokumentacja: https://docs.discord.com/developers/interactions/application-commands#permissions
