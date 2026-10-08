# discord-bot

## Logi systemowe

`UPTIME_WEBHOOK` wskazuje kanał dziennika. Wiadomości startu/restartu, gotowości,
zamknięcia, awarii i zmian połączenia są wysyłane osobno. Panel statusu jest
edytowany co 5 minut, a HTTP jest sprawdzane co 10 minut; alerty HTTP pojawiają się
przy zmianie stanu. Powtarzające się identyczne błędy są ograniczone do jednego
alertu na minutę, ale każdy błąd zwiększa licznik sesji.

Historia sesji korzysta z osobnego rekordu `bot_state.id=2` w Supabase; dane
sklepu nadal są w `id=1`. `SIGINT` i `SIGTERM` zapisują zakończenie i próbują
wysłać log przed wyłączeniem (maksymalnie 5 sekund). Po gwałtownym zabiciu procesu
lub utracie hosta log zakończenia nie jest możliwy; kolejny start pokaże brak
potwierdzenia zamknięcia, bez wymyślania przyczyny awarii.

Testy bez logowania do Discorda i bez odczytu sekretów: `npm test`.
