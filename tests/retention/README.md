# Verifica della retention SQLite

Eseguire `npm run test:retention` con le dipendenze del progetto già disponibili.
La suite usa database SQLite in memoria e non avvia il servizio.

In assenza delle dipendenze può essere eseguita con un Node che espone
`node:sqlite` (Node 22.13+ o successivo compatibile). Il supporto di test usa
SQLite reale, adatta transazioni e pragma all'interfaccia di better-sqlite3 e
sostituisce soltanto cache, StatsD e oggetti IRC. La diagnostica iniziale indica
il backend e la versione SQLite effettivamente utilizzati.

Questa modalità verifica il codice reale della retention, le query dello store,
rollback, TARGETS e pulizia degli orfani. **Non sostituisce** la suite Jest con
better-sqlite3 9.6.0, né verifica il protocollo IRC completo o le prestazioni del
database di produzione. I retry SQLITE_BUSY vengono verificati tramite errori
iniettati dopo operazioni SQL reali e rollback effettivo.

Per vietare qualsiasi fallback usare:

```
KIWIBNC_RETENTION_REQUIRE_REAL=1 npm run test:retention
```

Questa modalità deve essere eseguita con le dipendenze già presenti e Node
compatibile con il server (versione comunicata: 22.23.1). Se manca il driver o
una dipendenza dello store, il test fallisce anziché usare sostituti.

## Proprietà della modifica

- Niente elenco globale dei valori di `data`, DISTINCT o tabelle TEMP.
- Ricerca di account e buffer con seek nell'indice `logs_user_id_ts`.
- Pagine di massimo 150 righe esaminate; cursore `(time, rowid)`.
- Due ricerche distinte per continuare nel timestamp corrente e in quelli
  successivi: nessun OFFSET o rilettura dei rowid precedenti.
- Prefisso vuoto: categoria server esclusiva, anche nei buffer dei canali.
- Soglie temporali fissate a inizio ciclo con la convenzione `Date.setDate`.
- Messaggi, TARGETS e relativi orfani nella stessa transazione. Massimo 750
  riferimenti candidati per pagina: il fallimento ripristina anche i messaggi.
- Pause da 50 ms fra pagine, anche senza cancellazioni; yield fra account e
  buffer. Retry SQLITE_BUSY e codici estesi SQLITE_BUSY_*: massimo 10 attese
  da 200 ms, ripartendo dalla transazione dopo il rollback.
- Nessuna modifica dello schema, di CHATHISTORY o di `temp_store`.

I record con account o buffer NULL non vengono cancellati. Riferimenti senza
nome risolvibile sono conservati, salvo le righe identificabili come server.
Inserimenti retrodatati o nuovi gruppi già oltrepassati dal cursore vengono
recuperati dal ciclo successivo. La scansione non mantiene uno snapshot globale.

## Prima dell'utilizzo beta

Su una copia coerente con WAL, verificare con le versioni distribuite:

1. Suite Jest e questa suite, senza adattatore.
2. EXPLAIN QUERY PLAN, soprattutto il seek `time = ? AND rowid > ?`.
3. RSS, heap, durata massima delle operazioni sincrone, event-loop lag, latenza
   IRC/CHATHISTORY e lunghezza della coda messaggi durante un intero ciclo.
4. Costo del ricalcolo dei TARGETS e dei controlli degli orfani nella transazione:
   i controlli riusano gli indici esistenti, ma I/O e distribuzione fra network
   possono incidere sulla durata. Un warning segnala transazioni oltre 100 ms,
   con durata separata di TARGETS e pulizia orfani. Il totale include COMMIT,
   ma esclude le attese asincrone fra retry. Non è una garanzia di latenza massima.

La correzione server/PM cambia esplicitamente il comportamento precedente:
con server a 60 giorni e PM a 30 giorni, i messaggi server fra 30 e 60 giorni
restano conservati. Non modificare i dati per riprodurre il vecchio difetto.

Per il rollback del codice usare la revisione precedente con la normale
procedura di distribuzione: non è necessaria una migrazione inversa. Le
cancellazioni già confermate richiedono comunque un backup per essere annullate.
