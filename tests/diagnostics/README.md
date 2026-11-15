# Diagnostica della cache `dataCache`

La politica LRU rimane invariata: `max=50000000`, peso `key.length`.
Il peso conta unità UTF-16 delle chiavi, non byte residenti. I valori sono ID
numerici; Map, Entry e nodi Yallist aggiungono overhead dipendente da V8.
Latin-1 può usare una rappresentazione a un byte, altri caratteri due byte per
unità; un emoji può occupare due unità UTF-16. UTF-8 non misura la memoria V8.

Lo store offre `getDataCacheStats()`, senza query o timer. Restituisce hit/miss
dall'avvio, hit rate, cardinalità, peso attuale e budget. `utf16PayloadBytes`
è soltanto la dimensione logica a due byte delle chiavi: esclude overhead e
non è una misura dell'heap o dell'RSS. La lettura è O(1) e non enumera le chiavi.
Il percorso `dataId()` aggiunge soltanto un incremento numerico per lookup.

Per misurare l'efficacia per intervallo, usare le differenze dei contatori:
`deltaHits / (deltaHits + deltaMisses)`. Occorre associare queste letture alla
telemetria del processo prima di decidere un diverso budget: il metodo non
pubblica automaticamente metriche e non apre un endpoint aggiuntivo.

Il profilo sintetico riproducibile è:

```
node tests/diagnostics/data-cache.js
```

Usa processi separati, GC esplicita solo nel test, 100.000 chiavi piatte da 41
unità e ID numerici. Non apre database e non accede a servizi. Il delta heap
include il rumore di V8: è una stima controllata, non una misura della produzione
né una garanzia sui byte per entry. Non estrapolare il risultato al budget
completo senza conoscere cardinalità e distribuzione delle chiavi reali.

La suite `tests/unit/data_cache.js` verifica anche un replay sintetico con
query SQLite reali: 10.000 lookup, 2.121 miss e 7.879 hit. Questo dimostra come
misurare l'efficacia; non è l'hit rate di cloud.simosnap.com.

Una riduzione del budget potrebbe aumentare miss, INSERT tentati e SELECT.
Non è stata applicata: prima servono misure della cache sul carico effettivo.
