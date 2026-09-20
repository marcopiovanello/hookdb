# timeflight

Motore minimale in Go che:

1. mantiene un datalake di file **Parquet** organizzati per tabella
   (`DataDir/<tabella>/*.parquet`);
2. esegue **compaction periodica** dei file piccoli in file più grandi
   ordinati per tempo, usando DuckDB come motore di merge;
3. espone il tutto via **Arrow Flight SQL** su gRPC, così da poter essere
   interrogato da qualunque client Flight SQL — incluso il datasource
   InfluxDB (modalità SQL / Flight SQL) di Grafana.

## Architettura in breve

```
                depositi esterni
                (writer / ETL / Telegraf...)
                       |
                       v
              DataDir/<tabella>/*.parquet
                       |
        +--------------+---------------+
        |                              |
   Catalog.Scan()                 Compactor
   (scopre nuovi file        (ogni N secondi, unisce
    ogni N secondi)           i file "stabili" in uno
        |                     solo, ordinato per tempo)
        v                              |
  CREATE OR REPLACE VIEW <tabella>     |
  AS SELECT * FROM read_parquet([...]) <-- aggiornata anche dal Compactor
        |
        v
   DuckDB (motore SQL in-memory)
        |
        v
  Arrow Flight SQL server (gRPC)
        |
        v
     Grafana / qualunque client Flight SQL / ADBC
```

Punti chiave delle scelte fatte:

- **DuckDB come motore di query**, invece di scrivere un query planner
  Arrow da zero in Go: legge nativamente Parquet con pushdown di filtri e
  proiezioni, supporta SQL completo, ed è imbarcabile in un binario Go via
  CGO (`github.com/marcboeker/go-duckdb`). È lo stesso approccio pragmatico
  usato da molti motori "Parquet + SQL" recenti.
- **Nessun parser SQL custom**: ogni tabella è esposta a DuckDB come una
  `VIEW` su `read_parquet([...])`. Le query che arrivano da Grafana
  (`SELECT * FROM cpu WHERE time > ...`) vengono eseguite da DuckDB così
  come sono.
- **Compaction copy-on-write**: il compactor scrive un nuovo file, poi
  aggiorna la vista, e cancella i file vecchi solo dopo un periodo di
  grazia (`TF_DELETE_GRACE`, default nel codice 30s) per non rompere query
  Flight SQL già in corso su quei file.
- **Compaction a più livelli ("size-tiered", stile LSM)**: i file non
  vengono compattati tutti insieme in un unico blob crescente, ma a
  cascata su livelli (`config.LevelConfig`):
  - **L0**: file grezzi, uno per batch di ingestion, piccoli e numerosi;
  - quando un livello accumula `MinFiles` file "stabili" (più vecchi di
    `MinFileAge`), vengono fusi in **un solo** file scritto al livello
    successivo;
  - il livello più alto configurato è il "cold tier": file grandi, non
    ulteriormente toccati.

  Il livello di un file è ricavato dal **nome stesso del file**
  (prefisso `L<N>-`), non da un metadata store separato: questo permette
  al catalogo di ricostruire tutto dal solo filesystem al riavvio. File
  senza questo prefisso (es. quelli scritti da un writer esterno che non
  conosce il concetto di livello) sono trattati come L0, che è il
  comportamento corretto.

## Cosa NON fa (ancora)

- Non implementa ingestion "a riga" (line protocol / gRPC di scrittura):
  si aspetta che qualcos'altro depositi file `.parquet` completi nella
  cartella della tabella (scrittura + `rename` atomico, per evitare letture
  a metà).
- I metodi Flight SQL di **metadata** (cataloghi/schemi/tabelle,
  `DoGetTables`, `DoGetSqlInfo`, ecc.) sono implementati solo in parte
  (vedi TODO in `internal/server/flightsql.go`). L'esecuzione diretta di
  query SQL funziona comunque; quello che potrebbe mancare è
  l'autocomplete/schema-browser lato Grafana.
- Nessuna autenticazione/TLS sul server gRPC: da aggiungere prima di
  esporlo fuori da una rete fidata (Flight supporta middleware per questo).
- Non gestisce cancellazioni/upsert: è pensato per dati append-only tipici
  delle time-series.

## Build

```bash
go mod tidy   # risolve le versioni esatte delle dipendenze
CGO_ENABLED=1 go build -o timeflight ./cmd/timeflight
```

`go-duckdb` usa CGO: serve un toolchain C funzionante (gcc/clang) nella
macchina di build.

## Esecuzione

```bash
export TF_DATA_DIR=./data
export TF_LISTEN_ADDR=0.0.0.0:32010
./timeflight
```

Layout dati atteso, es. per una tabella `cpu` dopo un po' di ingestion e
qualche ciclo di compaction:

```
data/
  cpu/
    L0-20260920-101600-ghi789.parquet   <- file grezzo, appena arrivato
    L1-1758360900123456789.parquet      <- prodotto da una fusione L0->L1
    L2-1758360600000000001.parquet      <- prodotto da una fusione L1->L2
  mem/
    L0-20260920-101530-xyz002.parquet
```

Il prefisso `L<N>-` è ciò che dice al catalogo a quale livello appartiene
un file (vedi sezione sulla compaction a più livelli qui sopra). Un
writer esterno che deposita file senza questo prefisso va comunque bene:
verranno trattati come L0.

## Collegare Grafana (datasource InfluxDB, modalità Flight SQL / InfluxDB3)

Nel datasource InfluxDB di Grafana, in modalità "SQL" (quella che parla
Flight SQL, la stessa usata per InfluxDB 3 / IOx):

- **Host:Port**: indirizzo del tuo `timeflight` (es. `mio-host:32010`)
- **Database**: qualunque valore accettato dal catalogo DuckDB (con questo
  scaffold, `information_schema` riporta un solo catalogo — vedi
  `DoGetCatalogs`)
- Nessun IOx/Influx-specifico è richiesto lato protocollo: Grafana in
  modalità SQL parla Flight SQL "generico" (`CommandStatementQuery` +
  chiamate di metadata), non un'estensione proprietaria di InfluxDB —
  motivo per cui questo approccio è compatibile in linea di principio.

Se l'autocomplete delle tabelle non si popola, è il sintomo dei TODO sui
metodi di metadata citati sopra: nel frattempo puoi comunque scrivere SQL
a mano nei pannelli.

## Prossimi passi suggeriti

1. Completare `DoGetDBSchemas` / `DoGetTables` / `DoGetSqlInfo` in
   `internal/server/flightsql.go` (il modo più veloce è copiare
   l'implementazione dall'esempio ufficiale SQLite di arrow-go e adattare
   le query a DuckDB/`information_schema`).
2. Aggiungere un meccanismo di ingestion vero (es. un piccolo endpoint che
   accetta batch Arrow/line-protocol e li scrive come parquot via DuckDB
   `COPY ... TO ... (FORMAT PARQUET)`).
3. TLS + auth middleware su Flight prima di qualunque esposizione oltre
   localhost/rete fidata.
4. Politiche di compaction più furbe (per-tabella, per size invece che per
   count, tiering "hot/warm/cold").
