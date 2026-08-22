# nowplaying

Backend für das Live-Musik-Widget auf rospies.dev. Ersetzt das LastFM-Scrobbling:
Die App registriert sich via [librespot](https://github.com/librespot-org/librespot)
als Spotify-Connect-Device und beobachtet über die Connect-Cluster-Updates, was auf
**beliebigen Geräten** des Accounts abgespielt wird — nicht nur, wenn sie selbst
das aktive Wiedergabegerät ist.

- **WSS-Push** (`/ws`): Besucher bekommen neue Abspielvorgänge sofort gepusht
  (Cover-URL, Interpret, Album, Titel, Songlänge, Startzeitpunkt, Spotify-Link)
  — plus **Live-Replikation des Playback-Zustands**: Pause/Resume/Seek/Stop und
  die aktuelle Position spiegeln sich als leichte `state`-Frames auf der
  Website (Scrobble-Zählung bleibt davon unberührt).
- **Top-Listen** (`/api/top`): meistgespielte Songs & Künstler der letzten 30 Tage
  aus Postgres. Ein Play zählt erst nach 30 s Hördauer (Scrobble-Regel).
- **TLS** terminiert nginx davor (`deploy/nginx-nowplaying.conf`); die App
  spricht plain HTTP auf Loopback (`NP_BIND_ADDR`, Default `127.0.0.1:8080`).
- **OTEL**: Traces, Metriken und Logs via OTLP/gRPC an einen konfigurierbaren
  Endpunkt (`service.name` per Config, `service.version` = CI-CalVer, zur
  Build-Zeit via `NP_BUILD_VERSION` eingebacken; Rest setzt Alloy).
  - HTTP: `http.server.request.duration`-Histogramm (Sekunden, semconv-Buckets)
    mit strikt begrenzter Kardinalität — `http.route` ist das gematchte
    Template (unbekannte Pfade → `unmatched`), Methoden normalisiert auf das
    RFC-Set (`_OTHER` sonst), Status numerisch. Request-Spans tragen
    `client.address` (rechtester `X-Forwarded-For`-Eintrag = der vom eigenen
    nginx angehängte; Fallback Socket-Peer bei Direktzugriff), `http.route`,
    Methode und Pfad; WS-Sessions haben einen eigenen `ws.session`-Span mit
    `client.address` und Connect-/Disconnect-Logs.
  - Spotify-Pfad: `spotify.connect` (pro Verbindungsaufbau),
    `cluster.handle_update` (update.reason, active_device, track.uri/title,
    playing/paused, position, outcome), `metadata.resolve`
    (source=cache|fetch|cluster_map), `play.persist` (hängt als spätes Kind am
    Trace des auslösenden Updates), DB-Spans mit track_id/limit.
  - Fehler-Spans tragen `otel.status_code=ERROR` (Request-Spans bei 5xx,
    metadata_failed, fehlgeschlagener Persist) — in Tempo/Grafana als
    fehlgeschlagen filterbar. Pool-Sättigung via `db.client.connection.count`
    {state=used|idle} und `db.client.connection.max` (semconv); vom
    Replay-Guard geschluckte Plays via `plays_replay_suppressed_total`.

## API

| Route | Beschreibung |
|---|---|
| `GET /ws` | WebSocket. Direkt nach Connect kommt `{"type":"now_playing",...}` (letzter Track inkl. aktuellem `playback`-Zustand), `{"type":"play",...}` bei jedem neuen Abspielvorgang und `{"type":"state",...}` (leichtes Frame) bei Pause/Resume/Seek/Stop. Unbekannte `type`-Werte sollten Clients ignorieren. |
| `GET /api/now-playing` | Letzter Play inkl. `playback` als JSON (`state` kann `stopped` sein), `204` wenn seit Start keiner erkannt wurde. |
| `GET /api/top?limit=10` | `{"songs":[{track_id,track_url,title,artists,album,cover_url,plays}],"artists":[{artist,plays}]}`, `limit` 1–50. |
| `GET /healthz` | `{"spotify_connected":bool,"db_ok":bool}`, 503 wenn DB down. |

Payload-Beispiele (`play` / `now_playing` identisch aufgebaut; `state` ist das
leichte Live-Delta, korreliert über `track_id`):

```json
{
  "type": "play",
  "track_id": "4uLU6hMCjMI75M1A2tKUQC",
  "track_url": "https://open.spotify.com/track/4uLU6hMCjMI75M1A2tKUQC",
  "title": "…", "artists": ["…"], "album": "…",
  "cover_url": "https://i.scdn.co/image/…",
  "duration_ms": 213000,
  "started_at": "2026-07-28T12:34:56Z",
  "playback": { "state": "playing", "position_ms": 0, "as_of": "2026-07-28T12:34:56Z" },
  "lyrics": { "lines": [ { "start_ms": 1000, "end_ms": 4200, "text": "Nzqmr gswby lkvv ehm tp" } ] }
}
```

`lyrics` ist optional und nur vorhanden, wenn zeilensynchronisierte Lyrics
existieren (`play`/`now_playing` und `/api/now-playing`; `state`-Frames tragen
es nie). Der Text ist serverseitig gescrambelt — erster Buchstabe jedes Worts
echt, Rest durch zufällige Buchstaben gleicher Case-Klasse ersetzt, Zeichen-
länge/Interpunktion/Zeilenstruktur und Timestamps echt. Gedacht als
verwaschener Hintergrund, der per `playback`-Extrapolation mitläuft.

```json
{
  "type": "state",
  "track_id": "4uLU6hMCjMI75M1A2tKUQC",
  "playback": { "state": "paused", "position_ms": 83000, "as_of": "2026-07-28T12:36:19Z" }
}
```

`playback.state` ∈ `playing | paused | stopped`. Client-Extrapolation der
Position: solange `playing` gilt `position_ms + (now − as_of)`, bei `paused`/
`stopped` ist sie eingefroren. Seeks kommen als neues `state`-Frame mit
Positionssprung; Spielt zwischenzeitlich nichts mehr, kommt `state:"stopped"`.

## Konfiguration

Alles per CLI-Flag **oder** Env-Var (`nowplaying --help` zeigt alle):

| Env | Default | Zweck |
|---|---|---|
| `NP_BIND_ADDR` | `127.0.0.1:8080` | HTTP-Bind hinter nginx (Loopback lassen — nur so ist `X-Forwarded-For` vertrauenswürdig) |
| `DATABASE_URL` | — | Postgres (Pflicht außer bei `--login`) |
| `NP_DEVICE_NAME` | `NowPlaying Widget` | Name in der Spotify-Geräteliste |
| `NP_CACHE_DIR` | `/var/lib/nowplaying/cache` | Credentials-Cache + stabile device_id |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | OTLP gRPC |
| `OTEL_SERVICE_NAME` | `nowplaying` | service.name |
| `NP_ALLOWED_ORIGINS` | `https://rospies.dev,https://*.rospies.dev` | CORS (`*` = alle) |
| `NP_MIN_PLAY_MS` | `30000` | Hördauer, ab der ein Play persistiert wird |
| `NP_TOP_LIMIT` | `10` | Default-Limit für /api/top |
| `RUST_LOG` | `info,librespot=warn` | Log-Filter |

## Setup

### 1. Postgres

```sh
sudo -u postgres psql -c "CREATE USER nowplaying WITH PASSWORD '…';"
sudo -u postgres psql -c "CREATE DATABASE nowplaying OWNER nowplaying;"
```

Migrationen laufen automatisch beim Start (`sqlx::migrate!`).

### 2. Einmaliger Spotify-Login (OAuth)

Username/Passwort-Auth existiert bei Spotify nicht mehr. Einmalig:

```sh
nowplaying --login --cache-dir /var/lib/nowplaying/cache
```

Der Befehl gibt eine `accounts.spotify.com`-URL aus und lauscht auf
`127.0.0.1:8898` auf den Redirect. Auf einer headless VM vorher den Port
weiterleiten und die URL im lokalen Browser öffnen:

```sh
ssh -L 8898:127.0.0.1:8898 user@vm
```

Danach liegen wiederverwendbare Credentials im Cache-Verzeichnis; alle
weiteren Starts sind non-interaktiv. (Der Login muss als derselbe User laufen,
dem das Cache-Verzeichnis gehört — sonst danach `chown -R nowplaying:`.)

### 3. Deployment

```sh
cargo build --release
sudo install -m 755 target/release/nowplaying /usr/local/bin/
sudo install -d /etc/nowplaying
sudo cp deploy/env.example /etc/nowplaying/env   # anpassen!
sudo cp deploy/nowplaying.service /etc/systemd/system/
sudo useradd -r -s /usr/sbin/nologin nowplaying
sudo systemctl daemon-reload && sudo systemctl enable --now nowplaying
```

Die App beendet sich absichtlich, wenn Spotify-Reconnects zu oft fehlschlagen
(>5 in 10 min) — `Restart=always` in der Unit übernimmt dann.

### 4. nginx (TLS-Termination)

```sh
sudo cp deploy/nginx-nowplaying.conf /etc/nginx/conf.d/nowplaying.conf  # server_name/Certs anpassen
sudo nginx -t && sudo systemctl reload nginx
```

nginx terminiert TLS (certbot übernimmt die Cert-Rotation), proxied per
HTTP/1.1 mit Upgrade-Headern (WebSocket) an `127.0.0.1:8080` und hängt die
Besucher-IP an `X-Forwarded-For` an. `proxy_read_timeout` liegt mit 75 s über
dem 30-s-Keepalive-Ping der App, damit idle WS-Verbindungen offen bleiben.

## Diagnose: `spike_dealer`

Eigenständiges Binary, das den Kern-Mechanismus isoliert testet (Meilenstein 0):
Es registriert das Connect-Device und printet parallel jedes decodierte
Cluster-Update inklusive aller Metadata-Map-Keys.

```sh
NP_CACHE_DIR=./spike-cache SPIKE_LOGIN=1 cargo run --bin spike_dealer
# dann am Handy Play/Pause/Skip/Seek drücken und die Ausgabe beobachten
```

Prüfen: (a) Gerät bleibt in der Spotify-App sichtbar, (b) jede Aktion vom
Handy erscheint als `[cluster]`-Zeile, (c) welche `meta`-Keys geliefert werden.

## Tests

`cargo test` ist vollständig selbstversorgend — kein Docker, kein lokales Postgres nötig:

- **Unit-Tests** (`src/`): `PlaybackTracker`-Dedup-Logik (Trackwechsel, Pause,
  Seek, Restart, Duplikat-Updates, Paused-Startup), Protobuf-Extraktion,
  Hörzeit-Logik (`ListenClock` pur, `PendingPersist` mit pausierter
  tokio-Uhr), **`handle_update`-Integration** (Fake-Fetcher via
  `FetchTrack`-Trait: Broadcast sofort, Persist nach Hörzeit, Skip/Stop/Pause,
  Metadata-Fehler droppt den Play), Resolver-Cache-Semantik (Fallback wird
  nicht gecached), Metadata-Fallback, CORS-Origin- und XFF-Matching,
  WS-Payload-Format, persistierte device_id.
- **`tests/web_tests.rs`**: In-Process-HTTP/WS ohne TLS und ohne DB
  (`connect_lazy`-Pool, kurzes acquire_timeout): now-playing 204/200, WS-
  Initial-Replay, Fanout, Lagged-Resync, WS-Origin-Enforcement (403),
  Wildcard-Modus, CORS erlaubt/verboten, healthz→503 und /api/top→500 bei
  DB-Ausfall.
- **`tests/db_tests.rs`**: echtes SQL gegen ein **embedded Postgres**
  (`postgresql_embedded` lädt beim ersten Lauf die zonky-Binaries — Version
  gepinnt — nach `~/.theseus/postgresql-zonky`; erster Lauf braucht daher
  Netz und etwas Geduld). Die zonky-Archive sind self-contained (libxml2,
  libicu usw. liegen im mitgelieferten `lib/`), es sind also keine
  System-Libraries auf dem Host nötig.
  Insert-Idempotenz, atomarer Replay-Guard, E2E-Persist (`PendingPersist`
  gegen echte DB), Top-Songs (30-Tage-Fenster, jüngste Metadaten pro Track),
  Top-Artists (`unnest`), Router mit echter DB (healthz 200, Limit-Clamping).
- **`tests/metrics_tests.rs`**: eigener Prozess mit `InMemoryMetricExporter` —
  prüft, dass die instrumentierten Pfade ihre OTEL-Metriken wirklich recorden
  (HTTP-Histogramm, db_errors, WS-Counter, plays_discarded).
- **`tests/telemetry_tests.rs`**: Smoke-Test für `telemetry::init`/`shutdown`
  ohne Collector (Wiring, EnvFilter, Shutdown-Reihenfolge).

## CI/CD (GitLab)

`.gitlab-ci.yml` läuft auf MR- und main-Pipelines (Runner-Tag `rust`):

| Stage | Jobs |
|---|---|
| build | `cargo build --locked --all-targets` |
| test | `cargo llvm-cov nextest` → JUnit (MR-Widget) + Cobertura (Coverage-Widget) + lcov (Sonar) |
| sast | `rustfmt` (Gate), `clippy -D warnings` (Gate, plus JSON-Report für Sonar) |
| deps | `cargo audit` (RustSec-Gate + natives SARIF für Sonar, allow_failure), `trivy fs --format sarif` (Zweitmeinung, nur Artefakt) |
| sonar | sonar-scanner mit lcov + clippy-Report + cargo-audit-SARIF (`sonar.sarifReportPaths`, gemappt auf `Cargo.lock`), `qualitygate.wait` |
| release | nativer `cargo build --release`; Binary als Job-Artefakt unter `dist/` (MR: `nowplaying-dev-…`, main: `nowplaying-1.<yy>.<ww>.<iid>` + git-Tag) |

Build-Infrastruktur: Toolchain via `rust-toolchain.toml` gepinnt (rustup am Runner
zieht sie automatisch), Cargo-Registry + `target/` werden auf `Cargo.lock` gekeyt
gecacht, Tools (nextest/llvm-cov/audit) in einem separaten stabilen Cache.
Debuginfo ist in CI abgeschaltet (`CARGO_PROFILE_*_DEBUG=0`), sonst wächst der
target-Cache auf viele GB. Gebaut wird nativ auf der CI-VM — deren glibc ist
damit die Mindestversion für das Deploy-Ziel (Runner- und Prod-Distro sollten
zusammenpassen, siehe Abschnitt Deployment).

libgcc wird statisch gelinkt (`.cargo/config.toml` + Linker-Script-Shim
`.cargo/libgcc_s.a`, Details im Kommentar dort): Das Binary hat keine
`libgcc_s.so.1`-Abhängigkeit und damit keinerlei Laufzeitbezug zur (custom)
gcc-Installation des Runners — NEEDED sind nur noch libc/libm/ld-linux.
Achtung: Die CI-Variable `CARGO_TARGET_…_RUSTFLAGS` überschreibt die
config.toml-Flags und muss sie deshalb wiederholen.

Hinweise: Die db_tests laden beim ersten Lauf auf einem frischen Runner die
embedded-Postgres-Binaries (zonky, self-contained, Version gepinnt) nach
`$HOME/.theseus/postgresql-zonky`; auf persistenten Runnern bleibt das
erhalten, und der Runner braucht keine Postgres-Systembibliotheken
(libxml2/libicu). `initdb` verweigert Root — der Runner-User darf
nicht root sein. Die Sonar-Keys (`sonar.rust.*`) setzen die offizielle
Rust-Analyse voraus (SonarQube ab 2025.1); ältere Server mit dem
Community-Plugin nutzen stattdessen `community.rust.*`-Keys.

## Verifikation (manuell, nach Deploy)

1. Lokal `curl http://127.0.0.1:8080/healthz`, dann durch nginx
   `curl https://nowplaying.rospies.dev/healthz` → `{"spotify_connected":true,"db_ok":true}`.
2. Track am Handy starten → Log zeigt `new play`, `curl https://nowplaying.rospies.dev/api/now-playing` liefert ihn.
3. `websocat wss://nowplaying.rospies.dev/ws` (2 Terminals): beide bekommen sofort `now_playing`, bei Trackwechsel beide `play`; Pause/Seek/Stop am Handy → beide sehen `state`-Frames (paused mit stehender Position, Positionssprung, stopped); Verbindung übersteht >60 s idle (Keepalive-Ping durch den Proxy).
4. 2 Tracks ganz hören, 1 nach 10 s skippen → `/api/top` zählt genau 2 Plays.
5. Grafana: `plays_total` steigt, Traces `cluster.handle_update` sichtbar, Logs mit Trace-Korrelation.
6. `kill -9` → systemd startet neu, kein doppelter Play (Replay-Guard, idempotenter Insert).

## Architektur-Notizen

- **Cluster-Beobachtung**: `session.dealer().listen_for("hm://connect-state/v1/cluster")`
  parallel zu Spirc — der Dealer fan-out't Messages an alle Subscriber einer URI
  (verifiziert in librespot 0.8.0, `dealer/maps.rs`). Registrierung muss **vor**
  `Spirc::new` erfolgen; Spirc connected die Session selbst.
- **Play-Erkennung**: `PlaybackTracker` (reine State-Machine, unit-getestet)
  unterscheidet Trackwechsel / Pause / Seek / Restart; `started_at` wird aus
  `timestamp − position_as_of_timestamp` abgeleitet und bleibt bei Seeks stabil.
- **Persistenz**: Broadcast sofort, DB-Insert erst nach `NP_MIN_PLAY_MS` über
  einen abbrechbaren Task; Reconnect-Replays werden über einen 10-s-Guard plus
  idempotenten Insert (`ON CONFLICT DO NOTHING`) unterdrückt.
- **Kein Audio**: eigener No-op-Sink, keine Audio-Bibliotheken im Build.
- **Upstream-Ausblick**: librespot PR #1704 würde Cluster-Events offiziell
  öffentlich machen; nur `spotify/cluster.rs` müsste dann umgestellt werden.
