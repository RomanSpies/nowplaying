# `nowplaying` — Spotify Live-Widget Backend (Rust)

> **Erster Umsetzungsschritt**: Diesen Plan als `PLAN.md` nach `/home/roman/claude/testing/` kopieren (User-Wunsch).

## Context
Ersatz für das LastFM-Widget auf der Website (rospies.dev). Statt zu scrobbeln klinkt sich eine Rust-App via librespot als Spotify-Connect-Device in den Account ein und beobachtet über die Connect-Cluster-Updates, was **auf beliebigen Geräten** des Accounts abgespielt wird. Neue Abspielvorgänge werden Website-Besuchern per WSS gepusht (Cover-URL, Interpret, Album, Titel, Songlänge, Startzeitpunkt, Spotify-Link); ein HTTPS-Endpunkt liefert die Top-Songs/-Künstler der letzten 30 Tage aus Postgres. Voll instrumentiert mit OTEL (Traces, Metriken, Logs) über OTLP/gRPC an einen konfigurierbaren Endpunkt (Alloy-Pipeline dahinter).

## Entscheidungen (mit User geklärt)
- **Tracking**: Account-weit via Dealer-Subscription auf `hm://connect-state/v1/cluster` (nicht nur wenn App aktives Gerät ist).
- **Top-Listen**: rollierend 30 Tage.
- **Play-Wertung**: Track zählt erst nach 30 s Hördauer (Scrobble-Regel). WSS-Push erfolgt unabhängig davon sofort beim Trackwechsel.
- **Storage**: PostgreSQL auf derselben VM (sqlx 0.9).
- **TLS**: App terminiert selbst (Cert-/Key-PEM-Pfade per Config, axum-server + rustls, Hot-Reload für Cert-Rotation).
- **CORS**: nur `https://rospies.dev` + Subdomains (konfigurierbar).

## Zentrale technische Erkenntnisse (recherchiert, Juli 2026)
- **librespot 0.8.0** (MSRV 1.85, edition 2024). `PlayerEvent`s feuern nur, wenn dieses Device selbst abspielt. Lösung: `session.dealer()` (`DealerManager` ist öffentlich) → `listen_for("hm://connect-state/v1/cluster", ...)` **parallel zu Spirc**; Protobuf `ClusterUpdate` (librespot-protocol) enthält aktives Gerät, Track (inkl. Metadata-Map), `timestamp`, `position_as_of_timestamp`, `is_paused`.
  - ⚠️ **Unverifiziert**: ob zwei `listen_for`-Subscriber auf derselben URI sauber fan-outen oder sich Messages stehlen → **Meilenstein 0 (Spike) gate't die Architektur**. Fallbacks: git-Pin auf librespot PR #1704 (macht Cluster-Events öffentlich, unmerged) oder Betrieb ohne Spirc.
- **Auth**: Username/Passwort tot. Einmalig OAuth (`librespot-oauth`, Scope `streaming`), Credentials via `Session::connect(creds, true)` in `Cache` persistieren → Neustarts non-interaktiv. Headless-OAuth via `ssh -L` dokumentieren.
- **Kein Audio nötig**: `librespot` mit `default-features = false, features = ["rustls-tls-webpki-roots", "with-libmdns"]`; eigener No-op-`Sink` (write → `Ok(())`), Softmixer `mixer::find(None)`.
- **Metadata**: Cluster-Metadata-Map zuerst prüfen (Keys im Spike notieren); sonst `Track::get(&session, &SpotifyUri)` → Name, `album.name`, `album.covers`, Artists, Dauer. Cover-URL-Template aus Session-User-Attribut `image-url` (Fallback `https://i.scdn.co/image/{file_id}`). Spotify-Link: `https://open.spotify.com/track/{uri.to_id()?}`.
- **Stabilität**: Session nach Invalidierung nicht wiederverwendbar, Bibliothek reconnected nicht selbst → eigener Reconnect-Loop wie librespot-CLI (`select!` über spirc_task / dealer-stream / `session.is_invalid()` / shutdown; max 5 Reconnects/600 s, dann exit + systemd `Restart=always`). `Spirc::new` gibt `(Spirc, SpircTask-Future)` zurück — Future muss gespawnt werden.
- **OTEL-Versionsmatrix**: opentelemetry/_sdk/-otlp **0.32**, tracing-opentelemetry **0.33** (Offset!), opentelemetry-appender-tracing 0.32, tracing-subscriber ≥0.3.22. `opentelemetry-otlp` zwingend `default-features = false, features = ["grpc-tonic","trace","metrics","logs"]` (Default wäre HTTP + blocking reqwest). Alle OTEL-Crates nur atomar upgraden.

## Projektstruktur
```
Cargo.toml
migrations/0001_create_plays.sql
src/
  main.rs            # Bootstrap: Config → Telemetry → DB → Spotify-Task → Webserver; graceful shutdown
  config.rs          # clap 4.6 derive + env (kein TOML nötig)
  telemetry.rs       # OTEL-Provider (Traces/Metriken/Logs), tracing-subscriber-Wiring, Shutdown
  events.rs          # PlayEvent, WsMessage (serde-tagged), Einmal-Serialisierung zu Bytes
  state.rs           # AppState { broadcast::Sender<Bytes>, last_play: RwLock<Option<(PlayEvent, Bytes)>>, PgPool, Config }
  db.rs              # Pool, insert_play, top_songs, top_artists, sqlx::migrate!
  spotify/
    mod.rs           # spawn() + Supervision-/Reconnect-Loop
    session.rs       # Auth (Cache → OAuth-Fallback), Session/Player/Spirc-Aufbau
    sink.rs          # NoopSink
    cluster.rs       # Dealer-Subscription, ClusterUpdate → ClusterSnapshot, PlaybackTracker (Dedup-Statemachine)
    metadata.rs      # Enrichment via Track::get + LruCache(256), Cover-URL-Bau
  web/
    mod.rs           # Router, axum-server + RustlsConfig, Cert-Reload-Task (12 h), CorsLayer
    ws.rs            # GET /ws: Upgrade, Initial-Replay, Broadcast-Forwarding, Lagged-Handling
    api.rs           # GET /api/top, /api/now-playing, /healthz
src/bin/spike_dealer.rs  # M0-Spike, bleibt als Diagnose-Tool im Baum
```

## Kernlogik
### PlayEvent (WSS-Payload & DB-Zeile)
```rust
struct PlayEvent { track_id, track_url, title, artists: Vec<String>, album,
                   cover_url: Option<String>, duration_ms: u32, started_at: DateTime<Utc> }
```
JSON: `{"type":"play"|"now_playing", ...}`.

### PlaybackTracker (pure Statemachine, voll unit-testbar)
`fn observe(&mut self, snap: ClusterSnapshot, now) -> Option<NewPlay>` mit `ClusterSnapshot { track_uri, metadata_map, timestamp_ms, position_as_of_timestamp_ms, is_paused, active_device_id }` (Protobuf → Plain-Struct entkoppelt).
- Position extrapolieren: `position_as_of_timestamp + (now − timestamp)` (nur wenn nicht pausiert).
- **Anderer track_uri** → NEUER PLAY; `started_at = timestamp − position_as_of_timestamp` (Wallclock von Position 0), einmalig fixiert.
- Pause/Resume → kein Event. Positionssprung (>~3 s Abweichung) → Seek, kein Event — **außer** Restart (Position < 5 s, vorher > 30 s) → neuer Play.
- Frischer Start/Reconnect mit laufendem Track → als neuer Play behandeln, aber Replay-Guard: gleiche track_id + `|started_at − letzte DB-Zeile| < 10 s` → unterdrücken.

### Persistenz: sofort pushen, verzögert schreiben
Beim neuen Play: sofort Broadcast an WSS. DB-Insert erst nach `min_play_ms` (30 000, Config) über verzögerten Task, den der Tracker bei früherem Trackwechsel abbricht → Skips landen nie in den Top-Listen, kein UPDATE-Bookkeeping.

### Schema & Top-Queries
```sql
CREATE TABLE plays (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  track_id TEXT NOT NULL, title TEXT NOT NULL, album TEXT NOT NULL,
  artists TEXT[] NOT NULL, cover_url TEXT, duration_ms INTEGER NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  UNIQUE (track_id, started_at)      -- idempotente Inserts (ON CONFLICT DO NOTHING)
);
CREATE INDEX plays_started_at_idx ON plays (started_at DESC);
```
Top-Songs: `GROUP BY track_id`, jüngste Metadaten via `(array_agg(... ORDER BY started_at DESC))[1]`, `WHERE started_at >= now() - interval '30 days'`. Top-Artists: `unnest(artists)` + count.

## API
| Route | Zweck |
|---|---|
| `GET /ws` | WSS-Upgrade; direkt nach Connect gecachtes `now_playing` senden, dann Broadcast forwarden |
| `GET /api/now-playing` | letzter PlayEvent als JSON, 204 wenn keiner |
| `GET /api/top?limit=10` | `{ "songs": [...], "artists": [{artist, plays}] }` — beide Listen in einem Roundtrip, limit 1..=50 |
| `GET /healthz` | `{ spotify_connected, db_ok }` |

Fanout: `broadcast::Sender<Bytes>` (Kapazität 32), Payload einmal serialisiert; bei `RecvError::Lagged`: loggen, Counter, `last_play` als Resync nachsenden. CORS via `tower_http::cors::CorsLayer` nur für `/api/*` (WS-Handshakes unterliegen keinem CORS).

## OTEL (telemetry.rs)
- `Resource::builder().with_service_name(cfg.service_name)`; ein Resource für alle drei Provider. Endpoint aus Config (`with_endpoint`).
- Traces: `tracing_opentelemetry::layer()`; Logs: `OpenTelemetryTracingBridge` mit eigenem EnvFilter, der `hyper`/`tonic`/`h2`/`tower` unterdrückt (Feedback-Loop!); Metriken: `global::meter("nowplaying")`.
- Spans: `cluster.handle_update` (track_id, is_new_play), `metadata.fetch` (cache_hit), `db.insert_play`, `api.top_query`, HTTP via `tower_http::trace::TraceLayer`.
- Metriken: `plays_total`, `play_events_suppressed_total{reason}`, `ws_connections_active`, `ws_messages_sent_total`, `ws_lagged_total`, `dealer_reconnects_total`, `spotify_session_invalid_total`, `metadata_fetch_duration_ms` (Histogramm), `metadata_cache_{hits,misses}_total`, `db_errors_total`.
- Shutdown-Reihenfolge: tracer → logger → meter.

## Config (clap derive, Flag + Env + Default)
| Flag | Env | Default |
|---|---|---|
| `--bind-addr` | `NP_BIND_ADDR` | `0.0.0.0:8443` |
| `--tls-cert` / `--tls-key` | `NP_TLS_CERT` / `NP_TLS_KEY` | *(required, PEM)* |
| `--database-url` | `DATABASE_URL` | *(required)* |
| `--device-name` | `NP_DEVICE_NAME` | `NowPlaying Widget` |
| `--cache-dir` | `NP_CACHE_DIR` | `/var/lib/nowplaying/cache` |
| `--otlp-endpoint` | `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` |
| `--service-name` | `OTEL_SERVICE_NAME` | `nowplaying` |
| `--allowed-origins` | `NP_ALLOWED_ORIGINS` | `https://rospies.dev,https://*.rospies.dev` |
| `--min-play-ms` | `NP_MIN_PLAY_MS` | `30000` |
| `--top-default-limit` | `NP_TOP_LIMIT` | `10` |
| `--log-filter` | `RUST_LOG` | `info,librespot=warn` |

## Dependencies (Cargo.toml, exakte Versionen gepinnt)
```toml
librespot = { version = "0.8.0", default-features = false, features = ["rustls-tls-webpki-roots", "with-libmdns"] }
# ggf. librespot-protocol = "0.8", protobuf = "3" (in M0 prüfen ob re-exportiert)
tokio = { version = "1.53", features = ["macros", "rt-multi-thread", "sync", "time", "signal"] }
axum = { version = "0.8.9", features = ["ws"] }
axum-server = { version = "0.8", features = ["tls-rustls"] }
tower-http = { version = "0.6", features = ["cors", "trace"] }
sqlx = { version = "0.9", default-features = false, features = ["postgres", "runtime-tokio", "tls-rustls", "chrono", "migrate", "macros"] }
serde = { version = "1", features = ["derive"] }; serde_json = "1"; bytes = "1"
chrono = { version = "0.4", features = ["serde"] }
clap = { version = "4.6", features = ["derive", "env"] }; lru = "0.16"
anyhow = "1"; thiserror = "2"
tracing = "0.1"; tracing-subscriber = { version = "0.3.22", features = ["env-filter"] }
opentelemetry = "0.32"; opentelemetry_sdk = "0.32"
opentelemetry-otlp = { version = "0.32", default-features = false, features = ["grpc-tonic", "trace", "metrics", "logs"] }
tracing-opentelemetry = "0.33"; opentelemetry-appender-tracing = "0.32"
```
Achtung: rustls-Provider-Konsistenz (axum-server `tls-rustls` = aws-lc-rs; sqlx `tls-rustls` und librespot `rustls-tls-webpki-roots` entsprechend prüfen, nur ein Crypto-Provider im Baum).

## Meilensteine (nach Risiko geordnet)
- **M0 — Dealer-Spike (ZUERST, gate't alles)**: `src/bin/spike_dealer.rs` — Auth → Session → NoopSink-Player → Spirc (spawnen) → eigene Cluster-Subscription → decodierte Updates printen. Verifizieren: Device bleibt in Spotify-App sichtbar, jede Aktion vom Handy (Play/Pause/Skip/Seek) kommt an, Metadata-Map-Keys notieren. Bei Message-Stealing: Fallback-Entscheidung (PR #1704 git-Pin vs. Spirc-los) **vor** weiterem Bau.
- **M1 — Skeleton**: cargo init, config, telemetry, axum + TLS + `/healthz`, graceful shutdown. Verifizieren: `curl -k https://host:8443/healthz`; Telemetrie in Alloy/Grafana sichtbar.
- **M2 — Listener**: session/sink/cluster/metadata, Reconnect-Loop, Broadcast, `/api/now-playing`. Verifizieren: Handy-Playback → Event im Log + Endpunkt; Tracker-Unit-Tests grün.
- **M3 — Persistenz**: Migration, Deferred-Insert, Top-Queries, `/api/top`. Verifizieren: 2 Tracks voll + 1 Skip bei 10 s → genau 2 Zeilen.
- **M4 — WSS**: ws.rs, Initial-Replay, Lagged-Handling, CORS. Verifizieren: `websocat -k wss://host:8443/ws` mit 2 Clients; frischer Client bekommt sofort `now_playing`; Browser-Test von rospies.dev.
- **M5 — OTEL komplett + Ops**: alle Metriken/Spans, Cert-Reload-Task, systemd-Unit (`Restart=always`, `EnvironmentFile=`), OAuth-First-Run-Doku. Verifizieren: `plays_total` in Grafana; kill -9 → Neustart ohne doppelte Play-Zeile.
- **M6 — Härtung**: 24 h-Soak mit echtem Hören, Suppressed-Counter vs. Realität abgleichen (Seek-Schwelle tunen), README.

## Tests
- **Unit (pur)**: PlaybackTracker table-driven (Trackwechsel, Pause/Resume, Seek, Repeat-One-Restart, Reconnect-Replay); Cover-URL-Templating; started_at-Ableitung.
- **DB**: `#[sqlx::test]` gegen lokales Postgres — Insert-Idempotenz, Top-Queries inkl. 30-Tage-Grenze und Multi-Artist-unnest.
- **WS/HTTP-Integration**: Router in-process (ohne TLS), tokio-tungstenite-Clients: Initial-Replay, Fanout an N Clients, Lagged-Resync.
- **Manuell**: alles Spotify-seitige per Meilenstein-Checkliste; Spike-Binary bleibt als Diagnose-Tool.

## Risiken
| Risiko | Mitigation |
|---|---|
| Dealer-Subscription stiehlt Spirc Messages (unverifiziert) | M0-Spike zuerst; Fallback PR #1704 git-Pin oder Spirc-los |
| Session-Drops (routine) | Reconnect-Loop + systemd; Replay-Guard gegen Duplikate |
| Cluster-Metadata-Map unzureichend | `Track::get`-Pfad ist Default, Map nur Optimierung |
| `hm://connect-state` ist interne Spotify-API | isoliert in cluster.rs; Migrationspfad zu #1704 |
| OTEL-Versionsdrift (0.32/0.33-Offset) | exakt pinnen, nur atomar upgraden |
| Langsamer WS-Client | broadcast + Lagged-Skip/Resync, nie Producer-Backpressure |
