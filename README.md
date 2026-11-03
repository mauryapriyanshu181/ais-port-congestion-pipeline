# AIS port congestion pipeline

Real-time ship tracking with Kafka, Spark Structured Streaming and Delta Lake.
Ship positions (AIS) stream into Kafka, Spark cleans them into a lakehouse
(bronze → silver → gold), and the gold table shows how many ships are around
each major port in each time window, split into waiting, berthed and moving.
A Streamlit dashboard shows the results on a live map.

```
aisstream.io / simulator ──► producer ──► Kafka (ais.raw, keyed by MMSI)
                                              │
                         spark-positions  ◄───┘
                           ├─► bronze/ais_raw          raw messages + Kafka metadata
                           └─► silver/ship_positions   parsed, validated, deduplicated, with activity
                                              │
                         spark-congestion ◄───┘
                           └─► gold/port_congestion    waiting / berthed / moving ships, per port per window
                                              │
                         dashboard ◄──────────┘        Streamlit: leaderboard, trends, live map
```

## How ships are classified

Each position report gets an `activity` in silver, checked in this order:

| Check | Activity |
|---|---|
| Speed ≥ 1 knot | moving |
| Navigational status 5 (moored) | berthed |
| Stopped otherwise (at anchor, or the status wasn't updated) | waiting |
| Speed unknown and status isn't 1 or 5 | unknown |

Speed is checked first because crews often forget to update the status. In gold, each
ship is counted once per window by its last report, so the numbers read "at the end of
this window, N ships were waiting".

**Known gap:** a ship tied up at a berth whose crew left the status as "under way" is
counted as waiting. Fixing that needs exact berth locations.

Everything runs locally in Docker. The same jobs run on the cloud later by
changing `KAFKA_BOOTSTRAP` and `LAKE_PATH` (for example to Confluent Cloud and `s3a://...`).

## Layout

| Path | What it is |
|---|---|
| `docker-compose.yml` | Kafka (KRaft), topic setup, Kafka UI, producer, two Spark jobs |
| `producer/producer.py` | Publishes AIS position reports to Kafka; `live` (aisstream.io) or `mock` mode |
| `spark/stream_positions.py` | Kafka → bronze + silver Delta tables |
| `spark/port_congestion.py` | silver → gold: geofences ships to ports, counts per window |
| `spark/show_tables.py` | Prints row counts and the latest congestion window |
| `spark/submit.sh` | `spark-submit` with the Kafka and Delta packages and settings |
| `dashboard/app.py` | Streamlit dashboard reading the Delta tables directly, no Spark needed |
| `config/ports.json` | Ports and their geofence radius; used by the simulator and the gold job |
| `data/` | The lake and streaming checkpoints (created on first run, git-ignored) |

## Run it

Needs Docker Desktop with about 6 GB of memory available.

1. Create your `.env` (starts in mock mode, no API key needed):

```bash
cp .env.example .env
```

2. Start the stack:

```bash
docker compose up -d --build
```

The first start downloads the Spark packages, so the Spark jobs take a minute or two to begin.

3. Watch it work:

- Kafka UI: http://localhost:8080 → Topics → `ais.raw` → Messages
- Spark UI for bronze/silver: http://localhost:4040 → Structured Streaming
- Spark UI for gold: http://localhost:4041
- **Dashboard: http://localhost:8501** (refreshes every minute; the map fills in first, the charts once the first window is written)

```bash
docker compose logs -f producer spark-positions spark-congestion
```

4. Look at the tables. Gold rows appear once a 15-minute window has closed and the 5-minute watermark has passed, so allow up to about 25 minutes after the first start. The first window is partial, so its counts are low:

```bash
docker compose exec spark-congestion bash /opt/jobs/submit.sh /opt/jobs/show_tables.py
```

5. Stop everything (the lake in `data/` is kept; delete `data/` to start fresh):

```bash
docker compose down
```

## Switching to real data

1. Get a free API key from [aisstream.io](https://aisstream.io).
2. In `.env`, set `PRODUCER_MODE=live` and `AISSTREAM_API_KEY=<your key>`.
   The whole world is a lot of messages; set `BOUNDING_BOXES` to a few regions if your laptop struggles.
3. Restart the producer:

```bash
docker compose up -d producer
```

If you switch after running mock mode, delete `data/` first (with the stack stopped) so the simulated ships don't mix with the real ones.

The congestion window (`CONGESTION_WINDOW`, default 15 minutes) and late-data allowance (`CONGESTION_WATERMARK`, default 5 minutes) are set in `.env`. To change them after the gold job has run, stop the stack and delete `data/lake/gold` and `data/checkpoints/gold_port_congestion`, because the saved progress remembers the old window size.

## Things to build next

- **Dwell time**: how long each ship has been waiting near a port (`applyInPandasWithState`).
- **Anomalies**: ships that go silent ("AIS dark"), speed spikes, or sudden route changes, published to an `ais.alerts` topic.
- **ETA** to the declared destination (needs the `ShipStaticData` message type too).
- **Berth locations**: geofence the terminals themselves to catch moored ships with a stale status.
- **Cloud**: Confluent Cloud or MSK for Kafka, EMR Serverless or Databricks for Spark, S3 for the lake, Terraform for all of it.
- **Quality and ops**: Avro + Schema Registry, data-quality checks, consumer lag alerts, Airflow for daily `OPTIMIZE` and reports.
