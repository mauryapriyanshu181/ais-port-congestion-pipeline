# AIS port congestion pipeline

A real-time pipeline that tracks ships near major ports and measures how congested
each port is. Live AIS ship positions stream through Kafka, Spark Structured Streaming
turns them into Delta Lake tables (bronze → silver → gold), and a Streamlit dashboard
shows the result on a map.

## Why

When every berth at a port is busy, arriving ships drop anchor nearby and wait. The size
of that queue is a good early signal of supply-chain delays, and AIS data makes it
visible: every large ship broadcasts its position, speed and status every few seconds,
and the data is public. I wanted a project that works with a real, never-ending stream
instead of a static CSV, and this problem needed streaming, stateful aggregation and
some domain logic all at once.

## Architecture

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

| Path | What it does |
|---|---|
| `producer/producer.py` | Streams AIS position reports into Kafka, from aisstream.io (`live`) or a built-in ship simulator (`mock`) |
| `spark/stream_positions.py` | Kafka → bronze (raw) and silver (clean) Delta tables |
| `spark/port_congestion.py` | silver → gold: matches ships to ports and counts them per window |
| `spark/show_tables.py` | Prints row counts and the latest congestion window |
| `spark/submit.sh` | Shared `spark-submit` with the Kafka and Delta packages |
| `dashboard/app.py` | Streamlit dashboard; reads the Delta tables directly, no Spark needed |
| `config/ports.json` | The ports being watched and the radius of each port area |
| `docker-compose.yml` | Kafka (KRaft), Kafka UI, producer, both Spark jobs and the dashboard |

## Design decisions

**Kafka between the producer and Spark.** The AIS feed doesn't wait for anyone. With
Kafka in the middle, Spark can restart or fall behind without losing messages, and it
picks up from its checkpoint. Messages are keyed by MMSI, the ship's ID, so each ship's
reports stay in order within a partition.

**Bronze keeps everything as-is.** Silver applies cleaning rules (dropping impossible
coordinates, converting AIS "not available" codes like speed 102.3 to nulls, removing
duplicates). If one of those rules turns out to be wrong, silver can be rebuilt from bronze.

**Deduplication with a watermark.** The same broadcast is often picked up by more than
one receiver. Silver drops repeats of the same ship and timestamp, and only remembers
keys for 10 minutes of event time, so the state doesn't grow forever.

**Classifying what a ship is doing.** Each report gets an `activity`, checked in this order:

| Check | Activity |
|---|---|
| Speed ≥ 1 knot | moving |
| Navigational status 5 (moored) | berthed |
| Stopped otherwise (at anchor, or status not updated) | waiting |
| Speed unknown and status isn't 1 or 5 | unknown |

Speed comes first because crews often forget to update the navigational status. The
1-knot threshold leaves room for anchored ships, which drift and swing a little.

**One state per ship per window.** A ship can arrive, slow down and anchor within one
window. Gold first reduces each window to one row per ship (its last report), then counts
per port. Every ship lands in exactly one group, and the counts are exact rather than
approximate. Spark 3.4+ allows chaining the two windowed aggregations in append mode.

**15-minute windows with a 5-minute watermark.** Anchored ships only report about every
3 minutes, so short windows undercount exactly the ships that matter. Fifteen minutes
gives each one several chances to be seen. The watermark waits 5 minutes for late
messages before a window is written, so gold runs about 6–7 minutes behind real time.

**Geofencing with haversine distance.** A ship belongs to a port if it's within that
port's radius, measured as great-circle distance. The port list is tiny, so it's
broadcast to every executor instead of shuffled.

## Known limitations

- A ship tied up at a berth whose crew left the status as "under way" is counted as
  waiting. Fixing that needs the actual berth locations, not just a port center and radius.
- Port areas are circles around approximate coordinates, which doesn't match real
  anchorage shapes.
- aisstream.io relies on volunteer receivers on land, so coverage varies by port. The
  `position_reports` column helps tell a quiet port from a poorly covered one.
- The first window after a start only has partial data, so its counts are low.
- A count of waiting ships isn't "congestion" until it's compared with that port's
  normal level, which needs a few weeks of history.

## Running it

Needs Docker Desktop with about 6 GB of memory.

```bash
cp .env.example .env
```

```bash
docker compose up -d --build
```

`.env` starts in mock mode, which simulates 300 ships around the ports in
`config/ports.json`, so it runs without an API key. For real data, get a free key from
[aisstream.io](https://aisstream.io), then set `PRODUCER_MODE=live` and
`AISSTREAM_API_KEY` in `.env`. `BOUNDING_BOXES` limits the feed to some regions if the
whole world is too much for the machine. Delete `data/` (with the stack stopped) when
switching from mock to live, so simulated ships don't mix with real ones.

The first start downloads the Spark packages, so the Spark jobs take a minute or two to
begin. The dashboard map fills in within a few minutes; the charts appear once the first
window is written, about 25 minutes after the first start.

| What | Where |
|---|---|
| Dashboard | http://localhost:8501 |
| Kafka UI | http://localhost:8080 |
| Spark UI (bronze/silver) | http://localhost:4040 |
| Spark UI (gold) | http://localhost:4041 |

To print the tables from the terminal:

```bash
docker compose exec spark-congestion bash /opt/jobs/submit.sh /opt/jobs/show_tables.py
```

To stop everything (the lake in `data/` is kept):

```bash
docker compose down
```

`CONGESTION_WINDOW` and `CONGESTION_WATERMARK` in `.env` control the window size and
late-data allowance. Changing them after gold has been written means deleting
`data/lake/gold` and `data/checkpoints/gold_port_congestion` first, because the
checkpoint remembers the old window size.

## Roadmap

- [x] Kafka ingestion from aisstream.io, with a simulator for offline runs
- [x] Bronze/silver/gold Delta tables with Spark Structured Streaming
- [x] Waiting / berthed / moving classification, one state per ship per window
- [x] Streamlit dashboard with a live map
- [ ] Dwell time: how long each ship waits at a port, using stateful streaming
- [ ] Alerts for ships that go silent ("AIS dark"), published to an `ais.alerts` topic
- [ ] Compare each port with its own normal level to flag real congestion
- [ ] Deploy to the cloud (Kafka on Confluent Cloud or MSK, Spark on Databricks or EMR, lake on S3) with Terraform
- [ ] Throughput and latency measurements
- [ ] Avro schemas, data-quality checks and tests
