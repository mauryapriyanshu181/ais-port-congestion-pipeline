"""Publishes AIS ship position reports to Kafka.

PRODUCER_MODE=live streams real data from aisstream.io (needs AISSTREAM_API_KEY).
PRODUCER_MODE=mock simulates ships around the ports in ports.json, using the same
message format, so the rest of the pipeline runs without an API key.

Messages are keyed by MMSI (the ship's ID), so all reports from one ship land in
the same partition, in order.
"""
import asyncio
import json
import math
import os
import random
import sys
from datetime import datetime, timezone

import websockets
from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "ais.raw")
MODE = os.getenv("PRODUCER_MODE", "mock")
API_KEY = os.getenv("AISSTREAM_API_KEY", "")
# [[[lat_min, lon_min], [lat_max, lon_max]], ...]; defaults to the whole world
BOUNDING_BOXES = json.loads(os.getenv("BOUNDING_BOXES") or "[[[-90, -180], [90, 180]]]")
PORTS_FILE = os.getenv("PORTS_FILE", "../config/ports.json")
MOCK_SHIPS = int(os.getenv("MOCK_SHIPS", "300"))
# simulated seconds per real second, so mock ships visibly move
MOCK_SPEEDUP = float(os.getenv("MOCK_SPEEDUP", "60"))

AIS_URL = "wss://stream.aisstream.io/v0/stream"
EARTH_RADIUS_KM = 6371.0
KNOTS_TO_KMH = 1.852

sent = 0


def on_delivery(err, msg):
    if err is not None:
        print(f"delivery failed: {err}", file=sys.stderr)


def publish(producer, mmsi, payload):
    global sent
    key = str(mmsi) if mmsi is not None else None
    while True:
        try:
            producer.produce(TOPIC, key=key, value=payload, on_delivery=on_delivery)
            break
        except BufferError:
            # local queue is full; let librdkafka send some messages first
            producer.poll(1)
    producer.poll(0)
    sent += 1
    if sent % 1000 == 0:
        print(f"published {sent} messages", flush=True)


async def stream_live(producer):
    if not API_KEY:
        sys.exit("AISSTREAM_API_KEY is not set: get a free key at aisstream.io, or use PRODUCER_MODE=mock")
    subscription = json.dumps({
        "APIKey": API_KEY,
        "BoundingBoxes": BOUNDING_BOXES,
        "FilterMessageTypes": ["PositionReport"],
    })
    backoff = 1
    while True:
        try:
            async with websockets.connect(AIS_URL, ping_interval=20) as ws:
                # aisstream.io closes the connection if the subscription isn't sent within 3 seconds
                await ws.send(subscription)
                print("connected to aisstream.io", flush=True)
                backoff = 1
                async for raw in ws:
                    msg = json.loads(raw)
                    if "error" in msg:
                        sys.exit(f"aisstream.io error: {msg['error']}")
                    publish(producer, msg.get("MetaData", {}).get("MMSI"), raw)
        except (websockets.ConnectionClosed, OSError) as exc:
            print(f"connection lost ({exc}); reconnecting in {backoff}s", file=sys.stderr, flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


def haversine_km(lat1, lon1, lat2, lon2):
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def destination(lat, lon, bearing_deg, distance_km):
    """Point reached by travelling distance_km from (lat, lon) on the given bearing."""
    d = distance_km / EARTH_RADIUS_KM
    b = math.radians(bearing_deg)
    lat1, lon1 = math.radians(lat), math.radians(lon)
    lat2 = math.asin(math.sin(lat1) * math.cos(d) + math.cos(lat1) * math.sin(d) * math.cos(b))
    lon2 = lon1 + math.atan2(math.sin(b) * math.sin(d) * math.cos(lat1), math.cos(d) - math.sin(lat1) * math.sin(lat2))
    return math.degrees(lat2), (math.degrees(lon2) + 540) % 360 - 180


def make_ships(ports):
    ships = []
    for i in range(MOCK_SHIPS):
        port = random.choice(ports)
        anchored = random.random() < 0.4
        max_distance = port["radius_km"] * (0.8 if anchored else 2)
        lat, lon = destination(port["lat"], port["lon"], random.uniform(0, 360), random.uniform(0, max_distance))
        ships.append({
            "mmsi": 200000000 + i,
            "name": f"MOCK VESSEL {i:03d}",
            "port": port,
            "lat": lat,
            "lon": lon,
            "anchored": anchored,
            "sog": random.uniform(0, 0.5) if anchored else random.uniform(6, 18),
            "cog": random.uniform(0, 360),
        })
    return ships


def step(ship, seconds):
    # now and then a ship weighs anchor, or drops it
    if random.random() < 0.002:
        ship["anchored"] = not ship["anchored"]
        ship["sog"] = random.uniform(0, 0.5) if ship["anchored"] else random.uniform(6, 18)
    if ship["anchored"]:
        ship["sog"] = random.uniform(0, 0.5)
        return
    ship["cog"] = (ship["cog"] + random.uniform(-5, 5)) % 360
    ship["lat"], ship["lon"] = destination(ship["lat"], ship["lon"], ship["cog"], ship["sog"] * KNOTS_TO_KMH * seconds / 3600)
    port = ship["port"]
    if haversine_km(ship["lat"], ship["lon"], port["lat"], port["lon"]) > port["radius_km"] * 2:
        ship["cog"] = (ship["cog"] + 180) % 360


def to_ais_message(ship, now):
    """Shape a simulated ship like an aisstream.io PositionReport message."""
    lat, lon = round(ship["lat"], 6), round(ship["lon"], 6)
    return {
        "MessageType": "PositionReport",
        "MetaData": {
            "MMSI": ship["mmsi"],
            "ShipName": ship["name"],
            "latitude": lat,
            "longitude": lon,
            "time_utc": now.strftime("%Y-%m-%d %H:%M:%S.%f +0000 UTC"),
        },
        "Message": {
            "PositionReport": {
                "UserID": ship["mmsi"],
                "Latitude": lat,
                "Longitude": lon,
                "Sog": round(ship["sog"], 1),
                "Cog": round(ship["cog"], 1),
                "TrueHeading": round(ship["cog"]) % 360,
                # 1 = at anchor, 0 = under way using engine
                "NavigationalStatus": 1 if ship["anchored"] else 0,
            }
        },
    }


async def stream_mock(producer):
    with open(PORTS_FILE) as f:
        ports = json.load(f)
    ships = make_ships(ports)
    print(f"simulating {len(ships)} ships around {len(ports)} ports", flush=True)
    while True:
        now = datetime.now(timezone.utc)
        for ship in ships:
            step(ship, MOCK_SPEEDUP)
        # real transponders report at different rates, so only some ships report each tick
        for ship in random.sample(ships, k=max(1, len(ships) // 5)):
            publish(producer, ship["mmsi"], json.dumps(to_ais_message(ship, now)))
        await asyncio.sleep(1)


def main():
    if MODE not in ("live", "mock"):
        sys.exit(f"PRODUCER_MODE must be 'live' or 'mock', got {MODE!r}")
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 50,
        "compression.type": "lz4",
    })
    print(f"mode={MODE} topic={TOPIC} bootstrap={BOOTSTRAP}", flush=True)
    try:
        asyncio.run(stream_live(producer) if MODE == "live" else stream_mock(producer))
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()
