"""
MFS Transaction Generator — Component 1
Produces synthetic MFS transactions to Kafka.

Usage:
    # activate venv first, then:
    python producer.py

Configuration via .env (copy .env.example → .env and adjust):
    EVENTS_PER_SECOND=20   # increase for load testing
    MAX_MESSAGES=100       # 0 = run forever; use 100 for a quick sanity check
"""
import json
import os
import random
import signal
import sys
import time
import uuid
from collections import deque
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv

import reference_data as ref
import scenarios

load_dotenv()

# ---------------------------------------------------------------------------
# Config — all values come from environment / .env file
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP",    "localhost:9092")
TOPIC             = os.getenv("TOPIC",               "mfs.transactions")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "20"))
DUPLICATE_RATE    = float(os.getenv("DUPLICATE_RATE",    "0.02"))
LATE_RATE         = float(os.getenv("LATE_RATE",         "0.03"))
MALFORMED_RATE    = float(os.getenv("MALFORMED_RATE",    "0.01"))
FRAUD_ENABLED     = os.getenv("FRAUD_ENABLED", "true").lower() == "true"
SEED              = int(os.getenv("SEED", "42"))
MAX_MESSAGES      = int(os.getenv("MAX_MESSAGES", "0"))
LOG_EVERY         = int(os.getenv("LOG_EVERY", "500"))

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
rng  = random.Random(SEED)
data = ref.generate(SEED)
pt   = scenarios.PassThroughTracker()

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "acks":              "all",
    "enable.idempotence": "true",   # prevents producer-side network-retry duplicates
    "linger.ms":         5,         # small batching for throughput
    "batch.size":        16384,
})

# (send_at: float, value: bytes, key: bytes)
pending_dupes: deque = deque()

stats = {
    "total":     0,
    "dupes":     0,
    "late":      0,
    "malformed": 0,
    "fraud":     0,
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _delivery_cb(err, msg):
    if err:
        print(f"[ERR] delivery failed: {err}", file=sys.stderr)


def _build_txn(now: datetime) -> dict:
    txn_type = rng.choices(ref.TXN_TYPES, weights=ref.TXN_WEIGHTS, k=1)[0]
    wallet   = rng.choice(data["wallets"])
    agent    = rng.choice(data["agents"])
    amount   = round(rng.uniform(10, 25_000), 2)

    # Fraud: large cash-out (~0.1 % of CASH_OUT transactions)
    if FRAUD_ENABLED and txn_type == "CASH_OUT" and rng.random() < 0.001:
        amount = round(rng.uniform(25_001, 60_000), 2)
        stats["fraud"] += 1

    fee = round(amount * ref.CASHOUT_FEE_RATE, 2) if txn_type == "CASH_OUT" else 0.0

    txn = {
        "txn_id":          f"TXN-{uuid.uuid4()}",
        "txn_type":        txn_type,
        "sender_wallet":   wallet,
        "receiver_wallet": rng.choice(data["wallets"]) if txn_type == "SEND_MONEY" else None,
        "agent_id":        agent["agent_id"] if txn_type in ("CASH_IN", "CASH_OUT") else None,
        "merchant_id":     rng.choice(data["merchants"]) if txn_type == "MERCHANT_PAY" else None,
        "biller":          rng.choice(data["billers"]) if txn_type == "BILL_PAY" else None,
        "amount":          amount,
        "fee":             fee,
        "channel":         rng.choices(["APP", "USSD"], weights=[0.80, 0.20], k=1)[0],
        "district":        agent["district"],
        "event_time":      now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    # Track SEND_MONEY receivers for pass-through fraud
    if FRAUD_ENABLED and txn_type == "SEND_MONEY" and txn["receiver_wallet"]:
        pt.record(txn["receiver_wallet"], amount, now)

    # Late event injection
    late_time, was_late = scenarios.maybe_late_event_time(rng, LATE_RATE, now)
    if was_late:
        txn["event_time"] = late_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        stats["late"] += 1

    # Malformed injection
    txn, was_malformed = scenarios.maybe_malform(rng, MALFORMED_RATE, txn)
    if was_malformed:
        stats["malformed"] += 1

    return txn


def _send(txn: dict):
    key   = (txn.get("sender_wallet") or "").encode()
    value = json.dumps(txn, default=str).encode()
    producer.produce(TOPIC, key=key, value=value, callback=_delivery_cb)

    if scenarios.should_duplicate(rng, DUPLICATE_RATE):
        delay = rng.uniform(0.1, 5.0)
        pending_dupes.append((time.monotonic() + delay, value, key))
        stats["dupes"] += 1


def _flush_pending_dupes():
    now = time.monotonic()
    while pending_dupes and pending_dupes[0][0] <= now:
        _, value, key = pending_dupes.popleft()
        producer.produce(TOPIC, key=key, value=value, callback=_delivery_cb)


def _graceful_exit(sig, frame):
    remaining = len(pending_dupes)
    print(f"\nShutting down… flushing {remaining} pending dupes + in-flight messages")
    # Send all remaining scheduled duplicates immediately
    while pending_dupes:
        _, value, key = pending_dupes.popleft()
        producer.produce(TOPIC, key=key, value=value, callback=_delivery_cb)
    producer.flush(timeout=15)
    _print_stats(force=True)
    sys.exit(0)


def _print_stats(force: bool = False):
    if not force and stats["total"] % LOG_EVERY != 0:
        return
    elapsed = time.monotonic() - _start_time
    rate    = stats["total"] / elapsed if elapsed > 0 else 0
    print(
        f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
        f"sent={stats['total']:,}  rate={rate:.1f}/s  "
        f"dupes={stats['dupes']}  late={stats['late']}  "
        f"malformed={stats['malformed']}  fraud={stats['fraud']}"
    )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
signal.signal(signal.SIGINT,  _graceful_exit)
signal.signal(signal.SIGTERM, _graceful_exit)

interval    = 1.0 / EVENTS_PER_SECOND
_start_time = time.monotonic()

print("MFS Producer starting")
print(f"  broker          : {KAFKA_BOOTSTRAP}")
print(f"  topic           : {TOPIC}")
print(f"  rate            : {EVENTS_PER_SECOND} events/sec")
print(f"  duplicate_rate  : {DUPLICATE_RATE*100:.1f}%")
print(f"  late_rate       : {LATE_RATE*100:.1f}%")
print(f"  malformed_rate  : {MALFORMED_RATE*100:.1f}%")
print(f"  fraud_enabled   : {FRAUD_ENABLED}")
print(f"  seed            : {SEED}")
print(f"  max_messages    : {'unlimited' if MAX_MESSAGES == 0 else MAX_MESSAGES}")
print()

while True:
    tick = time.monotonic()
    now  = datetime.now(timezone.utc)

    # Fraud: velocity burst (~0.3 % per tick → roughly 1 burst per 333 normal events)
    if FRAUD_ENABLED and rng.random() < 0.003:
        for txn in scenarios.velocity_burst(rng, data, now):
            _send(txn)
            stats["fraud"] += 1

    # Fraud: pass-through CASH_OUT (~0.1 % per tick)
    if FRAUD_ENABLED and rng.random() < 0.001:
        candidate = pt.pop_candidate(rng, now)
        if candidate:
            _send(scenarios.pass_through_cashout(rng, candidate, data, now))
            stats["fraud"] += 1

    # Normal transaction
    _send(_build_txn(now))
    _flush_pending_dupes()
    producer.poll(0)   # trigger delivery callbacks without blocking
    stats["total"] += 1

    _print_stats()

    if MAX_MESSAGES and stats["total"] >= MAX_MESSAGES:
        _graceful_exit(None, None)

    # Pace to target rate
    sleep_for = interval - (time.monotonic() - tick)
    if sleep_for > 0:
        time.sleep(sleep_for)
