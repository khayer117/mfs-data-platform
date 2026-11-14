"""
Injection scenarios: duplicates, late events, malformed records, fraud patterns.
All functions accept an rng (random.Random) so the whole run is seeded.
"""
import random
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional


# ---------------------------------------------------------------------------
# Late events
# ---------------------------------------------------------------------------

def maybe_late_event_time(
    rng: random.Random,
    late_rate: float,
    event_time: datetime,
) -> tuple[datetime, bool]:
    """Return (possibly-shifted event_time, was_late)."""
    if rng.random() < late_rate:
        hours_back = rng.uniform(1, 48)
        return event_time - timedelta(hours=hours_back), True
    return event_time, False


# ---------------------------------------------------------------------------
# Malformed records
# ---------------------------------------------------------------------------

def maybe_malform(
    rng: random.Random,
    malformed_rate: float,
    txn: dict,
) -> tuple[dict, bool]:
    """Return (possibly-corrupted txn copy, was_malformed)."""
    if rng.random() >= malformed_rate:
        return txn, False

    txn = txn.copy()
    problem = rng.choice(["null_amount", "negative_amount", "unknown_type", "missing_id"])

    if problem == "null_amount":
        txn["amount"] = None
        txn["fee"] = None
    elif problem == "negative_amount":
        txn["amount"] = round(-rng.uniform(10, 1_000), 2)
    elif problem == "unknown_type":
        txn["txn_type"] = rng.choice(["REFUND", "REVERSAL", "UNKNOWN_OP"])
    elif problem == "missing_id":
        txn["txn_id"] = None

    return txn, True


# ---------------------------------------------------------------------------
# Duplicate scheduling
# ---------------------------------------------------------------------------

def should_duplicate(rng: random.Random, duplicate_rate: float) -> bool:
    return rng.random() < duplicate_rate


# ---------------------------------------------------------------------------
# Fraud: velocity burst (R1)
# ---------------------------------------------------------------------------

def velocity_burst(
    rng: random.Random,
    ref_data: dict,
    now: datetime,
) -> list[dict]:
    """
    8–12 rapid SEND_MONEY transactions from one wallet within 60 seconds.
    Triggers the R1_VELOCITY fraud rule downstream.
    """
    sender = rng.choice(ref_data["wallets"])
    count  = rng.randint(8, 12)
    return [
        {
            "txn_id":          f"TXN-{uuid.uuid4()}",
            "txn_type":        "SEND_MONEY",
            "sender_wallet":   sender,
            "receiver_wallet": rng.choice(ref_data["wallets"]),
            "agent_id":        None,
            "merchant_id":     None,
            "biller":          None,
            "amount":          round(rng.uniform(500, 5_000), 2),
            "fee":             0.0,
            "channel":         rng.choice(["APP", "USSD"]),
            "district":        rng.choice(ref_data["districts"]),
            "event_time":      (now + timedelta(seconds=rng.uniform(0, 55))).strftime(
                                   "%Y-%m-%dT%H:%M:%SZ"
                               ),
        }
        for _ in range(count)
    ]


# ---------------------------------------------------------------------------
# Fraud: pass-through / mule (R3)
# ---------------------------------------------------------------------------

@dataclass
class PassThroughTracker:
    """
    Records recent SEND_MONEY recipients so a matching CASH_OUT can be
    triggered shortly after, simulating a mule / pass-through wallet.
    """
    _window: deque = field(default_factory=lambda: deque(maxlen=100))

    def record(self, wallet: str, amount: float, event_time: datetime) -> None:
        self._window.append({
            "wallet":      wallet,
            "amount":      amount,
            "received_at": event_time,
        })

    def pop_candidate(
        self,
        rng: random.Random,
        now: datetime,
        max_age_seconds: int = 90,
    ) -> Optional[dict]:
        candidates = [
            r for r in self._window
            if (now - r["received_at"]).total_seconds() <= max_age_seconds
        ]
        return rng.choice(candidates) if candidates else None


def pass_through_cashout(
    rng: random.Random,
    candidate: dict,
    ref_data: dict,
    now: datetime,
) -> dict:
    """
    Generate a CASH_OUT of ≥90 % of the received amount from the same wallet.
    Triggers the R3_PASS_THROUGH fraud rule downstream.
    """
    agent  = rng.choice(ref_data["agents"])
    amount = round(candidate["amount"] * rng.uniform(0.90, 1.00), 2)
    return {
        "txn_id":          f"TXN-{uuid.uuid4()}",
        "txn_type":        "CASH_OUT",
        "sender_wallet":   candidate["wallet"],
        "receiver_wallet": None,
        "agent_id":        agent["agent_id"],
        "merchant_id":     None,
        "biller":          None,
        "amount":          amount,
        "fee":             round(amount * 0.0185, 2),
        "channel":         rng.choice(["APP", "USSD"]),
        "district":        agent["district"],
        "event_time":      (now + timedelta(seconds=rng.uniform(10, 110))).strftime(
                               "%Y-%m-%dT%H:%M:%SZ"
                           ),
    }
