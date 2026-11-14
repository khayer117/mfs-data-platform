"""
Static reference data for the MFS transaction generator.
Everything is synthetic — no real phone numbers or personal data.
"""
import random

DISTRICTS = [
    "Dhaka", "Chattogram", "Rajshahi", "Khulna", "Sylhet",
    "Barishal", "Rangpur", "Mymensingh", "Cumilla", "Gazipur",
]

BILLERS = ["DESCO", "TITAS", "WASA", "DPDC", "NESCO", "BREB"]

TXN_TYPES   = ["SEND_MONEY", "CASH_OUT", "CASH_IN", "MERCHANT_PAY", "BILL_PAY"]
TXN_WEIGHTS = [0.35,         0.25,       0.20,       0.12,           0.08]

# CASH_OUT fee rate per the spec
CASHOUT_FEE_RATE = 0.0185


def generate(seed: int = 42) -> dict:
    """
    Returns a dict with wallets, agents, merchants, billers, districts.
    Deterministic: same seed → same reference data every run.
    """
    rng = random.Random(seed)

    # 5,000 customer wallets — Bangladeshi-style 11-digit numbers (013–019)
    prefixes = ["013", "014", "015", "016", "017", "018", "019"]
    wallets = [
        rng.choice(prefixes) + str(rng.randint(10_000_000, 99_999_999))
        for _ in range(5_000)
    ]

    # 200 agents, each tied to a district
    agents = [
        {"agent_id": f"AG-{i:04d}", "district": rng.choice(DISTRICTS)}
        for i in range(1, 201)
    ]

    # 300 merchants
    merchants = [f"MR-{i:04d}" for i in range(1, 301)]

    return {
        "wallets":   wallets,
        "agents":    agents,
        "merchants": merchants,
        "billers":   BILLERS,
        "districts": DISTRICTS,
    }
