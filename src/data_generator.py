import argparse
import json
import random
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml

RNG = random.Random()

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
DEFAULT_PROFILE = BASE_DIR / "config" / "data_profile.yml"

COUNTRY_STATES = {
    "US": ["CA", "NY", "TX", "WA", "IL"],
    "IN": ["TN", "KA", "MH", "DL"],
    "CA": ["ON", "BC", "QC"],
    "UK": ["ENG", "SCT"],
}

SIGNUP_CHANNELS = ["web", "mobile", "partner", "retail"]
KYC_STATUS = ["verified", "pending", "failed"]
CUSTOMER_STATUS = ["active", "inactive", "suspended"]
SOURCE_SYSTEMS = ["oracle", "mysql", "mongodb", "dynamodb"]

CURRENCIES = ["USD", "EUR", "GBP", "INR"]
PAYMENT_METHODS = ["card", "paypal", "bank_transfer", "wallet"]
ORDER_STATUS = ["completed", "pending", "cancelled", "refunded"]
ORDER_CHANNELS = ["web", "mobile", "partner"]
ORDER_SYSTEMS = ["oracle", "mysql"]

EVENT_TYPES = ["view", "add_to_cart", "checkout", "purchase"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
BROWSERS = ["chrome", "safari", "edge", "firefox"]
REGIONS = ["NA", "EU", "APAC", "LATAM"]
REFERRERS = ["direct", "email", "social", "ads", "partner"]


def default_profile():
    return {
        "seed": 42,
        "sizes": {"customers": 800, "orders": 2800, "web_events": 5000},
        "date_ranges": {
            "customer_start": "2022-01-01",
            "order_start": "2023-01-01",
            "event_start": "2023-03-01",
        },
    }


def load_profile(path: Path):
    profile = default_profile()
    if path and path.exists():
        loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        profile["seed"] = loaded.get("seed", profile["seed"])
        profile["sizes"].update(loaded.get("sizes", {}))
        profile["date_ranges"].update(loaded.get("date_ranges", {}))
    return profile


def parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def _random_email(name):
    domains = ["gmail.com", "yahoo.com", "outlook.com", "company.com"]
    return f"{name.lower()}{RNG.randint(1,999)}@{RNG.choice(domains)}"


def inject_if(df, idx, col, value):
    if idx < len(df):
        df.loc[idx, col] = value


def build_customers(n, profile):
    first_names = ["Ava", "Liam", "Noah", "Mia", "Aria", "Zoe", "Ethan", "Ivy", "Riya", "Sana"]
    last_names = ["Patel", "Smith", "Johnson", "Khan", "Chen", "Singh", "Garcia", "Brown"]

    rows = []
    base_date = parse_date(profile["date_ranges"]["customer_start"])
    countries = list(COUNTRY_STATES.keys())

    for i in range(1, n + 1):
        name = f"{RNG.choice(first_names)} {RNG.choice(last_names)}"
        email = _random_email(name.replace(" ", ""))
        age = RNG.randint(19, 75)
        status = RNG.choice(CUSTOMER_STATUS)
        created_at = base_date + timedelta(days=RNG.randint(0, 900))
        country = RNG.choice(countries)
        state = RNG.choice(COUNTRY_STATES[country])
        signup_channel = RNG.choice(SIGNUP_CHANNELS)
        kyc_status = RNG.choice(KYC_STATUS)
        gdpr_consent = RNG.choice([True, False])
        source_system = RNG.choice(SOURCE_SYSTEMS)
        rows.append(
            {
                "customer_id": i,
                "full_name": name,
                "email": email,
                "age": age,
                "status": status,
                "created_at": created_at.strftime("%Y-%m-%d"),
                "country": country,
                "state": state,
                "signup_channel": signup_channel,
                "kyc_status": kyc_status,
                "gdpr_consent": gdpr_consent,
                "source_system": source_system,
            }
        )

    df = pd.DataFrame(rows)
    df["gdpr_consent"] = df["gdpr_consent"].astype("object")

    # Inject data quality issues
    inject_if(df, 5, "email", "bad-email")
    inject_if(df, 7, "age", 16)
    inject_if(df, 9, "status", "unknown")
    if len(df) > 13:
        inject_if(df, 12, "email", df.loc[13, "email"])
    inject_if(df, 20, "created_at", None)
    inject_if(df, 25, "country", "XX")
    inject_if(df, 27, "signup_channel", "call_center")
    inject_if(df, 30, "gdpr_consent", None)
    inject_if(df, 32, "kyc_status", "expired")
    inject_if(df, 35, "state", "ZZ")

    return df


def build_orders(customers, n, profile):
    rows = []
    base_date = parse_date(profile["date_ranges"]["order_start"])
    for i in range(1, n + 1):
        cust_id = RNG.choice(customers["customer_id"].tolist())
        order_total = round(max(0, RNG.gauss(120, 55)), 2)
        discount_pct = round(min(0.4, RNG.random() * 0.4), 3)
        tax_amount = round(order_total * 0.08, 2)
        shipping_amount = round(RNG.uniform(0, 25), 2)
        order_date = base_date + timedelta(days=RNG.randint(0, 500))
        rows.append(
            {
                "order_id": i,
                "customer_id": cust_id,
                "order_total": order_total,
                "order_date": order_date.strftime("%Y-%m-%d"),
                "currency": RNG.choice(CURRENCIES),
                "payment_method": RNG.choice(PAYMENT_METHODS),
                "order_status": RNG.choice(ORDER_STATUS),
                "discount_pct": discount_pct,
                "tax_amount": tax_amount,
                "shipping_amount": shipping_amount,
                "channel": RNG.choice(ORDER_CHANNELS),
                "source_system": RNG.choice(ORDER_SYSTEMS),
            }
        )

    df = pd.DataFrame(rows)
    inject_if(df, 10, "order_total", -45.0)
    inject_if(df, 15, "customer_id", 99999)
    inject_if(df, 20, "order_date", None)
    inject_if(df, 22, "currency", "ABC")
    inject_if(df, 24, "discount_pct", 0.75)
    inject_if(df, 26, "payment_method", "crypto")
    inject_if(df, 28, "order_status", "chargeback")

    return df


def build_web_events(customers, n, profile):
    rows = []
    base_ts = parse_date(profile["date_ranges"]["event_start"])
    for i in range(1, n + 1):
        cust_id = RNG.choice(customers["customer_id"].tolist())
        event_ts = base_ts + timedelta(minutes=RNG.randint(0, 80000))
        session_len = RNG.randint(5, 1800)
        rows.append(
            {
                "event_id": f"EVT-{i:05d}",
                "customer_id": cust_id,
                "event_type": RNG.choice(EVENT_TYPES),
                "event_ts": event_ts.isoformat(),
                "session_length_sec": session_len,
                "device_type": RNG.choice(DEVICE_TYPES),
                "browser": RNG.choice(BROWSERS),
                "region": RNG.choice(REGIONS),
                "referrer": RNG.choice(REFERRERS),
                "is_bot": RNG.choice([True, False]),
            }
        )

    # Inject issues
    if len(rows) > 5:
        rows[5]["event_type"] = "click"
    if len(rows) > 8:
        rows[8]["session_length_sec"] = 4000
    if len(rows) > 12:
        rows[12]["event_ts"] = None
    if len(rows) > 15:
        rows[15]["device_type"] = "smart_tv"
    if len(rows) > 18:
        rows[18]["is_bot"] = None

    return pd.DataFrame(rows)


def write_sqlite(customers, orders, path):
    conn = sqlite3.connect(path)
    customers.to_sql("customers", conn, if_exists="replace", index=False)
    orders.to_sql("orders", conn, if_exists="replace", index=False)
    conn.close()


def write_jsonl(events, path):
    with open(path, "w", encoding="utf-8") as f:
        for record in events.to_dict(orient="records"):
            f.write(json.dumps(record) + "\n")


def parse_args():
    parser = argparse.ArgumentParser(description="Generate synthetic datasets for data quality checks.")
    parser.add_argument("--config", default=str(DEFAULT_PROFILE), help="Path to data profile YAML.")
    parser.add_argument("--seed", type=int, help="Override random seed.")
    parser.add_argument("--customers", type=int, help="Number of customers.")
    parser.add_argument("--orders", type=int, help="Number of orders.")
    parser.add_argument("--web-events", type=int, dest="web_events", help="Number of web events.")
    return parser.parse_args()


def main():
    args = parse_args()
    profile = load_profile(Path(args.config))

    if args.seed is not None:
        profile["seed"] = args.seed

    sizes = profile["sizes"]
    customers_n = args.customers or sizes.get("customers", 800)
    orders_n = args.orders or sizes.get("orders", 2800)
    events_n = args.web_events or sizes.get("web_events", 5000)

    RNG.seed(profile["seed"])

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    customers = build_customers(customers_n, profile)
    orders = build_orders(customers, orders_n, profile)
    events = build_web_events(customers, events_n, profile)

    write_sqlite(customers, orders, RAW_DIR / "rdbms.db")
    write_jsonl(events, RAW_DIR / "web_events.jsonl")

    summary = {
        "customers": len(customers),
        "orders": len(orders),
        "web_events": len(events),
    }
    print("Generated data:", summary)


if __name__ == "__main__":
    main()
