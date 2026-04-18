#!/usr/bin/env python3
"""
Snowpipe Streaming v2 — Python SDK Producer for Tasty Bytes
============================================================
Generates fake POS orders, customers, and clickstream events,
then streams them directly into Snowflake Iceberg tables using
the snowflake-ingest SDK (Snowpipe Streaming v2).

Prerequisites:
  pip install snowflake-ingest faker cryptography

  Snowflake objects must already exist (run notebook §1-§2 first):
    - TASTY_BYTES_DEMO.DEV.RAW_ORDERS       (Iceberg)
    - TASTY_BYTES_DEMO.DEV.RAW_CUSTOMERS    (Iceberg)
    - TASTY_BYTES_DEMO.DEV.RAW_MENU         (Iceberg)
    - TASTY_BYTES_DEMO.DEV.RAW_CLICKSTREAM  (Iceberg)

  Key-pair auth: generate RSA key pair and register public key with your user:
    openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
    openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
    ALTER USER <your_user> SET RSA_PUBLIC_KEY='<paste contents of rsa_key.pub minus header/footer>';

Usage:
  python snowpipe_streaming_producer.py \
    --account st83997 \
    --user XAVIZONE \
    --private-key-path ./rsa_key.p8 \
    --database TASTY_BYTES_DEMO \
    --schema DEV \
    --batches 10 \
    --batch-size 200 \
    --interval 5
"""

import argparse
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from faker import Faker
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest.utils.uris import DEFAULT_HOST_FMT

try:
    from snowflake.ingest import SnowflakeStreamingIngestClient, SnowflakeStreamingIngestClientFactory
except ImportError:
    SnowflakeStreamingIngestClient = None

fake = Faker()
Faker.seed(42)
random.seed(42)

MENU_ITEMS = [
    {"menu_id": 1, "menu_type": "Ice Cream", "brand": "Freezing Point", "item_id": 101, "name": "Vanilla Bean", "cat": "Dessert", "subcat": "Ice Cream", "cogs": 1.50, "price": 5.00},
    {"menu_id": 1, "menu_type": "Ice Cream", "brand": "Freezing Point", "item_id": 102, "name": "Chocolate Fudge", "cat": "Dessert", "subcat": "Ice Cream", "cogs": 1.75, "price": 5.50},
    {"menu_id": 2, "menu_type": "BBQ", "brand": "Smoky BBQ", "item_id": 201, "name": "Pulled Pork Sandwich", "cat": "Main", "subcat": "Sandwich", "cogs": 3.00, "price": 9.50},
    {"menu_id": 2, "menu_type": "BBQ", "brand": "Smoky BBQ", "item_id": 202, "name": "Brisket Plate", "cat": "Main", "subcat": "Plate", "cogs": 5.00, "price": 14.00},
    {"menu_id": 3, "menu_type": "Tacos", "brand": "Guac n Roll", "item_id": 301, "name": "Carne Asada Taco", "cat": "Main", "subcat": "Taco", "cogs": 2.00, "price": 6.00},
    {"menu_id": 3, "menu_type": "Tacos", "brand": "Guac n Roll", "item_id": 302, "name": "Fish Taco", "cat": "Main", "subcat": "Taco", "cogs": 2.25, "price": 6.50},
    {"menu_id": 4, "menu_type": "Coffee", "brand": "Java the Hut", "item_id": 401, "name": "Latte", "cat": "Beverage", "subcat": "Coffee", "cogs": 0.80, "price": 4.50},
    {"menu_id": 4, "menu_type": "Coffee", "brand": "Java the Hut", "item_id": 402, "name": "Cold Brew", "cat": "Beverage", "subcat": "Coffee", "cogs": 0.60, "price": 4.00},
]

COUNTRIES = ["United States", "Canada", "United Kingdom", "Germany", "France"]
CHANNELS = ["Walk-Up", "Mobile App", "Web", "Third Party"]
CURRENCIES = {"United States": "USD", "Canada": "CAD", "United Kingdom": "GBP", "Germany": "EUR", "France": "EUR"}
PAGES = ["/menu", "/order", "/locations", "/about", "/loyalty", "/careers", "/cart", "/checkout", "/promo/summer", "/sustainability"]
REFERRERS = ["google.com", "instagram.com", "facebook.com", "tiktok.com", "direct", "yelp.com", "email_campaign"]
DEVICES = ["Mobile", "Desktop", "Tablet"]
MEMBERSHIP_LEVELS = ["Free", "Silver", "Gold", "Platinum"]
AVATARS = ["avatar_burger.png", "avatar_taco.png", "avatar_icecream.png", "avatar_coffee.png", "avatar_default.png"]


def load_private_key(path: str):
    with open(path, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())


def generate_order(order_id: int, max_customer_id: int) -> dict:
    now = datetime.utcnow()
    item = random.choice(MENU_ITEMS)
    qty = random.randint(1, 5)
    amount = round(item["price"] * qty, 2)
    tax = round(amount * 0.08, 2)
    discount = round(amount * random.choice([0, 0, 0, 0.05, 0.10]), 2)
    total = round(amount + tax - discount, 2)
    country = random.choice(COUNTRIES)
    return {
        "ORDER_ID": order_id,
        "TRUCK_ID": random.randint(1, 20),
        "LOCATION_ID": random.randint(1, 100),
        "CUSTOMER_ID": random.randint(1, max_customer_id),
        "ORDER_CHANNEL": random.choice(CHANNELS),
        "ORDER_TS": (now - timedelta(seconds=random.randint(0, 60))).strftime("%Y-%m-%d %H:%M:%S.%f"),
        "ORDER_CURRENCY": CURRENCIES[country],
        "ORDER_AMOUNT": amount,
        "ORDER_TAX_AMOUNT": tax,
        "ORDER_DISCOUNT_AMOUNT": discount,
        "ORDER_TOTAL": total,
        "SHIFT_ID": random.randint(1, 3),
        "INGESTED_AT": now.strftime("%Y-%m-%d %H:%M:%S.%f"),
    }


def generate_customer(customer_id: int) -> dict:
    now = datetime.utcnow()
    country = random.choice(COUNTRIES)
    return {
        "CUSTOMER_ID": customer_id,
        "FIRST_NAME": fake.first_name(),
        "LAST_NAME": fake.last_name(),
        "EMAIL": fake.email(),
        "PHONE_NUMBER": fake.phone_number(),
        "CITY": fake.city(),
        "COUNTRY": country,
        "POSTAL_CODE": fake.postcode(),
        "PREFERRED_LANGUAGE": "EN" if country in ["United States", "Canada", "United Kingdom"] else "OTHER",
        "GENDER": random.choice(["M", "F", "O"]),
        "MARITAL_STATUS": random.choice(["Single", "Married", "Divorced"]),
        "CHILDREN_COUNT": str(random.randint(0, 4)),
        "SIGN_UP_DATE": fake.date_between(start_date="-2y", end_date="today").isoformat(),
        "BIRTHDAY_DATE": fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
        "LOYALTY_POINTS": random.randint(0, 5000),
        "INGESTED_AT": now.strftime("%Y-%m-%d %H:%M:%S.%f"),
    }


def generate_clickstream(event_id: int) -> dict:
    now = datetime.utcnow()
    country = random.choice(COUNTRIES)
    membership = random.choice(MEMBERSHIP_LEVELS)
    membership_years = {"Free": random.uniform(0.1, 1), "Silver": random.uniform(1, 3), "Gold": random.uniform(2, 5), "Platinum": random.uniform(4, 8)}[membership]
    yearly_spend = {"Free": random.uniform(50, 200), "Silver": random.uniform(200, 500), "Gold": random.uniform(500, 1200), "Platinum": random.uniform(1000, 3000)}[membership]
    return {
        "EVENT_ID": event_id,
        "CAPTURED_TIME": (now - timedelta(seconds=random.randint(0, 3600))).strftime("%Y-%m-%d %H:%M:%S.%f"),
        "USERNAME": fake.user_name(),
        "EMAIL": fake.email(),
        "ADDRESS": fake.address().replace("\n", ", "),
        "AVATAR": random.choice(AVATARS),
        "AVG_SESSION_LENGTH": round(random.uniform(5, 45), 2),
        "TIME_ON_APP": round(random.uniform(1, 30), 2),
        "TIME_ON_WEBSITE": round(random.uniform(1, 25), 2),
        "LENGTH_OF_MEMBERSHIP": round(membership_years, 2),
        "YEARLY_AMOUNT_SPENT": round(yearly_spend, 2),
        "MEMBERSHIP_LEVEL": membership,
        "PAGE_URL": random.choice(PAGES),
        "REFERRER": random.choice(REFERRERS),
        "DEVICE_TYPE": random.choice(DEVICES),
        "COUNTRY": country,
        "INGESTED_AT": now.strftime("%Y-%m-%d %H:%M:%S.%f"),
    }


def generate_menu_row(item: dict) -> dict:
    return {
        "MENU_ID": item["menu_id"],
        "MENU_TYPE": item["menu_type"],
        "TRUCK_BRAND_NAME": item["brand"],
        "MENU_ITEM_ID": item["item_id"],
        "MENU_ITEM_NAME": item["name"],
        "ITEM_CATEGORY": item["cat"],
        "ITEM_SUBCATEGORY": item["subcat"],
        "COST_OF_GOODS_USD": item["cogs"],
        "SALE_PRICE_USD": item["price"],
        "INGESTED_AT": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"),
    }


def main():
    parser = argparse.ArgumentParser(description="Snowpipe Streaming v2 Producer for Tasty Bytes")
    parser.add_argument("--account", required=True, help="Snowflake account identifier (e.g. st83997)")
    parser.add_argument("--user", required=True, help="Snowflake user with RSA key-pair auth")
    parser.add_argument("--private-key-path", required=True, help="Path to PKCS8 private key file")
    parser.add_argument("--role", default="ACCOUNTADMIN", help="Snowflake role")
    parser.add_argument("--database", default="TASTY_BYTES_DEMO")
    parser.add_argument("--schema", default="DEV")
    parser.add_argument("--batches", type=int, default=10, help="Number of micro-batches to send")
    parser.add_argument("--batch-size", type=int, default=200, help="Rows per batch per table")
    parser.add_argument("--interval", type=float, default=5.0, help="Seconds between batches")
    args = parser.parse_args()

    private_key = load_private_key(args.private_key_path)

    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    from snowflake.connector import connect

    conn = connect(
        account=args.account,
        user=args.user,
        private_key=private_key_bytes,
        role=args.role,
        database=args.database,
        schema=args.schema,
    )

    print(f"Connected to {args.account} as {args.user}")
    print(f"Streaming {args.batches} batches × {args.batch_size} rows, {args.interval}s apart")
    print("=" * 70)

    cur = conn.cursor()

    menu_rows = [generate_menu_row(item) for item in MENU_ITEMS]
    cols = list(menu_rows[0].keys())
    values_clause = ", ".join([f"%({c})s" for c in cols])
    insert_sql = f"INSERT INTO {args.database}.{args.schema}.RAW_MENU ({', '.join(cols)}) VALUES ({values_clause})"
    cur.executemany(insert_sql, menu_rows)
    print(f"  Seeded {len(menu_rows)} menu items.")

    order_id_counter = 1
    customer_id_counter = 1
    event_id_counter = 1

    for batch_num in range(args.batches):
        batch_start = time.time()

        customers = [generate_customer(customer_id_counter + i) for i in range(args.batch_size // 4)]
        customer_id_counter += len(customers)
        if customers:
            cols = list(customers[0].keys())
            values_clause = ", ".join([f"%({c})s" for c in cols])
            sql = f"INSERT INTO {args.database}.{args.schema}.RAW_CUSTOMERS ({', '.join(cols)}) VALUES ({values_clause})"
            cur.executemany(sql, customers)

        orders = [generate_order(order_id_counter + i, customer_id_counter) for i in range(args.batch_size)]
        order_id_counter += len(orders)
        if orders:
            cols = list(orders[0].keys())
            values_clause = ", ".join([f"%({c})s" for c in cols])
            sql = f"INSERT INTO {args.database}.{args.schema}.RAW_ORDERS ({', '.join(cols)}) VALUES ({values_clause})"
            cur.executemany(sql, orders)

        clicks = [generate_clickstream(event_id_counter + i) for i in range(args.batch_size)]
        event_id_counter += len(clicks)
        if clicks:
            cols = list(clicks[0].keys())
            values_clause = ", ".join([f"%({c})s" for c in cols])
            sql = f"INSERT INTO {args.database}.{args.schema}.RAW_CLICKSTREAM ({', '.join(cols)}) VALUES ({values_clause})"
            cur.executemany(sql, clicks)

        elapsed = time.time() - batch_start
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"  [{ts}] Batch {batch_num+1}/{args.batches} — "
              f"{len(customers)} customers, {len(orders)} orders, {len(clicks)} clicks "
              f"({elapsed:.2f}s)")

        if batch_num < args.batches - 1:
            time.sleep(max(0, args.interval - elapsed))

    print("=" * 70)
    for table in ["RAW_ORDERS", "RAW_CUSTOMERS", "RAW_MENU", "RAW_CLICKSTREAM"]:
        cur.execute(f"SELECT COUNT(*) FROM {args.database}.{args.schema}.{table}")
        count = cur.fetchone()[0]
        print(f"  {table}: {count:,} rows")

    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
