#!/usr/bin/env python3
"""
Kafka Producer — Tasty Bytes Data Generator
=============================================
Generates fake POS, customer, and clickstream data and publishes
JSON messages to Kafka topics. Use with Apache NiFi or standalone.

NiFi Integration:
  - Use ExecuteScript processor with this script, OR
  - Use NiFi's GenerateFlowFile → JoltTransformJSON → PublishKafka_2_6

Standalone:
  pip install kafka-python faker
  python kafka_producer.py --bootstrap-servers localhost:9092 --batches 10 --interval 5

Topics created:
  - tasty_bytes.orders
  - tasty_bytes.customers
  - tasty_bytes.clickstream
  - tasty_bytes.menu
"""

import argparse
import json
import random
import time
from datetime import datetime, timedelta

from faker import Faker
from kafka import KafkaProducer

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


def gen_order(oid, max_cust):
    now = datetime.utcnow()
    item = random.choice(MENU_ITEMS)
    qty = random.randint(1, 5)
    amt = round(item["price"] * qty, 2)
    tax = round(amt * 0.08, 2)
    disc = round(amt * random.choice([0, 0, 0, 0.05, 0.10]), 2)
    country = random.choice(COUNTRIES)
    return {
        "ORDER_ID": oid, "TRUCK_ID": random.randint(1, 20), "LOCATION_ID": random.randint(1, 100),
        "CUSTOMER_ID": random.randint(1, max_cust), "ORDER_CHANNEL": random.choice(CHANNELS),
        "ORDER_TS": (now - timedelta(seconds=random.randint(0, 60))).isoformat(),
        "ORDER_CURRENCY": CURRENCIES[country], "ORDER_AMOUNT": amt, "ORDER_TAX_AMOUNT": tax,
        "ORDER_DISCOUNT_AMOUNT": disc, "ORDER_TOTAL": round(amt + tax - disc, 2),
        "SHIFT_ID": random.randint(1, 3), "INGESTED_AT": now.isoformat()
    }


def gen_customer(cid):
    now = datetime.utcnow()
    country = random.choice(COUNTRIES)
    return {
        "CUSTOMER_ID": cid, "FIRST_NAME": fake.first_name(), "LAST_NAME": fake.last_name(),
        "EMAIL": fake.email(), "PHONE_NUMBER": fake.phone_number(), "CITY": fake.city(),
        "COUNTRY": country, "POSTAL_CODE": fake.postcode(),
        "PREFERRED_LANGUAGE": "EN" if country in ["United States", "Canada", "United Kingdom"] else "OTHER",
        "GENDER": random.choice(["M", "F", "O"]), "MARITAL_STATUS": random.choice(["Single", "Married", "Divorced"]),
        "CHILDREN_COUNT": str(random.randint(0, 4)),
        "SIGN_UP_DATE": fake.date_between(start_date="-2y", end_date="today").isoformat(),
        "BIRTHDAY_DATE": fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
        "LOYALTY_POINTS": random.randint(0, 5000), "INGESTED_AT": now.isoformat()
    }


def gen_click(eid):
    now = datetime.utcnow()
    mem = random.choice(MEMBERSHIP_LEVELS)
    return {
        "EVENT_ID": eid,
        "CAPTURED_TIME": (now - timedelta(seconds=random.randint(0, 3600))).isoformat(),
        "USERNAME": fake.user_name(), "EMAIL": fake.email(),
        "ADDRESS": fake.address().replace("\n", ", "), "AVATAR": random.choice(AVATARS),
        "AVG_SESSION_LENGTH": round(random.uniform(5, 45), 2),
        "TIME_ON_APP": round(random.uniform(1, 30), 2),
        "TIME_ON_WEBSITE": round(random.uniform(1, 25), 2),
        "LENGTH_OF_MEMBERSHIP": round({"Free": random.uniform(0.1, 1), "Silver": random.uniform(1, 3), "Gold": random.uniform(2, 5), "Platinum": random.uniform(4, 8)}[mem], 2),
        "YEARLY_AMOUNT_SPENT": round({"Free": random.uniform(50, 200), "Silver": random.uniform(200, 500), "Gold": random.uniform(500, 1200), "Platinum": random.uniform(1000, 3000)}[mem], 2),
        "MEMBERSHIP_LEVEL": mem, "PAGE_URL": random.choice(PAGES),
        "REFERRER": random.choice(REFERRERS), "DEVICE_TYPE": random.choice(DEVICES),
        "COUNTRY": random.choice(COUNTRIES), "INGESTED_AT": now.isoformat()
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--batches", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--interval", type=float, default=5.0)
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all", retries=3, linger_ms=50, batch_size=32768,
    )

    for item in MENU_ITEMS:
        row = {"MENU_ID": item["menu_id"], "MENU_TYPE": item["menu_type"], "TRUCK_BRAND_NAME": item["brand"],
               "MENU_ITEM_ID": item["item_id"], "MENU_ITEM_NAME": item["name"], "ITEM_CATEGORY": item["cat"],
               "ITEM_SUBCATEGORY": item["subcat"], "COST_OF_GOODS_USD": item["cogs"],
               "SALE_PRICE_USD": item["price"], "INGESTED_AT": datetime.utcnow().isoformat()}
        producer.send("tasty_bytes.menu", key=str(item["item_id"]), value=row)
    producer.flush()
    print(f"Seeded {len(MENU_ITEMS)} menu items to tasty_bytes.menu")

    oid, cid, eid = 1, 1, 1
    for b in range(args.batches):
        t0 = time.time()
        n_cust = args.batch_size // 4
        for i in range(n_cust):
            producer.send("tasty_bytes.customers", key=str(cid), value=gen_customer(cid))
            cid += 1
        for i in range(args.batch_size):
            producer.send("tasty_bytes.orders", key=str(oid), value=gen_order(oid, cid))
            oid += 1
        for i in range(args.batch_size):
            producer.send("tasty_bytes.clickstream", key=str(eid), value=gen_click(eid))
            eid += 1
        producer.flush()
        elapsed = time.time() - t0
        print(f"  [{datetime.now().strftime('%H:%M:%S')}] Batch {b+1}/{args.batches} — "
              f"{n_cust} customers, {args.batch_size} orders, {args.batch_size} clicks ({elapsed:.2f}s)")
        if b < args.batches - 1:
            time.sleep(max(0, args.interval - elapsed))

    producer.close()
    print("Done. Messages are flowing through Kafka → Snowflake Kafka Connector → Iceberg tables.")


if __name__ == "__main__":
    main()
