# NiFi Flow — Tasty Bytes Data Generation → Kafka → Snowflake
# =============================================================
#
# This describes a NiFi flow that generates fake POS, customer,
# and clickstream data, publishes to Kafka, where the Snowflake
# Kafka Connector picks it up and streams into Iceberg tables.
#
# ARCHITECTURE:
#
#   ┌─────────────┐     ┌───────────┐     ┌──────────────────────┐     ┌─────────────────────┐
#   │  NiFi Flow  │────►│   Kafka   │────►│ Kafka Connect        │────►│ Snowflake Iceberg   │
#   │  (Generate  │     │  Topics   │     │ (Snowflake Sink      │     │ Tables (RAW_ORDERS, │
#   │   + Route)  │     │           │     │  Snowpipe Streaming) │     │  RAW_CUSTOMERS, ..) │
#   └─────────────┘     └───────────┘     └──────────────────────┘     └─────────────────────┘
#
# ═══════════════════════════════════════════════════════════════
# OPTION A: NiFi Processors (drag-and-drop, no code)
# ═══════════════════════════════════════════════════════════════
#
# Processor 1: GenerateFlowFile (POS Orders)
#   Schedule: 5 sec
#   Properties:
#     Custom Text: (leave empty — we use InvokeScriptedProcessor next)
#     Batch Size: 1
#
# OR — use ExecuteScript for full control:
#
# Processor 1: ExecuteScript (orders_generator)
#   Script Engine: python
#   Script Body:
#     import json, random, time
#     from datetime import datetime, timedelta
#     from org.apache.nifi.processor.io import StreamCallback
#     ... (see kafka_producer.py for data generation logic)
#
# ═══════════════════════════════════════════════════════════════
# OPTION B (RECOMMENDED): NiFi Flow — No-Code Approach
# ═══════════════════════════════════════════════════════════════
#
# This is the simplest NiFi flow for the demo:
#
# ┌────────────────────┐     ┌──────────────────────┐     ┌────────────────────┐
# │ GenerateFlowFile   │────►│ JoltTransformJSON    │────►│ PublishKafka_2_6   │
# │ (Timer: 5 sec)     │     │ (Add INGESTED_AT,    │     │                    │
# │                    │     │  randomize fields)   │     │ Topic: route by    │
# │ Custom Text:       │     │                      │     │ attribute          │
# │ (JSON template)    │     │                      │     │                    │
# └────────────────────┘     └──────────────────────┘     └────────────────────┘
#
# ═══════════════════════════════════════════════════════════════
# DETAILED PROCESSOR CONFIGS:
# ═══════════════════════════════════════════════════════════════
#
# ── FLOW 1: Orders ──────────────────────────────────────────
#
# [GenerateFlowFile - Orders]
#   Scheduling Strategy: Timer Driven
#   Run Schedule: 5 sec
#   Batch Size: 200
#   Custom Text:
#     {"ORDER_ID":"${UUID()}","TRUCK_ID":${random():mod(20):plus(1)},
#      "LOCATION_ID":${random():mod(100):plus(1)},
#      "CUSTOMER_ID":${random():mod(150):plus(1)},
#      "ORDER_CHANNEL":"${random():mod(4):equals(0):ifElse('Walk-Up',
#        random():mod(4):equals(1):ifElse('Mobile App',
#        random():mod(4):equals(2):ifElse('Web','Third Party')))}",
#      "ORDER_TS":"${now():format('yyyy-MM-dd HH:mm:ss.SSS')}",
#      "ORDER_CURRENCY":"USD",
#      "ORDER_AMOUNT":${random():mod(5000):plus(500):divide(100)},
#      "ORDER_TOTAL":${random():mod(6000):plus(600):divide(100)},
#      "SHIFT_ID":${random():mod(3):plus(1)},
#      "INGESTED_AT":"${now():format('yyyy-MM-dd HH:mm:ss.SSS')}"}
#
#   → connect to:
#
# [UpdateAttribute - Orders]
#   kafka.topic: tasty_bytes.orders
#   kafka.key: ${UUID()}
#
#   → connect to:
#
# [PublishKafka_2_6 - Orders]
#   Kafka Brokers: your-kafka-broker:9092
#   Topic Name: ${kafka.topic}
#   Message Key Field: ${kafka.key}
#   Delivery Guarantee: GUARANTEE_REPLICATED_DELIVERY
#   Max Message Size: 1 MB
#   Compression Type: snappy
#
# ── FLOW 2: Customers ──────────────────────────────────────
#
# [GenerateFlowFile - Customers]
#   Run Schedule: 5 sec
#   Batch Size: 50
#   Custom Text:
#     {"CUSTOMER_ID":${nextInt()},
#      "FIRST_NAME":"John","LAST_NAME":"Doe",
#      "EMAIL":"john.doe@example.com",
#      "PHONE_NUMBER":"555-0100",
#      "CITY":"Denver","COUNTRY":"United States",
#      "POSTAL_CODE":"80202","PREFERRED_LANGUAGE":"EN",
#      "GENDER":"M","MARITAL_STATUS":"Single",
#      "CHILDREN_COUNT":"0",
#      "SIGN_UP_DATE":"${now():format('yyyy-MM-dd')}",
#      "BIRTHDAY_DATE":"1990-01-15",
#      "LOYALTY_POINTS":${random():mod(5000)},
#      "INGESTED_AT":"${now():format('yyyy-MM-dd HH:mm:ss.SSS')}"}
#
#   → UpdateAttribute (kafka.topic = tasty_bytes.customers)
#   → PublishKafka_2_6
#
# ── FLOW 3: Clickstream ────────────────────────────────────
#
# [GenerateFlowFile - Clickstream]
#   Run Schedule: 2 sec  (higher frequency for web events)
#   Batch Size: 200
#   Custom Text:
#     {"EVENT_ID":${nextInt()},
#      "CAPTURED_TIME":"${now():format('yyyy-MM-dd HH:mm:ss.SSS')}",
#      "USERNAME":"user_${random():mod(1000)}",
#      "EMAIL":"user${random():mod(1000)}@tasty.com",
#      "ADDRESS":"123 Main St, Denver, CO",
#      "AVATAR":"avatar_default.png",
#      "AVG_SESSION_LENGTH":${random():mod(4000):plus(500):divide(100)},
#      "TIME_ON_APP":${random():mod(3000):plus(100):divide(100)},
#      "TIME_ON_WEBSITE":${random():mod(2500):plus(100):divide(100)},
#      "LENGTH_OF_MEMBERSHIP":${random():mod(800):plus(10):divide(100)},
#      "YEARLY_AMOUNT_SPENT":${random():mod(300000):plus(5000):divide(100)},
#      "MEMBERSHIP_LEVEL":"Gold",
#      "PAGE_URL":"/menu",
#      "REFERRER":"google.com",
#      "DEVICE_TYPE":"Mobile",
#      "COUNTRY":"United States",
#      "INGESTED_AT":"${now():format('yyyy-MM-dd HH:mm:ss.SSS')}"}
#
#   → UpdateAttribute (kafka.topic = tasty_bytes.clickstream)
#   → PublishKafka_2_6
#
# ═══════════════════════════════════════════════════════════════
# OPTION C: Use the standalone Python Kafka producer
# ═══════════════════════════════════════════════════════════════
#
# If NiFi feels heavy for the demo, use kafka_producer.py directly:
#
#   python kafka_producer.py \
#     --bootstrap-servers your-kafka-broker:9092 \
#     --batches 10 --batch-size 200 --interval 5
#
# ═══════════════════════════════════════════════════════════════
# KAFKA TOPIC SETUP (run once):
# ═══════════════════════════════════════════════════════════════
#
#   kafka-topics.sh --create --topic tasty_bytes.orders \
#     --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1
#
#   kafka-topics.sh --create --topic tasty_bytes.customers \
#     --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1
#
#   kafka-topics.sh --create --topic tasty_bytes.clickstream \
#     --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1
#
#   kafka-topics.sh --create --topic tasty_bytes.menu \
#     --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
#
#   kafka-topics.sh --create --topic tasty_bytes_dlq \
#     --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
#
# ═══════════════════════════════════════════════════════════════
# DEPLOY KAFKA CONNECT SNOWFLAKE SINK:
# ═══════════════════════════════════════════════════════════════
#
#   # Install the connector (one-time):
#   confluent-hub install snowflakeinc/snowflake-kafka-connector:2.4.0
#
#   # Deploy the connector config:
#   curl -X POST http://localhost:8083/connectors \
#     -H "Content-Type: application/json" \
#     -d @kafka-connect-snowflake-sink.json
#
#   # Check status:
#   curl http://localhost:8083/connectors/snowflake-iceberg-sink-orders/status
#
# ═══════════════════════════════════════════════════════════════
