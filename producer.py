"""
ITO5202 Assessment 2 - Kafka producer 
Student ID: 29701201

Replays the held-out streaming subset data/stream_data.parquet through Kafka, to simulate orders
arriving in real time.

- Reads the streaming subset in batches (default 500 records, set with --batch-size).
- Adds an `event_timestamp` to every record: the UTC time the batch is published.
- Serialises each batch as ONE JSON array and publishes it as one Kafka message.
- Pauses exactly 5 seconds between batches.
- Logs the record count and publish timestamp of every batch to stdout.

The file is read as saved; the data is never re-split here.

Run from the project folder, inside the Jupyter/Spark container (same Docker
network as Kafka):

    python producer.py                    # full replay, 500 records per batch
    python producer.py --batch-size 200   # smaller batches
    python producer.py --max-batches 3    # short test run
    python producer.py --dry-run          # print the first record, no Kafka needed
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone

import pandas as pd

# ---------------------------------------------------------------------------
# Fields sent for each order
# ---------------------------------------------------------------------------
# The 16 model inputs (A.2 / A.5 REQUIRED_INPUT_COLS)
CATEGORICAL_FEATURES = ["customer_state", "main_seller_state", "main_category", "payment_type"]
NUMERIC_FEATURES = [
    "distance_km", "total_price", "total_freight", "total_weight_g",
    "total_volume_cm3", "shipping_limit_days", "approval_hours",          # log-transformed in the pipeline
    "same_state", "n_items", "n_sellers", "payment_installments",
    "estimated_delivery_days",                                            # used as they are
]
INTEGER_FIELDS = {"same_state", "n_items", "n_sellers", "payment_installments", "is_late"}

# Order identifier, the original purchase time, and the actual outcome.
# `is_late` is NOT a model input: it is sent only so the consumer can compare
# predicted and actual late orders per window. The other outcome columns
# (delivery_days, delay_days, review_score) are not sent.
OUTGOING_FIELDS = (["order_id", "order_purchase_timestamp"]
                   + CATEGORICAL_FEATURES + NUMERIC_FEATURES + ["is_late"])

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"      # matches the consumer's to_timestamp format


def parse_args():
    p = argparse.ArgumentParser(description="Replay the streaming subset through Kafka.")
    p.add_argument("--bootstrap", default="kafka:9092",
                   help="Kafka bootstrap server (default: kafka:9092, the container name on the Docker network)")
    p.add_argument("--topic", default="events", help="Kafka topic (default: events)")
    p.add_argument("--data", default="data/stream_data.parquet",
                   help="Streaming subset saved before Part A (default: data/stream_data.parquet)")
    p.add_argument("--batch-size", type=int, default=500,
                   help="Records per batch, 100-1000 (default: 500)")
    p.add_argument("--interval", type=float, default=5.0,
                   help="Seconds to pause between batches (default: 5, as the brief requires)")
    p.add_argument("--max-batches", type=int, default=None,
                   help="Stop after this many batches (default: send the whole subset)")
    p.add_argument("--dry-run", action="store_true",
                   help="Print the first record of the first batch and exit, without Kafka")
    args = p.parse_args()
    if not 100 <= args.batch_size <= 1000:
        p.error("--batch-size must be between 100 and 1000")
    return args


def log(message):
    '''Print a log line with the current UTC time, flushed immediately.'''
    print(f"[{datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT)} UTC] {message}", flush=True)


def load_stream(path):
    '''Read the streaming subset and keep only the fields that are sent.'''
    df = pd.read_parquet(path)
    missing = [c for c in OUTGOING_FIELDS if c not in df.columns]
    if missing:
        sys.exit(f"{path} is missing columns: {missing}")
    df = df[OUTGOING_FIELDS].sort_values(["order_purchase_timestamp", "order_id"])
    df["order_purchase_timestamp"] = df["order_purchase_timestamp"].dt.strftime(TIMESTAMP_FORMAT)
    return df.reset_index(drop=True)


def to_records(batch_df, event_time):
    '''Convert a batch to a list of JSON-safe dicts, adding event_timestamp.
    Missing values become null (JSON has no NaN), integers stay integers.'''
    records = []
    for row in batch_df.to_dict(orient="records"):
        rec = {}
        for k, v in row.items():
            if pd.isna(v):
                rec[k] = None
            elif k in INTEGER_FIELDS:
                rec[k] = int(v)
            elif isinstance(v, float):
                rec[k] = float(v)
            else:
                rec[k] = v
        rec["event_timestamp"] = event_time
        records.append(rec)
    return records


def make_producer(bootstrap, topic):
    '''Connect to Kafka and make sure the topic exists.
    Works with kafka-python 2.x and 3.x (some error classes were renamed in 3.x).'''
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    # 1. Create the topic (1 partition) if it does not exist yet
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="a2-producer-admin")
    except Exception as exc:
        sys.exit(f"Cannot reach Kafka at {bootstrap} ({type(exc).__name__}). Is the kafka "
                 f"container running and on the same Docker network as this container?")
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
        log(f"Created topic '{topic}' (1 partition)")
    except TopicAlreadyExistsError:
        log(f"Topic '{topic}' already exists")
    except Exception as exc:
        # Not fatal: Kafka creates the topic automatically on the first send
        log(f"Could not create topic explicitly ({type(exc).__name__}); "
            f"relying on Kafka to create it on first send")
    finally:
        admin.close()

    # 2. The producer itself
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda batch: json.dumps(batch).encode("utf-8"),  # list -> JSON array
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",                       # wait until Kafka has stored the batch
        max_request_size=2 * 1024 * 1024, # a 1,000-record batch is well under this
    )


def main():
    args = parse_args()
    df = load_stream(args.data)
    n_batches = -(-len(df) // args.batch_size)          # ceiling division
    if args.max_batches:
        n_batches = min(n_batches, args.max_batches)

    log(f"Loaded {len(df):,} orders from {args.data}")
    log(f"Plan: {n_batches} batches of up to {args.batch_size} records, "
        f"{args.interval:g} s apart, to topic '{args.topic}' on {args.bootstrap}")

    if args.dry_run:
        sample = to_records(df.head(1), datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT))
        print(json.dumps(sample, indent=2))
        return

    producer = make_producer(args.bootstrap, args.topic)
    sent, batches_sent = 0, 0
    try:
        for b in range(n_batches):
            batch_df = df.iloc[b * args.batch_size:(b + 1) * args.batch_size]
            event_time = datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT)
            records = to_records(batch_df, event_time)

            meta = producer.send(args.topic, key=f"batch-{b + 1}", value=records).get(timeout=30)
            sent += len(records)
            batches_sent += 1
            log(f"Batch {b + 1}/{n_batches}: {len(records)} records published "
                f"(event_timestamp={event_time}, partition={meta.partition}, offset={meta.offset}, "
                f"total sent={sent:,})")

            if b < n_batches - 1:
                time.sleep(args.interval)   # exactly 5 s between batches
    except KeyboardInterrupt:
        log("Interrupted by user")
    finally:
        producer.flush()
        producer.close()
        log(f"Finished: {sent:,} records sent in {batches_sent} batches")


if __name__ == "__main__":
    main()