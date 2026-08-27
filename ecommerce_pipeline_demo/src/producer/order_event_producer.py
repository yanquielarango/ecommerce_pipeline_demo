import argparse
import csv
import os
import random
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
from zerobus.sdk.sync import ZerobusSdk


DEFAULT_NUM_EVENTS = 20
DISCOUNT_CODES = ["SUMMER25", "WELCOME10", "FLASH15", None]


def parse_args():
    parser = argparse.ArgumentParser(description="Order Zerobus producer")

    parser.add_argument("--orders-path", default="data/orders/orders.csv", help="Path to orders.csv")
    parser.add_argument("--order-items-path", default="data/order_items/order_items.csv", help="Path to order_items.csv")

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--num-events", type=int, default=None, help=f"Number of events to send (default: {DEFAULT_NUM_EVENTS})")
    mode.add_argument("--continuous", action="store_true", help="Send events continuously until stopped with Ctrl+C")

    parser.add_argument("--min-batch-size", type=int, default=1)
    parser.add_argument("--max-batch-size", type=int, default=10)
    parser.add_argument("--min-batch-delay", type=float, default=0.3)
    parser.add_argument("--max-batch-delay", type=float, default=2.5)
    parser.add_argument("--include-discount-code", action="store_true", help="Include discount_code in generated events")
    parser.add_argument("--invalid-event", choices=["quantity-zero", "negative-price", "missing-customer-id"],
                         help="Inject one invalid event for data-quality testing")

    return parser.parse_args()


def validate_args(args):
    if args.num_events is not None and args.num_events <= 0:
        raise ValueError("num-events must be greater than 0")
    if args.min_batch_size <= 0:
        raise ValueError("min-batch-size must be greater than 0")
    if args.max_batch_size < args.min_batch_size:
        raise ValueError("max-batch-size must be greater than or equal to min-batch-size")
    if args.min_batch_delay < 0:
        raise ValueError("min-batch-delay cannot be negative")
    if args.max_batch_delay < args.min_batch_delay:
        raise ValueError("max-batch-delay must be greater than or equal to min-batch-delay")


def get_required_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} environment variable is required")
    return value


def load_orders(path):
    orders = {}
    with Path(path).open(newline="", encoding="utf-8") as file:
        for row in csv.DictReader(file):
            orders[row["order_id"]] = {"customer_id": row["customer_id"], "order_purchase_timestamp": row["order_purchase_timestamp"]}
    return orders


def load_order_items(path):
    items = defaultdict(lambda: {"quantity": 0, "price": 0.0})
    with Path(path).open(newline="", encoding="utf-8") as file:
        for row in csv.DictReader(file):
            key = (row["order_id"], row["product_id"])
            items[key]["quantity"] += 1
            items[key]["price"] += float(row["price"])
    return items


def build_order_events(orders, order_items):
    events = []

    for (order_id, product_id), item in order_items.items():
        order = orders.get(order_id)
        if not order:
            continue

        order_timestamp = datetime.strptime(order["order_purchase_timestamp"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

        events.append({
            "order_id": order_id,
            "customer_id": order["customer_id"],
            "product_id": product_id,
            "quantity": item["quantity"],
            "price": round(item["price"], 2),
            "order_timestamp": order_timestamp.isoformat(),
        })

    random.shuffle(events)
    return events


def make_invalid_event(event, invalid_event_type):
    payload = event.copy()

    if invalid_event_type == "quantity-zero":
        payload["quantity"] = 0
    elif invalid_event_type == "negative-price":
        payload["price"] = -10.0
    elif invalid_event_type == "missing-customer-id":
        payload["customer_id"] = None
    else:
        raise ValueError(f"Unsupported invalid event type: {invalid_event_type}")

    return payload


def create_zerobus_stream(server_endpoint, workspace_url, table_name, client_id, client_secret):
    sdk = ZerobusSdk(server_endpoint, workspace_url)
    table_properties = TableProperties(table_name)
    options = StreamConfigurationOptions(record_type=RecordType.JSON)
    return sdk.create_stream(client_id, client_secret, table_properties, options)


def send_events(stream, events, args):
    sent_count = 0
    event_index = 0
    batch_number = 0
    invalid_event_injected = False

    try:
        while args.continuous or event_index < len(events):
            if event_index >= len(events):
                event_index = 0
                random.shuffle(events)

            batch_size = random.randint(args.min_batch_size, args.max_batch_size)
            batch = events[event_index:event_index + batch_size]
            batch_number += 1

            for event in batch:
                payload = event.copy()

                if args.invalid_event and not invalid_event_injected:
                    payload = make_invalid_event(payload, args.invalid_event)
                    invalid_event_injected = True
                    print(f"Injected invalid event: {args.invalid_event}")

            
                payload["discount_code"] = random.choice(DISCOUNT_CODES) if args.include_discount_code else None
                payload["ingest_datetime"] = datetime.now(timezone.utc).isoformat()

                offset = stream.ingest_record_offset(payload)
                stream.wait_for_offset(offset)
                sent_count += 1

            event_index += len(batch)
            print(f"Batch {batch_number}: {len(batch)} events sent (total: {sent_count})")

            if args.continuous or event_index < len(events):
                time.sleep(random.uniform(args.min_batch_delay, args.max_batch_delay))

    except KeyboardInterrupt:
        print("\nProducer stopped by user.")

    return sent_count


def main():
    args = parse_args()
    validate_args(args)

    server_endpoint = get_required_env("ZEROBUS_SERVER_ENDPOINT")
    workspace_url = get_required_env("DATABRICKS_WORKSPACE_URL")
    client_id = get_required_env("DATABRICKS_CLIENT_ID")
    client_secret = get_required_env("DATABRICKS_CLIENT_SECRET")
    table_name = os.environ.get("ZEROBUS_TABLE_NAME", "dbr_dev.ecommerce_bronze.brz_orders")

    orders = load_orders(args.orders_path)
    order_items = load_order_items(args.order_items_path)
    source_events = build_order_events(orders, order_items)

    if not source_events:
        raise RuntimeError("No order events were generated from the source files")

    if args.continuous:
        events = source_events
        print("Mode: continuous")
        print(f"Source events: {len(events)}")
        print("Press Ctrl+C to stop")
    else:
        num_events = args.num_events if args.num_events is not None else DEFAULT_NUM_EVENTS

        if num_events > len(source_events):
            raise ValueError(f"Requested {num_events} events, but only {len(source_events)} are available")

        events = source_events[:num_events]
        print("Mode: finite")
        print(f"Events to send: {len(events)}")

    print(f"Target: {table_name}")
    print(f"Batch size: {args.min_batch_size}-{args.max_batch_size}")

    if args.include_discount_code:
        print("Schema evolution: discount_code enabled")
    if args.invalid_event:
        print(f"DQ test: {args.invalid_event}")

    stream = create_zerobus_stream(server_endpoint, workspace_url, table_name, client_id, client_secret)

    try:
        sent_count = send_events(stream, events, args)
    finally:
        stream.close()

    print(f"\nProducer finished: {sent_count} events sent")


if __name__ == "__main__":
    main()