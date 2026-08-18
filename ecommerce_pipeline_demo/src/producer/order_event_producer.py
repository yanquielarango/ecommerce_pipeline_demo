import argparse
import csv
import json
import os
import random
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from azure.eventhub import EventData, EventHubProducerClient

DISCOUNT_CODES = ["SUMMER25", "WELCOME10", "FLASH15", None]


def parse_args():
    parser = argparse.ArgumentParser(description="Order Event Hub producer")

    parser.add_argument(
        "--orders-path",
        default="data/orders/orders.csv",
        help="Path to orders.csv",
    )

    parser.add_argument(
        "--order-items-path",
        default="data/order_items/order_items.csv",
        help="Path to order_items.csv",
    )

    parser.add_argument(
        "--num-events",
        type=int,
        default=20,
        help="Number of order events to send",
    )

    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.0,
        help="Delay between events",
    )

    parser.add_argument(
        "--include-discount-code",
        action="store_true",
        help="Add discount_code to generated events",
    )

    return parser.parse_args()


def load_orders(path: str) -> dict:
    orders = {}

    with Path(path).open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            orders[row["order_id"]] = {
                "customer_id": row["customer_id"],
                "order_purchase_timestamp": row["order_purchase_timestamp"],
            }

    return orders


def load_order_items(path: str) -> dict:
    grouped_items = defaultdict(
        lambda: {
            "quantity": 0,
            "price": 0.0,
        }
    )

    with Path(path).open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            key = (
                row["order_id"],
                row["product_id"],
            )

            grouped_items[key]["quantity"] += 1
            grouped_items[key]["price"] += float(row["price"])

    return grouped_items


def build_order_events(
    orders: dict,
    order_items: dict,
    num_events: int,
) -> list[dict]:
    events = []

    for (order_id, product_id), item_data in order_items.items():
        order = orders.get(order_id)

        if not order:
            continue

        timestamp = datetime.strptime(
            order["order_purchase_timestamp"],
            "%Y-%m-%d %H:%M:%S",
        )

        events.append(
            {
                "order_id": order_id,
                "customer_id": order["customer_id"],
                "product_id": product_id,
                "quantity": item_data["quantity"],
                "price": round(item_data["price"], 2),
                "order_timestamp": timestamp.strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
            }
        )

    random.shuffle(events)

    return events[:num_events]


def send_events(
    events: list[dict],
    connection_string: str,
    eventhub_name: str,
    sleep_seconds: float,
    include_discount_code: bool,
) -> int:
    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_string,
        eventhub_name=eventhub_name,
    )

    sent_count = 0

    try:
        for event in events:
            if include_discount_code:
                event["discount_code"] = random.choice(DISCOUNT_CODES)

            batch = producer.create_batch()
            batch.add(EventData(json.dumps(event)))
            producer.send_batch(batch)

            sent_count += 1

            print(
                f"Sent order={event['order_id']} "
                f"product={event['product_id']} "
                f"quantity={event['quantity']}"
            )

            time.sleep(sleep_seconds)

    finally:
        producer.close()

    return sent_count


def main():
    args = parse_args()

    if args.num_events <= 0:
        raise ValueError("num-events must be greater than 0")

    if args.sleep_seconds < 0:
        raise ValueError("sleep-seconds cannot be negative")

    connection_string = os.environ.get(
        "EVENTHUB_CONNECTION_STRING"
    )

    eventhub_name = os.environ.get(
        "EVENTHUB_NAME",
        "evh_brazilian_ecommerce",
    )

    if not connection_string:
        raise RuntimeError(
            "EVENTHUB_CONNECTION_STRING environment variable is required"
        )

    orders = load_orders(args.orders_path)
    order_items = load_order_items(args.order_items_path)

    events = build_order_events(
        orders=orders,
        order_items=order_items,
        num_events=args.num_events,
    )

    print(f"Loaded {len(events)} events")

    sent_count = send_events(
        events=events,
        connection_string=connection_string,
        eventhub_name=eventhub_name,
        sleep_seconds=args.sleep_seconds,
        include_discount_code=args.include_discount_code,
    )

    print(
        f"Completed: {sent_count} events sent "
        f"(discount_code={args.include_discount_code})"
    )


if __name__ == "__main__":
    main()