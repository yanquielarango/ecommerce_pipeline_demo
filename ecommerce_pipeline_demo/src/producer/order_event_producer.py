import argparse
import csv
import os
import random
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from zerobus.sdk.shared import (
    RecordType,
    StreamConfigurationOptions,
    TableProperties,
)
from zerobus.sdk.sync import ZerobusSdk


DISCOUNT_CODES = [
    "SUMMER25",
    "WELCOME10",
    "FLASH15",
    None,
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Order Zerobus producer"
    )

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
        help="Total number of order events to send",
    )

    parser.add_argument(
        "--min-batch-size",
        type=int,
        default=1,
        help="Minimum number of events per batch",
    )

    parser.add_argument(
        "--max-batch-size",
        type=int,
        default=10,
        help="Maximum number of events per batch",
    )

    parser.add_argument(
        "--min-batch-delay",
        type=float,
        default=0.3,
        help="Minimum delay between batches in seconds",
    )

    parser.add_argument(
        "--max-batch-delay",
        type=float,
        default=2.5,
        help="Maximum delay between batches in seconds",
    )

    parser.add_argument(
        "--include-discount-code",
        action="store_true",
        help="Add random discount_code values to generated events",
    )

    return parser.parse_args()


def get_required_env(name: str) -> str:
    value = os.environ.get(name)

    if not value:
        raise RuntimeError(
            f"{name} environment variable is required"
        )

    return value


def load_orders(path: str) -> dict:
    orders = {}

    with Path(path).open(
        newline="",
        encoding="utf-8",
    ) as file:
        reader = csv.DictReader(file)

        for row in reader:
            orders[row["order_id"]] = {
                "customer_id": row["customer_id"],
                "order_purchase_timestamp": row[
                    "order_purchase_timestamp"
                ],
            }

    return orders


def load_order_items(path: str) -> dict:
    grouped_items = defaultdict(
        lambda: {
            "quantity": 0,
            "price": 0.0,
        }
    )

    with Path(path).open(
        newline="",
        encoding="utf-8",
    ) as file:
        reader = csv.DictReader(file)

        for row in reader:
            key = (
                row["order_id"],
                row["product_id"],
            )

            grouped_items[key]["quantity"] += 1
            grouped_items[key]["price"] += float(
                row["price"]
            )

    return grouped_items


def build_order_events(
    orders: dict,
    order_items: dict,
    num_events: int,
) -> list[dict]:
    events = []

    for (
        order_id,
        product_id,
    ), item_data in order_items.items():

        order = orders.get(order_id)

        if not order:
            continue

        timestamp = datetime.strptime(
            order["order_purchase_timestamp"],
            "%Y-%m-%d %H:%M:%S",
        ).replace(
            tzinfo=timezone.utc
        )

        events.append(
            {
                "order_id": order_id,
                "customer_id": order["customer_id"],
                "product_id": product_id,
                "quantity": item_data["quantity"],
                "price": round(
                    item_data["price"],
                    2,
                ),
                "order_timestamp": timestamp.isoformat(),
            }
        )

    random.shuffle(events)

    return events[:num_events]


def create_zerobus_stream(
    server_endpoint: str,
    workspace_url: str,
    table_name: str,
    client_id: str,
    client_secret: str,
):
    sdk = ZerobusSdk(
        server_endpoint,
        workspace_url,
    )

    table_properties = TableProperties(
        table_name
    )

    options = StreamConfigurationOptions(
        record_type=RecordType.JSON
    )

    return sdk.create_stream(
        client_id,
        client_secret,
        table_properties,
        options,
    )


def send_events(
    events: list[dict],
    server_endpoint: str,
    workspace_url: str,
    table_name: str,
    client_id: str,
    client_secret: str,
    min_batch_size: int,
    max_batch_size: int,
    min_batch_delay: float,
    max_batch_delay: float,
    include_discount_code: bool,
) -> int:

    stream = create_zerobus_stream(
        server_endpoint=server_endpoint,
        workspace_url=workspace_url,
        table_name=table_name,
        client_id=client_id,
        client_secret=client_secret,
    )

    sent_count = 0
    event_index = 0
    batch_number = 0

    try:
        while event_index < len(events):

            # Random number of events for this batch
            batch_size = random.randint(
                min_batch_size,
                max_batch_size,
            )

            # Avoid exceeding the total number of events
            batch = events[
                event_index:
                event_index + batch_size
            ]

            batch_number += 1

            print(
                f"\nBatch {batch_number}: "
                f"sending {len(batch)} events"
            )

            for event in batch:
                payload = event.copy()

                if include_discount_code:
                    payload["discount_code"] = random.choice(
                        DISCOUNT_CODES
                    )
                else:
                    payload["discount_code"] = None

                payload["ingest_datetime"] = (
                    datetime.now(
                        timezone.utc
                    ).isoformat()
                )

                offset = stream.ingest_record_offset(
                    payload
                )

                stream.wait_for_offset(
                    offset
                )

                sent_count += 1

                print(
                    f"Sent order={payload['order_id']} "
                    f"product={payload['product_id']} "
                    f"quantity={payload['quantity']} "
                    f"price={payload['price']} "
                    f"discount={payload['discount_code']} "
                    f"offset={offset}"
                )

            event_index += len(batch)

            print(
                f"Progress: "
                f"{sent_count}/{len(events)} events"
            )

            # Only wait if there are still events remaining
            if event_index < len(events):

                delay = random.uniform(
                    min_batch_delay,
                    max_batch_delay,
                )

                print(
                    f"Waiting {delay:.2f}s "
                    "before next batch..."
                )

                time.sleep(
                    delay
                )

    finally:
        stream.close()

    return sent_count


def main():
    args = parse_args()

    if args.num_events <= 0:
        raise ValueError(
            "num-events must be greater than 0"
        )

    if args.min_batch_size <= 0:
        raise ValueError(
            "min-batch-size must be greater than 0"
        )

    if args.max_batch_size < args.min_batch_size:
        raise ValueError(
            "max-batch-size must be greater than "
            "or equal to min-batch-size"
        )

    if args.min_batch_delay < 0:
        raise ValueError(
            "min-batch-delay cannot be negative"
        )

    if args.max_batch_delay < args.min_batch_delay:
        raise ValueError(
            "max-batch-delay must be greater than "
            "or equal to min-batch-delay"
        )

    server_endpoint = get_required_env(
        "ZEROBUS_SERVER_ENDPOINT"
    )

    workspace_url = get_required_env(
        "DATABRICKS_WORKSPACE_URL"
    )

    client_id = get_required_env(
        "DATABRICKS_CLIENT_ID"
    )

    client_secret = get_required_env(
        "DATABRICKS_CLIENT_SECRET"
    )

    table_name = os.environ.get(
        "ZEROBUS_TABLE_NAME",
        "dbr_dev.ecommerce_bronze.brz_orders",
    )

    orders = load_orders(
        args.orders_path
    )

    order_items = load_order_items(
        args.order_items_path
    )

    events = build_order_events(
        orders=orders,
        order_items=order_items,
        num_events=args.num_events,
    )

    print(
        f"Loaded {len(events)} events"
    )

    print(
        f"Target table: {table_name}"
    )

    print(
        f"Batch size: "
        f"{args.min_batch_size}-"
        f"{args.max_batch_size}"
    )

    print(
        f"Batch delay: "
        f"{args.min_batch_delay}-"
        f"{args.max_batch_delay}s"
    )

    sent_count = send_events(
        events=events,
        server_endpoint=server_endpoint,
        workspace_url=workspace_url,
        table_name=table_name,
        client_id=client_id,
        client_secret=client_secret,
        min_batch_size=args.min_batch_size,
        max_batch_size=args.max_batch_size,
        min_batch_delay=args.min_batch_delay,
        max_batch_delay=args.max_batch_delay,
        include_discount_code=args.include_discount_code,
    )

    print(
        f"\nCompleted: {sent_count} events sent "
        f"(discount_code="
        f"{args.include_discount_code})"
    )


if __name__ == "__main__":
    main()