import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List

import pandas as pd
import yfinance as yf
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [producer] %(message)s",
)


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_prices")
STOCKS = [
    symbol.strip().upper()
    for symbol in os.getenv("STOCKS", "AAPL,GOOGL,MSFT,AMZN,TSLA").split(",")
    if symbol.strip()
]
FETCH_INTERVAL_SECONDS = int(os.getenv("FETCH_INTERVAL_SECONDS", "10"))
MAX_BACKOFF_SECONDS = int(os.getenv("KAFKA_MAX_BACKOFF_SECONDS", "60"))


def wait_for_kafka(topic: str) -> None:
    backoff_seconds = 1
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    while True:
        try:
            metadata = admin_client.list_topics(timeout=5)
            if topic not in metadata.topics:
                logging.info("Kafka topic %s is missing, creating it.", topic)
                futures = admin_client.create_topics(
                    [NewTopic(topic, num_partitions=1, replication_factor=1)]
                )
                for _, future in futures.items():
                    try:
                        future.result()
                    except Exception as exc:  # noqa: BLE001
                        if "TOPIC_ALREADY_EXISTS" not in str(exc):
                            raise
            logging.info("Connected to Kafka broker at %s.", KAFKA_BOOTSTRAP_SERVERS)
            return
        except Exception as exc:  # noqa: BLE001
            logging.warning(
                "Kafka is not ready yet (%s). Retrying in %s seconds.",
                exc,
                backoff_seconds,
            )
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, MAX_BACKOFF_SECONDS)


def delivery_report(err, msg) -> None:
    if err is not None:
        logging.error("Delivery failed for key=%s: %s", msg.key(), err)


def _extract_snapshot(frame: pd.DataFrame, symbol: str) -> Dict[str, object] | None:
    cleaned = frame.dropna(how="all")
    if cleaned.empty:
        return None

    latest_row = cleaned.iloc[-1]
    price = latest_row.get("Close")
    volume_series = cleaned["Volume"].fillna(0)
    non_zero_volume = volume_series[volume_series > 0]
    volume = non_zero_volume.iloc[-1] if not non_zero_volume.empty else volume_series.iloc[-1]
    if pd.isna(price) or pd.isna(volume):
        return None

    if float(volume) <= 0:
        logging.warning("Skipping %s because Yahoo Finance returned a non-positive volume.", symbol)
        return None

    sampled_at = datetime.now(timezone.utc)

    return {
        "symbol": symbol,
        "price": round(float(price), 2),
        "volume": int(volume),
        "timestamp": sampled_at.isoformat().replace("+00:00", "Z"),
    }


def fetch_latest_snapshots(symbols: List[str]) -> List[Dict[str, object]]:
    dataset = yf.download(
        tickers=" ".join(symbols),
        period="1d",
        interval="1m",
        auto_adjust=False,
        prepost=True,
        group_by="ticker",
        progress=False,
        threads=True,
    )

    if dataset.empty:
        logging.warning("Yahoo Finance returned no rows for the requested tickers.")
        return []

    snapshots: List[Dict[str, object]] = []
    multi_index_columns = isinstance(dataset.columns, pd.MultiIndex)

    for symbol in symbols:
        if multi_index_columns:
            if symbol not in dataset.columns.get_level_values(0):
                logging.warning("Ticker %s is missing from the Yahoo Finance response.", symbol)
                continue
            symbol_frame = dataset[symbol]
        else:
            symbol_frame = dataset

        snapshot = _extract_snapshot(symbol_frame, symbol)
        if snapshot is not None:
            snapshots.append(snapshot)

    return snapshots


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "stock-market-producer",
        }
    )


def main() -> None:
    logging.info("Starting stock producer for tickers: %s", ", ".join(STOCKS))
    wait_for_kafka(KAFKA_TOPIC)
    producer = build_producer()

    while True:
        iteration_started_at = time.time()
        try:
            snapshots = fetch_latest_snapshots(STOCKS)
            if not snapshots:
                logging.warning("No stock snapshots were produced in this cycle.")

            for snapshot in snapshots:
                producer.produce(
                    KAFKA_TOPIC,
                    key=snapshot["symbol"],
                    value=json.dumps(snapshot),
                    on_delivery=delivery_report,
                )
                producer.poll(0)

            producer.flush(10)
            logging.info("Published %s stock snapshots to topic %s.", len(snapshots), KAFKA_TOPIC)
        except Exception as exc:  # noqa: BLE001
            logging.exception("Producer iteration failed: %s", exc)

        elapsed = time.time() - iteration_started_at
        sleep_for = max(0, FETCH_INTERVAL_SECONDS - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
