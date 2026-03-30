import json
import logging
import os
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread
from typing import Any, Dict

import psycopg
from psycopg import sql
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [consumer] %(message)s",
)


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_prices")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "stock-price-postgres-writer")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "stocks")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")
HEALTH_PORT = int(os.getenv("CONSUMER_HEALTH_PORT", "8081"))
MAX_BACKOFF_SECONDS = int(os.getenv("CONSUMER_MAX_BACKOFF_SECONDS", "60"))

APPLICATION_STATE: Dict[str, Any] = {
    "ready": False,
    "db_connected": False,
    "kafka_connected": False,
    "last_message_at": None,
}
STATE_LOCK = Lock()


def set_state(**kwargs: Any) -> None:
    with STATE_LOCK:
        APPLICATION_STATE.update(kwargs)


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/health":
            self.send_response(404)
            self.end_headers()
            return

        with STATE_LOCK:
            ready = bool(APPLICATION_STATE["ready"])
            payload = {
                "status": "ok" if ready else "starting",
                "db_connected": APPLICATION_STATE["db_connected"],
                "kafka_connected": APPLICATION_STATE["kafka_connected"],
                "last_message_at": APPLICATION_STATE["last_message_at"],
            }

        response = json.dumps(payload).encode("utf-8")
        self.send_response(200 if ready else 503)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return


def start_health_server() -> None:
    server = ThreadingHTTPServer(("", HEALTH_PORT), HealthHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logging.info("Consumer health endpoint is available on port %s.", HEALTH_PORT)


def wait_for_kafka(topic: str) -> None:
    backoff_seconds = 1
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    while True:
        try:
            metadata = admin_client.list_topics(topic=topic, timeout=5)
            if topic in metadata.topics and metadata.topics[topic].error is None:
                set_state(kafka_connected=True)
                logging.info("Kafka topic %s is available.", topic)
                return
            raise RuntimeError(f"Topic {topic} is not available yet.")
        except Exception as exc:  # noqa: BLE001
            set_state(kafka_connected=False, ready=False)
            logging.warning(
                "Kafka is not ready yet (%s). Retrying in %s seconds.",
                exc,
                backoff_seconds,
            )
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, MAX_BACKOFF_SECONDS)


def connect_postgres() -> psycopg.Connection:
    backoff_seconds = 1

    while True:
        try:
            connection = psycopg.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                connect_timeout=10,
            )
            initialize_schema(connection)
            connection.commit()
            set_state(db_connected=True)
            logging.info("Connected to PostgreSQL at %s:%s.", POSTGRES_HOST, POSTGRES_PORT)
            return connection
        except Exception as exc:  # noqa: BLE001
            set_state(db_connected=False, ready=False)
            logging.warning(
                "PostgreSQL is not ready yet (%s). Retrying in %s seconds.",
                exc,
                backoff_seconds,
            )
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, MAX_BACKOFF_SECONDS)


def initialize_schema(connection: psycopg.Connection) -> None:
    with connection.cursor() as cursor:
        # PostgreSQL requires the partition key to be part of the primary key on a
        # partitioned table, so the table uses an identity column plus a composite key.
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_stock_prices (
                id BIGINT GENERATED ALWAYS AS IDENTITY,
                symbol VARCHAR(10) NOT NULL,
                price NUMERIC(10, 2) NOT NULL,
                volume BIGINT NOT NULL,
                event_timestamp TIMESTAMPTZ NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (event_timestamp, id),
                UNIQUE (symbol, event_timestamp)
            ) PARTITION BY RANGE (event_timestamp);
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_raw_stock_prices_symbol
            ON raw_stock_prices (symbol);
            """
        )
        ensure_monthly_partition(connection, datetime.now(timezone.utc))


def first_day_of_month(value: datetime) -> datetime:
    utc_value = value.astimezone(timezone.utc)
    return utc_value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def next_month(value: datetime) -> datetime:
    if value.month == 12:
        return value.replace(year=value.year + 1, month=1)
    return value.replace(month=value.month + 1)


def ensure_monthly_partition(connection: psycopg.Connection, event_timestamp: datetime) -> None:
    partition_start = first_day_of_month(event_timestamp)
    partition_end = next_month(partition_start)
    partition_name = f"raw_stock_prices_{partition_start:%Y_%m}"

    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {partition_name}
                PARTITION OF raw_stock_prices
                FOR VALUES FROM ({partition_start}) TO ({partition_end});
                """
            ).format(
                partition_name=sql.Identifier(partition_name),
                partition_start=sql.Literal(partition_start),
                partition_end=sql.Literal(partition_end),
            )
        )


def build_consumer() -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "client.id": "stock-postgres-consumer",
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    return consumer


def parse_message(message_value: bytes) -> Dict[str, Any]:
    payload = json.loads(message_value.decode("utf-8"))
    payload["timestamp"] = datetime.fromisoformat(payload["timestamp"].replace("Z", "+00:00"))
    return payload


def write_record(connection: psycopg.Connection, payload: Dict[str, Any]) -> bool:
    ensure_monthly_partition(connection, payload["timestamp"])
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO raw_stock_prices (symbol, price, volume, event_timestamp)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (symbol, event_timestamp) DO NOTHING;
            """,
            (
                str(payload["symbol"]).upper(),
                round(float(payload["price"]), 2),
                int(payload["volume"]),
                payload["timestamp"].astimezone(timezone.utc),
            ),
        )
        return cursor.rowcount == 1


def run() -> None:
    start_health_server()

    while True:
        postgres_connection = None
        kafka_consumer = None

        try:
            postgres_connection = connect_postgres()
            wait_for_kafka(KAFKA_TOPIC)
            kafka_consumer = build_consumer()
            set_state(ready=True)

            while True:
                message = kafka_consumer.poll(1.0)
                if message is None:
                    continue

                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(message.error())

                payload = parse_message(message.value())
                inserted = write_record(postgres_connection, payload)
                postgres_connection.commit()
                kafka_consumer.commit(message=message, asynchronous=False)

                set_state(
                    ready=True,
                    last_message_at=payload["timestamp"].astimezone(timezone.utc).isoformat(),
                )
                if inserted:
                    logging.info(
                        "Stored %s at %s in PostgreSQL.",
                        payload["symbol"],
                        payload["timestamp"].astimezone(timezone.utc).isoformat(),
                    )
                else:
                    logging.info(
                        "Skipped duplicate event for %s at %s.",
                        payload["symbol"],
                        payload["timestamp"].astimezone(timezone.utc).isoformat(),
                    )
        except Exception as exc:  # noqa: BLE001
            logging.exception("Consumer loop failed: %s", exc)
            set_state(ready=False, kafka_connected=False, db_connected=False)
            if postgres_connection is not None:
                try:
                    postgres_connection.rollback()
                except Exception:  # noqa: BLE001
                    pass
            time.sleep(5)
        finally:
            if kafka_consumer is not None:
                kafka_consumer.close()
            if postgres_connection is not None:
                postgres_connection.close()


if __name__ == "__main__":
    run()
