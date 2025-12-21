import os
import csv
import json
import time
import ssl
import random
import pika
from dotenv import load_dotenv

load_dotenv()
payload_dir = os.getenv("PAYLOAD_DIR")
tmp_dir = os.getenv("TMP_DIR")
ca_cert= os.environ.get("CA_PATH")
rmq_url = os.environ.get("RMQ_HOST")
rmq_port = int(os.environ.get("RMQ_PORT"))
rmq_username = os.environ.get("RMQ_USER")
rmq_password = os.environ.get("RMQ_PW")
interval = int(os.environ.get("INT_PERIOD"))

QUEUE_NAME = "source_data_intake"

PUBLISH_INTERVAL = interval

def get_rmq_connection():
    credentials = pika.PlainCredentials(
        rmq_username,
        rmq_password
    )

    ssl_context = ssl.create_default_context(cafile=ca_cert)
    ssl_context.check_hostname = True
    ssl_context.verify_mode = ssl.CERT_REQUIRED

    ssl_options = pika.SSLOptions(
        context=ssl_context,
        server_hostname=rmq_url
    )

    params = pika.ConnectionParameters(
        host=rmq_url,
        port=rmq_port,
        credentials=credentials,
        ssl_options=ssl_options,
        heartbeat=60,
        blocked_connection_timeout=30
    )

    return pika.BlockingConnection(params)


def load_csv():
    with open(f"{payload_dir}/flights.csv", newline="", encoding="utf-8") as csvfile:
        return list(csv.DictReader(csvfile))


def main():
    print("Loading CSV into memory...")
    rows = load_csv()
    print(f"Loaded {len(rows)} rows")

    connection = get_rmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    try:
        while True:
            row = random.choice(rows)

            message = {
                "passenger_id": row["Passenger ID"],
                "first_name": row["First Name"],
                "last_name": row["Last Name"],
                "age": int(row["Age"]),
                "nationality": row["Nationality"],
                "departure_date": row["Departure Date"],
                "arrival_airport": row["Arrival Airport"],
                "flight_status": row["Flight Status"],
                "ingested_at": int(time.time())
            }

            body = json.dumps(message)

            channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2
                )
            )

            print(f"Published random passenger {message['passenger_id']}")

            with open(f"{tmp_dir}/ingested", "a") as f: 
                f.write(row)

            time.sleep(PUBLISH_INTERVAL)
    except KeyboardInterrupt:
        print("Stopping publisher...")
    finally:
        connection.close()

if __name__ == "__main__":
    main()
