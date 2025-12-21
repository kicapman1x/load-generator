import json
import ssl
import pika
import os
from dotenv import load_dotenv
import hmac
import hashlib
import base64
import mysql.connector
import uuid

load_dotenv()

rmq_url = os.environ.get("RMQ_HOST")
rmq_port = int(os.environ.get("RMQ_PORT"))
rmq_username = os.environ.get("RMQ_USER")
rmq_password = os.environ.get("RMQ_PW")
ca_cert = os.environ.get("CA_PATH")
secret_key = os.environ.get("HMAC_SECRET").encode("utf-8")
mysql_url = os.environ.get("MYSQL_HOST")
mysql_port = int(os.environ.get("MYSQL_PORT"))
mysql_user = os.environ.get("MYSQL_USER")
mysql_password = os.environ.get("MYSQL_PW")
mysql_db = os.environ.get("MYSQL_DB")

CONSUME_QUEUE_NAME = "source_data_intake"
PRODUCE_QUEUE_NAME = "source_data_passenger"

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

def get_mysql_connection():
    return mysql.connector.connect(
        host=mysql_url,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_db,

        ssl_ca=ca_cert,
        ssl_verify_cert=True,
        ssl_verify_identity=True,  

        autocommit=False
    )

def process_message(channel, method, properties, body):
    try:
        message = json.loads(body)
        print("Received message:")
        print(json.dumps(message, indent=2))

        p_key = process_person(message)
        if(p_key):
            message["passenger_key"] = p_key
            channel.queue_declare(queue=PRODUCE_QUEUE_NAME, durable=True)
            
            body = json.dumps(message)
            channel.basic_publish(
                exchange="",
                routing_key=PRODUCE_QUEUE_NAME,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2
                )
            )
        else:
            print("Passenger details not written - On to next message!")
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error processing message: {e}")
        channel.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=True
        )

def process_person(message):
    p_id = message["passenger_id"]
    sn = message["first_name"]
    ln = message["last_name"]
    p_fn = f"{sn} {ln}"
    p_nat = message["nationality"]
    p_age = int(message["age"])

    p_key = generate_p_key(p_id,p_fn,p_nat)

    conn = get_mysql_connection()
    try:
        if passenger_exists(conn, p_key):
            return None
        else: 
            trace_id = str(uuid.uuid4())
            insert_passenger(conn, p_key, p_fn, p_nat, p_age, trace_id)
            conn.commit()
            return p_key
    except mysql.connector.errors.IntegrityError:
        conn.rollback()
        return None
    finally:
        conn.close()

def passenger_exists(conn, passenger_key):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM passengers WHERE passenger_key = %s LIMIT 1",
        (passenger_key,)
    )
    return cursor.fetchone() is not None

def insert_passenger(conn, passenger_key, passenger_name, passenger_nationality, passenger_age, trace_id):
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO passengers (
            passenger_key,
            passenger_name,
            passenger_age,
            passenger_nationality,
            trace_id,
            to_delete
        )
        VALUES (%s, %s, %s, %s, %s, FALSE)
        """,
        (
            passenger_key,
            passenger_name,
            passenger_age,
            passenger_nationality,
            trace_id
        )
    )

def generate_p_key(id,fn,nat):
    canonical = f"{id}|{fn}|{nat}".lower().strip()
    digest = hmac.new(
        secret_key,
        canonical.encode("utf-8"),
        hashlib.sha256
    ).digest()
    return base64.urlsafe_b64encode(digest).decode("utf-8")[:32]


def main():
    print("Starting SSL RabbitMQ consumer...")
    global connection, channel 

    connection = get_rmq_connection()
    channel = connection.channel()

    channel.queue_declare(queue=CONSUME_QUEUE_NAME, durable=True)

    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(
        queue=CONSUME_QUEUE_NAME,
        on_message_callback=process_message,
        auto_ack=False
    )

    try:
        print("Waiting for messages. Ctrl+C to exit.")
        channel.start_consuming()

    except KeyboardInterrupt:
        print("Stopping consumer...")

    finally:
        channel.stop_consuming()
        connection.close()


if __name__ == "__main__":
    main()