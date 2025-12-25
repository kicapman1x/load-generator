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
import logging
import requests
import gzip
from datetime import datetime

logger = logging.getLogger(__name__)

load_dotenv()

def bootstrap():
    #Environment variables
    global facial_dir, facial_api, rmq_url, rmq_port, rmq_username, rmq_password, ca_cert, secret_key, mysql_url, mysql_port, mysql_user, mysql_password, mysql_db, CONSUME_QUEUE_NAME, PRODUCE_QUEUE_NAME
    facial_dir = os.environ.get("FACIAL_DIR")
    facial_api = os.environ.get("image_gen_api")
    rmq_url = os.environ.get("RMQ_HOST")
    rmq_port = int(os.environ.get("RMQ_PORT"))
    rmq_username = os.environ.get("RMQ_USER")
    rmq_password = os.environ.get("RMQ_PW")
    ca_cert = os.environ.get("CA_PATH")
    secret_key = os.environ.get("HMAC_KEY").encode("utf-8")
    mysql_url = os.environ.get("MYSQL_HOST")
    mysql_port = int(os.environ.get("MYSQL_PORT"))
    mysql_user = os.environ.get("MYSQL_USER")
    mysql_password = os.environ.get("MYSQL_PW")
    mysql_db = os.environ.get("MYSQL_DB")
    CONSUME_QUEUE_NAME = "source_data_flight"
    PRODUCE_QUEUE_NAME = "source_data_facial"
    logdir = os.environ.get("log_directory", ".")
    loglvl = os.environ.get("log_level", "INFO").upper()

    #Logging setup
    log_level = getattr(logging, loglvl, logging.INFO)
    logging.basicConfig(
        filename=f'{logdir}/facial-svc.log',
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

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
    global conn
    conn = get_mysql_connection()
    try:
        message = json.loads(body)
        logger.info(f"Received message: {message}")

        p_key = message["passenger_key"]

        if passenger_exists(conn, p_key):
            logger.warning(f"Passenger exists : {p_key} - Skipping facial insertion.")
            if facial_exists(conn, p_key):
                logger.warning(f"Facial data exists for passenger : {p_key} - Skipping facial insertion.")
            else:
                logger.info(f"Feature not implemented yet - next deployment.")
                #generate pic - next deployment
        else:
            facial_b64 = get_facial_image(p_key)
            trace_id = get_traceid(conn, p_key)
            logger.info(f"Inserting facial data for passenger: {p_key} with trace ID: {trace_id}")
            insert_facial(conn, message["passenger_key"], trace_id)
            conn.commit()

            logger.info(f"Publishing facial details to {PRODUCE_QUEUE_NAME}")
            channel.queue_declare(queue=PRODUCE_QUEUE_NAME, durable=True)
            message_push = {
                "passenger_key": message["passenger_key"],
                "trace_id": trace_id,
                "facial_image": facial_b64
            }
            body = json.dumps(message_push)
            channel.basic_publish(
                exchange="",
                routing_key=PRODUCE_QUEUE_NAME,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2
                )
            )
            logger.info("Facial details written and message published.")
        channel.basic_ack(delivery_tag=method.delivery_tag)    
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        channel.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=True
        )
    conn.close()

def get_facial_image(passenger_key):
    logger.debug(f"Generating facial image for passenger: {passenger_key}")
    resp = requests.get(
        facial_api,
        timeout=10
    )
    resp.raise_for_status()
    logger.debug(f"Facial image retrieved for passenger: {passenger_key}")
    gzipped = gzip.compress(resp.content)
    facial_b64 = base64.b64encode(gzipped).decode("utf-8")
    with open(f"{facial_dir}/{passenger_key}.b64", "w") as f:
        f.write(facial_b64)
    return facial_b64

def passenger_exists(conn, passenger_key):
    logger.debug(f"Checking if passenger exists: {passenger_key}")
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM facial WHERE passenger_key = %s LIMIT 1",
        (passenger_key,)
    )
    return cursor.fetchone() is not None

def facial_exists(conn, passenger_key):
    logger.debug(f"Checking if facial data exists: {passenger_key}")
    cursor = conn.cursor()
    cursor.execute(
        "SELECT has_image FROM facial WHERE passenger_key = %s LIMIT 1",
        (passenger_key,)
    )
    return cursor.fetchone() is not None

def get_traceid(conn, passenger_key):
    logger.debug(f"Retrieving trace ID for passenger: {passenger_key}")
    cursor = conn.cursor()
    cursor.execute(
        "SELECT trace_id FROM passengers WHERE passenger_key = %s LIMIT 1",
        (passenger_key,)
    )
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        return None

def insert_facial(conn, passenger_key, trace_id):
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO facial (
            passenger_key,
            trace_id
        )
        VALUES (%s, %s)
        """,
        (
            passenger_key,
            trace_id
        )
    )

def main():
    bootstrap()
    logger.info("**********Starting passenger service**********")

    logger.info("Starting SSL RabbitMQ consumer...")
    global connection, channel 
    connection = get_rmq_connection()
    channel = connection.channel()

    logger.info(f"Declaring queue {CONSUME_QUEUE_NAME}")
    channel.queue_declare(queue=CONSUME_QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=1)

    logger.info(f"Consuming messages from {CONSUME_QUEUE_NAME}")
    channel.basic_consume(
        queue=CONSUME_QUEUE_NAME,
        on_message_callback=process_message,
        auto_ack=False
    )

    try:
        logger.info("Waiting for messages. Ctrl+C to exit.")
        channel.start_consuming()

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        channel.stop_consuming()
        connection.close()


if __name__ == "__main__":
    main()