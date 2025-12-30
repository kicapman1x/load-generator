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
    global facial_dir, facial_api, rmq_url, rmq_port, rmq_username, rmq_password, ca_cert, secret_key, mysql_url, mysql_port, mysql_user, mysql_password, CONSUME_QUEUE_NAME, logdir, loglvl, mysql_db_s1
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
    mysql_db_s1 = os.environ.get("MYSQL_DB_SATELLITE1")
    CONSUME_QUEUE_NAME = "ingest_facial_data_s1"
    logdir = os.environ.get("log_directory", ".")
    loglvl = os.environ.get("log_level", "INFO").upper()

    #Logging setup
    log_level = getattr(logging, loglvl, logging.INFO)
    logging.basicConfig(
        filename=f'{logdir}/satellite1.log',
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

def get_mysql_connection_s1():
    return mysql.connector.connect(
        host=mysql_url,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_db_s1,

        ssl_ca=ca_cert,
        ssl_verify_cert=True,
        ssl_verify_identity=True,  

        autocommit=False
    )

def process_message(ch, method, properties, body):
    global conn_s1
    conn_s1 = get_mysql_connection_s1()
    try:
        message = json.loads(body)
        logger.info(f"Received message for satellite 1: {message}")

        p_key = message["passenger_key"]
        trace_id = message["trace_id"]
        facial_image = message["facial_image"]
        departure_date = datetime.strptime(message["departure_date"], "%Y-%m-%d %H:%M")
        arrival_airport = message["arrival_airport"]
        logger.info(f"[{trace_id}] Inserting data for passenger: {p_key} with trace ID: {trace_id}")

        insert_full_data_satellite1(
            conn_s1,
            p_key,
            trace_id,
            facial_image,
            departure_date,
            arrival_airport
        )
        conn_s1.commit()
        logger.info(f"[{trace_id}] Successfully commited data for passenger: {p_key} into satellite 1 database.")
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        channel.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=True
        )
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        conn_s1.close()

def insert_full_data_satellite1(conn, passenger_key, trace_id, facial_image, departure_date, arrival_airport):
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO touchpoint (passenger_key, trace_id, facial_image, departure_date, arrival_airport)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, (passenger_key, trace_id, facial_image, departure_date, arrival_airport))
    logger.info(f"[{trace_id}] Inserted data for passenger {passenger_key} into satellite 1 database.")

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