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
    global facial_dir, facial_api, rmq_url, rmq_port, rmq_username, rmq_password, ca_cert, secret_key, mysql_url, mysql_port, mysql_user, mysql_password, mysql_db, CONSUME_QUEUE_NAME, PRODUCE_QUEUE_NAME, logdir, loglvl, mysql_db_s1, mysql_db_s2, mysql_db_s3
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
    mysql_db_s1 = os.environ.get("MYSQL_DB_SATELLITE1")
    mysql_db_s2 = os.environ.get("MYSQL_DB_SATELLITE2")
    mysql_db_s3 = os.environ.get("MYSQL_DB_SATELLITE3")
    CONSUME_QUEUE_NAME = "source_data_facial"
    PRODUCE_QUEUE_NAME = "ingest_facial_data_"
    logdir = os.environ.get("log_directory", ".")
    loglvl = os.environ.get("log_level", "INFO").upper()

    #Logging setup
    log_level = getattr(logging, loglvl, logging.INFO)
    logging.basicConfig(
        filename=f'{logdir}/satellite-interface.log',
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

def get_mysql_connection_s2():
    return mysql.connector.connect(
        host=mysql_url,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_db_s2,

        ssl_ca=ca_cert,
        ssl_verify_cert=True,
        ssl_verify_identity=True,  

        autocommit=False
    )

def get_mysql_connection_s3():
    return mysql.connector.connect(
        host=mysql_url,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_db_s3,

        ssl_ca=ca_cert,
        ssl_verify_cert=True,
        ssl_verify_identity=True,  

        autocommit=False
    )

def process_message(channel, method, properties, body):
    global conn, conn_s1, conn_s2, conn_s3
    conn = get_mysql_connection()
    conn_s1 = get_mysql_connection_s1()
    conn_s2 = get_mysql_connection_s2()
    conn_s3 = get_mysql_connection_s3()
    try:
        message = json.loads(body)
        logger.info(f"Received message: {message}")

        p_key = message["passenger_key"]
        trace_id = message["trace_id"]

        if passenger_exists_satellite(conn_s1, conn_s2, conn_s3, p_key):
            logger.warning(f"[{trace_id}] Passenger already exists : {p_key} - skipping ingesting to satellite queue.")
        else:
            selected_satellite = None
            departure_date = message["departure_date"]
            arrival_airport = message["arrival_airport"]
            satelite_check = flight_exists_satellite(conn_s1, conn_s2, conn_s3, datetime.strptime(departure_date,"%Y-%m-%d %H:%M"), arrival_airport)
            if satelite_check:
                selected_satellite = satelite_check
                logger.info(f"[{trace_id}] Flight exists in satellite {selected_satellite} - routing facial data accordingly.")
            else:
                selected_satellite = random.choice(["s1", "s2", "s3"])
                logger.info(f"[{trace_id}] Flight does not exist in any satellite - defaulting to randomly selected satellite {selected_satellite} for ingestion.")

            trace_id = message["trace_id"]
            logger.info(f"[{trace_id}] Ingesting data for passenger: {p_key} with trace ID: {trace_id}")

            logger.info(f"[{trace_id}] Publishing facial details to {PRODUCE_QUEUE_NAME}{selected_satellite}")
            channel.queue_declare(queue=PRODUCE_QUEUE_NAME + selected_satellite, durable=True)
            message_push = {
                "passenger_key": message["passenger_key"],
                "trace_id": trace_id,
                "facial_image": message["facial_image"],
                "departure_date": departure_date,
                "arrival_airport": arrival_airport
            }
            body = json.dumps(message_push)
            channel.basic_publish(
                exchange="",
                routing_key=PRODUCE_QUEUE_NAME+selected_satellite,
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

def passenger_exists_satellite_db(conn, passenger_key):
    logger.debug(f"Checking if passenger exists: {passenger_key}")
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM facial WHERE passenger_key = %s LIMIT 1",
        (passenger_key,)
    )
    return cursor.fetchone() is not None

def passenger_exists_satellite(conn_s1, conn_s2, conn_s3, passenger_key):
    return (
        passenger_exists_satellite_db(conn_s1, passenger_key) or
        passenger_exists_satellite_db(conn_s2, passenger_key) or
        passenger_exists_satellite_db(conn_s3, passenger_key)
    )

def flight_exists_satellite_db(conn, departure_date, arrival_airport):
    logger.debug(f"Checking if flight exists: {departure_date} to {arrival_airport}")
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM flights WHERE departure_date = %s AND arrival_airport = %s LIMIT 1",
        (departure_date, arrival_airport)
    )
    return cursor.fetchone() is not None

def flight_exists_satellite(conn_s1, conn_s2, conn_s3, departure_date, arrival_airport):
    if flight_exists_satellite_db(conn_s1, departure_date, arrival_airport):
        return "s1"
    elif flight_exists_satellite_db(conn_s2, departure_date, arrival_airport):
        return "s2"
    elif flight_exists_satellite_db(conn_s3, departure_date, arrival_airport):
        return "s3"
    else:
        return None

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