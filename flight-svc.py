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
from datetime import datetime

logger = logging.getLogger(__name__)

load_dotenv()

def bootstrap():
    #Environment variables
    global rmq_url, rmq_port, rmq_username, rmq_password, ca_cert, secret_key, mysql_url, mysql_port, mysql_user, mysql_password, mysql_db, CONSUME_QUEUE_NAME_PRE_FACIAL, CONSUME_QUEUE_NAME_POST_PROCESS, PRODUCE_QUEUE_NAME_PRE_FACIAL,PRODUCE_QUEUE_NAME_POST_FACIAL, conn
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
    CONSUME_QUEUE_NAME_PRE_FACIAL = "source_data_passenger"
    CONSUME_QUEUE_NAME_POST_PROCESS = "source_data_facial"
    PRODUCE_QUEUE_NAME_PRE_FACIAL = "source_data_flight"
    PRODUCE_QUEUE_NAME_POST_FACIAL = "upd_facial_data_flight"
    logdir = os.environ.get("log_directory", ".")
    loglvl = os.environ.get("log_level", "INFO").upper()

    #Logging setup
    log_level = getattr(logging, loglvl, logging.INFO)
    logging.basicConfig(
        filename=f'{logdir}/flight-svc.log',
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    #global sql connection
    conn = mysql.connector.connect(
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

def process_message_pre_facial(channel, method, properties, body):
    try:
        message = json.loads(body)
        logger.info(f"Received message: {message}")

        p_key = message["passenger_key"]

        if passenger_exists(conn, p_key):
            logger.warning(f"Passenger exists : {p_key} - Skipping flight insertion.")
        else:
            trace_id = message["trace_id"]
            logger.info(f"[{trace_id}]  Inserting flight for passenger: {p_key}")
            insert_flights(conn, message["passenger_key"], trace_id, datetime.strptime(message["departure_date"],"%Y-%m-%d %H:%M"), message["arrival_airport"])
            conn.commit()

            logger.info(f"[{trace_id}]  Publishing flight details to {PRODUCE_QUEUE_NAME_PRE_FACIAL}")
            channel.queue_declare(queue=PRODUCE_QUEUE_NAME_PRE_FACIAL, durable=True)
            message_push = {
                "passenger_key": message["passenger_key"],
                "trace_id": trace_id
            }
            body = json.dumps(message)
            channel.basic_publish(
                exchange="",
                routing_key=PRODUCE_QUEUE_NAME_PRE_FACIAL,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2
                )
            )
            logger.info(f"[{trace_id}]  Flight details written and message published.")
        channel.basic_ack(delivery_tag=method.delivery_tag)    
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        channel.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=True
        )
    conn.close()

def process_message_post_facial(channel, method, properties, body):
    try:
        message = json.loads(body)
        p_key = message["passenger_key"]
        trace_id = message["trace_id"]
        logger.info(f"Received post facial message for passenger: {p_key}")

        if passenger_exists(conn, p_key):
            logger.info(f"Passenger exists : {p_key} - fetching flight details.")
            flight_details = get_flight_details(conn, p_key)
            logger.info(f"Flight details for passenger {p_key}: {flight_details}")
            logger.info(f"Publishing flight details to {PRODUCE_QUEUE_NAME_POST_FACIAL}")
            channel.queue_declare(queue=PRODUCE_QUEUE_NAME_POST_FACIAL, durable=True)
            message_push = {
                "passenger_key": p_key,
                "trace_id": trace_id,
                "departure_date": parse_departure_date(flight_details["departure_date"]),
                "arrival_airport": flight_details["arrival_airport"]
            }
            body = json.dumps(message_push)
            channel.basic_publish(
                exchange="",
                routing_key=PRODUCE_QUEUE_NAME_POST_FACIAL,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2
                )
            )
            logger.info(f"[{trace_id}] Flight details published post facial processing with facial data.")
        else:
            logger.error(f"Passenger does not exist : {p_key} - cannot fetch flight details.")
        channel.basic_ack(delivery_tag=method.delivery_tag)    
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        channel.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=True
        )
    conn.close()

def passenger_exists(conn, passenger_key):
    logger.debug(f"Checking if passenger exists: {passenger_key}")
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM flights WHERE passenger_key = %s LIMIT 1",
        (passenger_key,)
    )
    return cursor.fetchone() is not None

def get_flight_details(conn, passenger_key):
    logger.debug(f"Fetching flight details for passenger: {passenger_key}")
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT departure_date, arrival_airport 
        FROM flights 
        WHERE passenger_key = %s
        """,
        (passenger_key,)
    )
    return cursor.fetchone()

def insert_flights(conn, passenger_key, trace_id, departure_date, arrival_airport):
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO flights (
            passenger_key,
            departure_date,
            arrival_airport,
            trace_id,
            to_delete
        )
        VALUES (%s, %s, %s, %s, FALSE)
        """,
        (
            passenger_key,
            departure_date,
            arrival_airport,
            trace_id
        )
    )
    logger.info(f"[{trace_id}] Inserted flight for passenger: {passenger_key}")

def main():
    bootstrap()
    logger.info("**********Starting passenger service**********")

    logger.info("Starting SSL RabbitMQ consumer...")
    global connection, channel 
    connection = get_rmq_connection()
    channel = connection.channel()

    logger.info(f"Declaring queue {CONSUME_QUEUE_NAME_PRE_FACIAL}")
    channel.queue_declare(queue=CONSUME_QUEUE_NAME_PRE_FACIAL, durable=True)
    channel.queue_declare(queue=CONSUME_QUEUE_NAME_POST_FACIAL, durable=True)
    channel.basic_qos(prefetch_count=1)

    logger.info(f"Consuming messages from {CONSUME_QUEUE_NAME_PRE_FACIAL}")

    channel.basic_consume(
        queue=CONSUME_QUEUE_NAME_PRE_FACIAL,
        on_message_callback=process_message_pre_facial,
        auto_ack=False
    )

    logger.info(f"Consuming messages from {CONSUME_QUEUE_NAME_POST_FACIAL}")

    channel.basic_consume(
        queue=CONSUME_QUEUE_NAME_POST_FACIAL,
        on_message_callback=process_message_post_facial,
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