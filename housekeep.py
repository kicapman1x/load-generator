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
import random
import threading

logger = logging.getLogger(__name__)

load_dotenv()

def bootstrap():
    #Environment variables
    global ca_cert, secret_key, mysql_url, mysql_port, mysql_user, mysql_password, mysql_db, logdir, loglvl, mysql_db_s1, mysql_db_s2, mysql_db_s3, check_in_interval
    ca_cert = os.environ.get("CA_PATH")
    mysql_url = os.environ.get("MYSQL_HOST")
    mysql_port = int(os.environ.get("MYSQL_PORT"))
    mysql_user = os.environ.get("MYSQL_USER")
    mysql_password = os.environ.get("MYSQL_PW")
    mysql_db = os.environ.get("MYSQL_DB")
    mysql_db_s1 = os.environ.get("MYSQL_DB_SATELLITE1")
    mysql_db_s2 = os.environ.get("MYSQL_DB_SATELLITE2")
    mysql_db_s3 = os.environ.get("MYSQL_DB_SATELLITE3")
    logdir = os.environ.get("log_directory", ".")
    loglvl = os.environ.get("log_level", "INFO").upper()
    check_in_interval = int(os.environ.get("check_in_interval", "60"))
    delete_flight_interval = int(os.environ.get("delete_flight_interval", "60"))
    delete_facial_passenger_interval = int(os.environ.get("delete_facial_passenger_interval", "60"))
    delete_satellite_interval = int(os.environ.get("delete_satellite_interval", "60"))

    #Logging setup
    log_level = getattr(logging, loglvl, logging.INFO)
    logging.basicConfig(
        filename=f'{logdir}/satellite-interface.log',
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

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

def soft_delete_by_departure_dates(departure_dates):
    conn = get_mysql_connection()
    cursor = conn.cursor()
    query = "UPDATE flights SET to_delete = TRUE WHERE departure_date < (UTC_TIMESTAMP() - INTERVAL 30 MINUTE)"
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()

def check_in():
    while True:
        #soft delete 
        soft_delete_by_departure_dates()
        logger.info("Soft deleted old flight records from flights table.")
    sleep(check_in_interval)

def flights_delete():
    while True:
        #hard delete flight
        conn = get_mysql_connection()
        cursor = conn.cursor()
        delete_query = "DELETE FROM flights WHERE to_delete = TRUE"
        cursor.execute(delete_query)
        deleted_count = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Deleted {deleted_count} records from flights table.")
    sleep(delete_flight_interval)

def facial_n_passenger_delete():
    while True:
        #hard delete facial and passenger data
        conn = get_mysql_connection()
        cursor = conn.cursor()
        delete_query_faciial = "DELETE facial from facial LEFT JOIN flights ON facial.passenger_key = flights.passenger_key WHERE flights.passenger_key IS NULL"
        cursor.execute(delete_query_facial)
        deleted_facial_count = cursor.rowcount
        logger.info(f"Deleted {deleted_facial_count} records from facial table.")

        delete_query_passenger = "DELETE passenger from passenger LEFT JOIN flights ON passenger.passenger_key = flights.passenger_key WHERE flights.passenger_key IS NULL"
        cursor.execute(delete_query_passenger)
        deleted_passenger_count = cursor.rowcount
        logger.info(f"Deleted {deleted_passenger_count} records from passenger table.")

        conn.commit()
        cursor.close()
        conn.close()
    sleep(delete_facial_passenger_interval)

def satellite_delete():
    while True:
        conn = get_mysql_connection()
        cursor = conn.cursor()

        delete_query_s1 = "DELETE FROM touchpoint FROM touchpoint.s1 AS touchpoint LEFT JOIN flights.hq AS flights ON touchpoint.passenger_key = flights.passenger_key WHERE flights.passenger_key IS NULL"
        cursor.execute(delete_query_s1)
        conn.commit()

        delete_query_s2 = "DELETE FROM touchpoint FROM touchpoint.s2 AS touchpoint LEFT JOIN flights.hq AS flights ON touchpoint.passenger_key = flights.passenger_key WHERE flights.passenger_key IS NULL"
        cursor.execute(delete_query_s2)
        conn.commit()

        delete_query_s3 = "DELETE FROM touchpoint FROM touchpoint.s3 AS touchpoint LEFT JOIN flights.hq AS flights ON touchpoint.passenger_key = flights.passenger_key WHERE flights.passenger_key IS NULL"
        cursor.execute(delete_query_s3)
        conn.commit()

        cursor.close()
        conn.close()
        logger.info("Deleted orphaned records from satellite databases.")
    sleep(delete_satellite_interval)

def main():
    bootstrap()
    logger.info("**********Starting housekeep service**********")

    threading.Thread(target=check_in, daemon=True).start()
    threading.Thread(target=flights_delete, daemon=True).start()
    threading.Thread(target=facial_n_passenger_delete, daemon=True).start()
    threading.Thread(target=satellite_delete, daemon=True).start()

    while True:
        sleep(36000)

if __name__ == "__main__":
    bootstrap()
    logger.info("**********Starting check-in application**********")
    check_in()