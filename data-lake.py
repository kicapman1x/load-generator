import os
import csv
import json
import random
import time
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

def bootstrap():
    #Environment variables
    load_dotenv()
    global payload_dir, tmp_dir, ca_cert, interval
    payload_dir = os.getenv("PAYLOAD_DIR")
    tmp_dir = os.getenv("TMP_DIR")
    ca_cert= os.environ.get("CA_PATH")
    interval = int(os.environ.get("DATA_GENERATION_INTERVAL"))
    logdir = os.environ.get("log_directory", ".")
    loglvl = os.environ.get("log_level", "INFO").upper()

    #Logging setup
    log_level = getattr(logging, loglvl, logging.INFO)
    logging.basicConfig(
        filename=f'{logdir}/data-lake.log',
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
def load_csv():
    with open(f"{payload_dir}/flights.csv", newline="", encoding="utf-8") as csvfile:
        return list(csv.DictReader(csvfile))


def main():
    bootstrap()
    logger.info("**********Starting source data publisher**********")
    logger.info(f"Loading payloads from {payload_dir}/flights.csv")
    rows = load_csv()
    logger.info(f"Loaded {len(rows)} rows")

    header = [
        "Passenger ID", "First Name", "Last Name", "Gender", "Age",
        "Nationality", "Airport Name", "Airport Country Code", "Country Name",
        "Airport Continent", "Continents",
        "Departure Date", "Arrival Airport",
        "Pilot Name", "Flight Status"
    ]

    while True:
        for i in range(10):
            logger.info("Selecting 60 random rows")
            sampled_rows = random.sample(rows, 60)

            ref_row = random.choice(sampled_rows)
            uniform_departure_date = ref_row["Departure Date"]
            uniform_arrival_airport = ref_row["Arrival Airport"]

            logger.info(
                f"Normalized batch to departure_date={uniform_departure_date}, "
                f"arrival_airport={uniform_arrival_airport}"
            )

            for r in sampled_rows:
                rows.remove(r)
                r["Departure Date"] = uniform_departure_date
                r["Arrival Airport"] = uniform_arrival_airport

            output_file = f"{tmp_dir}/batch_payload.csv"
            file_exists = os.path.exists(output_file)

            with open(f"{tmp_dir}/batch_payload.csv", "a", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=header)

                if not file_exists:
                    writer.writeheader() 
                    file_exists = True

                writer.writerows(sampled_rows)

            logger.info(f"Wrote batch payload to {tmp_dir}/batch_payload.csv")
        logger.info(f"Sleeping for {interval} seconds before generating next batches")
    time.sleep(interval)

if __name__ == "__main__":
    main()
