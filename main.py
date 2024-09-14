import psycopg2
from psycopg2 import pool
import csv
import time
import logging
import os
from datetime import datetime

# Setup logging
current_location = os.getcwd()

log_folder = f"{current_location}/python_log"

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

logging.basicConfig(filename=f'{log_folder}/{datetime.now().strftime("%Y-%m-%d")}_senddata.log', 
                    level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Database configuration
DB_CONFIG = {
    'dbname': 'naqd',
    'user': 'naqd_owner',
    'password': 'sFet4nhVP5cp',
    'host': 'ep-snowy-cherry-a5cbwpfu.us-east-2.aws.neon.tech',
    'port': '5432'
}
# Set up connection pool
connection_pool = psycopg2.pool.SimpleConnectionPool(1, 10, **DB_CONFIG)


def send_data():
    database = datetime.now().strftime("%Y-%m-%d")
    csv_file = f"{current_location}/data/{database}.txt"
    print(csv_file)

    if not os.path.exists(csv_file):
        logging.warning(f"File not found: {csv_file}")
        return False

    try:
        conn = connection_pool.getconn()
        if conn:
            cursor = conn.cursor()
            with open(csv_file, mode='r') as file:
                csv_reader = csv.reader(file, delimiter=',')
                insert_sql = """
                INSERT INTO car_data (car_id, duration, end_date, start_date, ad_id, frame_id, person_id, created_at)
                VALUES (%s, %s, %s, %s, 7, 2, %s, %s)
                """
                row_count = 0
                for row in csv_reader:
                    try:
                        class_id = int(row[0])
                        value = int(row[1])
                        duration = float(row[2])
                        timestamp = datetime.fromisoformat(row[3])

                        if class_id == 0:
                            car_id = None
                            person_id = value
                        elif class_id == 2:
                            car_id = value
                            person_id = None
                        else:
                            continue

                        cursor.execute(insert_sql, (car_id, duration, timestamp, timestamp, person_id, timestamp))
                        row_count += 1
                        print(row_count)

                        if row_count % 100 == 0:
                            conn.commit()  # Commit after every 100 rows
                            print(f"Inserted batch of 100 rows.")

                    except Exception as e:
                        logging.warning(f"Error processing row: {e}")
                        continue

                conn.commit()  # Final commit for remaining rows
                print("Data has been inserted successfully.")
                logging.info("Data has been inserted successfully.")
                return True

    except (Exception, psycopg2.DatabaseError) as error:
        logging.warning(f"Database error: {error}")
    finally:
        if connection_pool:
            connection_pool.putconn(conn)


def main():
    print("Starting")

    while True:
        try:
            send_data()
            time.sleep(120)  # 2 minutes wait
        except Exception as exception:
            print(exception)
            logging.warning(f"Exception occurred: {exception}")
            time.sleep(120)  # Wait 2 minutes even in case of an error


if __name__ == "__main__":
    main()
