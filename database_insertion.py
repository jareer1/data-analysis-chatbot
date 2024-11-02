import csv
import psycopg2
from datetime import datetime
import psycopg2.extras
# Connection to the TimescaleDB database
CONNECTION = "postgres://tsdbadmin:rsveqmy9k3v9bd01@fsbog41cv9.f38anlyk4s.tsdb.cloud.timescale.com:34693/tsdb?sslmode=require"
conn = psycopg2.connect(CONNECTION)
cursor = conn.cursor()

# Create the 'rides' table if it does not exist
query_create_sensors_table = """
    CREATE TABLE IF NOT EXISTS rides (
        vendor_id TEXT,
        pickup_datetime TIMESTAMP NOT NULL,
        dropoff_datetime TIMESTAMP NOT NULL,
        passenger_count NUMERIC,
        trip_distance NUMERIC,
        pickup_longitude NUMERIC,
        pickup_latitude NUMERIC,
        rate_code INTEGER,
        dropoff_longitude NUMERIC,
        dropoff_latitude NUMERIC,
        payment_type INTEGER,
        fare_amount NUMERIC,
        extra NUMERIC,
        mta_tax NUMERIC,
        tip_amount NUMERIC,
        tolls_amount NUMERIC,
        improvement_surcharge NUMERIC,
        total_amount NUMERIC
    );
"""

# Execute the table creation query
cursor.execute(query_create_sensors_table)
conn.commit()

# Path to the CSV file
csv_file_path = "new_data/nyc_data_rides.csv"

# Insert data in bulk
BATCH_SIZE = 10000
insert_query = """
    INSERT INTO rides (
        vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
        pickup_longitude, pickup_latitude, rate_code, dropoff_longitude, dropoff_latitude,
        payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
        improvement_surcharge, total_amount
    ) VALUES %s
"""


# Helper function to process rows in bulk
def process_bulk_insert(data_batch):
    try:
        psycopg2.extras.execute_values(
            cursor, insert_query, data_batch, template=None, page_size=BATCH_SIZE
        )
    except psycopg2.Error as e:
        print(f"Error during bulk insert: {e}")
    finally:
        conn.commit()


# Open CSV file and prepare for bulk insertion
with open(csv_file_path, "r", encoding="utf-8") as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip the header row

    batch = []
    counter = 0
    print("Currently inserting data into 'rides' table...")

    for row in csv_data:
        # Convert date formats from M/D/YYYY H:MM to YYYY-MM-DD HH:MM:SS
        try:
            row[1] = datetime.strptime(row[1], "%m/%d/%Y %H:%M").strftime('%Y-%m-%d %H:%M:%S')
            row[2] = datetime.strptime(row[2], "%m/%d/%Y %H:%M").strftime('%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            print(f"Error formatting datetime at row {counter}: {e}")
            continue

        # Replace empty cells with None for SQL insertion
        row = [None if cell == '' else cell for cell in row]

        # Append row to batch
        batch.append(row)
        counter += 1

        # Insert in bulk for every BATCH_SIZE rows
        if counter % BATCH_SIZE == 0:
            process_bulk_insert(batch)
            batch = []  # Clear the batch after inserting
            print(f"Inserted {counter} rows so far.")

    # Insert remaining rows if any
    if batch:
        process_bulk_insert(batch)

print("Data insertion complete.")

# Close the cursor and the connection
cursor.close()
conn.close()
