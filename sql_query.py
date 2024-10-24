import psycopg2

# Database connection string
sql_db = 'postgresql://tsdbadmin:tbo0mp4fvj2aukky@pg9i4yanln.f38anlyk4s.tsdb.cloud.timescale.com:33188/tsdb'


def run_query(query):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(sql_db)

        # Create a cursor to execute SQL queries
        cursor = conn.cursor()

        # Execute the SQL query
        cursor.execute(query)

        # Fetch all the results (if applicable)
        result = cursor.fetchall()

        # Print the results
        for row in result:
            print(row)

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    # Example query: Calculate average fare amount
    query = '''

SELECT time_bucket('30 minute', pickup_datetime) AS thirty_min, count(*)
FROM rides
WHERE pickup_datetime < '2016-01-02 00:00'
GROUP BY thirty_min
ORDER BY thirty_min;'''

    # Run the query
    run_query(query)
