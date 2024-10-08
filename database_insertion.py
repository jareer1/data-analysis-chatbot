import csv
import os
import mysql.connector
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password='jareer'
)
mycursor = mydb.cursor()
mycursor.execute("CREATE DATABASE IF NOT EXISTS ecommerce")
# Add password parameter if needed
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    database="ecommerce",
    password='jareer'
)
mycursor = mydb.cursor()
mycursor.execute("CREATE TABLE distribution_centers(id INT, name VARCHAR(255), latitude FLOAT, longitude FLOAT)")
mycursor.execute("CREATE TABLE events(id INT, user_id INT, sequence_number INT, session_id VARCHAR(255), created_at TIMESTAMP, ip_address VARCHAR(255), city VARCHAR(255), state VARCHAR(255), postal_code VARCHAR(255), browser VARCHAR(255), traffic_source VARCHAR(255), uri VARCHAR(255), event_type VARCHAR(255))")
mycursor.execute("CREATE TABLE inventory_items(id INT, product_id INT, created_at TIMESTAMP, sold_at TIMESTAMP, cost FLOAT, product_category VARCHAR(255), product_name VARCHAR(255), product_brand VARCHAR(255), product_retail_price FLOAT, product_department VARCHAR(255), product_sku VARCHAR(255), product_distribution_center_id INT)")
mycursor.execute("CREATE TABLE order_items(id INT, order_id INT, user_id INT, product_id INT, inventory_item_id INT, status VARCHAR(255), created_at TIMESTAMP, shipped_at TIMESTAMP, delivered_at TIMESTAMP, returned_at TIMESTAMP, sale_price FLOAT)")
mycursor.execute("CREATE TABLE orders(order_id INT, user_id INT, status VARCHAR(255), gender VARCHAR(255), created_at TIMESTAMP, returned_at TIMESTAMP, shipped_at TIMESTAMP, delivered_at TIMESTAMP, num_of_item INT)")
mycursor.execute("CREATE TABLE products(id INT, cost FLOAT, category VARCHAR(255), name VARCHAR(255), brand VARCHAR(255), retail_price FLOAT, department VARCHAR(255), sku VARCHAR(255), distribution_center_id INT)")
mycursor.execute("CREATE TABLE users(id INT, first_name VARCHAR(255), last_name VARCHAR(255), email VARCHAR(255), age INT, gender VARCHAR(255), state VARCHAR(255), street_address VARCHAR(255), postal_code VARCHAR(255), city VARCHAR(255), country VARCHAR(255), latitude FLOAT, longitude FLOAT, traffic_source VARCHAR(255), created_at TIMESTAMP)")
# Read data from CSV files and insert that data into corresponding tables in a database.
table_names = ["distribution_centers", "events", "inventory_items", "order_items", "orders", "products", "users"]

for table_name in table_names:
    with open("data/%s.csv" % table_name, "r", encoding="utf-8") as file:
        csv_data = csv.reader(file)
        next(csv_data)  # Skip headers
        counter = 0
        print("Currently inserting data into table %s" % (table_name))
        for row in csv_data:
            if counter % 10000 == 0:
                print("Progress is", counter)
            row = [None if cell == '' else cell.replace(" UTC", "") for cell in row]
            postfix = ','.join(["%s"] * len(row))
            try:
                mycursor.execute("INSERT INTO %s VALUES(%s)" % (table_name, postfix), row)
            except mysql.connector.Error as err:
                print("Error occurred:", err)
            counter += 1
        mydb.commit()