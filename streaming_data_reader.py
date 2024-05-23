"""
Streaming data consumer
"""
from datetime import datetime
from kafka import KafkaConsumer
import mysql.connector
import time

DATABASE = 'tolldata'
USERNAME = 'root'
PASSWORD = 'MTExMy1vcGV5ZW1p'
HOST = '127.0.0.1'
PORT = 3306
TOPIC = 'toll' 

def connect_to_database():
    print("Connecting to the database")
    try:
        connection = mysql.connector.connect(
            host=HOST, 
            port=PORT, 
            database=DATABASE, 
            user=USERNAME, 
            password=PASSWORD
        )
        print("Connected to database")
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def connect_to_kafka():
    print("Connecting to Kafka")
    try:
        consumer = KafkaConsumer(TOPIC)
        print(f"Connected to Kafka, reading messages from the topic {TOPIC}")
        return consumer
    except Exception as err:
        print(f"Error: {err}")
        return None

def main():
    connection = connect_to_database()
    if connection is None:
        print("Database connection failed. Exiting.")
        return

    consumer = connect_to_kafka()
    if consumer is None:
        print("Kafka connection failed. Exiting.")
        connection.close()
        return

    cursor = connection.cursor()
    
    for msg in consumer:
        try:
            # Extract information from Kafka
            message = msg.value.decode("utf-8")

            # Transform the date format to suit the database schema
            timestamp, vehicle_id, vehicle_type, plaza_id = message.split(",")

            dateobj = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')
            timestamp = dateobj.strftime("%Y-%m-%d %H:%M:%S")

            # Loading data into the database table
            sql = "INSERT INTO livetolldata (timestamp, vehicle_id, vehicle_type, plaza_id) VALUES (%s, %s, %s, %s)"
            cursor.execute(sql, (timestamp, vehicle_id, vehicle_type, plaza_id))
            connection.commit()
            print(f"A {vehicle_type} was inserted into the database")
        
        except Exception as err:
            print(f"Error processing message: {err}")

    connection.close()

if __name__ == "__main__":
    main()
