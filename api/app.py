import time
import requests
import mysql.connector
from mysql.connector import Error
from datetime import datetime

def get_data():
    """ Get the data from the Star's API"""

    url = 'https://data.explore.star.fr/api/explore/v2.1/catalog/datasets/tco-metro-circulation-deux-prochains-passages-tr/records?limit=65' # API's url
    response = requests.get(url) 
    data = response.json() # Data are under JSON file format
    return data['results']  # Use the key 'results'

def format_datetime(value):
    """Function to turn in a good format the dates for later insert it in the SQL database"""
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None

def connect_to_database():
    """Function to establish a connection to the MySQL database.
    Retries the connection up to 10 times with a 5-second delay between attempts."""
    
    connection = None
    max_retries = 10
    for attempt in range(max_retries):
        try:
            connection = mysql.connector.connect(
                host='db',
                database='metro',
                user='root',
                password='example'
            )
            if connection.is_connected():
                return connection
        except Error as e:
            time.sleep(5)  # Wait 5 seconds before next try
    raise Exception("Impossible to connect to the database after several attempts")

def update_database(data):
    """ Update the 'passages' table in the 'metro' database with the provided data.
    If the table or database does not exist, it will be created.
    Existing records will be updated if they already exist, and rows with NULL times will be deleted."""
    
    # Try to connect to database
    connection = connect_to_database()
    
    # If connection done successfully start creating/updating database
    if connection and connection.is_connected():
        try:
            cursor = connection.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS metro")
            cursor.execute("USE metro")
            
            # To identificate the data as unique and avoid duplicates, the primary key of the table is made from "Line_name, destination, station_name"
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS passages (
                    nomcourtligne VARCHAR(255), 
                    destination VARCHAR(255), 
                    nomarret VARCHAR(255), 
                    departfirsttrain DATETIME, 
                    departsecondtrain DATETIME,
                    PRIMARY KEY (nomcourtligne, destination, nomarret)
                )
            """)
            
            for record in data:
                cursor.execute("""
                    INSERT INTO passages 
                    (nomcourtligne, destination, nomarret, departfirsttrain, departsecondtrain) 
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    departfirsttrain = VALUES(departfirsttrain),
                    departsecondtrain = VALUES(departsecondtrain)
                """, (
                    record.get('nomcourtligne', 'inconnu'), 
                    record.get('destination', 'inconnu'), 
                    record.get('nomarret', 'inconnu'), 
                    format_datetime(record.get('departfirsttrain')), 
                    format_datetime(record.get('departsecondtrain'))
                ))

            # Removing rows where time information is NULL
            cursor.execute("DELETE FROM passages WHERE departfirsttrain IS NULL OR departsecondtrain IS NULL")
            connection.commit()
        except Error as e:
            print(f"Error during data insertion: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
    else:
        print("Data base is not available")

if __name__ == '__main__':
    while True:
        data = get_data()
        update_database(data)
        time.sleep(15)

