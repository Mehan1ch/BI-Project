# %% tags=["parameters"]
upstream = ['create_farms']
product = None
transform_path = None

# %%
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Database connection details from .env file
db_config = {
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'host': '127.0.0.1',  # TODO: Change this to the container when running in docker
    'database': os.getenv('MYSQL_DATABASE'),
    'raise_on_warnings': True
}

# Load the CSV file into a DataFrame
df = pd.read_csv(transform_path + 'Migrations/create_farms.csv')

# Convert numpy data types to native Python data types
df = df.astype({
    'id': int,
    'farm_name': str,
    'altitude': float
})

# Connect to the MySQL database
try:
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    # Drop the farms table if it exists
    try:
        # Disable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        cursor.execute("DROP TABLE IF EXISTS farms")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_TABLE_ERROR:
            print("Table does not exist, continuing...")
        else:
            raise Exception(err)
    finally:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

    # Create the farms table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS farms (
        id INT PRIMARY KEY,
        farm_name VARCHAR(255) NOT NULL,
        altitude FLOAT
    )
    """
    cursor.execute(create_table_query)

    # Insert data into the farms table
    insert_query = """
    INSERT INTO farms (id, farm_name, altitude)
    VALUES (%s, %s, %s)
    """
    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            int(row['id']), row['farm_name'], float(row['altitude'])))

    # Commit the transaction
    cnx.commit()

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        # Throw an error if the credentials are wrong
        raise Exception("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        raise Exception("Database does not exist")
    else:
        raise Exception(err)
finally:
    if 'cursor' in locals() and cursor:
        cursor.close()
    if 'cnx' in locals() and cnx:
        cnx.close()
