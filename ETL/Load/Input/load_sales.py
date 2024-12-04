# %% tags=["parameters"]
upstream = ['create_sales', 'load_drinks']
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
df = pd.read_csv(transform_path+'Migrations/create_sales.csv')

# Convert data types if necessary
df = df.astype({
    'id': int,
    'transaction_date': str,
    'transaction_time': str,
    'transaction_qty': int,
    'drink_id': int
})

# Connect to the MySQL database
try:
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()

    # Drop the sales table if it exists
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        cursor.execute("DROP TABLE IF EXISTS sales")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_TABLE_ERROR:
            print("Table does not exist, continuing...")
        else:
            raise Exception(err)
    finally:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

    # Create the sales table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS sales (
        id INT PRIMARY KEY,
        transaction_date DATE,
        transaction_time DATETIME,
        transaction_qty INT,
        drink_id INT,
        FOREIGN KEY (drink_id) REFERENCES drinks(id)
    )
    """
    cursor.execute(create_table_query)

    # Insert data into the sales table
    insert_query = """
    INSERT INTO sales (id, transaction_date, transaction_time, transaction_qty, drink_id)
    VALUES (%s, %s, %s, %s, %s)
    """
    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            int(row['id']), row['transaction_date'], row['transaction_time'], int(row['transaction_qty']),
            int(row['drink_id'])
        ))

    # Commit the transaction
    cnx.commit()

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
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
