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
    'host': 'db',  # '127.0.0.1',
    'database': os.getenv('MYSQL_DATABASE'),
    'raise_on_warnings': True
}


def load_data_to_db(file_path, table_name):
    # Load the CSV file into a DataFrame
    df = pd.read_csv(file_path)

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

        # Drop the table if it exists
        try:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_TABLE_ERROR:
                print(f"Table {table_name} does not exist, continuing...")
            else:
                raise Exception(err)
        finally:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

        # Create the table
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT PRIMARY KEY,
            transaction_date DATE,
            transaction_time DATETIME,
            transaction_qty INT,
            drink_id INT,
            FOREIGN KEY (drink_id) REFERENCES drinks(id)
        )
        """
        cursor.execute(create_table_query)

        # Insert data into the table
        insert_query = f"""
        INSERT INTO {table_name} (id, transaction_date, transaction_time, transaction_qty, drink_id)
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


# Load data into sales table
load_data_to_db(transform_path + 'Migrations/create_sales.csv', 'sales')

# Load data into shop_sales table
load_data_to_db(transform_path + 'Migrations/create_shop_sales.csv', 'shop_sales')
