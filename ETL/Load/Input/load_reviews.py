# %% tags=["parameters"]
upstream = ['load_blends', 'load_roasters', 'load_countries', 'create_reviews']
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

# Load the CSV file into a DataFrame
df = pd.read_csv(transform_path + 'Migrations/create_reviews.csv')

df = df.astype({
    'id': int,
    'rating': int,
    'blend_id': int,
    'roaster_id': int,
    'loc_country_id': int,
    'origin_id': int
})

# Connect to the MySQL database
try:
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    # Drop the reviews table if it exists
    try:
        # Disable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        cursor.execute("DROP TABLE IF EXISTS reviews")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_TABLE_ERROR:
            print("Table does not exist, continuing...")
        else:
            raise Exception(err)
    finally:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

    # Create the reviews table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS reviews (
        id INT PRIMARY KEY,
        rating INT NOT NULL,
        blend_id INT,
        roaster_id INT,
        loc_country_id INT,
        origin_id INT,
        FOREIGN KEY (blend_id) REFERENCES blends(id),
        FOREIGN KEY (roaster_id) REFERENCES roasters(id),
        FOREIGN KEY (loc_country_id) REFERENCES countries(id),
        FOREIGN KEY (origin_id) REFERENCES countries(id)
    )
    """
    cursor.execute(create_table_query)

    # Insert data into the reviews table
    insert_query = """
    INSERT INTO reviews (id, rating, blend_id, roaster_id, loc_country_id, origin_id)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            int(row['id']), int(row['rating']), int(row['blend_id']), int(row['roaster_id']),
            int(row['loc_country_id']), int(row['origin_id'])))

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
