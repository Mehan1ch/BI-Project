# %% tags=["parameters"]
upstream = ['create_qualities', 'load_countries', 'load_farms', 'load_companies', 'load_varieties',
            'load_processing_methods']
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
df = pd.read_csv(transform_path + 'Migrations/create_qualities.csv')

# Convert numpy data types to native Python data types
df = df.astype({
    'id': int,
    'farm_id': int,
    'company_id': int,
    'variety_id': int,
    'processing_method_id': int,
    'harvest_year': int,
    'aroma': float,
    'flavor': float,
    'aftertaste': float,
    'acidity': float,
    'body': float,
    'balance': float,
    'uniformity': float,
    'clean_cup': float,
    'sweetness': float,
    'overall': float,
    'total_cup_points': float,
    'moisture_percentage': float
})

# Connect to the MySQL database
try:
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    # Drop the qualities table if it exists
    try:
        # Disable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        cursor.execute("DROP TABLE IF EXISTS qualities")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_TABLE_ERROR:
            print("Table does not exist, continuing...")
        else:
            raise Exception(err)
    finally:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

    # Create the qualities table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS qualities (
        id INT PRIMARY KEY,
        farm_id INT,
        company_id INT,
        country_id INT,
        variety_id INT,
        processing_method_id INT,
        harvest_year INT,
        aroma FLOAT,
        flavor FLOAT,
        aftertaste FLOAT,
        acidity FLOAT,
        body FLOAT,
        balance FLOAT,
        uniformity FLOAT,
        clean_cup FLOAT,
        sweetness FLOAT,
        overall FLOAT,
        total_cup_points FLOAT,
        moisture_percentage FLOAT,
        FOREIGN KEY (farm_id) REFERENCES farms(id),
        FOREIGN KEY (company_id) REFERENCES companies(id),
        FOREIGN KEY (country_id) REFERENCES countries(id),
        FOREIGN KEY (variety_id) REFERENCES varieties(id),
        FOREIGN KEY (processing_method_id) REFERENCES processing_methods(id)
    )
    """
    cursor.execute(create_table_query)

    # Insert data into the qualities table
    insert_query = """
    INSERT INTO qualities (id, farm_id, company_id,country_id, variety_id, processing_method_id, harvest_year, aroma, flavor, aftertaste, acidity, body, balance, uniformity, clean_cup, sweetness, overall, total_cup_points, moisture_percentage)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            int(row['id']), int(row['farm_id']), int(row['company_id']), int(row['country_id']), int(row['variety_id']),
            int(row['processing_method_id']), int(row['harvest_year']), float(row['aroma']), float(row['flavor']),
            float(row['aftertaste']), float(row['acidity']), float(row['body']), float(row['balance']),
            float(row['uniformity']), float(row['clean_cup']), float(row['sweetness']), float(row['overall']),
            float(row['total_cup_points']), float(row['moisture_percentage']
                                                  )))

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
        cnx.close()  # %%
