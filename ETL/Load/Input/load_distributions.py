# %% tags=["parameters"]
upstream = ['create_distributions', 'load_countries']
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
df = pd.read_csv(transform_path + 'Migrations/create_distributions.csv')

# Convert numpy data types to native Python data types
df = df.astype({
    'id': int,
    'country_id': int,
    'year': int,
    'arabica_production': int,
    'bean_exports': int,
    'bean_imports': int,
    'beginning_stocks': int,
    'domestic_consumption': int,
    'ending_stocks': int,
    'other_production': int,
    'production': int,
    'roast_and_ground_exports': int,
    'roast_and_ground_imports': int,
    'robusta_production': int,
    'roast_ground_dom_cons': int,
    'soluble_dom_cons': int,
    'soluble_exports': int,
    'soluble_imports': int,
    'total_distribution': int,
    'total_supply': int
})

# Connect to the MySQL database
try:
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    # Drop the distributions table if it exists
    try:
        # Disable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        cursor.execute("DROP TABLE IF EXISTS distributions")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_TABLE_ERROR:
            print("Table does not exist, continuing...")
        else:
            raise Exception(err)
    finally:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

    # Create the distributions table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS distributions (
        id INT PRIMARY KEY,
        country_id INT,
        year INT,
        arabica_production INT,
        bean_exports INT,
        bean_imports INT,
        beginning_stocks INT,
        domestic_consumption INT,
        ending_stocks INT,
        other_production INT,
        production INT,
        roast_and_ground_exports INT,
        roast_and_ground_imports INT,
        robusta_production INT,
        roast_ground_dom_cons INT,
        soluble_dom_cons INT,
        soluble_exports INT,
        soluble_imports INT,
        total_distribution INT,
        total_supply INT,
        FOREIGN KEY (country_id) REFERENCES countries(id)
    )
    """
    cursor.execute(create_table_query)

    # Insert data into the distributions table
    insert_query = """
    INSERT INTO distributions (id, country_id, year, arabica_production, bean_exports, bean_imports, beginning_stocks, domestic_consumption, ending_stocks, other_production, production, roast_and_ground_exports, roast_and_ground_imports, robusta_production, roast_ground_dom_cons, soluble_dom_cons, soluble_exports, soluble_imports, total_distribution, total_supply)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            int(row['id']), int(row['country_id']), int(row['year']), int(row['arabica_production']),
            int(row['bean_exports']), int(row['bean_imports']),
            int(row['beginning_stocks']), int(row['domestic_consumption']), int(row['ending_stocks']),
            int(row['other_production']), int(row['production']),
            int(row['roast_and_ground_exports']), int(row['roast_and_ground_imports']), int(row['robusta_production']),
            int(row['roast_ground_dom_cons']),
            int(row['soluble_dom_cons']), int(row['soluble_exports']), int(row['soluble_imports']),
            int(row['total_distribution']), int(row['total_supply'])
        ))

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
