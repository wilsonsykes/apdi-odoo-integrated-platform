import psycopg2
import pandas as pd

# Database connection details
DB_NAME = "apdireports"
DB_USER = "postgres"
DB_PASSWORD = "d4s31n@"
DB_HOST = "192.168.2.152"  # Change if using a remote server
DB_PORT = "5432"  # Default PostgreSQL port

# CSV file path
CSV_FILE_PATH = "sales_raw_data_import.csv"

# PostgreSQL table name
TABLE_NAME = "sales_raw_data"

def import_csv_to_postgres():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()

        # Load CSV into pandas dataframe
        df = pd.read_csv(CSV_FILE_PATH)

        # Generate SQL INSERT query dynamically
        columns = ",".join(df.columns)
        values = ",".join(["%s"] * len(df.columns))
        insert_query = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({values})"

        # Convert dataframe to list of tuples for execution
        data_to_insert = [tuple(row) for row in df.itertuples(index=False, name=None)]

        # Execute batch insert
        cur.executemany(insert_query, data_to_insert)
        conn.commit()

        print(f"Data successfully imported into {TABLE_NAME}")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    import_csv_to_postgres()
