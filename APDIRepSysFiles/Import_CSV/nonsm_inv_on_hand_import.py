import psycopg2
import pandas as pd

# Database connection details
DB_NAME = "apdireports"
DB_USER = "postgres"
DB_PASSWORD = "d4s31n@"
DB_HOST = "192.168.2.152"
DB_PORT = "5432"

# CSV file path
CSV_FILE_PATH = r"C:\Users\OP5\Desktop\APDIRepSysFiles\Import_CSV\nonsm_inv_on_hand_import.csv"

# PostgreSQL table name
TABLE_NAME = "nonsm_inv_on_hand"

# Define actual column names (excluding 'id' since it's auto-incremented)
DB_COLUMNS = ["product", "store_name", "store_whse", "qty_on_hand"]

# Define columns where we need to remove commas & convert to integer
NUMERIC_COLUMNS = ["qty_on_hand"]

def clean_data(df):
    """Cleans numeric columns by removing commas and converting to integers."""
    for col in NUMERIC_COLUMNS:
        df[col] = df[col].astype(str).str.replace(",", "").astype(float).astype(int)
    df = df.where(pd.notna(df), None)
    return df

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

        # TRUNCATE the table before inserting new data
        cur.execute(f"TRUNCATE TABLE {TABLE_NAME}")
        print(f"⚠️ Table {TABLE_NAME} truncated.")

        # Read and process CSV
        df = pd.read_csv(CSV_FILE_PATH, header=None, skiprows=1, usecols=range(1, 5))
        df.columns = DB_COLUMNS
        df = clean_data(df)

        # Prepare insert query
        columns = ",".join(DB_COLUMNS)
        values = ",".join(["%s"] * len(DB_COLUMNS))
        insert_query = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({values})"
        data_to_insert = [tuple(row) for row in df.itertuples(index=False, name=None)]

        # Execute batch insert
        cur.executemany(insert_query, data_to_insert)
        conn.commit()

        print(f"✅ Data successfully imported into {TABLE_NAME}")

    except Exception as e:
        print(f"❌ Error: {e}")
        conn.rollback()

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    import_csv_to_postgres()
