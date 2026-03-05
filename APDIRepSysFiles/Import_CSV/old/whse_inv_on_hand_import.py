import psycopg2
import pandas as pd

# Database connection details
DB_NAME = "apdireports"
DB_USER = "postgres"
DB_PASSWORD = "d4s31n@"
DB_HOST = "192.168.2.152"
DB_PORT = "5432"

# CSV file path
CSV_FILE_PATH = r"C:\Users\OP5\Desktop\APDIRepSys\Import CSV\whse_inv_on_hand_import.csv"

# PostgreSQL table name
TABLE_NAME = "whse_inv_on_hand_import"

# Define actual column names (excluding 'id' since it's auto-incremented)
DB_COLUMNS = [
    "product", "store_name", "store_whse", "customer", "qty_on_hand"
]  # 18 columns (excluding 'id')

# Define columns where we need to remove commas but keep them as VARCHAR
CLEAN_COLUMNS = ["qty_on_hand"]

def clean_data(df):
    """Cleans the dataframe by removing commas from specific columns but keeping them as VARCHAR."""
    for col in CLEAN_COLUMNS:
        df[col] = df[col].astype(str).str.replace(",", "")  # Remove commas but keep as string
    
    # Convert NaN values to None (for PostgreSQL compatibility)
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

        # Read CSV while skipping the first column (id)
        df = pd.read_csv(CSV_FILE_PATH, header=None, skiprows=1, usecols=range(1, 19))

        # Assign correct column names
        df.columns = DB_COLUMNS

        # Clean data (remove commas but keep as VARCHAR)
        df = clean_data(df)

        # Generate SQL INSERT query
        columns = ",".join(DB_COLUMNS)
        values = ",".join(["%s"] * len(DB_COLUMNS))
        insert_query = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({values})"

        # Convert dataframe to list of tuples for insertion
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
