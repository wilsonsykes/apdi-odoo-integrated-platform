import psycopg2
import pandas as pd

# Database connection details
DB_NAME = "apdireports"
DB_USER = "postgres"
DB_PASSWORD = "d4s31n@"
DB_HOST = "192.168.2.152"
DB_PORT = "5432"

# CSV file path
CSV_FILE_PATH = r"C:\Users\OP5\Desktop\APDIRepSysFIles\Import_CSV\celes_masterlist_import.csv"

# PostgreSQL table name
TABLE_NAME = "celes_masterlist"

# Actual column names (excluding 'id' since it's auto-incremented)
DB_COLUMNS = ["product", "md_one", "md_two", "md_three", "md_four", "md_five", "new_srp"]

# Numeric fields that need cleanup
NUMERIC_COLUMNS = ["md_one", "md_two", "md_three", "md_four", "md_five", "new_srp"]

def clean_data(df):
    """Cleans numeric columns by removing commas and safely converting to integers."""
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "")
                .str.strip()
                .replace("", "0")
                .replace("nan", "0")
                .astype(float)
                .astype(int)
            )
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

        # Read CSV (skipping ID column)
        df = pd.read_csv(CSV_FILE_PATH, header=None, skiprows=1, usecols=range(0, 7))
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
