import psycopg2
import pandas as pd

# Database connection details
DB_NAME = "apdireports"
DB_USER = "postgres"
DB_PASSWORD = "d4s31n@"
DB_HOST = "192.168.2.152"
DB_PORT = "5432"

# CSV file path
CSV_FILE_PATH = r"C:\Users\OP5\Desktop\APDIRepSysFIles\Import_CSV\products_list_raw.csv"

# PostgreSQL table name
TABLE_NAME = "products_list_raw"

# Define actual column names
DB_COLUMNS = [
    "product", "product_category", "subcategory", "size", "cost", "unit_price",
    "description", "store_group"
]

# Define numeric columns that may contain commas or blanks
NUMERIC_COLUMNS = ["unit_price", "cost"]

def clean_data(df):
    for col in NUMERIC_COLUMNS:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "")
            .str.strip()
            .replace(["", "nan", "None"], None)
            .apply(lambda x: float(x) if x not in [None, "None", "nan"] else None)
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

        # TRUNCATE the table before inserting
        cur.execute(f"TRUNCATE TABLE {TABLE_NAME}")
        print(f"⚠️ Table {TABLE_NAME} truncated.")

        # Read and prepare CSV
        df = pd.read_csv(CSV_FILE_PATH, header=None, skiprows=1, usecols=range(0, 8))
        df.columns = DB_COLUMNS
        df = clean_data(df)

        # Prepare SQL insert
        columns = ",".join(DB_COLUMNS)
        values = ",".join(["%s"] * len(DB_COLUMNS))
        insert_query = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({values})"
        data_to_insert = [tuple(row) for row in df.itertuples(index=False, name=None)]

        # Execute insert
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
