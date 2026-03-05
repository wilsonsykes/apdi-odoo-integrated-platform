import psycopg2
import pandas as pd

# Database connection details
DB_NAME = "apdireports"
DB_USER = "postgres"
DB_PASSWORD = "d4s31n@"
DB_HOST = "192.168.2.152"
DB_PORT = "5432"

# CSV file path
CSV_FILE_PATH = r"C:\Users\OP5\Desktop\APDIRepSysFiles\Import_CSV\sales_raw_data_import.csv"

# PostgreSQL table name
TABLE_NAME = "sm_sales_raw_data"

# Define actual column names (excluding 'id')
DB_COLUMNS = [
    "order_reference", "delivery_date", "product", "customer", "quantity", "unit_price",
    "description", "product_category", "weight", "salesperson", "type_of_payment",
    "invoice_status", "order_status", "delivery_quantity", "barcode", "subtotal",
    "variant_sku", "spatio_description"
]

# Columns that should be treated as numeric
NUMERIC_COLUMNS = ["quantity", "unit_price", "subtotal", "delivery_quantity", "weight"]

def clean_data(df):
    """Cleans the dataframe by removing commas and converting numerics properly."""
    for col in NUMERIC_COLUMNS:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df = df.where(pd.notna(df), None)  # Convert NaNs to None for PostgreSQL
    return df

def import_csv_to_postgres():
    conn = None
    cur = None
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

        # Load and process CSV
        df = pd.read_csv(CSV_FILE_PATH, header=None, skiprows=1, usecols=range(1, 19))
        df.columns = DB_COLUMNS
        df = clean_data(df)

        # Convert dataframe to list of tuples (handle numpy types)
        data_to_insert = [
            tuple(x.item() if hasattr(x, "item") else x for x in row)
            for row in df.itertuples(index=False, name=None)
        ]

        # Prepare SQL INSERT query
        columns = ",".join(DB_COLUMNS)
        values = ",".join(["%s"] * len(DB_COLUMNS))
        insert_query = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({values})"

        # Execute insert
        cur.executemany(insert_query, data_to_insert)
        conn.commit()

        print(f"✅ Data successfully imported into {TABLE_NAME}")

    except Exception as e:
        print(f"❌ Error: {e}")
        if conn:
            conn.rollback()
    
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    import_csv_to_postgres()
