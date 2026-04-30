import pandas as pd
import snowflake.connector
import os
import logging

# 👉 import config variables
from config import *

# ==============================
# LOGGING SETUP
# ==============================
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

log = logging.getLogger()

# ==============================
# CONNECT TO SNOWFLAKE
# ==============================
def connect_snowflake():
    log.info("Connecting to Snowflake")

    conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
    )

    cursor = conn.cursor()

    # set context (important)
    cursor.execute(f"USE WAREHOUSE {WAREHOUSE}")
    cursor.execute(f"USE DATABASE {DATABASE}")
    cursor.execute(f"USE SCHEMA {SCHEMA}")

    return conn, cursor


# ==============================
# EXTRACT DATA (SIMULATED SOURCE)
# ==============================
def extract_data():
    log.info("Extracting data")

    return {
        "employees": pd.DataFrame({
            "emp_id": [101, 102, 103],
            "name": ["Yash", "Rahul", "Priya"],
            "salary": [75000, 65000, 80000]
        }),
        "departments": pd.DataFrame({
            "dept_id": [1, 2],
            "dept_name": ["Engineering", "Finance"]
        })
    }


# ==============================
# CREATE TABLE
# ==============================
def create_table(cursor, table_name, df):
    cols = []
    for col, dtype in zip(df.columns, df.dtypes):
        if "int" in str(dtype):
            cols.append(f"{col} INT")
        else:
            cols.append(f"{col} STRING")

    sql = f"""
    CREATE TABLE IF NOT EXISTS ORASYNC_DB.RAW.{table_name} (
        {', '.join(cols)}
    )
    """

    cursor.execute(sql)


# ==============================
# LOAD DATA
# ==============================
def load_table(cursor, table_name, df):
    os.makedirs("output", exist_ok=True)

    file_path = f"output/{table_name}.parquet"
    df.to_parquet(file_path, index=False)

    cursor.execute("CREATE STAGE IF NOT EXISTS ORASYNC_STAGE")

    # 🔥 important fix (avoid duplicate data)
    cursor.execute(f"TRUNCATE TABLE ORASYNC_DB.RAW.{table_name}")

    abs_path = os.path.abspath(file_path).replace("\\", "/")

    cursor.execute(f"PUT 'file://{abs_path}' @ORASYNC_STAGE")

    cursor.execute(f"""
        COPY INTO ORASYNC_DB.RAW.{table_name}
        FROM @ORASYNC_STAGE
        FILE_FORMAT=(TYPE=PARQUET)
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
    """)


# ==============================
# VALIDATION
# ==============================
def validate(cursor, table_name, expected):
    cursor.execute(f"SELECT COUNT(*) FROM ORASYNC_DB.RAW.{table_name}")
    actual = cursor.fetchone()[0]

    if actual == expected:
        log.info(f"{table_name} validation passed ({actual})")
        print(f"✅ {table_name}: {actual} rows")
    else:
        log.error(f"{table_name} mismatch {expected} vs {actual}")
        print(f"❌ {table_name}: mismatch")


# ==============================
# MAIN PIPELINE
# ==============================
def run_pipeline():
    try:
        conn, cursor = connect_snowflake()
        tables = extract_data()

        for table_name, df in tables.items():
            log.info(f"Processing {table_name}")

            create_table(cursor, table_name, df)
            load_table(cursor, table_name, df)
            validate(cursor, table_name, len(df))

        cursor.close()
        conn.close()

        log.info("Pipeline completed successfully")
        print("\n🚀 Pipeline completed")

    except Exception as e:
        log.error(f"Pipeline failed: {e}")
        print("\n❌ Error:", e)


# ==============================
# RUN
# ==============================
if __name__ == "__main__":
    run_pipeline()
