import pandas as pd
import snowflake.connector
import os

# ---------------- CONFIG ----------------
USER = "ORASYNC"
PASSWORD = "Yash@9111324476"
ACCOUNT = "usvioke-gr39576"
WAREHOUSE = "COMPUTE_WH"
DATABASE = "ORASYNC_DB"
SCHEMA = "RAW"

# ---------------- CONNECT ----------------
conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
)

cursor = conn.cursor()

cursor.execute(f"USE WAREHOUSE {WAREHOUSE}")
cursor.execute(f"USE DATABASE {DATABASE}")
cursor.execute(f"USE SCHEMA {SCHEMA}")

print("Connected to Snowflake")

# ---------------- DATA ----------------
tables = {
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

os.makedirs("output", exist_ok=True)

# ---------------- PIPELINE ----------------
for table_name, df in tables.items():

    print(f"\nProcessing table: {table_name}")

    # 1. Save parquet
    file_path = f"output/{table_name}.parquet"
    df.to_parquet(file_path, index=False)

    # 2. Create table dynamically
    cols = []
    for col, dtype in zip(df.columns, df.dtypes):
        if "int" in str(dtype):
            cols.append(f"{col} INT")
        else:
            cols.append(f"{col} STRING")

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS ORASYNC_DB.RAW.{table_name} (
        {', '.join(cols)}
    )
    """

    cursor.execute(create_sql)

    # 3. Stage
    cursor.execute("CREATE STAGE IF NOT EXISTS ORASYNC_STAGE")

    # 4. Upload
    abs_path = os.path.abspath(file_path).replace("\\", "/")
    cursor.execute(f"PUT 'file://{abs_path}' @ORASYNC_STAGE")

    # 5. Load
    cursor.execute(f"""
        COPY INTO ORASYNC_DB.RAW.{table_name}
        FROM @ORASYNC_STAGE
        FILE_FORMAT=(TYPE=PARQUET)
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
    """)

    # 6. Validate
    cursor.execute(f"SELECT COUNT(*) FROM ORASYNC_DB.RAW.{table_name}")
    count = cursor.fetchone()[0]

    print(f"{table_name} → {count} rows loaded")

# ---------------- CLOSE ----------------
cursor.close()
conn.close()

print("\n🚀 Multi-table pipeline completed")
