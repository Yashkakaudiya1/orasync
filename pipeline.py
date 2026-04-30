import pandas as pd
import snowflake.connector
import os

# CONFIG
USER = "ORASYNC"
PASSWORD = "Yash@9111324476"
ACCOUNT = "usvioke-gr39576"
WAREHOUSE = "COMPUTE_WH"
DATABASE = "ORASYNC_DB"
SCHEMA = "RAW"

# CREATE DATA
os.makedirs("output", exist_ok=True)

df = pd.DataFrame({
    "emp_id": [101, 102, 103],
    "name": ["Yash", "Rahul", "Priya"],
    "salary": [75000, 65000, 80000]
})

file_path = "output/employees.parquet"
df.to_parquet(file_path, index=False)

print("Parquet created")

# CONNECT
conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
)

cursor = conn.cursor()

# SET CONTEXT
cursor.execute(f"USE WAREHOUSE {WAREHOUSE}")
cursor.execute(f"USE DATABASE {DATABASE}")
cursor.execute(f"USE SCHEMA {SCHEMA}")

print("Connected")

# CREATE TABLE
cursor.execute("""
CREATE TABLE IF NOT EXISTS ORASYNC_DB.RAW.employees (
    emp_id INT,
    name STRING,
    salary INT
)
""")

# CREATE STAGE
cursor.execute("""
CREATE STAGE IF NOT EXISTS ORASYNC_DB.RAW.ORASYNC_STAGE
""")

# UPLOAD FILE
abs_path = os.path.abspath(file_path).replace("\\", "/")
cursor.execute(f"PUT 'file://{abs_path}' @ORASYNC_DB.RAW.ORASYNC_STAGE")

# LOAD DATA
cursor.execute("""
COPY INTO ORASYNC_DB.RAW.employees
FROM @ORASYNC_DB.RAW.ORASYNC_STAGE
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
""")

# VERIFY
cursor.execute("SELECT * FROM ORASYNC_DB.RAW.employees")
rows = cursor.fetchall()

for row in rows:
    print(row)

cursor.close()
conn.close()

print("Pipeline completed 🚀")
