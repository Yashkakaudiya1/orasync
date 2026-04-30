import snowflake.connector

conn = snowflake.connector.connect(
    user="ORASYNC",
    password="Yash@9111324476",
    account="usvioke-gr39576",
    warehouse="COMPUTE_WH",   # important
    database="ORASYNC_DB",    # optional but good
    schema="RAW"              # optional
)

print("Connected to Snowflake")

conn.close()
