import json
import pyodbc
from pyspark.sql.functions import current_timestamp, lit

# ============================================================================
# CONFIGURATION - REPLACE PLACEHOLDERS
# ============================================================================
SQL_SERVER = "YOUR_SQL_SERVER_NAME.database.windows.net"
SQL_DATABASE = "YOUR_DATABASE_NAME"
SQL_USERNAME = "YOUR_SQL_USERNAME"
SQL_PASSWORD = "YOUR_SQL_PASSWORD"
JDBC_URL = f"jdbc:sqlserver://{SQL_SERVER}:1433;database={SQL_DATABASE}"

# ============================================================================
# GET PARAMETERS
# ============================================================================
dbutils.widgets.text("table_id", "")
dbutils.widgets.text("source_system_id", "")
dbutils.widgets.text("source_system_name", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("primary_key_columns", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("rows_copied", "0")

table_id = dbutils.widgets.get("table_id")
source_system_id = dbutils.widgets.get("source_system_id")
source_system_name = dbutils.widgets.get("source_system_name")
schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")
primary_key_columns = dbutils.widgets.get("primary_key_columns")
run_id = dbutils.widgets.get("run_id")
rows_copied = int(dbutils.widgets.get("rows_copied"))

print(f"Processing Initial Load: {schema_name}.{table_name} (RunID: {run_id})")

try:
    # Paths
    bronze_path = f"/mnt/datalake/bronze/{source_system_id}/{schema_name}/{table_name}/initial_load/{run_id}"
    silver_path = f"/mnt/datalake/silver/{source_system_id}/{schema_name}/{table_name}"

    # 1. Read Bronze
    df_bronze = spark.read.parquet(bronze_path)

    # 2. Validate PKs
    pk_cols = [pk.strip() for pk in primary_key_columns.split(',')]
    for pk in pk_cols:
        if pk not in df_bronze.columns:
            raise ValueError(f"PK Column {pk} missing in source data")

    # 3. Add Metadata
    df_enriched = df_bronze \
        .withColumn("_load_timestamp", current_timestamp()) \
        .withColumn("_source_system_id", lit(source_system_id)) \
        .withColumn("_source_system_name", lit(source_system_name)) \
        .withColumn("_run_id", lit(run_id)) \
        .withColumn("_is_deleted", lit(False)) \
        .withColumn("_cdc_operation", lit("INITIAL_LOAD"))

    # 4. Write to Silver (Overwrite for Initial Load)
    df_enriched.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(silver_path)

    # 5. Optimize
    spark.sql(f"OPTIMIZE delta.`{silver_path}`")

    # 6. Update Control Table (Using PyODBC for Transactional Update)
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}"

    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            # We don't set last_sync_version yet, wait for first CDC run or manual set
            # Or assume 0 start. Here we mark completed=1
            sql = f"""
            UPDATE control.table_metadata
            SET last_load_status = 'success',
                last_load_timestamp = GETDATE(),
                rows_processed_last_run = {rows_copied},
                initial_load_completed = 1,
                last_run_id = '{run_id}',
                updated_at = GETDATE()
            WHERE table_id = '{table_id}'
            """
            cursor.execute(sql)
            conn.commit()

    print("Success")
    dbutils.notebook.exit(json.dumps({"status": "success", "rows": rows_copied}))

except Exception as e:
    print(f"Error: {str(e)}")
    # Log failure to control DB
    try:
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}"
        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"UPDATE control.table_metadata SET last_load_status = 'failed' WHERE table_id = '{table_id}'")
                conn.commit()
    except:
        pass
    raise e
