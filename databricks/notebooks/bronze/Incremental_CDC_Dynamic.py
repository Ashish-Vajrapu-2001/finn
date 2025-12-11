import json
import pyodbc
from pyspark.sql.functions import current_timestamp, lit, col
from delta.tables import DeltaTable

# ============================================================================
# CONFIGURATION - REPLACE PLACEHOLDERS
# ============================================================================
SQL_SERVER = "YOUR_SQL_SERVER_NAME.database.windows.net"
SQL_DATABASE = "YOUR_DATABASE_NAME"
SQL_USERNAME = "YOUR_SQL_USERNAME"
SQL_PASSWORD = "YOUR_SQL_PASSWORD"

# ============================================================================
# GET PARAMETERS
# ============================================================================
# (Same widgets as Initial Load)
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

print(f"Processing CDC Load: {schema_name}.{table_name}")

try:
    bronze_path = f"/mnt/datalake/bronze/{source_system_id}/{schema_name}/{table_name}/incremental/{run_id}"
    silver_path = f"/mnt/datalake/silver/{source_system_id}/{schema_name}/{table_name}"

    # 1. Read CDC Data
    df_cdc = spark.read.parquet(bronze_path)

    # Get Max Sync Version for Control Update
    max_sync_version = df_cdc.agg({"_current_sync_version": "max"}).collect()[0][0]
    if max_sync_version is None: max_sync_version = 0

    # 2. Add Metadata
    df_prepared = df_cdc \
        .withColumn("_load_timestamp", current_timestamp()) \
        .withColumn("_source_system_id", lit(source_system_id)) \
        .withColumn("_source_system_name", lit(source_system_name)) \
        .withColumn("_run_id", lit(run_id)) \
        .withColumn("_is_deleted", col("SYS_CHANGE_OPERATION") == "D") \
        .withColumn("_cdc_operation", col("SYS_CHANGE_OPERATION")) \
        .drop("_current_sync_version")

    # 3. Build MERGE Condition (Handles Composite Keys)
    pk_cols = [pk.strip() for pk in primary_key_columns.split(',')]
    merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in pk_cols])
    print(f"Merge Condition: {merge_condition}")

    # 4. Perform Merge
    delta_table = DeltaTable.forPath(spark, silver_path)

    # Columns to update (exclude system cols like SYS_CHANGE_VERSION from source if they exist)
    # We map source cols to target cols
    update_mapping = {col: f"source.{col}" for col in df_prepared.columns if not col.startswith("SYS_")}

    delta_table.alias("target").merge(
        df_prepared.alias("source"),
        merge_condition
    ).whenMatchedUpdate(
        condition = "source.SYS_CHANGE_OPERATION != 'D'",
        set = update_mapping
    ).whenMatchedUpdate(
        condition = "source.SYS_CHANGE_OPERATION = 'D'",
        set = {
            "_is_deleted": "true",
            "_load_timestamp": "source._load_timestamp",
            "_run_id": "source._run_id",
            "_cdc_operation": "source._cdc_operation"
        }
    ).whenNotMatchedInsert(
        condition = "source.SYS_CHANGE_OPERATION != 'D'",
        values = update_mapping
    ).execute()

    # 5. Update Control Metadata
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}"
    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            sql = f"""
            UPDATE control.table_metadata
            SET last_load_status = 'success',
                last_load_timestamp = GETDATE(),
                rows_processed_last_run = {rows_copied},
                last_sync_version = {max_sync_version},
                last_run_id = '{run_id}',
                updated_at = GETDATE()
            WHERE table_id = '{table_id}'
            """
            cursor.execute(sql)
            conn.commit()

    print(f"Success. Sync Version Updated to {max_sync_version}")
    dbutils.notebook.exit(json.dumps({"status": "success", "rows": rows_copied}))

except Exception as e:
    print(f"Error: {str(e)}")
    # Log failure
    try:
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}"
        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"UPDATE control.table_metadata SET last_load_status = 'failed' WHERE table_id = '{table_id}'")
                conn.commit()
    except:
        pass
    raise e
