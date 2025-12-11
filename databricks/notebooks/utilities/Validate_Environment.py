import pyodbc

# REPLACE WITH SAME PLACEHOLDERS
SQL_SERVER = "YOUR_SQL_SERVER_NAME.database.windows.net"
SQL_DATABASE = "YOUR_DATABASE_NAME"
SQL_USERNAME = "YOUR_SQL_USERNAME"
SQL_PASSWORD = "YOUR_SQL_PASSWORD"

print("1. Testing ADLS Mount...")
try:
    dbutils.fs.ls("/mnt/datalake/bronze")
    print("ADLS Mounted successfully")
except Exception as e:
    print(f"ADLS Mount Failed: {e}")

print("\n2. Testing SQL Connection (pyodbc)...")
try:
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}"
    with pyodbc.connect(conn_str) as conn:
        print("SQL Connection successful")
except Exception as e:
    print(f"SQL Connection Failed: {e}")
