# DISPLAY ONLY
print("""
SETUP INSTRUCTIONS
==================

1. REPLACE PLACEHOLDERS
   Go to /setup/Mount_ADLS.py and fill in Storage Account & Service Principal details.
   Go to /bronze/Initial_Load_Dynamic.py and fill in SQL Server details.
   Go to /bronze/Incremental_CDC_Dynamic.py and fill in SQL Server details.

2. MOUNT STORAGE
   Run the Mount_ADLS notebook once.

3. VALIDATE
   Run the Validate_Environment notebook to check connections.

4. DEPLOY SQL
   Ensure control tables are populated using scripts in sql/control_tables/
""")
