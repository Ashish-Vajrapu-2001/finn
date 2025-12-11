# Deployment Guide

## Prerequisites
*   Azure Data Factory
*   Azure SQL Database (S0 or higher recommended)
*   Azure Databricks Workspace
*   ADLS Gen2 Storage Account
*   Service Principal with "Storage Blob Data Contributor" role

## Steps

### 1. Database Setup
1.  Connect to your Azure SQL Database.
2.  Run `sql/control_tables/01_create_control_tables.sql`.
3.  Run `sql/control_tables/02_populate_control_tables.sql`.
4.  Run all scripts in `sql/stored_procedures/`.

### 2. ADF Deployment
1.  Open ADF Studio.
2.  Import Linked Services (Update placeholders FIRST).
3.  Import Datasets.
4.  Import Pipelines.
5.  Publish.

### 3. Databricks Setup
1.  Import notebooks from `databricks/notebooks/` folder.
2.  Open `setup/Mount_ADLS.py`, update placeholders, and run it.
3.  Update SQL placeholders in `bronze/Initial_Load_Dynamic.py` and `bronze/Incremental_CDC_Dynamic.py`.

### 4. Enable Change Tracking (On Source)
Run this on your source databases (SRC-001, SRC-002, SRC-003):
```sql
ALTER DATABASE [DB_Name] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 7 DAYS, AUTO_CLEANUP = ON);
ALTER TABLE [Schema].[Table] ENABLE CHANGE_TRACKING;
```

### 5. Execution
Trigger `PL_Master_Orchestrator` in ADF.
