# Metadata-Driven CDC Pipeline for CLV Analytics

## Overview
This project implements a robust, metadata-driven Change Data Capture (CDC) pipeline using Azure Data Factory, Azure SQL Database, and Azure Databricks. It is designed to synchronize data from multiple source systems (ERP, CRM, Marketing) into a Silver Layer Delta Lake for Customer Lifetime Value analytics.

## Architecture
Data Flow:
`Source (SQL Server) -> ADF (Copy) -> ADLS Gen2 (Parquet) -> Databricks (PySpark) -> Delta Lake (Silver)`

Key Components:
1.  **Control Database:** Stores metadata, dependencies, and sync versions.
2.  **ADF Orchestrator:** Gets load order dynamically and processes up to 20 tables in parallel.
3.  **Databricks:** Handles schema drift, metadata enrichment, and MERGE operations.

## Key Features
*   **Dependency Aware:** Loads `Customers` before `Orders` automatically.
*   **Composite Key Support:** Handles tables like `CITY_TIER_MASTER` correctly.
*   **Idempotent:** Uses Pipeline RunIDs to prevent data duplication on re-runs.
*   **Multi-Source:** Handles CRM, ERP, and Marketing systems simultaneously.

## Adding New Tables
No code changes required! Just run SQL:
```sql
INSERT INTO control.table_metadata (table_id, source_system_id, ...) VALUES (...);
```
The pipeline picks it up automatically on the next run.
