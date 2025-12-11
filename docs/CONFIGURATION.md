# Configuration & Placeholders

The code contains placeholders that MUST be replaced before deployment.

## Placeholder Reference

| Placeholder | Location | Description |
|-------------|----------|-------------|
| YOUR_SQL_SERVER_NAME | LS_AzureSQL_Control, Notebooks | Azure SQL Server Name (e.g., `myserver`) |
| YOUR_DATABASE_NAME | LS_AzureSQL_Control, Notebooks | Database Name |
| YOUR_SQL_USERNAME | LS_AzureSQL_Control, Notebooks | Admin Username |
| YOUR_SQL_PASSWORD | LS_AzureSQL_Control, Notebooks | Admin Password |
| YOUR_STORAGE_ACCOUNT_NAME | LS_AzureDataLake, Mount_ADLS.py | ADLS Gen2 Account Name |
| YOUR_STORAGE_ACCOUNT_KEY | LS_AzureDataLake | Access Key |
| YOUR_DATABRICKS_URL | LS_AzureDatabricks | Workspace URL (https://adb-...) |
| YOUR_DATABRICKS_TOKEN | LS_AzureDatabricks | PAT Token |
| YOUR_SERVICE_PRINCIPAL_ID | Mount_ADLS.py | App Registration Client ID |
| YOUR_SERVICE_PRINCIPAL_SECRET | Mount_ADLS.py | Client Secret |
| YOUR_TENANT_ID | Mount_ADLS.py | Azure AD Tenant ID |

## Security Note
In a production environment, do NOT hardcode secrets in Notebooks. Use Azure Key Vault backed Secret Scopes in Databricks:
`dbutils.secrets.get(scope="my-scope", key="sql-password")`
