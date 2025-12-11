# ============================================================================
# MOUNT ADLS GEN2 TO DATABRICKS
# ============================================================================
# TODO: REPLACE PLACEHOLDERS BEFORE RUNNING
# ============================================================================

STORAGE_ACCOUNT_NAME = "YOUR_STORAGE_ACCOUNT_NAME"
CONTAINER_NAME = "datalake"

# Ideally, retrieve these from Key Vault using dbutils.secrets.get()
SP_CLIENT_ID = "YOUR_SERVICE_PRINCIPAL_CLIENT_ID"
SP_CLIENT_SECRET = "YOUR_SERVICE_PRINCIPAL_SECRET"
SP_TENANT_ID = "YOUR_TENANT_ID"

mount_point = "/mnt/datalake"
source = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/"

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": SP_CLIENT_ID,
    "fs.azure.account.oauth2.client.secret": SP_CLIENT_SECRET,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{SP_TENANT_ID}/oauth2/token"
}

# Unmount if exists
if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(mount_point)

# Mount
dbutils.fs.mount(source=source, mount_point=mount_point, extra_configs=configs)

# Create Layer Structure
dbutils.fs.mkdirs(f"{mount_point}/bronze")
dbutils.fs.mkdirs(f"{mount_point}/silver")
dbutils.fs.mkdirs(f"{mount_point}/gold")

print(f"Mounted {source} to {mount_point}")
display(dbutils.fs.ls(mount_point))
