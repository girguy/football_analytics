# Databricks notebook source
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.text("storage_account", "")

# COMMAND ----------

def mount_adls(storageAccountName, containerName, secret_scope):
    # Get secrets from Key Vault
    clientId = dbutils.secrets.get(scope = secret_scope, key = "servicePrincipalClientId") # key in Azure Key Vault
    tenantId = dbutils.secrets.get(scope = secret_scope, key = "servicePrincipalTenantId")
    clientSecret = dbutils.secrets.get(scope = secret_scope, key = "servicePrincipalSecrets")
    
    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": clientId,
              "fs.azure.account.oauth2.client.secret": clientSecret,
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantId}/oauth2/token"}
    
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storageAccountName}/{containerName}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storageAccountName}/{containerName}")
    
    # Mount the storage account container
    dbutils.fs.mount(
      source = f"abfss://{containerName}@{storageAccountName}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storageAccountName}/{containerName}",
      extra_configs = configs)
    
    print(f"/mnt/{storageAccountName}/{containerName} is mounted.")

# COMMAND ----------

storage_account = dbutils.widgets.get("storage_account")
secret_scope = dbutils.widgets.get("secret_scope")

# COMMAND ----------

mount_adls(storage_account, 'bronze', secret_scope)

# COMMAND ----------

mount_adls(storage_account, 'silver', secret_scope)

# COMMAND ----------

mount_adls(storage_account, 'gold', secret_scope)
