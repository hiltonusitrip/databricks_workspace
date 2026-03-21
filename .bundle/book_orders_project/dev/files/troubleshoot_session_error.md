# Troubleshooting: Spark Session Not Found Error

## Error Message
```
[INVALID_HANDLE.SESSION_NOT_FOUND] The handle ... is invalid. Session not found.
```

## Quick Fixes (Try in Order)

### 1. Restart the Notebook Session
**In Databricks Notebook:**
- Click **Detach & Re-attach** in the cluster dropdown
- Or go to **Run** → **Restart Session**
- This will create a fresh Spark session

### 2. Restart the Cluster
**In Databricks:**
- Go to **Compute** → Select your cluster → **Restart**
- Wait for cluster to be running
- Re-attach notebook to the cluster

### 3. Re-run All Cells
- After restarting session/cluster, run all cells from the beginning
- This ensures Spark session is properly initialized

### 4. Check Cluster Status
- Verify the cluster is **Running** (not Terminated or Error)
- Check cluster logs for any issues
- Ensure cluster has proper permissions

### 5. For Delta Live Tables Pipelines
If this error occurs in a DLT pipeline:
- Check pipeline logs in **Workflows** → **Delta Live Tables**
- Verify the pipeline cluster configuration
- Ensure serverless compute is enabled (if using serverless)
- Check that catalog and schema permissions are correct

## Prevention Tips

1. **Use Serverless Compute** (already configured in your pipeline)
   - Serverless automatically manages sessions
   - Less prone to session expiration

2. **Add Session Check in Notebooks**
   ```python
   # Add this at the beginning of notebooks
   try:
       spark.sql("SELECT 1")
   except Exception as e:
       print(f"Session error: {e}")
       # Session will be recreated automatically
   ```

3. **Use Proper Error Handling**
   ```python
   from pyspark.sql import SparkSession
   
   def get_spark_session():
       try:
           return spark
       except:
           return SparkSession.builder.getOrCreate()
   ```

4. **Monitor Cluster Activity**
   - Set cluster auto-termination appropriately
   - Use cluster pools for faster startup
   - Monitor cluster health metrics

## For Pipeline-Specific Issues

If this occurs in your DLT pipeline (`dlt_china`):

1. **Check Pipeline Configuration**
   - Verify `serverless: true` is set (already configured)
   - Check `catalog` and `schema` settings
   - Verify notebook paths are correct

2. **Check Pipeline Logs**
   - Go to **Workflows** → **Delta Live Tables** → Your pipeline
   - Review error logs for specific cell failures
   - Check if it's a permissions issue (previous error)

3. **Redeploy Pipeline**
   ```bash
   databricks bundle deploy
   ```

## Common Causes in DLT Pipelines

1. **Schema Permissions** (if not resolved)
   - Ensure catalog/schema exists
   - Verify permissions are granted

2. **Volume Access**
   - Check access to `/Volumes/hilton_catalog/bronze/dataset`
   - Verify volume permissions

3. **Notebook Path Issues**
   - Ensure notebooks exist at specified paths
   - Check `root_path` configuration

