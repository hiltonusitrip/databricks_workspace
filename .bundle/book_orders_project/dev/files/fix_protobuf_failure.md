# Fix: Proto Json Module Error Causing Pipeline Failure

## Problem
Pipeline is failing with: "Proto Json module is unavailable on classpath, proto serializer not enabled"

## Changes Made

### 1. Updated Pipeline Configuration
- Changed `channel` from `CURRENT` to `STABLE` (more stable runtime)
- Added Spark configuration optimizations

### 2. Next Steps to Try

#### Option A: Deploy and Test (Recommended First)
```bash
databricks bundle deploy
```
Then start the pipeline and check if the STABLE channel resolves the issue.

#### Option B: If Still Failing - Try PREVIEW Channel
If STABLE doesn't work, try PREVIEW channel which may have fixes:
```yaml
channel: PREVIEW
```

#### Option C: Check Pipeline Logs for Exact Error
1. Go to **Workflows** → **Delta Live Tables** → Your pipeline
2. Click on the failed run
3. Check the **Error** tab for the exact error message
4. Look for the specific cell/notebook that's failing

#### Option D: Add Initialization Cell to Notebooks
If the issue persists, add this as the first cell in your DLT notebooks:

**For SQL notebooks (dlt.ipynb, cdc.ipynb):**
```sql
-- Set Spark configuration to handle protobuf
SET spark.sql.connect.protobuf.enabled = false;
```

#### Option E: Contact Databricks Support
If none of the above work, this might be a known issue with the runtime. Contact Databricks support with:
- Pipeline ID: `190a06ec-3905-4b71-beff-5c0903f18215`
- Error message: "Proto Json module is unavailable on classpath"
- Runtime channel: STABLE/PREVIEW/CURRENT
- Serverless: true

## Current Configuration

The pipeline now uses:
- **Channel**: `STABLE` (changed from `CURRENT`)
- **Serverless**: `true`
- **Spark Config**: Added optimizations

## Verification

After deploying, check:
1. Pipeline starts successfully
2. No protobuf errors in logs
3. Tables are created in `hilton_hotmail.dev`

## Additional Notes

- This error is typically related to Spark Connect's Java backend
- Serverless compute should handle dependencies automatically
- The STABLE channel may have better compatibility than CURRENT
- If the error persists, it might require a Databricks runtime update

