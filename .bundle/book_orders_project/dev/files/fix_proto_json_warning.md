# Fix: Proto Json Module Unavailable Warning

## Understanding the Error

The message "Proto Json module is unavailable on classpath, proto serializer not enabled" is typically a **warning** (not an error) from Spark Connect's Java backend. In most cases, it doesn't affect functionality.

## Is This Actually a Problem?

**Check if it's blocking your work:**
- If your code runs successfully → **Safe to ignore**
- If you're getting actual errors → **Follow solutions below**

## Solutions

### Solution 1: Usually Safe to Ignore (Most Common)

If your Spark operations are working correctly, this is just a warning from the Java backend and can be safely ignored. Spark Connect will fall back to alternative serialization methods.

### Solution 2: Update Databricks Connect (If Using Locally)

If you're using `databricks-connect` locally for development:

```bash
# Update to latest compatible version
uv sync --group dev
```

Or update the version in `pyproject.toml`:
```toml
[dependency-groups]
dev = [
    "pytest",
    "databricks-dlt",
    "databricks-connect>=15.4,<15.6",  # Try newer version
]
```

### Solution 3: For DLT Pipelines (Your Case)

Since your pipeline uses `serverless: true`, this warning should **not affect pipeline execution**. Serverless compute manages all dependencies automatically.

**If the pipeline is failing:**
1. Check the actual error in pipeline logs (not just warnings)
2. Verify schema permissions (previous issue)
3. Check volume access permissions

### Solution 4: Suppress the Warning (If Needed)

If the warning is cluttering your logs, you can suppress it by setting Spark configuration:

```python
# In your notebook or code
spark.conf.set("spark.sql.connect.protobuf.enabled", "false")
```

**Note:** This disables protobuf serialization, but Spark will use alternative methods.

### Solution 5: Add Protobuf Dependency (Rarely Needed)

If you're packaging a custom library, you might need to add protobuf:

```toml
[dependencies]
protobuf = ">=4.0.0"  # Python protobuf library
```

However, this usually doesn't help since the error is from the Java classpath, not Python.

## For Your DLT Pipeline

Your pipeline configuration looks correct:
- `serverless: true` ✅
- Proper catalog/schema setup ✅
- Notebook paths configured ✅

**Recommendation:** If your pipeline runs successfully despite this warning, **ignore it**. It's a known Spark Connect warning that doesn't affect Delta Live Tables execution.

## Verification

To verify everything is working:
1. Check if your pipeline completes successfully
2. Verify tables are created in `hilton_hotmail.dev`
3. Check pipeline logs for actual errors (not warnings)

If the pipeline works, the warning can be safely ignored.

