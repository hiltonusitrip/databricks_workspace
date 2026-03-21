-- Setup script to create catalog, schema, and grant permissions
-- Run this script in a Databricks SQL notebook or SQL editor before deploying the pipeline

-- Step 1: Create catalog if it doesn't exist
CREATE CATALOG IF NOT EXISTS hilton_hotmail;

-- Step 2: Grant USE CATALOG permission to your user
GRANT USE CATALOG ON CATALOG hilton_hotmail TO `hma68@hotmail.com`;

-- Step 3: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS hilton_hotmail.dev;

-- Step 4: Grant necessary permissions on the schema
-- These permissions are required for Delta Live Tables pipelines
GRANT USE SCHEMA ON SCHEMA hilton_hotmail.dev TO `hma68@hotmail.com`;
GRANT CREATE TABLE ON SCHEMA hilton_hotmail.dev TO `hma68@hotmail.com`;
GRANT CREATE FUNCTION ON SCHEMA hilton_hotmail.dev TO `hma68@hotmail.com`;
GRANT MODIFY ON SCHEMA hilton_hotmail.dev TO `hma68@hotmail.com`;
GRANT SELECT ON SCHEMA hilton_hotmail.dev TO `hma68@hotmail.com`;
GRANT READ_METADATA ON SCHEMA hilton_hotmail.dev TO `hma68@hotmail.com`;

-- Step 5: Grant permissions on the catalog itself (for creating schemas)
GRANT CREATE SCHEMA ON CATALOG hilton_hotmail TO `hma68@hotmail.com`;

-- Verify permissions
SHOW GRANTS ON CATALOG hilton_hotmail;
SHOW GRANTS ON SCHEMA hilton_hotmail.dev;

