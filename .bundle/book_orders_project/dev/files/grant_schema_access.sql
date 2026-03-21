-- Grant access to hilton_hotmail/dev schema for user hma68@hotmail.com
-- Run this in Databricks SQL Editor or a SQL notebook

-- Step 1: Ensure catalog exists and grant catalog access
CREATE CATALOG IF NOT EXISTS hilton_hotmail;
GRANT USE CATALOG ON CATALOG hilton_hotmail TO `hma68@hotmail.com`;

-- Step 2: Ensure schema exists
CREATE SCHEMA IF NOT EXISTS hilton_hotmail.dev;

-- Step 3: Grant all necessary permissions on the schema
-- These are required for Delta Live Tables pipelines to work properly
GRANT USE SCHEMA ON SCHEMA hilton_hotmail.dev TO `hma68@hotmail.com`;
GRANT CREATE TABLE ON SCHEMA hilton_hotmail.dev TO `hma68@hotmail.com`;
GRANT CREATE FUNCTION ON SCHEMA hilton_hotmail.dev TO `hma68@hotmail.com`;
GRANT MODIFY ON SCHEMA hilton_hotmail.dev TO `hma68@hotmail.com`;
GRANT SELECT ON SCHEMA hilton_hotmail.dev TO `hma68@hotmail.com`;
GRANT READ_METADATA ON SCHEMA hilton_hotmail.dev TO `hma68@hotmail.com`;

-- Optional: Grant CREATE SCHEMA permission on catalog (if user needs to create schemas)
GRANT CREATE SCHEMA ON CATALOG hilton_hotmail TO `hma68@hotmail.com`;

-- Verify the permissions were granted
SHOW GRANTS ON SCHEMA hilton_hotmail.dev;

