CREATE OR REFRESH STREAMING TABLE vendor_demo_bronze 
AS SELECT * FROM STREAM read_files (
  '/Volumes/hilton_catalog/bronze/data_files/',
  format=>'csv'
)