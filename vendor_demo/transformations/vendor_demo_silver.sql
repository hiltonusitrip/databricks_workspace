CREATE OR REFRESH STREAMING TABLE vendor_demo_silver 
AS SELECT *, curdate() load_date FROM STREAM vendor_demo_bronze