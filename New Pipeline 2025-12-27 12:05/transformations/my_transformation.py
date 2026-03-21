import dlt
from pyspark.sql import functions as F

@dlt.table
def masked_vendor_demo():
    return (
        dlt.read('vendor_demo_bronze')
        .filter(F.col('period_start_date') < F.lit('2025-01-01'))
    )
