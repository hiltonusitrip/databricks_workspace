from pyspark.sql import functions as F
source_path = "/Volumes/<catalog>/<schema>/<volume>/incoming_csv/"

@dlt.table
def masked_vendor_demo_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format","csv")
            .load('/Volumes/hilton_catalog/bronze/data_files/'
            )
    )

def mask_generic_drug_code(generic_drug_code):
    return F.sha2(F.col(generic_drug_code),256)

@dlt.table
def masked_vendor_demo_silver():
    return (
        dlt.read_stream("masked_vendor_demo_bronze")
        .select('PERIOD_START_DATE', 
                mask_generic_drug_code('GENERIC_DRUG_CODE').alias("masked_generic_drug_code"))
    )