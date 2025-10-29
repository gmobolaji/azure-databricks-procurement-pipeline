from pyspark.sql.functions import input_file_name, current_timestamp

# Load raw CSV data
bronze_df = (
    spark.read.option("header", True)
    .csv("/Workspace/data/purchase_orders.csv")
    .withColumn("source_file", input_file_name())
    .withColumn("ingestion_time", current_timestamp())
)

# Write to Bronze layer (raw Delta)
bronze_df.write.format("delta").mode("overwrite").save("/mnt/bronze/purchase_orders")

display(bronze_df.limit(5))
