from pyspark.sql.functions import col, to_date

# Load Bronze data
bronze_orders = spark.read.format("delta").load("/mnt/bronze/purchase_orders")
bronze_suppliers = spark.read.option("header", True).csv("/Workspace/data/suppliers.csv")

# Clean & Join
silver_df = (
    bronze_orders.join(bronze_suppliers, "supplier_id", "left")
    .withColumn("amount", col("amount").cast("double"))
    .withColumn("order_date", to_date(col("date"), "yyyy-MM-dd"))
    .filter(col("amount") > 0)
)

# Save Silver (curated)
silver_df.write.format("delta").mode("overwrite").save("/mnt/silver/purchase_orders_enriched")

display(silver_df.limit(5))
