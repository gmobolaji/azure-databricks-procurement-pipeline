from pyspark.sql.functions import sum, count

# Load Silver
silver_df = spark.read.format("delta").load("/mnt/silver/purchase_orders_enriched")

# Aggregate Spend
gold_df = (
    silver_df.groupBy("supplier_id", "category")
    .agg(sum("amount").alias("total_spend"), count("*").alias("order_count"))
)

# Save to Gold
gold_df.write.format("delta").mode("overwrite").save("/mnt/gold/supplier_spend_summary")

display(gold_df.limit(10))
