from pyspark.sql import SparkSession

def process_bronze_to_silver():
    spark = SparkSession.builder.appName("ETL").getOrCreate()

    # Read Bronze layer
    df = spark.read.format("delta").load("/mnt/delta/bronze/raw_data")

    # Data cleaning and transformations
    df_cleaned = df.dropDuplicates().filter(df["status"] == "active")

    # Save to Silver layer
    df_cleaned.write.format("delta").mode("overwrite").save("/mnt/delta/silver/processed_data")

    print("Bronze to Silver processing completed.")

def process_silver_to_gold():
    spark = SparkSession.builder.appName("ETL").getOrCreate()

    # Read Silver layer
    df = spark.read.format("delta").load("/mnt/delta/silver/processed_data")

    # Aggregation and business rules
    df_aggregated = df.groupBy("category").count()

    # Save to Gold layer
    df_aggregated.write.format("delta").mode("overwrite").save("/mnt/delta/gold/aggregated_data")

    print("Silver to Gold processing completed.")

if __name__ == "__main__":
    process_bronze_to_silver()
    process_silver_to_gold()
