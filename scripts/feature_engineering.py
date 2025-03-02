from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

def generate_features():
    spark = SparkSession.builder.appName("FeatureEngineering").getOrCreate()

    df = spark.read.format("delta").load("/mnt/delta/gold/aggregated_data")

    # Feature Engineering
    assembler = VectorAssembler(inputCols=["count"], outputCol="features")
    df_transformed = assembler.transform(df)

    df_transformed.write.format("delta").mode("overwrite").save("/mnt/delta/features/")

    print("Feature engineering completed.")

if __name__ == "__main__":
    generate_features()
