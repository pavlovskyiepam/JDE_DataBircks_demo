from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler

def train_model():
    spark = SparkSession.builder.appName("ModelTraining").getOrCreate()

    df = spark.read.format("delta").load("/mnt/delta/features/")

    # Define model
    lr = LogisticRegression(featuresCol="features", labelCol="label")

    # Train model
    model = lr.fit(df)

    # Save trained model
    model.write().overwrite().save("/mnt/models/latest_model")

    print("Model training completed.")

if __name__ == "__main__":
    train_model()
