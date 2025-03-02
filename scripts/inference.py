from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegressionModel

def run_inference():
    spark = SparkSession.builder.appName("ModelInference").getOrCreate()

    df = spark.read.format("delta").load("/mnt/delta/features/")

    # Load trained model
    model = LogisticRegressionModel.load("/mnt/models/latest_model")

    # Predict
    predictions = model.transform(df)

    predictions.show()

if __name__ == "__main__":
    run_inference()
