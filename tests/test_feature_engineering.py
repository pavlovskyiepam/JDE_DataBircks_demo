import unittest
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from scripts.feature_engineering import generate_features

class TestFeatureEngineering(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestFeatureEngineering").getOrCreate()
        cls.test_data = cls.spark.createDataFrame([
            (1, 10.0),
            (2, 20.0),
            (3, 30.0)
        ], ["id", "count"])

    def test_feature_engineering(self):
        assembler = VectorAssembler(inputCols=["count"], outputCol="features")
        df_transformed = assembler.transform(self.test_data)
        self.assertTrue("features" in df_transformed.columns)
        self.assertEqual(df_transformed.count(), 3)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == "__main__":
    unittest.main()