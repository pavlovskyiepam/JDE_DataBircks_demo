import unittest
import pickle
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegressionModel

class TestModel(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestModel").getOrCreate()
        cls.test_data = cls.spark.createDataFrame([
            (0, [1.0, 2.0, 3.0]),
            (1, [4.0, 5.0, 6.0]),
            (0, [7.0, 8.0, 9.0])
        ], ["label", "features"])
        
        # Load trained model
        with open("/dbfs/mnt/models/latest_model.pkl", "rb") as f:
            cls.model = pickle.load(f)

    def test_model_prediction(self):
        predictions = self.model.transform(self.test_data)
        self.assertTrue("prediction" in predictions.columns)
        self.assertEqual(predictions.count(), 3)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == "__main__":
    unittest.main()
