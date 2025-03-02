import unittest
from unittest.mock import patch, MagicMock
from scripts.train_model import train_model

class TestTrainModel(unittest.TestCase):

    @patch('scripts.train_model.SparkSession')
    @patch('scripts.train_model.LogisticRegression')
    def test_train_model(self, MockLogisticRegression, MockSparkSession):
        # Mock Spark session and LogisticRegression
        mock_spark = MagicMock()
        MockSparkSession.builder.appName.return_value.getOrCreate.return_value = mock_spark
        mock_df = mock_spark.read.format.return_value.load.return_value
        mock_lr = MockLogisticRegression.return_value
        mock_model = mock_lr.fit.return_value

        # Call the function
        train_model()

        # Assertions
        MockSparkSession.builder.appName.assert_called_once_with("ModelTraining")
        mock_spark.read.format.assert_called_once_with("delta")
        mock_spark.read.format().load.assert_called_once_with("/mnt/delta/features/")
        MockLogisticRegression.assert_called_once_with(featuresCol="features", labelCol="label")
        mock_lr.fit.assert_called_once_with(mock_df)
        mock_model.write().overwrite().save.assert_called_once_with("/mnt/models/latest_model")

if __name__ == "__main__":
    unittest.main()
