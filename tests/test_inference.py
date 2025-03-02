import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegressionModel
from scripts.inference import run_inference

class InferenceTest(unittest.TestCase):

    @patch('scripts.inference.SparkSession')
    @patch('scripts.inference.LogisticRegressionModel')
    def test_run_inference(self, mock_lr_model, mock_spark_session):
        # Mock Spark session
        mock_spark = MagicMock()
        mock_spark_session.builder.appName.return_value.getOrCreate.return_value = mock_spark
        mock_df = MagicMock()
        mock_spark.read.format.return_value.load.return_value = mock_df
        
        # Mock model
        mock_model = MagicMock()
        mock_lr_model.load.return_value = mock_model
        mock_model.transform.return_value = mock_df
        
        # Run inference
        run_inference()
        
        # Assertions
        mock_spark_session.builder.appName.assert_called_once_with("ModelInference")
        mock_spark.read.format.assert_called_once_with("delta")
        mock_spark.read.format().load.assert_called_once_with("/mnt/delta/features/")
        mock_lr_model.load.assert_called_once_with("/mnt/models/latest_model")
        mock_model.transform.assert_called_once_with(mock_df)
        mock_df.show.assert_called_once()

if __name__ == "__main__":
    unittest.main()
