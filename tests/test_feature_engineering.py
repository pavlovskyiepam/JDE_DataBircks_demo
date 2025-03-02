import unittest
from unittest.mock import patch
from scripts.feature_engineering import generate_features

class TestFeatureEngineering(unittest.TestCase):

    @patch('scripts.feature_engineering.SparkSession')
    def test_generate_features(self, MockSparkSession):
        mock_spark = MockSparkSession.builder.appName().getOrCreate()
        mock_df = mock_spark.read.format().load()
        
        # Mock the transformation and write process
        mock_assembler = patch('scripts.feature_engineering.VectorAssembler').start()
        mock_transformed_df = mock_assembler.return_value.transform.return_value
        mock_transformed_df.write.format().mode().save()

        generate_features()

        mock_spark.read.format.assert_called_with("delta")
        mock_spark.read.format().load.assert_called_with("/mnt/delta/gold/aggregated_data")
        mock_assembler.assert_called_with(inputCols=["count"], outputCol="features")
        mock_assembler.return_value.transform.assert_called_with(mock_df)
        mock_transformed_df.write.format.assert_called_with("delta")
        mock_transformed_df.write.format().mode.assert_called_with("overwrite")
        mock_transformed_df.write.format().mode().save.assert_called_with("/mnt/delta/features/")

if __name__ == "__main__":
    unittest.main()
