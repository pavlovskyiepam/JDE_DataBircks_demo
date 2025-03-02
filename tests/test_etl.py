import unittest
from unittest.mock import patch
from scripts.etl import process_bronze_to_silver, process_silver_to_gold

class TestETL(unittest.TestCase):

    @patch('scripts.etl.SparkSession')
    def test_process_bronze_to_silver(self, mock_spark_session):
        mock_spark = mock_spark_session.builder.appName().getOrCreate()
        mock_df = mock_spark.read.format().load()
        mock_df.dropDuplicates.return_value = mock_df
        mock_df.filter.return_value = mock_df

        process_bronze_to_silver()

        mock_spark.read.format.assert_called_with("delta")
        mock_spark.read.format().load.assert_called_with("/mnt/delta/bronze/raw_data")
        mock_df.dropDuplicates.assert_called_once()
        mock_df.filter.assert_called_once_with(mock_df["status"] == "active")
        mock_df.write.format().mode().save.assert_called_with("/mnt/delta/silver/processed_data")

    @patch('scripts.etl.SparkSession')
    def test_process_silver_to_gold(self, mock_spark_session):
        mock_spark = mock_spark_session.builder.appName().getOrCreate()
        mock_df = mock_spark.read.format().load()
        mock_df.groupBy.return_value.count.return_value = mock_df

        process_silver_to_gold()

        mock_spark.read.format.assert_called_with("delta")
        mock_spark.read.format().load.assert_called_with("/mnt/delta/silver/processed_data")
        mock_df.groupBy.assert_called_with("category")
        mock_df.groupBy().count.assert_called_once()
        mock_df.write.format().mode().save.assert_called_with("/mnt/delta/gold/aggregated_data")

if __name__ == "__main__":
    unittest.main()
