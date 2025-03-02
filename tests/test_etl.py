import unittest
from scripts.etl import process_bronze_to_silver

class TestETL(unittest.TestCase):
    def test_process_bronze_to_silver(self):
        try:
            process_bronze_to_silver()
            self.assertTrue(True)
        except:
            self.assertTrue(False)

if __name__ == '__main__':
    unittest.main()
