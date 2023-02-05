from unittest import TestCase
from pyspark.sql import SparkSession
from lib.utils import load_stocks_df


class UtilsTestCase(TestCase):
    spark = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("HelloSparkTest") \
            .getOrCreate()

    def test_datafile_loading(self):
        sample_df = load_stocks_df(self.spark, "data/msft.csv")
        result_count = sample_df.count()
        self.assertEqual(result_count, 1259, "Record count should be 1259")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
