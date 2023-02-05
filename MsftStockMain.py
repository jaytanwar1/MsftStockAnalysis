import sys
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import *

if __name__ == "__main__":

    # Retriving Spark Conf
    conf = get_spark_app_config()

    # Initializing Spark Session
    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

    # Initializing Loggers
    logger = Log4j(spark)

    # Check input file path provided as argument
    if len(sys.argv) != 2:
        logger.error("Usage: StockAnalysis <filename>")
        sys.exit(-1)

    logger.info("Starting StockAnalysis")

    stocks_raw_df = load_stocks_df(spark, sys.argv[1])
    partitioned_df = stocks_raw_df.repartition(3)
    logger.info("No of of records found in the file : "+str(partitioned_df.count()))
    # Write the DF in mysql
    write_df_mysql(partitioned_df)

    logger.info("Finished StockAnalysis")
    spark.stop()
