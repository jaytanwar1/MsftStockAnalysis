import configparser

from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, DoubleType

# Method to load raw CSV file as DF
def load_stocks_df(spark, data_file):
    # File Schema Defination
    stocksSchemaStruct = StructType([
        StructField("date", DateType()),
        StructField("open_price", DoubleType()),
        StructField("high_price", DoubleType()),
        StructField("low_price", DoubleType()),
        StructField("close_price", DoubleType()),
        StructField("adj_close_price", DoubleType()),
        StructField("volume", IntegerType())
    ])

    return spark.read \
        .option("header", "true") \
        .schema(stocksSchemaStruct) \
        .csv(data_file)

# Method to update Mysql Table
def write_df_mysql(df):

    jdbcUrl = "jdbc:mysql://localhost:3306/msft_database"
    properties = {"user": "root", "password": "root", "driver": "com.mysql.jdbc.Driver"}
    table = "daily_stocks_data"

    df.write.jdbc(url=jdbcUrl, \
                  table=table, \
                  mode="overwrite", \
                  properties=properties)


# Method to assign conf from SPARK CONF file
def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf
