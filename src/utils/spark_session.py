from pyspark.sql import SparkSession
from config import config


def get_spark_session() -> SparkSession:

    spark = (
        SparkSession.builder
        .appName(config.SPARK_APP_NAME)
        .master(config.SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.executor.cores", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
