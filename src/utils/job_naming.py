from pyspark.sql import SparkSession


def set_job_description(name: str, description: str = None) -> None:
    
    spark = SparkSession.getActiveSession()
    if spark is None:
        return
    spark.sparkContext.setJobGroup(name, description or name)


def clear_job_description() -> None:
    spark = SparkSession.getActiveSession()
    if spark is None:
        return
    spark.sparkContext.setJobGroup("", "")
