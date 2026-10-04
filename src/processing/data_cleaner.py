from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from config import config
from src.utils.job_naming import set_job_description


class DataCleaner:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    # clean data
    # convert text nodes into int, drop self linked nodes and drop duplicates
    def clean_edges(self, raw_df: DataFrame) -> DataFrame:
        cleaned = (
            raw_df
            .withColumn("from_id", F.col("from_id").cast("int"))
            .withColumn("to_id", F.col("to_id").cast("int"))
            .dropna(subset=["from_id", "to_id"])
            .filter(F.col("from_id") != F.col("to_id"))
            .dropDuplicates(["from_id", "to_id"])
        )
        set_job_description("Clean Edges", "Remove duplicates, self-citations, malformed rows")
        print(f"Cleaned edge count: {cleaned.count()}")
        return cleaned

    # get nodes (vertices)
    def build_vertices(self, cleaned_edges: DataFrame) -> DataFrame:
        vertices = (
            cleaned_edges.select(F.col("from_id").alias("id"))
            .union(cleaned_edges.select(F.col("to_id").alias("id")))
            .distinct()
        )
        set_job_description("Build Vertices", "Extract distinct vertex IDs from edges")
        print(f"Vertex count: {vertices.count()}")
        return vertices

    # get edges
    def build_edges(self, cleaned_edges: DataFrame) -> DataFrame:
        edges = cleaned_edges.select(
            F.col("from_id").alias("src"),
            F.col("to_id").alias("dst"),
        )
        return edges

    # save the processed vertices and edges in to hadoop
    def persist_to_hdfs(self, vertices: DataFrame, edges: DataFrame) -> None:
        set_job_description("Persist Vertices", f"Write vertex table to {config.HDFS_VERTICES_PATH}")
        vertices.write.mode("overwrite").parquet(config.HDFS_VERTICES_PATH)

        set_job_description("Persist Edges", f"Write edge table to {config.HDFS_EDGES_PATH}")
        edges.write.mode("overwrite").parquet(config.HDFS_EDGES_PATH)
        print(
            f" Wrote vertices -> {config.HDFS_VERTICES_PATH}, "
            f"edges -> {config.HDFS_EDGES_PATH}"
        )
