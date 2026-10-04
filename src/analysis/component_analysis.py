from pyspark.sql import DataFrame, SparkSession
from graphframes import GraphFrame
from src.utils.job_naming import set_job_description


# finds the connected groups of papers
class ComponentAnalysis:
    @staticmethod
    def compute(spark: SparkSession, graph: GraphFrame, checkpoint_dir: str) -> DataFrame:
        spark.sparkContext.setCheckpointDir(checkpoint_dir)

        set_job_description("Connected Components", "Find weakly connected subgraphs")
        result = graph.connectedComponents()
        components_df = result.select(
            "id", result["component"].alias("component_id")
        )

        set_job_description("Connected Components", "Count distinct components")
        num_components = components_df.select("component_id").distinct().count()
        print(f"Found {num_components} connected components")
        return components_df
