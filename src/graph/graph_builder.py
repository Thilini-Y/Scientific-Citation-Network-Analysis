from pyspark.sql import DataFrame
from graphframes import GraphFrame
from src.utils.job_naming import set_job_description


# Build a GraphFrame from the vertices and citation edges
class GraphBuilder:
    @staticmethod
    def build(vertices: DataFrame, edges: DataFrame) -> GraphFrame:
        graph = GraphFrame(vertices, edges)
        set_job_description("Build GraphFrame", "Count vertices/edges of the constructed GraphFrame")
        print(
            f"Built graph with "
            f"{graph.vertices.count()} vertices and {graph.edges.count()} edges"
        )
        return graph
