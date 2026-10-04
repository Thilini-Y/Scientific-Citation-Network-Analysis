from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from graphframes import GraphFrame


# counts the number of incoming citations for every paper, including papers with zero citations
class CitationCounter:
    @staticmethod
    def compute(graph: GraphFrame) -> DataFrame:
        in_degrees = graph.inDegrees.withColumnRenamed("inDegree", "citation_count")
       
        result = (
            graph.vertices
            .join(in_degrees, on="id", how="left")
            .fillna(0, subset=["citation_count"])
        )
        print("Computed citation counts")
        return result
