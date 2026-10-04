from pyspark.sql import DataFrame
from graphframes import GraphFrame
from config import config
from src.utils.job_naming import set_job_description


# calculate PageRank score for every paper
class PageRankAnalysis:
    @staticmethod
    def compute(graph: GraphFrame) -> DataFrame:

        set_job_description(
            "PageRank",
            f"Compute PageRank (alpha={1 - config.PAGERANK_RESET_PROBABILITY}, "
            f"maxIter={config.PAGERANK_MAX_ITER})",
        )
        result = graph.pageRank(
            resetProbability=config.PAGERANK_RESET_PROBABILITY,
            maxIter=config.PAGERANK_MAX_ITER,
        )
        pagerank_df = result.vertices.select(
            "id", result.vertices["pagerank"].alias("pagerank_score")
        )
        print("Computed PageRank scores")
        return pagerank_df
