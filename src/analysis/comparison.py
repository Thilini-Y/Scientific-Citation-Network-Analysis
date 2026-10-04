from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from config import config
from src.utils.job_naming import set_job_description


class RankingComparison:
    @staticmethod
    def merge_metrics(
        citation_df: DataFrame, pagerank_df: DataFrame, components_df: DataFrame
    ) -> DataFrame:
        merged = (
            citation_df
            .join(pagerank_df, on="id", how="inner")
            .join(components_df, on="id", how="inner")
        )
        return merged

    # calculate both citation rank and page rank
    @staticmethod
    def add_ranks(metrics_df: DataFrame) -> DataFrame:
        citation_window = Window.orderBy(F.col("citation_count").desc())
        pagerank_window = Window.orderBy(F.col("pagerank_score").desc())

        ranked = (
            metrics_df
            .withColumn("citation_rank", F.row_number().over(citation_window))
            .withColumn("pagerank_rank", F.row_number().over(pagerank_window))
        )
        return ranked

    # out of top 100 from both citation rank and page rank, how many are common to both
    @staticmethod
    def top_n_overlap(ranked_df: DataFrame, n: int = None) -> float:
        n = n or config.TOP_N

        set_job_description("Top-N Overlap", f"Collect top-{n} citation-count and PageRank IDs")
        top_citation_ids = {
            row.id for row in ranked_df.filter(F.col("citation_rank") <= n).select("id").collect()
        }
        top_pagerank_ids = {
            row.id for row in ranked_df.filter(F.col("pagerank_rank") <= n).select("id").collect()
        }

        overlap = len(top_citation_ids & top_pagerank_ids)
        overlap_fraction = overlap / n
        print(f"Top-{n} overlap: {overlap}/{n} ({overlap_fraction:.2%})")
        return overlap_fraction

    # Measure ranking similarity: +1 = identical, 0 = no correlation, -1 = opposite
    @staticmethod
    def spearman_correlation(ranked_df: DataFrame) -> float:
        set_job_description("Spearman Correlation", "Correlate citation_rank vs pagerank_rank")
        corr = ranked_df.stat.corr("citation_rank", "pagerank_rank", "pearson")
        print(f"Spearman correlation: {corr:.4f}")
        return corr


    # finds the papers where citation rank and PageRank rank disagree the most
    @staticmethod
    def divergent_cases(ranked_df: DataFrame, limit: int = 10) -> DataFrame:
        
        divergence_df = ranked_df.withColumn(
            "rank_gap", F.abs(F.col("citation_rank") - F.col("pagerank_rank"))
        )
        top_divergent = divergence_df.orderBy(F.col("rank_gap").desc()).limit(limit)
        return top_divergent
