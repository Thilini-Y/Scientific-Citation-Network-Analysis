import sys
import os

sys.path.append(os.path.dirname(__file__))

from config import config
from src.utils.spark_session import get_spark_session
from src.analysis.comparison import RankingComparison
from src.utils.job_naming import set_job_description


def main():
    spark = get_spark_session()

    set_job_description("Read Metrics", f"Read persisted metrics from {config.HDFS_METRICS_PATH}")
    print(f"[analyze_results] Reading metrics from {config.HDFS_METRICS_PATH}")
    ranked_df = spark.read.parquet(config.HDFS_METRICS_PATH)
    ranked_df.cache()

    set_job_description("Count Papers", "Count total papers in metrics table")
    total_papers = ranked_df.count()

    set_job_description("Count Components", "Count distinct component_id values")
    num_components = ranked_df.select("component_id").distinct().count()

    print("=" * 60)
    print("PHASE 4 - COMPARATIVE EVALUATION RESULTS")
    print("=" * 60)
    print(f"Total papers (nodes):        {total_papers}")
    print(f"Connected components found:  {num_components}")

    overlap = RankingComparison.top_n_overlap(ranked_df)
    correlation = RankingComparison.spearman_correlation(ranked_df)

    print(f"Top-{config.TOP_N} overlap (citation count vs PageRank): {overlap:.2%}")
    print(f"Spearman rank correlation:                      {correlation:.4f}")

    print("-" * 60)
    print("Top 10 most divergent papers (citation_rank vs pagerank_rank):")
    divergent = RankingComparison.divergent_cases(ranked_df, limit=10)
    set_job_description("Divergent Cases", "Show top-10 citation vs PageRank rank mismatches")
    divergent.select(
        "id", "citation_count", "citation_rank", "pagerank_score", "pagerank_rank", "rank_gap"
    ).show(10, truncate=False)

    print("-" * 60)
    print("Largest connected components (top 10 by size):")
    set_job_description("Component Sizes", "Group and count papers per component_id")
    (
        ranked_df.groupBy("component_id")
        .count()
        .orderBy("count", ascending=False)
        .show(10, truncate=False)
    )

    print("=" * 60)
    print("[analyze_results] Done.")
    spark.stop()


if __name__ == "__main__":
    main()
