import sys
import os

sys.path.append(os.path.dirname(__file__))

from config import config
from src.utils.spark_session import get_spark_session
from src.ingestion.hdfs_loader import HDFSLoader
from src.processing.data_cleaner import DataCleaner
from src.graph.graph_builder import GraphBuilder
from src.analysis.citation_counter import CitationCounter
from src.analysis.pagerank_analysis import PageRankAnalysis
from src.analysis.component_analysis import ComponentAnalysis
from src.analysis.comparison import RankingComparison
from src.export.neo4j_exporter import Neo4jExporter
from src.utils.neo4j_schema import ensure_paper_id_constraint
from src.utils.job_naming import set_job_description


def main():
    spark = get_spark_session()

    loader = HDFSLoader(spark)

    loader.upload_to_hdfs()

    raw_df = loader.read_raw_edges()

    cleaner = DataCleaner(spark)
    cleaned_edges = cleaner.clean_edges(raw_df)
    vertices = cleaner.build_vertices(cleaned_edges)
    edges = cleaner.build_edges(cleaned_edges)
    cleaner.persist_to_hdfs(vertices, edges)

    graph = GraphBuilder.build(vertices, edges)

    citation_df = CitationCounter.compute(graph)
    pagerank_df = PageRankAnalysis.compute(graph)
    components_df = ComponentAnalysis.compute(
        spark, graph, checkpoint_dir=config.HDFS_CHECKPOINT_DIR
    )

    comparison = RankingComparison()
    metrics_df = comparison.merge_metrics(citation_df, pagerank_df, components_df)
    ranked_df = comparison.add_ranks(metrics_df)

    comparison.top_n_overlap(ranked_df)
    comparison.spearman_correlation(ranked_df)
    divergent = comparison.divergent_cases(ranked_df)
    set_job_description("Divergent Cases", "Show top-10 citation vs PageRank rank mismatches")
    divergent.show(10, truncate=False)

    # Persist final metrics table to HDFS, for reproducibility
    set_job_description("Persist Metrics", f"Write final metrics table to {config.HDFS_METRICS_PATH}")
    ranked_df.write.mode("overwrite").parquet(config.HDFS_METRICS_PATH)

    ensure_paper_id_constraint()

    Neo4jExporter.write_nodes(ranked_df)
    Neo4jExporter.write_relationships(edges)

    print("[main] Pipeline finished successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
