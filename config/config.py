import os

# Project paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LOCAL_DATASET_DIR = os.path.join(PROJECT_ROOT, "dataset")

LOCAL_RAW_FILENAME = "cit-HepPh.txt"

# HDFS paths
HDFS_NAMENODE = "hdfs://localhost:9000"

HDFS_RAW_DIR = f"{HDFS_NAMENODE}/data/citation/raw"
HDFS_RAW_PATH = f"{HDFS_RAW_DIR}/cit-HepPh.txt"

HDFS_PROCESSED_DIR = f"{HDFS_NAMENODE}/data/citation/processed"
HDFS_VERTICES_PATH = f"{HDFS_PROCESSED_DIR}/vertices.parquet"
HDFS_EDGES_PATH = f"{HDFS_PROCESSED_DIR}/edges.parquet"
HDFS_METRICS_PATH = f"{HDFS_PROCESSED_DIR}/metrics.parquet"

HDFS_CHECKPOINT_DIR = f"{HDFS_NAMENODE}/tmp/checkpoints"

# Spark settings
SPARK_APP_NAME = "CitationGraphAnalyser"
SPARK_MASTER = "local[*]"

# PageRank settings
PAGERANK_RESET_PROBABILITY = 0.15  # equivalent to damping factor alpha = 0.85
PAGERANK_MAX_ITER = 20

# Neo4j connection settings
NEO4J_URL = "bolt://localhost:7687"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "test1234"
NEO4J_DATABASE = "neo4j"

NODE_LABEL = "Paper"
RELATIONSHIP_TYPE = "CITES"

TOP_N = 100


def local_raw_file_path() -> str:
    return os.path.join(LOCAL_DATASET_DIR, LOCAL_RAW_FILENAME)
