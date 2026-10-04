import glob
import gzip
import os
import shutil
import subprocess

from pyspark.sql import SparkSession, DataFrame
from config import config
from src.utils.job_naming import set_job_description


class HDFSLoader:
    def __init__(self, spark: SparkSession):
        self.spark = spark


    def resolve_local_file(self) -> str:
        
        configured_path = config.local_raw_file_path()

        if os.path.exists(configured_path):
            return configured_path

        gz_path = configured_path + ".gz"
        if os.path.exists(gz_path):
            return self._decompress(gz_path)

        txt_candidates = glob.glob(os.path.join(config.LOCAL_DATASET_DIR, "*.txt"))
        if txt_candidates:
            print(f"File discovered: {txt_candidates[0]}")
            return txt_candidates[0]

        gz_candidates = glob.glob(os.path.join(config.LOCAL_DATASET_DIR, "*.txt.gz"))
        if gz_candidates:
            return self._decompress(gz_candidates[0])

        raise FileNotFoundError(
            f"No dataset file found in {config.LOCAL_DATASET_DIR}. "
            f"Expected something like 'cit-HepPh.txt' or 'cit-HepPh.txt.gz'."
        )

    @staticmethod
    def _decompress(gz_path: str) -> str:
        txt_path = gz_path[:-3]
        if not os.path.exists(txt_path):
            print(f"[HDFSLoader] Decompressing {gz_path} -> {txt_path}")
            with gzip.open(gz_path, "rb") as f_in, open(txt_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        return txt_path


   # upload local data files in to hadoop distributed file system
    def upload_to_hdfs(self, local_path: str = None, hdfs_path: str = None) -> None:
    
        local_path = local_path or self.resolve_local_file()
        hdfs_path = hdfs_path or config.HDFS_RAW_PATH
        hdfs_dir = hdfs_path.rsplit("/", 1)[0]

        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True)
        subprocess.run(["hdfs", "dfs", "-put", "-f", local_path, hdfs_path], check=True)
        print(f"Files uploaded into HDFS: {local_path} -> {hdfs_path}")

    # Spark read data from HDFS
    # Remove comments, define nodes and edges
    def read_raw_edges(self, hdfs_path: str = None) -> DataFrame:
        
        hdfs_path = hdfs_path or config.HDFS_RAW_PATH

        raw_df = (
            self.spark.read
            .option("comment", "#")
            .option("sep", "\t")
            .csv(hdfs_path)
            .toDF("from_id", "to_id")
        )
        set_job_description("Read Raw Edges", f"Read raw citation edge list from {hdfs_path}")
        print(f"Read {raw_df.count()} raw rows from {hdfs_path}")
        return raw_df
