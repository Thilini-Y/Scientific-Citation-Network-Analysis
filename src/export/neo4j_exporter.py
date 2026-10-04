from pyspark.sql import DataFrame
from config import config
from src.utils.job_naming import set_job_description


class Neo4jExporter:
    @staticmethod
    def write_nodes(metrics_df: DataFrame) -> None:
       
        set_job_description("Write Paper Nodes", "Write :Paper nodes + metrics to Neo4j")
        (
            metrics_df.coalesce(1).write.format("org.neo4j.spark.DataSource")
            .mode("Overwrite")
            .option("url", config.NEO4J_URL)
            .option("authentication.basic.username", config.NEO4J_USERNAME)
            .option("authentication.basic.password", config.NEO4J_PASSWORD)
            .option("database", config.NEO4J_DATABASE)
            .option("labels", f":{config.NODE_LABEL}")
            .option("node.keys", "id")
            .option("schema.optimization.type", "NODE_CONSTRAINTS")
            .save()
        )
        print(f"[Neo4jExporter] Wrote {metrics_df.count()} :{config.NODE_LABEL} nodes to Neo4j")

    @staticmethod
    def write_relationships(edges_df: DataFrame) -> None:
       
        set_job_description("Write CITES Relationships", "Write :CITES relationships to Neo4j")
        (
            edges_df.coalesce(1).write.format("org.neo4j.spark.DataSource")
            .mode("Overwrite")
            .option("url", config.NEO4J_URL)
            .option("authentication.basic.username", config.NEO4J_USERNAME)
            .option("authentication.basic.password", config.NEO4J_PASSWORD)
            .option("database", config.NEO4J_DATABASE)
            .option("relationship", config.RELATIONSHIP_TYPE)
            .option("relationship.save.strategy", "keys")
            .option("relationship.source.labels", config.NODE_LABEL)
            .option("relationship.source.node.keys", "src:id")
            .option("relationship.target.labels", config.NODE_LABEL)
            .option("relationship.target.node.keys", "dst:id")
            .option("batch.size", "2000")
            .save()
        )
        print(f"[Neo4jExporter] Wrote {edges_df.count()} :{config.RELATIONSHIP_TYPE} relationships to Neo4j")
