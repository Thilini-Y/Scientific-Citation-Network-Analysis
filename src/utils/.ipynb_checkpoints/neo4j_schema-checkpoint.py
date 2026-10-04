from neo4j import GraphDatabase
from config import config


def ensure_paper_id_constraint() -> None:
  
    driver = GraphDatabase.driver(
        config.NEO4J_URL,
        auth=(config.NEO4J_USERNAME, config.NEO4J_PASSWORD),
    )
    try:
        with driver.session(database=config.NEO4J_DATABASE) as session:
            session.run(
                f"""
                CREATE CONSTRAINT paper_id_unique IF NOT EXISTS
                FOR (p:{config.NODE_LABEL}) REQUIRE p.id IS UNIQUE
                """
            )
        print(f"[Neo4jSchema] Ensured uniqueness constraint on :{config.NODE_LABEL}(id)")
    finally:
        driver.close()
