# Citation Graph Analyser

A PySpark + GraphFrames pipeline that analyses a citation network SNAP `cit-HepPh` dataset, computes influence metrics, compares them, and exports
the results into Neo4j for exploration.

## What it does

The pipeline runs in four phases, plus a presentation step:

1. **Data Preparation**
   - Ingestion: uploads the raw edge list dataset into HDFS, reads it back as a Spark DataFrame.
   - Cleaning: casts IDs to integers, drops malformed/null rows, removes self-citations and
     duplicate edges, then builds the `vertices` (distinct paper IDs) and `edges`
     (`src`, `dst`) tables GraphFrames expects. Cleaned tables are persisted back to HDFS
     as Parquet.

2. **Graph Construction**
   - Builds a `GraphFrame` from the cleaned vertices/edges.

3. **Graph Analysis**
   - **Citation count**: in-degree of each paper (how many times it's cited).
   - **PageRank**: iterative influence score (who cites you matters, not just how many).
   - **Connected components**:  groups papers into weakly connected clusters.

4. **Comparative Evaluation**
   - Merges all three metrics into one table and ranks papers by citation count and
     by PageRank.
   - Computes **Top-N overlap** (how many papers appear in both top-N lists) and
     **Spearman rank correlation** (how similarly the two methods rank the *whole* dataset).
   - Surfaces the most **divergent cases**:  papers ranked very differently by each metric.
   - Persists the final ranked metrics table to HDFS.

5. **Presentation - Neo4j Export**

## Prerequisites

- **Python 3.9+** with a virtual environment
- **Hadoop/HDFS** running locally (`hdfs://localhost:9000` by default — see `config.py`)
- **Neo4j** running locally (`bolt://localhost:7687` by default), with a known username/password
- Alternatively, **Docker** with Compose to run HDFS and Neo4j in containers instead (see below)
- The raw dataset file (e.g. `cit-HepPh.txt` from [SNAP](https://snap.stanford.edu/data/)) placed in `dataset/`

## Setup

```bash
cd graph_analyser
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Running HDFS and Neo4j with Docker (optional)

Instead of installing Hadoop and Neo4j locally, you can run both as containers:

```bash
docker compose up -d
```

This starts a single-node HDFS (`namenode` + `datanode`, Hadoop 3.4.1) and Neo4j 5, matching
the defaults in `config.py`:

| Service | Endpoint | Web UI |
|---|---|---|
| HDFS | `hdfs://localhost:9000` | http://localhost:9870 |
| Neo4j | `bolt://localhost:7687` (user `neo4j`, password `test1234`) | http://localhost:7474 |

The HDFS containers use host networking so Spark on the host can reach the datanode directly,
which means ports 9000, 9870 and 9864–9867 must be free. HDFS settings live in `docker/hadoop.env`.
Data persists in named volumes across restarts.

The ingestion step shells out to `hdfs dfs`, so put the bundled wrapper on your `PATH` before
running the pipeline. It forwards `hdfs` commands into the `namenode` container, where
`dataset/` is mounted:

```bash
export PATH=$PWD/scripts:$PATH
hdfs dfs -ls /   # sanity check
```

Stop with `docker compose down`, or `docker compose down -v` to also wipe HDFS and Neo4j data.

## Running the full pipeline

```bash
spark-submit \
    --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12,org.neo4j:neo4j-connector-apache-spark_2.12:5.3.1_for_spark_3 \
    main.py
```

This runs all four phases plus the Neo4j export in one go. `--packages` lets Spark
resolve the GraphFrames and Neo4j connector jars via Ivy on first run (cached after that).

## Running interactively (notebook)

```bash
source venv/bin/activate
pip install jupyter
jupyter notebook
```

Open `citation_graph_analysis.ipynb` and run cells top to bottom. It mirrors `main.py`
phase by phase so you can inspect intermediate results (raw edges, cleaned edges,
vertices, each metric, the comparison numbers) before committing to the slow steps
(connected components checkpointing, Neo4j export).

**Note:** launch Jupyter from the project root so the `sys.path` setup in the first
cell can find the `src/` and `config/` packages.

## Exploring the results in Neo4j

Open Neo4j Browser at `http://localhost:7474`. Try useful starting queries. Eg:

```cypher
// Complete graph
MATCH p=()-[:CITES]->() RETURN p;
```

```cypher
// A paper's citation neighborhood
MATCH (p:Paper {id: 9803315})-[:CITES]-(neighbor)
RETURN p, neighbor
LIMIT 100
```

```cypher
// Main connected component
MATCH (p:Paper {component_id: 1001})-[r:CITES]->(q:Paper {component_id: 1001})
RETURN p, r, q
LIMIT 500
```

## Key metrics explained

| Metric | What it measures                                              |
|---|---------------------------------------------------------------|
| `citation_count` | Raw in-degree: how many papers cite this one                  |
| `pagerank_score` | Influence, weighted by the importance of the citing papers    |
| `component_id` | Which connected cluster this paper belongs to                 |
| Top-N overlap | How much the two rankings agree on *which* papers matter most |
| Spearman correlation | How similarly the two rankings order the *entire* dataset     |
