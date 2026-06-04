# spark-learn

Docker-based Apache Spark learning environment. 11-step hands-on labs from basics to internal architecture + real-world project.

## Environment

| Service | URL | Description |
|--------|-----|------|
| JupyterLab | http://localhost:8888 | Token: see `.env` |
| Spark Master UI | http://localhost:8080 | 2 Workers connected |
| Spark App UI | http://localhost:4040 | Monitor running apps |
| MinIO Console | http://localhost:9001 | S3-compatible storage |

## Getting Started

```bash
cp .env.example .env
docker compose up -d
# Open http://localhost:8888 → run notebooks in the work/ folder
```

## Curriculum

### A) Fundamentals (Steps 1~7)

| Step | Notebook | Topic |
|------|--------|------|
| 1 | `01_rdd_dataframe_basics` | RDD, DataFrame, Lazy Evaluation, Caching |
| 2 | `02_spark_sql_catalyst` | Spark SQL, Catalyst Optimizer, Execution Plans |
| 3 | `03_shuffle_partitioning` | Shuffle, repartition vs coalesce, Data Skew |
| 4 | `04_join_strategies` | BHJ, SMJ, SHJ, Skew Join, Join Order Optimization |
| 5 | `05_memory_tuning` | Memory Structure, Spill, GC, OOM Debugging |
| 6 | `06_structured_streaming` | Watermark, Window, Stream-Static Join |
| 7 | `07_data_pipeline` | Bronze/Silver/Gold, Data Quality, SCD Type 2 |

### B) Advanced (Steps 8~11)

| Step | Notebook | Topic |
|------|--------|------|
| 8 | `08_pandas_udf_arrow` | Apache Arrow, 4 Types of Pandas UDF, applyInPandas |
| 9 | `09_spark_internals` | Job→Stage→Task, DAG Scheduler, BlockManager |
| 10 | `10_tungsten_codegen` | UnsafeRow, Whole-Stage Code Generation |
| 11 | `11_performance_profiling` | 5 Bottleneck Types, AQE Deep Dive, Profiling Checklist |

### C) Real-World Project

| Project | Path | Description |
|----------|------|------|
| Large-Scale Log Analysis | `projects/log_analysis/` | 5M clickstream events → Funnel/Session/Anomaly Detection |

## Project Structure

```
spark-learn/
├── docker-compose.yml
├── .env / .env.example
├── notebooks/
│   ├── 01~11_*.ipynb              # Learning notebooks
│   └── projects/
│       └── log_analysis/          # Real-world project
│           ├── 01_data_generation.ipynb
│           ├── 02_silver_sessionization.ipynb
│           └── 03_gold_analytics.ipynb
└── data/                          # Practice data (gitignored)
```

## Production Spark Cluster Sizing Guide

A reference for the scale you'll encounter in real-world production after completing this course.

### Apache Spark Official Hardware Recommendations

Based on the [Spark Hardware Provisioning docs](https://spark.apache.org/docs/latest/hardware-provisioning.html):

| Item | Recommended |
|------|--------|
| CPU | Min 8~16 cores per node, scalable to dozens of cores |
| Memory | 8GB ~ hundreds of GB per node, allocate 75% to Spark |
| JVM Heap | Do not exceed 200GB in a single JVM → split into multiple Executors per node |
| Local Disk | 4~8 SSDs per node (no RAID, separate mounts) |
| Network | 10Gbps or higher recommended |

### Executor Sizing Principles

Based on [Spark Tuning docs](https://spark.apache.org/docs/latest/tuning.html) and [practical guide](https://spoddutur.github.io/spark-notes/distribution_of_executors_cores_and_memory_for_spark_application.html):

```
Cores per Executor: 3~5 (HDFS optimal throughput is ~5 tasks/executor)
Memory per Executor: 8~20GB (GC delays worsen beyond 32GB)
Memory Overhead: max(384MB, executor.memory × 7%)

❌ Fat Executor (15+ cores, 60GB+ memory) → Excessive GC, HDFS bottleneck
❌ Tiny Executor (1 core, 2GB memory) → JVM overhead, excessive broadcast copies
✅ Balanced (4~5 cores, 8~20GB memory) → Balance between parallelism and efficiency
```

### Cluster Configuration by Data Scale (Practical Reference)

| | TB-scale (1~10TB) | Tens of TB (10~100TB) | PB-scale (100TB+) |
|---|---|---|---|
| **Node Count** | 10~50 | 50~200 | 200~1,000+ |
| **Node Spec** | 16 cores, 64GB | 32 cores, 128GB | 32~64 cores, 128~256GB |
| **Per Executor** | 4 cores, 8~16GB | 5 cores, 16~20GB | 5 cores, 16~20GB |

> According to the official Spark FAQ, the largest known Spark cluster has **8,000 nodes**. Spark sorted **100TB of data 3x faster than Hadoop MapReduce using 1/10 the machines**, and has been used for **1PB sorting** as well. — [Apache Spark FAQ](https://spark.apache.org/faq.html)

### Real-World Enterprise Use Cases

| Company | Scale | Source |
|------|------|------|
| **Netflix** | 3,000 EC2 nodes, PB-scale data warehouse (40PB reads/3PB writes), Spark on YARN | [Netflix: Spark at Petabyte Scale](https://www.slideshare.net/piaozhexiu/netflix-integrating-spark-at-petabyte-scale-53391704) |
| **Uber** | 100+ PB data platform, 100K+ jobs/day, 10,000+ vCPUs | [Uber Big Data Evolution (InfoQ)](https://www.infoq.com/news/2018/11/uber-big-data-evolution/) |
| **Alibaba** | Thousands of nodes, PB-scale graph mining | [Apache Spark FAQ](https://spark.apache.org/faq.html) |

### Cost Optimization Strategies

Based on general cloud Spark optimization practices and [AWS EMR optimization guide](https://aws.amazon.com/ec2/spot/use-case/emr/):

| Strategy | Savings | Description |
|------|----------|------|
| **Spot/Preemptible Instances** | Up to 90% | Spark Tasks auto-retry on failure, making them Spot-friendly. Keep Driver on On-Demand |
| **Auto Scaling** | 30~50% | Auto-shrink workers when idle, scale up at peak |
| **Job Clusters** | Savings vs always-on clusters | Dedicated cluster per batch job → auto-terminate after run |
| **Delta Lake / Iceberg** | I/O reduction | Optimize reads with Data Skipping and Z-ordering |
| **Partitioning + Pushdown** | 50~90% I/O reduction per query | Read only necessary partitions/columns |
| **AQE Enabled** | 10~30% | Runtime partition merging, automatic Skew handling |

> Spark Executors auto-resubmit failed tasks, making them resilient to Spot instance reclamation (2-min warning, <5% reclamation rate). — [AWS EMR Spot Instances](https://aws.amazon.com/ec2/spot/use-case/emr/)

### Key Takeaways

```
As scale grows:
  Code optimization < Partitioning design < Infrastructure design  (in terms of cost impact)

Most important things learned in this repo for PB-scale workloads:
  1. Minimize Shuffle (Steps 3, 4)
  2. Partitioning strategy (Steps 3, 7)
  3. Handling Data Skew (Steps 3, 4, 11)
  4. Memory/Executor sizing (Step 5)
  5. Habit of checking execution plans (Steps 2, 11)
```

## Next Steps

- [spark-streaming-pipeline](../spark-streaming-pipeline) — Kafka → Spark Streaming → MinIO
- [spark-cdc-pipeline](../spark-cdc-pipeline) — PostgreSQL → Debezium → Kafka → Spark
