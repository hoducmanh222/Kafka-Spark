# Assignment coverage

| Assignment question or deliverable | Where it is addressed | Verification or evidence |
| --- | --- | --- |
| Part A 1 DStreams versus Structured Streaming | REPORT sections 3 and 4 | Comparison and processing model analysis |
| Part A 1 Continuous ETL importance | REPORT section 4 | Schema, state, recovery and sink analysis |
| Part A 1 WAL checkpointing exactly once | REPORT sections 4 and 9 | Conditional guarantees and failure sequence |
| Part A 2 Real-time recommendation engines | REPORT section 5 | Continuous features and batch-model comparison |
| Part A 2 Clickstream live intent | retail/transforms.py and REPORT section 8 | Spark scoring and suppression tests |
| Part A 3 Kafka concepts and architecture | REPORT section 6 | Brokers, topics, partitions, producers, consumers, groups, KRaft and ZooKeeper |
| Part A 3 Kafka pros cons and connector | REPORT sections 6 and 7 | Version-matched connector and offset semantics |
| Kafka versus Flume matrix | REPORT section 6 | Explicit comparison matrix |
| Cited ETL optimization paper | REPORT section 7 | Armbrust et al 2015 summary and critical application |
| Business scenario | REPORT sections 1 and 5 | Session tracker and in-session cart personalization |
| Task 1 producer.py | producer.py and retail/domain.py | Streaming CSV reader, typed JSON, pacing, acknowledgements and tests |
| Task 2 Kafka at least two partitions | docker-compose.yml kafka and topic-init | Topic describe and acceptance assertions |
| Task 3 spark.readStream and from_json | streaming_etl.py and retail/transforms.py | Explicit schema, malformed audit and type checks |
| Task 3 epoch milliseconds to TimestampType | retail/transforms.py parse_events | timestamp_millis and schema test |
| Task 3 10-minute windows and 5-minute watermark | retail/transforms.py aggregate_intent | One-minute slide, watermark acceptance test |
| Task 3 weights 1 5 10 and rolling aggregates | retail/transforms.py aggregate_intent | Score 6 and score 16 checks |
| Task 3 high intent and dynamic recommendations | retail/transforms.py recommendations | Cart eligibility, purchase suppression, live popularity, unseen candidates |
| Task 3 structured micro-batch logging | retail/sinks.py ProgressLogger and EpochSink | Batch ID, raw record count, duration and state metrics |
| Task 3 live console sink | streaming_etl.py --console and EpochSink | Native console query and recommendation tables |
| Task 3 partitioned Parquet and checkpointLocation | streaming_etl.py and retail/sinks.py | Audit date partitions, intent date partitions, checkpoint tests |
| Single-command Kafka and Spark setup | docker-compose.yml and Dockerfile | README quick start |
| REPORT.md and setup documentation | REPORT.md and README.md | Full report, references and manual |
| 10 to 15 minute live demonstration | DEMO.md and scripts/live_demo.py | 13-minute schedule and reproducible commands |
| Presentation | Accompanying PowerPoint presentation | Theory, implementation and demo notes |
| Modular code | retail modules, scripts and tests | Producer, transforms, sinks and serving readers separated |
| Git repository link | https://github.com/hoducmanh222/Kafka-Spark and README | Prepared for the supplied remote; publication requires GitHub authentication |

The source document's high-intent inequality is incomplete. This solution explicitly sets `score >= 5`, requires a cart and excludes transactions for the same visitor and item within the serving window. The source does not specify a slide interval. This solution uses one minute. These choices appear consistently in code, tests, report and slides.
