# Validation record

Prepared 10 October 2026.

## Executed checks

- `python -m unittest tests.test_domain tests.test_spark -v`: 13 tests passed, process exit code 0, total test duration 22.628 seconds.
- Environment for those tests: Windows, bundled Python 3.12, PySpark 3.5.7 and portable Microsoft OpenJDK 17.0.20.1. The supported Docker image uses Python 3.11 and Java 17 instead.
- Genuine Retail Rocket sample: 5,000 chronologically sorted records, 4,879 views, 98 carts and 23 transactions. Selected from the public events.csv archive.
- Spark checks used native SQL input DataFrames with the delivered JSON parser, window aggregation, snapshot merge and recommendation functions. They verified schema handling, scores, purchase suppression, exclusion of seen items, state expiry and no overlap-induced popularity multiplication.

The Windows test run emitted Hadoop winutils and socket cleanup warnings. The test process returned 0 and all assertions passed. Those warnings do not validate native Windows file persistence, which is outside the supported execution route.

## Checks requiring the Linux Docker stack

Docker and WSL were not installed on the preparation machine. The following checks are supplied but are not claimed as executed here:

- Full Kafka to Spark to Parquet and checkpoint acceptance: `docker compose --profile tools run --rm acceptance`.
- Atomic sink failure after publication and batch replay: `python -m unittest tests.test_sink_integration.SinkTests.test_atomic_publish_then_retry -v` inside the project image.
- Driver restart and no duplicate source-coordinate persistence: follow the ordered recovery test commands in README.md.
- Container build and initial image/Maven downloads.

The GitHub Actions workflow runs the Linux test sequence and saves logs and metrics. Its status must be checked after the repository is published. No fabricated CI badge, throughput, p95 latency or recommendation-accuracy result is included.
