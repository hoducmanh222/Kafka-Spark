# Validation record

Prepared 10 October 2026.

## Executed checks

- `python -m unittest tests.test_domain tests.test_spark tests.test_verifier -v`: 21 tests passed, process exit code 0, total test duration 23.197 seconds after the final full-data revision (11 producer/source, 5 PySpark, 5 verifier tests).
- Environment for those tests: Windows, bundled Python 3.12, PySpark 3.5.7 and portable Microsoft OpenJDK 17.0.20.1. The supported Docker image uses Python 3.11 and Java 17 instead.
- Genuine Retail Rocket sample: 5,000 chronologically sorted records, 4,879 views, 98 carts and 23 transactions. Selected from the public events.csv archive.
- Full dataset preparation: the delivered `scripts/prepare_data.py` successfully validated and stably sorted all 2,756,101 source events using SQLite. The resulting complete CSV is included in the downloadable submission bundle.
- Full CSV category and chronological checks passed: 2,664,312 views, 69,332 carts, 22,457 transactions, no timestamp-order regression. The default data guard accepted the full file.
- The genuine demo selector ran against the complete CSV and selected visitor 732139 / item 61273 with actual transaction 3099 and nearby genuine candidate views. The live Kafka replay itself still requires Docker execution.
- Revised producer tests verify bounded asynchronous acknowledgement completion and propagation of broker acknowledgement failures. The default setup rejects an incomplete sample as its full input.
- Spark checks used native SQL input DataFrames with the delivered JSON parser, window aggregation, snapshot merge and recommendation functions. They verified schema handling, scores, purchase suppression, exclusion of seen items, state expiry and no overlap-induced popularity multiplication.

The Windows test run emitted Hadoop winutils and socket cleanup warnings. The test process returned 0 and all assertions passed. Those warnings do not validate native Windows file persistence, which is outside the supported execution route.

- Completion-verifier tests passed with small explicitly labeled test fixtures: valid committed counts, duplicate rejection, invalid-row rejection, per-partition intent progress and exclusion of orphan Parquet files with compacted metadata. These tests are not a claimed full Kafka run.

## Checks requiring the Linux Docker stack

Docker and WSL were not installed on the preparation machine. The following checks are supplied but are not claimed as executed here:

- Full Kafka to Spark to Parquet and checkpoint acceptance: `docker compose --profile tools run --rm acceptance`.
- Complete 2,756,101-row downstream replay verification: `docker compose --profile tools run --rm verify-full`. The workflow now starts the full producer and requires this verification before controlled acceptance or the genuine classroom trace.
- Atomic sink failure after publication and batch replay: `python -m unittest tests.test_sink_integration.SinkTests.test_atomic_publish_then_retry -v` inside the project image.
- Driver restart and no duplicate source-coordinate persistence: follow the ordered recovery test commands in README.md.
- Container build and initial image/Maven downloads.

The GitHub Actions workflow runs the Linux test sequence and saves logs and metrics. Its status must be checked after the repository is published. No fabricated CI badge, throughput, p95 latency or recommendation-accuracy result is included.
