# Retail Rocket: Kafka and PySpark Structured Streaming

The complete assignment uses **Python and PySpark**, with the **full 2,756,101-event Retail Rocket events.csv** as its default input. Read [REPORT.md](REPORT.md): detailed Part A theory appears first, followed by Part B implementation and the full-data demonstration. [REQUIREMENTS.md](REQUIREMENTS.md) maps every question to its implementation. The report PDF and presentation are under `docs/`.

## Single-command full-data setup

Use Docker Engine or Docker Desktop with Linux containers, Docker Compose v2.20+, an x86-64 host or amd64 emulation, approximately 12 GB available memory and 15 GB free disk. A 16 GB RAM host is preferable. Internet is needed for the initial image and connector build. Windows users need Docker Desktop's WSL2 backend and virtualization. Host Python and Java are not needed.

From this folder:

```powershell
docker compose up --build -d
docker compose ps -a
docker compose logs -f data-init producer spark
```

This starts Kafka 3.9.1 in KRaft mode, creates `retail-rocket-events` with **two partitions**, starts **PySpark 3.5.7** and produces **every row of the complete CSV**, without a sample limit. The finite producer exits successfully only after all expected acknowledgements. Kafka and Spark stay running.

The downloadable bundle includes both the original source archive and the complete sorted CSV. A Git clone omits these large files: `data-init` automatically downloads and sorts the original archive if the complete CSV is missing. It refuses to accept the 5,000-row development fixture as the default input. The complete source contains 2,664,312 views, 69,332 carts and 22,457 transactions. See `data/ATTRIBUTION.md` for the separate dataset license.

All business code is `.py`. The required connector coordinate `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7` has a JVM binary compatibility suffix; it does **not** make this a Scala application. Do not remove that suffix or change the connector version independently of PySpark.

## Verify the entire dataset end to end

Build and run this before class. Full processing duration depends on the actual machine; a 13-minute presentation is not a promised full-replay runtime.

```powershell
docker compose --profile tools run --rm verify-full
```

The verifier waits up to two hours for the producer receipt, all 2,756,101 Parquet rows and the intent query's source progress. It checks matching event-category counts, valid records, both Kafka partitions and unique source coordinates using disk-backed SQLite. Only success writes `runtime/full_replay_verified.json`. The verifier honors Spark's `_spark_metadata` committed-file list and excludes abandoned uncommitted files. Display that receipt as evidence of full-source completion. A producer exit alone does not prove Spark caught up. To set a different waiting limit:

```powershell
docker compose run --rm --no-deps spark python scripts/verify_full_replay.py --timeout 10800
```

Do not run acceptance injections or a second CSV producer concurrently with this first full replay. They would interleave records in the producer's offset ranges. Run the following sections after full verification.

## Observe Kafka, PySpark and persisted output

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic retail-rocket-events
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic retail-rocket-events --from-beginning --max-messages 5
docker compose run --rm --no-deps spark python scripts/inspect_output.py --parquet
```

Spark UI: http://localhost:4040. Host Kafka: `localhost:29092`. Container Kafka: `kafka:9092`.

```text
runtime/producer_complete.json             All input acknowledgements and partition offsets
runtime/full_replay_verified.json          Successful full downstream verification
runtime/curated/is_valid=.../ingest_date=.../*.parquet
runtime/checkpoints/curated/               Audit offsets and file-sink commits
runtime/checkpoints/intent/                Intent offsets, commits and window state
runtime/checkpoints/console/               Separate console query progress
runtime/intent/epochs/<batch_id>/state/event_date=.../*.parquet
runtime/intent/epochs/<batch_id>/recommendations/event_date=.../*.parquet
runtime/intent/epochs/<batch_id>/recommendations.jsonl
runtime/intent/epochs/<batch_id>/manifest.json
runtime/metrics/progress.jsonl
runtime/application.log
```

Read the highest completed epoch as the current replacement snapshot. Earlier epochs are history; never union and sum all their totals. An empty current recommendation snapshot retracts older output. Empty recommendations have schema-bearing Parquet even when there is no date partition to write.

## Live demonstration from the complete real dataset

After `verify-full` succeeds:

```powershell
docker compose --profile tools run --rm demo
```

This selects actual view, cart, transaction and nearby candidate-view rows from the complete `data/events.csv`. It preserves their IDs, actions, transaction ID and relative event-time gaps. It applies one disclosed timestamp shift and starts beyond earlier active state so the visible transition is isolated. It waits for cart recommendations, then sends the real purchase and checks retraction. Output is labeled `REAL DATASET TRACE`, with original timestamps and new Kafka coordinates. It refuses to run without verified full-source completion.

The selected trace is a controlled **second replay of genuine source rows** for visibility during class. It does not substitute for the complete ingestion. Synthetic malformed and very late payloads are used only in the separate acceptance test. [DEMO.md](DEMO.md) gives the 13-minute schedule and preparation procedure.

## Producer controls and performance

The default full replay uses `--interval 0`, bounded asynchronous futures, Kafka batching, idempotence, `acks=all` and a maximum of five in-flight requests. Every future must acknowledge before a completion receipt is published. `--max-pending` defaults to 2000. Input is read incrementally, not loaded into a Python list.

For a deliberately slower **full** replay in a fresh experiment:

```powershell
docker compose run --rm --no-deps producer python producer.py --csv data/events.csv --interval 0.001 --replay-clock --expected-rows 2756101 --completion-file runtime/producer_complete.json
```

The 0.001-second pacing alone takes at least about 46 minutes for the full file; actual duration adds acknowledgement and scheduling costs. The zero-interval default avoids that intentional delay, but makes no unmeasured throughput claim. `--replay-clock` uses a constant shift; event-time gaps remain those of the original source. Omit it to retain the 2015 timestamps exactly.

The Spark trigger is five seconds and `maxOffsetsPerTrigger` is 25000. One event contributes to ten overlapping windows, so increasing offsets also increases state work. Driver memory is 4 GB. When quickly replaying months of history, the current recommendation snapshot represents the newest event-time frontier in each batch; the audit still retains all source rows. Use the paced genuine trace to see the individual cart transition.

The small `data/events_sample.csv` remains an optional development fixture only. It is not used by the default Compose run or the classroom procedure. Rerunning any CSV on the same topic creates new Kafka offsets; producer idempotence does not identify that deliberate second ingestion as a duplicate business event.

## Tests and recovery

```powershell
docker compose run --rm --no-deps spark python -m unittest tests.test_domain tests.test_spark tests.test_verifier -v
docker compose run --rm --no-deps spark python -m unittest tests.test_sink_integration.SinkTests.test_atomic_publish_then_retry -v
```

After full replay verification, run controlled Kafka acceptance and restart checks:

```powershell
docker compose --profile tools run --rm acceptance
docker compose restart spark
docker compose up -d --wait spark
docker compose --profile tools run --rm demo
docker compose run --rm --no-deps spark python scripts/check_recovery.py
```

Acceptance uses explicit test input for two partitions, score 6, two-minute-late input, score 16 with purchase suppression, expiry, invalid audit, Parquet schema, metrics and checkpoints. The restart check verifies a retained query ID, changed run ID and unique persisted Kafka coordinates. The GitHub Actions workflow also performs the complete real dataset replay before these checks and saves its receipts and logs. See `VALIDATION.md` for what was actually executed locally.

Keep Kafka data, all checkpoints and epoch outputs together across recovery:

```powershell
docker compose stop
docker compose start kafka spark
```

For a deliberate clean experiment, preserve the runtime first:

```powershell
docker compose down -v
Move-Item -LiteralPath ./runtime -Destination ("./runtime-archive-" + (Get-Date -Format yyyyMMdd-HHmmss))
docker compose up --build -d
```

`down -v` deletes this project's Kafka volume intentionally. The move preserves old audit and checkpoints. On Bash use `mv runtime "runtime-archive-$(date +%Y%m%d-%H%M%S)"`. Do not delete only checkpoints while retaining old epochs, or delete Kafka while keeping its old checkpoint. Do not run two intent writers on the same output directory.

## Troubleshooting and submission

- Missing complete CSV in a clone: inspect `docker compose logs data-init`. If Kaggle changes its public endpoint or requires login, obtain the original events.csv from the source page and run `scripts/prepare_data.py` with a writable data mount.
- Broker failure: inspect Kafka logs, disk space and port 29092 conflicts.
- Maven resolution failure: retain exact PySpark/connector versions and allow Maven Central during the build.
- Empty recommendations: inspect the current cart, transaction counts, aligned window and unseen candidates. The current view is allowed to be empty.
- Full verification timeout: inspect producer progress and query metrics. Increase the verifier timeout or tune resources; do not claim completion from a timeout.
- Native Windows Hadoop or worker errors: use the supplied Linux Docker execution route.
- A batch takes longer than five seconds: this is possible; lower source load or offsets and inspect state/shuffle/sink work.

Submission repository: https://github.com/hoducmanh222/Kafka-Spark. Publishing from an extracted bundle:

```powershell
git init
git add .
git commit -m "Implement full-data Retail Rocket Kafka and PySpark assignment"
git branch -M main
git remote add origin https://github.com/hoducmanh222/Kafka-Spark.git
git push -u origin main
```

Large source files are excluded from Git, but the single-command data preparation restores them. Include team names in the submission form. Retain actual full-replay and recovery evidence before claiming a successful executed classroom run.
