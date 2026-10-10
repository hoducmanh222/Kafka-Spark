# Retail Rocket session tracker and personalization engine

This repository implements the supplied Kafka and Spark Streaming assignment. Read [REPORT.md](REPORT.md) for the full technical report, [DEMO.md](DEMO.md) for the 13-minute demonstration, and [REQUIREMENTS.md](REQUIREMENTS.md) for question-by-question coverage. The presentation and PDF report accompany this repository in the submission bundle.

## Quick start

Prerequisites: Docker Engine or Docker Desktop running Linux containers, Docker Compose v2.20 or newer, an x86-64 host (ARM hosts require amd64 emulation), Internet access for the first build, and approximately 8 GB available RAM plus 10 GB free disk. On Windows, install Docker Desktop with its WSL2 backend and enable virtualization. The project does not require a host Java or Python installation when using Docker.

From this repository directory:

```powershell
docker compose up --build -d
docker compose ps -a
docker compose logs -f producer spark
```

This single command starts a Kafka 3.9.1 KRaft broker, creates `retail-rocket-events` with two partitions, starts PySpark 3.5.7 in `local[2]`, and streams the included 5,000 genuine Retail Rocket records. The finite producer exits successfully after the sample, which takes at least 12.5 minutes at 0.15 seconds per event. A finite historical replay is intentional. Spark remains active waiting for more events. First build time depends on image and Maven download speed.

The version pins target a consistent classroom environment. They are not claims that these are the newest releases. Do not change the Spark version without changing the connector version and checking Scala compatibility.

## Observable output

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic retail-rocket-events
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic retail-rocket-events --from-beginning --max-messages 5
docker compose run --rm --no-deps spark python scripts/inspect_output.py --parquet
```

Open the Spark UI at http://localhost:4040. Kafka is reachable by host clients at `localhost:29092`; containers use `kafka:9092`. `CURRENT RECOMMENDATIONS` tables show the current serving snapshot. An empty table means no eligible visitor has an unseen candidate at that event time.

Files written under `runtime/`:

```text
curated/is_valid=true/ingest_date=.../*.parquet
curated/is_valid=false/ingest_date=.../*.parquet
checkpoints/curated/           Kafka offsets and file-sink commits
checkpoints/intent/            Kafka offsets, commits and window state
checkpoints/console/           Independent console progress
intent/epochs/<batch_id>/state/event_date=.../*.parquet
intent/epochs/<batch_id>/recommendations/event_date=.../*.parquet
intent/epochs/<batch_id>/recommendations.jsonl
intent/epochs/<batch_id>/manifest.json
metrics/progress.jsonl
application.log
```

Read the highest completed epoch for current state. Earlier epochs are history. Never union every state epoch and sum its totals. Empty recommendation output is also a valid replacement snapshot and removes earlier recommendations.

## Deterministic cart and purchase demonstration

Stop the sample producer so that its simulated event clock does not advance during the demonstration:

```powershell
docker compose stop producer
docker compose --profile tools run --rm demo
```

The mock demo creates live candidate items, sends a view and a cart, waits until recommendations are visible, then sends a transaction and verifies retraction. It chooses timestamps after the current stream event maximum. Mock events are explicitly labeled. The genuine sample remains separate.

## Full Retail Rocket dataset

The archive download and disk-backed sort use only Python's standard library. Run inside the image to avoid installing host packages. Override the read-only data mount with the writable mount shown below:

```powershell
docker compose run --rm --no-deps -v "${PWD}/data:/app/data" spark python scripts/download_data.py
docker compose run --rm --no-deps -v "${PWD}/data:/app/data" spark python scripts/prepare_data.py data/retailrocket_events.csv.zip data/events.csv
docker compose stop producer
docker compose run --rm --no-deps producer python producer.py --csv data/events.csv --interval 0.01 --replay-clock
```

For Bash, use `-v "$PWD/data:/app/data"`. The original archive is not globally event-time sorted. Sorting prevents a historical replay from unintentionally exceeding the watermark. The sorter uses a temporary SQLite database on disk and validates every row before writing a stable timestamp ordering. It preserves ties in original file order. Reserve several GB for its temporary files. Kaggle may change its download or login requirements; if the script receives HTML or an authentication error, download `events.csv` from the dataset page and sort that CSV instead.

`--replay-clock` applies one constant shift to all timestamps, preserving every original event-time difference. It does not make event-time spacing equal to wall-clock send spacing. Omit it to preserve the 2015 timestamps exactly. Watermarks follow event time, so historical timestamps still work. For a short replay use `--limit 20000`. A 2.75-million-row replay at 0.01 seconds per event takes at least about 7.6 hours, plus acknowledgement overhead. Use `--interval 0` for a throughput experiment and increase `--max-offsets` only after monitoring state and batch duration.

Do not rerun the CSV producer on the same topic and treat it as new ingestion without considering duplicates. Idempotence protects broker retries within one producer session. A new CSV replay creates new Kafka offsets and represents new input to Spark.

## Tests

Pure producer, validation and sorting tests:

```powershell
docker compose run --rm --no-deps spark python -m unittest tests.test_domain -v
```

Spark transformation tests and process-failure sink publication test:

```powershell
docker compose run --rm --no-deps spark python -m unittest tests.test_spark tests.test_sink_integration.SinkTests.test_atomic_publish_then_retry -v
```

Real Kafka to Spark to Parquet acceptance checks:

```powershell
docker compose stop producer
docker compose --profile tools run --rm acceptance
docker compose restart spark
docker compose up -d --wait spark
docker compose --profile tools run --rm demo
docker compose run --rm --no-deps spark python scripts/check_recovery.py
```

Allow up to three minutes for each expected epoch on a slow laptop. The acceptance script checks both Kafka partitions, a score of 6 for view plus cart, unseen recommendations, score 16 and suppression after purchase, watermark eviction, rejection of an event one hour behind the stream maximum, invalid-payload audit, Parquet fields, checkpoint commits and micro-batch metrics. `check_recovery.py` verifies retained query IDs, changed run IDs and unique Kafka coordinates in the persisted audit. Run all commands in order so it can observe two driver runs. GitHub Actions contains the same Linux Docker checks. No CI run is claimed until the workflow actually runs.

## Recovery and reset

```powershell
docker compose stop
docker compose start kafka spark
```

Keep Kafka volumes, all checkpoints and epoch outputs together. Starting offsets apply only to a new query; recovery uses saved offsets. Keep the logical query and shuffle partition count unchanged during recovery. For a clean experiment, preserve the current files first:

```powershell
docker compose down -v
Move-Item -LiteralPath ./runtime -Destination ("./runtime-archive-" + (Get-Date -Format yyyyMMdd-HHmmss))
docker compose up --build -d
```

`down -v` intentionally deletes this Compose project's Kafka volume. The subsequent move preserves the old audit, checkpoints and epochs. In Bash replace `Move-Item` with `mv runtime "runtime-archive-$(date +%Y%m%d-%H%M%S)"`. Never delete just the checkpoint while retaining old epochs, or delete Kafka while reusing its checkpoint. Do not run two intent writers against the same output directory.

## Troubleshooting

- Broker not healthy: check `docker compose logs kafka`, available disk and port 29092 conflicts.
- Build cannot resolve jars: permit access to Maven Central, retry the build, and retain the exact Spark/connector coordinates. The connector is resolved at image build time into Ivy's cache.
- Spark running but no recommendations: inspect cart and transaction counts, current event time, unseen candidates and the latest epoch. Purchases correctly suppress recommendations even with a high score.
- Python worker or Hadoop errors on native Windows: use the Linux Docker image. Host Java and Python are not needed for the supported execution route.
- Batch exceeds five seconds: the trigger is a scheduling interval, not a latency guarantee. Lower input rate or offsets per trigger, inspect the Spark UI and provision more resources.
- Spark exits after `Injected failure`: that is the opt-in test hook. Start the same query with the same checkpoints and sink directory to exercise retry skipping.

## Git submission

The selected submission repository is https://github.com/hoducmanh222/Kafka-Spark. If publishing from another machine, run these commands from the project folder:

```powershell
git init
git add .
git commit -m "Implement Retail Rocket Kafka and Spark streaming project"
git branch -M main
git remote add origin https://github.com/hoducmanh222/Kafka-Spark.git
git push -u origin main
```

The supplied report and presentation are also included under `docs/`. Include team names in your submission form. Check the Actions run and retain its logs before claiming an executed full-stack result. Dataset attribution is in `data/ATTRIBUTION.md`.
