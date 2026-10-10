# Full-data classroom demonstration: 13 minutes

The demonstration uses the complete **2,756,101-event Retail Rocket CSV** and the delivered **Python/PySpark** pipeline. Complete and verify the full replay before the timed presentation, then show its actual evidence and replay a genuine shopping trace selected from that same full CSV. The 13-minute presentation duration is separate from the machine-dependent full processing duration.

## Before class

1. Install and start Docker with Linux containers. Confirm `docker info` and `docker compose version` work.
2. Run `docker compose up --build -d`. The full producer has no limit. In a Git clone, data-init downloads and stably sorts the entire source automatically.
3. Run `docker compose --profile tools run --rm verify-full`. Do not proceed until it reports `PASS FULL DATASET END TO END` and creates `runtime/full_replay_verified.json`.
4. Run the unit tests, sink fault test and controlled acceptance commands in README. Save their actual outputs.
5. Keep the Kafka and PySpark services running, with the full producer already completed. Open the Spark UI at http://localhost:4040 and a second terminal running `docker compose logs -f spark`.

## Minute 0-2: scenario and full-source evidence

Explain in-session cart personalization and the 1/5/10 weights. State the 10-minute window, one-minute slide and five-minute watermark. Show the complete CSV and its 2,756,101 events: 2,664,312 views, 69,332 carts and 22,457 transactions. Explain that a cart is required and a purchase suppresses that visitor-item anchor, even when the numerical score increases.

## Minute 2-4: Kafka topic and complete producer receipt

```powershell
docker compose ps -a
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic retail-rocket-events
Get-Content runtime/producer_complete.json
```

Show `PartitionCount: 2`, all 2,756,101 acknowledged input records and the per-partition offset ranges. Explain that one KRaft broker is the permitted local single-node environment and does not provide replicated availability.

## Minute 4-6: full downstream processing and PySpark logs

```powershell
Get-Content runtime/full_replay_verified.json
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic retail-rocket-events --from-beginning --max-messages 3
docker compose run --rm --no-deps spark python scripts/inspect_output.py --parquet
```

The full verifier checked all source rows in the audit, matching event counts, source-coordinate uniqueness, both partitions and intent-query catch-up. Show `stream_progress` with batch ID, raw record count and execution duration. Explain why input rows differ from window aggregate rows. Open `streaming_etl.py` or `retail/transforms.py` to show the PySpark APIs; the connector JAR suffix is not Scala application code.

## Minute 6-9: genuine cart and purchase transition

```powershell
docker compose --profile tools run --rm demo
```

This command requires full replay verification. It selects actual records from the full CSV: candidate views and a visitor's view/cart/purchase sequence. It prints `REAL DATASET TRACE`, original timestamps and newly assigned Kafka coordinates. The constant timestamp shift preserves the original intervals. The trace starts beyond earlier active state so its output is clear. Explain that this is a disclosed second replay of selected genuine rows after complete-source ingestion.

Show score 6 after view plus cart and the unseen recommended item IDs. Wait for the actual transaction. Show score 16 in state and recommendation retraction. Do not describe these records as mock or invent product names.

## Minute 9-11: storage, watermark and checkpoint

```powershell
docker compose run --rm --no-deps spark python scripts/inspect_output.py --parquet
Get-ChildItem runtime/checkpoints/intent/commits
Get-Content runtime/metrics/progress.jsonl -Tail 3
```

Show partitioned Parquet, checkpoint commit files, source progress and state metrics. Explain how the five-minute watermark applies in event time and why the audit retains records even if they are too late for intent. Controlled malformed/late test injections are separate acceptance checks, already executed before class.

## Minute 11-13: restart and continued genuine processing

```powershell
docker compose restart spark
docker compose up -d --wait spark
docker compose --profile tools run --rm demo
docker compose run --rm --no-deps spark python scripts/check_recovery.py
```

Show new genuine source messages processed after the restart, a retained query ID and changed run ID. Explain source replay, checkpointed state and retry-safe epoch publication. If startup takes too long, show saved evidence from the actual pre-class restart and clearly label it as saved evidence.

## Likely oral questions

- Why PySpark rather than Scala? The assignment specifies PySpark; all producer and transformation code is Python. Spark's JVM connector binary is a dependency.
- Why full data before the timed demo? Full-source processing time varies by hardware. Its verified result proves complete ingestion; a paced real trace makes the immediate business transition observable in the 10-15-minute presentation.
- Why update mode? Final append-only windows would delay in-session recommendation changes.
- Why does score 16 not qualify? Purchase absence is a separate eligibility condition from engagement weight.
- Why one-minute slide? The brief supplies window length and watermark but leaves the slide unspecified. This explicit choice creates ten overlapping windows per event.
- Why retain raw Kafka coordinates? They support audit and recovery checks without wrongly deduplicating identical business clicks.
- What is exactly once here? A replayable source, checkpoint state and retry-safe sink produce one logical effect within their stated contract. Console text and independent queries are not one global transaction.

On Bash replace `Get-Content` with `cat`/`tail` and `Get-ChildItem` with `ls`. Follow README for full reset rather than deleting a checkpoint in isolation.
