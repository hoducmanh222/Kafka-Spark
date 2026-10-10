# Classroom demonstration

Target duration: 13 minutes. Build the image before class so network downloads do not consume presentation time. Use the PowerPoint deck as a visual guide and spend most of the time in terminals and the Spark UI.

## Preparation before class

1. Install and start Docker Desktop or Docker Engine. Confirm `docker compose version` and `docker info` work.
2. Run `docker compose build spark` and both unit-test commands in README.md.
3. Start `docker compose up -d --wait spark`. Run acceptance checks once, restart Spark and run the recovery checks. Save the output for the appendix to the team submission.
4. Keep two terminal windows in the repository. Use one for commands and one for `docker compose logs -f spark`. Open http://localhost:4040.
5. Ensure the sample producer is stopped while demonstrating controlled lateness or mock transactions. Check disk space.

## Minute 0 to 2

Explain the business case: a visitor adds an item to a cart and needs a useful suggestion while still shopping. Describe the five dataset fields, nullable transaction ID, 1/5/10 weights, 10-minute windows, one-minute slide and five-minute watermark. State the selected threshold of 5 and the separate purchase exclusion.

## Minute 2 to 4

```powershell
docker compose up -d --wait spark
docker compose ps -a
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic retail-rocket-events
```

Show `PartitionCount: 2`. Explain visitor keys and parallel source reads. The single broker is a classroom deployment with replication factor one. KRaft removes the need for ZooKeeper.

## Minute 4 to 6

```powershell
docker compose run --rm --no-deps producer python producer.py --csv data/events_sample.csv --limit 100 --interval 0.1 --replay-clock
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic retail-rocket-events --from-beginning --max-messages 3
```

Show real CSV records converted to JSON and the logged partition and offset. Point out numeric identifiers and JSON null. In the Spark logs, show `live_events_console`, `stream_progress` and `batch_committed`. Explain record count versus aggregate-row count.

## Minute 6 to 9

```powershell
docker compose --profile tools run --rm demo
```

Show clearly labeled mock telemetry. Explain that score 6 follows one view and one cart. The output recommends other unseen items based on live window popularity. Wait for the purchase stage. Explain that score 16 contains stronger engagement but the transaction removes cart eligibility. The current replacement snapshot retracts the user's recommendations.

## Minute 9 to 11

```powershell
docker compose --profile tools run --rm acceptance
docker compose run --rm --no-deps spark python scripts/inspect_output.py --parquet
```

Show acceptance checks for the watermark, malformed audit and both partitions. Explain that the very late event lies behind all affected windows, so it cannot change retained intent state. The audit still stores its Kafka record. Show date partitions and active checkpoint commit files.

## Minute 11 to 13

```powershell
docker compose restart spark
docker compose up -d --wait spark
docker compose --profile tools run --rm demo
docker compose run --rm --no-deps spark python scripts/check_recovery.py
```

Show that the application processes new records after restart using saved offsets and state. Explain why an atomic published epoch is skipped on retry and why multiple outputs are not a single global transaction. Close with the measured batch duration visible in logs and the limitations of popularity-based recommendations. Do not substitute an invented latency number.

If restart takes too long, use saved evidence from the pre-class run and clearly label it as that run. The remainder of the demonstration still runs live.

## Expected oral questions

- Why update mode? Append mode would wait for final windows, delaying in-session recommendations.
- Why does purchase score 10 not trigger? Cart presence and zero transactions are independent eligibility conditions.
- Why a one-minute slide? The brief specifies a 10-minute sliding window but leaves the slide unspecified. It creates ten overlapping windows per event and a practical aligned serving window.
- Does five-minute watermark mean sleep five minutes? No. It advances with observed event time.
- Is the algorithm trained ML? No. It is an explicit streaming intent and popularity baseline. ALS can provide an offline candidate model later.
- Is the system exactly once everywhere? Replay-safe sinks and checkpoints protect processing retries under their filesystem contract. Console logs and independent queries have separate guarantees.
- Why no ZooKeeper? The selected Kafka deployment uses KRaft controllers.
