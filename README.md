# rupert-prosumer

rupert-prosumer provides reusable Kafka producer/consumer/admin helpers for Rupert services.

It includes:

- A base prosumer class for sending and consuming events.
- A Kafka admin client wrapper for create/list/delete topic operations.
- A logger wrapper around Loguru.
- A utility script to drain unread messages for a consumer group.

## Module Overview

- `RupertProsumer` in `rupert_prosumer.py`
  - Loads config and logger.
  - Sends bytes to Kafka with `send(topic, event_bytes)`.
  - Consumes with `listen(topic)` and delegates message handling to `process_event(...)`.
  - Converts dict payloads to JSON with `serialize_to_json(event)`.

- `RupertProsumerAdminClient` in `rupert_prosumer.py`
  - `initialize()` to create topics from config.
  - `get_topics()` to retrieve topic metadata.
  - `reset()` to delete topics from config.

- `RupertLogger` in `rupert_logger.py`
  - Loguru-backed logger setup and level helpers (`trace`, `debug`, `info`, `success`, etc.).

- `RupertConfig` in `rupert_config.py`
  - JSON config loading/reloading helper.

- `drain_kafka_topic.py`
  - CLI script that consumes and commits offsets to clear unread messages for a topic/group.

## Required Modules

External Python packages:

- `beartype`
- `confluent-kafka`
- `loguru`
- `behave` (for BDD tests under `features/`)

## Configuration

Create a JSON config file:

```json
{
  "kafka": {
    "connection": {
      "bootstrap.servers": "localhost:9092"
    },
    "consumer": {
      "group.id": "rupert-consumer-group",
      "auto.offset.reset": "earliest"
    },
    "topics": {
      "topic_one": "rupert.topic.one",
      "topic_two": "rupert.topic.two"
    }
  },
  "logging": {
    "rotation": "10 MB",
    "retention": "7 days",
    "level": "INFO"
  }
}
```

## Usage Examples

### 1) Producer Example

```python
from rupert_prosumer import RupertProsumer


class ProducerService(RupertProsumer):
    def __init__(self, config_file: str):
        super().__init__(config_file)


svc = ProducerService("./settings.json")
event = {"event": "order_created", "order_id": 12345}
payload = svc.serialize_to_json(event)
svc.send("topic_one", payload.encode("utf-8"))
```

### 2) Consumer Example

`listen(...)` expects `self.consumer_cfg` to be set before use.

```python
import json
from rupert_prosumer import RupertProsumer


class ConsumerService(RupertProsumer):
    def __init__(self, config_file: str):
        super().__init__(config_file)
        self.consumer_cfg = self.config["kafka"]["connection"] | self.config["kafka"]["consumer"]

    def process_event(self, consumer_message) -> None:
        raw = consumer_message.value().decode("utf-8")
        event = json.loads(raw)
        self.logger.info(f"Received event: {event}")


svc = ConsumerService("./settings.json")
svc.listen("topic_one")
```

### 3) Kafka Admin Example

```python
from rupert_prosumer import RupertProsumerAdminClient


admin = RupertProsumerAdminClient("./settings.json")

# Create all topics listed in kafka.topics
admin.initialize()

# Read topic metadata
topics = admin.get_topics()
print(list(topics.keys()))

# Delete all topics listed in kafka.topics
# admin.reset()
```

### 4) Drain Unread Messages for a Consumer Group

Use the CLI utility to consume and commit offsets until the topic is drained:

```bash
python drain_kafka_topic.py \
  --config-file ./settings.json \
  --topic topic_one
```

With consumer group override:

```bash
python drain_kafka_topic.py \
  --config-file ./settings.json \
  --topic rupert.topic.one \
  --group-id maintenance-drain-group
```

Important:

- Draining clears unread messages for the selected consumer group by advancing committed offsets.
- It does not delete messages from the Kafka topic itself.

## Operational Notes

- `send(...)` expects bytes payloads. Use `serialize_to_json(...)` + `.encode("utf-8")` for JSON events.
- `serialize_to_json(...)` is intended for JSON-serializable dict payloads.
- `listen(...)` loops until `stop()` is called.
