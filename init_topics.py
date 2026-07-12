#!/usr/bin/env python3
"""Simple CLI runner for RupertConfig."""

import argparse
import json
import os
import sys
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from rupert_config import RupertConfig


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Initialize Kafka topics and print Rupert JSON configuration.",
    )
    parser.add_argument(
        "--config-file",
        default=os.environ.get("RUPERT_CONFIG_PATH"),
        help="Path to the Rupert config JSON file. Defaults to RUPERT_CONFIG_PATH.",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Print compact JSON instead of pretty formatted JSON.",
    )
    parser.add_argument(
        "--skip-topic-initialize",
        action="store_true",
        help="Skip Kafka topic initialization.",
    )
    return parser


def initialize_topics(config: dict) -> None:
    kafka_cfg = config.get("kafka", {})
    connection_cfg = kafka_cfg.get("connection")
    topics_cfg = kafka_cfg.get("topics")

    if not isinstance(connection_cfg, dict):
        raise ValueError("kafka.connection must be a dictionary")
    if not isinstance(topics_cfg, dict):
        raise ValueError("kafka.topics must be a dictionary")

    admin_client = AdminClient(connection_cfg)
    configured_topics = list(topics_cfg.values())
    metadata = admin_client.list_topics(timeout=10)
    existing_topics = set(metadata.topics.keys())
    missing_topics = [topic for topic in configured_topics if topic not in existing_topics]

    if not missing_topics:
        print("Kafka topics already initialized.")
        return

    print("Creating Kafka topics:")
    for topic in missing_topics:
        print(f"  {topic}")

    new_topics = [NewTopic(topic=topic, num_partitions=1, replication_factor=1) for topic in missing_topics]
    topic_futures = admin_client.create_topics(new_topics=new_topics, validate_only=False)
    for future in topic_futures.values():
        future.result()


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if not args.config_file:
        parser.error("Missing --config-file and RUPERT_CONFIG_PATH is not set.")

    config = RupertConfig(args.config_file).config

    if not args.skip_topic_initialize:
        try:
            initialize_topics(config)
        except (KafkaException, TypeError, ValueError) as e:
            print(f"Failed to initialize topics: {e}")
            return 1

    if args.compact:
        print(json.dumps(config, separators=(",", ":")))
    else:
        print(json.dumps(config, indent=2, sort_keys=True))

    return 0


if __name__ == "__main__":
    sys.exit(main())
