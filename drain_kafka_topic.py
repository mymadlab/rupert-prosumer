#!/usr/bin/env python3
"""
Drains unread messages from a Kafka topic for a consumer group.

This advances offsets for the selected group by consuming until no new
messages are returned for a configurable number of consecutive polls.
"""

import argparse
import sys
from typing import Optional

from beartype import beartype
from confluent_kafka import Consumer, KafkaException
from rupert_prosumer import RupertProsumer


class RupertTopicDrainer(RupertProsumer):
	"""Consumes unread messages and commits offsets for a topic."""

	@beartype
	def __init__(self, config_file: str, group_id: Optional[str] = None) -> None:
		super().__init__(config_file)
		try:
			self.consumer_cfg = self.config['kafka']['connection'] | self.config['kafka']['consumer']
			if group_id:
				self.consumer_cfg['group.id'] = group_id
			self.consumer_cfg['enable.auto.commit'] = False
			self.consumer_cfg.setdefault('auto.offset.reset', 'earliest')
		except (AttributeError, KeyError, TypeError, ValueError) as e:
			print(f"Kafka config error: {e}")
			self.logger.error(f"Kafka config error: {e}")
			sys.exit(1)

	@beartype
	def _resolve_topic_name(self, topic: str) -> str:
		try:
			return self.config['kafka']['topics'].get(topic, topic)
		except (AttributeError, KeyError, TypeError) as e:
			print(f"Topic config error: {e}")
			self.logger.error(f"Topic config error: {e}")
			sys.exit(1)

	@beartype
	def drain_topic(
		self,
		topic: str,
		poll_timeout: float = 1.0,
		idle_polls: int = 3,
		commit_every: int = 100,
	) -> int:
		topic_name = self._resolve_topic_name(topic)
		drained_count = 0
		empty_polls = 0
		consumer = None

		try:
			consumer = Consumer(self.consumer_cfg)
			consumer.subscribe([topic_name])

			self.logger.info(
				f"Draining topic '{topic_name}' for consumer group '{self.consumer_cfg.get('group.id')}'."
			)

			while empty_polls < idle_polls:
				msg = consumer.poll(poll_timeout)

				if msg is None:
					empty_polls += 1
					continue

				if msg.error():
					self.logger.warning(f"Kafka message error while draining: {msg.error()}")
					continue

				empty_polls = 0
				drained_count += 1

				if drained_count % commit_every == 0:
					consumer.commit(asynchronous=False)

			if drained_count > 0:
				consumer.commit(asynchronous=False)

			return drained_count
		except (KafkaException, RuntimeError, TypeError, ValueError) as e:
			print(f"Kafka drain error: {e}")
			self.logger.error(f"Kafka drain error: {e}")
			sys.exit(1)
		finally:
			if consumer is not None:
				try:
					consumer.close()
				except (KafkaException, RuntimeError, TypeError, ValueError):
					pass


def _build_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(
		description='Consume and clear unread Kafka messages for a topic and consumer group.'
	)
	parser.add_argument(
		'--config-file',
		required=True,
		help='Path to the Rupert Prosumer JSON config file.',
	)
	parser.add_argument(
		'--topic',
		required=True,
		help='Topic key from kafka.topics or a raw Kafka topic name.',
	)
	parser.add_argument(
		'--group-id',
		default=None,
		help='Optional override for kafka.consumer.group.id.',
	)
	parser.add_argument(
		'--poll-timeout',
		type=float,
		default=1.0,
		help='Consumer poll timeout in seconds. Default: 1.0',
	)
	parser.add_argument(
		'--idle-polls',
		type=int,
		default=3,
		help='Number of consecutive empty polls before exiting. Default: 3',
	)
	parser.add_argument(
		'--commit-every',
		type=int,
		default=100,
		help='Commit offsets after this many drained messages. Default: 100',
	)
	return parser


def main() -> int:
	parser = _build_parser()
	args = parser.parse_args()

	if args.poll_timeout <= 0:
		parser.error('--poll-timeout must be greater than 0')
	if args.idle_polls < 1:
		parser.error('--idle-polls must be at least 1')
	if args.commit_every < 1:
		parser.error('--commit-every must be at least 1')

	drainer = RupertTopicDrainer(args.config_file, args.group_id)
	drained_count = drainer.drain_topic(
		topic=args.topic,
		poll_timeout=args.poll_timeout,
		idle_polls=args.idle_polls,
		commit_every=args.commit_every,
	)

	summary = f"Drained {drained_count} unread message(s) from topic '{args.topic}'."
	print(summary)
	drainer.logger.success(summary)
	return 0


if __name__ == '__main__':
	sys.exit(main())