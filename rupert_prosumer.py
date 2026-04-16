"""
Description: Parent module largely used to ensure a consistent
interface for communicating with kafka
"""

import sys
import json
import time
from beartype import beartype
from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from rupert_config import RupertConfig

class RupertProsumerAdminClient():
	"""
		Description: Responsible for managing Kafka topics for Rupert Prosumer
		Responsible for:
			1. Creating topics
			2. Deleting topics
			3. Retrieving topics
	"""
	@beartype
	def __init__(self, config_file: str) -> None:
		"""
			Description: Constructor for initializing Kafka
			Responsible for:
				1. Confirm the kafka settings file exists
				2. Load settings
				3. Init other variables
		"""

		self.config_file = config_file
		try:
			self.config = RupertConfig(self.config_file).config
		except FileNotFoundError as e:
			print(f"Settings file not found: {e}")
			sys.exit(1)
		except json.JSONDecodeError as e:
			print(f"JSON decode error in settings file: {e}")
			sys.exit(1)
		except PermissionError as e:
			print(f"Permission error loading settings file: {e}")
			sys.exit(1)
		except IsADirectoryError as e:
			print(f"Expected a file but found a directory: {e}")
			sys.exit(1)
		except OSError as e:
			print(f"OS error loading settings file: {e}")
			sys.exit(1)
		except UnicodeDecodeError as e:
			print(f"Encoding error loading settings file: {e}")
			sys.exit(1)

		try:
			self.consumer_cfg = self.config['kafka']['connection'] | self.config['kafka']['consumer']
			self.admin_client = AdminClient(self.config['kafka']['connection'])
		except KeyError as e:
			print(f"Missing Kafka config key: {e}")
			sys.exit(1)
		except TypeError as e:
			print(f"Type error in Kafka config: {e}")
			sys.exit(1)
		except ValueError as e:
			print(f"Value error in Kafka config: {e}")
			sys.exit(1)
		except KafkaException as e:
			print(f"Kafka exception: {e}")
			sys.exit(1)

	@beartype
	def get_topics(self) -> dict:
		"""
			Description: Resets Kafaka for synapses
			Responsible for:
				1. retrieving a list of topics
		"""
		try:
			consumer = Consumer(self.consumer_cfg)
			return consumer.list_topics().topics
		except KafkaException as e:
			print(f"Kafka exception: {e}")
			sys.exit(1)
		except TypeError as e:
			print(f"Type error in Kafka config: {e}")
			sys.exit(1)
		except ValueError as e:
			print(f"Value error in Kafka config: {e}")
			sys.exit(1)
		except RuntimeError as e:
			print(f"Runtime error in Kafka client: {e}")
			sys.exit(1)

	@beartype
	def initialize(self) -> None:
		"""
			Description: Initializes Kafaka for synapses
			Responsible for:
				1. Creates topics
		"""

		topics = []
		print('Creating Topics')
		for topic in self.config['kafka']['topics']:
			print(f"  {self.config['kafka']['topics'][topic]}")
			topics.append(NewTopic(topic=self.config['kafka']['topics'][topic],
				num_partitions=1, replication_factor=1))
		try:
			topic_futures = self.admin_client.create_topics(new_topics=topics, validate_only=False)
			for topic, future in topic_futures.items():
				future.result()
		except KafkaException as e:
			print(f"Kafka exception: {e}")
			sys.exit(1)
		except TypeError as e:
			print(f"Type error in Kafka config: {e}")
			sys.exit(1)
		except ValueError as e:
			print(f"Value error in Kafka config: {e}")
			sys.exit(1)

	@beartype
	def reset(self) -> None:
		"""
			Description: Resets Kafaka for synapses
			Responsible for:
				1. Deletes topics
		"""
		topics = []
		print('Deleting Topics')
		for topic in self.config['kafka']['topics']:
			print(f"  {self.config['kafka']['topics'][topic]}")
			topics.append(self.config['kafka']['topics'][topic])
		try:
			topic_futures = self.admin_client.delete_topics(topics)
			for topic, future in topic_futures.items():
				future.result()
		except KafkaException as e:
			print(f"Kafka exception: {e}")
			sys.exit(1)
		except TypeError as e:
			print(f"Type error in Kafka config: {e}")
			sys.exit(1)
		except ValueError as e:
			print(f"Value error in Kafka config: {e}")
			sys.exit(1)
