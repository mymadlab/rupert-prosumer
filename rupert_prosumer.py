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
from .rupert_config import RupertConfig
from .rupert_logger import RupertLogger

class RupertProsumer():
	"""
		Description: Parent class used by rupert prosumers.
		Responsible for:
			1. Basic constructor for starting prosumers.
			2. Initiates Kafka consumer
			3. Contains method to push to Kafka as producer
	"""
	@beartype
	def __init__(self, config_file: str) -> None:
		"""
			Construct for rupert prosumer classes.
			Responsible for:
				1. Loading config.
				2. Initate topic consumer
			Requires:
				config_file - Path to the rupert prosumer setting file
		"""
		self.config_file = config_file
		self.config = {}
		self.__load_config()
		self.logger = None
		self.consumer = None
		self.close_consumer = False

	@beartype
	def listen(self, topic: str) -> None:
		"""
			Description: Connects to Kafka and consumes a topic
			Responsible for:
				1. Connecting to Kafka and listening for events/messages
				2. Calls the process_event method
			Requires:
				Nothing
			Raises:
				RuntimeError if called on a closed consumer
		"""
		self.logger = RupertLogger(self.config['logging'])
		try:
			topic_name = self.config['kafka']['topics'].get(topic, topic)
			self.consumer = Consumer(self.config['kafka']['connection'] | self.config['kafka']['consumer'])
			self.consumer.subscribe([topic_name])
		except (AttributeError, KeyError, TypeError, ValueError) as e:
			print(f"Kafka consumer error: {e}")
			self.logger.error(f"Kafka consumer error: {e}")
			sys.exit(1)
		while True:
			if self.close_consumer:
				self.consumer.close()
				sys.exit()
			try:
				msg = self.consumer.poll(1.0)
			except (KafkaException, RuntimeError,TypeError) as e:
				print(f"Kafka connection error: {e}")
				self.logger.error(f"Kafka connection error: {e}")
				continue
			if msg is None:
				pass
			elif msg.error():
				print(msg.error())
			else:
				try:
					self.process_event(msg)
				except (UnicodeDecodeError, TypeError) as e:
					print(f"Error processing event: {e}")
					self.logger.error(f"Error processing event: {e}")

	@beartype
	def process_event(self, consumer_message) -> None:
		"""
			Description: Each rupert prosumer should overide this method. The code here mostly
				is to support testing connectivity with Kafka
			Responsible for:
				1. Converts the messages value to string
				2. Returns the string
				3. Quits the class
			Requires:
				consuer_message
		"""
		try:
			message_bytes = consumer_message.value()
			decoded_message = message_bytes.decode('utf-8')
			self.logger.success("Successfully received an event.")
			self.logger.trace("RECEIVED MESSAGE: " + decoded_message)
		except UnicodeDecodeError as e:
			print(f"Message decoding error: {e}")
			self.logger.error(f"Message decoding error: {e}")
			try:
				self.logger.trace(f"Message bytes: {message_bytes!r}")
			except (AttributeError, TypeError, ValueError, RuntimeError):
				pass
		except (AttributeError, TypeError) as e:
			print(f"Error processing message: {e}")
			self.logger.error(f"Error processing message: {e}")
		time.sleep(10)
		self.stop()

	@beartype
	def reload(self):
		"""
			Reloads the rupert prosumer
				1. Reloads the configuration
		"""
		self.__load_config()

	@beartype
	def send(self, topic: str, event_bytes: bytes) -> None:
		"""
			Description: Sends a byte array to Kafka as a producer
			Responsible for:
				1. Send event bytes to topic
			Requires:
				1. topic - Name of the topic to send message/event to (string)
				2. event_bytes - array of bytes
			Raises:
				BufferError - if the internal producer message queue is full 
				KafkaException - for other errors, see exception code
				NotImplementedError - if timestamp is specified without underlying library support.
		"""
		try:
			producer = Producer(self.config['kafka']['connection'])
			producer.produce(self.config['kafka']['topics'][topic], event_bytes)
			self.logger.success("Successfully sent an event.")
			producer.poll(10000)
			producer.flush()
		except BufferError as e:
			print(f"Producer buffer error: {e}")
			self.logger.error(f"Producer buffer error: {e}")
		except (KeyError, TypeError, ValueError) as e:
			print(f"JSON error: {e}")
			self.logger.error(f"JSON error: {e}")
		except (KafkaException, RuntimeError) as e:
			print(f"Kafka producer error: {e}")
			self.logger.error(f"Kafka producer error: {e}")

	@beartype
	def serialize_to_json(self, event: dict) -> str:
		"""
			Description: Serializes a dictionary to JSON and sends it as bytes to Kafka
			Responsible for:
				1. Serializes a dictionary to JSON
				2. Converts the JSON string to bytes
				3. Send the byte array to Kafka as a producer
		"""
		try:
			event_json = json.dumps(event)
		except (TypeError, ValueError) as e:
			print(f"JSON serialization error: {e}")
			self.logger.error(f"JSON serialization error: {e}")
			self.logger.debug(f"Event that failed to serialize: {event!r}")

		return event_json

	@beartype
	def stop(self) -> None:
		"""Closes the the consumer and exit"""
		self.close_consumer = True
		self.logger.warning("Shutting down Rupert Prosumer...")


	## Private methods, best not to overide anything beyond this point

	@beartype
	def __load_config(self) -> None:
		"""
			Description: Parent class used by other rupert prosumers.
			Responsible for:
				1. Loading config
			Requires:
				config_file - Path to the rupert prosumer setting file
		"""
		with open(self.config_file, 'r', encoding='utf-8') as config:
			config_json = config.read()
		self.config = json.loads(config_json)

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
				1. Confirm the kafka config file exists
				2. Load config
				3. Init other variables
		"""

		self.config_file = config_file
		self.config = RupertConfig(self.config_file).config

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
			Description: Gets a list of Kafaka topics
			Responsible for:
				1. retrieving a list of topics
		"""
		try:
			consumer = Consumer(self.config['kafka']['connection'] | self.config['kafka']['consumer'])
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
			Description: Initializes Kafaka topics
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
			Description: Resets Kafaka topics
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
