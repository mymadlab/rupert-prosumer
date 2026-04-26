# pylint: disable=missing-module-docstring,missing-function-docstring,not-callable,invalid-name,too-few-public-methods
"""
Kafka Admin Client, Producer, and Consumer Test Cases
"""

import json
import tempfile
import time
import uuid
from pathlib import Path

from behave import given, then, when
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import NewTopic
from rupert_prosumer import RupertProsumer, RupertProsumerAdminClient


class _TestRupertProsumer(RupertProsumer):
	def __init__(self, config_file: str) -> None:
		super().__init__(config_file)
		self.received_message = None

	def process_event(self, consumer_message) -> None:
		self.received_message = consumer_message.value().decode('utf-8')
		self.stop()


def _build_kafka_settings(log_file_path: str | None = None) -> dict:
	test_id = uuid.uuid4().hex[:8]
	settings = {
		'kafka': {
			'connection': {
				'bootstrap.servers': '192.168.1.240:9092'
			},
			'consumer': {
				'group.id': f'rupert-behave-tests-{test_id}',
				'auto.offset.reset': 'earliest'
			},
			'topics': {
				'topic_one': f'rupert.behave.{test_id}.topic.one',
				'topic_two': f'rupert.behave.{test_id}.topic.two'
			}
		}
	}
	if log_file_path is not None:
		settings['logging'] = {
			'log_file': log_file_path,
			'rotation': None,
			'retention': None,
			'level': 'INFO'
		}
	return settings


def _wait_for_topics_state(client: RupertProsumerAdminClient,
									 topics: list[str],
									 should_exist: bool,
									 timeout_seconds: float = 15.0) -> dict:
	deadline = time.time() + timeout_seconds
	last_seen_topics = {}
	while time.time() < deadline:
		last_seen_topics = _list_topics_with_timeout(client, timeout_seconds=5.0)
		if should_exist and all(topic in last_seen_topics for topic in topics):
			return last_seen_topics
		if (not should_exist) and all(topic not in last_seen_topics for topic in topics):
			return last_seen_topics
		time.sleep(0.5)
	return last_seen_topics


def _create_topics_with_timeout(client: RupertProsumerAdminClient, timeout_seconds: float = 15.0) -> None:
	topics = [
		NewTopic(topic=topic_name, num_partitions=1, replication_factor=1)
		for topic_name in client.config['kafka']['topics'].values()
	]
	futures = client.admin_client.create_topics(new_topics=topics, validate_only=False)
	for future in futures.values():
		future.result(timeout=timeout_seconds)


def _delete_topics_with_timeout(client: RupertProsumerAdminClient, timeout_seconds: float = 15.0) -> None:
	topics = list(client.config['kafka']['topics'].values())
	futures = client.admin_client.delete_topics(topics)
	for future in futures.values():
		future.result(timeout=timeout_seconds)


def _list_topics_with_timeout(client: RupertProsumerAdminClient, timeout_seconds: float = 10.0) -> dict:
	consumer = Consumer(client.consumer_cfg)
	try:
		return consumer.list_topics(timeout=timeout_seconds).topics
	finally:
		consumer.close()


def _consume_message_with_timeout(client: RupertProsumer,
								 topic_key: str,
								 timeout_seconds: float = 20.0) -> str | None:
	consumer_cfg = client.config['kafka']['connection'] | client.config['kafka']['consumer']
	topic_name = client.config['kafka']['topics'][topic_key]
	consumer = Consumer(consumer_cfg)
	consumer.subscribe([topic_name])

	deadline = time.time() + timeout_seconds
	try:
		while time.time() < deadline:
			try:
				msg = consumer.poll(1.0)
			except (KafkaException, RuntimeError, TypeError):
				continue
			if msg is None:
				continue
			if msg.error():
				continue
			return msg.value().decode('utf-8')
	finally:
		consumer.close()

	return None


def _send_message_with_timeout(client: RupertProsumer,
							  topic_key: str,
							  payload: bytes,
							  timeout_seconds: float = 8.0) -> bool:
	producer_cfg = dict(client.config['kafka']['connection'])
	producer_cfg['message.timeout.ms'] = int(timeout_seconds * 1000)
	producer_cfg['socket.timeout.ms'] = int(timeout_seconds * 1000)
	producer = Producer(producer_cfg)
	try:
		producer.produce(client.config['kafka']['topics'][topic_key], payload)
		producer.poll(0)
		remaining = producer.flush(timeout_seconds)
		return remaining == 0
	finally:
		producer.flush(0)


def _write_temp_settings_file() -> str:
	settings = _build_kafka_settings()

	with tempfile.NamedTemporaryFile(mode='w',
																	suffix='.json',
																	delete=False,
																	encoding='utf-8') as temp_file:
		json.dump(settings, temp_file)
		return temp_file.name


def _write_temp_prosumer_settings_file() -> tuple[str, str]:
	with tempfile.NamedTemporaryFile(mode='w',
																	suffix='.log',
																	delete=False,
																	encoding='utf-8') as log_file:
		log_file_path = log_file.name

	settings = _build_kafka_settings(log_file_path=log_file_path)

	with tempfile.NamedTemporaryFile(mode='w',
																	suffix='.json',
																	delete=False,
																	encoding='utf-8') as temp_file:
		json.dump(settings, temp_file)
		return temp_file.name, log_file_path


def _cleanup(context):
	if hasattr(context, 'created_topics') and context.created_topics:
		try:
			if hasattr(context, 'admin_client'):
				_delete_topics_with_timeout(context.admin_client, timeout_seconds=8.0)
			elif hasattr(context, 'client') and hasattr(context.client, 'reset'):
				_delete_topics_with_timeout(context.client, timeout_seconds=8.0)
		except (SystemExit, TimeoutError, RuntimeError, TypeError, ValueError):
			pass
	if hasattr(context, 'settings_file_path'):
		Path(context.settings_file_path).unlink(missing_ok=True)
	if hasattr(context, 'prosumer_settings_file_path'):
		Path(context.prosumer_settings_file_path).unlink(missing_ok=True)
	if hasattr(context, 'prosumer_log_file_path'):
		Path(context.prosumer_log_file_path).unlink(missing_ok=True)

# Admin Client Test Cases

@given("a Kafka admin client is set up")
def a_Kafka_admin_client_is_set_up(context):
	context.settings_file_path = _write_temp_settings_file()
	context.client = RupertProsumerAdminClient(context.settings_file_path)
	context.created_topics = False

@when("we create a new topic")
def we_create_a_new_topic(context):
	_create_topics_with_timeout(context.client, timeout_seconds=12.0)
	context.created_topics = True

@then("the topic should be successfully created in the Kafka cluster")
def the_topic_should_be_successfully_created_in_the_Kafka_cluster(context):
	expected_topics = list(context.client.config['kafka']['topics'].values())
	actual_topics = _wait_for_topics_state(context.client, expected_topics, should_exist=True)

	assert all(topic in actual_topics for topic in expected_topics), 'Expected topics to be created.'
	_cleanup(context)

@when("we delete an existing topic")
def we_delete_an_existing_topic(context):
	_create_topics_with_timeout(context.client, timeout_seconds=12.0)
	context.created_topics = True
	_delete_topics_with_timeout(context.client, timeout_seconds=12.0)
	context.created_topics = False

@then("the topic should be successfully deleted from the Kafka cluster")
def the_topic_should_be_successfully_deleted_from_the_Kafka_cluster(context):
	expected_topics = list(context.client.config['kafka']['topics'].values())
	actual_topics = _wait_for_topics_state(context.client, expected_topics, should_exist=False)

	assert all(topic not in actual_topics for topic in expected_topics), 'Expected topics to be deleted.'
	_cleanup(context)


@when("we list the Kafka topics")
def we_list_the_Kafka_topics(context):
	_create_topics_with_timeout(context.client, timeout_seconds=12.0)
	context.created_topics = True
	context.topics = _list_topics_with_timeout(context.client, timeout_seconds=8.0)


@then("we should receive a dictionary of topics")
def we_should_receive_a_dictionary_of_topics(context):
	expected_topics = list(context.client.config['kafka']['topics'].values())
	assert isinstance(context.topics, dict), 'Expected get_topics() to return a dictionary.'
	assert all(topic in context.topics for topic in expected_topics), 'Expected all configured topics to be listed.'
	_cleanup(context)


@when("topic listing fails")
def topic_listing_fails(context):
	context.client.consumer_cfg = None
	context.exit_code = None
	try:
		context.client.get_topics()
	except SystemExit as exc:
		context.exit_code = exc.code


@then("the Kafka admin client should exit with an error code")
def the_Kafka_admin_client_should_exit_with_an_error_code(context):
	assert context.exit_code == 1, 'Expected get_topics() failure to exit with code 1.'
	_cleanup(context)


@given("a Kafka producer and consumer are set up")
def a_Kafka_producer_and_consumer_are_set_up(context):
	(
		context.prosumer_settings_file_path,
		context.prosumer_log_file_path,
	) = _write_temp_prosumer_settings_file()
	context.client = _TestRupertProsumer(context.prosumer_settings_file_path)
	context.client.consumer_cfg = (
		context.client.config['kafka']['connection']
		| context.client.config['kafka']['consumer']
	)
	context.admin_client = RupertProsumerAdminClient(context.prosumer_settings_file_path)
	_create_topics_with_timeout(context.admin_client, timeout_seconds=12.0)
	context.created_topics = True

	expected_topics = list(context.admin_client.config['kafka']['topics'].values())
	actual_topics = _wait_for_topics_state(context.admin_client, expected_topics, should_exist=True)
	assert all(topic in actual_topics for topic in expected_topics), 'Expected producer/consumer topics to exist.'


@when("the producer sends a message to a topic")
def the_producer_sends_a_message_to_a_topic(context):
	context.sent_event = {'message': 'rupert-behave-message'}
	context.sent_message = context.client.serialize_to_json(context.sent_event)
	context.send_ok = _send_message_with_timeout(
		context.client,
		'topic_one',
		context.sent_message.encode('utf-8'),
		timeout_seconds=8.0,
	)
	context.received_message = _consume_message_with_timeout(context.client, 'topic_one', timeout_seconds=20.0)


@then("the consumer should receive the message from the topic")
def the_consumer_should_receive_the_message_from_the_topic(context):
	assert context.send_ok, (
		'Kafka producer timed out while flushing. '
		'Check broker connectivity and advertised.listeners for reachable addresses.'
	)
	assert context.received_message is not None, (
		'Expected a Kafka message but timed out waiting for topic_one. '
		'This usually indicates a broker connectivity or advertised.listeners issue.'
	)
	assert context.received_message == context.sent_message
	assert json.loads(context.received_message) == context.sent_event
	_cleanup(context)
