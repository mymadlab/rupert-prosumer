# pylint: disable=missing-module-docstring,missing-function-docstring,not-callable,invalid-name,too-few-public-methods
"""
Kafka Admin Client, Producer, and Consumer Test Cases
"""

import json
import tempfile
from pathlib import Path

from behave import given, then, when
import rupert_prosumer
from rupert_prosumer import RupertProsumer, RupertProsumerAdminClient


class _FakeFuture:
	def __init__(self, result_value=None):
		self._result_value = result_value

	def result(self):
		return self._result_value


class _FakeAdminClient:
	def __init__(self):
		self.created_topics = []
		self.deleted_topics = []

	def create_topics(self, new_topics, validate_only=False):
		_ = validate_only
		self.created_topics = [topic.topic for topic in new_topics]
		return {topic.topic: _FakeFuture(None) for topic in new_topics}

	def delete_topics(self, topics):
		self.deleted_topics = list(topics)
		return {topic: _FakeFuture(None) for topic in topics}


class _TopicListResult:
	def __init__(self, topics):
		self.topics = topics


class _FakeConsumer:
	def __init__(self, cfg):
		self.cfg = cfg

	def list_topics(self):
		return _TopicListResult(
			{
				'rupert.behave.topic.one': object(),
				'rupert.behave.topic.two': object()
			}
		)


class _FailingConsumer:
	def __init__(self, cfg):
		raise RuntimeError('consumer init failed')


class _FakeKafkaMessage:
	def __init__(self, payload: bytes):
		self._payload = payload

	def value(self):
		return self._payload

	def error(self):
		return None


class _InMemoryBroker:
	topics = {}

	@classmethod
	def reset(cls):
		cls.topics = {}


class _FakeStreamingConsumer:
	def __init__(self, cfg):
		self.cfg = cfg
		self.subscriptions = []

	def subscribe(self, topics):
		self.subscriptions = list(topics)

	def poll(self, timeout):
		_ = timeout
		for topic in self.subscriptions:
			messages = _InMemoryBroker.topics.get(topic, [])
			if messages:
				return _FakeKafkaMessage(messages.pop(0))
		return None

	def close(self):
		pass


class _FakeProducer:
	def __init__(self, cfg):
		self.cfg = cfg

	def produce(self, topic, event_bytes):
		_InMemoryBroker.topics.setdefault(topic, []).append(event_bytes)

	def poll(self, timeout):
		_ = timeout

	def flush(self):
		pass


class _TestRupertProsumer(RupertProsumer):
	def __init__(self, config_file: str) -> None:
		super().__init__(config_file)
		self.received_message = None

	def process_event(self, consumer_message) -> None:
		self.received_message = consumer_message.value().decode('utf-8')
		self.stop()


def _write_temp_settings_file() -> str:
	settings = {
		'kafka': {
			'connection': {
				'bootstrap.servers': '192.168.1.240:9092'
			},
			'consumer': {
				'group.id': 'rupert-behave-tests',
				'auto.offset.reset': 'earliest'
			},
			'topics': {
				'topic_one': 'rupert.behave.topic.one',
				'topic_two': 'rupert.behave.topic.two'
			}
		}
	}

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

	settings = {
		'kafka': {
			'connection': {
				'bootstrap.servers': '192.168.1.240:9092'
			},
			'consumer': {
				'group.id': 'rupert-behave-tests',
				'auto.offset.reset': 'earliest'
			},
			'topics': {
				'topic_one': 'rupert.behave.topic.one',
				'topic_two': 'rupert.behave.topic.two'
			}
		},
		'logging': {
			'log_file': log_file_path,
			'rotation': None,
			'retention': None,
			'level': 'INFO'
		}
	}

	with tempfile.NamedTemporaryFile(mode='w',
																	suffix='.json',
																	delete=False,
																	encoding='utf-8') as temp_file:
		json.dump(settings, temp_file)
		return temp_file.name, log_file_path


def _cleanup(context):
	if hasattr(context, 'settings_file_path'):
		Path(context.settings_file_path).unlink(missing_ok=True)
	if hasattr(context, 'prosumer_settings_file_path'):
		Path(context.prosumer_settings_file_path).unlink(missing_ok=True)
	if hasattr(context, 'prosumer_log_file_path'):
		Path(context.prosumer_log_file_path).unlink(missing_ok=True)
	if hasattr(context, 'original_consumer'):
		rupert_prosumer.Consumer = context.original_consumer
	if hasattr(context, 'original_producer'):
		rupert_prosumer.Producer = context.original_producer

# Admin Client Test Cases

@given("a Kafka admin client is set up")
def a_Kafka_admin_client_is_set_up(context):
	context.settings_file_path = _write_temp_settings_file()
	context.client = RupertProsumerAdminClient(context.settings_file_path)
	context.fake_admin_client = _FakeAdminClient()
	context.client.admin_client = context.fake_admin_client
	context.original_consumer = rupert_prosumer.Consumer

@when("we create a new topic")
def we_create_a_new_topic(context):
	context.client.initialize()

@then("the topic should be successfully created in the Kafka cluster")
def the_topic_should_be_successfully_created_in_the_Kafka_cluster(context):
	created_topics = context.fake_admin_client.created_topics
	expected_topics = list(context.client.config['kafka']['topics'].values())

	assert created_topics, 'Expected topics to be created.'
	assert sorted(created_topics) == sorted(expected_topics)
	_cleanup(context)

@when("we delete an existing topic")
def we_delete_an_existing_topic(context):
	context.client.reset()

@then("the topic should be successfully deleted from the Kafka cluster")
def the_topic_should_be_successfully_deleted_from_the_Kafka_cluster(context):
	deleted_topics = context.fake_admin_client.deleted_topics
	expected_topics = list(context.client.config['kafka']['topics'].values())

	assert deleted_topics, 'Expected topics to be deleted.'
	assert sorted(deleted_topics) == sorted(expected_topics)
	_cleanup(context)


@when("we list the Kafka topics")
def we_list_the_Kafka_topics(context):
	rupert_prosumer.Consumer = _FakeConsumer
	context.topics = context.client.get_topics()


@then("we should receive a dictionary of topics")
def we_should_receive_a_dictionary_of_topics(context):
	assert isinstance(context.topics, dict), 'Expected get_topics() to return a dictionary.'
	assert 'rupert.behave.topic.one' in context.topics
	assert 'rupert.behave.topic.two' in context.topics
	_cleanup(context)


@when("topic listing fails")
def topic_listing_fails(context):
	rupert_prosumer.Consumer = _FailingConsumer
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

	context.original_consumer = rupert_prosumer.Consumer
	context.original_producer = rupert_prosumer.Producer
	rupert_prosumer.Consumer = _FakeStreamingConsumer
	rupert_prosumer.Producer = _FakeProducer
	_InMemoryBroker.reset()


@when("the producer sends a message to a topic")
def the_producer_sends_a_message_to_a_topic(context):
	context.sent_event = {'message': 'rupert-behave-message'}
	context.sent_message = context.client.serialize_to_json(context.sent_event)
	context.client.send('topic_one', context.sent_message.encode('utf-8'))
	context.listen_exit_code = None
	try:
		context.client.listen('topic_one')
	except SystemExit as exc:
		context.listen_exit_code = exc.code


@then("the consumer should receive the message from the topic")
def the_consumer_should_receive_the_message_from_the_topic(context):
	assert context.client.received_message == context.sent_message
	assert json.loads(context.client.received_message) == context.sent_event
	assert context.listen_exit_code in (None, 0)
	_cleanup(context)
