# pylint: disable=missing-module-docstring,missing-function-docstring,not-callable
"""
Kafka Admin Client, Producer, and Consumer Test Cases
"""

import json
import tempfile
from pathlib import Path

from behave import given, then, when
import rupert_prosumer
from rupert_prosumer import RupertProsumerAdminClient


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

	with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as temp_file:
		json.dump(settings, temp_file)
		return temp_file.name


def _cleanup(context):
	if hasattr(context, 'settings_file_path'):
		Path(context.settings_file_path).unlink(missing_ok=True)
	if hasattr(context, 'original_consumer'):
		rupert_prosumer.Consumer = context.original_consumer

"""Admin Client Test Cases"""

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

"""
@given("a Kafka producer and consumer are set up")
def a_Kafka_producer_and_consumer_are_set_up(context):
	raise StepNotImplementedError('Given a Kafka producer and consumer are set up')

@when("the producer sends a message to a topic")
def the_producer_sends_a_message_to_a_topic(context):
	raise StepNotImplementedError('When the producer sends a message to a topic')

@then("the consumer should receive the message from the topic")
def the_consumer_should_receive_the_message_from_the_topic(context):
	raise StepNotImplementedError('Then the consumer should receive the message from the topic')
"""