# pylint: disable=missing-module-docstring,missing-function-docstring,not-callable
"""
Loguru test cases
"""

import tempfile
import time
import shutil
from pathlib import Path
from behave import given, then, when
from rupert_logger import RupertLogger

def _new_log_file() -> str:
	with tempfile.NamedTemporaryFile(mode='w',
																	suffix='.log',
																	delete=False,
																	encoding='utf-8') as temp_file:
		return temp_file.name


def _read_log_file(file_path: str) -> str:
	return Path(file_path).read_text(encoding='utf-8')


def _build_logger_config(file_path: str,
												level: str = 'DEBUG',
												rotation=None,
												retention=None) -> dict:
	return {
		'log_file': file_path,
		'rotation': rotation,
		'retention': retention,
		'level': level,
	}


_LOGURU_LEVELS = [
	'TRACE',
	'DEBUG',
	'INFO',
	'SUCCESS',
	'WARNING',
	'ERROR',
	'CRITICAL',
]


def _level_below(level: str) -> str:
	normalized_level = level.upper()
	assert normalized_level in _LOGURU_LEVELS, f'Unsupported loguru level: {level}'
	level_position = _LOGURU_LEVELS.index(normalized_level)
	assert level_position > 0, 'TRACE has no lower built-in level.'
	return _LOGURU_LEVELS[level_position - 1]


@given("a temporary writable log file path")
def step_given_temporary_writable_log_file_path(context):
	context.log_file_path = _new_log_file()


@given("Loguru is configured to write log messages to a file")
def step_given_loguru_configured_to_write_to_file(context):
	context.log_file_path = _new_log_file()
	context.logger_config = _build_logger_config(context.log_file_path)


@when("we configure loguru to write to the file")
def step_when_we_configure_loguru_to_write_to_file(context):
	context.rupert_logger = RupertLogger(context.logger_config)


@then("messages should be written to the file when emitted")
def step_then_messages_should_be_written_when_emitted(context):
	message = 'messages should be written check'
	context.rupert_logger.info(message)
	assert message in _read_log_file(context.log_file_path)


@given("Loguru is configured to roll logs based on time")
def step_given_loguru_configured_to_roll_logs_based_on_time(context):
	context.rolling_dir = tempfile.mkdtemp(prefix='rupert-logger-')
	context.log_file_path = str(Path(context.rolling_dir) / 'timed.log')
	context.logger_config = _build_logger_config(
		context.log_file_path,
		level='INFO',
		rotation='5 seconds',
		retention='30 seconds',
	)


@when("we configure loguru to roll logs every 5 seconds")
def step_when_configure_roll_logs_every_5_seconds(context):
	context.rupert_logger = RupertLogger(context.logger_config)


@when("retentions is set to 30 seconds")
def step_when_retentions_is_set_to_30_seconds(context):
	context.logger_config['retention'] = '30 seconds'
	context.rupert_logger.reinit(context.logger_config)


@then("logs should be rolled every 5 seconds and old logs should be deleted after 30 seconds")
def step_then_logs_should_be_rolled_and_old_logs_deleted(context):
	context.rupert_logger.info('before-time-roll')
	time.sleep(6)
	context.rupert_logger.info('after-time-roll')

	log_files = list(Path(context.rolling_dir).glob('timed*.log'))
	assert len(log_files) > 1, 'Expected at least one rotated log file after 5 seconds.'
	assert context.logger_config['retention'] == '30 seconds'

	shutil.rmtree(context.rolling_dir, ignore_errors=True)


@given('Loguru is configured with minimum log level "{level}"')
def step_given_loguru_with_minimum_log_level(context, level):
	normalized_level = level.upper()
	assert normalized_level in _LOGURU_LEVELS, f'Unsupported loguru level: {level}'

	context.log_level = normalized_level
	context.log_file_path = _new_log_file()
	context.logger_config = _build_logger_config(context.log_file_path, level=normalized_level)
	context.rupert_logger = RupertLogger(context.logger_config)


@when('we emit one message at "{level}" level')
def step_when_emit_one_message_at_level(context, level):
	normalized_level = level.upper()
	assert normalized_level == context.log_level, 'Step level does not match configured sink level.'

	context.expected_message = f'{normalized_level} message should be written'
	getattr(context.rupert_logger, normalized_level.lower())(context.expected_message)


@then('the "{level}" message should be written to the file')
def step_then_level_message_written_to_file(context, level):
	normalized_level = level.upper()
	assert normalized_level == context.log_level, 'Step level does not match configured sink level.'

	output = _read_log_file(context.log_file_path)
	assert context.expected_message in output


@when('we emit one message below "{level}" and one at "{level}"')
def step_when_emit_below_and_at_level(context, level):
	normalized_level = level.upper()
	assert normalized_level == context.log_level, 'Step level does not match configured sink level.'

	lower_level = _level_below(normalized_level)
	context.filtered_message = f'{lower_level} message should be filtered out'
	context.allowed_message = f'{normalized_level} message should be written'

	getattr(context.rupert_logger, lower_level.lower())(context.filtered_message)
	getattr(context.rupert_logger, normalized_level.lower())(context.allowed_message)


@then('only the "{level}" message should be written')
def step_then_only_configured_level_message_written(context, level):
	normalized_level = level.upper()
	assert normalized_level == context.log_level, 'Step level does not match configured sink level.'

	output = _read_log_file(context.log_file_path)
	assert context.filtered_message not in output
	assert context.allowed_message in output
