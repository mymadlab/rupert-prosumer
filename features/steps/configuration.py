# pylint: disable=missing-module-docstring,missing-function-docstring,not-callable
"""
Configuration test cases
"""
import json
import tempfile
from pathlib import Path
from behave import given, then, when
from rupert_config import RupertConfig

def _read_configuration(file_path: str) -> dict:
	config = RupertConfig(file_path)
	return config.config

@given("a valid configuration file exists")
def step_given_valid_configuration_file_exists(context):
	with tempfile.NamedTemporaryFile(mode='w',
	                                 suffix='.json',
	                                 delete=False,
	                                 encoding='utf-8') as temp_file:
		temp_file.write('{"service": "rupert", "timeout": 30}')
		context.config_file_path = temp_file.name

@when("we are able to read the file")
def step_when_we_are_able_to_read_file(context):
	context.result = _read_configuration(context.config_file_path)

@then("we return it's contents as a dictionary")
def step_then_we_return_its_contents_as_dictionary(context):
	assert isinstance(context.result, dict), 'Expected configuration to be loaded as a dictionary.'
	assert context.result == {'service': 'rupert', 'timeout': 30}

	Path(context.config_file_path).unlink(missing_ok=True)


@given("a configuration file that does not exist")
def step_given_missing_configuration_file(context):
	context.config_file_path = '/tmp/rupert_file_does_not_exist.json'
	Path(context.config_file_path).unlink(missing_ok=True)

@when("we are unable to open the file")
def step_when_we_are_unable_to_open_file(context):
	try:
		_read_configuration(context.config_file_path)
		context.exception = None
	except FileNotFoundError as exc:	# noqa: BLE001
		context.exception = exc

@then("we want to return the exception")
def step_then_we_want_to_return_exception(context):
	assert context.exception is not None, 'Expected an exception when file cannot be opened.'
	assert isinstance(context.exception, FileNotFoundError)


@given("a configuration file with invalid syntax exists")
def step_given_invalid_syntax_configuration_file_exists(context):
	with tempfile.NamedTemporaryFile(mode='w',
	                                 suffix='.json',
	                                 delete=False,
	                                 encoding='utf-8') as temp_file:
		temp_file.write("{'service': 'rupert', }")
		context.invalid_config_file_path = temp_file.name

@when("we get an invalid syntax error")
def step_when_we_get_invalid_syntax_error(context):
	try:
		_read_configuration(context.invalid_config_file_path)
		context.exception = None
	except json.JSONDecodeError as exc:	# noqa: BLE001
		context.exception = exc

@then("we return the exception")
def step_then_we_return_the_exception(context):
	assert context.exception is not None, 'Expected an exception for invalid configuration syntax.'
	assert isinstance(context.exception, json.JSONDecodeError)

	Path(context.invalid_config_file_path).unlink(missing_ok=True)
