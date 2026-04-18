Feature: Rupert Logger

	Scenario: Writing log messages to a file
		Given Loguru is configured to write log messages to a file
		When we configure loguru to write to the file
		Then messages should be written to the file when emitted

	Scenario: Rolling logs based on time
		Given Loguru is configured to roll logs based on time
		When we configure loguru to roll logs every 5 seconds
		And retentions is set to 30 seconds
		Then logs should be rolled every 5 seconds and old logs should be deleted after 30 seconds

	Scenario Outline: Set each built-in log level as the minimum sink threshold
		Given Loguru is configured with minimum log level "<level>"
		When we emit one message at "<level>" level
		Then the "<level>" message should be written to the file

		Examples:
			| level    |
			| TRACE    |
			| DEBUG    |
			| INFO     |
			| SUCCESS  |
			| WARNING  |
			| ERROR    |
			| CRITICAL |

	Scenario Outline: Filter out messages below the configured minimum level
		Given Loguru is configured with minimum log level "<level>"
		When we emit one message below "<level>" and one at "<level>"
		Then only the "<level>" message should be written

		Examples:
			| level    |
			| DEBUG    |
			| INFO     |
			| SUCCESS  |
			| WARNING  |
			| ERROR    |
			| CRITICAL |

