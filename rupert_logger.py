"""
Description: Managers logging for rupert
"""
import sys
from loguru import logger
from beartype import beartype

class RupertLogger():
	"""
		Description: Main class for initiating the logging
	"""
	@beartype
	def __init__(self, config: dict) -> None:
		"""
			Construct for RupertLogger class
			Responsible for:
				1. Initiating the logger with the provided configuration
		"""
		self.config = config
		self.__cfg_logger()

	@beartype
	def reinit(self, config: dict) -> None:
		"""
			Method to reinitialize the logger with new configuration
		"""
		self.config = config
		logger.remove()  # Remove existing sinks before reconfiguring
		self.__cfg_logger()

	# Log level methods
	@beartype
	def trace(self, message: str) -> None:
		"""Logs a message with level TRACE."""
		try:
			logger.trace(message)
		except TypeError as e:
			print(f"Type error while logging TRACE message: {e}")
			sys.exit(1)
		except ValueError as e:
			print(f"Value error while logging TRACE message: {e}")
			sys.exit(1)
		except RuntimeError as e:
			print(f"Runtime error while logging TRACE message: {e}")
			sys.exit(1)

	@beartype
	def debug(self, message: str) -> None:
		"""Logs a message with level DEBUG."""
		try:
			logger.debug(message)
		except TypeError as e:
			print(f"Type error while logging DEBUG message: {e}")
			sys.exit(1)
		except ValueError as e:
			print(f"Value error while logging DEBUG message: {e}")
			sys.exit(1)
		except RuntimeError as e:
			print(f"Runtime error while logging DEBUG message: {e}")
			sys.exit(1)

	@beartype
	def info(self, message: str) -> None:
		"""Logs a message with level INFO."""
		try:
			logger.info(message)
		except TypeError as e:
			print(f"Type error while logging INFO message: {e}")
			sys.exit(1)
		except ValueError as e:
			print(f"Value error while logging INFO message: {e}")
			sys.exit(1)
		except RuntimeError as e:
			print(f"Runtime error while logging INFO message: {e}")
			sys.exit(1)

	@beartype
	def success(self, message: str) -> None:
		"""Logs a message with level SUCCESS."""
		try:
			logger.success(message)
		except TypeError as e:
			print(f"Type error while logging SUCCESS message: {e}")
			sys.exit(1)
		except ValueError as e:
			print(f"Value error while logging SUCCESS message: {e}")
			sys.exit(1)
		except RuntimeError as e:
			print(f"Runtime error while logging SUCCESS message: {e}")
			sys.exit(1)

	@beartype
	def warning(self, message: str) -> None:
		"""Logs a message with level WARNING."""
		try:
			logger.warning(message)
		except TypeError as e:
			print(f"Type error while logging WARNING message: {e}")
			sys.exit(1)
		except ValueError as e:
			print(f"Value error while logging WARNING message: {e}")
			sys.exit(1)
		except RuntimeError as e:
			print(f"Runtime error while logging WARNING message: {e}")
			sys.exit(1)

	@beartype
	def error(self, message: str) -> None:
		"""Logs a message with level ERROR."""
		try:
			logger.error(message)
		except TypeError as e:
			print(f"Type error while logging ERROR message: {e}")
			sys.exit(1)
		except ValueError as e:
			print(f"Value error while logging ERROR message: {e}")
			sys.exit(1)
		except RuntimeError as e:
			print(f"Runtime error while logging ERROR message: {e}")
			sys.exit(1)

	@beartype
	def critical(self, message: str) -> None:
		"""Logs a message with level CRITICAL."""
		try:
			logger.critical(message)
		except TypeError as e:
			print(f"Type error while logging CRITICAL message: {e}")
			sys.exit(1)
		except ValueError as e:
			print(f"Value error while logging CRITICAL message: {e}")
			sys.exit(1)
		except RuntimeError as e:
			print(f"Runtime error while logging CRITICAL message: {e}")
			sys.exit(1)

	@beartype
	def __cfg_logger(self) -> None:
		"""
			Private method to configure the logger based on the provided configuration
		"""
		try:
			logger.add(self.config['log_file'],
							rotation=self.config['rotation'],
							retention=self.config['retention'],
							level=self.config['level'])
		except KeyError as e:
			print(f"Missing logger config key: {e}")
			sys.exit(1)
		except TypeError as e:
			print(f"Type error in logger configuration: {e}")
			sys.exit(1)
		except ValueError as e:
			print(f"Value error in logger configuration: {e}")
			sys.exit(1)
		except FileNotFoundError as e:
			print(f"Log file path not found: {e}")
			sys.exit(1)
		except PermissionError as e:
			print(f"Permission error creating log sink: {e}")
			sys.exit(1)
		except IsADirectoryError as e:
			print(f"Expected a log file but found a directory: {e}")
			sys.exit(1)
		except OSError as e:
			print(f"OS error creating log sink: {e}")
			sys.exit(1)
		except RuntimeError as e:
			print(f"Runtime error creating log sink: {e}")
			sys.exit(1)
