"""
Description: Open and manage configuration files for Rupert
"""
import json
from beartype import beartype

class RupertConfig():
	"""
		Description: Main class for initiating the Rupert configuration
		Responsible for:
			1. Locate configuration file
			2. Loading configuration
			3. Perform CRUD operations on configuration
	"""
	@beartype
	def __init__(self, config_file: str) -> None:
		"""
			Construct for RupertConfig class
			Responsible for:
				1. Locates configuration file if present
				2. Loads default configurations
				3. If config is found loads it
		"""
		self.config_file = config_file
		self.load(self.config_file)
		self.config = self.load(self.config_file)

	@beartype
	def load(self, config_file: str) -> dict:
		"""
			Description: Load the configurations
			Responsible for:
				1. Opens the config file
				2. Loads the settings
		"""
		try:
			with open(config_file, 'r', encoding='utf-8') as cfg:
				cfg_json = cfg.read()
			config = json.loads(cfg_json)
			return config
		except FileNotFoundError as fnf_exc:
			raise fnf_exc
		except json.JSONDecodeError as jde_exc:
			raise jde_exc
		except OSError as os_exc:
			raise os_exc

	@beartype
	def reload(self) -> None:
		"""
			Description: Reload the configuration
			Responsible for:
				1. Reopens the config file
				2. Loads the settings
		"""
		self.config = self.load(self.config_file)
