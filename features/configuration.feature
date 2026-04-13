Feature: Configuration Reader

  Scenario: Open a valid configuration file
    Given a valid configuration file exists
    When we are able to read the file
    Then we return it's contents as a dictionary

  Scenario: Error when opening configuration file
    Given a configuration file that does not exist
    When we are unable to open the file
    Then we want to return the exception

  Scenario: Open a configuration file with invalid syntax
    Given a configuration file with invalid syntax exists
    When we get an invalid syntax error
    Then we return the exception
