Feature: Kafka Admin Client, Producer, and Consumer Functionality

	Scenario: Kafka admin client to create topics
		Given a Kafka admin client is set up
		When we create a new topic
		Then the topic should be successfully created in the Kafka cluster
	
	Scenario: Kafka admin client to delete topics
		Given a Kafka admin client is set up
		When we delete an existing topic
		Then the topic should be successfully deleted from the Kafka cluster

	Scenario: Kafka admin client can list topics
		Given a Kafka admin client is set up
		When we list the Kafka topics
		Then we should receive a dictionary of topics

	Scenario: Kafka admin client handles list topic failures
		Given a Kafka admin client is set up
		When topic listing fails
		Then the Kafka admin client should exit with an error code

	Scenario: Kafka producer and consumer can send and receive messages
		Given a Kafka producer and consumer are set up
		When the producer sends a message to a topic
		Then the consumer should receive the message from the topic
