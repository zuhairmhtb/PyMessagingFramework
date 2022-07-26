from PyMessagingFramework.src.framework import MessagingFramework
from PyMessagingFramework.example.consumer.commands.show_message_command import ShowMessageCommand
from PyMessagingFramework.example.producer.config import BROKER_URL, BROKER_PORT, BROKER_USERNAME, BROKER_PASSWORD
from PyMessagingFramework.example.producer.config import QUEUE_NAME as MY_QUEUE
from PyMessagingFramework.example.consumer.config import QUEUE_NAME as CONSUMER_QUEUE


"""
This file contains a demo service which acts as a producer. It sends message to other services through PyMessagingFramework. 
RabbitMQ acts as the message broker between the services. This service contains the following files:

3. main.py: This is the main startup file which initializes the MessagingFramework, configures the brokers, imports the 
command class and queue name from the consumer, hooks up the command with the queue and publishes a message to the framework 
so that it is parsed correctly and forwarded to the consumer. 

4. config.py This file contains the configuration parameter of this service which might be required by other services to 
communicate with this service.
"""

if __name__ == "__main__":
    # Initialize the framework
    framework = MessagingFramework(
        broker_url=BROKER_URL, # URL of rabbiMQ
        broker_port=BROKER_PORT, # port of rabbiMQ
        broker_username=BROKER_USERNAME, # username of rabbiMQ
        broker_password=BROKER_PASSWORD, # password of rabbiMQ
        queue_name=MY_QUEUE, # Queue name of producer
        auto_delete=True  # Whether to auto delete the queue when application is stopped
    )

    # Register the command with the QUEUE of consumer to which it should be forwarded
    framework.register_commands_as_producer(command=ShowMessageCommand, routing_key=CONSUMER_QUEUE, exchange_name='')
    # Publish a command to send a message to consumer
    framework.publish_message(ShowMessageCommand(message="Hello world", number=12, array=[1, 2, 3]))
    # Close the RabbitMQ connection
    framework.close_connection()

