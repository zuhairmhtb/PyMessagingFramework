from PyMessagingFramework.src.framework import MessagingFramework
from PyMessagingFramework.example.consumer.commands.show_message_command import ShowMessageCommand
from PyMessagingFramework.example.consumer.handlers.show_message_handler import ShowMessageHandler
from PyMessagingFramework.example.consumer.config import BROKER_URL, BROKER_PORT, BROKER_USERNAME, BROKER_PASSWORD
from PyMessagingFramework.example.consumer.config import QUEUE_NAME as MY_QUEUE

"""
This file contains a demo service which acts as a consumer. It receives message from other service through PyMessagingFramework 
and performs some task. RabbitMQ acts as the message broker  between the service. This service contains the following files:

1. commands/show_message_command: This file contains a command which is received by the consumer from other service.

2. handlers/show_message_handler: This file contains the handler method which is executed when another service sends the 
ShowMessageCommand command to this service.

3. main.py: This is the main startup file which initializes the MessagingFramework, configures the brokers, hooks up the 
command with the handler and starts the framework so that it listens for messages.

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
        queue_name=MY_QUEUE, # Queue name of consumer,
        auto_delete=True # Whether to auto delete the queue when application is stopped
    )

    # Register the command with the QUEUE to which it should be forwarded
    framework.register_commands_as_consumer(command=ShowMessageCommand, handler=ShowMessageHandler)
    # Start the application to start listening for messages
    print("Starting application")
    framework.start()