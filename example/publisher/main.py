from PyMessagingFramework_zuhairmhtb.framework import MessagingFramework, CommandTypes
from PyMessagingFramework.example.publisher.event.message_processed_event import DirectMessageProcessedEvent, FanoutMessageProcessedEvent, TopicMessageProcessedEvent
from PyMessagingFramework.example.publisher.config import BROKER_URL, BROKER_PORT, BROKER_USERNAME, BROKER_PASSWORD
from PyMessagingFramework.example.publisher.config import QUEUE_NAME as MY_QUEUE


"""
This file contains a demo service which acts as a publisher. It sends events to other services through PyMessagingFramework. 
RabbitMQ acts as the message broker between the services. This service contains the following files:

3. main.py: This is the main startup file which initializes the MessagingFramework, configures the brokers, imports the 
event class and exchange name, hooks up the event with the exchange and a routing key, and publishes an event to the framework 
so that it is parsed correctly and forwarded to the subscribers. 

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

    # Register the event with the Exchange so that subscribers can subscribe to the exchange and listen for the message

    # Register an event which is sent as a direct message to MQ
    framework.register_events_as_producer(
        event=DirectMessageProcessedEvent,
        exchange_name=DirectMessageProcessedEvent.EXCHANGE_NAME,
        routing_key=DirectMessageProcessedEvent.ROUTING_KEY
    )

    # Register an event which is sent as a fanout message to MQ
    framework.register_events_as_producer(
        event=FanoutMessageProcessedEvent,
        exchange_name=FanoutMessageProcessedEvent.EXCHANGE_NAME,
        routing_key=FanoutMessageProcessedEvent.ROUTING_KEY
    )

    # Register an event which is sent as a topic message to MQ
    framework.register_events_as_producer(
        event=TopicMessageProcessedEvent,
        exchange_name=TopicMessageProcessedEvent.EXCHANGE_NAME,
        routing_key=TopicMessageProcessedEvent.ROUTING_KEY
    )

    # Publish the event to send a message to subscribers
    framework.publish_message(
        command=DirectMessageProcessedEvent(message_content="This is a direct event message", message_value=12, message_array=[1, 2, 3]),
        command_type=CommandTypes.EVENT
    )
    framework.publish_message(
        command=FanoutMessageProcessedEvent(message_content="This is a fanout event message", message_value=12, message_array=[1, 2, 3]),
        command_type=CommandTypes.EVENT
    )
    framework.publish_message(
        command=TopicMessageProcessedEvent(message_content="This is a topic event message", message_value=12, message_array=[1, 2, 3]),
        command_type=CommandTypes.EVENT
    )

    # Close the RabbitMQ connection
    framework.close_connection()

