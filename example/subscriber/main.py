from PyMessagingFramework_zuhairmhtb.framework import MessagingFramework
from PyMessagingFramework.example.publisher.event.message_processed_event import DirectMessageProcessedEvent, FanoutMessageProcessedEvent, TopicMessageProcessedEvent
from PyMessagingFramework.example.subscriber.handlers.message_processed_handler import DirectMessageProcessedHandler, FanoutMessageProcessedHandler, TopicMessageProcessedHandler
from PyMessagingFramework.example.publisher.config import BROKER_URL, BROKER_PORT, BROKER_USERNAME, BROKER_PASSWORD
from PyMessagingFramework.example.publisher.config import QUEUE_NAME as MY_QUEUE


"""
This file contains a demo service which acts as a subscriber. It receives events from other services through PyMessagingFramework. 
RabbitMQ acts as the message broker between the services. This service contains the following files:

3. main.py: This is the main startup file which initializes the MessagingFramework, configures the brokers, imports the 
event class, routing key and exchange name, hooks up the event with the routing key, and consumes the event through the framework 
so that it is parsed correctly. 

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
    framework.register_events_as_consumer(
        event=DirectMessageProcessedEvent,
        handler=DirectMessageProcessedHandler,
        routing_key=DirectMessageProcessedEvent.ROUTING_KEY,
        exchange_name=DirectMessageProcessedEvent.EXCHANGE_NAME
    )

    # Register an event which is sent as a fanout message to MQ
    framework.register_events_as_consumer(
        event=FanoutMessageProcessedEvent,
        handler=FanoutMessageProcessedHandler,
        routing_key=FanoutMessageProcessedEvent.ROUTING_KEY,
        exchange_name=FanoutMessageProcessedEvent.EXCHANGE_NAME
    )

    # Register an event which is sent as a topic message to MQ
    framework.register_events_as_consumer(
        event=TopicMessageProcessedEvent,
        handler=TopicMessageProcessedHandler,
        routing_key='*.topic.*',
        exchange_name=TopicMessageProcessedEvent.EXCHANGE_NAME
    )

    # Start the subscriber and listen for events
    print("Starting to listen for events")
    framework.start()
