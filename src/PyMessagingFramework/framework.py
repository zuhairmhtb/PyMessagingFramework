from typing import Type, List, Dict
import pika, json, inspect
from PyMessagingFramework.utils import BaseCommand, BaseCommandHandler
from PyMessagingFramework.utils import ConsumerCommandContainer, ConsumerEventContainer
from PyMessagingFramework.utils import ProducerCommandContainer, ProducerEventContainer
from PyMessagingFramework.utils import CommandTypes, BrokerMessage, Utility, Exchanges
from PyMessagingFramework.consumers import Consumers, Consumer, MQConsumer
from PyMessagingFramework.consumers import MQBlockingConsumer, MQNonBlockingConsumer


class MessagingFramework:

    def __init__(self,
                 broker_url:str, broker_port:int, broker_username:str, broker_password:str,
                 queue_name: str = '', passive: bool = False,
                 durable: bool = False, exclusive: bool = False, auto_delete: bool = False,
                 declare_queue_arguments: dict = None, declare_queue: bool = True, consume_arguments: dict = None,
                 non_blocking_connection:bool=False, broker:str=Consumers.MQ
                 ):
        """
        This is the main MessagingFramework class which creates a queue for the consumer/subscriber and executes task when a
        valid message is received from the broker. It also sends commands or publishes events to queues and exchanges of
        the message broker.

        :param broker_url: Url of the message broker
        :param broker_port: Port of the message broker
        :param broker_username: Username of the message broker
        :param broker_password: Password of the message broker
        :param queue_name: Name of the queue which will be used to create a queue
        :param passive: Pika argument for declaring queue
        :param durable: Pika argument for declaring queue
        :param exclusive: Pika argument for declaring queue
        :param auto_delete: Pika argument for declaring queue
        :param declare_queue_arguments: Pika argument for declaring queue
        :param declare_queue: Boolean to set whether to declare a queue
        :param consume_arguments: Pika argument for consuming queue
        :param non_blocking_connection: Whether the connection established will be non blocking
        :param broker: Which broker to use
        """
        if type(non_blocking_connection) != bool:
            raise  ValueError("Argument non_blocking_connection must be boolean")
        self.non_blocking_connection:bool = non_blocking_connection
        if type(broker) != str or not (broker in Consumers.get_consumers()):
            print("Broker not recognized. Falling back to RabbitMQ as default")
            broker = Consumers.MQ

        self.broker:Consumer = None
        if broker == Consumers.MQ:
            if not self.non_blocking_connection:
                self.broker = MQBlockingConsumer(
                    broker_url=broker_url, broker_port=broker_port,
                    broker_username=broker_username, broker_password=broker_password,
                    queue_name=queue_name, passive=passive, durable=durable, exclusive=exclusive,
                    auto_delete=auto_delete, declare_queue_arguments=declare_queue_arguments, declare_queue=declare_queue,
                    consume_arguments=consume_arguments, non_blocking_connection=non_blocking_connection,
                    consumer_message_callback=self.handle_queue_message
                )
            else:
                self.broker = MQNonBlockingConsumer(
                    broker_url=broker_url, broker_port=broker_port,
                    broker_username=broker_username, broker_password=broker_password,
                    queue_name=queue_name, passive=passive, durable=durable, exclusive=exclusive,
                    auto_delete=auto_delete, declare_queue_arguments=declare_queue_arguments,
                    declare_queue=declare_queue,
                    consume_arguments=consume_arguments, non_blocking_connection=non_blocking_connection,
                    consumer_message_callback=self.handle_queue_message
                )

        self.producer_commands:dict[str, ProducerCommandContainer] = {}
        self.consumer_commands:dict[str, ConsumerCommandContainer] = {}
        self.producer_events:dict[str, ProducerEventContainer] = {}
        self.consumer_events:dict[str, ConsumerEventContainer]  = {}

    # Command and Event registration related methods
    def register_commands_as_consumer(self, command:Type[BaseCommand], handler:Type[BaseCommandHandler]):
        """
        Hooks up the command with command handler so that the handler is called when the command is consumed from the
        queue
        @param command: Class reference of the command which extends BaseCommand
        @param handler: Class reference of the command handler which extends BaseCommandHandler
        :return: None
        """
        if inspect.isclass(command) and inspect.isclass(handler):
            if issubclass(command, BaseCommand) and issubclass(handler, BaseCommandHandler):
                command_name = command.__name__
                if not (command_name in self.consumer_commands):
                    self.consumer_commands[command_name] = ConsumerCommandContainer(command=command, handler=handler)

    def register_events_as_consumer(self, event:Type[BaseCommand], handler:Type[BaseCommandHandler], exchange_name:str='', routing_key:str=''):
        """
        Hooks up the events with event handler so that the handler is called when the event is consumed from the
        queue
        @param event: Class reference of the event which extends BaseCommand
        @param handler: Class reference of the event handler which extends BaseCommandHandler
        @param exchange_name: The exchange name to which the handler will listen
        @param routing_key: The routing key used
        :return: None
        """
        if inspect.isclass(event) and inspect.isclass(handler):
            if issubclass(event, BaseCommand) and issubclass(handler, BaseCommandHandler):
                event_name = event.__name__
                if not (event_name in self.consumer_events):
                    self.consumer_events[event_name] = ConsumerEventContainer(event=event, handler=handler)
                    try:
                        if isinstance(self.broker, MQConsumer):
                            self.broker.bind_queue(exchange_name=exchange_name, routing_key=routing_key)
                    except Exception as e:
                        print(e)


    def register_commands_as_producer(self, command:Type[BaseCommand], exchange_name:str='', routing_key:str='', properties:pika.BasicProperties=None):
        """
        Hooks up the command with routing key and properties so that when the command is published, the message is sent
        to the appropriate routing key of the broker.
        :param command: Class reference of the command which extends the base command
        :param exchange_name: Name of the exchange to use. Empty is default exchange
        :param routing_key: Routing key as string to use for the sending the command
        :param properties: pika properties to use when publishing the command
        :return: None
        """
        if inspect.isclass(command):
            if issubclass(command, BaseCommand):
                command_name = command.__name__
                if not (command_name in self.producer_commands):
                    self.producer_commands[command_name] = ProducerCommandContainer(command=command, routing_key=routing_key, exchange_name=exchange_name, properties=properties)


    def register_events_as_producer(self, event:Type[BaseCommand], exchange_name:str='', routing_key:str='' , properties:pika.BasicProperties=None):
        """
        Hooks up the events with the routing key and exchange name so that when the event is published, the message is
        sent to the appropriate exchange with the appropriate routing key
        :param event: Class reference of the events which extend the base command
        :param exchange_name: The exchange name to which the event will be sent
        :param routing_key: Routing key which will be attached to the event
        :param properties: pika properties to use when publishing the command
        :return: None
        """
        if inspect.isclass(event):
            if issubclass(event, BaseCommand):
                event_name = event.__name__
                if not (event_name in self.producer_events):
                    exchanges = Exchanges.get_exchanges_as_dict()
                    if type(exchange_name) == str and (len(exchange_name) == 0 or (exchange_name in exchanges)):
                        self.producer_events[event_name] = ProducerEventContainer(event=event, exchange_name=exchange_name,
                                                                                  routing_key=routing_key,
                                                                                  exchange_type=exchanges[exchange_name].exchange_type,
                                                                                  properties=properties
                                                                                  )
                        print(f"Registered {event_name}")
                    else:
                        print(f"Could not register event {event_name} as the exchange key {exchange_name} is invalid")

    # Methods to handle incoming message
    def handle_queue_message(self, broker_message:BrokerMessage):
        """
        The main method which receives messages from queue and passes it to appropriate handlers.
        :param broker_message: The message is received from queue and parsed to object
        :return: None
        """
        if broker_message.command_type == CommandTypes.COMMAND:
            if broker_message.command_name in self.consumer_commands:
                command = Utility.convert_json_to_command(command=json.loads(broker_message.message),
                                                          command_type=self.consumer_commands[
                                                              broker_message.command_name].command)
                if not (self.consumer_commands[broker_message.command_name].handler is None):
                    self.consumer_commands[broker_message.command_name].handler().handle(command)
                else:
                    print(f"Handler is not provided to {broker_message.command_names}")
            else:
                print(f"Command name {broker_message.command_name} is not found in the consumer command list")
        elif broker_message.command_type == CommandTypes.EVENT:
            if broker_message.command_name in self.consumer_events:
                event = Utility.convert_json_to_command(
                    command=json.loads(broker_message.message),
                    command_type=self.consumer_events[broker_message.command_name].event
                )
                if not (self.consumer_events[broker_message.command_name].handler is None):
                    self.consumer_events[broker_message.command_name].handler().handle(event)
                else:
                    print(
                        f"Handler is not provided to {broker_message.command_name} is not found in the consumer event list")
        else:
            raise ValueError(
                f"Could not recognize the message received from the broker. Message type provided: {broker_message.command_type}")

    # Methods to handle outgoing message
    def publish_message(self, command:BaseCommand, command_type:str=CommandTypes.COMMAND):
        """
        Converts the base command to a json string and publishes it to the appropriate routing key
        :param command: BaseCommand which is being sent
        :param command_type: Type of command being published
        :return: None
        """
        if isinstance(command, BaseCommand):
            command_name = type(command).__name__
            json_command = None
            try:
                json_command = Utility.convert_command_to_json(command)
            except TypeError as e:
                print("Cannot publish the message as the command could not be serialized to a jon object")
                print(e)
            if type(json_command) == dict:
                selected_list = None
                if command_type == CommandTypes.COMMAND:
                    selected_list = self.producer_commands
                elif command_type == CommandTypes.EVENT:
                    selected_list = self.producer_events
                if not (selected_list is None):
                    if command_name in selected_list:
                        if isinstance(self.broker, MQConsumer):
                            self.broker.publish_message(
                                data=Utility.convert_command_to_json(
                                        BrokerMessage(message=json.dumps(json_command), command_name=command_name,
                                                      command_type=command_type)
                                    ),
                                exchange_name=selected_list[command_name].exchange_name,
                                routing_key=selected_list[command_name].routing_key,
                                properties=selected_list[command_name].properties
                            )
                    else:
                        print(f"Could not publish message as the command {command_name} is not registered")

    # Methods to manage framework
    def start(self):
        self.broker.start()

    def close_connection(self):
        self.broker.stop()



