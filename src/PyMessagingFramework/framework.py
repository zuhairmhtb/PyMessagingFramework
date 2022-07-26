from typing import Type, List, Dict
import pika, json, inspect

class BaseCommand:
    """
    This is the base command class. All commands must extend this class
    """

class BaseCommandHandler:
    """
    This is the base command handler class. All Handlers must extend this class
    """
    def handle(self, command:BaseCommand):
        """
        Base command handler class that recieves a command and executes a task
        :param command: The Base command object containing the command data
        :return:
        """
        pass

class ConsumerCommandContainer:
    """
    This is the container which holds the command and handler class references for the consumer so that if a command of
    this type appears, the message is parsed and passed to the corresponding handler
    """
    def __init__(self, command:Type[BaseCommand], handler:Type[BaseCommandHandler]):
        self.command:Type[BaseCommand] = command
        self.handler:Type[BaseCommandHandler] = handler

class ConsumerEventContainer:
    """
    This is the container which holds the event and handler class references for the consumer so that if an event of
    this type appears, the message is parsed and passed to the corresponding handler
    """
    def __init__(self, event: Type[BaseCommand], handler: Type[BaseCommandHandler]):
        """
        :param event: The event class reference
        :param handler: The event handler class reference
        """
        self.event: Type[BaseCommand] = event
        self.handler: Type[BaseCommandHandler] = handler

class ProducerCommandContainer:
    """
        This is the container which holds the command class reference, routing key and properties so that if a producer
        publishes this command, it is parsed to json and published to the appropriate routing key
        """
    def __init__(self, command:Type[BaseCommand], routing_key:str, exchange_name:str, properties:pika.BasicProperties):
        self.command:Type[BaseCommand] = command
        self.routing_key:str = routing_key
        self.exchange_name:str = exchange_name
        self.properties:pika.BasicProperties = properties

class ProducerEventContainer:
    """
    This is the container which holds the event class reference, exchange key, and routing key and properties so that if a producer
    publishes this command, it is parsed to json and published to the appropriate routing key and exchange
    """
    def __init__(self, event: Type[BaseCommand], exchange_type:str, exchange_name:str, routing_key:str, properties:pika.BasicProperties):
        """
        :param event: The event class reference
        :param exchange_name: The exchange to which the queue listens for the event
        :param routing_key: The routing key which is used
        :param properties: Pika properties
        """
        self.event: Type[BaseCommand] = event
        self.exchange_name:str = exchange_name
        self.routing_key:str = routing_key
        self.exchange_type = exchange_type
        self.properties:pika.BasicProperties = properties


class CommandTypes:
    COMMAND = "command"
    EVENT = "event"

class BrokerMessage:
    """
    This is the message which is sent to the broker. The MessagingFramework receives this message from the broker, idetifies the
    command name and calls the appropriate handler (if it exists) by parsing the json message to command object and passing
    it to the handler
    """
    def __init__(self, message:str, command_name:str, command_type:str=CommandTypes.COMMAND):
        """
        :param message: The message as json string
        :param command_name: The command name as string
        :param command_type: The type of command being sent or received: command or event
        """
        self.message:str = message
        self.command_name:str = command_name
        self.command_type:str = command_type

class Utility:
    @staticmethod
    def convert_command_to_json(command:BaseCommand) -> dict:
        """
        This method converts a command object to dictionary
        :param command: BaseCommand object
        :return: Dictionary
        """
        return json.loads(json.dumps(vars(command)))

    @staticmethod
    def convert_json_to_command(command:dict, command_type:Type[BaseCommand]) -> BaseCommand:
        """
        This method converts a json object to the corresponding command
        :param command: The dictionary which needs to be converted to object
        :param command_type: The class reference to which the command must be converted
        :return: Command object
        """
        return command_type(**command)

class Exchange:
    def __init__(self, name:str, exchange_type:str):
        self.name:str = name
        self.exchange_type:str = exchange_type

class Exchanges:
    DIRECT = Exchange(name='direct_exchange', exchange_type='direct')
    FANOUT = Exchange(name='fanout_exchange', exchange_type='fanout')
    TOPIC = Exchange(name='topic_exchange', exchange_type='topic')

    @staticmethod
    def get_exchange_keys() -> List[str]:
        return [Exchanges.DIRECT.name, Exchanges.FANOUT.name, Exchanges.TOPIC.name]

    @staticmethod
    def get_exchanges_as_dict() -> Dict[str, str]:
        return {
            Exchanges.DIRECT.name : Exchanges.DIRECT,
            Exchanges.FANOUT.name : Exchanges.FANOUT,
            Exchanges.TOPIC.name : Exchanges.TOPIC
        }

class MessagingFramework:

    def __init__(self,
                 broker_url:str, broker_port:int, broker_username:str, broker_password:str,
                 queue_name: str = '', passive: bool = False,
                 durable: bool = False, exclusive: bool = False, auto_delete: bool = False,
                 declare_queue_arguments: dict = None, declare_queue: bool = True, consume_arguments: dict = None,
                 non_blocking_connection:bool=False
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
        """
        if type(broker_url) != str or len(broker_url) == 0:
            raise  ValueError("The broker url must be a string")
        self.broker_url:str = broker_url
        if type(broker_port) != int or broker_port <= 0:
            raise  ValueError("The broker port must be an integer")
        self.broker_port:int = broker_port
        if type(broker_username) != str:
            raise ValueError("The broker username must be a string")
        self.broker_username:str = broker_username
        if type(broker_password) != str:
            raise ValueError("The broker password must be a string")
        self.broker_password:str = broker_password
        if type(queue_name) != str:
            raise ValueError("Queue name must be a string")
        self.queue_name:str = queue_name
        if type(passive) != bool:
            raise  ValueError("Argument passive must be boolean")
        self.passive:bool = passive
        if type(durable) != bool:
            raise  ValueError("Argument durable must be boolean")
        self.durable:bool = durable
        if type(exclusive) != bool:
            raise  ValueError("Argument exclusive must be boolean")
        self.exclusive:bool = exclusive
        if type(auto_delete) != bool:
            raise  ValueError("Argument auto_delete must be boolean")
        self.auto_delete:bool = auto_delete
        if not (declare_queue_arguments is None) and type(declare_queue_arguments) != dict:
            raise  ValueError("Argument declare_queue_arguments must be dictionary or None as default")
        self.declare_queue_arguments:dict = declare_queue_arguments
        if type(declare_queue) != bool:
            raise  ValueError("Argument declare_queue must be boolean")
        self.declare_queue:bool = declare_queue
        if not (consume_arguments is None) and type(consume_arguments) != dict:
            raise  ValueError("Argument consume_arguments must be dictionary or None as default")
        self.consume_arguments:dict = consume_arguments
        if type(non_blocking_connection) != bool:
            raise  ValueError("Argument non_blocking_connection must be boolean")
        self.non_blocking_connection:bool = non_blocking_connection


        self.connection_params:pika.ConnectionParameters = None
        self.connection:pika.BlockingConnection = None
        self.channel:pika.adapters.blocking_connection.BlockingChannel = None
        self.declare_queue_result = None

        self.producer_commands:dict[str, ProducerCommandContainer] = {}
        self.consumer_commands:dict[str, ConsumerCommandContainer] = {}
        self.producer_events:dict[str, ProducerEventContainer] = {}
        self.consumer_events:dict[str, ConsumerEventContainer]  = {}

        self.__create_connection(non_blocking=self.non_blocking_connection)
        self.__create_exchanges()
        self.__create_queue_and_handle_commands(
            queue_name=self.queue_name, passive=self.passive, durable=self.durable,
            exclusive=self.exclusive, auto_delete=self.auto_delete, declare_queue_arguments=self.declare_queue_arguments,
            declare_queue=self.declare_queue, consume_arguments=self.consume_arguments
        )
    # RabbitMQ configuration and management related methods

    def __create_connection(self, non_blocking:bool):
        """
        Creates a connection with the message broker
        :param non_blocking: Boolean to set whether to create a blocking connection or not
        :return: None
        """
        if not (self.connection is None) and self.connection.is_open:
            print("A connection is already open. Closing the existing connection and creating a new one")
            self.close_connection()

        credentials = None
        if len(self.broker_username) > 0 and len(self.broker_password) > 0:
            credentials = pika.PlainCredentials(username=self.broker_username, password=self.broker_password)
        self.connection_params = pika.ConnectionParameters(
            port=self.broker_port,
            host=self.broker_url,
            credentials=credentials
        )
        self.connection = pika.BlockingConnection(self.connection_params)
        self.channel = self.connection.channel()


    def __create_exchanges(self):
        if not (self.channel is None) and not self.channel.is_closed:
            self.channel.exchange_declare(exchange=Exchanges.DIRECT.name, exchange_type=Exchanges.DIRECT.exchange_type)
            self.channel.exchange_declare(exchange=Exchanges.FANOUT.name, exchange_type=Exchanges.FANOUT.exchange_type)
            self.channel.exchange_declare(exchange=Exchanges.TOPIC.name, exchange_type=Exchanges.TOPIC.exchange_type)

    def __create_queue_and_handle_commands(self, queue_name:str, passive:bool,
                                  durable:bool, exclusive:bool, auto_delete:bool,
                                  declare_queue_arguments:dict, declare_queue:bool, consume_arguments:dict):
        """
        Creates a queue for the application
        :param queue_name: Name of the queue which will be used to create a queue
        :param passive: Pika argument for declaring queue
        :param durable: Pika argument for declaring queue
        :param exclusive: Pika argument for declaring queue
        :param auto_delete: Pika argument for declaring queue
        :param declare_queue_arguments: Pika argument for declaring queue
        :param declare_queue: Boolean to set whether to declare a queue
        :param consume_arguments: Pika argument for consuming queue
        :return: None
        """
        if not (self.connection is None) and self.connection.is_open and not (self.channel is None):
            if declare_queue:
                self.declare_queue_result = self.channel.queue_declare(
                    queue=queue_name,
                    passive=passive,
                    durable=durable,
                    exclusive=exclusive,
                    auto_delete=auto_delete,
                    arguments=declare_queue_arguments
                )

            self.channel.basic_consume(
                queue=queue_name,
                auto_ack=False,
                arguments=consume_arguments,
                on_message_callback=lambda ch, method, properties, body : self.handle_queue_message(ch, method, properties, body)
            )

    def close_connection(self):
        """
        Closes the connection with broker
        :return: None
        """
        if not (self.connection is None) and self.connection.is_open:
            self.connection.close()

    def start(self):
        """
        Starts the main service application to start consuming data from queue
        :return: None
        """
        if not (self.channel is None) and not (self.connection is None) and self.connection.is_open:
            self.channel.start_consuming()
        else:
            raise ValueError("A connection with the message broker does not exist")

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
                if type(exchange_name) == str and (len(exchange_name) == 0 or exchange_name in Exchanges.get_exchange_keys()):
                    event_name = event.__name__
                    if not (event_name in self.consumer_events):
                        self.consumer_events[event_name] = ConsumerEventContainer(event=event, handler=handler)
                        try:
                            if not (self.channel is None):
                                self.channel.queue_bind(exchange=exchange_name, routing_key=routing_key, queue=self.queue_name)
                        except:
                            pass
                else:
                    print(f"Could not register as invalid exchange key '{exchange_name}' was provided. It must be one of: {','.join(Exchanges.get_exchange_keys())}")


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


    def handle_queue_command(self, broker_message:BrokerMessage):
        """
        Handles commands sent to the queue
        :param self: The MessagingFramework object
        :param broker_message: Message sent by the broker
        :return: None
        """
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


    def handle_queue_event(self, broker_message:BrokerMessage):
        """
        Handles events sent to the queue
        :param self: The MessagingFramework object
        :param broker_message: Message sent by the broker
        :return: None
        """
        if broker_message.command_name in self.consumer_events:
            event = Utility.convert_json_to_command(
                command=json.loads(broker_message.message),
                command_type=self.consumer_events[broker_message.command_name].event
            )
            if not (self.consumer_events[broker_message.command_name].handler is None):
                self.consumer_events[broker_message.command_name].handler().handle(event)
            else:
                print(f"Handler is not provided to {broker_message.command_name} is not found in the consumer event list")




    def handle_queue_message(self, ch, method, properties, body):
        """
        The main method which receives messages from queue and passes it to appropriate handlers.
        :param self: The MessagingFramework object
        :param ch: Channel information
        :param method: Method information
        :param properties: Pika properties
        :param body: Main message body as byte string
        :return: None
        """
        try:
            body = body.decode()
            broker_message:BrokerMessage = Utility.convert_json_to_command(json.loads(body), BrokerMessage)
            if broker_message.command_type == CommandTypes.COMMAND:
                self.handle_queue_command(broker_message=broker_message)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            elif broker_message.command_type == CommandTypes.EVENT:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self.handle_queue_event(broker_message=broker_message)
            else:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                print(f"Could not recognize the message received from the broker. Message type provided: {broker_message.command_type}")
        except Exception as e:
            print("Received message " + body + ". Could not parse the data.")
            print(e)
            ch.basic_ack(delivery_tag=method.delivery_tag)

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
                        self.channel.basic_publish(
                            routing_key=selected_list[command_name].routing_key,
                            properties=selected_list[command_name].properties,
                            body=json.dumps(
                                Utility.convert_command_to_json(
                                    BrokerMessage(message=json.dumps(json_command), command_name=command_name,
                                                  command_type=command_type)
                                )
                            ),
                            exchange=selected_list[command_name].exchange_name
                        )
                    else:
                        print(f"Could not publish message as the command {command_name} is not registered")



