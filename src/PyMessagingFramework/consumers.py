from abc import ABC, abstractmethod
from PyMessagingFramework.src.PyMessagingFramework.utils import CommandTypes, Exchanges, BrokerMessage, Utility
import pika, json
from typing import Callable

class Consumers:
    MQ = 'rabbitmq'

    @staticmethod
    def get_consumers():
        return [Consumers.MQ]

class Consumer(ABC):

    def __init__(self,  broker_url:str, broker_port:int, broker_username:str, broker_password:str, consumer_message_callback:Callable,
                 queue_name: str = '',auto_delete: bool = False, declare_queue: bool = True,
                 non_blocking_connection:bool=False):
        """
        This is the base consumer class which creates a queue for the consumer/subscriber and executes task when a
        valid message is received from the broker. It also sends commands or publishes events to queues and exchanges of
        the message broker.

        :param broker_url: Url of the message broker
        :param broker_port: Port of the message broker
        :param broker_username: Username of the message broker
        :param broker_password: Password of the message broker
        :param consumer_message_callback: Method which is executed when queue receives a message
        :param queue_name: Name of the queue which will be used to create a queue
        :param auto_delete: Pika argument for declaring queue
        :param declare_queue: Boolean to set whether to declare a queue
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
        if type(auto_delete) != bool:
            raise  ValueError("Argument auto_delete must be boolean")
        self.auto_delete:bool = auto_delete
        if type(declare_queue) != bool:
            raise  ValueError("Argument declare_queue must be boolean")
        self.declare_queue:bool = declare_queue
        if type(non_blocking_connection) != bool:
            raise  ValueError("Argument non_blocking_connection must be boolean")
        self.non_blocking_connection:bool = non_blocking_connection
        self.consumer_message_callback = consumer_message_callback

    @abstractmethod
    def __create_connection(self, *args, **kwargs):
        """
        Creates a connection with the message broker
        :return: None
        """

    @abstractmethod
    def __create_queue_and_handle_commands(self, *args, **kwargs):
        """
        Creates a queue for the application
        :return: None
        """

    @abstractmethod
    def close_connection(self, *args, **kwargs):
        """
        Closes the connection with broker
        :return: None
        """

    @abstractmethod
    def start(self, *args, **kwargs):
        """
        Starts the main service application to start consuming data from queue
        :return: None
        """

    @abstractmethod
    def publish_message(self, data:dict, *args, **kwargs):
        """
        Converts the base command to a json string and publishes it to the appropriate routing key
        :param command: BaseCommand which is being sent
        :param command_type: Type of command being published
        :return: None
        """

    @abstractmethod
    def __handle_message(self, *args, **kwargs):
        """
        It gets executed when the queue receives a message
        :return:
        """

class MQConsumer(Consumer):
    def __init__(self, broker_url:str, broker_port:int, broker_username:str, broker_password:str, consumer_message_callback:Callable,
                 queue_name: str = '', passive: bool = False,
                 durable: bool = False, exclusive: bool = False, auto_delete: bool = False,
                 declare_queue_arguments: dict = None, declare_queue: bool = True, consume_arguments: dict = None,
                 non_blocking_connection:bool=False):
        """
        This is the base RabbitMQ class which creates a queue for the consumer/subscriber and executes task when a
        valid message is received from the broker. It also sends commands or publishes events to queues and exchanges of
        the message broker.

        :param broker_url: Url of the message broker
        :param broker_port: Port of the message broker
        :param broker_username: Username of the message broker
        :param broker_password: Password of the message broker
        :param consumer_message_callback: Method to call when message is received in queue
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
        super(MQConsumer, self).__init__(broker_url=broker_url, broker_port=broker_port, broker_username=broker_username,
                                         broker_password=broker_password, queue_name=queue_name, auto_delete=auto_delete,
                                         declare_queue=declare_queue, non_blocking_connection=non_blocking_connection,
                                         consumer_message_callback=consumer_message_callback)
        if type(passive) != bool:
            raise ValueError("Argument passive must be boolean")
        self.passive: bool = passive
        if type(durable) != bool:
            raise ValueError("Argument durable must be boolean")
        self.durable: bool = durable
        if type(exclusive) != bool:
            raise ValueError("Argument exclusive must be boolean")
        self.exclusive: bool = exclusive
        if not (declare_queue_arguments is None) and type(declare_queue_arguments) != dict:
            raise ValueError("Argument declare_queue_arguments must be dictionary or None as default")
        self.declare_queue_arguments: dict = declare_queue_arguments
        if not (consume_arguments is None) and type(consume_arguments) != dict:
            raise ValueError("Argument consume_arguments must be dictionary or None as default")
        self.consume_arguments: dict = consume_arguments

        self.connection_params: pika.ConnectionParameters = None
        self.connection: pika.BlockingConnection = None
        self.channel: pika.adapters.blocking_connection.BlockingChannel = None
        self.declare_queue_result = None

    @abstractmethod
    def __create_exchanges(self, *args, **kwargs):
        """
        Creates required exchanges
        :return:
        """

    @abstractmethod
    def publish_message(self, data:dict, routing_key:str, exchange_name:str, properties:pika.BasicProperties, *args, **kwargs):
        """
        Converts the base command to a json string and publishes it to the appropriate routing key
        :param data: The data as dictionary to send to broker
        :param routing_key: Routing key as string
        :param exchange_name: Exchange name as string
        :param properties: Pika properties
        :return: None
        """


    @abstractmethod
    def bind_queue(self, exchange_name:str, routing_key:str, *args, **kwargs):
        """
        Bind a queue to the exchange
        :param exchange_name: Exchange name to bind with
        :param routing_key: Routing key used to bind
        :return: None
        """

    def _handle_message(self, ch, method, properties, body):
        """
        Executed when queue receives a message
        :param ch: The channel data
        :param method: The method data
        :param properties: The properties of pika
        :param body: The message content as string
        :return: None
        """
        try:
            body = body.decode()
            broker_message: BrokerMessage = Utility.convert_json_to_command(json.loads(body), BrokerMessage)
            if not (broker_message is None):
                if broker_message.command_type == CommandTypes.EVENT:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                if callable(self.consumer_message_callback):
                    self.consumer_message_callback(broker_message)
                if broker_message.command_type == CommandTypes.COMMAND:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                raise ValueError("Could not parse queue data to broker message")
        except Exception as e:
            print("Received message " + body + ". Could not parse the data.")
            print(e)
            ch.basic_ack(delivery_tag=method.delivery_tag)



class MQBlockingConsumer(MQConsumer):
    def __init__(self, broker_url:str, broker_port:int, broker_username:str, broker_password:str, consumer_message_callback:Callable,
                 queue_name: str = '', passive: bool = False,
                 durable: bool = False, exclusive: bool = False, auto_delete: bool = False,
                 declare_queue_arguments: dict = None, declare_queue: bool = True, consume_arguments: dict = None,
                 non_blocking_connection:bool=False):
        """
        This is the base RabbitMQ class for blocking connection which creates a queue for the consumer/subscriber and executes task when a
        valid message is received from the broker. It also sends commands or publishes events to queues and exchanges of
        the message broker.

        :param broker_url: Url of the message broker
        :param broker_port: Port of the message broker
        :param broker_username: Username of the message broker
        :param broker_password: Password of the message broker
        :param consumer_message_callback: Method to execute when message is received in queue
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
        super(MQBlockingConsumer, self).__init__(broker_url=broker_url, broker_port=broker_port,
                                         broker_username=broker_username,
                                         broker_password=broker_password, queue_name=queue_name,
                                         auto_delete=auto_delete,
                                         declare_queue=declare_queue, non_blocking_connection=non_blocking_connection,
                                         passive=passive, durable=durable, exclusive=exclusive,
                                         declare_queue_arguments=declare_queue_arguments, consume_arguments=consume_arguments,
                                         consumer_message_callback=consumer_message_callback)

        self.__create_connection()
        self.__create_exchanges()
        self.__create_queue_and_handle_commands()



    def __create_connection(self):
        """
        Creates a connection with the message broker
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

    def __create_queue_and_handle_commands(self):
        if not (self.connection is None) and self.connection.is_open and not (self.channel is None):
            if self.declare_queue:
                self.declare_queue_result = self.channel.queue_declare(
                    queue=self.queue_name,
                    passive=self.passive,
                    durable=self.durable,
                    exclusive=self.exclusive,
                    auto_delete=self.auto_delete,
                    arguments=self.declare_queue_arguments
                )

            self.channel.basic_consume(
                queue=self.queue_name,
                auto_ack=False,
                arguments=self.consume_arguments,
                on_message_callback=lambda ch, method, properties, body : self._handle_message(ch, method, properties, body)
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

    def publish_message(self, data:dict, routing_key:str, properties:pika.BasicProperties, exchange_name:str):
        """
        Converts the base command to a json string and publishes it to the appropriate routing key
        :param data: The data as dictionary to send to broker
        :param routing_key: Routing key as string
        :param exchange_name: Exchange name as string
        :param properties: Pika properties
        :return: None
        """
        self.channel.basic_publish(
            routing_key=routing_key,
            properties=properties,
            body=json.dumps(data),
            exchange=exchange_name
        )

    def bind_queue(self, exchange_name:str, routing_key:str):
        """
        Bind a queue to the exchange
        :param exchange_name: Exchange name to bind with
        :param routing_key: Routing key used to bind
        :return: None
        """
        if not (self.channel is None) and not self.channel.is_closed:
            if type(exchange_name) == str and (
                    len(exchange_name) == 0 or exchange_name in Exchanges.get_exchange_keys()):
                self.channel.queue_bind(exchange=exchange_name, routing_key=routing_key, queue=self.queue_name)
            else:
                raise ValueError(
                    f"Could not register as invalid exchange key '{exchange_name}' was provided. It must be one of: {','.join(Exchanges.get_exchange_keys())}")