from abc import ABC, abstractmethod
from PyMessagingFramework.utils import CommandTypes, Exchanges, BrokerMessage, Utility
import pika, json, functools, logging
from typing import Callable, List


LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

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
    def create_connection(self, *args, **kwargs):
        """
        Creates a connection with the message broker
        :return: None
        """

    @abstractmethod
    def create_queue_and_handle_commands(self, *args, **kwargs):
        """
        Creates a queue for the application
        :return: None
        """

    @abstractmethod
    def stop(self, *args, **kwargs):
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
    def handle_message(self, *args, **kwargs):
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
    def create_exchanges(self, *args, **kwargs):
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

    def handle_message(self, ch, method, properties, body):
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
            body = json.loads(body)
            print(type(body))
            broker_message: BrokerMessage = Utility.convert_json_to_command(command=body, command_type=BrokerMessage)
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

        self.create_connection()
        self.create_exchanges()
        self.create_queue_and_handle_commands()



    def create_connection(self):
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

    def create_exchanges(self):
        if not (self.channel is None) and not self.channel.is_closed:
            self.channel.exchange_declare(exchange=Exchanges.DIRECT.name, exchange_type=Exchanges.DIRECT.exchange_type)
            self.channel.exchange_declare(exchange=Exchanges.FANOUT.name, exchange_type=Exchanges.FANOUT.exchange_type)
            self.channel.exchange_declare(exchange=Exchanges.TOPIC.name, exchange_type=Exchanges.TOPIC.exchange_type)

    def create_queue_and_handle_commands(self):
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
                on_message_callback=lambda ch, method, properties, body : self.handle_message(ch=ch, method=method, properties=properties, body=body)
            )

    def stop(self):
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


class MQNonBlockingQueueBinding:
    def __init__(self, exchange_name:str, routing_key:str):
        self.exchange_name = exchange_name
        self.routing_key = routing_key

class MQNonBlockingConsumer(MQConsumer):
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
        super(MQNonBlockingConsumer, self).__init__(broker_url=broker_url, broker_port=broker_port,
                                         broker_username=broker_username,
                                         broker_password=broker_password, queue_name=queue_name,
                                         auto_delete=auto_delete,
                                         declare_queue=declare_queue, non_blocking_connection=non_blocking_connection,
                                         passive=passive, durable=durable, exclusive=exclusive,
                                         declare_queue_arguments=declare_queue_arguments, consume_arguments=consume_arguments,
                                         consumer_message_callback=consumer_message_callback)

        self.should_reconnect = False
        self.was_consuming = False
        self._closing = False
        self._consumer_tag = None
        self._consuming = False
        # In production, experiment with higher prefetch values
        # for higher consumer throughput
        self._prefetch_count = 1

        self.queue_bindings:List[MQNonBlockingQueueBinding] = []

    def create_connection(self):
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
        # Create connection, channel, exchanges
        self.connection = pika.SelectConnection(
            parameters=self.connection_params,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)


    def close_connection(self):
        self._consuming = False
        if self.connection.is_closing or self.connection.is_closed:
            LOGGER.info('Connection is closing or already closed')
        else:
            LOGGER.info('Closing connection')
            self.connection.close()

    def on_connection_open(self, _unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.
        :param pika.SelectConnection _unused_connection: The connection
        """
        LOGGER.info('Connection opened')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.
        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error
        """
        LOGGER.error('Connection open failed: %s', err)

    def on_connection_closed(self, _unused_connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.
        :param pika.connection.Connection connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.
        """
        self._channel = None
        if self._closing:
            self.connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reconnect necessary: %s', reason)

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.
        """
        LOGGER.info('Creating a new channel')
        self.connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.
        Since the channel is now open, we'll declare the exchange to use.
        :param pika.channel.Channel channel: The channel object
        """
        LOGGER.info('Channel opened')
        self.channel = channel
        self.channel.add_on_close_callback(self.on_channel_closed)
        self.create_exchanges()


    def on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.
        :param pika.channel.Channel: The closed channel
        :param Exception reason: why the channel was closed
        """
        LOGGER.warning('Channel %i was closed: %s', channel, reason)
        self.close_connection()

    def create_exchanges(self):
        if not (self.channel is None) and not self.channel.is_closed:
            self.setup_exchange(exchange_name=Exchanges.DIRECT.name, exchange_type=Exchanges.DIRECT.exchange_type)
            self.setup_exchange(exchange_name=Exchanges.FANOUT.name, exchange_type=Exchanges.FANOUT.exchange_type)
            self.setup_exchange(exchange_name=Exchanges.TOPIC.name, exchange_type=Exchanges.TOPIC.exchange_type)

    def setup_exchange(self, exchange_name, exchange_type):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.
        :param str|unicode exchange_name: The name of the exchange to declare
        """
        LOGGER.info('Declaring exchange: %s', exchange_name)
        # Note: using functools.partial is not required, it is demonstrating
        # how arbitrary data can be passed to the callback when it is called
        cb = functools.partial(
            self.on_exchange_declareok, userdata=exchange_name)
        self.channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=exchange_type,
            callback=cb)

    def on_exchange_declareok(self, _unused_frame, userdata):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.
        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame
        :param str|unicode userdata: Extra user data (exchange name)
        """
        LOGGER.info('Exchange declared: %s', userdata)

        self.create_queue_and_handle_commands()

    def create_queue_and_handle_commands(self):
        if not (self.connection is None) and self.connection.is_open and not (self.channel is None):
            if self.declare_queue:
                LOGGER.info('Declaring queue %s', self.queue_name)
                self.declare_queue_result = self.channel.queue_declare(
                    queue=self.queue_name,
                    passive=self.passive,
                    durable=self.durable,
                    exclusive=self.exclusive,
                    auto_delete=self.auto_delete,
                    arguments=self.declare_queue_arguments,
                    callback=self.on_queue_declareok
                )

    def on_queue_declareok(self, _unused_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.
        :param pika.frame.Method _unused_frame: The Queue.DeclareOk frame
        :param str|unicode userdata: Extra user data (queue name)
        """
        if type(self.queue_bindings) == list:
            for binding in self.queue_bindings:
                if type(binding) == MQNonBlockingQueueBinding:
                    self.channel.queue_bind(
                        queue=self.queue_name,
                        exchange=binding.exchange_name,
                        routing_key=binding.routing_key
                    )

        self.start_consuming()

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.
        """
        LOGGER.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self.channel.basic_consume(
            self.queue_name, lambda ch, method, properties, body: self.handle_message(ch, method, properties, body))
        self.was_consuming = True
        self._consuming = True

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.
        """
        LOGGER.info('Adding consumer cancellation callback')
        self.channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.
        :param pika.frame.Method method_frame: The Basic.Cancel frame
        """
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self.channel:
            self.channel.close()

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.
        """
        if self.channel:
            LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            cb = functools.partial(
                self.on_cancelok, userdata=self._consumer_tag)
            self.channel.basic_cancel(self._consumer_tag, cb)

    def on_cancelok(self, _unused_frame, userdata):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.
        :param pika.frame.Method _unused_frame: The Basic.CancelOk frame
        :param str|unicode userdata: Extra user data (consumer tag)
        """
        self._consuming = False
        LOGGER.info(
            'RabbitMQ acknowledged the cancellation of the consumer: %s',
            userdata)
        self.close_channel()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.
        """
        LOGGER.info('Closing the channel')
        self.channel.close()

    def start(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.
        """
        self.create_connection()
        print("Created connection")
        self.connection.ioloop.start()
        print("Started loop")

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.
        """
        if not self._closing:
            self._closing = True
            LOGGER.info('Stopping')
            if self._consuming:
                self.stop_consuming()
                self.connection.ioloop.stop()
            else:
                self.connection.ioloop.stop()
            LOGGER.info('Stopped')

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
                self.queue_bindings.append(MQNonBlockingQueueBinding(exchange_name=exchange_name, routing_key=routing_key))
            else:
                raise ValueError(
                    f"Could not register as invalid exchange key '{exchange_name}' was provided. It must be one of: {','.join(Exchanges.get_exchange_keys())}")