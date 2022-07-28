from typing import Type, List, Dict
import pika, json

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

class BrokerMessage(BaseCommand):
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