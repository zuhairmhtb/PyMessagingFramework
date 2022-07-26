from PyMessagingFramework_zuhairmhtb.framework import BaseCommand, Exchanges

class DirectMessageProcessedEvent(BaseCommand):

    ROUTING_KEY = 'publisher.direct.message'
    EXCHANGE_NAME = Exchanges.DIRECT.name

    def __init__(self, message_content:str, message_value:int, message_array:list):
        self.message_content = message_content
        self.message_value = message_value
        self.message_array = message_array


class FanoutMessageProcessedEvent(BaseCommand):

    ROUTING_KEY = 'publisher.fanout.message'
    EXCHANGE_NAME = Exchanges.FANOUT.name

    def __init__(self, message_content:str, message_value:int, message_array:list):
        self.message_content = message_content
        self.message_value = message_value
        self.message_array = message_array

class TopicMessageProcessedEvent(BaseCommand):

    ROUTING_KEY = 'publisher.topic.message'
    EXCHANGE_NAME = Exchanges.TOPIC.name

    def __init__(self, message_content:str, message_value:int, message_array:list):
        self.message_content = message_content
        self.message_value = message_value
        self.message_array = message_array