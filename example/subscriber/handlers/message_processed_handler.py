from PyMessagingFramework.src.PyMessagingFramework.framework import BaseCommandHandler
from PyMessagingFramework.example.publisher.event.message_processed_event import TopicMessageProcessedEvent, DirectMessageProcessedEvent, FanoutMessageProcessedEvent

class DirectMessageProcessedHandler(BaseCommandHandler):

    def handle(self, command:DirectMessageProcessedEvent):
        print(f"A direct event was received which has the message: {command.message_content}, a value: {command.message_value} and an array {command.message_array} ")


class FanoutMessageProcessedHandler(BaseCommandHandler):

    def handle(self, command: FanoutMessageProcessedEvent):
        print(f"A fanout event was received which has the message: {command.message_content}, a value: {command.message_value} and an array {command.message_array} ")


class TopicMessageProcessedHandler(BaseCommandHandler):

    def handle(self, command: TopicMessageProcessedEvent):
        print(f"A topic event was received which has the message: {command.message_content}, a value: {command.message_value} and an array {command.message_array} ")