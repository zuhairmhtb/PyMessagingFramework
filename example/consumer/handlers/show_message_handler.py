from typing import List
from PyMessagingFramework.src.framework import BaseCommandHandler
from PyMessagingFramework.example.consumer.commands.show_message_command import ShowMessageCommand

class ShowMessageHandler(BaseCommandHandler):
    
    def handle(self, command:ShowMessageCommand):
        print(f"Received message: {command.message}")
        print(f"Received number: {command.number}")
        print(f"Received list: {command.array}")