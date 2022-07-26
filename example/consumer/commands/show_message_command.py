from PyMessagingFramework_zuhairmhtb.framework import BaseCommand
from typing import List

class ShowMessageCommand(BaseCommand):

    def __init__(self, message:str, number:int, array:List[str]):
        self.message = message
        self.number = number
        self.array = array
        