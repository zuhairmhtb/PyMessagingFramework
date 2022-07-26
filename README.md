# PyMessagingFramework

This is a messaging framework to manage commands and events of RabbitMQ in a microservice architecture.

In a microservice architecture, currently commands and events are published as json data or via pickled objects. However, in a language independent system where RabbitMQ is the message broker, passing messages between the systems is feasible mainly via json objects.

If the data is passed as json, and if the message format changes, we have to find all json calls in the service and update them. This creates a problem as the same json data for a message can exist in multiple places.

This library creates a messaging framework which decouples the Broker from application and manages the messages passed between producer-consumer and publisher-subscriber.


## Producer passing a command to a consumer

In order to manage this centrally, each service can create commands as classes in separate command directory.

Example: Let Service A be a microservice with the following directory structure:

 
    - Base    
    |    
    -- Command    
    |    
    ----CommandA.py    
    |
    ----CommandB.py
    |
    -- CommandHandlers
    |
    ----HandlerA.py
    |
    ----HandlerB.py
    --main.py
    
   
Here, ServiceA contains the command classes in a directory and the handlers for the commands in a separate directory.
  
File: CommandA.py
  
```
from PyMessagingFramework.main import BaseCommand
class CommandA(BaseCommand):
    def __init__(self, param1:str, param2:str):
        self.param1 = param1
`       self.param2 = param2
```

This command class inherits the BaseCommand class and contains the parameters which the handler is expected to receive.

The handler for the command is as follows:

File: HandlerA.py

```
from PyMessagingFramework.main import BaseCommandHandler
from Base.Command.CommandA import CommandA

class HandlerA(BaseCommandHandler):
    def handle(self, command:CommandA):
        # Perform task after receiving the message
        pass
``` 

When the application starts we can configure the MessagingFramework so that it connects with RabbitMQ, creates the required exchanges and queue for the service.
Then we can hook up the command and handler so that if MQ sends a message to the service, it decodes the message and calls the appropriate handler of the message.

Filename: main.py

```
from PyMessagingFramework.main import MessagingFramework
from Base.Command.CommandA import CommandA
from Base.CommandHandlers.HandlerA import HandlerA

# Creates the framework objects, connects to MQ and creates the required exchanges and queues.

framework = MessagingFramework(
broker_url="localhost",
broker_port=5672,
broker_username="username",
broker_password="password",
queue_name="queue_name"
)

# Hook up the command and handler
framework.register_commands_as_consumer(CommandA, HandlerA)

# Start the framework to listen for requests
framework.start()
```

Now, whenever the application will receive a message matching CommandA, 'handle' method of HandlerA will be executed.

In order to send a command to ServiceA, let us create a new service ServiceB. We can package the 'Command' directory os serviceA and install it in serviceB. In that way the commands of ServiceA can be managed in one place and upgrading the package in ServiceB will automatically update the commands of ServiceA.

Similar to ServiceA, we can create a MessagingFramework object, connect it to RabbitMQ and send a command to ServiceA as follows:

Filename: producer.py

```
from PyMessagingFramework.main import MessagingFramework
from ServiceA.commands.CommandA import CommandA

# Creates the framework objects, connects to MQ and creates the required exchanges and queues.

framework = MessagingFramework(
broker_url="localhost",
broker_port=5672,
broker_username="username",
broker_password="password",
queue_name="service_b_queue"
)

# Hook up the command with the queue of ServiceA. The routing key used in serviceA is 'queue_name'

framework.register_commands_as_producer(command=CommandA, routing_key="queue_name", exchange_name='')

# Send a command to SerciceA
framework.publish_message(CommandA(param1="Hello", param2="World!"))
```

The MessagingFramework will convert the command object to json data and route it to the queue of ServiceA. The MessagingFramework of ServiceA will receive the json data, parse it to the command object and call the associated handler to execute the task. 

## Publisher publishing an event for one or more subscribers

Publisher and subscriber can interact with events similar to consumers and producers. Some example services are provided in the 'example' directory.

The following type of events can currently be created:

1. Direct

2. Fanout

3. Topic

## Future updates

1. Currently this library supports creating only blocking connections for the subscribers and consumers. Non-blocking connection will be implemented soon.

2. Currently the library supports only RabbitMQ as the message broker. We have a plan to add support for other message brokers like Redis, etc.

3. There is a plan to provide the functionality to implement Sagas for the services.

In order to contribute to the project or provide feedback please contact zuhairmhtb@gmail.com. We would love to hear from you.