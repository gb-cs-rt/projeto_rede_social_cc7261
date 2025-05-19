import asyncio
import aio_pika
import json  # Add this import at the top of the file

class Broker:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.response_queue = None
        self.responses = {}
        self.loop = asyncio.get_event_loop()

    async def connect(self):
        self.connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
        self.channel = await self.connection.channel()
        
        # Declare a response queue
        self.response_queue = await self.channel.declare_queue(exclusive=True)
        await self.response_queue.consume(self.on_response)

    async def disconnect(self):
        await self.connection.close()

    async def on_response(self, message: aio_pika.IncomingMessage):
        async with message.process():
            correlation_id = message.correlation_id
            if correlation_id in self.responses:
                self.responses[correlation_id].set_result(message.body.decode())

    async def rpc_publish(self, queue_name, message, correlation_id):
        # Create a future to wait for the response
        self.responses[correlation_id] = self.loop.create_future()
        
        # Publish the message
        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps(message).encode(),  # Serialize the message to JSON
                correlation_id=correlation_id,
                reply_to=self.response_queue.name,
            ),
            routing_key=queue_name,
        )
        
        # Wait for the response
        response = await self.responses[correlation_id]
        del self.responses[correlation_id]  # Clean up
        return response