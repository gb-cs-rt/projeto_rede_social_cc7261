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
        for attempt in range(10):
            try:
                self.connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/")
                self.channel = await self.connection.channel()
                self.response_queue = await self.channel.declare_queue(exclusive=True)
                await self.response_queue.consume(self.on_response)
                print("✅ Conectado ao RabbitMQ")
                return
            except Exception as e:
                print(f"⏳ Tentativa {attempt + 1}/10 – Aguardando RabbitMQ... {e}")
                await asyncio.sleep(5)  # Aumentado de 2 para 5 segundos

        # Só levanta exceção depois de esgotar as tentativas
        raise Exception("❌ Falha ao conectar ao RabbitMQ após 10 tentativas.")

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