from fastapi import FastAPI
from broker import Broker

app = FastAPI()
broker = Broker()

@app.on_event("startup")
async def startup_event():
    await broker.connect()

@app.on_event("shutdown")
async def shutdown_event():
    await broker.disconnect()

@app.post("/post")
async def create_post(data: dict):
    await broker.publish("post_queue", data)
    return {"status": "Post enviado com sucesso."}

@app.post("/message")
async def send_message(data: dict):
    await broker.publish("message_queue", data)
    return {"status": "Mensagem enviada com sucesso."}