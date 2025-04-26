from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from broker import Broker
from pydantic import BaseModel
import time
import uuid
from contextlib import asynccontextmanager
import json

app = FastAPI()
broker = Broker()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await broker.connect()
    yield
    await broker.disconnect()

app = FastAPI(lifespan=lifespan)

# Apply CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins (you can restrict this to specific domains)
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

class Post(BaseModel):
    username: str
    content: str
    timestamp: str | None = None

@app.post("/post")
async def create_post(post: Post):
    # Add timestamp to the post
    post.timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    
    # Prepare the message with operation type and data
    correlation_id = str(uuid.uuid4())  # Unique ID for the RPC call
    message = {
        "operation": "create_post",
        "data": post.model_dump()
    }
    
    # Publish the message and wait for the response
    response = await broker.rpc_publish("msg_queue", message, correlation_id)
    
    return response

@app.get("/posts")
async def get_posts():
    # Prepare the message with operation type
    correlation_id = str(uuid.uuid4())  # Unique ID for the RPC call
    message = {
        "operation": "get_posts",
        "data": {}
    }
    
    # Publish the message and wait for the response
    response = await broker.rpc_publish("msg_queue", message, correlation_id)
    response = json.loads(response)  # Parse the string into JSON
    return response