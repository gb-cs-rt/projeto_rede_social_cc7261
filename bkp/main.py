from fastapi import FastAPI
from pydantic import BaseModel
import time

app = FastAPI()

posts = []
users = {}

class User(BaseModel):
    username: str
    following: list[str] = []
    posts: list[str] = []
    followers: list[str] = []

class Post(BaseModel):
    username: str
    content: str
    timestamp: str | None = None


@app.get("/")
def get_page():
    return {"message": "Hello World!"}

@app.get("/{username}")
def get_user(username: str):
    if username not in users:
        users[username] = User(username=username)
    return users[username]

@app.post("/post")
def create_post(post: Post):
    post.timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    posts.append(post)
    post.username 
    return {"message": "Post created successfully!"}

@app.get("/get_posts")
def get_posts():
    return {"posts": posts}