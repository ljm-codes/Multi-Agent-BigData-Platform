from fastapi import APIRouter

route = APIRouter()


@route.post("/register")
async def register_user():
    pass


@route.get("/login")
async def login():
    pass


@route.post("/token")
async def get_token():
    pass
