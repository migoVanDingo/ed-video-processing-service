from fastapi import FastAPI

from app.api.controller.health_check import router as health_router
from strawberry.fastapi import GraphQLRouter

app = FastAPI(title="Core Service")

# REST endpoints
app.include_router(health_router, prefix="/health", tags=["Health"])
