from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    SERVICE_NAME: str = "example-core-service"
    DATABASE_URL: str = "sqlite+aiosqlite:///./test.db"
    REDIS_URL: str = "redis://localhost:6379"

    class Config:
        env_file = ".env"


settings = Settings()
