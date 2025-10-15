from pydantic import BaseModel
from pydantic_settings import BaseSettings
from typing import Optional, List

class Settings(BaseSettings):
      """Loads settings from environment variables / .env file."""
      DBT_PROJECT_DIR: str
      DBT_PROFILES_DIR: str
      API_KEY: str

      class Config:
            env_file = '.env'
            env_file_encoding = 'utf-8'
            extra = 'ignore'  # Ignore extra env vars
            case_sensitive = True

class DbtRunRequest(BaseModel):
      tags: List[str]
      target: Optional[str] = None

class DbtCommandResponse(BaseModel):
      success: bool = True
      command: str
      stdout: str
      stderr: str
      returncode: Optional[int] = None

class HealthResponse(BaseModel):
      status: str = "ok"


if __name__ == "__main__":
    None