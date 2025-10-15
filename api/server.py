import subprocess
from typing import Dict, Any

from fastapi import FastAPI, HTTPException, status, Depends, APIRouter, Security
from fastapi.security import APIKeyHeader
from loguru import logger

from api.models import DbtRunRequest, DbtCommandResponse, HealthResponse, Settings

# Instantiate settings globally
try:
      settings = Settings()
      logger.info("Settings loaded successfully.")
      logger.info(f"DBT_PROJECT_DIR: {settings.DBT_PROJECT_DIR}")
      logger.info(f"DBT_PROFILES_DIR: {settings.DBT_PROFILES_DIR}")
      logger.info(f"API_KEY: {'*' * (len(settings.API_KEY) - 4) + settings.API_KEY[-4:] if len(settings.API_KEY) > 4 else '****'}")
except Exception as e:
      logger.critical(
            f"FATAL ERROR: Could not load settings. Check .env file and environment variables. Error: {e}"
      )
      raise

# --- API Key Security Definition ---
API_KEY_NAME = "X-API-Key"
api_key_header_auth = APIKeyHeader(
      name=API_KEY_NAME, 
      auto_error=True
)

async def verify_api_key(api_key_header: str = Security(api_key_header_auth)):
      """Dependency function to verify the API key provided in the header."""
      if api_key_header == settings.API_KEY:
            return api_key_header # Or return True, value not typically used
      else:
            logger.warning(f"Unauthorized API key received: {api_key_header[:5]}...") # Log partial key
            raise HTTPException(
                  status_code=status.HTTP_401_UNAUTHORIZED,
                  detail="Invalid or missing API Key",
            )

app = FastAPI(
      title="DBT Runner API",
      description="API to trigger dbt commands remotely, with API key auth and versioning.",
      version="1.0.0",
)

# API Router for Version 1
router_v1 = APIRouter(
      prefix="/v1",
      tags=["DBT Execution v1"],
      dependencies=[Depends(verify_api_key)]
)

# Helper functions
def run_dbt_command(command_args: list[str]) -> Dict[str, Any]:
      """Runs dbt command, uses Loguru, reads paths from settings."""
      # Settings are already loaded and checked at startup
      cmd = ['dbt'] + command_args + ['--no-use-colors']
      command_str = ' '.join(cmd)
      logger.info(f"Running dbt command: {command_str}")

      try:
            process = subprocess.run(
                  cmd, 
                     capture_output=True, 
                  text=True, 
                    check=True, 
                      cwd=settings.DBT_PROJECT_DIR
            )
            stdout = process.stdout
            stderr = process.stderr
            logger.info(f"dbt command successful. stdout snippet: {(stdout or '')[:200]}...") # Log snippet
            if stderr:
                  logger.warning(f"dbt command stderr snippet: {(stderr or '')[:200]}...")
            return {
                  "success": True, 
                  "command": command_str, 
                  "stdout": stdout, 
                  "stderr": stderr, 
                  "returncode": 0
            }
      
      except subprocess.CalledProcessError as e:
            logger.error(f"dbt command failed with exit code {e.returncode}. stderr:\n{e.stderr}\nstdout:\n{e.stdout}")
            # Return a structured response WITHOUT throwing a server error so client can inspect failure
            return {
                  "success": False,
                  "command": command_str,
                  "stdout": e.stdout,
                  "stderr": e.stderr,
                  "returncode": e.returncode
            }
            
      except FileNotFoundError:
            logger.error("Error: 'dbt' command not found. Is dbt installed in the image PATH?")
            raise HTTPException(
                  status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                  detail="'dbt' command not found in container."
            )
            
      except Exception as e:
            logger.exception("An unexpected error occurred during dbt execution.")
            raise HTTPException(
                  status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                  detail=f"An unexpected server error occurred: {str(e)}"
            )

# API Endpoints
@app.get(
      "/", # Define path for the root URL
      tags=["Root"],
      summary="API Root / Welcome Message",
      response_model=Dict[str, Any]
)
async def read_root():
      """Provides a welcome message and links to key API sections."""
      logger.info("Root endpoint '/' accessed.")
      return {
            "message": "Welcome to the DBT Runner API!",
            "api_version": "v1",
            "documentation": "/docs",
            "health_check": "/health"
      }

# Health check - Placed on root app, no authentication needed
@app.get(
      "/health", 
      response_model=HealthResponse, 
      tags=["Health"], 
      summary="Check API health"
)
async def health_check():
      """Basic health check endpoint. Does not require API key."""
      return {"status": "ok"}

@router_v1.post(
      "/run", 
      response_model=DbtCommandResponse, 
      summary="Trigger dbt run"
)
async def dbt_run(request_body: DbtRunRequest) -> Dict[str, Any]:
      """Triggers 'dbt run' with tag AND logic. Requires valid X-API-Key header."""
      command_args = ['run']

      # Build intersection selector for tags (AND logic)
      tag_selector = ','.join([f'tag:{tag}' for tag in request_body.tags])
      command_args.extend(['--select', tag_selector])
      logger.info(f"Running dbt run with tag selector: {tag_selector}")

      if request_body.target:
            command_args.extend(['--target', request_body.target])
            logger.info(f"Running with target: {request_body.target}")

      result = run_dbt_command(command_args)
      return result

@router_v1.post(
      "/test", 
      response_model=DbtCommandResponse, 
      summary="Trigger dbt test"
)
async def dbt_test(request_body: DbtRunRequest) -> Dict[str, Any]:
      """Triggers 'dbt test' with tag AND logic. Requires valid X-API-Key header."""
      command_args = ['test']

      # Build intersection selector for tags
      tag_selector = ','.join([f'tag:{tag}' for tag in request_body.tags])
      command_args.extend(['--select', tag_selector])
      logger.info(f"Running dbt test with tag selector: {tag_selector}")

      if request_body.target:
            command_args.extend(['--target', request_body.target])
            logger.info(f"Running with target: {request_body.target}")

      result = run_dbt_command(command_args)
      return result

@router_v1.post("/debug", summary="Debug dbt command generation")
async def debug_dbt_command(request_body: DbtRunRequest) -> Dict[str, Any]:
      """Shows what dbt command would be generated without running it."""
      command_args = ['run']

      tag_selector = ','.join([f'tag:{tag}' for tag in request_body.tags])
      command_args.extend(['--select', tag_selector])

      if request_body.target:
            command_args.extend(['--target', request_body.target])

      # Command to be executed.
      cmd = ['dbt'] + command_args + ['--no-use-colors']
      command_str = ' '.join(cmd)

      return {
            "status": "debug",
            "command": command_str,
            "command_args": command_args,
            "request_body": request_body.dict()
      }

app.include_router(router_v1)