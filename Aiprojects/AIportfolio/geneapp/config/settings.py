import os
from dotenv import load_dotenv

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_PATH = os.getenv("DATABRICKS_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
MODEL= os.getenv("MODEL")

JOB_ID = os.getenv("JOB_ID")

CATALOG = "retail_analytics"
SCHEMA = "portfolio"