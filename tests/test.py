# Initialize logger
from time import time
import dotenv
import os
import sys
from openai import AzureOpenAI

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
dotenv.load_dotenv()
from betterlogger.main import Logger

my_logger = Logger(log_file_name="System",log_dir="./logs", include_database=True, database_name=os.getenv("invoice_database_A"), database_username=os.getenv("invoice_username_A"), database_password=os.getenv("invoice_password_A"), database_server=os.getenv("invoice_server_A"), table_name="ConsumptioLogs")

@my_logger.log()
def add_numbers(a, b):
    my_logger.logging("DUMB DUMB",log_level="ERROR")
    print("Inside add_numbers")
    return a + b

@my_logger.log()
def greet(name, greeting="Hello"):
    print("Inside greet")
    return f"{greeting}, {name}!"

@my_logger.log()
def slow_function():
    print("Starting slow function...")
    print("Finished slow function.")
    return "Done!"

@my_logger.log(include_ai=True)
def AI_Run(content):
    dotenv.load_dotenv()

    client = AzureOpenAI(
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),  
        api_version="2025-01-01-preview",
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
    )

    response = client.chat.completions.create(
        model="gpt-5-mini",
        messages=[
            {
                "role": "user",
                "content": content
            }
        ]
    )

    print(response)
    return response  # Return the full response object for AI logging

if __name__ == "__main__":
    add_numbers(5, 2)
    greet("Alice")
    greet("Bob", greeting="Hi")
    slow_function()
    AI_Run("Write a poem about a logger in Python.")