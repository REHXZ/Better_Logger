# Initialize logger
from time import time

import os
import sys
# Add the src directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from betterlogger.main import Logger

my_logger = Logger()

@my_logger.log(include_database=False)
def add_numbers(a, b):
    my_logger.logging("DUMB DUMB",log_level="INFO")
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

if __name__ == "__main__":
    add_numbers(5, 3)
    greet("Alice")
    greet("Bob", greeting="Hi")
    slow_function()