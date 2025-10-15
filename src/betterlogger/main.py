import os
import sys
import time
import functools
import pyodbc
import traceback
from datetime import datetime

class Logger:
    def __init__(self, log_file_name = "System", log_dir="logs", log_to_console=False, database_username=None, database_password=None, database_server=None, database_name=None, database_table=None):
        """
        Initialize the logger with a directory and console logging option.
        """
        self.log_file_name = log_file_name
        self.log_dir = log_dir
        self.log_to_console = log_to_console
        self.database_username = database_username
        self.database_password = database_password
        self.database_server = database_server
        self.database_name = database_name
        self.database_table = database_table
        os.makedirs(self.log_dir, exist_ok=True)


    def logging(self, message, log_file = None, log_level="INFO"):
        """
        Write message to log file and optionally to console.
        """

        if log_file is None:
            log_file = os.path.join(self.log_dir, self.log_file_name + ".log")


        with open(log_file, "a") as f:
            timestamp = datetime.now().strftime("%d %B %Y %H:%M:%S") + f".{datetime.now().microsecond // 1000:03d}"
            f.write(timestamp + " - " + log_level + " - " + message + "\n")
        if self.log_to_console:
            print(message)

    def check_database_exists(self, server_name, database_name, username, password, driver="ODBC Driver 17 for SQL Server"):
        """
        Checks if a specific database exists on the SQL Server.

        Args:
            server_name (str): The name of the SQL Server instance.
            database_name (str): The name of the database to check.
            username (str): The username for SQL Server authentication.
            password (str): The password for SQL Server authentication.
            driver (str): The ODBC driver to use for the connection.

        Returns:
            bool: True if the database exists, False otherwise.
        """
        if not all([server_name, database_name, username, password]):
            raise ValueError("All parameters must be provided and non-empty, if not turn off the include_database option to False for function decorators.")
        
        connection_string = (
            f"DRIVER={{{driver}}};"  # Adjust driver as needed
            f"SERVER={server_name};"
            f"UID={username};"  # Uncomment and provide if not using trusted connection
            f"PWD={password};"  # Uncomment and provide if not using trusted connection
        )

        try:
            cnxn = pyodbc.connect(connection_string)
            cursor = cnxn.cursor()

            # Query to check if the database exists
            query = f"SELECT DB_ID('{database_name}')"
            cursor.execute(query)

            db_id = cursor.fetchone()[0]

            cursor.close()
            cnxn.close()

            return db_id is not None

        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"Error connecting to SQL Server: {sqlstate}")
            return False

    def log(self, include_duration=True, include_database=False, include_traceback=True, include_print=True, include_timestamp=True, log_level="INFO"):
        """
        Decorator method to log function calls, arguments, and return values.

        Args:
            log_level (str): The logging level (e.g., "INFO", "DEBUG").
            include_timestamp (bool): Whether to include a timestamp in the logs.
            include_duration (bool): Whether to include the duration of the function call in the logs.
            include_database (bool): Whether to include database logging.
            include_traceback (bool): Whether to include full traceback for exceptions.

        """
        def decorator(func):
            @functools.wraps(func)                
            def wrapper(*args, **kwargs):           
                log_file = os.path.join(self.log_dir, self.log_file_name + ".log")

                if include_database:
                    if self.check_database_exists(self.database_server, self.database_name, self.database_username, self.database_password):
                        self.logging(f"Database connection successful.", log_file)
                    else:
                        self.logging(f"Database connection failed or database does not exist.", log_file, log_level="ERROR")
                        raise ValueError("Database connection failed or database does not exist.")

                self.logging(f"Calling function: {func.__name__}", log_file)

                if args:
                    self.logging(f"Positional arguments: {args}", log_file)
                if kwargs:
                    self.logging(f"Keyword arguments: {kwargs}", log_file)

                start_time = time.time()
                
                try:
                    result = func(*args, **kwargs)
                    end_time = time.time()

                    if include_duration:
                        self.logging(f"Execution time: {end_time - start_time:.4f} seconds", log_file)
                    self.logging(f"Return value: {result}", log_file)

                    return result
                    
                except Exception as e:
                    end_time = time.time()
                    
                    # Log exception details
                    self.logging(f"Exception occurred in {func.__name__}", log_file, log_level="ERROR")
                    self.logging(f"Exception type: {type(e).__name__}", log_file, log_level="ERROR")
                    self.logging(f"Exception message: {str(e)}", log_file, log_level="ERROR")

                    if include_duration:
                        self.logging(f"Execution time before error: {end_time - start_time:.4f} seconds", log_file, log_level="ERROR")

                    # Log full traceback if enabled
                    if include_traceback:
                        self.logging(f"Traceback:", log_file, log_level="ERROR")
                        self.logging(traceback.format_exc(), log_file, log_level="ERROR")

                    # Re-raise the exception to maintain normal error propagation
                    raise
                    
            return wrapper
        return decorator