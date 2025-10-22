import os
import sys
import time
import functools
import traceback
from datetime import datetime
import pyodbc
from urllib.parse import quote_plus

class Logger:
    
    def __init__(self, log_file_name="System", log_dir="logs", log_to_console=False, database_username=None, database_password=None, database_server=None, database_name=None, database_type="mssql", include_duration=False, include_traceback=True, include_print=False, include_database=False, include_function_args=False, table_name="ConsumptionLogs"):
        """
        Initialize the logger with a directory and console logging option.
        
        Args:
            log_file_name (str): Name of the log file (without extension)
            log_dir (str): Directory where log files will be stored
            log_to_console (bool): Whether to print logs to console
            database_username (str): Database username for logging
            database_password (str): Database password for logging
            database_server (str): Database server address
            database_name (str): Database name
            database_type (str): Database type - 'mssql' (SQL Server) or 'mysql' (MySQL)
            include_duration (bool): Whether to log function execution duration
            include_traceback (bool): Whether to log full traceback on errors
            include_print (bool): Whether to include print statements
            include_database (bool): Whether to log to database
            include_function_args (bool): Whether to log function arguments
            table (dict): Table configuration with 'table_name' key
        """
        self.log_file_name = log_file_name
        self.log_dir = log_dir
        self.log_to_console = log_to_console
        self.database_username = database_username
        self.database_password = database_password
        self.database_server = database_server
        self.database_name = database_name
        self.database_type = database_type
        self.include_duration = include_duration
        self.include_traceback = include_traceback
        self.include_print = include_print
        self.include_database = include_database
        self.include_function_args = include_function_args
        self.table_name = table_name
        self.connection = None
        os.makedirs(self.log_dir, exist_ok=True)

    def logging(self, message, log_file=None, log_level="INFO"):
        """
        Write message to log file and optionally to console.
        
        Args:
            message (str): The message to log
            log_file (str): Path to log file (optional)
            log_level (str): Log level (INFO, ERROR, DEBUG, etc.)
        """
        if log_file is None:
            log_file = os.path.join(self.log_dir, self.log_file_name + ".log")

        timestamp = datetime.now().strftime("%d %B %Y %H:%M:%S") + f".{datetime.now().microsecond // 1000:03d}"
        
        with open(log_file, "a") as f:
            f.write(timestamp + " - " + log_level + " - " + message + "\n")
        
        if self.log_to_console:
            print(timestamp + " - " + log_level + " - " + message)

    def _get_connection(self):
        """
        Create and return a pyodbc connection for the configured database.
        Supports both SQL Server (mssql) and MySQL.
        """
        if not all([self.database_server, self.database_name, self.database_username, self.database_password]):
            raise ValueError(f"All database parameters must be provided (server, database, username, password)")
        
        try:
            if self.database_type == "mssql":
                # SQL Server connection string
                connection_string = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={self.database_server};"
                    f"DATABASE={self.database_name};"
                    f"UID={self.database_username};"
                    f"PWD={self.database_password}"
                )
            elif self.database_type == "mysql":
                # MySQL connection string
                connection_string = (
                    f"DRIVER={{MySQL ODBC 8.0 Driver}};"
                    f"SERVER={self.database_server};"
                    f"DATABASE={self.database_name};"
                    f"UID={self.database_username};"
                    f"PWD={self.database_password}"
                )
            else:
                raise ValueError(f"Unsupported database type: {self.database_type}. Use 'mssql' or 'mysql'")
            
            connection = pyodbc.connect(connection_string)
            return connection
            
        except Exception as e:
            raise ValueError(f"Failed to create database connection: {str(e)}")

    def _check_database_exists(self):
        """
        Checks if the database connection is valid and database exists.
        
        Returns:
            bool: True if the database exists and is accessible, False otherwise.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            print(f"Error connecting to database: {str(e)}")
            return False

    def _check_table_exists(self, table_name):
        """
        Check if a table exists in the database.
        
        Args:
            table_name (str): Name of the table to check
            
        Returns:
            bool: True if table exists, False otherwise
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            if self.database_type == "mssql":
                # SQL Server query to check if table exists
                query = """
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = ?
                """
            elif self.database_type == "mysql":
                # MySQL query to check if table exists
                query = """
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                """
                cursor.execute(query, (self.database_name, table_name))
                result = cursor.fetchone()[0]
                cursor.close()
                conn.close()
                return result > 0
            
            cursor.execute(query, (table_name,))
            result = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return result > 0
            
        except Exception as e:
            print(f"Error checking if table exists: {str(e)}")
            return False

    def _log_ai_interaction(self, ai_provider, model, prompt, completion, tokens_prompt, tokens_completion, tokens_total, duration, user_id="", user_name="", user_email="", user_dept="", company_code=""):
        """
        Log AI model interaction to database.
        
        Args:
            ai_provider (str): Name of AI provider (e.g., "Azure OpenAI")
            model (str): Model name used
            prompt (str): Input prompt sent to the model
            completion (str): Response from the model
            tokens_prompt (int): Number of tokens in the prompt
            tokens_completion (int): Number of tokens in the completion
            tokens_total (int): Total tokens used
            duration (float): Duration in seconds
            user_id (str): Optional user ID
            user_name (str): Optional user name
            user_email (str): Optional user email
            user_dept (str): Optional user department
            company_code (str): Optional company code
        """
        if not self.include_database:
            return
                    
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            insert_query = f"""
            INSERT INTO {self.table_name} (
            [AIProvider], [Model], [Prompt], [Completion],
            [TokensPrompt], [TokensCompletion], [TokensTotal], [DurationSeconds],
            [userId], [userName], [userEmail], [userDept], [companyCode]
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            cursor.execute(
                insert_query,
                (
                    ai_provider,
                    model,
                    prompt,
                    completion,
                    tokens_prompt,
                    tokens_completion,
                    tokens_total,
                    duration,
                    user_id,
                    user_name,
                    user_email,
                    user_dept,
                    company_code
                )
            )
            conn.commit()
            cursor.close()
            conn.close()
                
        except pyodbc.Error as ex:
            self.logging(f"Error inserting AI log into database: {str(ex)}", log_file=None, log_level="ERROR")
            raise

    def _insert_database(self, log_message, log_file, log_level, timestamp):
        """
        Main method to handle database logging with validation and error handling.
        """
        if not self._check_database_exists():
            self.logging(f"Database connection failed or database does not exist.", log_file, log_level="ERROR")
            raise ValueError("Database connection failed or database does not exist.")
        
        if self.table_name is None:
            raise Exception(
                "Table name must be provided if include_database is set to True in the Logger class. "
                "Table schema must follow this: LogID (int, Primary Key), LogTime (datetime), "
                "LogLevel (varchar(50)), LogMessage (text/varchar(max))"
            )
        
        try:            
            self._insert_log(log_message=log_message, log_level=log_level, timestamp=timestamp)
            
        except Exception as e:
            self.logging(f"Database logging failed: {str(e)}", log_file, log_level="ERROR")
            raise

    def _insert_log(self, log_message, log_level, timestamp):
        """
        Insert a log entry into the database.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            insert_query = f"""
            INSERT INTO {self.table_name} (LogTime, LogLevel, LogMessage)
            VALUES (?, ?, ?)
            """
            
            cursor.execute(insert_query, (timestamp, log_level, log_message))
            conn.commit()
            cursor.close()
            conn.close()
            
        except pyodbc.Error as ex:
            raise Exception(f"Error inserting log into database: {str(ex)}")
        
    def _log_args(self, args, kwargs, log_file):
        """Log function arguments if enabled."""
        if self.include_function_args:
            if args:
                self.logging(f"Positional arguments: {args}", log_file)
            if kwargs:
                self.logging(f"Keyword arguments: {kwargs}", log_file)

    def _log_include_duration(self, start_time, end_time, log_file, result):
        """Log execution duration and return value."""
        if self.include_duration:
            self.logging(f"Execution time: {end_time - start_time:.4f} seconds", log_file)
        self.logging(f"Return value: {result}", log_file)
        
    def log(self, log_level="INFO", include_ai=False):
        """
        Decorator method to log function calls, arguments, and return values.

        Args:
            log_level (str): The logging level (e.g., "INFO", "DEBUG").
            include_ai (bool): Whether to log AI model interactions to database.
        """ 

        def decorator(func):
            @functools.wraps(func)                
            def wrapper(*args, **kwargs):    
                log_file = os.path.join(self.log_dir, self.log_file_name + ".log")

                self.logging(f"Calling function: {func.__name__}", log_file)

                self._log_args(args, kwargs, log_file)

                start_time = time.time()
                
                try:
                    result = func(*args, **kwargs)
                    end_time = time.time()

                    # If AI logging is enabled and result has the expected attributes
                    if include_ai and hasattr(result, 'usage') and hasattr(result, 'model'):
                        try:
                            # Extract prompt from function arguments
                            prompt = args[0] if args else str(kwargs.get('content', ''))
                            
                            #Execution Time
                            duration = end_time - start_time

                            # Extract completion text
                            completion = result.choices[0].message.content if result.choices else ""
                            
                            # Log AI details to file
                            self.logging(f"AI Provider: Azure OpenAI", log_file)
                            self.logging(f"Model: {result.model}", log_file)
                            self.logging(f"Prompt Tokens: {result.usage.prompt_tokens}", log_file)
                            self.logging(f"Completion Tokens: {result.usage.completion_tokens}", log_file)
                            self.logging(f"Total Tokens: {result.usage.total_tokens}", log_file)
                            
                            # Log AI interaction to database if enabled
                            if self.include_database:
                                self._log_ai_interaction(
                                    ai_provider="Azure OpenAI",
                                    model=result.model,
                                    prompt=prompt,
                                    completion=completion,
                                    tokens_prompt=result.usage.prompt_tokens,
                                    tokens_completion=result.usage.completion_tokens,
                                    tokens_total=result.usage.total_tokens,
                                    duration=duration
                                )
                            
                        except Exception as ai_log_error:
                            self.logging(f"Error logging AI interaction: {str(ai_log_error)}", log_file, log_level="ERROR")

                    self._log_include_duration(start_time, end_time, log_file, result)

                    return result
                    
                except Exception as e:
                    end_time = time.time()
                    
                    # Log exception details
                    self.logging(f"Exception occurred in {func.__name__}", log_file, log_level="ERROR")
                    self.logging(f"Exception type: {type(e).__name__}", log_file, log_level="ERROR")
                    self.logging(f"Exception message: {str(e)}", log_file, log_level="ERROR")

                    if self.include_duration:
                        self.logging(f"Execution time before error: {end_time - start_time:.4f} seconds", log_file, log_level="ERROR")

                    # Log full traceback if enabled
                    if self.include_traceback:
                        self.logging(f"Traceback:", log_file, log_level="ERROR")
                        self.logging(traceback.format_exc(), log_file, log_level="ERROR")

                    # Re-raise the exception to maintain normal error propagation
                    raise
                    
            return wrapper
        return decorator