import os
import sys
import time
import functools
import traceback
from datetime import datetime
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus

class Logger:
    
    def __init__(self, log_file_name = "System", log_dir="logs", log_to_console=False, database_username=None, database_password=None, database_server=None, database_name=None, database_type="mssql", include_duration=False, include_traceback=True, include_print=False, include_database=False, include_function_args=False, table={"table_name": "Logs", "create_table_if_not_exists": False}):
        """
        Initialize the logger with a directory and console logging option.
        
        Args:
            database_type (str): Database type - 'mssql' (SQL Server) or 'mysql' (MySQL)
        """
        self.log_file_name = log_file_name
        self.log_dir = log_dir
        self.log_to_console = log_to_console
        self.database_username = database_username
        self.database_password = database_password
        self.database_server = database_server
        self.database_name = database_name
        self.database_type = database_type.lower()
        self.include_duration = include_duration
        self.include_traceback = include_traceback
        self.include_print = include_traceback
        self.include_database = include_database
        self.include_function_args = include_function_args
        self.table_name = table
        self.engine = None
        os.makedirs(self.log_dir, exist_ok=True)

    def logging(self, message, log_file = None, log_level="INFO", include_database=False):
        """
        Write message to log file and optionally to console.
        """

        if log_file is None:
            log_file = os.path.join(self.log_dir, self.log_file_name + ".log")


        with open(log_file, "a") as f:
            timestamp = datetime.now().strftime("%d %B %Y %H:%M:%S") + f".{datetime.now().microsecond // 1000:03d}"
            f.write(timestamp + " - " + log_level + " - " + message + "\n")
        if self.log_to_console:
            print(timestamp + " - " + log_level + " - " + message + "\n")

        if include_database:
            self._insert_database(message, log_file, log_level, timestamp)

    def _get_engine(self):
        """
        Create and return a SQLAlchemy engine for the configured database.
        Supports both SQL Server (mssql) and MySQL.
        """
        if self.engine is None:
            if not all([self.database_server, self.database_name, self.database_username, self.database_password]):
                raise ValueError("All database parameters must be provided (server, database, username, password)")
            
            # URL-encode password to handle special characters
            encoded_password = quote_plus(self.database_password)
            
            if self.database_type == "mssql":
                # SQL Server connection string
                # Format: mssql+pyodbc://user:pass@server/database?driver=ODBC+Driver+17+for+SQL+Server
                connection_string = (
                    f"mssql+pyodbc://{self.database_username}:{encoded_password}@"
                    f"{self.database_server}/{self.database_name}?"
                    f"driver=ODBC+Driver+17+for+SQL+Server"
                )
            elif self.database_type == "mysql":
                # MySQL connection string
                # Format: mysql+pymysql://user:pass@server/database
                connection_string = (
                    f"mysql+pymysql://{self.database_username}:{encoded_password}@"
                    f"{self.database_server}/{self.database_name}"
                )
            else:
                raise ValueError(f"Unsupported database type: {self.database_type}. Use 'mssql' or 'mysql'")
            
            try:
                self.engine = create_engine(connection_string, pool_pre_ping=True)
            except Exception as e:
                raise ValueError(f"Failed to create database engine: {str(e)}")
        
        return self.engine

    def _check_database_exists(self):
        """
        Checks if the database connection is valid and database exists.
        
        Returns:
            bool: True if the database exists and is accessible, False otherwise.
        """
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                # Simple query to test connection
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            print(f"Error connecting to database: {str(e)}")
            return False

    def _create_table_if_not_exists(self):
        """
        Creates the logging table if it doesn't exist.
        Handles different SQL syntax for SQL Server vs MySQL.
        """
        try:
            engine = self._get_engine()
            
            if self.database_type == "mssql":
                # SQL Server syntax with IDENTITY
                create_table_query = f"""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{self.table_name['table_name']}')
                BEGIN
                    CREATE TABLE {self.table_name['table_name']} (
                        LogID INT IDENTITY(1,1) PRIMARY KEY,
                        LogTime DATETIME DEFAULT GETDATE(),
                        LogLevel VARCHAR(50),
                        LogMessage VARCHAR(MAX)
                    )
                END
                """
            elif self.database_type == "mysql":
                # MySQL syntax with AUTO_INCREMENT
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name['table_name']} (
                    LogID INT AUTO_INCREMENT PRIMARY KEY,
                    LogTime DATETIME DEFAULT CURRENT_TIMESTAMP,
                    LogLevel VARCHAR(50),
                    LogMessage TEXT
                )
                """
            
            with engine.connect() as conn:
                conn.execute(text(create_table_query))
                conn.commit()
                
        except SQLAlchemyError as ex:
            print(f"Error creating table: {str(ex)}")
            raise

    def _insert_log(self, log_message, log_level, timestamp):
        """
        Insert a log entry into the database table.
        """
        try:
            engine = self._get_engine()
            
            insert_query = f"""
            INSERT INTO {self.table_name['table_name']} (LogTime, LogLevel, LogMessage)
            VALUES (:log_time, :log_level, :log_message)
            """
            
            with engine.connect() as conn:
                conn.execute(
                    text(insert_query),
                    {
                        "log_time": timestamp,
                        "log_level": log_level,
                        "log_message": log_message
                    }
                )
                conn.commit()
                
        except SQLAlchemyError as ex:
            print(f"Error inserting log into database: {str(ex)}")
            raise

    def _insert_database(self, log_message, log_file, log_level, timestamp):
        """
        Main method to handle database logging with validation and error handling.
        """
        if not self._check_database_exists():
            self.logging(f"Database connection failed or database does not exist.", log_file, log_level="ERROR")
            raise ValueError("Database connection failed or database does not exist.")
        
        if self.table_name["table_name"] is None:
            raise Exception(
                "Table name must be provided if include_database is set to True in the Logger class. "
                "Table schema must follow this: LogID (int, Primary Key), LogTime (datetime), "
                "LogLevel (varchar(50)), LogMessage (text/varchar(max))"
            )
        
        try:
            if self.table_name["create_table_if_not_exists"]:
                self._create_table_if_not_exists()
            
            self._insert_log(log_message=log_message, log_level=log_level, timestamp=timestamp)
            
        except Exception as e:
            self.logging(f"Database logging failed: {str(e)}", log_file, log_level="ERROR")
            raise
        
    def _log_args(self,args, kwargs, log_file):
        if self.include_function_args:
            if args:
                self.logging(f"Positional arguments: {args}", log_file)
            if kwargs:
                self.logging(f"Keyword arguments: {kwargs}", log_file)

    def _log_include_duration(self, start_time, end_time, log_file, result):
        if self.include_duration:
            self.logging(f"Execution time: {end_time - start_time:.4f} seconds", log_file)
        self.logging(f"Return value: {result}", log_file)
        
    def log(self, log_level="INFO", include_ai=False):
        """
        Decorator method to log function calls, arguments, and return values.

        Args:
            log_level (str): The logging level (e.g., "INFO", "DEBUG").
            include_database (bool): Whether to include database logging.
            include_ai (bool): include model input and output logging.
        """ 
        def decorator(func):
            @functools.wraps(func)                
            def wrapper(*args, **kwargs):    
                self.include_ai = True if include_ai else False

                log_file = os.path.join(self.log_dir, self.log_file_name + ".log")

                self.logging(f"Calling function: {func.__name__}", log_file)

                self._log_args(args, kwargs, log_file)

                start_time = time.time()
                
                try:
                    result = func(*args, **kwargs)
                    end_time = time.time()

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