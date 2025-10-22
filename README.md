# Bester Logger

A powerful Python logging library that makes it easy to track function execution, errors, and AI interactions. Perfect for debugging, monitoring, and auditing your applications.

## ✨ Features

- 🎯 **Simple Decorator-Based Logging** - Just add `@logger.log()` to any function
- 📁 **File Logging** - Automatic log file management with timestamps
- 🗄️ **Database Logging** - Optional SQL Server/MySQL integration
- 🤖 **AI Interaction Tracking** - Automatic logging of OpenAI/Azure OpenAI API calls with token usage
- ⏱️ **Performance Monitoring** - Track function execution time
- 🐛 **Error Tracking** - Capture full tracebacks and exception details
- 🔧 **Highly Configurable** - Control what gets logged with simple parameters

## 📦 Installation

```bash
pip install bester-logger
```

## 🚀 Quick Start

### Basic Usage

```python
from bester_logger import Logger

# Initialize the logger
logger = Logger(log_file_name="MyApp", log_dir="./logs")

# Decorate any function you want to log
@logger.log()
def add_numbers(a, b):
    return a + b

@logger.log()
def greet(name):
    return f"Hello, {name}!"

# Use your functions normally
result = add_numbers(5, 3)
greeting = greet("Alice")
```

**Output in `logs/MyApp.log`:**
```
22 October 2025 10:15:30.123 - INFO - Calling function: add_numbers
22 October 2025 10:15:30.125 - INFO - Return value: 8
22 October 2025 10:15:30.126 - INFO - Calling function: greet
22 October 2025 10:15:30.127 - INFO - Return value: Hello, Alice!
```

### Manual Logging

You can also log messages directly without using decorators:

```python
from bester_logger import Logger

logger = Logger(log_file_name="MyApp", log_dir="./logs")

# Log messages directly with different severity levels
logger.logging("Application started", log_level="INFO")
logger.logging("Processing data...", log_level="DEBUG")
logger.logging("Connection failed", log_level="ERROR")
logger.logging("Warning: Low memory", log_level="WARNING")

# Combine decorator and manual logging
@logger.log()
def process_order(order_id):
    logger.logging(f"Starting to process order {order_id}", log_level="INFO")
    
    # Your processing logic here
    if order_id < 0:
        logger.logging(f"Invalid order ID: {order_id}", log_level="ERROR")
        return None
    
    logger.logging(f"Order {order_id} processed successfully", log_level="INFO")
    return "Success"

process_order(12345)
```

**Parameters for `.logging()` method:**
- `message` (str, required): The message to log
- `log_file` (str, optional): Custom log file path (defaults to the logger's configured file)
- `log_level` (str, optional): Severity level - `"INFO"`, `"DEBUG"`, `"ERROR"`, `"WARNING"` (default: `"INFO"`)

**Output in `logs/MyApp.log`:**
```
22 October 2025 10:15:30.123 - INFO - Application started
22 October 2025 10:15:30.124 - DEBUG - Processing data...
22 October 2025 10:15:30.125 - ERROR - Connection failed
22 October 2025 10:15:30.126 - WARNING - Warning: Low memory
22 October 2025 10:15:30.127 - INFO - Calling function: process_order
22 October 2025 10:15:30.128 - INFO - Starting to process order 12345
22 October 2025 10:15:30.129 - INFO - Order 12345 processed successfully
22 October 2025 10:15:30.130 - INFO - Return value: Success
```

## 📚 Configuration Options

### Logger Initialization

```python
Logger(
    log_file_name="System",           # Name of log file (without .log extension)
    log_dir="./logs",                  # Directory for log files
    log_to_console=False,              # Print logs to console
    include_duration=False,            # Log execution time for functions
    include_traceback=True,            # Log full error tracebacks
    include_function_args=False,       # Log function arguments
    include_database=False,            # Enable database logging
    database_username=None,            # Database username
    database_password=None,            # Database password
    database_server=None,              # Database server address
    database_name=None,                # Database name
    database_type="mssql",             # "mssql" or "mysql"
    table_name="ConsumptionLogs"       # Database table name
)
```

### Decorator Options

```python
@logger.log(
    log_level="INFO",     # "INFO", "DEBUG", "ERROR", "WARNING"
    include_ai=False      # Enable AI interaction logging
)
```

## 🎨 Usage Examples

### 1. Track Function Performance

```python
from bester_logger import Logger

logger = Logger(
    log_file_name="Performance",
    log_dir="./logs",
    include_duration=True  # Enable execution time tracking
)

@logger.log()
def process_data(data):
    # Your processing logic
    return processed_data

result = process_data(my_data)
```

**Log Output:**
```
22 October 2025 10:20:15.123 - INFO - Calling function: process_data
22 October 2025 10:20:18.456 - INFO - Execution time: 3.3330 seconds
22 October 2025 10:20:18.456 - INFO - Return value: [processed data]
```

### 2. Debug with Function Arguments

```python
from bester_logger import Logger

logger = Logger(
    log_file_name="Debug",
    log_dir="./logs",
    include_function_args=True  # Log function inputs
)

@logger.log()
def calculate(x, y, operation="add"):
    if operation == "add":
        return x + y
    elif operation == "multiply":
        return x * y

result = calculate(5, 3, operation="multiply")
```

**Log Output:**
```
22 October 2025 10:25:30.123 - INFO - Calling function: calculate
22 October 2025 10:25:30.124 - INFO - Positional arguments: (5, 3)
22 October 2025 10:25:30.124 - INFO - Keyword arguments: {'operation': 'multiply'}
22 October 2025 10:25:30.125 - INFO - Return value: 15
```

### 3. Error Tracking

```python
from bester_logger import Logger

logger = Logger(
    log_file_name="Errors",
    log_dir="./logs",
    include_traceback=True  # Full error details
)

@logger.log()
def divide(a, b):
    return a / b

try:
    result = divide(10, 0)
except ZeroDivisionError:
    print("Error logged to file!")
```

**Log Output:**
```
22 October 2025 10:30:45.123 - INFO - Calling function: divide
22 October 2025 10:30:45.125 - ERROR - Exception occurred in divide
22 October 2025 10:30:45.125 - ERROR - Exception type: ZeroDivisionError
22 October 2025 10:30:45.125 - ERROR - Exception message: division by zero
22 October 2025 10:30:45.126 - ERROR - Traceback:
Traceback (most recent call last):
  File "main.py", line 285, in wrapper
    result = func(*args, **kwargs)
  File "test.py", line 12, in divide
    return a / b
ZeroDivisionError: division by zero
```

### 4. Database Logging

```python
from bester_logger import Logger

logger = Logger(
    log_file_name="Database",
    log_dir="./logs",
    include_database=True,
    database_server="your-server.database.windows.net",
    database_name="MyDatabase",
    database_username="admin",
    database_password="SecurePassword123",
    database_type="mssql",
    table_name="ApplicationLogs"
)

@logger.log()
def important_operation():
    # Critical operation that needs database audit trail
    return "Success"
```

**Required Database Table Schema:**
```sql
CREATE TABLE ApplicationLogs (
    LogID INT PRIMARY KEY IDENTITY(1,1),
    LogTime DATETIME DEFAULT GETDATE(),
    LogLevel VARCHAR(50),
    LogMessage TEXT
)
```

### 5. AI Interaction Tracking (OpenAI/Azure OpenAI)

Track token usage and costs automatically!

```python
from bester_logger import Logger
from openai import AzureOpenAI
import os

logger = Logger(
    log_file_name="AI_Usage",
    log_dir="./logs",
    include_database=True,  # Optional: Store in database
    database_server="your-server.database.windows.net",
    database_name="AITracking",
    database_username="admin",
    database_password="SecurePassword123",
    table_name="ConsumptionLogs"
)

@logger.log(include_ai=True)  # ⭐ Enable AI logging
def ask_ai(question):
    client = AzureOpenAI(
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        api_version="2025-01-01-preview",
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
    )
    
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": question}]
    )
    
    # ⚠️ IMPORTANT: Return the full response object, not just the text
    return response

# Use it
answer = ask_ai("What is Python?")
print(answer.choices[0].message.content)
```

**Log Output:**
```
22 October 2025 11:00:15.123 - INFO - Calling function: ask_ai
22 October 2025 11:00:17.456 - INFO - AI Provider: Azure OpenAI
22 October 2025 11:00:17.456 - INFO - Model: gpt-4
22 October 2025 11:00:17.456 - INFO - Prompt Tokens: 12
22 October 2025 11:00:17.456 - INFO - Completion Tokens: 150
22 October 2025 11:00:17.456 - INFO - Total Tokens: 162
22 October 2025 11:00:17.457 - INFO - Return value: [ChatCompletion object]
```

**Database Schema for AI Logging:**
```sql
CREATE TABLE ConsumptionLogs (
    LogID INT PRIMARY KEY IDENTITY(1,1),
    AIProvider VARCHAR(100),
    Model VARCHAR(100),
    Prompt TEXT,
    Completion TEXT,
    TokensPrompt INT,
    TokensCompletion INT,
    TokensTotal INT,
    DurationSeconds FLOAT,
    userId VARCHAR(100) NULL,
    userName VARCHAR(100) NULL,
    userEmail VARCHAR(100) NULL,
    userDept VARCHAR(100) NULL,
    companyCode VARCHAR(100) NULL,
    LogTime DATETIME DEFAULT GETDATE()
)
```

## 🔧 Advanced Configuration

### Multiple Loggers for Different Purposes

```python
from bester_logger import Logger

# API request logger
api_logger = Logger(log_file_name="API", log_dir="./logs/api")

# Database operation logger
db_logger = Logger(
    log_file_name="Database",
    log_dir="./logs/db",
    include_duration=True
)

# AI usage logger
ai_logger = Logger(
    log_file_name="AI",
    log_dir="./logs/ai",
    include_database=True,
    database_server="...",
    database_name="...",
    database_username="...",
    database_password="...",
    table_name="AILogs"
)

@api_logger.log()
def fetch_data(endpoint):
    # API logic
    pass

@db_logger.log()
def save_to_database(data):
    # Database logic
    pass

@ai_logger.log(include_ai=True)
def generate_summary(text):
    # AI logic
    pass
```

### Console Output During Development

```python
from bester_logger import Logger

logger = Logger(
    log_file_name="Development",
    log_dir="./logs",
    log_to_console=True  # Print to console AND write to file
)

@logger.log()
def test_function():
    return "Testing"

test_function()
# Logs appear in console AND in logs/Development.log
```

## ❓ Troubleshooting

### Issue: `ImportError: cannot import name 'Logger'`
**Solution:** Make sure you installed the latest version:
```bash
pip uninstall bester-logger
pip install bester-logger
```

### Issue: Database connection fails
**Solution:** 
1. Verify all database credentials are correct
2. Ensure SQL Server/MySQL ODBC driver is installed
3. Check firewall allows database connections
4. Test connection manually first

### Issue: AI logging not capturing tokens
**Solution:** 
- Ensure you return the **full response object**, not just `response.choices[0].message.content`
- ✅ Correct: `return response`
- ❌ Wrong: `return response.choices[0].message.content`

### Issue: Logs not appearing
**Solution:**
- Check the `log_dir` path exists and is writable
- Verify the function is being called
- Try `log_to_console=True` to see immediate output

## 📄 License

MIT License - See LICENSE file for details

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
Please Update the pyproject.toml version to the next iteration before running the github action, if the version is the same as previously stated it will not run.

## 📞 Support

If you encounter any issues or have questions, please open an issue on GitHub.

---

**Made with ❤️ for Python developers who value clean logs and easy debugging**
