# AI Logging Guide

## Overview
The Logger class now supports automatic logging of AI model interactions when using the `@logger.log(include_ai=True)` decorator.

## How It Works

### 1. **Decorator Setup**
Use the `include_ai=True` parameter when decorating your AI function:

```python
@my_logger.log(include_ai=True)
def AI_Run(content):
    # Your AI code here
    return response  # Must return the full response object
```

### 2. **Return the Full Response Object**
**IMPORTANT**: Your function must return the complete Azure OpenAI response object, not just the content string.

```python
# ✅ CORRECT
return response

# ❌ WRONG
return response.choices[0].message.content
```

### 3. **What Gets Captured**

The decorator automatically extracts and logs:

- **AI Provider**: "Azure OpenAI" (hardcoded, can be modified)
- **Model**: `response.model` (e.g., "gpt-4", "gpt-3.5-turbo")
- **Prompt**: First positional argument or `content` keyword argument
- **Completion**: `response.choices[0].message.content`
- **Tokens Prompt**: `response.usage.prompt_tokens`
- **Tokens Completion**: `response.usage.completion_tokens`
- **Tokens Total**: `response.usage.total_tokens`
- **Timestamp**: Automatically generated

### 4. **Logging Destinations**

#### File Logging (Always)
AI metrics are logged to your log file:
```
16 October 2025 14:23:45.123 - INFO - Calling function: AI_Run
16 October 2025 14:23:46.456 - INFO - AI Provider: Azure OpenAI
16 October 2025 14:23:46.456 - INFO - Model: gpt-4
16 October 2025 14:23:46.456 - INFO - Prompt Tokens: 15
16 October 2025 14:23:46.456 - INFO - Completion Tokens: 150
16 October 2025 14:23:46.456 - INFO - Total Tokens: 165
```

#### Database Logging (If Enabled)
If `include_database=True` in Logger initialization, AI interactions are inserted into your database table.

**Required Database Schema:**
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
    userId VARCHAR(100) NULL,
    userName VARCHAR(100) NULL,
    userEmail VARCHAR(100) NULL,
    userDept VARCHAR(100) NULL,
    companyCode VARCHAR(100) NULL,
    LogTime DATETIME DEFAULT GETDATE()
)
```

### 5. **Complete Example**

```python
from betterlogger.main import Logger
from openai import AzureOpenAI
import os
import dotenv

# Initialize logger with database support
my_logger = Logger(
    log_file_name="AI_System",
    log_dir="./logs",
    include_database=True,  # Enable database logging
    database_username="your_username",
    database_password="your_password",
    database_server="your_server",
    database_name="your_database",
    database_type="mssql",
    table={"table_name": "ConsumptionLogs"}
)

@my_logger.log(include_ai=True)
def AI_Run(content):
    dotenv.load_dotenv()
    
    client = AzureOpenAI(
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        api_version="2025-01-01-preview",
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
    )
    
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {
                "role": "user",
                "content": content
            }
        ]
    )
    
    print(response.choices[0].message.content)
    return response  # MUST return full response object

# Use the function
result = AI_Run("Write a poem about Python logging")
```

## Technical Details

### How Detection Works
The decorator checks if the returned object has these attributes:
```python
hasattr(result, 'usage') and hasattr(result, 'model')
```

If both exist, it treats it as an AI response and extracts the data.

### Error Handling
- If AI logging fails, an error is logged but the function continues
- The original function's return value is preserved
- Exceptions in AI logging don't break your application

### Supported AI Providers
Currently optimized for **Azure OpenAI**, but can be extended to support:
- OpenAI API
- Anthropic Claude
- Google PaLM
- Other providers with similar response structures

## Customization

### Adding User Context
You can extend `_log_ai_interaction()` to accept additional parameters:

```python
self._log_ai_interaction(
    ai_provider="Azure OpenAI",
    model=result.model,
    prompt=prompt,
    completion=completion,
    tokens_prompt=result.usage.prompt_tokens,
    tokens_completion=result.usage.completion_tokens,
    tokens_total=result.usage.total_tokens,
    timestamp=timestamp,
    user_id="user123",  # Optional
    user_name="John Doe",  # Optional
    user_email="john@example.com",  # Optional
    user_dept="Engineering",  # Optional
    company_code="COMP001"  # Optional
)
```

### Changing AI Provider Detection
Modify the `log()` decorator to detect other AI providers:

```python
if include_ai:
    # Azure OpenAI
    if hasattr(result, 'usage') and hasattr(result, 'model'):
        # Azure OpenAI logging
    # Anthropic Claude
    elif hasattr(result, 'usage') and hasattr(result, 'model'):
        # Claude logging
```

## Troubleshooting

### Issue: AI data not logging
**Solution**: Ensure you're returning the full response object, not just the content.

### Issue: Database insert fails
**Solution**: 
1. Check database connection parameters
2. Verify table schema matches expected structure
3. Check logs for specific SQL errors

### Issue: Tokens showing as 0
**Solution**: Verify your API response includes `usage` information. Some API configurations may not return token counts.

## Benefits

1. **Automatic Token Tracking**: Monitor AI usage costs automatically
2. **Audit Trail**: Complete record of all AI interactions
3. **Performance Metrics**: Track response times alongside token usage
4. **Error Tracking**: Captures failures in AI calls with full context
5. **Compliance**: Maintain logs of AI interactions for regulatory requirements