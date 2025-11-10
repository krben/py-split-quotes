# Quote Splitter Service

A Python service that processes quote JSON files from Azure Blob Storage, extracts specific objects, and organizes them into separate files.

## Features

- **Parallel Processing**: Processes multiple quotes concurrently using 4 worker threads for improved performance
- **Smart Filtering**: Automatically skips quotes with missing or empty key fields
- **Empty Object Handling**: Skips file generation when extract objects are empty (empty arrays, empty dicts, etc.)
- **Automatic Archiving**: Moves processed quotes to the `Original` folder regardless of whether files were created
- **Azure Integration**: Seamlessly integrates with Azure Blob Storage and Azure Application Insights for logging

## How It Works

1. **Reads quote files** from the configured source path in Azure Blob Storage
2. **Validates key field** - Skips quotes where the key field (e.g., `QuoteId`) is missing or empty
3. **Extracts objects** - For each configured extract object (e.g., `QuoteCharges`):
   - Checks if the object exists in the quote data
   - Validates that the object contains data (not empty)
   - Creates a separate JSON file with the key field, the extracted object, and Tracking information
4. **Archives original** - Moves the original quote file to the `Original` folder after processing
5. **Logs events** - All operations are logged to Azure Application Insights

## Configuration

The service is configured via `config.json`:

```json
{
  "key_field": "QuoteId",
  "extract_objects": [
    "QuoteCharges"
  ]
}
```

### Configuration Options

- **`key_field`**: The field name used as the unique identifier for quotes (default: `"QuoteId"`)
- **`extract_objects`**: Array of object names to extract from each quote into separate files

## File Structure

### Input
- Source path: `files/sbt/quotes/`
- Files are expected in JSON format

### Output
- Extracted objects: `files/sbt/quotes/{ObjectName}/{timestamp}_{quoteId}_{ObjectName}.json`
- Archived originals: `files/sbt/quotes/Original/{original_filename}.json`

### File Naming
- Format: `{timestamp}_{quoteId}_{ObjectName}.json`
- Timestamp is extracted from the original filename or generated if not present
- QuoteId is extracted from the filename or from the key_field value

## Processing Rules

### Skipped Quotes
The service will skip processing (and log a warning) when:
- The key field is missing from the quote data
- The key field value is empty (None, empty string, empty array, empty dict)
- The quote is already in the `Original` folder
- The quote is already in an extract object folder
- The quote file has already been split (contains `_{ObjectName}` in filename)

### Skipped Files
The service will skip creating a file for an extract object when:
- The object doesn't exist in the quote data
- The object is empty (empty array `[]`, empty dict `{}`, empty string, etc.)

### Original File Movement
- Original files are **always** moved to the `Original` folder after processing
- This happens even if no extract files were created (e.g., when all extract objects are empty)
- The original file is deleted from the source location after successful copy

## Threading

The service uses **4 worker threads** to process quotes in parallel:
- Each thread processes one quote at a time
- Thread-safe Azure Blob Storage operations
- Independent error handling per thread
- Improved throughput for large batches of quotes

## Logging

All events are logged to Azure Application Insights with the following event types:

- **`Start`**: Service startup
- **`split_quote_skipped`**: Quote skipped due to missing/empty key field
- **`extract_object_skipped`**: Extract object skipped due to being empty
- **`quote_split_success`**: Successfully created an extract file
- **`quote_moved_to_original`**: Original file moved to archive
- **`blob_processing_error`**: Error processing a specific blob
- **`blob_processing_failed`**: Failed to process a blob
- **`General Error`**: General execution errors

## Requirements

See `requirements/requirements.txt` for Python dependencies.

## Usage

Run the service:

```bash
python app/app.py
```

The service will:
1. Connect to Azure Blob Storage using configured credentials
2. List all quotes in the source path
3. Process each quote in parallel (4 threads)
4. Generate extract files and archive originals
5. Log all operations to Azure Application Insights

## Error Handling

- Individual blob processing errors are caught and logged without stopping the entire process
- Each thread handles its own errors independently
- Failed blobs are logged with detailed error information
- The service continues processing remaining blobs even if some fail

