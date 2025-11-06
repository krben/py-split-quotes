

import config
import logging
import requests
import json
import traceback
import os
import time
from pathlib import Path
from opencensus.ext.azure.log_exporter import AzureLogHandler
from opencensus.common.transports.async_ import AsyncTransport
from azure.identity import ClientSecretCredential
# from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.storage.blob import BlobServiceClient


# Get the name of the function that failed
def get_failed_function_name():
    """Get the name of the function that failed."""
    tb = traceback.extract_stack(limit=3)[-2]
    return tb.name


class CustomError(Exception):
    """Custom error class that includes the failed function and message."""
    def __init__(self, unit: str, message: str):
        self.unit = unit
        self.message = message
        super().__init__(f"[{unit}] {message}")


class LoggerAPI:
    """Class to log events to Azure Application Insights."""

    AZURE_INSIGHTS_LOGGER_NAME              = config.AZURE_INSIGHTS_LOGGER_NAME
    AZURE_INSIGHTS_INSTRUMENTATION_KEY      = config.AZURE_INSIGHTS_INSTRUMENTATION_KEY
    AZURE_INGESTS_ENDPOINT                  = config.AZURE_INGESTS_ENDPOINT
    AZURE_LIVE_ENDPOINT                     = config.AZURE_LIVE_ENDPOINT
    AZURE_APPLICATION_ID                    = config.AZURE_APPLICATION_ID
    AZURE_INSIGHTS_LOGGER_LEVEL             = config.AZURE_INSIGHTS_LOGGER_LEVEL

    def __init__(self):
        """Initialize the logger with Azure Application Insights handler."""
        # Configure logging
        self.logger = logging.getLogger(self.AZURE_INSIGHTS_LOGGER_NAME)
        self.logger.setLevel(self.AZURE_INSIGHTS_LOGGER_LEVEL)  # Change to logging.INFO to reduce verbosity  

        self.logger.addHandler(
            AzureLogHandler(
                connection_string=f"InstrumentationKey={self.AZURE_INSIGHTS_INSTRUMENTATION_KEY};IngestionEndpoint={self.AZURE_INGESTS_ENDPOINT};LiveEndpoint={self.AZURE_LIVE_ENDPOINT};ApplicationId={self.AZURE_APPLICATION_ID}",
                transport=AsyncTransport
            )
        )

    def log_event(self, event_name, message, severity="INFO", unit=None, **props):
        """ Structured log with event name and custom properties """

        log_method = getattr(self.logger, severity.lower(), self.logger.info)
        log_method(message, extra={"custom_dimensions": {"logger_name": self.logger.name, "event": event_name, "unit": unit, **props}})


class AzureDataFactory:
    """Class to interact with Azure Data Factory."""
    
    AZURE_DATA_FACTORY_NAME               = config.AZURE_DATA_FACTORY_NAME
    AZURE_DATA_FACTORY_RESOURCE_GROUP     = config.AZURE_DATA_FACTORY_RESOURCE_GROUP
    AZURE_DATA_FACTORY_SUBSCRIPTION_ID    = config.AZURE_DATA_FACTORY_SUBSCRIPTION_ID
    AZURE_DATA_FACTORY_PIPELINE_NAME      = config.AZURE_DATA_FACTORY_PIPELINE_NAME

    
    def __init__(self):
        """Initialize the Azure Data Factory client."""
        current_func = get_failed_function_name()
        try:
            self.credential = ClientSecretCredential(
                config.AZURE_TENANT_ID, 
                config.AZURE_DATA_FACTORY_CLIENT_ID, 
                config.AZURE_DATA_FACTORY_CLIENT_SECRET
            )

            token = self.credential.get_token("https://management.azure.com/.default").token

            # Construct ADF REST URL
            self.url = (
                f"https://management.azure.com/subscriptions/{self.AZURE_DATA_FACTORY_SUBSCRIPTION_ID}/resourceGroups/{self.AZURE_DATA_FACTORY_RESOURCE_GROUP}"
                f"/providers/Microsoft.DataFactory/factories/{self.AZURE_DATA_FACTORY_NAME}/pipelines/{self.AZURE_DATA_FACTORY_PIPELINE_NAME}/createRun"
                f"?api-version=2018-06-01"
            )

            self.headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
        except Exception as e:
            raise CustomError(current_func,f"Failed to initialize AzureDataFactory: {str(e)}")
        
    def trigger_pipeline(self, pipeline_parameters=None):
        """Trigger the Azure Data Factory pipeline."""
       
        response = requests.post(self.url, headers=self.headers, json={"parameters": pipeline_parameters})

        if response.status_code == 200:
            run_id = response.json()["runId"]
            
            logger_api.log_event(
                "pipeline_triggered",
                "Pipeline triggered successfully",
                severity="INFO",
                run_id=run_id
            )
            return run_id
        else:
           raise CustomError(get_failed_function_name(),f"Failed to trigger pipeline: {response.status_code} - {response.text}")


class AzureDataLake:
  """Class to upload files to Azure Data Lake Storage."""

  AZURE_ACCOUNT_NAME        = config.AZURE_ACCOUNT_NAME
  AZURE_CONTAINER_NAME      = config.AZURE_CONTAINER_NAME
  AZURE_REMOTE_PATH         = config.AZURE_REMOTE_PATH
  SOURCE_PREFIX             = "files/sbt/quotes/"
  

  def __init__(self, config_path="config.json"):
    """Initialize the Azure Data Lake client."""
    
    try: 
        self.credential = ClientSecretCredential(
        config.AZURE_TENANT_ID, 
        config.AZURE_CLIENT_ID, 
        config.AZURE_CLIENT_SECRET
        )
        self.service_client = BlobServiceClient(
        account_url=f"https://{self.AZURE_ACCOUNT_NAME}.blob.core.windows.net",
        credential=self.credential
        )
        self.container_client = self.service_client.get_container_client(self.AZURE_CONTAINER_NAME)
        
        # Load splitting config
        self.config_path = config_path
        self.config = self._load_config()

    except Exception as e:
        raise CustomError(get_failed_function_name(),f"Failed to initialize AzureDataLake: {str(e)}")

  def _load_config(self):
    """Load configuration from config.json."""
    with open(self.config_path, 'r', encoding='utf-8') as f:
      return json.load(f)

  def _get_key_value(self, quote_data):
    """Extract the key field value from quote data."""
    key_field = self.config.get("key_field", "QuoteId")
    return quote_data.get(key_field)

  def _extract_filename_parts(self, filename):
    """Extract timestamp and quote ID from filename.
    
    Expected format: {timestamp}_{quoteId}.json
    Returns: (timestamp, quote_id) or (None, None) if format doesn't match
    """
    # Remove .json extension
    base_name = filename.replace('.json', '')
    
    # Check if it's already a split file (contains underscore after quote ID)
    # Format: {timestamp}_{quoteId}_{ObjectName}.json
    parts = base_name.split('_')
    
    if len(parts) >= 2:
      timestamp = parts[0]
      quote_id = parts[1]
      return timestamp, quote_id
    
    return None, None

  def split_all_quotes(self):
    """Read all quote blobs, split them, and upload to Data Lake folders."""
    
    try:
        blobs = self.container_client.list_blobs(name_starts_with=self.SOURCE_PREFIX)
        extract_objects = self.config.get("extract_objects", [])
        
        for blob in blobs:
            blob_name = blob.name
           
            # Skip archived blobs
            if blob_name.startswith(f"{self.SOURCE_PREFIX}Archive") or blob_name.startswith(f"{self.SOURCE_PREFIX}Original"):
                continue
            
            # Skip blobs in extract_objects folders and original folder
            # Check if blob path contains any extract_object folder name
            blob_path_after_prefix = blob_name[len(self.SOURCE_PREFIX):]
            path_parts = blob_path_after_prefix.split('/')
            
            # If first part of path matches any extract_object, skip it
            if path_parts and path_parts[0] in extract_objects:
                continue
            
            # Skip blobs already in original folder
            if path_parts and path_parts[0] == "original":
                continue
            
            # Skip already split files (contain _{ObjectName})
            base_name = os.path.basename(blob_name).replace('.json', '')
            parts = base_name.split('_')
            if len(parts) > 2:  # Already split
                continue

            # Read original blob content
            source_blob = self.container_client.get_blob_client(blob_name)
            raw_content = source_blob.download_blob().readall().decode('utf-8')
            print(f"Blob name: {blob_name}")
            #print(f"Raw content: {raw_content}")
            # Parse JSON
            quote_data = json.loads(raw_content)

            # Get key value
            key_value = self._get_key_value(quote_data)
            if not key_value:
                logger_api.log_event(
                    "split_quote_skipped",
                    f"Key field '{self.config['key_field']}' not found in quote data",
                    severity="WARNING",
                    blob_name=blob_name
                )
                continue

            # Extract filename parts
            filename = os.path.basename(blob_name)
            timestamp, quote_id_from_file = self._extract_filename_parts(filename)
            
            # Use quote_id from file if available, otherwise use key_value
            quote_id = quote_id_from_file if quote_id_from_file else key_value

            # If no timestamp in filename, use current timestamp
            if not timestamp:
                import time
                timestamp = str(int(time.time()))

            # Extract each object
            extract_objects = self.config.get("extract_objects", [])
            split_successful = False
            
            for obj_name in extract_objects:
                if obj_name not in quote_data:
                    # Skip if object doesn't exist in quote data
                    continue
                
                # Create extracted data with key field, the object, and Tracking
                extracted_data = {
                    self.config["key_field"]: key_value,
                    obj_name: quote_data[obj_name]
                }
                
                # Always include Tracking object if it exists
                if "Tracking" in quote_data:
                    extracted_data["Tracking"] = quote_data["Tracking"]
                
                # Create output filename
                output_filename = f"{timestamp}_{quote_id}_{obj_name}.json"
                
                # Create blob path with object folder: {SOURCE_PREFIX}{obj_name}/{filename}
                output_blob_path = f"{self.SOURCE_PREFIX}{obj_name}/{output_filename}"
                
                # Upload extracted data to blob
                output_blob = self.container_client.get_blob_client(output_blob_path)
                output_content = json.dumps(extracted_data, indent=2, ensure_ascii=False)
                output_blob.upload_blob(output_content.encode('utf-8'), overwrite=True)
                
                logger_api.log_event(
                    "quote_split_success",
                    f"Split quote object uploaded",
                    severity="INFO",
                    blob_name=output_blob_path,
                    object_name=obj_name,
                    quote_id=key_value
                )
                
                print(f"Created: {output_blob_path}")
                split_successful = True
            
            # Move original blob to /original folder after successful split
            if split_successful:
                filename = os.path.basename(blob_name)
                original_blob_path = f"{self.SOURCE_PREFIX}Original/{filename}"
                
                # Copy blob to original folder (we already have the content in raw_content)
                original_blob = self.container_client.get_blob_client(original_blob_path)
                original_blob.upload_blob(raw_content.encode('utf-8'), overwrite=True)
                
                # Delete original blob after successful copy
                source_blob.delete_blob()
                
                logger_api.log_event(
                    "quote_moved_to_original",
                    f"Original quote moved to original folder",
                    severity="INFO",
                    original_path=blob_name,
                    new_path=original_blob_path,
                    quote_id=key_value
                )
                print(f"Moved original to: {original_blob_path}")

    except Exception as e:
        raise CustomError(get_failed_function_name(),f"Failed to split quotes: {str(e)}")

def main():

  try:
    # Initialize the Data Lake client and split quotes
    data_lake = AzureDataLake(config_path="config.json")
    data_lake.split_all_quotes()


  except CustomError as e:
    #print(e.message)
    logger_api.log_event(
        "General Error",
        "Failed to execute function",
        severity="ERROR",
        unit=str(e.unit),
        error_msg=str(e.message)
    )

 
if __name__ == "__main__":
  logger_api = LoggerAPI()
  logger_api.log_event(
    "Start",
    "Starting QuotesSplitter-Service script",
    severity="INFO",
  )

  main()
  

 # #Schedule the script to run daily at 00:00  
 # schedule.every().day.at("00:00").do(main)

 # while True:
 #   schedule.run_pending()
 #   time.sleep(300)  # Sleep for 5 minutes
