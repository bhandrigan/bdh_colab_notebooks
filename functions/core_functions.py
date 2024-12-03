
import subprocess
import sys
import importlib
import google.cloud.bigquery
import google.cloud.storage
import google.oauth2
from google.cloud import bigquery, storage, bigquery_storage

from google.oauth2 import service_account
import google.cloud.exceptions
from google.cloud.exceptions import NotFound
import pandas as pd
import numpy as np
import pandas_gbq
import datetime
from datetime import datetime, timedelta
import os
import json
import re
import uuid
import csv
import calendar
from calendar import week
import time
import random
import requests
import uuid
import simple_salesforce
from simple_salesforce import Salesforce, SalesforceMalformedRequest
import xlsxwriter
import yaml
import s3fs
import gcsfs
# import boto3
import os
import uuid
import pyarrow as pa
import pyarrow.parquet as pq
import fsspec
import gc
import numpy as np
import dask.dataframe as dd  # Use Dask for parallel processing
# from cudf import DataFrame as cudf  # Uncomment for GPU-accelerated processing with cuDF
import cudf
import dask
import dask.dataframe as dd

dask.config.set({"dataframe.backend": "cudf"})


class DataFrameConfig:
    def __init__(self, dataframe, config):
        self.dataframe = dataframe
        self.config = config

def initialize_clients(file_path='/home/developer/keys/project-keys/colab-settings.yaml', service_account_secret_name='SA_ADHOC_BILLING'):
    """
    Load configuration from a YAML file and initialize Google Cloud and Salesforce clients.
    
    Args:
        file_path (str): Path to the YAML configuration file.
        service_account_secret_name (str): Key in the YAML file for the service account path.

    Returns:
        tuple: A tuple containing the configuration dictionary, BigQuery client, Storage client, and Salesforce client.
    """
    # Load configuration from YAML
    response = {}
    try:
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found at: {file_path}")
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML configuration file: {e}")
    
    # Extract service account path from the configuration
    gcs_sa_path = config.get(service_account_secret_name)
    if not gcs_sa_path:
        raise ValueError(f"Service account path not found in config under key '{service_account_secret_name}'")

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcs_sa_path

    # Create credentials from the service account file
    try:
        credentials = service_account.Credentials.from_service_account_file(gcs_sa_path)
    except Exception as e:
        raise ValueError(f"Error loading service account credentials: {e}")
    
    # Initialize Google Cloud clients
    bigquery_client, storage_client, sf_client = None, None, None

    try:
        bigquery_client = bigquery.Client(credentials=credentials)
    except Exception as e:
        print(f"Error initializing BigQuery client: {e}")

    try:
        storage_client = storage.Client(credentials=credentials)
    except Exception as e:
        print(f"Error initializing GCS Storage client: {e}")
    
    # Initialize Salesforce client
    try:
        sf_config = config.get('veil_billing', {})
        sf_username = sf_config.get('SF_USERNAME')
        sf_password = sf_config.get('SF_PASSWORD')
        sf_token = sf_config.get('SF_TOKEN')
        if not (sf_username and sf_password and sf_token):
            raise ValueError("Salesforce credentials are incomplete in configuration.")
        sf_client = Salesforce(username=sf_username, password=sf_password, security_token=sf_token)
    except Exception as e:
        print(f"Error initializing Salesforce client: {e}")
    clients = dict({"bigquery_client": bigquery_client, "storage_client": storage_client, "sf_client": sf_client})
    response = dict({"config": config, "clients": clients})
    return response



def prep_columns_for_parquet(df, valid_final_cols, int_cols=None, date_cols=None, bool_cols=None, float_cols=None):
    """
    Prepare DataFrame columns for writing to Parquet by ensuring all columns are present and have the correct data types.
    
    Args:
        df (pd.DataFrame): The DataFrame to prepare.
        valid_final_cols (list of str): List of valid final columns to include in the output.
        int_cols (list of str): List of integer columns.
        date_cols (list of str): List of date columns.
        bool_cols (list of str): List of boolean columns.
        float_cols (list of str): List of float columns.
    
    Returns:
        pd.DataFrame: The prepared DataFrame.
    """
    # Ensure column lists are initialized
    int_cols = int_cols or []
    date_cols = date_cols or []
    bool_cols = bool_cols or []
    float_cols = float_cols or []
    all_cols = int_cols + date_cols + bool_cols + float_cols

    for col in df.columns:
        try:
            # Drop columns not in the valid final list
            if col not in valid_final_cols:
                print(f"Dropping column: {col}")
                df.drop(columns=col, inplace=True)
                continue

            # Trim whitespace from strings
            if df[col].dtype == 'object':
                print(f"Trimming whitespace for column: {col}")
                df[col] = df[col].str.strip()

            # Process integer columns
            if col in int_cols:
                print(f"Processing integer column: {col}")
                df[col] = df[col].replace(['', ' '], np.nan)  # Replace empty strings and spaces with NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert invalid values to NaN
                df[col] = df[col].fillna(-1).astype('Int64')  # Use nullable Int64 dtype

            # Process date columns
            elif col in date_cols:
                print(f"Processing date column: {col}")
                df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)

            # Process float columns
            elif col in float_cols:
                print(f"Processing float column: {col}")
                df[col] = df[col].replace(['', ' '], np.nan)  # Replace empty strings and spaces with NaN
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(-1.0).astype(float)

            # Process boolean columns
            elif col in bool_cols:
                print(f"Processing boolean column: {col}")
                df[col] = df[col].replace({1: True, 0: False, '1': True, '0': False, '': np.nan})
                df[col] = df[col].fillna(False).astype(bool)

            # Add missing columns with default values
            if col in valid_final_cols and col not in df.columns:
                print(f"Adding missing column: {col}")
                df[col] = ''

            # Ensure string columns are filled with empty strings
            if col in valid_final_cols and col not in all_cols:
                df[col] = df[col].fillna('').astype(str)

        except Exception as e:
            print(f"Error processing column '{col}': {e}")
            raise  # Re-raise the error after logging it for debugging

    # Print column types for debugging
    for col in df.columns:
        print(f"{col}: type: {df[col].dtype}")

    return df

# Enforce schema

def enforce_schema(df, schema):
    """
    Enforce a schema on a DataFrame, converting columns to specified types and filling missing values.

    Args:
        df (pd.DataFrame): The DataFrame to enforce the schema on.
        schema (dict): A dictionary where keys are column names and values are expected data types.

    Returns:
        pd.DataFrame: The DataFrame with enforced schema.

    Raises:
        Exception: If an error occurs during processing of a column, the column name and error message are displayed.
    """
    for column, dtype in schema.items():
        try:
            if column not in df.columns:
                print(f"Column '{column}' is missing. Adding as default.")
                if dtype == 'string':
                    df[column] = ''
                elif dtype in ['int64', 'float64']:
                    df[column] = -1
                elif dtype == 'bool':
                    df[column] = False
                elif dtype == 'datetime64[ns, UTC]':
                    df[column] = pd.NaT
                else:
                    df[column] = None
                continue

            # Enforce specific column types
            if dtype == 'string':
                df[column] = df[column].astype(str).fillna('')
            elif dtype == 'int64':
                df[column] = pd.to_numeric(df[column], errors='coerce').fillna(-1).astype('int64')
            elif dtype == 'float64':
                df[column] = pd.to_numeric(df[column], errors='coerce').fillna(-1.0).astype('float64')
            elif dtype == 'bool':
                df[column] = df[column].fillna(False).astype(bool)  # Handle NaN explicitly before conversion
            elif dtype == 'datetime64[ns, UTC]':
                # Gracefully handle invalid datetime values
                df[column] = pd.to_datetime(df[column], errors='coerce', utc=True)
                df[column].fillna(pd.Timestamp.min.tz_localize('UTC'), inplace=True)
            else:
                raise ValueError(f"Unsupported dtype '{dtype}' for column '{column}'")
        except Exception as e:
            print(f"Error processing column '{column}': {e}")
            raise  # Re-raise the exception after logging
    return df

# Define a function to fetch data from BigQuery
def fetch_gbq_data(query, bigquery_client):
    try:
        bqstorage_client = bigquery_storage.BigQueryReadClient()
        _df = bigquery_client.query(query).to_dataframe(bqstorage_client=bqstorage_client)
        return _df
    except Exception as e:
        print(f"Error fetching data from GBQ: {e}")
        return pd.DataFrame()
    
# Define a function to fetch data from Salesforce
def fetch_sfdc_data(object_name, fields, sf_client):
    sf = sf_client
    try:
        query = f"SELECT Id, {', '.join(fields)} FROM {object_name}"
        result = sf.query_all(query)
        records = result['records']
        for record in records:
            record.pop('attributes', None)
        return records
        # return result['records']
    except Exception as e:
        print(f"Error fetching data from Salesforce: {e}")
        return []

def fetch_max_last_updated(object_name, sf_client):
    sf = sf_client
    try:
        query = f"SELECT Id, Last_Updated__c FROM {object_name} WHERE Last_Updated__c != null ORDER BY Last_Updated__c DESC LIMIT 1"
        result = sf.query(query)
        if result['totalSize'] > 0:
            record = result['records'][0]
            last_updated = record.get('Last_Updated__c')
            return last_updated
        else:
            return '1990-01-01 00:00:00'
            # return result['records']
    except Exception as e:
        print(f"Error fetching max last_updated from Salesforce: {e}")
        return None
    
# Rename df columns to include a prefix
def rename_columns(df, prefix):
    for col in df.columns:
        if col.startswith(prefix):
            continue
        else:
            df[f"{prefix}_{col}"] = df[col]
            df.drop(columns=[col], inplace=True)
    return df


def show_more_dataframe():
    # Display all columns
    pd.set_option('display.max_columns', None)

    # Set unlimited column width
    pd.set_option('display.max_colwidth', None)

def reset_dataframe():
    pd.reset_option('display.max_columns', None)
    pd.reset_option('display.max_colwidth', None)


def sync_salesforce_tables(salesforce_url, salesforce_auth_token, sync_type, billing_run_salesforce_objects, limited_salesforce_objects, salesforce_objects=[], sync_salesforce_with_bvs_before_processing=False):
    _sync_salesforce_url = salesforce_url
    _sync_salesforce_auth_token = salesforce_auth_token
    _sync_salesforce_type = sync_type
    _custom_salesforce_objects = salesforce_objects

    if sync_salesforce_with_bvs_before_processing:
        if _sync_salesforce_type == 'FULL':
            url = f"{_sync_salesforce_url}"
            params = {'authToken': _sync_salesforce_auth_token}

            response = requests.get(url, params=params)

            result = (f'Sync Salesforce FULL result: {response.status_code}. ' + f'Response: {response.text}')
            print(result)
            return result
        elif _sync_salesforce_type == 'LIMITED':
            data = {
                "authToken": _sync_salesforce_auth_token,
                "sfdc_objects_to_clone": limited_salesforce_objects
            }

            url = f"{_sync_salesforce_url}"
            headers = {'Content-Type': 'application/json'}

            response = requests.post(url, json=data, headers=headers)
            result = (f'Sync Salesforce LIMITED result: {response.status_code}. ' + f'Response: {response.text}')
            print(result)
            return result
        elif _sync_salesforce_type == 'BILLING_RUN':
            data = {
                "authToken": _sync_salesforce_auth_token,
                "sfdc_objects_to_clone": billing_run_salesforce_objects
            }

            url = f"{_sync_salesforce_url}"
            headers = {'Content-Type': 'application/json'}

            response = requests.post(url, json=data, headers=headers)

            result = (f'Sync Salesforce BILLING RUN result: {response.status_code}. ' + f'Response: {response.text}')
            print(result)
            return result
        elif _sync_salesforce_type == 'CUSTOM':
            data = {
                "authToken": _sync_salesforce_auth_token,
                "sfdc_objects_to_clone": _custom_salesforce_objects
            }

            url = f"{_sync_salesforce_url}"
            headers = {'Content-Type': 'application/json'}

            response = requests.post(url, json=data, headers=headers)

            result = (f'Sync Salesforce BILLING RUN result: {response.status_code}. ' + f'Response: {response.text}')
            print(result)
            return result

        else:
            print('Invalid sync_salesforce_type specified.')
            return 'Invalid sync_salesforce_type specified.'

    else:
        print('Skipping sync Salesforce')
        return 'Skipping sync Salesforce'

# Function to clean the data
# look into how to improve
def clean_record(record):
    cleaned_record = {}
    for key, value in record.items():
        if pd.isna(value):
            cleaned_record[key] = None
        elif key == 'Year__c' and not str(value).isdigit():
            cleaned_record[key] = None
        else:
            cleaned_record[key] = value
    return cleaned_record

def generate_uuid():
    return str(uuid.uuid4())

def write_hive_partitioned_parquet(
    df, output_bucket, output_prefix, partition_cols, gcs_options, max_records_per_file=1_000_000
):
    import math
    import uuid
    import pyarrow as pa
    import pyarrow.parquet as pq
    import fsspec

    # Debugging: Check passed GCS options
    # print("Initializing GCS filesystem with options:", gcs_options)

    # Initialize GCS filesystem
    fs = fsspec.filesystem('gcs', **gcs_options, skip_instance_cache=True)  # Disable caching for safety

    # Group by partitions
    grouped = df.groupby(partition_cols)

    for keys, group in grouped:
        # Drop partition columns from the data
        group = group.drop(columns=partition_cols)

        # Create subdirectory for this partition
        partition_subdir = "/".join([f"{col}={val}" for col, val in zip(partition_cols, keys)])
        partition_path = f"gcs://{output_bucket}/{output_prefix}/{partition_subdir}"

        # Split into chunks if necessary
        num_chunks = math.ceil(len(group) / max_records_per_file)

        for chunk_idx in range(num_chunks):
            chunk = group.iloc[chunk_idx * max_records_per_file : (chunk_idx + 1) * max_records_per_file]

            # Generate a unique filename with UUID
            unique_filename = f"data-{uuid.uuid4().hex}.parquet"
            file_path = f"{partition_path}/{unique_filename}"

            # Write Parquet file for this partition
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            with fs.open(file_path, 'wb') as f:
                pq.write_table(table, f, compression="snappy")

            print(f"Written file: {file_path}")
            
def convert_to_string_except_exclusions(df, exclude_columns=None):
    """
    Convert all fields in the DataFrame to strings, including nested fields, except specified columns.

    Args:
        df (pd.DataFrame): Input DataFrame.
        exclude_columns (list): List of column names to exclude from conversion.

    Returns:
        pd.DataFrame: DataFrame with fields converted to strings.
    """
    exclude_columns = exclude_columns or []

    def convert_nested(value):
        """Recursively convert nested structures to strings."""
        if isinstance(value, dict):
            # Convert each key-value pair in the dictionary to strings
            return {k: convert_nested(v) for k, v in value.items()}
        elif isinstance(value, list):
            # Convert each item in the list to a string
            return [convert_nested(v) for v in value]
        elif pd.isna(value):
            # Handle NaN or None values
            return ''
        else:
            # Convert primitive types to strings
            return str(value)

    for col in df.columns:
        if col not in exclude_columns:
            if df[col] == 'attributes' | df[col] == 'profile__attributes':  # Likely contains nested objects
                df[col] = df[col].apply(lambda x: convert_nested(x))
            else:
                df[col] = df[col].astype(str)

    return df

def fetch_table_data(project_id, dataset_id, table_names, bigquery_client):
    """
    Fetch data from BigQuery tables and return a dictionary of DataFrames.

    Args:
        project_id (str): BigQuery project ID.
        dataset_id (str): BigQuery dataset ID.
        table_names (list): List of table names to fetch data from.
        bigquery_client: BigQuery client object.

    Returns:
        dict: A dictionary where keys are table names and values are pandas DataFrames.
    """
    dataframes = {}
    for table in table_names:
        fetch_sql = f"""
        SELECT * FROM {project_id}.{dataset_id}.{table}
        """
        dataframes[table] = fetch_gbq_data(fetch_sql, bigquery_client)
    return dataframes

def time_to_seconds(time_str):
    """
    Convert a time string in HH:MM:SS, MM:SS format, or a plain numeric string to seconds.
    If the format is invalid, return None.
    """
    try:
        if isinstance(time_str, str):
            if ":" in time_str:
                # Handle time in HH:MM:SS or MM:SS format
                parts = list(map(int, time_str.split(":")))
                if len(parts) == 3:  # HH:MM:SS
                    return int(parts[0] * 3600 + parts[1] * 60 + parts[2])
                elif len(parts) == 2:  # MM:SS
                    return int(parts[0] * 60 + parts[1])
            elif time_str.isdigit():  # Plain numeric string
                return int(time_str)
        elif isinstance(time_str, (int, float)):  # Already in seconds as a number
            return int(time_str)
    except (ValueError, AttributeError):
        pass
    return None

def remove_subseconds(timestamp):
    return timestamp.split('.')[0] + timestamp[-6:] if '.' in timestamp else timestamp

def preprocess_df(
    df,
    date_cols=None,
    int_cols=None,
    float_cols=None,
    bool_cols=None,
    lower_cols=None,
    str_cols=None,
    struct_cols=None,
    to_epoch_cols=None
):
    df = df.copy()

    # Process date columns
    if date_cols:
        for col in date_cols:
            df[col] = df[col].fillna('1971-01-01T00:00:00-00:00')
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
            if to_epoch_cols and col in to_epoch_cols:
                # Convert datetime to epoch seconds
                print(f'Converting {col} to epoch seconds')
                df[col + '_epoch'] = df[col].apply(lambda x: int(x.timestamp()) if pd.notna(x) else 0)
    # Process int columns
    if int_cols:
        for col in int_cols:
            fill_value = 0 if col == 'clone_of' else -1
            df[col] = df[col].fillna(fill_value).astype('int')

    # Process float columns
    if float_cols:
        for col in float_cols:
            df[col] = df[col].fillna(-1.0).astype('float')

    # Process boolean columns
    if bool_cols:
        for col in bool_cols:
            df[col] = df[col].fillna(False).astype('bool')

    # Process lowercase string columns
    if lower_cols:
        for col in lower_cols:
            df[col] = df[col].str.lower()

    # Process general string columns
    if str_cols:
        for col in str_cols:
            df[col] = df[col].fillna('').astype('str')

    # Process struct columns
    if struct_cols:
        for col in struct_cols:
            # Check if the column has valid struct rows
            if df[col].notna().any():
                struct_keys = df[col].dropna().iloc[0].keys()  # Safely get keys from the first non-null struct
                for key in struct_keys:
                    if key == 'length':
                        df['length_in_seconds'] = (
                            df[col]
                            .apply(lambda x: time_to_seconds(x.get(key)) if x else None)
                            .fillna(0)
                            .astype(int)
                        )
                    df[col + '_' + key] = df[col].apply(lambda x: x.get(key) if x else None)

    return df

def preprocess_dataframe(df_config):
    """
    Preprocess a DataFrame based on its configuration.

    Args:
        df_config (DataFrameConfig): Object containing the DataFrame and its preprocessing configuration.
    
    Returns:
        pd.DataFrame: Preprocessed DataFrame.
    """
    df = df_config.dataframe.copy()
    config = df_config.config

    # Process date columns
    if 'date_cols' in config:
        for col in config['date_cols']:
            df[col] = df[col].fillna('1971-01-01T00:00:00-00:00')
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
            if 'to_epoch_cols' in config and col in config['to_epoch_cols']:
                df[col + '_epoch'] = df[col].apply(lambda x: int(x.timestamp()) if pd.notna(x) else 0)

    # Process int columns
    if 'int_cols' in config:
        for col in config['int_cols']:
            fill_value = 0 if col == 'clone_of' else -1
            df[col] = df[col].fillna(fill_value).astype(int)
            
    # Process float columns
    if 'float_cols' in config:
        for col in config['float_cols']:
            df[col] = df[col].fillna(-1.0).astype('float')

    # Process boolean columns
    if 'bool_cols' in config:
        for col in config['bool_cols']:
            df[col] = df[col].fillna(False).astype('bool')

    # Process lowercase string columns
    if 'lower_cols' in config:
        for col in config['lower_cols']:
            df[col] = df[col].str.lower()

    # Process general string columns
    if 'str_cols' in config:
        for col in config['str_cols']:
            df[col] = df[col].fillna('').astype('str')

    # Process struct columns
    if 'struct_cols' in config:
        for col in config['struct_cols']:
            if df[col].notna().any():
                struct_keys = df[col].dropna().iloc[0].keys()
                for key in struct_keys:
                    if key == 'length':
                        df['length_in_seconds'] = (
                            df[col]
                            .apply(lambda x: time_to_seconds(x.get(key)) if x else None)
                            .fillna(0)
                            .astype(int)
                        )
                    df[col + '_' + key] = df[col].apply(lambda x: x.get(key) if x else None)

    return df


def delete_all_dataframes():
    """
    Delete all pandas DataFrame objects in the global namespace and trigger garbage collection.
    """
    # Get a list of all variables in the global namespace
    global_vars = globals()
    
    # Identify variables that are pandas DataFrames
    df_vars = [var for var in global_vars if isinstance(global_vars[var], pd.DataFrame)]
    
    # Delete each DataFrame variable
    for var in df_vars:
        print(f"Deleting DataFrame: {var}")
        del global_vars[var]
    
    # Trigger garbage collection
    gc.collect()
    print("Garbage collection completed. All DataFrame objects have been deleted.")
    
def clean_encodings_df(df):
    df['attributes_description'] = df['attributes_description'].fillna('') if 'attributes_description' in df.columns else ''

    # Define conditions
    conditions = [
        df['attributes_product_code'].notnull(),
        df['attributes_product_code'].isnull() & df['attributes_product_name'].notnull(),
        df['attributes_product_code'].isnull() & df['attributes_product_name'].isnull() & df['attributes_donovan_agency_product_code'].notnull(),
        df['attributes_product_code'].isnull() & df['attributes_product_name'].isnull() & df['attributes_donovan_agency_product_code'].isnull() & df['attributes_description'].notnull() & df['attributes_description'].str.len() > 10 & ~df['attributes_description'].str.startswith(('TV', 'RA')),
        df['attributes_product_code'].isnull() & df['attributes_product_name'].isnull() & df['attributes_donovan_agency_product_code'].isnull() & df['attributes_description'].notnull() & df['attributes_description'].str.len() > 10 & df['attributes_description'].str.startswith(('TV', 'RA'))
    ]

    # Define corresponding values
    choices = [
        df['attributes_product_code'],
        df['attributes_product_name'],
        df['attributes_donovan_agency_product_code'],
        df['attributes_description'].str[26:30].str.strip(),
        df['attributes_description'].str[6:10].str.strip()
    ]

    # Apply conditions and choices to create the new column
    df['product_code'] = np.select(conditions, choices, default=None)



    # Define conditions
    conditions = [
        df['attributes_isci'].notnull(),
        df['attributes_isci'].isnull() & df['attributes_project_name'].notnull(),
        df['attributes_isci'].isnull() & df['attributes_project_name'].isnull() & df['attributes_description'].notnull() & df['attributes_description'].str.len() > 10 & ~df['attributes_description'].str.startswith(('TV', 'RA')),
        df['attributes_isci'].isnull() & df['attributes_project_name'].isnull() & df['attributes_description'].notnull() & df['attributes_description'].str.len() > 10 & df['attributes_description'].str.startswith(('TV', 'RA'))
    ]

    # Define corresponding values
    choices = [
        df['attributes_isci'],
        df['attributes_project_name'],
        df['attributes_description'].str[8:18].str.strip(),
        df['attributes_description'].str[18:38].str.strip()
    ]

    # Apply conditions and choices to create the new column
    df['isci'] = np.select(conditions, choices, default=None)


    # Define conditions
    conditions = [
        df['attributes_advertiser'].notnull(),
        df['attributes_advertiser'].isnull() & df['attributes_client_code'].notnull(),
        df['attributes_advertiser'].isnull() & df['attributes_client_code'].isnull() & df['attributes_donovan_agency_advertiser_code'].notnull(),
        df['attributes_advertiser'].isnull() & df['attributes_project_name'].isnull() & df['attributes_donovan_agency_advertiser_code'].isnull() & df['attributes_description'].notnull() & df['attributes_description'].str.len() > 10 & ~df['attributes_description'].str.startswith(('TV', 'RA')),
        df['attributes_advertiser'].isnull() & df['attributes_project_name'].isnull() & df['attributes_donovan_agency_advertiser_code'].isnull() & df['attributes_description'].notnull() & df['attributes_description'].str.len() > 10 & df['attributes_description'].str.startswith(('TV', 'RA'))
    ]

    # Define corresponding values
    choices = [
        df['attributes_advertiser'],
        df['attributes_client_code'],
        df['attributes_donovan_agency_advertiser_code'],
        df['attributes_description'].str[22:26].str.strip(),
        df['attributes_description'].str[2:6].str.strip()
    ]

    # Apply conditions and choices to create the new column
    df['advertiser'] = np.select(conditions, choices, default=None)
    return df

# def clean_encodings_df(df, config=None, use_dask=True):
#     """
#     Cleans the encodings DataFrame with configurable conditions.
    
#     Args:
#         df (pd.DataFrame or dd.DataFrame): The DataFrame to clean.
#         config (dict): A dictionary specifying the columns, conditions, and choices.
#         use_dask (bool): Whether to use Dask for parallelized processing.
    
#     Returns:
#         pd.DataFrame or dd.DataFrame: The cleaned DataFrame.
#     """
#     # Convert to Dask DataFrame if use_dask is True
#     if use_dask:
#         df = dd.from_pandas(df, npartitions=8)  # Adjust partitions based on available memory and cores
    
#     # Fill 'attributes_description' if it exists
#     if 'attributes_description' in df.columns:
#         df['attributes_description'] = df['attributes_description'].fillna('')

#     # Default configuration if not provided
#     if config is None:
#         config = {
#             'product_code': {
#                 'conditions': [
#                     df['attributes_product_code'].notnull(),
#                     df['attributes_product_code'].isnull() & df['attributes_product_name'].notnull(),
#                     df['attributes_product_code'].isnull() & df['attributes_product_name'].isnull() & df['attributes_donovan_agency_product_code'].notnull(),
#                     df['attributes_product_code'].isnull() & df['attributes_product_name'].isnull() & df['attributes_donovan_agency_product_code'].isnull()
#                     & df['attributes_description'].notnull() & (df['attributes_description'].str.len() > 10)
#                     & ~df['attributes_description'].str.startswith(('TV', 'RA')),
#                     df['attributes_product_code'].isnull() & df['attributes_product_name'].isnull() & df['attributes_donovan_agency_product_code'].isnull()
#                     & df['attributes_description'].notnull() & (df['attributes_description'].str.len() > 10)
#                     & df['attributes_description'].str.startswith(('TV', 'RA')),
#                 ],
#                 'choices': [
#                     df['attributes_product_code'],
#                     df['attributes_product_name'],
#                     df['attributes_donovan_agency_product_code'],
#                     df['attributes_description'].str[26:30].str.strip(),
#                     df['attributes_description'].str[6:10].str.strip(),
#                 ]
#             },
#             # Add other configurations here...
#         }
    
#     # Apply each field's conditions and choices
#     for column, params in config.items():
#         conditions = params['conditions']
#         choices = params['choices']

#         # Combine conditions and choices into a Dask Series
#         result = df[conditions[0]].astype(object)  # Initialize result
#         for condition, choice in zip(conditions[1:], choices[1:]):
#             result = result.where(~condition, choice)
        
#         # Assign the result back to the DataFrame
#         df[column] = result
    
#     # Convert back to Pandas if Dask is used
#     if use_dask:
#         df = df.compute()
    
#     return df

def clean_encodings_df_cudf(df, config=None):
    """
    Cleans the encodings DataFrame using cuDF for GPU acceleration.
    
    Args:
        df (cudf.DataFrame): The cuDF DataFrame to clean.
        config (dict): A dictionary specifying the columns, conditions, and choices.
    
    Returns:
        cudf.DataFrame: The cleaned DataFrame.
    """
    # Fill 'attributes_description' if it exists
    if 'attributes_description' in df.columns:
        df['attributes_description'] = df['attributes_description'].fillna('')

    # Default configuration if not provided
    if config is None:
        config = {
            'product_code': {
                'conditions': [
                    df['attributes_product_code'].notnull(),
                    df['attributes_product_code'].isnull() & df['attributes_product_name'].notnull(),
                    df['attributes_product_code'].isnull() & df['attributes_product_name'].isnull() & df['attributes_donovan_agency_product_code'].notnull(),
                    df['attributes_product_code'].isnull() & df['attributes_product_name'].isnull() & df['attributes_donovan_agency_product_code'].isnull()
                    & df['attributes_description'].notnull() & (df['attributes_description'].str.len() > 10)
                    & ~df['attributes_description'].str.contains(r'^TV|^RA'),
                    df['attributes_product_code'].isnull() & df['attributes_product_name'].isnull() & df['attributes_donovan_agency_product_code'].isnull()
                    & df['attributes_description'].notnull() & (df['attributes_description'].str.len() > 10)
                    & df['attributes_description'].str.contains(r'^TV|^RA'),
                ],
                'choices': [
                    df['attributes_product_code'],
                    df['attributes_product_name'],
                    df['attributes_donovan_agency_product_code'],
                    df['attributes_description'].str.slice(26, 30).str.strip(),
                    df['attributes_description'].str.slice(6, 10).str.strip(),
                ]
            },
            'isci': {
                'conditions': [
                    df['attributes_isci'].notnull(),
                    df['attributes_isci'].isnull() & df['attributes_project_name'].notnull(),
                    df['attributes_isci'].isnull() & df['attributes_project_name'].isnull() & df['attributes_description'].notnull()
                    & (df['attributes_description'].str.len() > 10) & ~df['attributes_description'].str.contains(r'^TV|^RA'),
                    df['attributes_isci'].isnull() & df['attributes_project_name'].isnull() & df['attributes_description'].notnull()
                    & (df['attributes_description'].str.len() > 10) & df['attributes_description'].str.contains(r'^TV|^RA'),
                ],
                'choices': [
                    df['attributes_isci'],
                    df['attributes_project_name'],
                    df['attributes_description'].str.slice(8, 18).str.strip(),
                    df['attributes_description'].str.slice(18, 38).str.strip(),
                ]
            },
            'advertiser': {
                'conditions': [
                    df['attributes_advertiser'].notnull(),
                    df['attributes_advertiser'].isnull() & df['attributes_client_code'].notnull(),
                    df['attributes_advertiser'].isnull() & df['attributes_client_code'].isnull() & df['attributes_donovan_agency_advertiser_code'].notnull(),
                    df['attributes_advertiser'].isnull() & df['attributes_project_name'].isnull() & df['attributes_donovan_agency_advertiser_code'].isnull()
                    & df['attributes_description'].notnull() & (df['attributes_description'].str.len() > 10)
                    & ~df['attributes_description'].str.contains(r'^TV|^RA'),
                    df['attributes_advertiser'].isnull() & df['attributes_project_name'].isnull() & df['attributes_donovan_agency_advertiser_code'].isnull()
                    & df['attributes_description'].notnull() & (df['attributes_description'].str.len() > 10)
                    & df['attributes_description'].str.contains(r'^TV|^RA'),
                ],
                'choices': [
                    df['attributes_advertiser'],
                    df['attributes_client_code'],
                    df['attributes_donovan_agency_advertiser_code'],
                    df['attributes_description'].str.slice(22, 26).str.strip(),
                    df['attributes_description'].str.slice(2, 6).str.strip(),
                ]
            }
        }
    
    # Apply each field's conditions and choices
    for column, params in config.items():
        result = cudf.Series(None, index=df.index)  # Initialize with nulls
        for condition, choice in zip(params['conditions'], params['choices']):
            result = result.where(~condition, choice)
        df[column] = result
    
    return df

def clean_sfdc_df(df, id_col=None, name_col=None):

    for col in df.columns:
        new_col = col.replace('__c', '').lower()
        df = df.rename(columns={col: new_col})
        if new_col == 'id' and id_col:
            df.rename(columns={new_col: id_col}, inplace=True)
        if new_col == 'name' and name_col:
            df.rename(columns={new_col: name_col}, inplace=True)
        if new_col == 'account':
            df.rename(columns={new_col: 'sfdc_account_id'}, inplace=True)
        if new_col == 'related_rate_card':
            df.rename(columns={new_col: 'sfdc_rate_card_id'}, inplace=True)
         
    return df
    
# # Insert salesforce
# def insert_salesforce(object_name, records, batch_size=500):
#     """
#     Insert records into Salesforce in batches with error handling and logging.

#     Args:
#     - object_name (str): The Salesforce object name.
#     - records (list of dict): List of records to insert.
#     - batch_size (int): Number of records to insert per batch. Default is 200.

#     Returns:
#     - None
#     """
#     total_records = len(records)
#     print(f"Total records to insert: {total_records}")

#     for i in range(0, total_records, batch_size):
#         batch = records[i:i + batch_size]
#         success_count = 0
#         error_count = 0

#         for record in batch:
#             try:
#                 record = clean_record(record)
#                 sf.__getattr__(object_name).create(record)
#                 success_count += 1
#             except SalesforceMalformedRequest as e:
#                 error_count += 1
#                 print(f"Error inserting record: {record}")
#                 print(f"Error message: {e.content}")
#             except Exception as e:
#                 error_count += 1
#                 print(f"Unexpected error inserting record: {record}")
#                 print(f"Error message: {str(e)}")

#         print(f"Batch {i//batch_size + 1}: Successfully inserted {success_count} records, {error_count} errors")

#     print("Insert operation completed.")

# def update_salesforce(object_name, records, batch_size=200):
#     total_records = len(records)
#     print(f"Total records to update: {total_records}")

#     for i in range(0, total_records, batch_size):
#         batch = records[i:i + batch_size]
#         success_count = 0
#         error_count = 0

#         for record in batch:
#             record_id = record.pop('Id')  # Remove the Id field from the record data
#             record = clean_record(record)
#             try:
#                 sf.__getattr__(object_name).update(record_id, record)
#                 success_count += 1
#             except SalesforceMalformedRequest as e:
#                 error_count += 1
#                 print(f"Error updating record: {record}")
#                 print(f"Error message: {e.content}")
#             except Exception as e:
#                 error_count += 1
#                 print(f"Unexpected error updating record: {record}")
#                 print(f"Error message: {str(e)}")

#         print(f"Batch {i//batch_size + 1}: Successfully updated {success_count} records, {error_count} errors")

#     print("Update operation completed.")

# # Define a function to delete records where a specific field is empty or null
# def delete_records_with_null_field(sf, object_name, field_name):
#     query = f"SELECT Id FROM {object_name} WHERE {field_name} = null"
#     result = sf.query_all(query)
#     records = result['records']

#     # Extract the Ids of the records to be deleted
#     ids_to_delete = [record['Id'] for record in records]

#     # Delete the records
#     for record_id in ids_to_delete:
#         try:
#             sf.__getattr__(object_name).delete(record_id)
#             print(f"Successfully deleted record with Id: {record_id}")
#         except Exception as e:
#             print(f"Error deleting record with Id: {record_id}")
#             print(f"Error message: {e}")

# # Define a function to sync data
# def sync_data_ids(gbq_query, gbq_to_sfdc_field_map, sfdc_object_name, sfdc_external_id_field):
#     merged_df = pd.DataFrame()
#     object_to_sync = None
#     # Fetch data from GBQ
#     gbq_data = fetch_gbq_data(gbq_query)

#     # Map GBQ fields to Salesforce fields
#     gbq_data.rename(columns=gbq_to_sfdc_field_map, inplace=True)

#     # Convert relevant fields to timestamps and fill NA with "1990-01-01 00:00:00"
#     if sfdc_object_name != 'Task':
#         for col in gbq_data.columns:
#             if col.startswith('Last_Updated__c'):
#                 gbq_data[col] = pd.to_datetime(gbq_data[col], errors='coerce').fillna(pd.Timestamp('1990-01-01 00:00:00'))
#                 if gbq_data[col].dt.tz is None:  # Check if tz-naive
#                     gbq_data[col] = gbq_data[col].dt.tz_localize('UTC')
#                 else:  # Convert tz-aware to UTC
#                     gbq_data[col] = gbq_data[col].dt.tz_convert('UTC')

#     # Ensure the external ID field is integer type
#     gbq_data[sfdc_external_id_field] = gbq_data[sfdc_external_id_field].astype('Int64')

#     print(f"GBQ Record Count: {len(gbq_data)}")
#     sfdc_data = pd.DataFrame()

#     # Fetch data from Salesforce
#     sfdc_fields = list(gbq_to_sfdc_field_map.values())
#     sfdc_raw = fetch_sfdc_data(sfdc_object_name, sfdc_fields)
#     if sfdc_raw:
#         sfdc_data = pd.DataFrame(sfdc_raw)
#         print("SFDC Data (raw):")
#     print(f"SFDC Record Count: {len(sfdc_data)}")

#     # Ensure the external ID field is integer type in Salesforce data
#     if len(sfdc_data) > 0:
#         if sfdc_object_name != 'Task':
#             sfdc_data[sfdc_external_id_field] = sfdc_data[sfdc_external_id_field].astype('Int64')
#         else:
#             sfdc_data[sfdc_external_id_field] = sfdc_data[sfdc_external_id_field].astype('Float64')

#         # Convert relevant fields to timestamps and fill NA with "1990-01-01 00:00:00" in Salesforce data
#         if sfdc_object_name != 'Task':
#             for col in sfdc_data.columns:
#                 if col.startswith('Last_Updated__c'):
#                     sfdc_data[col] = pd.to_datetime(sfdc_data[col], errors='coerce').fillna(pd.Timestamp('1990-01-01 00:00:00'))
#                     if sfdc_data[col].dt.tz is None:  # Check if tz-naive
#                         sfdc_data[col] = sfdc_data[col].dt.tz_localize('UTC')
#                     else:  # Convert tz-aware to UTC
#                         sfdc_data[col] = sfdc_data[col].dt.tz_convert('UTC')

#         # Drop rows with NaN values in the external ID field
#         sfdc_data.dropna(subset=[sfdc_external_id_field], inplace=True)

#     if not sfdc_data.empty:
#         print(f"SFDC Data to process: {len(sfdc_data)}")
#     else:
#         print('No SFDC records survived')

#     # Merge GBQ data with Salesforce data on the external ID field
#     if not sfdc_data.empty:
#         merged_df = pd.merge(gbq_data, sfdc_data, how='left', on=sfdc_external_id_field, suffixes=('', '_sfdc'))

#         # Filter records to insert (new records)
#         new_records = merged_df[merged_df['Id'].isnull()].copy()
#     else:
#         new_records = gbq_data
#         print(f"New Records to process: {len(new_records)}")

#     # Filter records to update (existing records)
#     existing_records = pd.DataFrame()
#     if not merged_df.empty:
#         if sfdc_object_name != 'Task':
#             existing_records = merged_df[(merged_df['Last_Updated__c'] > merged_df['Last_Updated__c_sfdc']) & merged_df['Id'].notnull()].copy()
#             print(f'Existing records to update: {len(existing_records)}')
#         else:
#             existing_records = merged_df[merged_df['Id'].notnull()].copy()
#             print(f'Existing records to update: {len(existing_records)}')
#     else:
#         print('No existing records to update')

#     # Prepare new records for insertion
#     if not new_records.empty:
#         if sfdc_object_name != 'Task':
#             new_records['Last_Updated__c'] = new_records['Last_Updated__c'].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
#         new_records = new_records.drop(columns=[col for col in new_records.columns if col.endswith('_sfdc')] + ['Id'], errors='ignore')
#         new_records = new_records.to_dict('records')
#         print(f"New records to be added: {len(new_records)}")
#         # Insert new records to Salesforce
#         if new_records:
#             insert_salesforce(sfdc_object_name, new_records)
#             object_to_sync = sfdc_object_name
#     else:
#         new_records = []
#         print('No new records to be added')

#     # Update existing records in Salesforce
#     if not existing_records.empty:
#         if sfdc_object_name != 'Task':
#             existing_records['Last_Updated__c'] = existing_records['Last_Updated__c'].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
#         existing_records = existing_records.drop(columns=[col for col in existing_records.columns if col.endswith('_sfdc')], errors='ignore')
#         existing_records = existing_records.to_dict('records')
#         print(f"Existing records to be updated: {len(existing_records)}")
#         update_salesforce(sfdc_object_name, existing_records)
#         object_to_sync = sfdc_object_name
#     else:
#         existing_records = []
#         print('No existing records to update')

#     return object_to_sync


# # Define a function to sync data
# def sync_data_ids_tasks(gbq_data, gbq_to_sfdc_field_map, sfdc_object_name, sfdc_external_id_field):
#     merged_df = pd.DataFrame()

#     # Map GBQ fields to Salesforce fields
#     gbq_data.rename(columns=gbq_to_sfdc_field_map, inplace=True)

#     # Ensure the external ID field is integer type

#     gbq_data[sfdc_external_id_field] = gbq_data[sfdc_external_id_field].astype('Float64')
#     # print("GBQ Data:")
#     # print(gbq_data.head())
#     print(f"GBQ Record Count: {len(gbq_data)}")
#     sfdc_data = pd.DataFrame()
#     # Fetch data from Salesforce
#     sfdc_fields = list(gbq_to_sfdc_field_map.values())
#     sfdc_raw = fetch_sfdc_data(sfdc_object_name, sfdc_fields)
#     if sfdc_raw:
#         sfdc_data = pd.DataFrame(sfdc_raw)
#         print("SFDC Data (raw):")
#     # print(sfdc_data.head())
#     print(f"SFDC Record Count: {len(sfdc_data)}")

#     # Ensure the external ID field is integer type in Salesforce data
#     if len(sfdc_data) > 0:

#         sfdc_data[sfdc_external_id_field] = sfdc_data[sfdc_external_id_field].astype('Float64')

#         # Drop rows with NaN values in the external ID field
#         sfdc_data.dropna(subset=[sfdc_external_id_field], inplace=True)
#     if not sfdc_data.empty:
#         print(f"SFDC Data to process: {len(sfdc_data)}")
#         # print(sfdc_data.head())
#     else:
#         print('No SFDC records survived')

#     # Merge GBQ data with Salesforce data on the external ID field
#     if not sfdc_data.empty:
#         merged_df = pd.merge(gbq_data, sfdc_data, how='left', on=sfdc_external_id_field, suffixes=('', '_sfdc'))

#         # Filter records to insert (new records)
#         new_records = merged_df[merged_df['Id'].isnull()].copy()
#     else:
#         new_records = gbq_data

#         print(f"New Records to process: {len(new_records)}")
#     # print(new_records.head())
#     # print(new_records.dtypes)

#     # Filter records to update (existing records)
#     existing_records = pd.DataFrame()
#     if not merged_df.empty:
#         existing_records = merged_df[merged_df['Id'].notnull()].copy()
#         print(f'Existing records to update: {len(existing_records)}')
#     else:
#         print('No existing records to update')

#     # Prepare new records for insertion
#     if not new_records.empty:
#         new_records = new_records.drop(columns=[col for col in new_records.columns if col.endswith('_sfdc')] + ['Id'], errors='ignore')
#         new_records = new_records.to_dict('records')
#     else:
#         new_records = []
#         print('No new records to be added')

#     # print(new_records)
#     # Prepare existing records for update
#     if not existing_records.empty:
#         update_fields = list(gbq_to_sfdc_field_map.values())
#         update_fields.remove(sfdc_external_id_field)  # External ID field should not be updated

#         # Drop _sfdc suffix fields and prepare the final update records
#         existing_records = existing_records[update_fields + ['Id']].drop(columns=[f'{col}_sfdc' for col in update_fields], errors='ignore')
#         existing_records = existing_records.to_dict('records')
#     else:
#         existing_records = []  # Ensure existing_records is an empty list if no records to update

#     print(f"Existing records to be updated: {len(existing_records)}" if existing_records else "No existing records to update")

#     # Insert new records to Salesforce
#     if new_records:
#         insert_salesforce(sfdc_object_name, new_records)

#     # Update existing records in Salesforce
#     if existing_records:
#         update_salesforce(sfdc_object_name, existing_records)

# # continue from here
# # Define a function to sync data
# def sync_data_chars(gbq_query, gbq_to_sfdc_field_map, sfdc_object_name, sfdc_external_id_field):
#     merged_df = pd.DataFrame()
#     # Fetch data from GBQ
#     gbq_data = fetch_gbq_data(gbq_query)

#     # Map GBQ fields to Salesforce fields
#     gbq_data.rename(columns=gbq_to_sfdc_field_map, inplace=True)

#     # Convert relevant fields to timestamps and fill NA with "1990-01-01 00:00:00"
#     for col in gbq_data.columns:
#         if col.startswith('Last_Updated__c'):
#             gbq_data[col] = pd.to_datetime(gbq_data[col], errors='coerce').fillna(pd.Timestamp('1990-01-01 00:00:00'))

#     # Ensure the external ID field is integer type

#     gbq_data[sfdc_external_id_field] = gbq_data[sfdc_external_id_field]
#     print("GBQ Data:")
#     print(gbq_data.head())

#     # Fetch data from Salesforce
#     sfdc_fields = list(gbq_to_sfdc_field_map.values())
#     sfdc_raw = fetch_sfdc_data(sfdc_object_name, sfdc_fields)
#     sfdc_data = pd.DataFrame(sfdc_raw)
#     print("SFDC Data (raw):")
#     print(sfdc_data.head())

#     # Ensure the external ID field is integer type in Salesforce data
#     sfdc_data[sfdc_external_id_field] = sfdc_data[sfdc_external_id_field].astype('Int64')

#     # Convert relevant fields to timestamps and fill NA with "1990-01-01 00:00:00" in Salesforce data
#     for col in sfdc_data.columns:
#         if col.startswith('Last_Updated__c'):
#             sfdc_data[col] = pd.to_datetime(sfdc_data[col], errors='coerce').fillna(pd.Timestamp('1990-01-01 00:00:00'))
#             # Uncomment following line to force update
#             # sfdc_data[col] = pd.Timestamp('1990-01-01 00:00:00')

#     # Drop rows with NaN values in the external ID field
#     sfdc_data.dropna(subset=[sfdc_external_id_field], inplace=True)
#     if not sfdc_data.empty:
#         print("SFDC Data (processed):")
#         print(sfdc_data.head())
#     else:
#         print('No SFDC records survived')

#     # Merge GBQ data with Salesforce data on the external ID field
#     if not sfdc_data.empty:
#         merged_df = pd.merge(gbq_data, sfdc_data, how='left', on=sfdc_external_id_field, suffixes=('', '_sfdc'))
#         print("Merged Data:")
#         print(merged_df.head())
#         print(merged_df.dtypes)

#         # Filter records to insert (new records)
#         new_records = merged_df[merged_df['Id'].isnull()].copy()
#     else:
#         new_records = gbq_data

#     print("New Records:")
#     print(new_records.head())
#     print(new_records.dtypes)

#     # Filter records to update (existing records)
#     existing_records = pd.DataFrame()
#     if not merged_df.empty:
#         existing_records = merged_df[(merged_df['Last_Updated__c'] > merged_df['Last_Updated__c_sfdc']) & merged_df['Id'].notnull()].copy()
#         print(f'Existing records to update: {len(existing_records)}')

#     # Prepare new records for insertion
#     if not new_records.empty:
#         new_records['Last_Updated__c'] = new_records['Last_Updated__c'].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
#         new_records = new_records.drop(columns=[col for col in new_records.columns if col.endswith('_sfdc')] + ['Id'], errors='ignore')
#         new_records = new_records.to_dict('records')
#         print(f"New records to be added: {len(new_records)}")
#     else:
#         new_records = []

#     print(new_records)
#     # Prepare existing records for update
#     if not existing_records.empty:
#         update_fields = list(gbq_to_sfdc_field_map.values())
#         update_fields.remove(sfdc_external_id_field)  # External ID field should not be updated

#         # Convert Last_Updated__c field to the desired string format
#         existing_records['Last_Updated__c'] = existing_records['Last_Updated__c'].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]

#         # Drop _sfdc suffix fields and prepare the final update records
#         existing_records = existing_records[update_fields + ['Id']].drop(columns=[f'{col}_sfdc' for col in update_fields], errors='ignore')
#         existing_records = existing_records.to_dict('records')
#     else:
#         existing_records = []  # Ensure existing_records is an empty list if no records to update

#     print(f"Existing records to be updated: {len(existing_records)}" if existing_records else "No existing records to update")

#     # Insert new records to Salesforce
#     if new_records:
#         insert_salesforce(sfdc_object_name, new_records)

#     # Update existing records in Salesforce
#     if existing_records:
#         update_salesforce(sfdc_object_name, existing_records)

# def update_bvs_customers_lookup_on_format(sf):
#     # Query all BVS_Formats__c records without the BVS_Customer__c lookup field set
#     query = "SELECT Id, Customer_ID__c FROM BVS_Format__c WHERE BVS_Customer__c = null"
#     bvs_format_records = sf.query_all(query)
#     print(len(bvs_format_records))

#     for record in bvs_format_records['records']:
#         bvs_format_record_id = record['Id']
#         customer_id = record['Customer_ID__c']

#         # Search for the BVS_Customer__c record with matching customer_id__c
#         customer_query = f"SELECT Id FROM BVS_Customer__c WHERE customer_id__c = {customer_id}"
#         result = sf.query(customer_query)

#         if result['totalSize'] == 1:
#             # If a matching BVS_Customer__c record is found, get its Id
#             bvs_customer_id = result['records'][0]['Id']

#             # Update the BVS_Formats__c record with the BVS_Customer lookup field
#             sf.BVS_Format__c.update(bvs_format_record_id, {'BVS_Customer__c': bvs_customer_id})
#             print(f"Updated BVS_Formats__c record {bvs_format_record_id} with BVS_Customer__c {bvs_customer_id}")
#         else:
#             # Handle case where no matching or multiple records are found
#             print(f"No matching or multiple BVS_Customer__c records found for Customer_ID__c: {customer_id}")

# def update_bvs_profile_lookup_on_format(sf):
#     # Query all BVS_Formats__c records without the BVS_Customer__c lookup field set
#     query = "SELECT Id, Profile_ID__c FROM BVS_Format__c WHERE BVS_Profile__c = null"
#     bvs_format_records = sf.query_all(query)
#     print(len(bvs_format_records))

#     for record in bvs_format_records['records']:
#         bvs_format_record_id = record['Id']
#         profile_id = record['Profile_ID__c']

#         # Search for the BVS_Profile__c record with matching BVS_Profile_ID__c
#         profile_query = f"SELECT Id FROM BVS_Profile__c WHERE BVS_Profile_ID__c = {profile_id}"
#         result = sf.query(profile_query)

#         if result['totalSize'] == 1:
#             # If a matching BVS_Profile__c record is found, get its Id
#             bvs_profile_id = result['records'][0]['Id']

#             # Update the BVS_Formats__c record with the BVS_Profile lookup field
#             sf.BVS_Format__c.update(bvs_format_record_id, {'BVS_Profile__c': bvs_profile_id})
#             print(f"Updated BVS_Formats__c record {bvs_format_record_id} with BVS_Profile__c {bvs_profile_id}")
#         else:
#             # Handle case where no matching or multiple records are found
#             print(f"No matching or multiple BVS_Profile__c records found for BVS_Profile_ID__c: {profile_id}")

# def sync_data_with_precalculated_id(gbq_query, gbq_to_sfdc_field_map, sfdc_object_name, sfdc_external_id_field):
#     # Fetch data from GBQ
#     object_to_sync = None
#     gbq_data = fetch_gbq_data(gbq_query)

#     # Map GBQ fields to Salesforce fields
#     gbq_data.rename(columns=gbq_to_sfdc_field_map, inplace=True)

#     # print("GBQ Data:")
#     # print(gbq_data.head())

#     # Fetch data from Salesforce
#     sfdc_fields = list(gbq_to_sfdc_field_map.values())
#     sfdc_raw = fetch_sfdc_data(sfdc_object_name, sfdc_fields)
#     sfdc_data = pd.DataFrame(sfdc_raw)
#     # print("SFDC Data (raw):")
#     # print(sfdc_data.head())

#     # Drop rows with NaN values in the external ID field in Salesforce data if data is not empty
#     if not sfdc_data.empty:
#         sfdc_data.dropna(subset=[sfdc_external_id_field], inplace=True)
#         print("SFDC Data (processed):")
#         # print(sfdc_data.head())
#     else:
#         print('No SFDC records survived')

#     # Merge GBQ data with Salesforce data on the external ID field
#     if not sfdc_data.empty:
#         merged_df = pd.merge(gbq_data, sfdc_data, how='left', on=sfdc_external_id_field, suffixes=('', '_sfdc'))
#         print("Merged Data:")
#         # print(merged_df.head())
#         # print(merged_df.dtypes)

#         # Filter records to insert (new records)
#         new_records = merged_df[merged_df['Id'].isnull()].copy()
#     else:
#         new_records = gbq_data

#     # print("New Records:")
#     # print(new_records.head())
#     # print(new_records.dtypes)

#     # Filter records to update (existing records)
#     existing_records = pd.DataFrame()
#     if not sfdc_data.empty and not merged_df.empty:
#         existing_records = merged_df[merged_df['Id'].notnull()].copy()
#         print(f'Existing records to update: {len(existing_records)}')

#     # Prepare new records for insertion
#     if not new_records.empty:
#         new_records = new_records.drop(columns=[col for col in new_records.columns if col.endswith('_sfdc')] + ['Id'], errors='ignore')
#         new_records = new_records.to_dict('records')
#         print(f"New records to be added: {len(new_records)}")
#     else:
#         new_records = []

#     print(new_records)
#     # Prepare existing records for update
#     if not existing_records.empty:
#         update_fields = list(gbq_to_sfdc_field_map.values())
#         update_fields.remove(sfdc_external_id_field)  # External ID field should not be updated

#         # Drop _sfdc suffix fields and prepare the final update records
#         existing_records = existing_records[update_fields + ['Id']].drop(columns=[f'{col}_sfdc' for col in update_fields], errors='ignore')
#         existing_records = existing_records.to_dict('records')
#     else:
#         existing_records = []  # Ensure existing_records is an empty list if no records to update

#     print(f"Existing records to be updated: {len(existing_records)}" if existing_records else "No existing records to update")

#     # Insert new records to Salesforce
#     if new_records:
#         insert_salesforce(sfdc_object_name, new_records)
#         object_to_sync = sfdc_object_name

#     # Update existing records in Salesforce
#     if existing_records:
#         update_salesforce(sfdc_object_name, existing_records)
#         object_to_sync = sfdc_object_name
#     return object_to_sync

# SHORTIO_API_KEY = userdata.get('SHORTIO_API_KEY')

# def get_short_url(long_url):
#     res = requests.post('https://api.short.io/links', json={
#         'domain': 'link.veil.global',
#         'originalURL': long_url,
#     }, headers = {
#         'authorization': SHORTIO_API_KEY,
#         'content-type': 'application/json'
#     }, )

#     res.raise_for_status()
#     data = res.json()
#     short_url = data['shortURL']
#     return short_url

# # # Update the BigQuery table
# # def update_bq_invoice_table_short_url(df, table_id):
# #     for index, row in df.iterrows():
# #         query = f"""
# #         UPDATE `{table_id}`
# #         SET excel_path_short = '{row['short_url']}'
# #         WHERE excel_path = '{row['long_url']}'
# #         AND excel_path_short IS NULL
# #         """
# #         query_job = bigquery_client.query(query)
# #         query_job.result()  # Wait for the query to complete

# def update_bq_invoice_table_short_url(df, table_id):

#     # Step 1: Write the DataFrame to a temporary table in BigQuery
#     temp_table_name = f"temp_table_{uuid.uuid4().hex}"
#     temp_table_id = f"{project_id}.{dataset_id}.{temp_table_name}"

#     # Ensure the temporary dataset exists
#     dataset_ref = bigquery.DatasetReference(bigquery_client.project, dataset_id)
#     try:
#         bigquery_client.get_dataset(dataset_ref)
#     except Exception:
#         dataset = bigquery.Dataset(dataset_ref)
#         bigquery_client.create_dataset(dataset)

#     # Load DataFrame to the temporary table
#     job_config = bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')
#     load_job = bigquery_client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
#     load_job.result()  # Wait for the load job to complete

#     # Step 2: Perform the batch update using a single query
#     query = f"""
#     UPDATE `{table_id}` AS T
#     SET excel_path_short = S.short_url
#     FROM `{temp_table_id}` AS S
#     WHERE T.excel_path = S.long_url
#     AND T.excel_path_short IS NULL
#     """
#     query_job = bigquery_client.query(query)
#     query_job.result()  # Wait for the query to complete

#     # Step 3: Delete the temporary table
#     bigquery_client.delete_table(temp_table_id)

# def update_bq_sfdc_opportunity_id(df, table_id):
#     dataset_id = 'avs_billing_process'
#     # Step 1: Write the DataFrame to a temporary table in BigQuery
#     temp_table_name = f"temp_table_{uuid.uuid4().hex}"
#     temp_table_id = f"{project_id}.{dataset_id}.{temp_table_name}"

#     # Ensure the temporary dataset exists
#     dataset_ref = bigquery.DatasetReference(bigquery_client.project, dataset_id)
#     try:
#         bigquery_client.get_dataset(dataset_ref)
#     except Exception:
#         dataset = bigquery.Dataset(dataset_ref)
#         bigquery_client.create_dataset(dataset)

#     # Load DataFrame to the temporary table
#     job_config = bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')
#     load_job = bigquery_client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
#     load_job.result()  # Wait for the load job to complete

#     # Step 2: Perform the batch update using a single query
#     query = f"""
#     UPDATE `{table_id}` AS T
#     SET sfdc_opportunity_id = S.sfdc_opportunity_id
#     FROM `{temp_table_id}` AS S
#     WHERE T.invoice_id = S.invoice_id
#     """
#     query_job = bigquery_client.query(query)
#     query_job.result()  # Wait for the query to complete

#     # Step 3: Delete the temporary table
#     bigquery_client.delete_table(temp_table_id)
