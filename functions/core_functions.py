
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
