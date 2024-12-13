import pyarrow.dataset as ds
import pyarrow.fs as fs
import pandas as pd


def load_encodings_for_detections(storage_options, bucket_name, hive_table_path, platform='s3', scheme='https', region='bws-stl'):
    # Construct the S3 URI
    s3_uri = f"{bucket_name}/{hive_table_path}/"

    # Initialize the S3FileSystem with MinIO configuration
    access_key = storage_options.get('key')

    secret_key = storage_options.get('secret')
    endpoint_override = storage_options.get('client_kwargs').get('endpoint_url')
    s3_fs = fs.S3FileSystem(
        access_key=access_key,
        secret_key=secret_key,
        endpoint_override=endpoint_override,
        scheme=scheme,  # Use 'https' if SSL is enabled
        region=region  # MinIO requires a region; can be arbitrary
    )

    # Define the partitioning scheme (Hive-style)
    partitioning = ds.partitioning(
        flavor='hive'
    )

    # Create the dataset
    dataset = ds.dataset(
        s3_uri,
        format='parquet',
        partitioning=partitioning,
        filesystem=s3_fs
    )

    # Convert the dataset to a PyArrow Table
    table = dataset.to_table()
    # table_subset = table.select(['encoding_id', 'isci', 'aeis_id', 'encoded_timestamp', 'format_id', 'format_name', 'customer_id', 'customer_name', 'sfdc_account_id', 'sfdc_account_name', 'sfdc_advertiser_id','attributes_cable_estimate', 'attributes_spot_estimate',
    #                          'encoder_group_id',  'encoder_id', 'encoder_group_name', 'length_in_seconds',
    #                          'billing_last_updated', 'billing_last_audit_id', 'clone_of', 'segments_format_id_group'])

    # Optionally, convert to pandas DataFrame
    df = table_subset.to_pandas()

    return df