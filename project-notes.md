### Create PARQUET Tablel in bq with metadata caching

```sql
-- Non Hive partitioned table with metadata caching
CREATE OR REPLACE EXTERNAL TABLE
  `next90-core-applications.omniData.sub_dma_maps`
  WITH CONNECTION `projects/next90-core-applications/locations/us-central1/connections/biglak-to-n90-data-lake`
  OPTIONS(
    format ="PARQUET",
    uris = ['gs://n90_veil_partner/advocado-looker/mongo-copies/sub_dma_maps/*'],
    max_staleness = INTERVAL 4 HOUR,
    metadata_cache_mode = 'AUTOMATIC'
  )
  ;

-- hive partitioned table without metadata caching
CREATE OR REPLACE EXTERNAL TABLE
  `next90-core-applications.omniData.wwtv_programs`
  WITH PARTITION COLUMNS
  OPTIONS(
    format ="PARQUET",
    uris = ['gs://n90_veil_partner/advocado-looker/wwtv/raw-programs/*'],
    hive_partition_uri_prefix = 'gs://n90_veil_partner/advocado-looker/wwtv/raw-programs'
  )
;
    ```

    