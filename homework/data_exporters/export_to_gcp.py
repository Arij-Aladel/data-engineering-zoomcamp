from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path

import pyarrow as pa
import pyarrow.parquet as pq

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """

    now = kwargs.get('execution_date')
    # print(now)
    # print(now.date())
    # print(now.day)
    # print(now.strftime("%y/%m/%d"))

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/ny-rides-arij-mage.json"
bucket_name = 'arij-mage-zoomcamp'
object_id = 'ny-rides-arij' 
table_name = "ny_taxi_data"
root_path = f"{bucket_name}/{table_name}"


    now_fpath = now.strftime("%Y/%m/%d")
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'arij-mage-zoomcamp'
    object_key = f'{now_fpath}/green_taxi_trips.parquet'
    # print(object_key)

    df['tpep_pickup_date'] = df['tpep_pickup_datetime'].dt.date

    table = pa.Table.from_pandas(df)

    # get the filesystem
    gcs = pa.fs.GcsFileSystem()

    # write to GCS
    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['tpep_pickup_date'],
        filesystem=gcs
    )