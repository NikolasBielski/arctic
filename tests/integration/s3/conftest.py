import pytest
import boto3

from moto import mock_s3

from arctic.s3.key_value_datastore import S3KeyValueStore
from arctic.s3.generic_version_store import register_versioned_storage, GenericVersionStore
from arctic.s3._kv_ndarray_store import KeyValueNdarrayStore
from arctic.s3._pandas_ndarray_store import PandasDataFrameStore, PandasSeriesStore, PandasPanelStore


@pytest.fixture()
def s3_bucket():
    return 'arctic2'


@pytest.fixture()
def s3_client():
    return boto3.client('s3')


@pytest.fixture()
def s3_store(s3_bucket, s3_client):
    with mock_s3():
        s3_client.create_bucket(Bucket=s3_bucket)
        s3_client.put_bucket_versioning(Bucket=s3_bucket,
                                        VersioningConfiguration={'MFADelete': 'Disabled',
                                                                 'Status': 'Enabled'})
        store = S3KeyValueStore(bucket=s3_bucket)
        yield store


@pytest.fixture()
def generic_version_store(library_name, s3_store):
    register_versioned_storage(KeyValueNdarrayStore)
    register_versioned_storage(PandasPanelStore)
    register_versioned_storage(PandasSeriesStore)
    register_versioned_storage(PandasDataFrameStore)
    return GenericVersionStore(library_name, backing_store=s3_store)
