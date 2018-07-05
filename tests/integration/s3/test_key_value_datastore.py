from datetime import datetime as dt, timedelta as dtd

import bson
import numpy as np
import numpy.testing as npt
import pytest
import boto3
from arctic.s3._kv_ndarray_store import KeyValueNdarrayStore
from arctic.s3.generic_version_store import GenericVersionStore
from mock import patch
from pymongo.server_type import SERVER_TYPE

from arctic.s3.key_value_datastore import S3KeyValueStore
from arctic.s3.generic_version_store import register_versioned_storage
from tests.integration.store.test_version_store import _query
from moto import mock_s3


@pytest.fixture()
def s3_bucket():
    return 'arctic2'


@pytest.fixture()
def s3_store(s3_bucket):
    store = S3KeyValueStore(bucket=s3_bucket)
    return store


@pytest.fixture()
def s3_client():
    return boto3.client('s3')


def setup_bucket(s3_bucket, s3_client):
    s3_client.create_bucket(Bucket=s3_bucket)
    s3_client.put_bucket_versioning(Bucket=s3_bucket,
                                    VersioningConfiguration={'MFADelete': 'Disabled',
                                                             'Status': 'Enabled'
                                                             })


@mock_s3
def test_save_read_version_doc(s3_bucket, s3_client, s3_store):
    setup_bucket(s3_bucket, s3_client)
    version_doc = {'symbol': 24, 'foo': 'bar'}
    s3_store.write_version(library_name='my_library', symbol='my_symbol', version_doc=version_doc)
    loaded_version_doc = s3_store.read_version(library_name='my_library', symbol='my_symbol')
    assert version_doc == loaded_version_doc


@mock_s3
def test_read_a_specific_version_doc(s3_bucket, s3_client, s3_store):
    setup_bucket(s3_bucket, s3_client)
    version_docs = [{'symbol': '000', 'foo': 'bar'}, {'symbol': '111', 'foo': 'bar'}]
    s3_store.write_version(library_name='my_library', symbol='my_symbol', version_doc=version_docs[0])
    s3_store.write_version(library_name='my_library', symbol='my_symbol', version_doc=version_docs[1])
    versions = s3_store.list_versions(library_name='my_library', symbol='my_symbol')

    for idx, row in versions.iterrows():
        # test read by version_id
        loaded_version_doc = s3_store.read_version(library_name='my_library',
                                                   symbol='my_symbol', version_id=row.VersionId)
        assert version_docs[idx] == loaded_version_doc
        # test read by as_of
        loaded_version_doc = s3_store.read_version(library_name='my_library',
                                                   symbol='my_symbol', as_of=row.LastModified)
        assert version_docs[idx] == loaded_version_doc

@mock_s3
def test_save_read_segments(s3_bucket, s3_client, s3_store):
    setup_bucket(s3_bucket, s3_client)
    segment_data = b'3424234235'
    segment_key = s3_store.write_segment(library_name='my_library', symbol='symbol', segment_data=segment_data)
    loaded_segment_data = list(s3_store.read_segments(library_name='my_library', segment_keys=[segment_key]))[0]
    assert segment_data == loaded_segment_data


@mock_s3
def test_list_symbols(s3_bucket, s3_client, s3_store):
    setup_bucket(s3_bucket, s3_client)
    version_doc = {'symbol': 24, 'foo': 'bar'}
    s3_store.write_version(library_name='my_library', symbol='my_symbol', version_doc=version_doc)
    s3_store.write_version(library_name='my_library', symbol='my_symbol2', version_doc=version_doc)
    symbols = s3_store.list_symbols(library_name='my_library')
    assert ['my_symbol', 'my_symbol2'] == symbols


@mock_s3
def test_list_versions(s3_bucket, s3_client, s3_store):
    setup_bucket(s3_bucket, s3_client)
    version_doc = {'symbol': 24, 'foo': 'bar'}
    s3_store.write_version(library_name='my_library', symbol='my_symbol', version_doc=version_doc)
    s3_store.write_version(library_name='my_library', symbol='my_symbol', version_doc=version_doc)
    versions = s3_store.list_versions(library_name='my_library', symbol='my_symbol')
    assert len(versions) == 2