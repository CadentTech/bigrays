import io
import json
import logging

import botocore
import pandas as pd

from . import exceptions as exc
from .resources import S3Client, SNSClient, SQLSession
from .utils import ReprMixin


pd.set_option('precision', 15)
# TODO: this should technically patched per call, a global patch
#       alters the users pandas library and assumes they are using
#       SQL Server.
# Faster inserting for sql server
def _execute_insert(self, conn, keys, data_iter):
    data = [dict((k, v) for k, v in zip(keys, row)) for row in data_iter]
    conn.execute(self.insert_statement().values(data))
pd.io.sql.SQLTable._execute_insert = _execute_insert


class SQLMixin:
    _logger = logging.getLogger(__name__)
    def read_query(self, query):
        self._logger.debug('running query: %s', query)
        connection = SQLSession.resource()
        df = pd.read_sql(query, con=connection)
        self._logger.debug('%s records retrieved' % len(df))
        return df

    def execute(self, statement):
        self._logger.debug('executing sql statement: %s', statement)
        connection = SQLSession.resource()
        # see http://docs.sqlalchemy.org/en/latest/core/connections.html#using-transactions
        with connection.begin() as txn:
            connection.execute(statement)

    def write(self, table, dataframe, **kwargs):
        self._logger.debug('writing %s rows to to table %s', len(dataframe), table)
        connection = SQLSession.resource()
        dataframe.to_sql(name=table, con=connection, **kwargs)


class S3Mixin:
    _logger = logging.getLogger(__name__)

    def upload(self, obj, bucket, key):
        """High-level upload method that attempts to convert `obj` to a byte
        stream and upload to s3://`bucket`/`key`.

        Raises:
            ValueError: If `obj` cannot be converted.
        """
        stream = self._format_object(obj)
        self.upload_byte_stream(stream, bucket, key)

    def list_objects(self, bucket, prefix, suffix):
        client = S3Client.resource()
        params = {}
        if prefix is not None:
            params['Prefix'] = prefix
        response = client.list_objects(Bucket=bucket, **params)
        keys = [obj['Key'] for obj in response['Contents']]
        if suffix is not None:
            keys = [k for k in keys if k.endswith(suffix)]
        return keys

    def download(self, bucket, key):
        client = S3Client.resource()
        try:
            stream = io.BytesIO()
            _ = client.download_fileobj(bucket, key, stream)
        except botocore.exceptions.ClientError as err:
            if err.response['Error']['Code'] == "404":  # not found
                raise Exception(
                    'The object bucket=%s, key=%s does not exist in S3'
                    % (bucket, key))
            else:
                raise err
        stream.seek(0)
        return stream

    def delete_object(self, bucket, key):
        client = S3Client.resource()
        try:
            client.delete_object(Bucket=bucket, Key=key)
        except botocore.exceptions.ClientError as err:
            self._logger.error(err)
            return False
        return True

    def upload_byte_stream(self, data, bucket, key):
        """Lower-level upload method that mimics the boto method, uploading
        the byte stream `data` to s3://`bucket`/`key`.

        Raises:
            ValueError: If `obj` cannot be converted.
        """
        client = S3Client.resource()
        if not self.overwrite_if_exists:
            if self.object_exists(bucket, key):
                raise exc.TaskError('the object %s exists in the bucket %s'
                                     % ( key, bucket))
        self._logger.debug('loading data to %s/%s', bucket, key)
        client.upload_fileobj(data, bucket, key,
                              ExtraArgs={'ServerSideEncryption': 'AES256'})

    def object_exists(self, bucket, key):
        """Return `True` if object exists on S3, otherwise return False."""
        import botocore
        client = S3Client.resource()
        try:
            # do a head request, this is fast regardless of the size of the
            # object if it exists
            self._logger.debug(f'checking existence of {bucket}/{key}')
            client.head_object(Bucket=bucket, Key=key)
            self.logger.warning('{bucket}/{key} exists')
        except botocore.exceptions.ClientError as err:
            if err.response['Error']['Code'] == '404':
                return False
            else:
                raise err
        return True

    @staticmethod
    def _format_object(obj):
        """Convert `obj` to byte stream if possible and return the stream.

        Raises:
            ValueError: If `obj` cannot be converted.
        """
        stream = io.BytesIO()
        if isinstance(obj, pd.DataFrame):
            stream.write(obj.to_csv(index=False).encode())
        elif isinstance(obj, str):
            stream.write(obj.encode())
        elif isinstance(obj, bytes):
            stream.write(obj)
        else:
            raise ValueError(f'unrecognized data type {type(obj)}')
        stream.seek(0)
        return stream


class SNSMixin:
    _logger = logging.getLogger(__name__)

    def publish(self, topic, message, **kwargs):
        client = SNSClient.resource()
        message = self._format_message(message)
        self._logger.debug('publishing %s to %s', message, topic)
        client.publish(TopicArn=topic, Message=message, **kwargs)

    @staticmethod
    def _format_message(message):
        if isinstance(message, dict):
            return json.dumps(message)
        return str(message)
