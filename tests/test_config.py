import unittest
from unittest import mock

import environ

from bigrays.config import Config, BigRaysConfig


class TestConfig(unittest.TestCase):
    def test_DB_CONNECT_URL(self):
        with mock.patch.object(BigRaysConfig, 'DB_UID', 'foo'), \
                mock.patch.object(BigRaysConfig, 'DB_PWD', 'bar'), \
                mock.patch.object(BigRaysConfig, 'DB_DSN', 'baz'), \
                mock.patch.object(BigRaysConfig, 'DB_FLAVOR', 'itsmysql'), \
                mock.patch('bigrays.config.urllib.parse.quote_plus', lambda x: x):
            expected = 'itsmysql+pyodbc:///?odbc_connect=DSN=baz;UID=foo;PWD=bar'
            actual = BigRaysConfig.DB_CONNECT_URL
            self.assertEqual(actual, expected)

        with mock.patch.object(BigRaysConfig, 'DB_UID', 'foo'), \
                mock.patch.object(BigRaysConfig, 'DB_PWD', 'bar'), \
                mock.patch.object(BigRaysConfig, 'DB_SERVER', 'baz', create=True), \
                mock.patch.object(BigRaysConfig, 'DB_ODBC_CONNECT_PARAMS', ['DB_UID', 'DB_PWD', 'DB_SERVER']), \
                mock.patch.object(BigRaysConfig, 'DB_FLAVOR', 'itsmysql'), \
                mock.patch('bigrays.config.urllib.parse.quote_plus', lambda x: x):
            expected = 'itsmysql+pyodbc:///?odbc_connect=UID=foo;PWD=bar;SERVER=baz'
            actual = BigRaysConfig.DB_CONNECT_URL
            self.assertEqual(actual, expected)

        with mock.patch('bigrays.config.urllib.parse.quote_plus', lambda x: x):
            config = environ.to_config(Config, {
                'BIGRAYS_DB_SERVER': 'foo.bar.com',
                'BIGRAYS_DB_UID': 'me',
                'BIGRAYS_DB_ODBC_CONNECT_PARAMS': 'DB_SERVER,DB_UID',
                'BIGRAYS_DB_FLAVOR': 'oracle'
            })
            actual = config.DB_CONNECT_URL
            expected = 'oracle+pyodbc:///?odbc_connect=SERVER=foo.bar.com;UID=me'
            self.assertEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()
