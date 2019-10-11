import unittest
from unittest import mock

import environ

from bigrays.config import Config, BigRaysConfig


class TestConfig(unittest.TestCase):
    def test_ODBC_CONNECT_URL(self):
        with mock.patch.object(BigRaysConfig, 'ODBC_UID', 'foo'), \
                mock.patch.object(BigRaysConfig, 'ODBC_PWD', 'bar'), \
                mock.patch.object(BigRaysConfig, 'ODBC_DSN', 'baz'), \
                mock.patch.object(BigRaysConfig, 'ODBC_FLAVOR', 'itsmysql'), \
                mock.patch('bigrays.config.urllib.parse.quote_plus', lambda x: x):
            expected = 'itsmysql+pyodbc:///?odbc_connect=DSN=baz;UID=foo;PWD=bar'
            actual = BigRaysConfig.ODBC_CONNECT_URL
            self.assertEqual(actual, expected)

        with mock.patch.object(BigRaysConfig, 'ODBC_UID', 'foo'), \
                mock.patch.object(BigRaysConfig, 'ODBC_PWD', 'bar'), \
                mock.patch.object(BigRaysConfig, 'ODBC_SERVER', 'baz', create=True), \
                mock.patch.object(BigRaysConfig, 'ODBC_CONNECT_PARAMS', ['ODBC_UID', 'ODBC_PWD', 'ODBC_SERVER']), \
                mock.patch.object(BigRaysConfig, 'ODBC_FLAVOR', 'itsmysql'), \
                mock.patch('bigrays.config.urllib.parse.quote_plus', lambda x: x):
            expected = 'itsmysql+pyodbc:///?odbc_connect=UID=foo;PWD=bar;SERVER=baz'
            actual = BigRaysConfig.ODBC_CONNECT_URL
            self.assertEqual(actual, expected)

        with mock.patch('bigrays.config.urllib.parse.quote_plus', lambda x: x):
            config = environ.to_config(Config, {
                'BIGRAYS_ODBC_SERVER': 'foo.bar.com',
                'BIGRAYS_ODBC_UID': 'me',
                'BIGRAYS_ODBC_CONNECT_PARAMS': 'ODBC_SERVER,ODBC_UID',
                'BIGRAYS_ODBC_FLAVOR': 'oracle'
            })
            actual = config.ODBC_CONNECT_URL
            expected = 'oracle+pyodbc:///?odbc_connect=SERVER=foo.bar.com;UID=me'
            self.assertEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()
