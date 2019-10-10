import logging
import urllib.parse

import environ


_logger = logging.getLogger(__name__)


def _tuple_converter(s):
    if isinstance(s, tuple):
        return s
    return tuple(s.split(','))


@environ.config(prefix='BIGRAYS')
class Config:

    # default all supported values to None
    AWS_REQUIRE_SECRETS = environ.bool_var(
        True,
        help=('Are AWS credentials required?'
              ' Set to False if using AWS roles or ~/.aws/credentials.'))
    AWS_ACCESS_KEY_ID = environ.var(None)
    AWS_SECRET_ACCESS_KEY = environ.var(None)
    AWS_REGION = environ.var(None)

    # we could do
    #   @environ.config
    #   class DB
    # here, but from the user perspective it doesn't matter
    # and not having a nested class makes requirement checking
    # simpler in resources.py
    DB_UID = environ.var(None, help='UID value for odbc_connect query parameter.')
    DB_PWD = environ.var(None, help='PWD value for odbc_connect query parameter.')
    DB_DSN = environ.var(None, help='DSN value for odbc_connect query parameter.')
    DB_SERVER = environ.var(None, help='DSN value for odbc_connect query parameter.')
    DB_FLAVOR = environ.var('mssql', help='The SQL flavor, or dialect.')
    DB_ODBC_CONNECT_PARAMS = environ.var((
        'DB_DSN',
        'DB_UID',
        'DB_PWD',
    ), converter=_tuple_converter)
    _connect_string = '{flavor}+pyodbc:///?odbc_connect={odbc_connect}'

    @property
    def DB_CONNECT_URL(self):
        odbc_connect = ';'.join(
            '%s=%s' % (k.replace('DB_', ''), getattr(self, k))
            for k in self.DB_ODBC_CONNECT_PARAMS)
        connect_url = self._connect_string.format(
            flavor=self.DB_FLAVOR,
            odbc_connect=urllib.parse.quote_plus(odbc_connect)
        )
        return connect_url


BigRaysConfig = environ.to_config(Config)
