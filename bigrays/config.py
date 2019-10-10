import logging
import urllib.parse

import environ


_logger = logging.getLogger(__name__)


@environ.config(prefix='BIGRAYS')
class BigRaysConfig:

    # default all supported values to None
    AWS_REQUIRE_SECRETS = environ.bool_var(True)
    AWS_ACCESS_KEY_ID = environ.var(None)
    AWS_SECRET_ACCESS_KEY = environ.var(None)
    AWS_REGION = environ.var(None)

    # we could do
    #   @environ.config
    #   class DB
    # here, but from the user perspective it doesn't matter
    # and not having a nested class makes requirement checking
    # simpler in resources.py
    DB_UID = environ.var(None)
    DB_PWD = environ.var(None)
    DB_DSN = environ.var(None)
    DB_FLAVOR = environ.var('mssql')
    DB_ODBC_CONNECT_PARAMS = (
        'DB_DSN',
        'DB_UID',
        'DB_PWD',
    )
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
        _logger.debug('rendering connect url as: %r',
                      connect_url.replace(self.DB_PWD, '***'))
        return connect_url


BigRaysConfig = environ.to_config(BigRaysConfig)
