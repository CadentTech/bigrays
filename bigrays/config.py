import logging
import urllib.parse

import environ


_logger = logging.getLogger(__name__)


def _odbc_connect_params(s):
    return tuple(f'ODBC_{ss}' for ss in s.split(','))


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
    ODBC_UID = environ.var(None, help='UID value for odbc_connect query parameter.')
    ODBC_PWD = environ.var(None, help='PWD value for odbc_connect query parameter.')
    ODBC_DSN = environ.var(None, help='DSN value for odbc_connect query parameter.')
    ODBC_SERVER = environ.var(None, help='Server value for odbc_connect query parameter.')
    ODBC_PORT = environ.var(None, help='Port value for odbc_connect query parameter.')
    ODBC_DRIVER = environ.var(None, help='The ODBC connection driver, e.g. "{ODBC Driver 17 for SQL Server}"')
    ODBC_FLAVOR = environ.var('mssql', help='The SQL flavor, or dialect.')

    ODBC_CONNECT_PARAMS = environ.var('SERVER,PORT,DRIVER,UID,PWD', converter=_odbc_connect_params)
    _connect_string = '{flavor}+pyodbc:///?odbc_connect={odbc_connect}'

    @property
    def ODBC_CONNECT_URL(self):
        odbc_connect = ';'.join(
            '%s=%s' % (k.replace('ODBC_', ''), getattr(self, k))
            for k in self.ODBC_CONNECT_PARAMS)
        connect_url = self._connect_string.format(
            flavor=self.ODBC_FLAVOR,
            odbc_connect=urllib.parse.quote_plus(odbc_connect)
        )
        return connect_url

BigRaysConfig = Config.from_environ()


if __name__ == '__main__':
    print('bigrays configurations. Set the following environment variables or '
          'assign to bigrays.config.BigRaysConfig directly.')
    print(BigRaysConfig.generate_help())
