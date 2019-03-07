import os
import types


class Config(types.SimpleNamespace):

    # default all supported values to None
    S3_ACCESS_KEY_ID = None
    S3_SECRET_ACCESS_KEY = None
    AWS_REGION = None
    DB_USER = None
    DB_PWD = None
    DB_DSN = None

    def __init__(self):
        kwargs = {k.split('_', 1)[1]: v
                  for k, v in os.environ.items()
                  if k.startswith('BIGRAYS_')}
        super().__init__(**kwargs)


BigRaysConfig = Config()
