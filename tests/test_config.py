import os
import unittest
from unittest import mock

from bigrays.config import Config


class TestConfig(unittest.TestCase):
    def test(self):
        environ = {
            'FOO': 'z',
            'BAR': 'y',
            'BIGRAYS_DB_DSN': '1',
            'BIGRAYS_DB_USER': '2',
            'BIGRAYS_ONE_TWO': '3',
        }
        with mock.patch.dict('bigrays.config.os.environ', environ, clear=True):
            config = Config()
        self.assertEqual(config.DB_DSN, '1')
        self.assertEqual(config.DB_USER, '2')
        self.assertEqual(config.ONE_TWO, '3')
        self.assertFalse(hasattr(config, 'FOO'))
        self.assertFalse(hasattr(config, 'BAR'))


if __name__ == '__main__':
    unittest.main()
