import unittest

import pandas as pd

from bigrays.mixins import S3Mixin, ReprMixin


class TestS3Mixin(unittest.TestCase):
    def test__obj_to_byte_stream_df(self):
        df = pd.DataFrame([[1, 2], [3, 4]], columns=['foo', 'bar'])
        actual = S3Mixin._format_object(df).read()
        expected = (
            b'foo,bar\n'
            b'1,2\n'
            b'3,4\n')
        self.assertEqual(actual, expected)

    def test__obj_to_byte_stream_str(self):
        actual = S3Mixin._format_object('foo bar').read()
        expected = b'foo bar'
        self.assertEqual(actual, expected)

    def test__obj_to_byte_stream_bytes(self):
        actual = S3Mixin._format_object(b'foo bar').read()
        expected = b'foo bar'
        self.assertEqual(actual, expected)


class TestReprMixin(unittest.TestCase):
    def test(self):
        class A(ReprMixin):
            foo = 1
            bar = 2
            _baz = '10'
            def __init__(self):
                self.x = '1'
                self._y = 'y'
            def f(self):
                pass
        actual = repr(A())
        expected = "A(bar=2, foo=1, x='1')"
        self.assertEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()