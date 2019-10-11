import unittest
from unittest import mock

from bigrays.resources import ResourceManager, BaseResource, BaseAWSClient, S3Client, SNSClient


class TestResourceManager(unittest.TestCase):
    def test_context_manager(self):
        class Resource1(BaseResource):
            open = mock.Mock()
            close = mock.Mock()
        class Resource2(BaseResource):
            open = mock.Mock()
            close = mock.Mock()

        # define these for readability
        r1_opened = Resource1.open.assert_called
        r1_not_opened = Resource1.open.assert_not_called
        r1_closed = Resource1.close.assert_called
        r1_not_closed = Resource1.close.assert_not_called
        r2_opened = Resource2.open.assert_called
        r2_not_opened = Resource2.open.assert_not_called
        r2_closed = Resource2.close.assert_called
        r2_not_closed = Resource2.close.assert_not_called

        # need to keep track of resource calls since they get overwritten
        r1_open_calls = []
        r2_open_calls = []
        def reset_mocks():
            r1_open_calls.extend(Resource1.open.call_args_list)
            r2_open_calls.extend(Resource2.open.call_args_list)
            Resource1.open.reset_mock()
            Resource1.close.reset_mock()
            Resource2.open.reset_mock()
            Resource2.close.reset_mock()

        with ResourceManager(None) as resource_manager:
            resource_manager.open_resource(Resource1)
            r1_opened() and r1_not_closed() and r2_not_opened() and r2_not_closed()
            reset_mocks()

            resource_manager.open_resource(Resource1)
            r1_not_opened() and r1_not_closed() and r2_not_opened() and r2_not_closed()
            reset_mocks()

            resource_manager.open_resource(Resource1)
            r1_not_opened() and r1_not_closed() and r2_not_opened() and r2_not_closed()
            reset_mocks()

            mock_config = object()
            resource_manager.open_resource(Resource1, mock_config)
            r1_opened() and r1_closed() and r2_not_opened() and r2_not_closed()
            reset_mocks()

            resource_manager.open_resource(Resource1, mock_config)
            r1_not_opened() and r1_not_closed() and r2_not_opened() and r2_not_closed()
            reset_mocks()

            resource_manager.open_resource(Resource2)
            r1_not_opened() and r1_closed() and r2_opened() and r2_not_closed()
            reset_mocks()

            resource_manager.open_resource(Resource2)
            r1_not_opened() and r1_not_closed() and r2_not_opened() and r2_not_closed()
            reset_mocks()

            resource_manager.open_resource(None)
            r1_not_opened() and r1_not_closed() and r2_not_opened() and r2_closed()

            resource_manager.open_resource(Resource1)
            r1_opened() and r1_not_closed() and r2_not_opened() and r2_not_closed()
            reset_mocks()

        self.assertEqual(
            r1_open_calls,
            [mock.call(None), mock.call(mock_config), mock.call(None)])
        self.assertEqual(r2_open_calls, [mock.call(None)])

    def test__exit__clean(self):
        # test exiting without an Exception
        class Resource(BaseResource):
            open = mock.Mock()
            close = mock.Mock()
        with ResourceManager(None) as resource_manager:
            resource_manager.open_resource(Resource)
        Resource.open.assert_called()
        Resource.close.assert_called()

    def test__exit__with_exception_after_resource_is_opened(self):
        class Resource(BaseResource):
            _open = mock.Mock()
            _close = mock.Mock()
        Resource._open.return_value = 1
        Resource._close.return_value = False
        class E(Exception): pass

        # this block tests two things:
        # 1. test that any opened resource gets closed
        # 2. test that any exceptions raised inside the ResourceManager context
        #    are not silently trapped and pass on to the surrounding context
        with self.assertRaises(E):
            with ResourceManager(None) as resource_manager:
                resource_manager.open_resource(Resource)
                raise E
        Resource._open.assert_called()

    def test__exit__with_exception_while_resource_is_opened(self):
        class Resource(BaseResource):
            open = mock.Mock()
            close = mock.Mock()
        class E(Exception): pass
        Resource.open.side_effect = E('nope!')

        # this block tests two things:
        # 1. ResourceManager does not try to close resources when an error
        #    occurred while opening the resource
        # 2. test that any exceptions raised inside the ResourceManager context
        #    while opening a resource are not silently trapped and pass on to
        #    the surrounding context
        with self.assertRaises(E):
            with ResourceManager(None) as resource_manager:
                resource_manager.open_resource(Resource)
        Resource.open.assert_called()
        Resource.close.assert_not_called()

    def test__exit__with_ignoring_exception_after_resource_is_opened(self):
        class Resource(BaseResource):
            _open = mock.Mock()
            _close = mock.Mock()
        Resource._open.return_value = 1
        Resource._close.return_value = True
        class E(Exception): pass

        # this block tests two things:
        # 1. test that any opened resource gets closed
        # 2. test that any exceptions raised inside the ResourceManager context
        #    can be ignored if the Resource decides to.
        with ResourceManager(None) as resource_manager:
            resource_manager.open_resource(Resource)
            raise E
        Resource._open.assert_called()


class TestBaseAWSClient(unittest.TestCase):
    def test_interface(self):
        with self.assertRaises(Exception):
            class A(BaseAWSClient):
                pass
        # should not raise error
        class A(BaseAWSClient):
            _client_name = 1

    @mock.patch('boto3.client')
    def test_S3Client__open(self, mock_client):
        class MockConfig:
            AWS_ACCESS_KEY_ID = 1
            AWS_SECRET_ACCESS_KEY = 2
            AWS_REGION = 3
        S3Client._open(MockConfig)
        mock_client.assert_called_with('s3', aws_access_key_id=1, aws_secret_access_key=2)

    @mock.patch('boto3.client')
    def test_SNSClient__open(self, mock_client):
        class MockConfig:
            AWS_ACCESS_KEY_ID = 1
            AWS_SECRET_ACCESS_KEY = 2
            AWS_REGION = 3
        SNSClient._open(MockConfig)
        mock_client.assert_called_with('sns', aws_access_key_id=1, aws_secret_access_key=2, region_name=3)


if __name__ == '__main__':
    unittest.main()
