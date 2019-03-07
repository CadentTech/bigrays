import unittest
from unittest import mock

from bigrays.resources import BaseResource
from bigrays.run import bigrays_run
from bigrays.tasks import BaseTask


class TestBigRaysRun(unittest.TestCase):
    @mock.patch('bigrays.tasks.TASK_REGISTER', [])
    @mock.patch('bigrays.run.BigRaysConfig')
    def test(self, mock_config):
        # this is one big ugly test, but we are really only looking for
        # three things here
        #
        # 1. tasks are executed in the order they are defined
        # 2. task output is set correctly
        # 3. resources are closed and opened as needed
        #
        # We can test that these criterion are satisfied by checking the
        # call values of certain methods and using mocks that return a
        # value popped from `side_effects`.

        side_effects = [
            'opening resource 1',
            'closing resource 1',
            'opening resource 1',
            'closing resource 1',
            'opening resource 2',
            'closing resource 2',
            'opening resource 1',
            'closing resource 1',
        ]
        side_effect = lambda *args, **kwargs: side_effects.pop(0)

        class Resource:
            @classmethod
            def open(cls, *args, **kwargs):
                res = cls._open()
                cls.log_call(res)
            @classmethod
            def close(cls, *args, **kwargs):
                res = cls._close()
                cls.log_call(res)
            log_call = mock.Mock()
        class Resource1(Resource):
            required_configs = ('foo', 'bar')
            _open = mock.Mock(side_effect=side_effect)
            _close = mock.Mock(side_effect=side_effect)
        class Resource2(Resource):
            required_configs = ('baz',)
            _open = mock.Mock(side_effect=side_effect)
            _close = mock.Mock(side_effect=side_effect)

        class Task1(BaseTask):
            required_resource = Resource1
            resource_config = None
            def run(self):
                return 1
        class Task2(BaseTask):
            required_resource = Resource2
            resource_config = None
            def run(self):
                return 2
        class Task3(BaseTask):
            required_resource = None
            def run(self):
                return UserTask1.output + UserTask5.output


        class UserTask1(Task1): pass
        class UserTask2(Task1): pass
        # chaning the resource config should trigger the resource to be reopened
        class UserTask3(Task1): resource_config = object()
        class UserTask4(Task1): resource_config = UserTask3.resource_config
        # make sure that its not just the resource config triggering the resource
        # to be opened/closed
        class UserTask5(Task2): resource_config = UserTask3.resource_config
        class UserTask7(Task3): pass
        class UserTask6(Task1): pass

        mock_config.foo = 1
        mock_config.bar = 1
        mock_config.baz = 1

        # ASSERTIONS
        bigrays_run()
        Resource.log_call.assert_has_calls([
            mock.call('opening resource 1'),
            mock.call('closing resource 1'),
            mock.call('opening resource 1'),
            mock.call('closing resource 1'),
            mock.call('opening resource 2'),
            mock.call('closing resource 2'),
            mock.call('opening resource 1'),
            mock.call('closing resource 1'),
        ])
        self.assertEqual(UserTask1.output, 1)
        self.assertEqual(UserTask2.output, 1)
        self.assertEqual(UserTask3.output, 1)
        self.assertEqual(UserTask4.output, 1)
        self.assertEqual(UserTask5.output, 2)
        self.assertEqual(UserTask7.output, 3)
        self.assertEqual(UserTask6.output, 1)


if __name__ == '__main__':
    unittest.main()
