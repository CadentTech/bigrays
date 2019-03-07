import unittest
from unittest import mock

from bigrays.exceptions import ConfigurationError
from bigrays.run import BigRays, bigrays_run
from bigrays.tasks import BaseTask
from bigrays import tasks


class TestBigRays(unittest.TestCase):
    @mock.patch('bigrays.run.BigRays._check_configs')
    @mock.patch('bigrays.resources.ResourceManager')
    @mock.patch('bigrays.tasks.TASK_REGISTER')
    def test_task_exectution_default(self, mock_register, *mocks):
        # test the order of task execution as well as how arguments are passed
        # to each task
        mock_task_1 = mock.Mock()
        mock_task_2 = mock.Mock()
        mock_task_3 = mock.Mock()
        mock_register.__iter__.return_value = [mock_task_1, mock_task_2, mock_task_3]
        bigrays_run()
        mock_task_1.assert_called()
        mock_task_2.assert_called()
        mock_task_3.assert_called()

    @mock.patch('bigrays.run.BigRays._check_configs')
    @mock.patch('bigrays.resources.ResourceManager')
    @mock.patch('bigrays.tasks.TASK_REGISTER')
    def test_task_exectution_custom(self, mock_register, *mocks):
        # test the order of task execution as well as how arguments are passed
        # to each task
        mock_task_1 = mock.Mock()
        mock_task_2 = mock.Mock()
        mock_task_3 = mock.Mock()
        # register defaults to test that mock_task_2 is not called
        mock_register.__iter__.return_value = [mock_task_1, mock_task_2, mock_task_3]
        bigrays_run(mock_task_1, mock_task_3)
        mock_task_1.assert_called()
        mock_task_2.assert_not_called()
        mock_task_3.assert_called()

    @mock.patch('bigrays.tasks.TASK_REGISTER', [1, 2, 3])
    def test__define_task_list_default(self):
        actual = BigRays._define_task_list(None)
        expected = [1, 2, 3]
        self.assertEqual(actual, expected)

    @mock.patch('bigrays.tasks.TASK_REGISTER', [1, 2, 3])
    def test__define_task_list_custom(self):
        actual = BigRays._define_task_list([1, 2])
        expected = [1, 2]
        self.assertEqual(actual, expected)

    def test__define_required_resources(self):
        class SimpleTask:
            def __init__(self, resource):
                self.required_resource = resource
        simple_tasks = [SimpleTask(1), SimpleTask(None), SimpleTask('foo'), SimpleTask(2)]
        actual = BigRays._define_required_resources(simple_tasks)
        expected = {1, 'foo', 2}
        self.assertEqual(actual, expected)

    def test__check_config(self):
        class Resource1:
            required_configs = ['foo', 'bar']
        class Resource2:
            required_configs = ['baz']
        resources = [Resource1, Resource2]
        class PassingConfig:
            foo = bar = baz = 1
        # make sure this doesn't raise an exception
        BigRays._check_configs(PassingConfig, resources)
        class FailingConfig1:
            foo = bar = 1
        class FailingConfig2:
            foo = bar = 1
            baz = None
        expected_msg = 'Resource2.*requires.*baz'
        with self.assertRaisesRegexp(ConfigurationError, expected_msg):
            BigRays._check_configs(FailingConfig1, resources)
        with self.assertRaisesRegexp(ConfigurationError, expected_msg):
            BigRays._check_configs(FailingConfig2, resources)

    def test__run_tasks(self):
        mock_resource_manager = mock.Mock()
        mock_tasks = [mock.Mock() for i in range(10)]
        BigRays._run_tasks(mock_tasks, mock_resource_manager)
        for m in mock_tasks:
            m.assert_called()

    def test__run_tasks_with_exceptions(self):
        mock_resource_manager = mock.Mock()
        mock_tasks = [mock.Mock() for i in range(10)]
        mock_tasks[4].side_effect = Exception('testing error 1')
        mock_tasks[7].side_effect = Exception('testing error 2')
        _ = [setattr(m, 'run_with_exceptions', False) for m in mock_tasks]
        mock_tasks[5].run_with_exceptions = mock_tasks[7].run_with_exceptions = mock_tasks[9].run_with_exceptions = True
        # make sure error is raised after all tasks are iterated through
        with self.assertRaisesRegex(Exception, 'exceptions occurred while running tasks') as err_cm:
            BigRays._run_tasks(mock_tasks, mock_resource_manager)
        err = err_cm.exception
        self.assertIs(err.__context__, mock_tasks[7].side_effect)
        self.assertIs(err.__context__.__context__, mock_tasks[4].side_effect)
        # make sure 1-5 and 7 and 9 ran but the rest didn't
        for i in range(len(mock_tasks)):
            if i in [6, 8]:
                mock_tasks[i].assert_not_called()
            else:
                mock_tasks[i].assert_called()


if __name__ == '__main__':
    unittest.main()
