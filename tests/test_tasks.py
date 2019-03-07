import collections
import unittest
from unittest import mock

from bigrays.exceptions import TaskError, TaskInterfaceError
from bigrays import tasks
from bigrays.tasks import ToS3, SQLExecute, SQLQuery, BaseTask


class TestTaskRegister(unittest.TestCase):
    @mock.patch('bigrays.tasks.TASK_REGISTER', [])
    def test_register_tasks(self):
        # define several tasks and make sure their subclasses are registered
        class Task1(BaseTask): pass
        class Task2(BaseTask): pass
        class UserTask1(Task1): pass
        class UserTask2(Task2): pass
        class UserTask3(Task1): pass
        expected_types = [UserTask1, UserTask2, UserTask3]
        self.assertEqual(len(tasks.TASK_REGISTER), len(expected_types))
        for actual, expected in zip(tasks.TASK_REGISTER, expected_types):
            self.assertTrue(actual is expected)

    def test__check_interface(self):
        class B1:
            attr1 = tasks.REQUIRED_ATTRIBUTE
            attr2 = tasks.REQUIRED_ATTRIBUTE
            attr3 = None
            attr4 = 1
        class B2:
            attr5 = tasks.REQUIRED_ATTRIBUTE
            attr6 = 1
        bases = [B1, B2]
        namespace_pass = {'attr1': 1, 'attr2': None, 'attr5': 'foo'}
        namespace_fail = {'attr1': 1}
        # should not fail
        tasks.Register._check_interface('some class', bases, namespace_pass)
        try:
            with self.assertRaisesRegexp(TaskInterfaceError, "some class must define.*'attr2', 'attr5'"):
                tasks.Register._check_interface('some class', bases, namespace_fail)
        except AssertionError:
            with self.assertRaisesRegexp(TaskInterfaceError, "some class must define.*'attr5', 'attr2'"):
                tasks.Register._check_interface('some class', bases, namespace_fail)


class TestTaskInterface(unittest.TestCase):
    @mock.patch('bigrays.tasks.TASK_REGISTER', [])
    def test(self):
        class T(tasks.BaseTask):
            foo = tasks.REQUIRED_ATTRIBUTE
            bar = tasks.REQUIRED_ATTRIBUTE
            baz = None

        class Pass(T):
            foo = bar = 1

        with self.assertRaises(Exception):
            class Fail(T):
                pass


class TestBaseTask(unittest.TestCase):
    def test_output(self):
        class Task1(tasks.Task):
            def run(self):
                return 1
        self.assertTrue(isinstance(Task1.output, tasks.Placeholder))
        task1_output_placeholder = Task1.output
        with self.assertRaises(AttributeError):
            Task1.output.value
        # instantiate, then run
        Task1()()
        self.assertEqual(Task1.output, 1)
        self.assertEqual(task1_output_placeholder.value, 1)


class TestSQLTasks(unittest.TestCase):
    @mock.patch('bigrays.tasks.SQLQuery.read_query')
    @mock.patch('bigrays.tasks.SQLExecute.execute')
    @mock.patch('bigrays.tasks.BaseTask.format_kws', {})
    def test_update_format_kws(self, mock_execute, mock_query):
        class QueryTask(SQLQuery):
            query = 'foo{bar} baz{punc}'
        query_task = QueryTask()

        class ExecuteTask(SQLExecute):
            statement = 'foo{bar} baz{punc}'
        execute_task = ExecuteTask()

        tasks.update_format_kws({'bar': 'BAR', 'punc': '!'})
        query_task.run()
        execute_task.run()
        mock_query.assert_called_with('fooBAR baz!')
        mock_execute.assert_called_with('fooBAR baz!')


class TestS3Tasks(unittest.TestCase):
    @mock.patch('bigrays.resources.S3Client.resource')
    @mock.patch('bigrays.tasks.ToS3.object_exists')
    def test_error_handling(self, mock_object_exists, mock_resource):
        to_s3 = ToS3()

        mock_object_exists.return_value = True
        with self.assertRaises(TaskError):
            to_s3.upload('fake-data', 'fake-bucket', 'fake-key')

        mock_object_exists.return_value = False
        to_s3.upload('fake-data', 'fake-bucket', 'fake-key')
        mock_resource.return_value.upload_fileobj.assert_called()


if __name__ == '__main__':
    unittest.main()
