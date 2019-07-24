import unittest

from bigrays.tasks import Task, REQUIRED_ATTRIBUTE
from bigrays import functional_interface as fns


class Test(unittest.TestCase):
    def test_wrap_task(self):
        class A(Task):
            def run(self):
                return self.a ** self.b
        wrapped_task = fns.wrap_task('MyA', A)
        self.assertEqual(wrapped_task(a=2, b=3), 8)

    def test__create_subtask(self):
        class A(Task):
            pass
        sub_task = fns._create_subtask('SubclassedA', A, a=1, b=2, c='c')
        self.assertTrue(issubclass(sub_task, A))
        self.assertEqual(sub_task.a, 1)
        self.assertEqual(sub_task.b, 2)
        self.assertEqual(sub_task.c, 'c')

    def test_required_attributes(self):
        class A(Task):
            a = REQUIRED_ATTRIBUTE
        wrapped_task = fns.wrap_task('a', A)
        with self.assertRaisesRegex(ValueError, 'All required attributes'):
            wrapped_task()


if __name__ == '__main__':
    unittest.main()