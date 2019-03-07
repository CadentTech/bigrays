"""Module implementing the functionality for running user defined tasks."""

import collections
import logging

from . import exceptions as exc
from .config import BigRaysConfig
from .resources import ResourceManager
from . import tasks as bigrays_tasks


class BigRays:
    _logger = logging.getLogger(__name__)

    @classmethod
    def run(cls, *tasks):
        tasks = cls._define_task_list(tasks if tasks else None)
        required_resources = cls._define_required_resources(tasks)
        cls._check_configs(BigRaysConfig, required_resources)
        cls._logger.info('running tasks')
        with ResourceManager(BigRaysConfig) as resource_manager:
            cls._run_tasks(tasks, resource_manager)
        cls._logger.info('all tasks complete')

    @classmethod
    def _define_task_list(cls, tasks):
        message = 'using {} task list'
        if tasks is not None:
            cls._logger.info(message.format('custom'))
        else:
            tasks = bigrays_tasks.TASK_REGISTER
            cls._logger.info(message.format('default'))
        return tasks

    @classmethod
    def _define_required_resources(cls, tasks):
        requirements = {task.required_resource
                        for task in tasks
                        if task.required_resource is not None}
        return requirements

    @staticmethod
    def _check_configs(config, resources):
        missing = []
        for resource in resources:
            for c in getattr(resource, 'required_configs', []):
                if getattr(config, c, None) is None:
                    missing.append(c)
        if missing:
            missing_env_vars = [f'BIGRAYS_{c}' for c in missing]
            err_msg = ' '.join([
                f'{resource} requires the configuration value(s) {", ".join(missing)}.',
                f'Explicitly set missing attribute(s) on `bigrays.config.BigRaysConfig`',
                f'or set the environment variable(s) {", ".join(missing_env_vars)}.',
            ])
            raise exc.ConfigurationError(err_msg)

    @classmethod
    def _run_tasks(cls, tasks, resource_manager):
        """Run all `tasks` in order.

        Args:
            tasks: An iterable of `bigrays` tasks.
            resource_manager: An instance of `bigrays.resources.ResourceManager`.

        Raises:
            `bigrays.exceptions.BigRaysError`: If one or more errors occurred
                inside of a task (or tasks).
        """
        failed_tasks = []
        # convert this to something we can .popleft() from
        tasks = collections.deque(tasks)
        try:
            cls._run_tasks_with_error_harness(tasks, resource_manager, failed_tasks)
        except Exception as err:
            raise exc.BigRaysError(
                'exceptions occurred while running tasks (includes failure to open '
                f'resources): {failed_tasks}') from err
            

    @classmethod
    def _run_tasks_with_error_harness(cls, tasks, resource_manager, failed_tasks):
        """
        Run all `tasks` with proper error handling.

        Args:
            tasks: An iterable of `bigrays` tasks.
            resource_manager: An instance of `bigrays.resources.ResourceManager`.
            failed_tasks: An empty `list` which failed tasks will be
                appended to. Note that the mutability of lists implies that
                the caller will be able to see the tasks appended to the list.

        Note: Proper error handling requires the following features:

        1. If a task raises an exception all subsequent tasks are skipped
            unless the attribute `run_with_exceptions` is `True`.
        2. Tasks with `run_with_exceptions = True` are run no matter what.
            Even when a previous task that set `run_with_exceptions = True`
            has raised an exception.
        3. A traceback from all errors occurring while a task is executed
            is printed (see note below on recursion on how this is
            implemented). This is to complements the previous requirement
            which allows more than one exception to be raised in a single
            pass through the task list which makes how to simply use the
            `raise ... from ...` construct less clear.

        On the implementation:
        To satisify the requirement 3 recursion is used to take advantage of how
        Python handles an exception during finally as demonstrated by the code
        snippet.

            >>> try:
            ...     raise Exception('foo')
            ... finally:
            ...     raise Exception('bar')
            ... 
            Traceback (most recent call last):
              File "<stdin>", line 2, in <module>
            Exception: foo

            During handling of the above exception, another exception occurred:

            Traceback (most recent call last):
              File "<stdin>", line 4, in <module>
            Exception: bar

        You can find more information on exception contexts here

            https://docs.python.org/3.6/reference/compound_stmts.html#finally

        Note that while the first exception gets swallowed and only the last
        exception propogates through we still get a traceback from the first.
        To gain some intuition on how we use recurrence in this method to
        leverage the Python implementation take a look at the simpler example
        below. Additionally the script "examples/running_with_exceptions_tb.py"
        demonstrates how all of this error handling looks in practice.

            >>> def f(x):       
            ...        if not x:   
            ...            return          
            ...        y = x.pop()
            ...        try:
            ...            raise Exception(y)
            ...        finally:
            ...            f(x)
            ... 
            >>> f([1, 2, 3])
            Traceback (most recent call last):
              File "<stdin>", line 6, in f
            Exception: 3

            During handling of the above exception, another exception occurred:

            Traceback (most recent call last):
              File "<stdin>", line 6, in f
            Exception: 2

            During handling of the above exception, another exception occurred:

            Traceback (most recent call last):
              File "<stdin>", line 1, in <module>
              File "<stdin>", line 8, in f
              File "<stdin>", line 8, in f
              File "<stdin>", line 6, in f
            Exception: 1
        """
        # Note that we don't actually intend to iterate through all of the tasks here.
        # Using "while" here is convenient because it allows us to isolate our code
        # for running a single task to one line with the "continue" statement.
        while tasks:
            task = tasks.popleft()
            try:
                if failed_tasks:
                    if not task.run_with_exceptions:
                        cls._logger.warning('skipping %s due to the occurrence of an exception', task)
                        continue
                    else:
                        cls._logger.warning('running %(task)s after the occurrence of an exception '
                                            'since `%(task)s.run_with_exceptions is True`',
                                            dict(task=task))
                cls._run_task(task, resource_manager)
            except Exception as err:
                if isinstance(err, exc.ResourceError):
                    cls._logger.warning('could not open resource for task %s', task)
                else:
                    cls._logger.warning('could not run task %s', task)
                failed_tasks.append(task)
                # re-raise error so that the traceback is printed
                raise err
            finally:
                cls._run_tasks_with_error_harness(tasks, resource_manager, failed_tasks)

    @classmethod
    def _run_task(cls, task, resource_manager):
        config = getattr(task, 'resource_config', None)
        resource_manager.open_resource(task.required_resource, config)
        # classes are instantiated here so that the Task protocol doesn't
        # require users to write classmethods i.e. the following works
        # >>> def (self, ...):
        task_instance = task()
        # actually execute the task now by calling the instance
        task_instance()


bigrays_run = BigRays.run
