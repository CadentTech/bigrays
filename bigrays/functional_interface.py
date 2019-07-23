import types

from . import exceptions as exc
from . import tasks
from .run import bigrays_run


def _wrap_task(fn_name, base_task):
    def wrapper(**kwargs):
        sub_task = _create_subtask(fn_name, base_task, **kwargs)
        bigrays_run(sub_task)
        return sub_task.output
    return wrapper


def _create_subtask(fn_name, base_task, **kwargs):
    try:
        return types.new_class(name=fn_name, bases=(base_task,),
                               exec_body=_exec_body(**kwargs))
    except exc.TaskInterfaceError as err:
        raise ValueError(
            'All required attributes for the task {base_task} '
            'must be provided as keyword arguments') from err

def _exec_body(**kwargs):
    def exec_body(ns):
        ns.update(kwargs)
        return ns
    return exec_body


sql_execute = _wrap_task('sql_execute', tasks.SQLExecute)
sql_query = _wrap_task('sql_query', tasks.SQLQuery)
sql_write = _wrap_task('sql_write', tasks.SQLWrite)
to_s3 = _wrap_task('to_s3', tasks.ToS3)
sns_task = _wrap_task('sns_task', tasks.SNSTask)
sns_publish = _wrap_task('sns_publish', tasks.SNSPublish)
sns_publish_email = _wrap_task('sns_publish_email', tasks.SNSPublishEmail)
to_csv = _wrap_task('to_csv', tasks.ToCSV)
