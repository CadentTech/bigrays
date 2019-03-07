"""Module exposing "tasks" for building jobs.

Tasks are the central feature in `bigrays`. A Task is any class that inherits
from `BaseTask` and implements a `run()` method (which should accept at
least a single positional argument and `**kwargs`). Additionally if a task's
methods require a particular resource it should set the attribute
`required_resource` to the resource it needs (e.g.
`bigrays.resources.SQLSession`).
"""

import logging
import os

from . import exceptions as exc
from . import mixins
from . import utils
from .resources import S3Client, SNSClient, SQLSession

UNSET = object()


class Placeholder:
    """Useful for referencing data that is not known until Runtime, e.g. Task output."""

    def __init__(self, name):
        self.name = name
        self._value = UNSET

    def __repr__(self):
        return f'{self.__class__.__name__}({self.name})'

    @property
    def value(self):
        if self._value is UNSET:
            raise AttributeError(
                "Tried to access an unset placeholder value on %s" % self)
        return self._value

    @value.setter
    def value(self, val):
        self._value = val

    def __get__(self, instance, owner):
        return self.value


class TaskOutput(utils.ReprMixin):
    def __init__(self):
        self._values = {}
        self._placeholders = {}

    def __get__(self, instance, owner):
        key = instance if isinstance(instance, Register) else owner
        if not key in self._values:
            if not key in self._placeholders:
                self._placeholders[key] = Placeholder(key.__name__)
            return self._placeholders[key]
        return self._values[key]

    def __set__(self, instance, value):
        key = instance if isinstance(instance, Register) else instance.__class__
        if key in self._placeholders:
            self._placeholders[key].value = value
        self._values[key] = value


TASK_REGISTER = tuple()

class Register(type):
    """Metaclass that providing registration of subclasses of this type."""

    output = TaskOutput()
    _defined_base_task = False

    def __new__(metacls, name, bases, namespace):
        global TASK_REGISTER
        cls = super(Register, metacls).__new__(metacls, name, bases, namespace)
        # this is BaseTask
        if not metacls._defined_base_task:
            metacls._defined_base_task = True
        # These are classes inheriting from BaseTask.
        # I.e. tasks we intend users to subclass.
        # For lack of a better name we'll call these public tasks
        elif not hasattr(cls, 'is_task'):
            cls.is_task = True
        # these are subclasses of "public tasks"
        else:
            metacls._check_interface(name, bases, namespace)
            # this may look a bit mysterious, note that cls.register is looked
            # up on the superclass.
            TASK_REGISTER += (cls,)
        return cls

    @staticmethod
    def _check_interface(name, bases, namespace):
        required_attributes = {attr for base in bases
                                    for attr, val in vars(base).items()
                                    if val is REQUIRED_ATTRIBUTE}
        missing = [attr for attr in required_attributes
                        if namespace.get(attr, REQUIRED_ATTRIBUTE) is REQUIRED_ATTRIBUTE]
        if missing:
            raise exc.TaskInterfaceError(
                '%s must define the attribute(s) %s' % (name, missing))


class BaseTask(utils.ReprMixin, metaclass=Register):
    """Base task class to which all user defined tasks and resources are
    registered.
    """
    logger = logging.getLogger(__name__)
    format_kws = None
    required_resource = None
    # resource config allows a specific task to override the default
    # config used by bigrays
    resource_config = None
    run_with_exceptions = False
    input = UNSET

    def __call__(self):
        self.logger.info('running task: %s', type(self).__name__)
        self.__class__.output = output = self.run()
        return output

    def run(self):
        raise NotImplementedError

    def reformat_keywords(self):
        if self.format_kws is not None:
            return {k: v.value if isinstance(v, Placeholder) else v
                    for k, v in self.format_kws.items()}
        return {}

    @classmethod
    def update_format_kws(cls, kws=None, **kwargs):
        cls.logger.debug('updating format keywords with %s', kws)
        if kws is None:
            kws = kwargs
        if cls.format_kws is None:
            cls.format_kws = {}
        cls.format_kws.update(kws)


update_format_kws = BaseTask.update_format_kws


REQUIRED_ATTRIBUTE = object()


class Task(BaseTask):
    """An extendable class for creating custom tasks."""


###########
# SQL stuff
###########

class SQLTask(BaseTask, mixins.SQLMixin):
    """An extendable class with SQL capabilities."""
    required_resource = SQLSession


class SQLExecute(BaseTask, mixins.SQLMixin):
    """A task providing basic functionality for executing SQL statements."""
    required_resource = SQLSession
    statement = REQUIRED_ATTRIBUTE

    def run(self):
        # format_kws is an argument for backwards compatability
        format_kws = self.reformat_keywords()
        return self.execute(self.statement.format(**format_kws))


class SQLQuery(BaseTask, mixins.SQLMixin):
    """A task providing basic funtionality for retrieving SQL query results."""
    required_resource = SQLSession
    query = REQUIRED_ATTRIBUTE
    _dry_run = False

    def run(self):
        # format_kws is an argument for backwards compatability
        format_kws = self.reformat_keywords()
        return self.read_query(self.query.format(**format_kws))


class SQLWrite(BaseTask, mixins.SQLMixin):
    """A task providing basic functionality for writing a table to a DB."""
    required_resource = SQLSession
    tablename = REQUIRED_ATTRIBUTE
    input = REQUIRED_ATTRIBUTE
    params = {'index': False}

    def run(self):
        return self.write(self.tablename, self.input, **self.params)


##########
# AWS tasks
##########


class S3Task(BaseTask, mixins.S3Mixin):
    required_resource = S3Client


class ToS3(BaseTask, mixins.S3Mixin):
    """Task providing basic functionality for uploading objects to S3."""
    required_resource = S3Client
    input = REQUIRED_ATTRIBUTE
    bucket = REQUIRED_ATTRIBUTE
    key = REQUIRED_ATTRIBUTE
    overwrite_if_exists = False

    def run(self):
        format_kws = self.reformat_keywords()
        bucket = self.bucket.format(**format_kws)
        key = self.key.format(**format_kws)
        self.upload(self.input, bucket, key)


class SNSTask(BaseTask, mixins.SNSMixin):
    required_resource = SNSClient


class SNSPublish(BaseTask, mixins.SNSMixin):
    required_resource = SNSClient
    input = REQUIRED_ATTRIBUTE
    topic = REQUIRED_ATTRIBUTE

    def run(self):
        self.publish(topic=self.topic, message=self.input)


class SNSPublishEmail(BaseTask, mixins.SNSMixin):
    required_resource = SNSClient
    input = REQUIRED_ATTRIBUTE
    topic = REQUIRED_ATTRIBUTE
    subject = REQUIRED_ATTRIBUTE

    def run(self):
        self.publish(topic=self.topic, message=self.input, Subject=self.subject)


##########
# other io
##########

class ToCSV(BaseTask):
    """Task providing basic functionality for writing data to a CSV."""
    filename = REQUIRED_ATTRIBUTE
    input = REQUIRED_ATTRIBUTE
    overwrite_if_exists = False
    params = {'index': False}

    def run(self):
        format_kws = self.reformat_keywords()
        file = self.filename.format(**format_kws)
        if os.path.exists(file) and not self.overwrite_if_exists:
            raise exc.TaskError('the file %s exists on disk' % file)
        self.logger.debug('writing %s rows to %s' % (len(self.input), file))
        self.input.to_csv(file, **self.params)


###############
# compatability
###############

S3Upload = ToS3
