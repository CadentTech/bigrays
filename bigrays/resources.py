"""Module providing access to external resources such as a database or S3.

This Module exposes the following classes

- ResourceManager
- SQLSession (Resource)
- S3Client (Resource)

Note:
    Resources should never be explicitly opened or closed. Instead resources
    should be opened by an instance of `ResourceManager` inside of its own
    context using `ResourceManager.open_resource()`.
"""

import logging

from .config import BigRaysConfig
from . import exceptions
from .utils import ReprMixin


class ResourceManager(ReprMixin):
    """Context manager that handles opening and closing of resources.

    Note:
        `ResourceManager` will not close an opened resource until

        1. A new resource is opened.
        2. The end of the `with` block in which `ResourceManager` is used
            is reached.

        This implies

        1. Users never need to request for a resource to be closed.
        2. A resources state is lost once a new resource is requested to be
            opened.
    """
    _logger = logging.getLogger(__name__)

    # Sentinel used to indicate that no resources have been opened.
    # 
    # The typical way to do this is: _none = object()
    # We deviate from the norm and define this attribtue as a class
    # for debugging purposes only since the class will be printed as
    #     <class bigrays.resources._none at 0x...>
    # instead of
    #     <object at 0x...>
    class _none: pass

    def __init__(self, default_config):
        """Intialize an instance of `ResourceManager` with the config needed
        to open resources.

        Args:
            config: A simple namespace exposing configurations needed by
                resources to be managed.
        """
        self.default_config = default_config
        self._init_state()

    def __enter__(self):
        """Enter the `ResourceManager` context."""
        self._init_state()
        return self

    def __exit__(self, *exc):
        """Exit the `ResourceManager` context."""
        ignore_exception = self._cleanup(*exc)
        return ignore_exception

    def open_resource(self, resource, config=None):
        """Open a resource.

        Args:
            resource: An object implementing the `BaseResource` protocol.
        """
        config = self.default_config if config is None else config
        if resource is not self.resource or config is not self.config:
            self._cleanup()
            if resource is not None:
                # these must be defined before opening the resource.
                # in the case an error occurs while the resource is being
                # opened the cleanup method will take appropriate action.
                self.resource, self.config = resource, config
                self._open_resource(resource, config)

    def _open_resource(self, resource, config):
        """Open `resource` with `config`."""
        self._opening_resource = True
        try:
            resource.open(config)
        except Exception as err:
            # if an exception occurs while opening the resource set this
            # attribute so we don't try to clean the resource up in __exit__()
            self._logger.warning('could not open resource %s', resource.__name__)
            raise err
        else:
            self._opening_resource = False

    def _cleanup(self, *exc):
        """Close the existing resource (if exists)."""
        ignore_exception = False
        if (self.resource is not self._none
                and self.config is not self._none
                and self.resource is not None):
            if not exc:
                exc = (None, None, None)  # mimic the Python call to __exit__()
            if not self._opening_resource:
                ignore_exception = self.resource.close(*exc)
            self._init_state()
        return ignore_exception

    def _init_state(self):
        """Initialize instance attributes to state representing no resources
        are currently opened or being opened - i.e. the instance was just
        created or a resource was just closed.
        """
        self.resource = self._none
        self.config = self._none
        self._opening_resource = False


class BaseResource(ReprMixin):
    """Base class defining the interface for resources.

    Subclasses of `BaseResource` are simply refered to as resources and must
    override `_open()` and optionally `_close()` if the resource they expose
    requires cleanup action.

    Note:
        1. A resource stores and exposes its state on itself (the class). This
            is intential for that reason that accessing `Resource.resource()`
            returns the same reference no matter where the call is made in the'
            code.
        2. Resources cannot be instantiated. While it is true that the point
            above is satisfied by accessing the classmethod on an instance -
            `Resource().resource()` - `ResourceManager` opens and closes
            resources based on the *reference* it receives - not the type it
            receives. See the code snippet below for clarity.

            >>> r1 = SQLResource
            >>> r2 = SQLResource
            >>> r1 is r2
            True
            >>> SQLResource() is SQLResource()
            False
    """

    required_configs = ()
    """Collection of attribute names required to open the resource. Prefer
    using a tuple for its immutability over mutable objects such as a list.
    """

    _resource = None

    _logger = logging.getLogger(__name__)

    def __new__(cls, *args, **kwargs):
        raise TypeError('Resources cannot be instantiated.')

    @classmethod
    def open(cls, config):
        """Open the resource.

        Args:
            config: A simple namespace exposing configurations needed by
                resources to be managed.
        """
        cls._logger.info('opening resource: %s', cls.__name__)
        try:
            resource = cls._open(config)
        except Exception as err:
            msg = 'could not open resource %s, cause: %s' \
                    % (cls.__name__, err)
            raise exceptions.ResourceError(msg)
        cls._register_resource(resource)
        return cls

    @classmethod
    def close(cls, *exc):
        """Close the resource and perform any necessary cleanup.

        Args:
            *exc: Exception info passed to the __exit__() method of a context
                manager if relevant.

        Returns:
            bool indicating whether the exception (if occurred) should be ignored
                or not.
        """
        cls._logger.info('closing resource: %s', cls.__name__)
        if cls._resource is None:
            raise exceptions.ResourceError(
                'attempted to close an unopened resource on %s' % cls.__name__)
        ignore_exception = cls._close(*exc)
        cls._resource = None
        return ignore_exception

    @classmethod
    def resource(cls):
        if cls._resource is None:
            raise RuntimeError('no opened resource for %s' % cls.__name__)
        return cls._resource

    @classmethod
    def _register_resource(cls, resource):
        """Save a reference to an opened resource (the raw resource, not a
        subclass of BaseResource) so that users can access it from
        `cls.resource()`
        """
        cls._resource = resource

    @classmethod
    def _open(cls, config):
        """Open and return a resource.

        Subclasses must override this method.
        """
        raise NotImplementedError('subclasses of %s must override _open()'
                                  % cls.__name__)

    @classmethod
    def _close(cls, *exc):
        """Close the resource and perform any necessary cleanup.

        Subclasses should override this method if the resource they expose
        requires any cleanup action.

        To ignore the exception that occurred (if any) return True, otherwise
        return False.
        """
        return False


class SQLSession(BaseResource):
    required_configs = BigRaysConfig.DB_ODBC_CONNECT_PARAMS

    @classmethod
    def _open(cls, config):
        """Create and return a `sqlalchemy.engine.Connection`."""
        cls._resource = cls._create_engine(config.DB_CONNECT_URL).connect()
        return cls._resource

    @classmethod
    def _close(cls, *exc):
        cls._resource.close()
        return False

    @classmethod
    def _create_engine(cls, connect_url):
        import sqlalchemy as sa
        return sa.create_engine(connect_url)


class BaseAWSClient:
    if BigRaysConfig.AWS_REQUIRE_SECRETS:
        required_configs = {
            'S3_ACCESS_KEY_ID': 'aws_access_key_id',
            'S3_SECRET_ACCESS_KEY': 'aws_secret_access_key',
        }
    else:
        required_configs = {}

    _client_name = None

    def __init_subclass__(cls):
        if cls._client_name is None:
            raise exceptions.BigRaysError(
                'sublasses of {cls.__name__} must define _client_name')

    @classmethod
    def _open(cls, config):
        import boto3
        kwargs = cls._calculte_client_kwargs(config)
        client = boto3.client(cls._client_name, **kwargs)
        return client

    @classmethod
    def _calculte_client_kwargs(cls, config):
        return {v: getattr(config, k) for k, v in cls.required_configs.items()}


class S3Client(BaseAWSClient, BaseResource):
    _client_name = 's3'


class SNSClient(BaseAWSClient, BaseResource):
    if BigRaysConfig.AWS_REQUIRE_SECRETS:
        required_configs = {
            'S3_ACCESS_KEY_ID': 'aws_access_key_id',
            'S3_SECRET_ACCESS_KEY': 'aws_secret_access_key',
            'AWS_REGION': 'region_name',
        }
    _client_name = 'sns'
