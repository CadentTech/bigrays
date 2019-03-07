class BigRaysError(Exception):
    """Base Exception class for all bigrays custom exceptions to inherit from."""


class ConfigurationError(BigRaysError):
    """Exception raised when required configurations are missing or invalid."""


class ResourceError(BigRaysError):
    """Exception raised during invalid attempts to open/close a resource."""


class TaskError(BigRaysError):
    """Exception raised when a programming error prevents a task from running."""


class TaskInterfaceError(TaskError):
    """Exception raised when a defined task fails to define the proper interface."""
