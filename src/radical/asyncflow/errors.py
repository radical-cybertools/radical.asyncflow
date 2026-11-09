class DependencyFailureError(Exception):
    """Exception raised when a workflow component cannot execute due to dependency
    failures.

    This exception provides detailed information about the failed dependencies and
    maintains the chain of causation for debugging purposes.
    """

    def __init__(self, message, failed_dependencies=None, root_cause=None):
        """Initialize DependencyFailureError exception.

        Args:
            message (str): Human-readable error message
            failed_dependencies (list, optional): List of failed dependency names
            root_cause (Exception, optional): The original exception that caused
                the failure
        """
        super().__init__(message)
        self.failed_dependencies = failed_dependencies or []
        self.root_cause = root_cause

        # Chain the exception if we have a root cause
        if root_cause:
            self.__cause__ = root_cause


class ComponentReleasedError(LookupError):
    """A component was looked up by uid after the engine released it.

    The engine forgets a task or block once it has finished and its dependents were
    notified, so a long-lived engine does not grow with every component. Its future
    stays usable as a dependency.
    """

    def __init__(self, uid):
        super().__init__(f"{uid} was released after it finished")
        self.uid = uid
