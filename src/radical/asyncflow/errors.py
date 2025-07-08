class DependencyFailure(Exception):
    """
    Exception raised when a workflow component cannot execute due to dependency failures.
    
    This exception provides detailed information about the failed dependencies
    and maintains the chain of causation for debugging purposes.
    """
    
    def __init__(self, message, failed_dependencies=None, root_cause=None):
        """
        Initialize DependencyFailure exception.
        
        Args:
            message (str): Human-readable error message
            failed_dependencies (list, optional): List of failed dependency names
            root_cause (Exception, optional): The original exception that caused the failure
        """
        super().__init__(message)
        self.failed_dependencies = failed_dependencies or []
        self.root_cause = root_cause
        
        # Chain the exception if we have a root cause
        if root_cause:
            self.__cause__ = root_cause
