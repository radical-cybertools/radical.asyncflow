import radical.pilot as rp


class Task(rp.TaskDescription):
    """Task description container extending radical.pilot TaskDescription.
    A specialized task description class that inherits from radical.pilot's
    TaskDescription to define and manage task configurations.
    Args:
        **kwargs: Configuration parameters for the task. All arguments are
            passed directly to the parent TaskDescription class.
    Attributes:
        Inherits all attributes from radical.pilot.TaskDescription.
    Example:
        >>> task = Task(executable='/bin/date',
        ...            cpu_processes=1,
        ...            gpu_processes=0)
    Note:
        This class uses dictionary-based initialization through the parent
        class's from_dict parameter to maintain compatibility with
        radical.pilot's configuration system.
    """

    def __init__(self, **kwargs):
        # we pass only the kwargs as dict to the rp.TaskDescription
        # as it only allows from_dict
        super().__init__(from_dict=kwargs)