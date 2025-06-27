import radical.pilot as rp


class Task(rp.TaskDescription):
    """
    Represents a task description by extending the `TaskDescription` class from `rp`
    (an external module).

    This class is primarily used to define and manage the details of a task, inheriting
    properties and methods from the `rp.TaskDescription` base class. Additional arguments
    and keyword arguments can be passed to further configure the task, which are then
    forwarded to the base class.

    Parameters
    ----------
    *args : tuple
        Positional arguments to pass to the parent class constructor, if needed.
    **kwargs : dict
        Keyword arguments to configure the task. Passed directly to the `TaskDescription`
        initializer.

    Methods
    -------
    None. This class relies on inherited methods from `rp.TaskDescription`.
    """

    def __init__(self, **kwargs):
        # we pass only the kwargs as dict to the rp.TaskDescription
        # as it only allows from_dict
        super().__init__(from_dict=kwargs)