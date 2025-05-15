import copy
import typeguard
from typing import Dict, Optional
import radical.utils as ru
import radical.pilot as rp

from .base import BaseExecutionBackend

class RadicalExecutionBackend(BaseExecutionBackend):
    """
    The RadicalExecutionBackend class is responsible for managing computing resources
    and creating sessions for executing tasks on a large scale. It interfaces with
    different resource management systems such SLURM and FLUX on diverse HPC machines.
    This backend is capable of initialize sessions, manage task execution, and submit
    resources required for the workflow.

    Attributes:
        session (rp.Session): A session instance used to manage and track task execution,
            uniquely identified by a generated ID. This session serves as the primary context for
            all task and resource management within the workflow.

        task_manager (rp.TaskManager): Manages the lifecycle of tasks, handling their submission,
            tracking, and completion within the session.

        pilot_manager (rp.PilotManager): Manages computing resources, known as "pilots," which
            are dynamically allocated based on the provided resources. The pilot manager
            coordinates these resources to support task execution.

        resource_pilot (rp.Pilot): Represents the submitted computing resources as a pilot.
            This pilot is described by the `resources` parameter provided during initialization,
            specifying details such as CPU, GPU, and memory requirements.

    Parameters:
        resources (Dict): A dictionary specifying the resource requirements for the pilot,
            including details like the number of CPUs, GPUs, and memory. This dictionary
            configures the pilot to match the needs of the tasks that will be executed.

    Raises:
        Exception: If session creation, pilot submission, or task manager setup fails,
            the RadicalExecutionBackend will raise an exception, ensuring the resources
            are correctly allocated and managed.

    Example:
        ```python
        resources = {"cpu": 4, "gpu": 1, "memory": "8GB"}
        backend = RadicalExecutionBackend(resources)
        ```
    """

    @typeguard.typechecked
    def __init__(self, resources: Dict, raptor_config: Optional[Dict] = None) -> None:
        """
        Initialize the RadicalExecutionBackend with the given resources and optional
        Raptor configuration.
        Args:
            resources (Dict): A dictionary specifying the resource configuration
                for the Radical Pilot session.
            raptor_config (Optional[Dict]): An optional dictionary containing
                configuration for enabling Raptor mode. Defaults to None.
        Raises:
            Exception: If the RadicalPilot execution backend fails to start.
            SystemExit: If a KeyboardInterrupt or SystemExit is encountered during
                initialization, providing a message with the session path for debugging.
        Notes:
            - Initializes a Radical Pilot session, task manager, and pilot manager.
            - Submits pilots based on the provided resource configuration.
            - Adds the pilots to the task manager.
            - Enables Raptor mode if a configuration is provided.
        """
        raptor_config = raptor_config or {}
        try:
            self.tasks = {}
            self.raptor_mode = False
            self.session = rp.Session(uid=ru.generate_id('flow.session',
                                                          mode=ru.ID_PRIVATE))
            self.task_manager = rp.TaskManager(self.session)
            self.pilot_manager = rp.PilotManager(self.session)
            self.resource_pilot = self.pilot_manager.submit_pilots(rp.PilotDescription(resources))
            self.task_manager.add_pilots(self.resource_pilot)

            if raptor_config:
                self.raptor_mode = True
                print('Enabling Raptor mode for RadicalExecutionBackend')
                self.setup_raptor_mode(raptor_config)

            print('RadicalPilot execution backend started successfully\n')

        except Exception:
            print('RadicalPilot execution backend Failed to start, terminating\n')
            raise

        except (KeyboardInterrupt, SystemExit) as e:
            # the callback called sys.exit(), and we can here catch the
            # corresponding KeyboardInterrupt exception for shutdown.  We also catch
            # SystemExit (which gets raised if the main threads exits for some other
            # reason).
            exception_msg = f'Radical execution backend failed'
            exception_msg += f' internally, please check {self.session.path}'
            
            raise SystemExit(exception_msg) from e

    def setup_raptor_mode(self, raptor_config):
        """
        Sets up the Raptor mode by configuring and submitting master and worker tasks.

        This method initializes the Raptor mode by creating and submitting master tasks
        and their associated worker tasks to the resource pilot. The configuration for
        the masters and workers is provided through the `raptor_config` dictionary.

        Args:
            raptor_config (dict): A dictionary containing the configuration for the
                Raptor mode. The structure of the dictionary is as follows:
                            'executable': str,  # Path to the master executable
                            'arguments': list,  # List of arguments for the master
                            'ranks': int,  # Number of ranks (CPU processes) for the master
                            'workers': [  # List of worker configurations
                                    'executable': str,  # Path to the worker executable
                                    'arguments': list,  # List of arguments for the worker
                                    'ranks': int  # Number of ranks (CPU processes) for the worker
                                },
                                ...
                            ]
                        },
                        ...
                    ]
                }

        Attributes:
            masters (list): A list of master tasks created and submitted.
            workers (list): A list of worker tasks created and submitted.
            master_selector (callable): A callable used to select a master.

        Steps:
            1. Deep copies the `raptor_config` to avoid modifying the original.
            2. Iterates through the master configurations in `raptor_config['masters']`.
            3. Extracts and removes the worker configurations from each master configuration.
            4. Creates and submits a master task using the `rp.TaskDescription`.
            5. Iterates through the worker configurations and creates worker tasks
               associated with the corresponding master.
            6. Submits the worker tasks to the master and stores them in the `workers` list.

        Raises:
            Any exceptions raised by the `rp.TaskDescription` or submission methods
            will propagate to the caller.
        """

        self.masters = []
        self.workers = []
        self.master_selector = self.select_master()

        cfg = copy.deepcopy(raptor_config)
        masters = cfg['masters']

        for master_description in masters:
            workers = master_description.pop('workers')

            md = rp.TaskDescription(master_description)
            md.uid = ru.generate_id('flow.master.%(item_counter)06d', ru.ID_CUSTOM,
                                     ns=self.session.uid)
            md.mode = rp.RAPTOR_MASTER
            master = self.resource_pilot.submit_raptors(md)[0]
            self.masters.append(master)

            for worker_description in workers:
                # Set default worker class and override if specified
                raptor_class = worker_description.pop('worker_type', 'DefaultWorker')

                # Create and configure worker
                worker = master.submit_workers(
                    rp.TaskDescription({
                        **worker_description,
                        'raptor_id': md.uid,
                        'mode': rp.RAPTOR_WORKER,
                        'raptor_class': raptor_class,
                        'uid': ru.generate_id('flow.worker.%(item_counter)06d', 
                                              ru.ID_CUSTOM, ns=self.session.uid)}))

                self.workers.append(worker)

    def select_master(self):
        """
        Balance tasks submission across N masters and N workers
        """
        if not self.raptor_mode or not self.masters:
            raise RuntimeError('Raptor mode is not enabled or no masters available')

        current_master = 0
        masters_uids = [m.uid for m in self.masters]

        while True:
            yield masters_uids[current_master]
            current_master = (current_master + 1) % len(self.masters)

    def register_callback(self, func):
        return self.task_manager.register_callback(func)

    def build_task(self, uid, task_desc, task_backend_specific_kwargs) -> rp.TaskDescription:

        rp_task = rp.TaskDescription(from_dict=task_backend_specific_kwargs)
        rp_task.uid = uid

        if task_desc['executable']:
            rp_task.executable = task_desc['executable']
        elif task_desc['function']:
            rp_task.mode = rp.TASK_FUNCTION
            rp_task.function = rp.PythonTask(task_desc['function'],
                                             task_desc['args'],
                                             task_desc['kwargs'])

        if rp_task.mode in [rp.TASK_FUNCTION, rp.TASK_EVAL,
                            rp.TASK_PROC, rp.TASK_METHOD]:
            if not self.raptor_mode:
                error_msg = f'Raptor mode is not enabled, cannot register {rp_task.mode}'
                raise RuntimeError(error_msg)

            rp_task.raptor_id = next(self.master_selector)

        self.tasks[uid] = rp_task

        return rp_task

    def link_explicit_data_deps(self, task_id, file_name=None, file_path=None):
        """
        Creates a dictionary linking explicit data dependencies between tasks.

        This method defines the source and target for data transfer between tasks, 
        using the provided task ID and optional file name. If no file name is provided, 
        the task ID is used as the file name.

        Args:
            task_id (str): The task ID to link data dependencies from.
            file_name (str, optional): The file name to be used in the data dependency. 
                                        Defaults to None, in which case the task_id is used.

        Returns:
            dict: A dictionary containing the data dependencies, including source, 
                target, and transfer action.
        """
        if not file_name and not file_path:
            raise ValueError('Either file_name or file_path must be provided')

        task = self.tasks[task_id]

        if not file_name:
            file_name = task_id

        if file_name:
            data_deps = {'source': f"pilot:///{task_id}/{file_name}",
                        'target': f"task:///{file_name}", 'action': rp.LINK}

        if file_path and file_name:
            data_deps = {'source': file_path,
                         'target': f"task:///{file_name}", 'action': rp.TRANSFER}

        task.input_staging.append(data_deps)

        return data_deps

    def link_implicit_data_deps(self, src_task):
        """
        Generates commands to link implicit data dependencies for a source task.

        This method creates shell commands to copy files from the source task's sandbox
        to the current task's sandbox, excluding task-related files. The commands are 
        returned as a list of Python shell commands to execute in the task environment.

        Args:
            src_task (Task): The source task whose files are to be copied.

        Returns:
            list: A list of shell commands to link implicit data dependencies between tasks.
        """
        task = self.tasks[src_task['uid']]
        cmd1 = f'export SRC_TASK_ID={src_task['uid']}'
        cmd2 = f'export SRC_TASK_SANDBOX="$RP_PILOT_SANDBOX/$SRC_TASK_ID"'

        cmd3 = '''files=$(cd "$SRC_TASK_SANDBOX" && ls | grep -ve "^$SRC_TASK_ID")
                  for f in $files
                  do 
                     ln -sf "$SRC_TASK_SANDBOX/$f" "$RP_TASK_SANDBOX"
                  done'''

        commands = [cmd1, cmd2, cmd3]

        task.pre_exec.extend(commands)

    def submit_tasks(self, tasks: list):

        _tasks = []
        for task in tasks:
            _tasks.append(self.build_task(task['uid'],
                          task, task['task_backend_specific_kwargs']))

        return self.task_manager.submit_tasks(_tasks)

    def state(self):
        """
        Retrieve the current state of the resource pilot.

        Returns:
            The current state of the resource pilot.
        """
        raise NotImplementedError

    def task_state_cb(self, task, state):
        """
        Callback function for handling task state changes.

        Args:
            task: The task object whose state has changed.
            state: The new state of the task.
        
        Note:
            This method is intended to be overridden or extended
            to perform specific actions when a task's state changes.
        """
        raise NotImplementedError

    def shutdown(self) -> None:
        """
        Gracefully shuts down the session, downloading any necessary data.

        This method ensures that the session is properly closed and any
        required data is downloaded before finalizing the shutdown.

        Returns:
            None
        """
        print('Shutdown is triggered, terminating the resources gracefully')
        self.session.close(download=True)
