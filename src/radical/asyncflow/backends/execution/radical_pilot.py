import copy
import threading
import typeguard

from concurrent.futures import Future
from typing import Dict, Optional, Callable

import radical.utils as ru
import radical.pilot as rp

from ...constants import StateMapper
from .base import BaseExecutionBackend


def service_ready_callback(future, task, state):
    """Callback function for handling service task readiness.
    
    This callback is specifically designed for service tasks that need to wait
    for additional information before being considered ready. It runs the
    wait_info() call in a separate daemon thread to avoid blocking the main
    execution flow.
    
    Args:
        future (Future): The future object to set the result or exception on.
        task: The task object that has the wait_info() method.
        state: The current state of the task (unused in this callback).
    
    Note:
        The wait_info() call is synchronous and potentially blocking, so it's
        executed in a daemon thread. The future will be set with either the
        result or an exception based on the outcome.
    """
    def wait_and_set():
        try:
            info = task.wait_info()  # synchronous call
            future.set_result(info)
        except Exception as e:
            future.set_exception(e)

    threading.Thread(target=wait_and_set, daemon=True).start()


class RadicalExecutionBackend(BaseExecutionBackend):
    """Radical Pilot-based execution backend for large-scale HPC task execution.

    The RadicalExecutionBackend manages computing resources and task execution
    using the Radical Pilot framework. It interfaces with various resource
    management systems (SLURM, FLUX, etc.) on diverse HPC machines, providing
    capabilities for session management, task lifecycle control, and resource
    allocation.

    This backend supports both traditional task execution and advanced features
    like Raptor mode for high-throughput computing scenarios. It handles pilot
    submission, task management, and provides data dependency linking mechanisms.

    Attributes:
        session (rp.Session): Primary session for managing task execution context,
            uniquely identified by a generated ID.
        task_manager (rp.TaskManager): Manages task lifecycle including submission,
            tracking, and completion within the session.
        pilot_manager (rp.PilotManager): Coordinates computing resources (pilots)
            that are dynamically allocated based on resource requirements.
        resource_pilot (rp.Pilot): Submitted computing resources configured
            according to the provided resource specifications.
        tasks (Dict): Dictionary storing task descriptions indexed by UID.
        raptor_mode (bool): Flag indicating whether Raptor mode is enabled.
        masters (list): List of master tasks when Raptor mode is enabled.
        workers (list): List of worker tasks when Raptor mode is enabled.
        master_selector (callable): Generator for load balancing across masters.
        _callback_func (Callable): Registered callback function for task events.

    Args:
        resources (Dict): Resource requirements for the pilot including CPU, GPU,
            and memory specifications.
        raptor_config (Optional[Dict]): Configuration for enabling Raptor mode.
            Contains master and worker task specifications.

    Raises:
        Exception: If session creation, pilot submission, or task manager setup fails.
        SystemExit: If KeyboardInterrupt or SystemExit occurs during initialization.

    Example:
        ::
            resources = {
                "resource": "local.localhost",
                "runtime": 30,
                "exit_on_error": True,
                "cores": 4
            }
            backend = RadicalExecutionBackend(resources)
            
            # With Raptor mode
            raptor_config = {
                "masters": [{
                    "executable": "/path/to/master",
                    "arguments": ["--config", "master.conf"],
                    "ranks": 1,
                    "workers": [{
                        "executable": "/path/to/worker",
                        "arguments": ["--mode", "compute"],
                        "ranks": 4
                    }]
                }]
            }
            backend = RadicalExecutionBackend(resources, raptor_config)
    """

    @typeguard.typechecked
    def __init__(self, resources: Dict, raptor_config: Optional[Dict] = None) -> None:
        """Initialize the RadicalExecutionBackend with resources and optional Raptor config.

        Creates a new Radical Pilot session, initializes task and pilot managers,
        submits pilots based on resource configuration, and optionally enables
        Raptor mode for high-throughput computing.

        Args:
            resources (Dict): Resource configuration for the Radical Pilot session.
                Must contain valid pilot description parameters such as:
                - resource: Target resource (e.g., "local.localhost")
                - runtime: Maximum runtime in minutes
                - cores: Number of CPU cores
                - gpus: Number of GPUs (optional)
            raptor_config (Optional[Dict]): Configuration for Raptor mode containing:
                - masters: List of master task configurations
                - Each master can have associated workers
                Defaults to None (Raptor mode disabled).

        Raises:
            Exception: If RadicalPilot backend fails to initialize properly.
            SystemExit: If keyboard interrupt or system exit occurs during setup,
                with session path information for debugging.

        Note:
            - Automatically registers backend states with the global StateMapper
            - Prints status messages for successful initialization or failures
            - Session UID is generated using radical.utils for uniqueness
        """
        raptor_config = raptor_config or {}
        try:
            self.tasks = {}
            self.raptor_mode = False
            self.session = rp.Session(uid=ru.generate_id('asyncflow.session',
                                                          mode=ru.ID_PRIVATE))
            self.task_manager = rp.TaskManager(self.session)
            self.pilot_manager = rp.PilotManager(self.session)
            self.resource_pilot = self.pilot_manager.submit_pilots(rp.PilotDescription(resources))
            self.task_manager.add_pilots(self.resource_pilot)
            self._callback_func: Callable[[Future], None] = lambda f: None

            if raptor_config:
                self.raptor_mode = True
                print('Enabling Raptor mode for RadicalExecutionBackend')
                self.setup_raptor_mode(raptor_config)

            # register the backend task states to the global state manager
            StateMapper.register_backend_states(backend=self, 
                                                done_state=rp.DONE,
                                                failed_state=rp.FAILED,
                                                canceled_state=rp.CANCELED,
                                                running_state=rp.AGENT_EXECUTING)

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
    
    def get_task_states_map(self):
        """Get the state mapper for this backend.
        
        Returns:
            StateMapper: StateMapper instance configured for RadicalPilot backend
                with appropriate state mappings (DONE, FAILED, CANCELED, AGENT_EXECUTING).
        """
        return StateMapper(backend=self)

    def setup_raptor_mode(self, raptor_config):
        """Set up Raptor mode by configuring and submitting master and worker tasks.

        Initializes Raptor mode by creating master tasks and their associated
        worker tasks based on the provided configuration. Masters coordinate
        work distribution while workers execute the actual computations.

        Args:
            raptor_config (Dict): Configuration dictionary with the following structure:
                {
                    'masters': [
                        {
                            'executable': str,  # Path to master executable
                            'arguments': list,  # Arguments for master
                            'ranks': int,       # Number of CPU processes
                            'workers': [        # Worker configurations
                                {
                                    'executable': str,    # Worker executable path
                                    'arguments': list,    # Worker arguments
                                    'ranks': int,         # Worker CPU processes
                                    'worker_type': str    # Optional worker class
                                },
                                ...
                            ]
                        },
                        ...
                    ]
                }

        Raises:
            Exception: If task description creation or submission fails.

        Note:
            - Creates unique UIDs for masters and workers using session namespace
            - Sets up master selector for load balancing across masters
            - Workers default to 'DefaultWorker' class if not specified
            - All master and worker tasks are stored in respective class attributes
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
        """Create a generator for load balancing task submission across masters.
        
        Provides a round-robin generator that cycles through available master
        UIDs to distribute tasks evenly across all masters in Raptor mode.
        
        Returns:
            Generator[str]: Generator yielding master UIDs in round-robin fashion.
        
        Raises:
            RuntimeError: If Raptor mode is not enabled or no masters are available.
        
        Example:
            ::
                selector = backend.select_master()
                master_uid = next(selector)  # Get next master for task assignment
        """
        if not self.raptor_mode or not self.masters:
            raise RuntimeError('Raptor mode is not enabled or no masters available')

        current_master = 0
        masters_uids = [m.uid for m in self.masters]

        while True:
            yield masters_uids[current_master]
            current_master = (current_master + 1) % len(self.masters)

    def register_callback(self, func):
        """Register a callback function for task state changes.
        
        Sets up a callback mechanism that handles task state transitions,
        with special handling for service tasks that require additional
        readiness confirmation.
        
        Args:
            func (Callable): Callback function that will be invoked on task state changes.
                Should accept parameters: (task, state, service_callback=None).
        
        Note:
            - Service tasks in AGENT_EXECUTING state get special service_ready_callback
            - All other tasks use the standard callback mechanism
            - The callback is registered with the underlying task manager
        """

        self._callback_func = func

        def backend_callback(task, state):
            service_callback = None
            # Attach backend-specific done_callback for service tasks
            if task.mode == rp.TASK_SERVICE and state == rp.AGENT_EXECUTING:
                service_callback = service_ready_callback

            # Forward to workflow manager's standard callback
            func(task, state, service_callback=service_callback)

        self.task_manager.register_callback(backend_callback)

    def build_task(self, uid, task_desc, task_backend_specific_kwargs) -> rp.TaskDescription:
        """Build a RadicalPilot task description from workflow task parameters.
        
        Converts a workflow task description into a RadicalPilot TaskDescription,
        handling different task modes (executable, function, service) and applying
        appropriate configurations.
        
        Args:
            uid (str): Unique identifier for the task.
            task_desc (Dict): Task description containing:
                - executable: Path to executable (for executable tasks)
                - function: Python function (for function tasks)
                - args: Function arguments
                - kwargs: Function keyword arguments
                - is_service: Boolean indicating service task
            task_backend_specific_kwargs (Dict): RadicalPilot-specific parameters
                for the task description.
        
        Returns:
            rp.TaskDescription: Configured RadicalPilot task description, or None
                if task creation failed.
        
        Note:
            - Function tasks require Raptor mode to be enabled
            - Service tasks cannot be Python functions
            - Failed tasks trigger callback with FAILED state
            - Raptor tasks are assigned to masters via load balancing
        
        Example:
            ::
                task_desc = {
                    'executable': '/bin/echo',
                    'args': ['Hello World'],
                    'is_service': False
                }
                rp_task = backend.build_task('task_001', task_desc, {})
        """

        is_service = task_desc.get('is_service', False)
        rp_task = rp.TaskDescription(from_dict=task_backend_specific_kwargs)
        rp_task.uid = uid

        if task_desc['executable']:
            rp_task.mode = rp.TASK_SERVICE if is_service else rp.TASK_EXECUTABLE
            rp_task.executable = task_desc['executable']
        elif task_desc['function']:
            if is_service:
                error_msg = 'RadicalExecutionBackend does not support function service tasks'
                rp_task['exception'] = ValueError(error_msg)
                self._callback_func(rp_task, rp.FAILED)
                return

            rp_task.mode = rp.TASK_FUNCTION
            rp_task.function = rp.PythonTask(task_desc['function'],
                                             task_desc['args'],
                                             task_desc['kwargs'])

        if rp_task.mode in [rp.TASK_FUNCTION, rp.TASK_EVAL,
                            rp.TASK_PROC, rp.TASK_METHOD]:
            if not self.raptor_mode:
                error_msg = f'Raptor mode is not enabled, cannot register {rp_task.mode}'
                rp_task['exception'] =  RuntimeError(error_msg)
                self._callback_func(rp_task, rp.FAILED)
                return

            rp_task.raptor_id = next(self.master_selector)

        self.tasks[uid] = rp_task

        return rp_task

    def link_explicit_data_deps(self, src_task=None, dst_task=None, file_name=None, file_path=None):
        """Link explicit data dependencies between tasks or from external sources.

        Creates data staging entries to establish explicit dependencies where
        files are transferred or linked from source to destination tasks.
        Supports both task-to-task dependencies and external file staging.

        Args:
            src_task (Optional[Dict]): Source task dictionary containing the file.
                None when staging from external path.
            dst_task (Dict): Destination task dictionary that will receive the file.
                Must contain 'task_backend_specific_kwargs' key.
            file_name (Optional[str]): Name of the file to stage. Defaults to:
                - src_task UID if staging from task
                - basename of file_path if staging from external path
            file_path (Optional[str]): External file path to stage (alternative
                to task-sourced files).

        Returns:
            Dict: The data dependency dictionary that was added to input staging.

        Raises:
            ValueError: If neither file_name nor file_path is provided, or if
                src_task is missing when file_path is not specified.

        Note:
            - External files use TRANSFER action
            - Task-to-task dependencies use LINK action
            - Files are staged to task:/// namespace in destination
            - Input staging list is created if it doesn't exist

        Example:
            ::
                # Link output from task1 to task2
                backend.link_explicit_data_deps(
                    src_task={'uid': 'task1'},
                    dst_task={'task_backend_specific_kwargs': {}},
                    file_name='output.dat'
                )
                
                # Stage external file
                backend.link_explicit_data_deps(
                    dst_task={'task_backend_specific_kwargs': {}},
                    file_path='/path/to/input.txt'
                )
        """
        if not file_name and not file_path:
            raise ValueError('Either file_name or file_path must be provided')

        dst_kwargs = dst_task['task_backend_specific_kwargs']

        # Determine the filename if not provided
        if not file_name:
            if file_path:
                file_name = file_path.split('/')[-1]  # Use basename from path
            elif src_task:
                file_name = src_task['uid']  # Fallback to task UID
            else:
                raise ValueError("Must provide either file_name, file_path, or src_task")

        # Create the appropriate data dependency
        if file_path:
            data_dep = {
                'source': file_path,
                'target': f"task:///{file_name}",
                'action': rp.TRANSFER}
        else:
            if not src_task:
                raise ValueError("src_task must be provided when file_path is not specified")
            data_dep = {
                'source': f"pilot:///{src_task['uid']}/{file_name}",
                'target': f"task:///{file_name}",
                'action': rp.LINK}

        # Add to input staging
        if 'input_staging' not in dst_kwargs:
            dst_kwargs['input_staging'] = [data_dep]
        else:
            dst_kwargs['input_staging'].append(data_dep)

        return data_dep

    def link_implicit_data_deps(self, src_task, dst_task):
        """Add implicit data dependencies through symbolic links in task sandboxes.

        Creates pre-execution commands that establish symbolic links from the
        source task's sandbox to the destination task's sandbox, simulating
        implicit data dependencies without explicit file specifications.

        Args:
            src_task (Dict): Source task dictionary containing 'uid' key.
            dst_task (Dict): Destination task dictionary with 'task_backend_specific_kwargs'.

        Note:
            - Links all files from source sandbox except the task UID file itself
            - Uses environment variables for source task identification
            - Commands are added to the destination task's pre_exec list
            - Symbolic links are created in the destination task's sandbox

        Implementation Details:
            1. Sets SRC_TASK_ID environment variable
            2. Sets SRC_TASK_SANDBOX path variable
            3. Creates symbolic links for all files except the task ID file

        Example:
            ::
                src_task = {'uid': 'producer_task'}
                dst_task = {'task_backend_specific_kwargs': {}}
                backend.link_implicit_data_deps(src_task, dst_task)
        """

        dst_kwargs = dst_task['task_backend_specific_kwargs']
        src_uid = src_task['uid']

        cmd1 = f'export SRC_TASK_ID={src_uid}'
        cmd2 = f'export SRC_TASK_SANDBOX="$RP_PILOT_SANDBOX/$SRC_TASK_ID"'

        cmd3 = '''files=$(cd "$SRC_TASK_SANDBOX" && ls | grep -ve "^$SRC_TASK_ID")
                for f in $files
                do 
                    ln -sf "$SRC_TASK_SANDBOX/$f" "$RP_TASK_SANDBOX"
                done'''

        commands = [cmd1, cmd2, cmd3]

        if dst_kwargs.get('pre_exec'):
            dst_kwargs['pre_exec'].extend(commands)
        else:
            dst_kwargs['pre_exec'] = commands

    def submit_tasks(self, tasks: list):
        """Submit a list of tasks for execution.

        Processes a list of workflow tasks, builds RadicalPilot task descriptions,
        and submits them to the task manager for execution. Handles task building
        failures gracefully by skipping invalid tasks.

        Args:
            tasks (list): List of task dictionaries, each containing:
                - uid: Unique task identifier
                - task_backend_specific_kwargs: RadicalPilot-specific parameters
                - Other task description fields

        Returns:
            The result of task_manager.submit_tasks() with successfully built tasks.

        Note:
            - Failed task builds are skipped (build_task returns None)
            - Only successfully built tasks are submitted to the task manager
            - Task building includes validation and error handling
        """

        _tasks = []
        for task in tasks:
            task_to_submit = self.build_task(task['uid'],
                          task, task['task_backend_specific_kwargs'])
            
            if not task_to_submit:
                continue

            _tasks.append(task_to_submit)

        return self.task_manager.submit_tasks(_tasks)

    def cancel_task(self, uid: str) -> bool:
        """
        Cancel a task in the execution backend.

        Args:
            uid (str): The UID of the task to cancel.

        Returns:
            bool: True if the task was found and cancellation was attempted, False otherwise.
        """
        if uid in self.tasks:
            self.task_manager.cancel_tasks(uid)
            return True
        return False

    def get_nodelist(self):
        """Get information about allocated compute nodes.

        Retrieves the nodelist from the active resource pilot, providing
        details about the compute nodes allocated for task execution.

        Returns:
            rp.NodeList: NodeList object containing information about allocated
                nodes. Each node in nodelist.nodes is of type rp.NodeResource.
                Returns None if the pilot is not in PMGR_ACTIVE state.

        Note:
            - Only returns nodelist when pilot is in active state
            - Nodelist provides detailed resource information for each node
            - Useful for resource-aware task scheduling and monitoring
        """
        nodelist = None
        if self.resource_pilot.state == rp.PMGR_ACTIVE:
            nodelist = self.resource_pilot.nodelist

        return nodelist

    def state(self):
        """Retrieve the current state of the resource pilot.

        Returns:
            The current state of the resource pilot.

        Note:
            This method is currently not implemented and serves as a placeholder.
        """
        raise NotImplementedError

    def task_state_cb(self, task, state):
        """Callback function for handling task state changes.

        Args:
            task: The task object whose state has changed.
            state: The new state of the task.
        
        Note:
            This method is currently not implemented and serves as a placeholder
            for custom task state change handling.
        """
        raise NotImplementedError

    def shutdown(self) -> None:
        """Gracefully shutdown the backend and clean up resources.

        Closes the RadicalPilot session with data download, ensuring proper
        cleanup of all resources including pilots, tasks, and session data.

        Note:
            - Downloads session data before closing
            - Ensures graceful termination of all backend resources
            - Prints confirmation message when shutdown is triggered
        """
        print('Shutdown is triggered, terminating the resources gracefully')
        self.session.close(download=True)
