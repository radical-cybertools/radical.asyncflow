import copy
import threading
import typeguard
from typing import Dict, Optional
import radical.utils as ru
import radical.pilot as rp

from ...constants import StateMapper
from .base import BaseExecutionBackend


def service_ready_callback(future, task, state):
    def wait_and_set():
        try:
            info = task.wait_info()  # synchronous call
            future.set_result(info)
        except Exception as e:
            future.set_exception(e)

    threading.Thread(target=wait_and_set, daemon=True).start()


class RadicalExecutionBackend(BaseExecutionBackend):
    """Manages HPC resources and task execution using RADICAL-Pilot.

    A backend implementation that interfaces with various resource management
    systems (e.g., SLURM, FLUX) on HPC machines. Handles session management,
    resource allocation, and task execution at scale.

    Args:
        resources (Dict): Resource requirements for the pilot, specifying:
            - CPU cores
            - GPU devices
            - Memory allocation
            - Other resource-specific parameters
        raptor_config (Optional[Dict]): Configuration for Raptor execution mode.
            Defaults to None.

    Attributes:
        session (rp.Session): Main session context with unique ID for task tracking
        task_manager (rp.TaskManager): Manages task lifecycle and execution
        pilot_manager (rp.PilotManager): Manages compute resource allocation
        resource_pilot (rp.Pilot): Active pilot job on the compute resources

    Example:
        >>> resources = {
        ...     "cpu": 4,
        ...     "gpu": 1,
        ...     "memory": "8GB"
        ... }
        >>> backend = RadicalExecutionBackend(resources)

    Note:
        The backend automatically handles:
        - Session creation and management
        - Resource allocation via pilots
        - Task scheduling and execution
        - Data staging and movement
    """

    @typeguard.typechecked
    def __init__(self, resources: Dict, raptor_config: Optional[Dict] = None) -> None:
        """Initializes a RadicalPilot execution backend instance.

        Sets up a RADICAL-Pilot session with the specified resource
        configuration and optional Raptor mode settings.

        Args:
            resources (Dict): Resource configuration containing:
                - Resource allocation requirements
                - System-specific parameters
                - Hardware specifications (CPU, GPU, memory)
            raptor_config (Optional[Dict], optional): Raptor mode settings.
                When provided, enables and configures Raptor execution mode.
                Defaults to None.

        Raises:
            Exception: If backend initialization fails
            SystemExit: If initialization is interrupted, includes session path
                in error message for debugging

        Side Effects:
            - Creates RADICAL-Pilot session
            - Initializes task and pilot managers
            - Submits pilot job
            - Configures Raptor mode if specified
            - Registers state mapping handlers

        Note:
            The initialization process follows these steps:
            1. Creates and configures session components
            2. Submits pilot based on resource configuration
            3. Enables Raptor mode if configured
            4. Sets up state mapping and callbacks
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
        return StateMapper(backend=self)

    def setup_raptor_mode(self, raptor_config):
        """Sets up Raptor execution mode with masters and workers.

        Initializes Raptor mode by configuring and submitting master tasks and
        their associated worker tasks to the resource pilot.

        Args:
            raptor_config (dict): Configuration dictionary containing masters.

        Master and Worker Configuration:
            Masters configurations:
                executable (str): Path to master executable
                arguments (list): Command line arguments
                ranks (int): Number of CPU processes
                workers (list[dict]):
            Worker configurations:
                executable (str): Path to worker executable
                arguments (list): Command line arguments
                ranks (int): Number of CPU processes
                worker_type (str, optional): Worker class name. Defaults to 'DefaultWorker'

        Attributes Modified:
            masters (list): Created and submitted master tasks
            workers (list): Created and submitted worker tasks
            master_selector (generator): Round-robin master selection generator

        Raises:
            rp.TaskDescription.Exception: On invalid task configuration
            RuntimeError: On submission failures

        Process Flow:
            1. Deep copies config to preserve original
            2. Creates and submits master tasks
            3. Creates and submits associated worker tasks
            4. Sets up master selection for task distribution
            5. Links workers to their respective masters

        Note:
            Each worker is automatically configured with a unique ID and
            linked to its master via raptor_id.
        """

        self.masters = []
        self.workers = []
        self.master_selector = self.select_master()

        cfg = copy.deepcopy(raptor_config)
        masters = cfg['masters']

        for master_description in masters:
            workers = master_description.pop('workers')

            md = rp.TaskDescription(master_description)
            md.uid = ru.generate_id('flow.master.%(item_counter)06d', ru.ID_CUSTOM, ns=self.session.uid)
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
        """Configures explicit data staging between tasks or from external sources.

        Sets up data dependencies by adding input staging entries to destination
        tasks. Supports both task-to-task data movement and staging from external
        paths.

        Args:
            src_task (Optional[dict]): Source task containing the file. None for
                external files. Contains task metadata and configuration.
            dst_task (Optional[dict]): Destination task where file will be staged. Must contain 'task_backend_specific_kwargs' field.
            file_name (Optional[str]): Target file name. Will default to:
                - basename of file_path if staging from path
                - src_task UID if staging from task
            file_path (Optional[str]): External file path for staging. Alternative to task-sourced files.

        Returns:
            dict: Staging configuration containing:
                - source: Original file location (task or path based)
                - target: Destination path in task sandbox
                - action: TRANSFER for external files, LINK for task files

        Raises:
            ValueError: If neither file_name nor file_path is provided
            ValueError: If src_task is missing when staging from task

        Note:
            At least one of file_name/file_path must be provided. For
            task-to-task transfers, src_task is required.
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
        """Creates implicit data dependencies between tasks via symlinks.

        Configures pre-execution commands in the destination task to create
        symlinks to all files in the source task's sandbox directory. This
        provides implicit data staging between tasks.

        Args:
            src_task (dict): Source task whose sandbox contains the files to link
                Must contain 'uid' field for identifying the source sandbox
            dst_task (dict): Destination task where symlinks will be created
                Must contain 'task_backend_specific_kwargs' for configuring
                pre-execution commands

        Side Effects:
            Modifies dst_task's 'pre_exec' commands to:
            - Set environment variables for source task location
            - Create symlinks to all files in source task sandbox
            - Skip files matching the source task's UID pattern

        Note:
            The implementation uses shell commands executed before task launch
            to establish the data dependencies at runtime. This approach allows
            flexible data sharing without explicit file declarations.
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
        """Submits a list of tasks to the RADICAL-Pilot task manager.

        Processes and submits multiple tasks for execution. Each task is built
        using the task description and backend-specific parameters before
        submission to the task manager.

        Args:
            tasks (list): List of task dictionaries, each containing:
                - uid (str): Unique identifier for the task
                - task_backend_specific_kwargs (dict): Backend configuration
                - Other task-specific parameters and descriptions

        Returns:
            list: Submitted task objects managed by RADICAL-Pilot

        Note:
            Tasks that fail during build phase (e.g., due to invalid
            configuration or Raptor mode requirements) are filtered out
            before submission.

        Process Flow:
            1. Builds each task using build_task()
            2. Filters out failed or invalid tasks
            3. Submits valid tasks to task manager
            4. Returns list of submitted task objects
        """
        _tasks = []
        for task in tasks:
            task_to_submit = self.build_task(task['uid'],
                          task, task['task_backend_specific_kwargs'])

            if not task_to_submit:
                continue

            _tasks.append(task_to_submit)

        return self.task_manager.submit_tasks(_tasks)

    def get_nodelist(self):
        """Retrieves information about allocated compute nodes.

        Gets the list of nodes allocated to the current pilot job. Only
        returns information when the pilot is in an active state.

        Returns:
            rp.NodeList: Object containing allocated node information:
                - nodes (list[rp.NodeResource]): List of node resources
                - Each node contains hardware/system information

        Note:
            Returns None if the pilot is not in PMGR_ACTIVE state
        """
        nodelist = None
        if self.resource_pilot.state == rp.PMGR_ACTIVE:
            nodelist = self.resource_pilot.nodelist

        return nodelist

    def state(self):
        """Retrieves the current state of the resource pilot.

        Returns:
            str: Current state of the pilot job

        Raises:
            NotImplementedError: This method must be implemented by subclasses

        Note:
            State values are specific to the backend implementation
        """
        raise NotImplementedError

    def task_state_cb(self, task, state):
        """Handles state change callbacks for tasks.

        Called when a task's state changes to perform any necessary
        state-specific actions.

        Args:
            task (Union[dict, object]): Task object whose state changed
            state (str): New state of the task

        Raises:
            NotImplementedError: This method must be implemented by subclasses

        Note:
            Subclasses should override this to implement specific state
            handling logic
        """
        raise NotImplementedError

    def shutdown(self) -> None:
        """Performs graceful shutdown of the execution backend.

        Closes the current session and ensures all data is properly downloaded
        before termination. This method should be called when workflow
        execution is complete or needs to be terminated.

        Returns:
            None

        Side Effects:
            - Closes active session
            - Downloads any pending data
            - Releases allocated resources
            - Logs shutdown message
        """
        print('Shutdown is triggered, terminating the resources gracefully')
        self.session.close(download=True)
        """
        print('Shutdown is triggered, terminating the resources gracefully')
        self.session.close(download=True)
        print('Shutdown is triggered, terminating the resources gracefully')
        self.session.close(download=True)
        """
        print('Shutdown is triggered, terminating the resources gracefully')
        self.session.close(download=True)
