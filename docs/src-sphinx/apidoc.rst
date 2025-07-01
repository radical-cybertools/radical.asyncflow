
.. _chapter_api_reference:

*************
API Reference
*************

.. toctree::

Workflow Manager
--------------------
.. autoclass:: radical.flow.workflow_manager.WorkflowEngine
   :members:

Task
----
.. autoclass:: radical.flow.task.Task
   :members:

Data
----
.. autoclass:: radical.flow.data.File
   :members:

.. autoclass:: radical.flow.data.InputFile
   :members:

.. autoclass:: radical.flow.data.OutputFile
   :members:

Backends
--------
.. autoclass:: radical.flow.backends.execution.base.BaseExecutionBackend
   :members:

.. autoclass:: radical.flow.backends.execution.base.Session
   :members:

.. autoclass:: radical.flow.backends.execution.noop.NoopExecutionBackend
   :members:

.. autoclass:: radical.flow.backends.execution.thread_pool.ThreadExecutionBackend
   :members:

.. autoclass:: radical.flow.backends.execution.radical_pilot.RadicalExecutionBackend
   :members:

.. autoclass:: radical.flow.backends.execution.dask_parallel.DaskExecutionBackend
   :members:
