import asyncio

_global_task_counter = 0


def get_next_uid():
    """Return the next unique task ID."""
    global _global_task_counter
    _global_task_counter += 1
    return f"{_global_task_counter:06d}"


def reset_uid_counter():
    """Reset the counter to zero (only call when backend shuts down)."""
    global _global_task_counter
    _global_task_counter = 0


def get_event_loop_or_raise(
    context_name: str = "AsyncWorkflowEngine",
) -> asyncio.AbstractEventLoop:
    """Get the current running event loop or raise a helpful error.

    Args:
        context_name: Name of the class/context for error messages

    Returns:
        asyncio.AbstractEventLoop: The current running event loop

    Raises:
        RuntimeError: If no event loop is running with helpful guidance
    """
    try:
        return asyncio.get_running_loop()
    except RuntimeError as e:
        raise RuntimeError(
            f"{context_name} must be created within an async context. "
            f"Use 'await {context_name}.create(...)' from within an async function, "
            "or run within asyncio.run()."
        ) from e


def register_optional_backends(globals_dict, all_list, package, backend_names):
    """Register optional backend classes, handling ImportError gracefully.

    Args:
        globals_dict: The globals() dictionary to update with backend classes
        all_list: The __all__ list to append backend names to if import succeeds
        package: The package name to import from (e.g. 'rhapsody.backends.execution')
        backend_names: List of backend class names to attempt importing
    """
    for backend_name in backend_names:
        try:
            # Import the module and get the backend class
            module = __import__(package, fromlist=[backend_name])
            backend_class = getattr(module, backend_name)
            globals_dict[backend_name] = backend_class
            all_list.append(backend_name)
        except ImportError:
            # Set to None to indicate the backend is unavailable
            globals_dict[backend_name] = None
