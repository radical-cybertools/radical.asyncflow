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
    """
    Get the current running event loop or raise a helpful error.

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
