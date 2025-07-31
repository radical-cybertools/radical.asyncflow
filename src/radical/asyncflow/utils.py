import asyncio

def _get_event_loop_or_raise(context_name: str = "AsyncWorkflowEngine") -> asyncio.AbstractEventLoop:
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
