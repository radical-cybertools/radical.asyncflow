import asyncio
import time

import pytest
import pytest_asyncio

from radical.asyncflow import LocalExecutionBackend, WorkflowEngine
from radical.asyncflow.errors import DependencyFailureError


class TestFutureResolution:
    """Test suite for Future argument resolution in WorkflowEngine."""

    @pytest_asyncio.fixture
    async def flow(self):
        """Create a WorkflowEngine instance for testing."""
        backend = await LocalExecutionBackend()
        flow = await WorkflowEngine.create(backend=backend)
        yield flow
        await flow.shutdown()

    @pytest.mark.asyncio
    async def test_simple_future_resolution(self, flow):
        """Test that Future arguments are resolved to actual values."""

        @flow.function_task
        async def task1():
            return 42

        @flow.function_task
        async def task2():
            return 24

        @flow.function_task
        async def task3(val1, val2):
            # These should be actual values, not Futures
            msg1 = "val1 should be resolved value, not Future"
            msg2 = "val2 should be resolved value, not Future"
            assert not isinstance(val1, asyncio.Future), msg1
            assert not isinstance(val2, asyncio.Future), msg2
            assert val1 == 42, f"Expected 42, got {val1}"
            assert val2 == 24, f"Expected 24, got {val2}"
            return val1 + val2

        # Execute workflow
        t1 = task1()
        t2 = task2()
        result_future = task3(t1, t2)  # Pass futures as arguments

        # Wait for completion
        result = await result_future
        assert result == 66, f"Expected 66, got {result}"

    @pytest.mark.asyncio
    async def test_mixed_args_resolution(self, flow):
        """Test resolution with mix of Futures and regular values."""

        @flow.function_task
        async def producer():
            return "from_future"

        @flow.function_task
        async def consumer(
            future_arg, regular_arg, kwarg_future=None, kwarg_regular=None
        ):
            # Check argument types and values
            assert not isinstance(future_arg, asyncio.Future)
            assert future_arg == "from_future"
            assert regular_arg == "regular_value"
            assert kwarg_future == "kwarg_from_future"
            assert kwarg_regular == "kwarg_regular_value"
            return "success"

        # Setup
        future_result = producer()

        @flow.function_task
        async def kwarg_producer():
            return "kwarg_from_future"

        kwarg_future = kwarg_producer()

        # Execute with mixed arguments
        result_future = consumer(
            future_result,  # Future argument
            "regular_value",  # Regular argument
            kwarg_future=kwarg_future,  # Future keyword argument
            kwarg_regular="kwarg_regular_value",  # Regular keyword argument
        )

        result = await result_future
        assert result == "success"

    @pytest.mark.asyncio
    async def test_no_futures_in_args(self, flow):
        """Test that tasks without Future arguments work normally."""

        @flow.function_task
        async def simple_task(val1, val2, kwarg1=None):
            return val1 + val2 + (kwarg1 or 0)

        # Execute with only regular arguments
        result_future = simple_task(10, 20, kwarg1=5)
        result = await result_future

        assert result == 35

    @pytest.mark.asyncio
    async def test_nested_dependency_chain(self, flow):
        """Test resolution in a chain of dependencies."""

        @flow.function_task
        async def step1():
            return 1

        @flow.function_task
        async def step2(prev_val):
            assert prev_val == 1
            return prev_val * 2

        @flow.function_task
        async def step3(prev_val):
            assert prev_val == 2
            return prev_val * 3

        @flow.function_task
        async def final_step(val1, val2):
            assert val1 == 1
            assert val2 == 6
            return val1 + val2

        # Build dependency chain
        s1 = step1()
        s2 = step2(s1)
        s3 = step3(s2)
        result_future = final_step(s1, s3)  # Multiple dependencies

        result = await result_future
        assert result == 7

    @pytest.mark.asyncio
    async def test_duplicate_future_args_resolve(self, flow):
        """A future passed twice is one dependency and must not hang the task."""

        @flow.function_task
        async def producer():
            await asyncio.sleep(0.3)  # still pending when consumer registers
            return 1

        @flow.function_task
        async def consumer(val1, val2):
            return val1 + val2

        fut = producer()
        result = await asyncio.wait_for(consumer(fut, fut), timeout=5)
        assert result == 2

    @pytest.mark.asyncio
    async def test_duplicate_future_args_block_consumer(self, flow):
        """A block receiving the same future twice must not hang."""

        @flow.function_task
        async def producer():
            await asyncio.sleep(0.3)  # still pending when block registers
            return 1

        @flow.block
        async def consumer_block(val1, val2):
            return val1 + val2

        fut = producer()
        result = await asyncio.wait_for(consumer_block(fut, fut), timeout=5)
        assert result == 2

    @pytest.mark.asyncio
    async def test_pending_kwarg_future_resolves(self, flow):
        """A future passed as a keyword argument is a dependency."""

        @flow.function_task
        async def producer():
            await asyncio.sleep(0.3)  # still pending when consumer registers
            return 1

        @flow.function_task
        async def consumer(x=None):
            return x

        result = await asyncio.wait_for(consumer(x=producer()), timeout=5)
        assert result == 1

    @pytest.mark.asyncio
    async def test_failed_kwarg_dependency_raises_dependency_failure(self, flow):
        """A failed keyword-argument dependency fails the dependent cleanly."""

        @flow.function_task
        async def failing():
            raise ValueError("boom")

        @flow.function_task
        async def consumer(x=None):
            return x

        with pytest.raises(DependencyFailureError) as exc_info:
            await asyncio.wait_for(consumer(x=failing()), timeout=5)
        assert isinstance(exc_info.value.root_cause, ValueError)

    @pytest.mark.asyncio
    async def test_cancelled_kwarg_dependency_keeps_engine_running(self, flow):
        """A cancelled keyword-argument dependency cancels the dependent and must not
        stop the run loop."""

        @flow.function_task
        async def slow():
            await asyncio.sleep(10)
            return "never"

        @flow.function_task
        async def consumer(x=None):
            return x

        @flow.function_task
        async def unrelated():
            return "ok"

        slow_fut = slow()
        slow_fut.cancel()

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(consumer(x=slow_fut), timeout=5)

        assert await asyncio.wait_for(unrelated(), timeout=5) == "ok"
        assert not flow._run_task.done()

    @pytest.mark.asyncio
    async def test_same_future_as_arg_and_kwarg(self, flow):
        """The same future as both arg and kwarg is one dependency."""

        @flow.function_task
        async def producer():
            await asyncio.sleep(0.3)  # still pending when consumer registers
            return 1

        @flow.function_task
        async def consumer(val, x=None):
            return val + x

        fut = producer()
        result = await asyncio.wait_for(consumer(fut, x=fut), timeout=5)
        assert result == 2

    @pytest.mark.asyncio
    async def test_kwarg_future_block_consumer(self, flow):
        """A block receiving a pending keyword-argument future gets its value."""

        @flow.function_task
        async def producer():
            await asyncio.sleep(0.3)  # still pending when block registers
            return 1

        @flow.block
        async def consumer_block(x=None):
            return x

        result = await asyncio.wait_for(consumer_block(x=producer()), timeout=5)
        assert result == 1

    @pytest.mark.asyncio
    async def test_extract_dependency_values_directly(self, flow):
        """Test the _extract_dependency_values method directly."""

        # Create some completed futures
        future1 = asyncio.Future()
        future1.set_result("value1")

        future2 = asyncio.Future()
        future2.set_result("value2")

        # Create component description with mixed args
        comp_desc = {
            "args": (future1, "regular_arg", future2),
            "kwargs": {"kwarg1": future1, "kwarg2": "regular_kwarg", "kwarg3": future2},
        }

        # Extract values
        resolved_args, resolved_kwargs = await flow._extract_dependency_values(
            comp_desc
        )

        # Verify resolution
        assert resolved_args == ("value1", "regular_arg", "value2")
        assert resolved_kwargs == {
            "kwarg1": "value1",
            "kwarg2": "regular_kwarg",
            "kwarg3": "value2",
        }

    @pytest.mark.asyncio
    async def test_cancelled_dependency_handling(self, flow):
        """Test handling of cancelled dependencies."""

        @flow.function_task
        async def slow_task():
            await asyncio.sleep(10)  # Long running task
            return "completed"

        @flow.function_task
        async def dependent_task(dep_value):
            return f"Got: {dep_value}"

        # Start tasks
        slow_future = slow_task()
        dependent_future = dependent_task(slow_future)

        # Cancel the dependency
        slow_future.cancel()

        # Wait a bit for cancellation to propagate
        await asyncio.sleep(0.1)

        # The dependent should also be cancelled or fail
        assert slow_future.cancelled()

        # Dependent task should handle the cancellation
        with pytest.raises((asyncio.CancelledError, Exception)):
            await dependent_future

    @pytest.mark.asyncio
    async def test_performance_no_regression(self, flow):
        """Test that future resolution doesn't significantly impact performance."""

        @flow.function_task
        async def producer(value):
            return value

        @flow.function_task
        async def consumer(val1, val2, val3):
            return val1 + val2 + val3

        # Measure time for workflow with future resolution
        start_time = time.time()

        # Create multiple producers
        p1 = producer(1)
        p2 = producer(2)
        p3 = producer(3)

        # Consumer depends on all producers
        result_future = consumer(p1, p2, p3)
        result = await result_future

        end_time = time.time()
        execution_time = end_time - start_time

        # Verify correctness
        assert result == 6

        # Verify reasonable performance (should complete quickly in dry-run mode)
        assert execution_time < 1.0, f"Execution took too long: {execution_time}s"


# Additional integration test
@pytest.mark.asyncio
async def test_real_workflow_scenario():
    """Integration test simulating a real workflow scenario."""

    backend = await LocalExecutionBackend()
    flow = await WorkflowEngine.create(backend=backend)

    try:

        @flow.function_task
        async def fetch_data(source):
            """Simulate data fetching."""
            await asyncio.sleep(0.01)  # Simulate I/O
            return f"data_from_{source}"

        @flow.function_task
        async def process_data(raw_data, config):
            """Simulate data processing."""
            await asyncio.sleep(0.01)  # Simulate processing
            return f"processed_{raw_data}_with_{config}"

        @flow.function_task
        async def combine_results(result1, result2):
            """Simulate combining results."""
            return f"combined_{result1}_and_{result2}"

        # Execute realistic workflow
        source1_data = fetch_data("api")
        source2_data = fetch_data("database")

        processed1 = process_data(source1_data, "config_a")
        processed2 = process_data(source2_data, "config_b")

        final_result = combine_results(processed1, processed2)

        # Verify final result
        result = await final_result
        expected = (
            "combined_processed_data_from_api_with_config_a_"
            "and_processed_data_from_database_with_config_b"
        )

        assert result == expected

    finally:
        await flow.shutdown()
