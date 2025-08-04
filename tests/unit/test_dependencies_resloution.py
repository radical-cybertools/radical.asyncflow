import asyncio
import pytest
import time
import pytest_asyncio
from unittest.mock import Mock, AsyncMock
from radical.asyncflow import WorkflowEngine
from radical.asyncflow import ConcurrentExecutionBackend

from concurrent.futures import ThreadPoolExecutor

class TestFutureResolution:
    """Test suite for Future argument resolution in WorkflowEngine."""

    @pytest_asyncio.fixture
    async def flow(self):
        """Create a WorkflowEngine instance for testing."""
        backend = await ConcurrentExecutionBackend(ThreadPoolExecutor())
        async with await WorkflowEngine.create(backend=backend) as flow:
            yield flow

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
            assert not isinstance(val1, asyncio.Future), "val1 should be resolved value, not Future"
            assert not isinstance(val2, asyncio.Future), "val2 should be resolved value, not Future"
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
        async def consumer(future_arg, regular_arg, kwarg_future=None, kwarg_regular=None):
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
            future_result,                    # Future argument
            "regular_value",                  # Regular argument
            kwarg_future=kwarg_future,        # Future keyword argument
            kwarg_regular="kwarg_regular_value"  # Regular keyword argument
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
    async def test_extract_dependency_values_directly(self, flow):
        """Test the _extract_dependency_values method directly."""

        # Create some completed futures
        future1 = asyncio.Future()
        future1.set_result("value1")

        future2 = asyncio.Future()
        future2.set_result("value2")

        # Create component description with mixed args
        comp_desc = {
            'args': (future1, "regular_arg", future2),
            'kwargs': {
                'kwarg1': future1,
                'kwarg2': "regular_kwarg",
                'kwarg3': future2
            }
        }

        # Extract values
        resolved_args, resolved_kwargs = await flow._extract_dependency_values(comp_desc)

        # Verify resolution
        assert resolved_args == ("value1", "regular_arg", "value2")
        assert resolved_kwargs == {
            'kwarg1': "value1",
            'kwarg2': "regular_kwarg", 
            'kwarg3': "value2"
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

    backend = await ConcurrentExecutionBackend(ThreadPoolExecutor())
    async with await WorkflowEngine.create(backend=backend) as flow:
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
        expected = "combined_processed_data_from_api_with_config_a_and_processed_data_from_database_with_config_b"
        assert result == expected

