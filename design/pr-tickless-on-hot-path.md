<!--
Title: Scheduler: tickless run loop (stacked on #100)
Base:  feature/hot-path (#100 at 5ccf4a2, rebased onto main on 2026-10-29; #91 closed in favour of #110)
Head:  feature/tickless-on-hot-path (open as #110, at 437a10d)
Commits:
  c8747b3 Scheduler: event-driven run loop, tunable safety scan   (from #91)
  e00d552 Scheduler: tickless run loop, thread-safe backend callbacks
  ab6347d Scheduler: thread-safe service callbacks; release dependents on late reports
  11f23f6 Changelog: tickless run loop and thread-safe callbacks
  0a3061a Merge feature/hot-path into feature/tickless-on-hot-path
  0157751 Scheduler: test late service resolves; bound the scheduler tests' waits
  84d9a27 Merge feature/hot-path into feature/tickless-on-hot-path
          (the rewritten #100, 5ccf4a2, and main #109, #111, #112)
  437a10d Scheduler: release a task whose future was resolved during submission
          (Andre Merzky's review)
Rewritten 2026-10-31 for the new base: every number re-measured against #100 at 5ccf4a2.
-->

## Summary

Stacked on #100, which is now on `main`. It replaces #91, which was closed in
favour of this PR, so it also carries #91's event-driven loop. Dependents are
released when a component resolves, and the idle run loop sleeps until
something becomes ready.

```diff
 completion handler (success / failure / cancellation)
   resolve the future
+  _on_done(uid)                 # release dependents now; _kick() if one became ready

 run loop
-  every pass: scan the running set for resolved futures, release their dependents
-  something changed: sleep 10 ms, then pass again
-  nothing changed: wait for the change Event or shutdown (two Tasks), timeout 1 s
+  idle: await one future; _kick() resolves it, shutdown cancels it; no timeout, no scan

 task_callbacks(task, state, service_callback)
+  on another thread: call_soon_threadsafe(task_callbacks, ...) and return
-  service_callback(future, ...)
+  service_callback(_ThreadSafeFuture(future), ...)  # set_result / set_exception hop to the loop thread

 handler finds the future already resolved (e.g. set_result outside the engine)
   log a warning
+  _on_done(uid)                 # #100 left this to the loop's next scan
```

On #100, a dependent is released on the run loop's next pass: 10 ms later
while the loop is busy, and up to 1 s later when it waits idle, because a
completion resolves the future without setting the change Event the idle loop
waits on. A task that runs longer than one 10 ms pass, as every Dragon task
does, finds the loop waiting idle.

Behaviour changes:

- Backend callbacks made from other threads are handled on the loop thread.
  The monitor thread of rhapsody's Dragon backend reports Dragon-side
  cancellations this way.
- Cancelling a pending task whose own dependencies are still running releases
  its dependents at once. Before, they waited for those dependencies to finish.
- A running task's future resolved outside the engine releases its dependents
  when the backend reports the task. Before, the loop's next scan released
  them. One resolved while the backend's `submit_tasks` is still running
  releases them when submission returns, even if the backend never reports the
  task.
- If user code resolves the future of a task that still waits for its own
  dependencies, dependents added afterwards run at once. Those added before
  wait for the task's dependencies to finish, as all of them do on #100. Final
  states are the same. Releasing both at once would need a done callback on
  every task future, which this PR avoids because it adds an event-loop
  callback to every task.
- Service callbacks may resolve the service future from their own thread, as
  the RADICAL-Pilot backend does and the `task_callbacks` docstring describes.
  On #100 the engine noticed only at the loop's 1 s timeout.
- **Code that touches asyncio objects from other threads** without
  `call_soon_threadsafe` (for example `set_result` on a future, or
  `Event.set()`) was always incorrect. The 1 s timeout of #100's run loop (and
  of 0.6.0's) hid it as a delay. With this PR it can hang, and I think that is
  the right behavior. Run with `PYTHONASYNCIODEBUG=1` to find it: asyncio then
  raises "Non-thread-safe operation invoked on an event loop other than the
  current one" at the offending call. No periodic safety net is added: it
  would turn such hangs into silent delays again. A search of ROSE, IMPRESS and
  the backends of rhapsody-py 0.6.0 found no case other than the service one
  fixed here.

## Evidence

All numbers below compare #100 at 5ccf4a2 with this PR at 437a10d, measured on
2026-10-31.

**Tests.** The scheduler tests this PR adds
(`test_scheduler_completion_paths.py`, `test_scheduler_responsiveness.py`), run
on both engines; 10 of the 18 fail on #100:

| Test | #100 | This PR |
|---|---|---|
| `test_handled_on_loop_thread` | fail | pass |
| `test_chain_has_no_poll_delay` | fail | pass |
| `test_pending_task_cancelled_before_its_dependencies_finish` | fail | pass |
| `test_idle_loop_does_not_poll` | fail¹ | pass |
| `test_resolved_from_callback_thread[result, exception]` | fail | pass |
| `test_resolved_after_future_is_done[set_result, set_exception]` | fail² | pass |
| `test_dependent_starts_promptly_after_dependency` | fail | pass |
| `test_sequential_round_trip_latency` | fail | pass |
| `test_task_resolved_outside_engine[DONE, FAILED, CANCELED]` | pass | pass |
| `test_task_resolved_outside_engine_during_submit` | pass | pass |
| the other 4 | pass | pass |

¹ #100 has no wake-up future to check; its polling shows in the idle-CPU row
below. ² It tests `_ThreadSafeFuture`, which this PR adds.

```text
test_chain_has_no_poll_delay
  backend completes each 10 ms task from its own thread
  10-step chain
  expect: done in < 0.5 s       (#100: each edge waits for the loop's next pass)

test_resolved_from_callback_thread
  engine otherwise idle; service_callback resolves the future from a daemon thread after 20 ms
  expect: the dependent is released in < 1 s     (#100: nothing wakes the loop before its 1 s timeout)

test_task_resolved_outside_engine
  running task; future.set_result(...) outside the engine; backend then reports DONE / FAILED / CANCELED
  expect: the dependent is released within 0.5 s of the report

test_resolved_after_future_is_done
  future already cancelled (at shutdown, say); a thread then calls set_result / set_exception on it
  expect: the late call is dropped on the loop thread, not raised there as InvalidStateError
```

Full unit suite on macOS: 214 passed on this PR with Python 3.13 and 213 with
Python 3.9 (5 and 6 skipped), the same with `PYTHONASYNCIODEBUG=1`, and asyncio
reported no non-thread-safe call. #100's own suite: 196 passed on Python 3.13.
CI runs Linux, Python 3.9 to 3.13.

Before #100 was rebased, I removed each part of the change in turn, 13 in all:
the thread hop; the service-future wrapper and its already-done check; the new
dependency rule; `_on_done` on the local-cancel path and on the three
late-report paths; `_kick` on completion and on registration; the idle wait
without a timeout; and the two running-set updates. Each removal failed at
least one scheduler test, and none hung them: every wait in the scheduler tests
has a time limit. Those limits held with the test process pinned to one core
shared with three busy loops.

**Dragon.** rhapsody's `DragonExecutionBackend` on the Linux machine
(rhapsody-py 0.6.0, dragonhpc 0.14.1, Python 3.12), executable tasks, two runs
per engine. Latency runs from the backend's report of a task to the release of
its dependent.

| Scenario | #100 | This PR |
|---|---|---|
| Chain of 20 tasks, median edge | 563–564 ms | 0.9–1.0 ms |
| Chain of 20 tasks, wall time | 19.9 s | 9.1–9.8 s |
| 1 → 64 → 1, fan-out / fan-in | 464–665 / 245–250 ms | 1.0–1.7 / 1.0–1.4 ms |
| Running task cancelled on the Dragon side, reported by the monitor thread of rhapsody's Dragon backend | 406–708 ms | 0.1–0.2 ms |
| Task stopped by a Dragon timeout, reported through the event loop | 561–662 ms | 0.1–0.2 ms |
| Dependent of a 2 s task, nothing else running | 560–710 ms | 0.7–1.1 ms |

Every scenario finished on both engines, normally and with
`PYTHONASYNCIODEBUG=1` (one more run each), with no hang and no leftover
processes. rhapsody hands completions to the event loop with
`call_soon_threadsafe`; the future resolves, but #100's idle loop is not woken,
so each dependent waits for the loop's 1 s timeout. Process launches take about
0.5 s per task in the chain on this PR; #100 adds about 0.5 s per edge on top. The monitor thread of rhapsody's Dragon backend calls asyncflow's
`task_callbacks` directly to report a Dragon-side cancellation, so on #100
asyncflow's cancellation handler runs on that thread. Debug mode rejected none
of its calls in these runs, but the handler changes the engine's state off the
loop thread (`test_handled_on_loop_thread`). This PR moves the callback onto
the loop thread before any handler runs.

**Measurements.** AMD EPYC 7502P, Python 3.14, one pinned core unless noted.
Medians of 7 runs (thread and idle: 3; local: 5; `01-workflows.py`: 15).
K is the number of tasks parked in flight.

| | #100 | This PR |
|---|---|---|
| Sequential chain, per task, K=0 | 10.4 ms | 34.6 µs |
| Sequential chain, per task, K=10k | 12.8 ms | 40.1 µs |
| Sequential chain, CPU per task, K=0 / K=10k | 189 / 2,684 µs | 34.6 / 40.0 µs |
| 1,024 concurrent diamonds, K=0 | 41,150 tasks/s | 53,570 tasks/s |
| 1,024 concurrent diamonds, K=10k | 27,650 tasks/s | 41,380 tasks/s |
| 1,024 concurrent diamonds, K=0, garbage collection off | 43,780 tasks/s | 60,250 tasks/s |
| 8,192 concurrent diamonds, K=0 | 41,710 tasks/s | 43,270 tasks/s |
| 8,192 concurrent diamonds, K=10k | 35,770 tasks/s | 36,450 tasks/s |
| Dependency edge, 2 ms tasks completed from a backend thread | 19.9 ms | 2.23 ms |
| Idle CPU, K=1k / K=10k (% of a core) | 0.070 / 0.66 | 0.003 / 0.004 |
| `LocalExecutionBackend` round trip (4 cores) | 10.9 ms | 0.215 ms |
| `LocalExecutionBackend`, a 50 ms task and its dependent (4 cores) | 1.03 s | 0.051 s |
| `examples/01-workflows.py`, 4 cores: workflows / CPU | 0.993 s / 1.67 s | 0.989 s / 1.67 s |
| `examples/01-workflows.py`, 1 core: workflows / CPU | 0.640 s / 0.88 s | 0.625 s / 0.86 s |
| Service ready 0.2 s after start, set from a daemon thread (macOS, 3 runs) | 1.01 s | 0.21 s |

- The `01-workflows.py` workflow time runs from the first "Starting workflow"
  to the last "completed at" timestamp the example prints. Its workflows keep
  the loop busy, so #100's 10 ms passes cost little there.
- The chain and the local backend show #100's run-loop passes directly: about
  10 ms per step while busy, about 1 s when the loop is idle between a task and
  its dependent.
- Throughput with nothing parked is 4% higher on 8,192 diamonds and 30% higher
  on 1,024, where #100's 10 ms passes are a larger share of the run.

## Merge Danger

**Door:** two-way

Engine-internal. Reverting this PR's commits restores #100's run loop.

**Blast Radius:** scheduler

- **Every workflow** runs through the changed completion path and run loop.
- **Code touching asyncio from other threads:** a delay of up to 1 s becomes a
  hang. Such code was incorrect before too; see above for how to find it.
- **Futures resolved outside the engine:** dependents of a task whose future
  user code resolves are released when the backend reports the task, instead
  of at the loop's next scan.
- **Merge order:** after #100. Withdrawing this PR leaves #100 unchanged.

Generated with Matteo's hands and Claude Code
