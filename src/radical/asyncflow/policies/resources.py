"""Resource-aware admission: a task starts when its resource demand fits.

:class:`ResourcePolicy` is a ready-queue policy that admits tasks by what they
need instead of by count. Each backend gets a :class:`Pool`: its nodes,
described by :class:`NodeShape`, and a ledger of what admitted tasks hold. A task
waits in the policy until its demand fits its pool, and gives the demand back
when the backend reports it done, failed or cancelled::

    node = NodeShape(cpus=64, gpus=4, host_mem=256, gpu_mem=40, name="gpu001")
    policy = ResourcePolicy(Pool([node], mode="node"), memory=MemoryModel())
    flow = await WorkflowEngine.create(backend=backend, policy=policy)

    @flow.function_task
    async def fold(seq, task_description={"resources": {"gpus": 0.25, "gpu_mem": 9}}):
        ...  # host memory is learned: declare it if no backend reports peaks

A task declares its demand under ``task_description["resources"]``: ``cpus``
(default 1), ``gpus`` (below 1, a share of one device; from 1, whole devices),
``host_mem`` and ``gpu_mem`` (GiB; ``gpu_mem`` is per device). Undeclared memory
counts as zero, unless the policy has a :class:`MemoryModel`, which learns it.

Modes:

- ``aggregate``: the demand must fit the pool's summed capacity. The backend
  still chooses the node, and the application the GPU.
- ``node``: the demand must fit one node, and a GPU share one GPU. The policy
  chooses them, worst fit first so load spreads, and writes the choice into the
  task description as ``placement``: ``{"node": index, "host": name, "gpus":
  [device, ...]}``. A backend that understands ``placement`` places the task
  there; others ignore it.

Memory is learned from the peaks the backend reports for each task, as
``observed`` in its description (``{"host_mem": ..., "gpu_mem": ...}``, GiB,
``gpu_mem`` per device); only tasks that finished count as samples. Until a kind
of task (function, backend and cpu/gpu shape) is learned, its tasks count against
a small per-node limit of tasks with unknown memory.

Order: tasks start by priority, then oldest first; a younger task starts first
only when it fits and the first does not. The first task that does not fit holds
a reservation: its demand is set aside on the node closest to fitting it among
those that can hold it (node mode) or on the pool (aggregate mode), and other
tasks start only in what is left. So a task that needs a whole node is not
passed over for ever by smaller ones, and smaller ones still use what the first
task cannot. A pool holds one reservation at a time.
"""

from __future__ import annotations

import heapq
import itertools
import logging
import math
from dataclasses import dataclass
from typing import Optional, Union

from .base import priority

logger = logging.getLogger(__name__)

# A GPU share is counted in slices of a device: 1/k of a device for k up to 16, and
# for 20, 25, 32, 50, 64 and 100, is a whole number of them, so k such shares fill it
GPU_SLICES = 14_414_400
MODES = ("aggregate", "node")
_EPS = 1e-9  # what the ledger's float sums may drift by


def _non_negative(name: str, value) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValueError(f"{name} must be a non-negative number, not {value!r}")
    return value


def _short(free: float, need: float) -> bool:
    """True if ``free`` cannot cover a positive ``need``."""
    return need > 0 and free + _EPS < need


@dataclass(frozen=True)
class NodeShape:
    """The resources of one node that tasks may use.

    ``host_mem`` and ``gpu_mem`` are in GiB, ``gpu_mem`` per GPU; None leaves
    that memory uncounted. ``cpu_overcommit`` scales the cores, for thread counts
    that are caps rather than reservations. ``name`` is the host name a
    placement reports.
    """

    cpus: float
    gpus: int = 0
    host_mem: Optional[float] = None
    gpu_mem: Optional[float] = None
    cpu_overcommit: float = 1.0
    name: Optional[str] = None

    def __post_init__(self):
        _non_negative("cpus", self.cpus)
        if isinstance(self.gpus, bool) or not isinstance(self.gpus, int):
            raise ValueError(f"gpus must be a whole number, not {self.gpus!r}")
        _non_negative("gpus", self.gpus)
        for dim in ("host_mem", "gpu_mem"):
            if getattr(self, dim) is not None:
                _non_negative(dim, getattr(self, dim))
        if _non_negative("cpu_overcommit", self.cpu_overcommit) == 0:
            raise ValueError("cpu_overcommit must be positive")


@dataclass(frozen=True)
class _Demand:
    kind: str  # what memory is learned for: function, backend, cpus and gpus
    cpus: float
    gpus: float
    host_mem: Optional[float]  # declared, GiB
    gpu_mem: Optional[float]  # declared, GiB per device

    @property
    def slices(self) -> int:
        if self.gpus <= 0:
            return 0
        if self.gpus < 1:
            return max(1, math.ceil(self.gpus * GPU_SLICES - 1e-6))
        return int(self.gpus) * GPU_SLICES

    @property
    def devices(self) -> int:
        return 0 if self.gpus <= 0 else max(1, int(self.gpus))


def _demand(desc: dict) -> _Demand:
    """Read ``task_description["resources"]``; one core when none is declared."""
    res = (desc.get("task_backend_specific_kwargs") or {}).get("resources") or {}
    if not isinstance(res, dict):
        raise ValueError(f"resources must be a dict, not {res!r}")
    unknown = set(res) - {"cpus", "gpus", "host_mem", "gpu_mem"}
    if unknown:
        raise ValueError(f"unknown resources {sorted(unknown)}")
    cpus = _non_negative("cpus", res.get("cpus", 1))
    gpus = _non_negative("gpus", res.get("gpus", 0))
    if gpus > 1 and gpus != int(gpus):
        raise ValueError(f"gpus above 1 must be whole devices, not {gpus!r}")
    host = res.get("host_mem")
    gpu_mem = res.get("gpu_mem")
    for dim, value in (("host_mem", host), ("gpu_mem", gpu_mem)):
        if value is not None:
            _non_negative(dim, value)
    backend = desc.get("_backend") or ""
    kind = f"{desc.get('name')}|{backend}|{float(cpus):g}|{float(gpus):g}"
    return _Demand(kind, cpus, gpus, host, gpu_mem)


class MemoryModel:
    """Peak memory per kind of task, learned from completed tasks.

    The estimate is the largest observed peak times ``headroom``, once a kind
    has ``min_samples`` observations. Admission must protect against the peak,
    hence the maximum and not an average.
    """

    def __init__(self, min_samples: int = 3, headroom: float = 1.1):
        if isinstance(min_samples, bool) or not isinstance(min_samples, int):
            raise ValueError(f"min_samples must be an int, not {min_samples!r}")
        if min_samples < 1:
            raise ValueError("min_samples must be >= 1")
        if _non_negative("headroom", headroom) < 1:
            raise ValueError("headroom must be >= 1")
        self.min_samples = min_samples
        self.headroom = headroom
        self._obs: dict = {}  # (kind, dim) -> [count, max]

    def observe(self, kind: str, dim: str, value, sample: bool = True) -> bool:
        """Record a peak; return True if this observation taught the kind.

        Every peak raises the estimate, but only a ``sample`` (a peak of a task
        that finished) counts towards ``min_samples``: a task that failed or was
        cancelled early may have peaked low. A value that is not a finite
        non-negative number is ignored.
        """
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value < 0
        ):
            return False
        obs = self._obs.setdefault((kind, dim), [0, 0.0])
        obs[1] = max(obs[1], float(value))
        if not sample:
            return False
        obs[0] += 1
        return obs[0] == self.min_samples

    def estimate(self, kind: str, dim: str) -> Optional[float]:
        obs = self._obs.get((kind, dim))
        if obs is None or obs[0] < self.min_samples:
            return None
        return obs[1] * self.headroom


class _Node:
    __slots__ = (
        "idx",
        "shape",
        "cpu",
        "host",
        "gpu_slices",
        "gpu_mem",
        "unknown",
        "tasks",
    )

    def __init__(self, idx: int, shape: NodeShape):
        self.idx = idx
        self.shape = shape
        self.cpu = shape.cpus * shape.cpu_overcommit
        self.host = math.inf if shape.host_mem is None else float(shape.host_mem)
        gpu_mem = math.inf if shape.gpu_mem is None else float(shape.gpu_mem)
        self.gpu_slices = [GPU_SLICES] * shape.gpus
        self.gpu_mem = [gpu_mem] * shape.gpus
        self.unknown = 0
        self.tasks = 0

    def hosts(self, d: _Demand) -> bool:
        """True if the node's shape has the cores and devices ``d`` needs."""
        return (
            d.cpus <= self.shape.cpus * self.shape.cpu_overcommit + _EPS
            and d.devices <= self.shape.gpus
        )

    def can_hold(self, d: _Demand, host: float, gpu_mem: float) -> bool:
        """True if the demand fits the node when nothing else runs on it."""
        shape_host = math.inf if self.shape.host_mem is None else self.shape.host_mem
        shape_gpu = math.inf if self.shape.gpu_mem is None else self.shape.gpu_mem
        return (
            self.hosts(d)
            and not _short(shape_host, host)
            and not (d.slices and _short(shape_gpu, gpu_mem))
        )

    def host_free_fraction(self) -> float:
        if self.shape.host_mem is None or self.shape.host_mem == 0:
            return 1.0
        return self.host / self.shape.host_mem


@dataclass(frozen=True)
class _Charge:
    """What an admitted task holds in its pool's ledger."""

    node: Optional[int]
    gpus: tuple  # the devices, in node mode
    devices: int
    cpus: float
    slices: int
    host: float
    gpu_mem: float  # per device
    unknown: bool


class Pool:
    """The capacity of one backend, and what admitted tasks hold of it.

    Args:
        nodes: The shape of each node the backend runs tasks on.
        mode: ``"aggregate"`` or ``"node"`` (see the module documentation).
        unknown_per_node: With a :class:`MemoryModel`, how many tasks of kinds
            whose memory is not yet learned may run per node (at least 1).
    """

    def __init__(self, nodes: list, mode: str = "aggregate", unknown_per_node: int = 2):
        if mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}, not {mode!r}")
        nodes = [nodes] if isinstance(nodes, NodeShape) else list(nodes)
        if not nodes or not all(isinstance(n, NodeShape) for n in nodes):
            raise ValueError("nodes must be a non-empty list of NodeShape")
        names = [n.name for n in nodes if n.name is not None]
        if mode == "node" and len(set(names)) < len(names):
            raise ValueError(f"node names must differ, not {names!r}")
        if (
            isinstance(unknown_per_node, bool)
            or not isinstance(unknown_per_node, int)
            or unknown_per_node < 1
        ):
            raise ValueError(
                f"unknown_per_node must be a positive int, not {unknown_per_node!r}"
            )
        self.mode = mode
        self.unknown_per_node = unknown_per_node
        self.nodes = [_Node(i, s) for i, s in enumerate(nodes)]
        # summed ledger: always kept; "aggregate" mode admits against it
        self.cpu = sum(n.cpu for n in self.nodes)
        self.host = sum(n.host for n in self.nodes)
        self.slices = sum(sum(n.gpu_slices) for n in self.nodes)
        self.gpu_mem = sum(sum(n.gpu_mem) for n in self.nodes)
        self.unknown = 0
        self._total = (self.cpu, self.host, self.slices, self.gpu_mem)
        # a GPU gives no task more than its own memory, in either mode
        self._device_mem = (
            max(
                (math.inf if n.shape.gpu_mem is None else n.shape.gpu_mem)
                for n in self.nodes
                if n.shape.gpus
            )
            if self.slices
            else 0.0
        )

    def fits_ever(self, d: _Demand, host: float, gpu_mem: float) -> bool:
        """Return True if the demand fits the pool when nothing else runs."""
        if self.mode == "aggregate":
            cpu, host_total, slices, gpu_mem_total = self._total
            return not (
                _short(cpu, d.cpus)
                or _short(host_total, host)
                or d.slices > slices
                or _short(gpu_mem_total, gpu_mem * d.devices)
                or (d.slices and _short(self._device_mem, gpu_mem))
            )
        return any(n.can_hold(d, host, gpu_mem) for n in self.nodes)

    def clamp(
        self, d: _Demand, host: float, gpu_mem: float, free_host: bool, free_gpu: bool
    ) -> tuple:
        """Lowers learned memory (``free_*``) to what one task can get, if need be.

        A learned peak above that would never fit, so the task runs alone instead.
        Declared memory is never changed.
        """
        if not (free_host or free_gpu) or self.fits_ever(d, host, gpu_mem):
            return host, gpu_mem
        if self.mode == "aggregate":
            cpu, host_total, slices, gpu_total = self._total
            if free_host:
                host = min(host, host_total)
            if free_gpu and d.devices:
                gpu_mem = min(gpu_mem, gpu_total / d.devices, self._device_mem)
            return host, gpu_mem
        best = None  # (fraction of the learned memory kept, host, gpu_mem)
        for n in self.nodes:
            shape_host = math.inf if n.shape.host_mem is None else n.shape.host_mem
            shape_gpu = math.inf if n.shape.gpu_mem is None else n.shape.gpu_mem
            if not n.hosts(d) or (not free_host and _short(shape_host, host)):
                continue
            if d.slices and not free_gpu and _short(shape_gpu, gpu_mem):
                continue
            h = min(host, shape_host) if free_host else host
            g = min(gpu_mem, shape_gpu) if free_gpu and d.slices else gpu_mem
            kept = min(h / host if host else 1.0, g / gpu_mem if gpu_mem else 1.0)
            if best is None or kept > best[0]:
                best = (kept, h, g)
        return (best[1], best[2]) if best else (host, gpu_mem)

    def _gpus(self, n: _Node, d: _Demand, gpu_mem: float) -> tuple:
        """The devices of node n for ``d`` now, most free first; () if too few fit."""
        if not d.slices:
            return ()
        per_dev = min(d.slices, GPU_SLICES)
        cands = [
            g
            for g in range(len(n.gpu_slices))
            if n.gpu_slices[g] >= per_dev and not _short(n.gpu_mem[g], gpu_mem)
        ]
        if len(cands) < d.devices:
            return ()
        cands.sort(key=lambda g: (-n.gpu_slices[g], -n.gpu_mem[g], g))
        return tuple(cands[: d.devices])

    def fit(self, d: _Demand, host: float, gpu_mem: float, unknown: bool):
        """Return the charge the demand would take now, or None."""
        if self.mode == "aggregate":
            if unknown and self.unknown >= self.unknown_per_node * len(self.nodes):
                return None
            if (
                _short(self.cpu, d.cpus)
                or _short(self.host, host)
                or self.slices < d.slices
                or _short(self.gpu_mem, gpu_mem * d.devices)
            ):
                return None
            return _Charge(
                None, (), d.devices, d.cpus, d.slices, host, gpu_mem, unknown
            )
        # node mode: worst fit, so load and memory spread over nodes and GPUs
        best, best_key = None, None
        for n in self.nodes:
            if unknown and n.unknown >= self.unknown_per_node:
                continue
            if _short(n.cpu, d.cpus) or _short(n.host, host):
                continue
            gpus = self._gpus(n, d, gpu_mem)
            if d.slices and not gpus:
                continue
            key = self._spread(n, gpus)
            if best_key is None or key > best_key:
                best, best_key = (n, gpus), key
        if best is None:
            return None
        n, gpus = best
        return _Charge(n.idx, gpus, d.devices, d.cpus, d.slices, host, gpu_mem, unknown)

    @staticmethod
    def _spread(n: _Node, gpus: tuple) -> tuple:
        """How empty node n and its devices are: the larger, the emptier."""
        return (
            min(n.gpu_slices[g] for g in gpus) if gpus else 0,
            n.host_free_fraction(),
            n.cpu,
            -n.idx,
        )

    def reserve(self, d: _Demand, host: float, gpu_mem: float, unknown: bool):
        """The charge to set aside for a demand that does not fit now.

        In node mode, on the node that is closest to fitting it among those that can
        hold it, and on that node's devices that are closest to fitting; among nodes as
        close, on the one running most tasks, as it is likely to free up soonest. In
        aggregate mode, on the pool.
        """
        if self.mode == "aggregate":
            return _Charge(
                None, (), d.devices, d.cpus, d.slices, host, gpu_mem, unknown
            )
        best, best_key = None, None
        for n in self.nodes:
            if not n.can_hold(d, host, gpu_mem):
                continue
            gpus = self._closest_gpus(n, d, gpu_mem)
            key = (self._shortfall(n, d, host, gpu_mem, gpus), -n.tasks, n.idx)
            if best_key is None or key < best_key:
                best, best_key = (n, gpus), key
        n, gpus = best
        return _Charge(n.idx, gpus, d.devices, d.cpus, d.slices, host, gpu_mem, unknown)

    @staticmethod
    def _device_shortfall(n: _Node, g: int, per_dev: int, gpu_mem: float) -> float:
        short = (per_dev - n.gpu_slices[g]) / GPU_SLICES
        if gpu_mem and n.shape.gpu_mem:
            short = max(short, (gpu_mem - n.gpu_mem[g]) / n.shape.gpu_mem)
        return short

    def _closest_gpus(self, n: _Node, d: _Demand, gpu_mem: float) -> tuple:
        if not d.slices:
            return ()
        per_dev = min(d.slices, GPU_SLICES)
        devices = sorted(
            range(len(n.gpu_slices)),
            key=lambda g: (self._device_shortfall(n, g, per_dev, gpu_mem), g),
        )
        return tuple(devices[: d.devices])

    def _shortfall(
        self, n: _Node, d: _Demand, host: float, gpu_mem: float, gpus: tuple
    ) -> float:
        """The largest share of a resource of node n that ``d`` misses now."""
        short = 0.0
        capacity = n.shape.cpus * n.shape.cpu_overcommit
        if d.cpus > 0 and capacity:
            short = max(short, (d.cpus - n.cpu) / capacity)
        if host > 0 and n.shape.host_mem:
            short = max(short, (host - n.host) / n.shape.host_mem)
        per_dev = min(d.slices, GPU_SLICES)
        for g in gpus:
            short = max(short, self._device_shortfall(n, g, per_dev, gpu_mem))
        return short

    def apply(self, c: _Charge, sign: int) -> None:
        """Take (sign 1) or give back (sign -1) a charge."""
        per_dev = min(c.slices, GPU_SLICES)
        self.cpu -= sign * c.cpus
        self.host -= sign * c.host
        self.slices -= sign * c.slices
        self.gpu_mem -= sign * c.gpu_mem * c.devices
        self.unknown += sign * c.unknown
        if c.node is None:
            return
        n = self.nodes[c.node]
        n.cpu -= sign * c.cpus
        n.host -= sign * c.host
        n.unknown += sign * c.unknown
        n.tasks += sign
        for g in c.gpus:
            n.gpu_slices[g] -= sign * per_dev
            n.gpu_mem[g] -= sign * c.gpu_mem

    def placement(self, c: _Charge) -> Optional[dict]:
        if c.node is None:
            return None
        return {
            "node": c.node,
            "host": self.nodes[c.node].shape.name,
            "gpus": list(c.gpus),
        }

    def state(self) -> dict:
        """Free capacity, summed and per node, for logs and debugging."""
        return {
            "mode": self.mode,
            "cpu_free": self.cpu,
            "host_free": self.host,
            "gpu_slices_free": self.slices,
            "gpu_mem_free": self.gpu_mem,
            "unknown": self.unknown,
            "nodes": [
                {
                    "node": n.idx,
                    "host": n.shape.name,
                    "cpu_free": n.cpu,
                    "host_free": n.host,
                    "gpu_slices_free": list(n.gpu_slices),
                    "gpu_mem_free": list(n.gpu_mem),
                    "unknown": n.unknown,
                }
                for n in self.nodes
            ],
        }


@dataclass
class _Waiting:
    uid: str
    desc: dict
    demand: _Demand
    rank: tuple  # (-priority, arrival): smaller starts first
    pid: int
    queue: tuple  # the key of the queue it waits in


def _queue_key(d: _Demand) -> tuple:
    """Tasks of a kind wait in one queue per power of two of declared memory: only
    the first of a queue is tried, so a task may wait behind one of its kind that
    declares up to twice its memory, and the queues stay few."""

    def bucket(mem):
        if mem is None:
            return None
        return math.frexp(mem)[1] if mem > 0 else -math.inf  # none: its own queue

    return (d.kind, bucket(d.host_mem), bucket(d.gpu_mem))


class ResourcePolicy:
    """Admit ready tasks when their resource demand fits their backend's pool.

    Tasks start by priority, then oldest first. The first task that does not fit
    holds a reservation, so smaller tasks do not pass it for ever (see the module
    documentation).

    Args:
        pools: A :class:`Pool` for every task, or ``{backend name: Pool}``, the
            default backend included. Tasks for a backend without a pool are not
            held back.
        memory: A :class:`MemoryModel` that learns undeclared memory from the
            peaks backends report. Without one, undeclared memory counts as zero.
    """

    def __init__(
        self,
        pools: Union[Pool, dict],
        *,
        memory: Optional[MemoryModel] = None,
    ):
        if isinstance(pools, Pool):
            self._single, self.pools = pools, {}
        elif (
            isinstance(pools, dict)
            and pools
            and all(isinstance(p, Pool) for p in pools.values())
        ):
            self._single, self.pools = None, dict(pools)
        else:
            raise ValueError("pools must be a Pool or a non-empty dict of Pools")
        if memory is not None and not isinstance(memory, MemoryModel):
            raise ValueError(f"memory must be a MemoryModel, not {memory!r}")
        self.memory = memory
        every = (
            [self._single] if self._single is not None else list(self.pools.values())
        )
        if len({id(p) for p in every}) < len(every):
            raise ValueError("each backend needs a Pool of its own")
        self._by_id = {id(p): p for p in every}
        # pool -> demand -> heap of (rank, uid); entries of dropped uids stay
        self._queues: dict = {pid: {} for pid in self._by_id}
        self._count = {pid: 0 for pid in self._by_id}  # queued tasks per pool
        self._blocked: set = set()  # pools where nothing fit since the last change
        self._reserved: dict = {}  # pool -> (uid, charge) set aside for it
        self._free: dict = {}  # uid -> desc of tasks for a backend without a pool
        self._waiting: dict = {}  # uid -> _Waiting, for each queued task
        self._inflight: dict = {}  # uid -> (pool, demand, charge)
        self._arrival = itertools.count()
        self._unreported: set = set()  # kinds warned about: no peaks reported

    # -- demand after learning --------------------------------------------------

    def _pool_for(self, desc: dict) -> Optional[Pool]:
        if self._single is not None:
            return self._single
        return self.pools.get(desc.get("_backend"))

    def _needs(self, pool: Pool, d: _Demand) -> tuple:
        """The memory to charge for ``d`` (host, per GPU), and whether it is unknown.

        Declared memory, else learned memory capped at what the task can get, else zero;
        unknown when a MemoryModel has not learned memory the pool counts.
        """
        host, gpu_mem = d.host_mem, d.gpu_mem if d.slices else 0.0
        learned_host = learned_gpu = unknown = False
        if self.memory is not None:
            if host is None and _counts(pool, "host_mem"):
                host = self.memory.estimate(d.kind, "host")
                learned_host, unknown = host is not None, host is None
            if gpu_mem is None and _counts(pool, "gpu_mem"):
                gpu_mem = self.memory.estimate(d.kind, "gpu")
                learned_gpu = gpu_mem is not None
                unknown = unknown or gpu_mem is None
        host, gpu_mem = pool.clamp(
            d, host or 0.0, gpu_mem or 0.0, learned_host, learned_gpu
        )
        return host, gpu_mem, unknown

    # -- policy operations -------------------------------------------------------

    def enqueue(self, uid: str, desc: dict) -> None:
        rank = (-priority(desc), next(self._arrival))
        pool = self._pool_for(desc)
        if pool is None:
            self._free[uid] = desc
            return
        d = _demand(desc)
        host, gpu_mem, _ = self._needs(pool, d)
        if not pool.fits_ever(d, host, gpu_mem):
            where = "any node" if pool.mode == "node" else "the pool"
            raise ValueError(
                f"{desc.get('name')}: {d.cpus} cpus, {d.gpus} gpus, {host} GiB host"
                f" and {gpu_mem} GiB per gpu can never fit {where}"
            )
        # Taken out, so a backend does not read it as its own setting (Dask does)
        (desc.get("task_backend_specific_kwargs") or {}).pop("resources", None)
        pid = id(pool)
        key = _queue_key(d)
        heapq.heappush(self._queues[pid].setdefault(key, []), (rank, uid))
        self._waiting[uid] = _Waiting(uid, desc, d, rank, pid, key)
        self._count[pid] += 1
        self._blocked.discard(pid)

    def runnable(self) -> bool:
        return bool(self._free) or any(
            n and pid not in self._blocked for pid, n in self._count.items()
        )

    def dispatch(self) -> list:
        out = list(self._free.values())
        self._free.clear()
        for pid, n in self._count.items():
            if n and pid not in self._blocked:
                out.extend(self._admit(self._by_id[pid]))
        return out

    def _heads(self, pid: int) -> list:
        """The first live task of each demand in pool ``pid``, first to start first."""
        heads = []
        queues = self._queues[pid]
        for key in list(queues):
            q = queues[key]
            while q and q[0][1] not in self._waiting:
                heapq.heappop(q)  # dropped while queued
            if q:
                heads.append(self._waiting[q[0][1]])
            else:
                del queues[key]
        heads.sort(key=lambda w: w.rank)
        return heads

    def _admit(self, pool: Pool) -> list:
        pid = id(pool)
        out = []
        while self._count[pid]:
            heads = self._heads(pid)
            first = heads[0]
            chosen = first
            charge = pool.fit(first.demand, *self._needs(pool, first.demand))
            if charge is None:
                # the first task holds a reservation; others start in what is left
                hold = self._hold(pool, first)
                pool.apply(hold, +1)
                try:
                    for w in heads[1:]:
                        charge = pool.fit(w.demand, *self._needs(pool, w.demand))
                        if charge is not None:
                            chosen = w
                            break
                finally:
                    pool.apply(hold, -1)
            if charge is None:
                self._blocked.add(pid)
                break
            heapq.heappop(self._queues[pid][chosen.queue])
            del self._waiting[chosen.uid]
            self._count[pid] -= 1
            if self._reserved.get(pid, (None,))[0] == chosen.uid:
                del self._reserved[pid]
            pool.apply(charge, +1)
            self._inflight[chosen.uid] = (pool, chosen.demand, charge)
            placement = pool.placement(charge)
            if placement is not None:
                chosen.desc["placement"] = placement
            out.append(chosen.desc)
        if not self._count[pid]:
            self._queues[pid].clear()  # only entries of dropped tasks are left
        return out

    def _hold(self, pool: Pool, w: _Waiting) -> _Charge:
        """The reservation of task w: what it needs now, on the node and devices
        chosen when it first had to wait, while that node can still hold it."""
        pid = id(pool)
        d = w.demand
        host, gpu_mem, unknown = self._needs(pool, d)
        held = self._reserved.get(pid)
        if (
            held is None
            or held[0] != w.uid
            or (
                held[1] is not None
                and not pool.nodes[held[1]].can_hold(d, host, gpu_mem)
            )
        ):
            c = pool.reserve(d, host, gpu_mem, unknown)
            held = self._reserved[pid] = (w.uid, c.node, c.gpus)
        _, node, gpus = held
        return _Charge(node, gpus, d.devices, d.cpus, d.slices, host, gpu_mem, unknown)

    def release(
        self, uid: str, state: Optional[str], observed: Optional[dict] = None
    ) -> None:
        if self._free.pop(uid, None) is not None:  # no pool: dropped while queued
            return
        w = self._waiting.pop(uid, None)
        if w is not None:  # dropped while queued; its heap entry is skipped
            self._count[w.pid] -= 1
            self._blocked.discard(w.pid)
            if self._reserved.get(w.pid, (None,))[0] == uid:
                del self._reserved[w.pid]
            if not self._count[w.pid]:
                self._queues[w.pid].clear()
            return
        rec = self._inflight.pop(uid, None)
        if rec is None:
            return
        pool, d, charge = rec
        pool.apply(charge, -1)
        self._blocked.discard(id(pool))
        if state is not None and self.memory is not None:
            # every reported peak raises the estimate; finished tasks are samples
            peaks = observed if isinstance(observed, dict) else {}
            done = state == "DONE"
            learned = self.memory.observe(d.kind, "host", peaks.get("host_mem"), done)
            learned |= self.memory.observe(d.kind, "gpu", peaks.get("gpu_mem"), done)
            if learned:
                self._recharge(d.kind)
            elif done and charge.unknown and d.kind not in self._unreported:
                missing = (
                    d.host_mem is None
                    and _counts(pool, "host_mem")
                    and peaks.get("host_mem") is None
                ) or (
                    d.slices
                    and d.gpu_mem is None
                    and _counts(pool, "gpu_mem")
                    and peaks.get("gpu_mem") is None
                )
                if not missing:
                    return
                self._unreported.add(d.kind)
                logger.warning(
                    "%s: the backend reported no peak memory, so with other tasks of "
                    "unknown memory at most %d run per node until it does; declare "
                    "host_mem and gpu_mem "
                    "in task_description['resources'] to lift the limit",
                    d.kind.split("|", 1)[0],
                    pool.unknown_per_node,
                )

    def _recharge(self, kind: str) -> None:
        """Charge the running tasks of a newly learned kind with the estimate."""
        for uid, (pool, d, c) in list(self._inflight.items()):
            if d.kind != kind or not c.unknown:
                continue
            host, gpu_mem, unknown = self._needs(pool, d)
            new = _Charge(
                c.node, c.gpus, c.devices, c.cpus, c.slices, host, gpu_mem, unknown
            )
            pool.apply(c, -1)
            pool.apply(new, +1)
            self._inflight[uid] = (pool, d, new)

    def state(self) -> dict:
        """Waiting and running tasks and free capacity, per pool."""
        names = {id(p): name for name, p in self.pools.items()}
        out = {}
        for pid, pool in self._by_id.items():
            st = pool.state()
            st["waiting"] = self._count[pid]
            st["running"] = sum(1 for p, _, _ in self._inflight.values() if p is pool)
            reserved = self._reserved.get(pid)
            st["reserved_for"] = reserved[0] if reserved else None
            out[names.get(pid)] = st
        return out


def _counts(pool: Pool, dim: str) -> bool:
    """True if some node of the pool counts this memory."""
    return any(getattr(n.shape, dim) is not None for n in pool.nodes)
