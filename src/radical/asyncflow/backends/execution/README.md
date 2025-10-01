# Dragon Execution Backend - Node Placement & Policy Guide

## Node-Aware Worker Configuration

### Basic Syntax

```python
from dragon.infrastructure.policy import Policy

resources = {
    "workers": [
        {
            "nprocs": int,              # Number of processes in this worker group
            "count": int,               # Number of worker groups with this config
            "nodes": [int, ...],        # Optional: explicit node IDs
            "policy": Policy            # Optional: custom Dragon Policy object
        }
    ]
}
```

## Usage Examples

### 1. Explicit Node Placement

Assign specific nodes to worker groups:

```python
# 4 nodes (512 cores): 2 workers, each gets 256 cores on 2 nodes
resources = {
    "workers": [
        {"nprocs": 256, "count": 1, "nodes": [0, 1]},  # Worker 0 → nodes 0-1
        {"nprocs": 256, "count": 1, "nodes": [2, 3]},  # Worker 1 → nodes 2-3
    ]
}
```

**Result**: 
- 2 ProcessGroups created
- ProcessGroup 0: 256 processes on nodes 0-1
- ProcessGroup 1: 256 processes on nodes 2-3

### 2. Custom Dragon Policy

Use Dragon's Policy API for advanced placement control:

```python
from dragon.infrastructure.policy import Policy

# Block distribution (contiguous placement)
policy = Policy()
policy.distribution = Policy.Distribution.BLOCK
policy.placement = Policy.Placement.DEFAULT

resources = {
    "workers": [
        {"nprocs": 128, "count": 4, "policy": policy}
    ]
}
```

**Available Distributions**:
- `Policy.Distribution.ROUNDROBIN` - Distribute processes round-robin across nodes
- `Policy.Distribution.BLOCK` - Place processes contiguously
- `Policy.Distribution.DEFAULT` - Use Dragon's default strategy

**Available Placements**:
- `Policy.Placement.DEFAULT` - Dragon chooses placement
- `Policy.Placement.HOST_NAME` - Place by hostname
- `Policy.Placement.HOST_ID` - Place by host ID

### 3. Automatic Placement (Default)

No explicit nodes or policy - Dragon distributes automatically:

```python
resources = {
    "workers": [
        {"nprocs": 64, "count": 8}  # 512 workers, auto-distributed
    ]
}
```

Backend creates a round-robin policy automatically.

## Mixed Configuration Example

Combine different placement strategies:

```python
from dragon.infrastructure.policy import Policy

# Compute-intensive policy
compute_policy = Policy()
compute_policy.distribution = Policy.Distribution.BLOCK

resources = {
    "workers": [
        # I/O workers on node 0
        {"nprocs": 32, "count": 1, "nodes": [0]},
        
        # Compute workers with block distribution
        {"nprocs": 64, "count": 2, "policy": compute_policy},
        
        # Memory-intensive workers on nodes 2-3
        {"nprocs": 128, "count": 1, "nodes": [2, 3]},
    ]
}
```

Total: 32 + 128 + 128 = 288 slots

## Priority Order

When both `nodes` and `policy` are specified:

1. **Custom `policy`** takes precedence (used as-is)
2. **`nodes` list** creates a policy with `HOST_NAME` placement on first node
3. **Neither** defaults to round-robin distribution

## Environment Variables

Each worker process receives:
- `DRAGON_WORKER_ID` - Unique worker identifier
- `DRAGON_RANK` - Task rank (0 to ranks-1)
- `DRAGON_NODE_HINT` - Suggested node ID (when using `nodes` parameter)

## Complete Example

```python
from dragon.infrastructure.policy import Policy

# Custom policy
policy = Policy()
policy.distribution = Policy.Distribution.ROUNDROBIN

# Configure backend
resources = {
    "workers": [
        {"nprocs": 256, "count": 1, "nodes": [0, 1]},      # Explicit nodes
        {"nprocs": 128, "count": 2, "policy": policy},     # Custom policy
        {"nprocs": 64, "count": 4},                        # Auto placement
    ],
    "working_dir": "/scratch/work",
    "reference_threshold": 1048576  # 1MB
}

# Initialize
backend = await DragonExecutionBackend(resources)
```

## Reference

Full Dragon Policy documentation:  
https://dragonhpc.github.io/dragon/doc/_build/html/ref/dragon.infrastructure.policy.Policy.html