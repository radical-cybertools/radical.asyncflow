# AsyncFlow Examples CI System

This directory contains the CI/CD configuration for automatically testing AsyncFlow examples across different Python versions and backend configurations.

## Overview

The CI system provides:

- **Automated Testing**: Examples are tested on every push/PR with manual dispatch option
- **Multi-Backend Support**: Tests examples with different execution backends (concurrent, dask, radical_pilot)
- **Configuration-Driven**: Uses `examples-config.yml` to specify per-example settings
- **Comprehensive Validation**: Includes syntax checking, dependency management, and output validation

## Configuration Format

The `examples-config.yml` file defines how each example should be tested:

```yaml
examples:
  example-name:
    script: "path/to/example.py"          # Path to example script
    backend: "concurrent"                 # Backend to use: noop, concurrent, dask, radical.pilot
    timeout_sec: 120                      # Timeout in seconds
    dependencies:                         # Additional pip packages
      - "numpy>=1.20"
      - "matplotlib"
    min_output_lines: 5                   # Minimum expected output lines
    skip_python:                          # Python versions to skip
      - "3.8"
```

### Configuration Options

- **`script`**: Path to the example Python file (defaults to `examples/{example-name}.py`)
- **`backend`**: Execution backend to use (default: `concurrent`)
- **`timeout_sec`**: Maximum execution time in seconds (default: 120)
- **`dependencies`**: List of additional pip packages to install
- **`min_output_lines`**: Minimum number of output lines expected (default: 1)
- **`skip_python`**: List of Python versions to skip for this example

## Workflows

### Examples CI (`examples.yml`)

Triggered on:

- Push to main/master/develop branches (when relevant files change)
- Pull requests (when relevant files change)
- Manual workflow dispatch

**What it tests:**

- Only examples affected by code changes
- Multiple Python versions (3.9, 3.11, 3.12)
- Fast feedback for development
- When manually dispatched, can test all examples with custom Python versions

## GitHub Action (`run-example`)

The reusable action handles:

1. **Environment Setup**: Python installation and caching
2. **Dependency Management**: Core package + example-specific + backend dependencies
3. **Configuration Loading**: Parsing YAML config and applying example settings
4. **Execution**: Running examples with proper timeout and error handling
5. **Validation**: Checking output for errors and minimum content requirements
6. **Artifact Collection**: Saving outputs for debugging failures

## Local Testing

You can validate the configuration locally:

```bash
# Install dependencies
pip install pyyaml

# Run validation script
python .github/bin/validate_examples.py
```

This script checks:

- Configuration file syntax and structure
- Example script existence and syntax
- Basic configuration validation

## Adding New Examples

1. **Create the example file** in the `examples/` directory
2. **Add configuration** to `examples-config.yml`:
   ```yaml
   examples:
     my-new-example:
       script: "examples/my-new-example.py"
       backend: "concurrent"
       timeout_sec: 60
       dependencies:
         - "requests"
   ```
3. **Test locally** using the validation script
4. **Commit changes** - CI will automatically test the new example

## Backend-Specific Configuration

### Concurrent Backend

- **Description**: Built-in Python `concurrent.futures` backend
- **Dependencies**: None (included with Python)
- **Use Case**: General examples, I/O-bound tasks

### Dask Backend

- **Description**: Distributed computing backend
- **Dependencies**: `dask[complete]`
- **Use Case**: CPU-intensive parallel tasks

### RADICAL-Pilot Backend

- **Description**: HPC-focused execution backend
- **Dependencies**: `radical.pilot`
- **Use Case**: HPC environments, large-scale computing

### NoOp Backend

- **Description**: No-operation backend for testing
- **Dependencies**: None
- **Use Case**: Testing workflow logic without execution
