# AsyncFlow — Installation Guide

AsyncFlow is a Python library built on top of `asyncio`, designed for orchestrating complex asynchronous workflows easily and reliably.
This guide will help you install AsyncFlow in a clean Python environment.

---

## Prerequisites

* Python ≥ **3.9** (recommended: 3.11 or newer)
* `pip` ≥ 22.0
* Optional: `conda` ≥ 4.10 for Conda environments

Make sure Python is installed on your system:

```bash
python --version
pip --version
```

---

## Recommended: Create a Clean Environment

It is **strongly recommended** to install AsyncFlow in an isolated environment (Conda or `venv`) to avoid conflicts with system packages.

---

## Install with **Conda**

### Create and activate a Conda environment:

```bash
conda create -n asyncflow python=3.11 -y
conda activate asyncflow
```

### Install AsyncFlow:

```bash
pip install radical-asyncflow
```

### Verify installation:

```bash
python -c "import radical.asyncflow; print('AsyncFlow installed ✅')"
```

---

## Install with **venv** (built-in)

If you don't use Conda, you can use Python's built-in `venv` module.

### Create and activate a virtual environment:

```bash
python -m venv ~/.venvs/asyncflow
source ~/.venvs/asyncflow/bin/activate
```

(For Windows: `~/.venvs/asyncflow/Scripts/activate`)

### Install AsyncFlow:

```bash
pip install radical-asyncflow
```

### Verify installation:

```bash
python -c "import radical.asyncflow; print('AsyncFlow installed ✅')"
```

---

## Install RHAPSODY for HPC Execution (optional)

To run workflows on HPC clusters, supercomputers, or with Dask, install [RHAPSODY](https://github.com/radical-cybertools/rhapsody):

```bash
pip install rhapsody
```

RHAPSODY provides HPC execution backends (`RadicalExecutionBackend`, `DaskExecutionBackend`, `DragonExecutionBackendV3`, etc.) that integrate seamlessly with AsyncFlow.

---

## Development Installation (optional)

If you want to contribute to AsyncFlow or use the latest code from GitHub:

```bash
git clone https://github.com/radical-cybertools/radical.asyncflow.git
cd radical.asyncflow
pip install -e .[dev,lint,doc]
```

The `-e` flag installs it in *editable mode* — any local changes you make to the code are reflected immediately.

---

!!! tip
  * Always activate your virtual environment before using AsyncFlow.
  * To deactivate an environment:

    * `conda deactivate` (Conda)
    * `deactivate` (`venv`)
  * You can list installed packages with `pip list`.
  * It is a good practice to upgrade `pip` and `setuptools`:

  ```bash
  pip install --upgrade pip setuptools
  ```

---

## 🚀 Next Steps

* [Getting Started with AsyncFlow →](basic.md)
  Learn how to write your first workflow with AsyncFlow!
