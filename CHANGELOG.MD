
Jul 7, 2025
By Javier D. Segura

## /
- Added `ruff.toml`. Contains linter configuration
- Extended requirements for CI to reflect CI readjustment. 

## /src/radical/asyncflow
- Updated workfow_manager to serve as a context manager both as async and sync

## /tests/
- All references to `flow.shutdown()` are now updated to use the context manager changelog. 

## /.github/
- Added `enhanced_ci.yml`
  - Removes redudant venv creation for pytest
  - Stores coverage to runner's artifacts 
  - Added linter 
- Added `ISSUE_TEMPLATE`
   - Contains templates for opening issues for bugs,documentation,enhancement and feature
