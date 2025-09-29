"""Tests for the optional dependency installation mechanism.

Tests that verify the pyproject.toml configuration and expected behavior when users
install different optional dependency groups.
"""

import sys
from pathlib import Path

import pytest


class TestInstallationMechanism:
    """Test the optional dependency installation mechanism."""

    @pytest.fixture
    def pyproject_path(self):
        """Get path to pyproject.toml file."""
        repo_root = Path(__file__).parent.parent.parent
        return repo_root / "pyproject.toml"

    def test_base_dependencies_minimal(self, pyproject_path):
        """Test that base AsyncFlow has minimal dependencies."""
        try:
            import tomllib
        except ImportError:
            # Python < 3.11 fallback
            import tomli as tomllib

        with open(pyproject_path, "rb") as f:
            config = tomllib.load(f)

        base_deps = config["project"]["dependencies"]

        # Should have minimal, essential dependencies only
        expected_base = {"pydantic", "typeguard", "requests"}
        actual_base = {dep.split()[0] for dep in base_deps}

        assert actual_base == expected_base, (
            f"Base dependencies should be minimal. "
            f"Expected: {expected_base}, Got: {actual_base}"
        )

        # Should NOT have rhapsody as base dependency
        assert not any("rhapsody" in dep for dep in base_deps)

    def test_optional_dependencies_structure(self, pyproject_path):
        """Test that optional dependencies are properly structured."""
        try:
            import tomllib
        except ImportError:
            # Python < 3.11 fallback
            import tomli as tomllib

        with open(pyproject_path, "rb") as f:
            config = tomllib.load(f)

        optional_deps = config["project"]["optional-dependencies"]

        # Should have HPC optional dependency groups
        expected_groups = {"hpc", "dask", "radicalpilot"}
        actual_groups = set(optional_deps.keys()) - {"lint", "dev"}  # Exclude dev deps

        assert expected_groups.issubset(actual_groups), (
            f"Missing optional dependency groups. "
            f"Expected: {expected_groups}, Got: {actual_groups}"
        )

    def test_hpc_dependencies_include_rhapsody(self, pyproject_path):
        """Test that all HPC optional deps include rhapsody."""
        try:
            import tomllib
        except ImportError:
            # Python < 3.11 fallback
            import tomli as tomllib

        with open(pyproject_path, "rb") as f:
            config = tomllib.load(f)

        optional_deps = config["project"]["optional-dependencies"]

        # All HPC groups should include rhapsody
        hpc_groups = ["hpc", "dask", "radicalpilot"]
        for group_name in hpc_groups:
            if group_name in optional_deps:
                deps = optional_deps[group_name]
                rhapsody_present = any("rhapsody" in dep for dep in deps)
                assert rhapsody_present, (
                    f"Optional dependency group '{group_name}' should include rhapsody"
                )

    def test_dask_group_includes_dask_distributed(self, pyproject_path):
        """Test that dask optional dependency includes dask[distributed]."""
        try:
            import tomllib
        except ImportError:
            # Python < 3.11 fallback
            import tomli as tomllib

        with open(pyproject_path, "rb") as f:
            config = tomllib.load(f)

        optional_deps = config["project"]["optional-dependencies"]

        if "dask" in optional_deps:
            deps = optional_deps["dask"]
            dask_distributed_present = any("dask[distributed]" in dep for dep in deps)
            assert dask_distributed_present, (
                "dask optional dependency should include 'dask[distributed]'"
            )

    def test_radicalpilot_group_includes_radical_pilot(self, pyproject_path):
        """Test that radicalpilot optional dependency includes radical.pilot."""
        try:
            import tomllib
        except ImportError:
            # Python < 3.11 fallback
            import tomli as tomllib

        with open(pyproject_path, "rb") as f:
            config = tomllib.load(f)

        optional_deps = config["project"]["optional-dependencies"]

        if "radicalpilot" in optional_deps:
            deps = optional_deps["radicalpilot"]
            radical_pilot_present = any("radical.pilot" in dep for dep in deps)
            assert radical_pilot_present, (
                "radicalpilot optional dependency should include 'radical.pilot'"
            )

    def test_rhapsody_uses_dev_branch(self, pyproject_path):
        """Test that rhapsody dependencies point to dev branch."""
        try:
            import tomllib
        except ImportError:
            # Python < 3.11 fallback
            import tomli as tomllib

        with open(pyproject_path, "rb") as f:
            config = tomllib.load(f)

        optional_deps = config["project"]["optional-dependencies"]

        # All rhapsody references should use dev branch
        for group_name, deps in optional_deps.items():
            if group_name in {"hpc", "dask", "radicalpilot"}:
                for dep in deps:
                    if "rhapsody" in dep:
                        assert "@dev" in dep, (
                            f"rhapsody dependency in '{group_name}' should use "
                            f"dev branch: {dep}"
                        )

    def test_no_removed_backends_in_optional_deps(self, pyproject_path):
        """Test that removed backends (dragon, flux) are not in optional deps."""
        try:
            import tomllib
        except ImportError:
            # Python < 3.11 fallback
            import tomli as tomllib

        with open(pyproject_path, "rb") as f:
            config = tomllib.load(f)

        optional_deps = config["project"]["optional-dependencies"]

        # Should not have removed backends
        removed_backends = {"dragon", "flux"}
        for backend_name in removed_backends:
            assert backend_name not in optional_deps, (
                f"Removed backend '{backend_name}' should not be in "
                f"optional dependencies"
            )

    @pytest.mark.skipif(
        sys.version_info < (3, 9),
        reason="Python 3.9+ required for modern importlib features",
    )
    def test_importlib_availability_in_runtime(self):
        """Test that importlib features work as expected in current Python."""
        import importlib

        # Test that importlib can handle missing modules gracefully
        try:
            importlib.import_module("non_existent_module_for_testing")
        except ImportError as e:
            assert "non_existent_module_for_testing" in str(e)

        # Test that we can check for module existence
        try:
            import importlib.util

            spec = importlib.util.find_spec("radical.asyncflow")
            assert spec is not None  # Should exist since we're testing it
        except (AttributeError, ImportError):
            # Some Python versions might not have util.find_spec
            pass

    def test_backend_factory_installation_hints_consistency(self):
        """Test that factory installation hints match pyproject.toml."""
        from radical.asyncflow.backends.factory import BackendFactory

        try:
            import tomllib
        except ImportError:
            # Python < 3.11 fallback
            import tomli as tomllib

        repo_root = Path(__file__).parent.parent.parent
        pyproject_path = repo_root / "pyproject.toml"

        with open(pyproject_path, "rb") as f:
            config = tomllib.load(f)

        optional_deps = config["project"]["optional-dependencies"]

        # Test that factory hints match pyproject.toml groups
        if "dask" in optional_deps:
            dask_hint = BackendFactory._suggest_installation("dask")
            assert "radical.asyncflow[dask]" in dask_hint

        if "radicalpilot" in optional_deps:
            radicalpilot_hint = BackendFactory._suggest_installation("radical_pilot")
            assert "radical.asyncflow[radicalpilot]" in radicalpilot_hint
