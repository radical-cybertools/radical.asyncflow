#!/usr/bin/env python3
"""Test script to validate AsyncFlow examples configuration and basic functionality."""

import subprocess
import sys
from pathlib import Path

import yaml


def main():
    """Test the examples configuration."""
    print("🚀 Testing AsyncFlow examples configuration...")

    # Load configuration
    config_path = Path(".github/examples-config.yml")
    if not config_path.exists():
        print("❌ Missing .github/examples-config.yml")
        return False

    with open(config_path) as f:
        config = yaml.safe_load(f)

    if "examples" not in config:
        print("❌ Missing 'examples' key in configuration")
        return False

    examples = config["examples"]
    print(f"📝 Found {len(examples)} configured examples:")

    success_count = 0
    total_count = len(examples)

    for example_key, example_config in examples.items():
        print(f"\n🔍 Testing {example_key}...")

        # Validate configuration
        script = example_config.get("script", f"examples/{example_key}.py")
        backend = example_config.get("backend", "concurrent")
        timeout = example_config.get("timeout_sec", 120)

        print(f"  Script: {script}")
        print(f"  Backend: {backend}")
        print(f"  Timeout: {timeout}s")

        # Check if script exists
        script_path = Path(script)
        if not script_path.exists():
            print(f"  ❌ Script not found: {script}")
            continue

        # Basic syntax check
        try:
            result = subprocess.run(
                [sys.executable, "-m", "py_compile", str(script_path)],
                capture_output=True,
                text=True,
            )

            if result.returncode == 0:
                print("  ✅ Syntax check passed")
                success_count += 1
            else:
                print(f"  ❌ Syntax error: {result.stderr.strip()}")

        except Exception as e:
            print(f"  ❌ Failed to check syntax: {e}")

    print(f"\n📊 Results: {success_count}/{total_count} examples passed syntax check")

    if success_count == total_count:
        print("✅ All examples configuration validated successfully!")
        return True
    else:
        print("❌ Some examples have issues")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
