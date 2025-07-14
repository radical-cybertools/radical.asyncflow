import sys
from pathlib import Path
import mkdocs_gen_files

nav = mkdocs_gen_files.Nav()

# Use the actual radical.flow source path
base_path = Path("src/radical/flow")

if not base_path.exists():
    raise RuntimeError(f"Source path {base_path} does not exist")

# Scan source files
for path in sorted(base_path.rglob("*.py")):
    if path.name == "__init__.py":
        continue

    # Get module path relative to src
    rel_path = path.relative_to("src")
    module_parts = list(rel_path.parts)
    
    # Create doc path without extra reference prefix
    doc_path = Path(*module_parts).with_suffix(".md")
    ident = ".".join(module_parts)[:-3]  # Remove .py extension

    nav[tuple(module_parts)] = doc_path.as_posix()

    print(f"Generating docs for: {ident}")
    with mkdocs_gen_files.open(doc_path, "w") as f:
        f.write(f"::: {ident}\n")
        f.write("    options:\n")
        f.write("      show_source: true\n")
        f.write("      members: true\n")

    mkdocs_gen_files.set_edit_path(doc_path, path)

# Generate SUMMARY.md
with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
        f.write(f"::: {actual_ident}\n")
        f.write("    options:\n")
        f.write("      show_source: true\n")
        f.write("      members: true\n")

    mkdocs_gen_files.set_edit_path(doc_path, path)

# Generate SUMMARY.md
with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
