"""Quality validation checks for release preparation."""

import tomllib
from pathlib import Path


def check_pyproject_complete(project_root: Path) -> None:
    """Verify pyproject.toml has all required fields"""
    with open(project_root / "pyproject.toml", "rb") as f:
        pyproject_data = tomllib.load(f)

    project = pyproject_data.get("project", {})
    required_fields = ["name", "description", "authors"]
    dynamic_fields = project.get("dynamic", [])

    missing_fields = [field for field in required_fields if field not in project]
    if missing_fields:
        raise ValueError(f"Missing required fields in pyproject.toml: {missing_fields}")

    if "version" not in project and "version" not in dynamic_fields:
        raise ValueError("Version must be in project or dynamic fields")
    
    print("[OK] pyproject.toml complete")


def check_readme_exists(project_root: Path) -> None:
    """Verify README file exists"""
    readme_files = list(project_root.glob("README*"))
    if not readme_files:
        raise FileNotFoundError("No README file found in project root")
    print("[OK] README exists")


def check_main_module_exists(project_root: Path) -> None:
    """Verify main module structure exists"""
    src_snowlib = project_root / "src" / "snowlib"
    if not src_snowlib.exists():
        raise FileNotFoundError("src/snowlib directory not found")
    if not (src_snowlib / "__init__.py").exists():
        raise FileNotFoundError("src/snowlib/__init__.py not found")
    print("[OK] Main module exists")


def check_tests_exist(project_root: Path) -> None:
    """Verify tests directory and test files exist"""
    tests_dir = project_root / "tests"
    if not tests_dir.exists():
        raise FileNotFoundError("tests directory not found")

    test_files = list(tests_dir.rglob("test_*.py"))
    if not test_files:
        raise FileNotFoundError("No test files (test_*.py) found in tests directory")
    print(f"[OK] Tests exist ({len(test_files)} test files)")


def check_no_problematic_files(project_root: Path) -> None:
    """Verify no sensitive files exist in project root"""
    problematic_files = ["passwords.txt", "secrets.json", "connections.toml"]
    found_files = [f for f in problematic_files if (project_root / f).exists()]
    if found_files:
        raise FileExistsError(f"Problematic files found in project root: {found_files}")
    print("[OK] No problematic files")


QUALITY_CHECKS = [
    check_pyproject_complete,
    check_readme_exists,
    check_main_module_exists,
    check_tests_exist,
    check_no_problematic_files,
]


def run_all_checks(project_root: Path) -> None:
    """Run all quality checks and raise on first failure"""
    print("Running quality checks...")
    print("=" * 40)
    for check in QUALITY_CHECKS:
        check(project_root)
    print("=" * 40)
    print("All quality checks passed!")
