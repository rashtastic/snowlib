"""Tox testing utilities for build process."""

import re
import shutil
import subprocess
from pathlib import Path


def strip_ansi_codes(text: str) -> str:
    """Remove ANSI escape sequences from text"""
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)


def clean_text_output(text: str) -> str:
    """Clean text for display and file output"""
    cleaned = strip_ansi_codes(text)
    cleaned = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', cleaned)
    return cleaned


def rebuild_tox_environments(
    project_dir: Path,
    rebuild: bool = False,
    envlist: list[str] | None = None
) -> tuple[str, str, int]:
    """Run tox tests across Python versions
    
    Args:
        project_dir: Root directory of the project
        rebuild: If True, delete .tox directory and use --recreate flag
        envlist: Optional list of tox environments (e.g., ['py314', 'py313'])
        
    Returns:
        Tuple of (clean_stdout, clean_stderr, returncode)
    """
    if rebuild:
        tox_dir = project_dir / '.tox'
        if tox_dir.exists():
            print(f"[REBUILD] Deleting {tox_dir}...")
            shutil.rmtree(tox_dir)
            print("[REBUILD] .tox directory deleted\n")
        else:
            print("[REBUILD] No .tox directory to delete\n")
    
    tox_cmd = ['tox']
    if rebuild:
        tox_cmd.append('--recreate')
    if envlist:
        tox_cmd.extend(['-e', ','.join(envlist)])
    
    result = subprocess.run(
        tox_cmd,
        cwd=project_dir,
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace'
    )
    
    clean_stdout = clean_text_output(result.stdout)
    clean_stderr = clean_text_output(result.stderr) if result.stderr else ""
    
    return clean_stdout, clean_stderr, result.returncode
