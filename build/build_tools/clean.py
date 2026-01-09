from pathlib import Path
import subprocess
import nbformat


def clear_notebook(path: Path) -> bool:
    nb = nbformat.read(path, as_version=4)
    changed = False

    for cell in nb.cells:
        if cell.cell_type == "code":
            if cell.outputs:
                cell.outputs = []
                changed = True
            if cell.execution_count is not None:
                cell.execution_count = None
                changed = True
            
            # Clear execution metadata
            if cell.metadata:
                if cell.metadata.pop('execution', None) is not None:
                    changed = True
                if cell.metadata.pop('scrolled', None) is not None:
                    changed = True
                if cell.metadata.pop('collapsed', None) is not None:
                    changed = True
    
    # Clear notebook-level widget state
    if nb.metadata.pop('widgets', None) is not None:
        changed = True

    if changed:
        nbformat.write(nb, path)

    return changed


def clean_tracked_notebooks(project_dir: Path) -> dict[str, int]:
    """Clean all git-tracked notebooks in the project
    
    Returns a dict with 'total', 'cleaned', and 'already_clean' counts
    """
    result = subprocess.run(
        ["git", "ls-files", "*.ipynb"],
        cwd=project_dir,
        capture_output=True, text=True
    )
    
    if result.returncode != 0:
        raise RuntimeError(f"Failed to list tracked notebooks: {result.stderr}")
    
    tracked_notebooks = [
        project_dir / nb.strip()
        for nb in result.stdout.strip().split('\n')
        if nb.strip()
    ]
    
    cleaned_count = 0
    for nb_path in tracked_notebooks:
        if nb_path.exists():
            changed = clear_notebook(nb_path)
            if changed:
                cleaned_count += 1
    
    return {
        'total': len(tracked_notebooks),
        'cleaned': cleaned_count,
        'already_clean': len(tracked_notebooks) - cleaned_count
    }
