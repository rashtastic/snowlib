<#
.SYNOPSIS
Activate the repo-local Python virtual environment for the current session.

.EXAMPLE
. .\venv.ps1

.DESCRIPTION
Dot-source this script. Running it without the leading dot activates a child
scope that disappears the moment the script ends, which looks like the
activation silently failing.

The venv is located relative to this script, not to the current directory, so
it activates the right environment from anywhere and cannot pick up a sibling
checkout's venv by accident.
#>

$RepoRoot = (Resolve-Path "$PSScriptRoot").Path
$Activate = Join-Path $RepoRoot ".venv\Scripts\Activate.ps1"

if (-not (Test-Path $Activate)) {
    Write-Error @"
No .venv at $RepoRoot\.venv. Create it with:
    uv venv --seed --python 3.12
    uv pip install --python .venv\Scripts\python.exe -e .[dev]
"@
    return
}

& $Activate
