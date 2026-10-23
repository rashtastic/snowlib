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

When .venv is missing, tools\bootstrap.py creates it first.
#>

$RepoRoot = (Resolve-Path "$PSScriptRoot").Path
$Activate = Join-Path $RepoRoot ".venv\Scripts\Activate.ps1"

if (-not (Test-Path $Activate)) {
    Write-Host "No .venv at $RepoRoot\.venv; creating it with tools\bootstrap.py"
    uv run --isolated --no-project (Join-Path $RepoRoot "tools\bootstrap.py")
    if ($LASTEXITCODE -ne 0) {
        Write-Error "tools\bootstrap.py failed; see its output above."
        return
    }
}

& $Activate
