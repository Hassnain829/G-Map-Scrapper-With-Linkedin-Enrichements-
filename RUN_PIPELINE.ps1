Set-Location -Path $PSScriptRoot

# Starts the web dashboard; it opens in the default browser.
if (Test-Path "venv\Scripts\python.exe") {
    & "venv\Scripts\python.exe" "app.py"
} elseif (Test-Path ".venv\Scripts\python.exe") {
    & ".venv\Scripts\python.exe" "app.py"
} else {
    & python "app.py"
}
