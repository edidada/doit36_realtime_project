$ErrorActionPreference = 'Stop'
$taskRoot = Split-Path -Parent $PSScriptRoot
Push-Location $taskRoot
try {
    $paths = @(git ls-files '*pom.xml')
    if ($LASTEXITCODE -ne 0) { throw 'Cannot enumerate Maven POMs.' }
    foreach ($path in $paths) {
        [xml]$model = [IO.File]::ReadAllText((Join-Path $taskRoot $path))
        foreach ($container in $model.SelectNodes('//*[local-name()="dependencies" or local-name()="exclusions"]')) {
            $seen = @{}
            foreach ($entry in $container.ChildNodes | Where-Object { $_.NodeType -eq 'Element' }) {
                $key = "$($entry.groupId):$($entry.artifactId):$($entry.type):$($entry.classifier)"
                if ($seen.ContainsKey($key)) { throw "Duplicate Maven entry in ${path}: $key" }
                $seen[$key] = $true
                foreach ($childGroup in @($entry.ChildNodes | Where-Object { $_.NodeType -eq 'Element' } | Group-Object LocalName)) {
                    if ($childGroup.Count -gt 1) { throw "Duplicate <$($childGroup.Name)> in ${path}: $key" }
                }
            }
        }
    }
    Write-Host "PASS: $($paths.Count) POMs parse without duplicate dependencies or exclusion blocks."
} finally { Pop-Location }
