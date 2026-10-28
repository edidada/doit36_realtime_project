$ErrorActionPreference = 'Stop'
$taskRoot = Split-Path -Parent $PSScriptRoot
Push-Location $taskRoot
try {
    $paths = @(git -c core.quotepath=false ls-files --cached --others --exclude-standard | Sort-Object -Unique)
    if ($LASTEXITCODE -ne 0) { throw 'Cannot enumerate repository files.' }
    $oldPackage = 'cn' + '.' + 'doitedu'
    $oldPaths = @($oldPackage, $oldPackage.Replace('.', '/'), $oldPackage.Replace('.', '\'))
    $problems = @()
    $sourceCount = 0
    foreach ($relative in $paths) {
        # Archived runtime logs are historical evidence, not executable configuration.
        if ($relative.StartsWith('logs/')) { continue }
        if (!(Test-Path -LiteralPath $relative -PathType Leaf)) { continue }
        $bytes = [IO.File]::ReadAllBytes((Join-Path $taskRoot $relative))
        if ($bytes -contains 0) { continue }
        $content = [Text.Encoding]::UTF8.GetString($bytes)
        foreach ($oldPath in $oldPaths) {
            if ($content.Contains($oldPath) -or $relative.Contains($oldPath)) {
                $problems += "Obsolete package reference: $relative"
                break
            }
        }
        if ($relative -match '^(.*?/src/(?:main|test)/(?:java|scala|groovy)/)(.+)\.(java|scala|groovy)$') {
            $sourcePath = $Matches[2]
            if ($content -notmatch '(?m)^\s*package\s+([\w.]+)') {
                $problems += "Missing package declaration: $relative"
                continue
            }
            $declaredPackage = $Matches[1]
            $expectedPath = $declaredPackage.Replace('.', '/') + '/' + [IO.Path]::GetFileNameWithoutExtension($relative)
            if ($sourcePath -cne $expectedPath -or !($declaredPackage -eq 'com.test.bigdata' -or $declaredPackage.StartsWith('com.test.bigdata.'))) {
                $problems += "Package/path mismatch: $relative ($declaredPackage)"
            }
            $sourceCount++
        }
    }
    if ($problems.Count -gt 0) { throw ($problems -join [Environment]::NewLine) }
    Write-Host "PASS: $sourceCount Java/Scala/Groovy source packages match their paths; no obsolete package references outside archived logs."
} finally {
    Pop-Location
}
