param([switch]$SkipBuild, [string]$LogDirectory)
$ErrorActionPreference = 'Stop'
$taskRoot = Split-Path -Parent $PSScriptRoot
Push-Location $taskRoot
try {
    if (!$SkipBuild) {
        & mvn -B -ntp -DskipTests package
        if ($LASTEXITCODE -ne 0) { throw 'Build failed.' }
    }
    foreach ($module in @('realtime_dw', 'batch_jobs', 'redis_demo', 'rt_rule_mk', 'fin-olap-engine', 'fin-risk-engine')) {
        & mvn -q -ntp -f "$module/pom.xml" dependency:build-classpath '-Dmdep.includeScope=runtime' '-Dmdep.outputFile=target/logging-classpath.txt'
        if ($LASTEXITCODE -ne 0) { throw "Cannot resolve runtime classpath: $module" }
        $dependencyPath = (Get-Content -LiteralPath "$module/target/logging-classpath.txt" -Raw).Trim()
        $obsolete = @($dependencyPath.Split([IO.Path]::PathSeparator) | Where-Object {
            [IO.Path]::GetFileName($_) -match '^(reload4j-[\d.]+|log4j-1\.[\d.]+)\.jar$'
        })
        if ($obsolete.Count -gt 0) {
            throw "Obsolete Log4j 1.x implementation in $module"
        }
        $classPath = (Join-Path $taskRoot "$module/target/classes") + [IO.Path]::PathSeparator + $dependencyPath
        $javaOptions = @()
        if ($LogDirectory) {
            $logPath = $LogDirectory
            if (![IO.Path]::IsPathRooted($logPath)) { $logPath = Join-Path $taskRoot $logPath }
            $javaOptions += '-Drealtime.log.dir=' + [IO.Path]::GetFullPath($logPath)
        }
        & java @javaOptions --class-path $classPath scripts/LoggingProbe.java $module
        if ($LASTEXITCODE -ne 0) { throw "Logging probe failed: $module" }
        Write-Host "PASS: $module SLF4J -> Log4j2 XML -> rolling file"
    }
} finally { Pop-Location }
