param([switch]$SkipBuild)
$ErrorActionPreference = 'Stop'
$taskRoot = Split-Path -Parent $PSScriptRoot
Push-Location $taskRoot
try {
    if (!$SkipBuild) {
        & mvn -B -ntp package
        if ($LASTEXITCODE -ne 0) { throw 'Maven package failed.' }
    }
    $release = 'realtime-dw-' + (Get-Date -Format 'yyyyMMdd-HHmmss') + '-' + [guid]::NewGuid().ToString('N').Substring(0, 8)
    $output = Join-Path $taskRoot ".artifacts/$release"
    if (Test-Path -LiteralPath $output) { throw 'Refusing to overwrite an existing bundle.' }
    [IO.Directory]::CreateDirectory($output) | Out-Null
    $logging = Join-Path $output 'logging'
    [IO.Directory]::CreateDirectory($logging) | Out-Null
    foreach ($module in @('realtime_dw', 'rt_rule_mk')) {
        $libraries = Join-Path $taskRoot "$module/target/$release-dependencies"
        & mvn -q -ntp -pl $module dependency:copy-dependencies '-DincludeScope=runtime' "-DoutputDirectory=$libraries"
        if ($LASTEXITCODE -ne 0) { throw "Dependency resolution failed: $module" }
        $destination = Join-Path $output "usrlib/$module"
        [IO.Directory]::CreateDirectory($destination) | Out-Null
        foreach ($library in Get-ChildItem -LiteralPath $libraries -Filter '*.jar') {
            if ($library.Name -like 'log4j-*.jar' -or $library.Name -like 'slf4j-*.jar') {
                if ($module -eq 'realtime_dw') { Copy-Item -LiteralPath $library.FullName -Destination $logging }
            } elseif ($library.Name -notlike 'flink-table-planner_*.jar') {
                # The Flink distribution already has planner-loader; never ship a second full planner.
                Copy-Item -LiteralPath $library.FullName -Destination $destination
            }
        }
        & jar --create --file (Join-Path $destination "$module-job.jar") -C "$module/target/classes" .
        if ($LASTEXITCODE -ne 0) { throw "Job JAR creation failed: $module" }
    }
    $tools = Join-Path $output 'tools'
    [IO.Directory]::CreateDirectory($tools) | Out-Null
    $compilePath = ((Get-ChildItem -LiteralPath $logging -Filter '*.jar').FullName -join [IO.Path]::PathSeparator)
    & javac --release 17 -cp $compilePath -d $tools scripts/LoggingProbe.java
    if ($LASTEXITCODE -ne 0) { throw 'Runtime probe compilation failed.' }
    Copy-Item -LiteralPath fin-risk-engine/target/fin-risk-engine-1.0-SNAPSHOT-app.jar -Destination (Join-Path $tools 'fin-risk-engine-app.jar')
    Copy-Item -LiteralPath deploy/k3s/Dockerfile.runtime-bundle -Destination (Join-Path $output 'Dockerfile')
    $manifest = foreach ($file in Get-ChildItem -LiteralPath $output -Recurse -File) {
        '{0}  {1}' -f (Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash.ToLowerInvariant(),
            $file.FullName.Substring($output.Length + 1).Replace('\', '/')
    }
    [IO.File]::WriteAllText((Join-Path $output 'SHA256SUMS'), ($manifest -join "`n") + "`n", [Text.Encoding]::ASCII)
    & tar -cf "$output.tar" -C $output .
    if ($LASTEXITCODE -ne 0) { throw 'Bundle archive creation failed.' }
    Write-Host "BUNDLE=$output.tar"
    Write-Host "RELEASE=$release"
} finally { Pop-Location }
