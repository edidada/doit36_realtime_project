param(
    [string]$StateDirectory = '.artifacts/fraud/local-demo',
    [int]$Port = 8088,
    [switch]$SkipBuild,
    [switch]$Background
)
$ErrorActionPreference = 'Stop'
$repository = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
Set-Location $repository
if (-not $SkipBuild) {
    & mvn -B -ntp -pl fin-risk-engine -am package | Out-Host
    if ($LASTEXITCODE -ne 0) { throw 'Maven build failed' }
}
# Keep these environment variables in this process for a restart. Never print their values.
foreach ($name in @('RISK_ALICE_TOKEN', 'RISK_BOB_TOKEN', 'RISK_NEWBIE_TOKEN', 'RISK_ADMIN_TOKEN')) {
    if (-not [Environment]::GetEnvironmentVariable($name, 'Process')) {
        $bytes = New-Object byte[] 32
        $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
        try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
        [Environment]::SetEnvironmentVariable($name, [Convert]::ToBase64String($bytes), 'Process')
    }
}
$env:RISK_PORT = [string]$Port
Write-Host 'Tokens are in the current PowerShell environment. Keep this window for restart; use demo-test.ps1 for a self-contained acceptance run.'
if ($Background) {
    New-Item -ItemType Directory -Force $StateDirectory | Out-Null
    $state = (Resolve-Path $StateDirectory).Path
    $jar = Join-Path $repository 'fin-risk-engine/target/fin-risk-engine-1.0-SNAPSHOT-app.jar'
    $processArgs = @('-Xmx256m', '-jar', ('"{0}"' -f $jar), 'serve-demo', ('"{0}"' -f $state))
    $server = Start-Process java -ArgumentList $processArgs -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $state 'stdout.log') -RedirectStandardError (Join-Path $state 'stderr.log')
    try {
        for ($attempt = 0; $attempt -lt 40; $attempt++) {
            if ($server.HasExited) { throw "Startup failed; check $state/stderr.log" }
            try {
                $health = Invoke-RestMethod "http://127.0.0.1:$Port/health" -TimeoutSec 1
                if ($health.status -eq 'UP') { return $server }
            } catch { Start-Sleep -Milliseconds 250 }
        }
        throw 'Server readiness timed out'
    } catch {
        if (-not $server.HasExited) { Stop-Process -Id $server.Id }
        throw
    }
}
& java -Xmx256m -jar fin-risk-engine/target/fin-risk-engine-1.0-SNAPSHOT-app.jar serve-demo $StateDirectory
if ($LASTEXITCODE -ne 0) { throw 'Risk application exited with failure' }
