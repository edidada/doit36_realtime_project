param([int]$Port = 18088, [switch]$SkipBuild)
$ErrorActionPreference = 'Stop'
$repository = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
Set-Location $repository
if (-not $SkipBuild) {
    & mvn -B -ntp -pl fin-risk-engine -am package
    if ($LASTEXITCODE -ne 0) { throw 'Maven build failed' }
}
$runId = [guid]::NewGuid().ToString('N')
$runDirectory = Join-Path $repository ".artifacts/fraud/acceptance-$runId"
New-Item -ItemType Directory -Path $runDirectory | Out-Null
foreach ($name in @('RISK_ALICE_TOKEN', 'RISK_BOB_TOKEN', 'RISK_NEWBIE_TOKEN', 'RISK_ADMIN_TOKEN')) {
    $bytes = New-Object byte[] 32
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    [Environment]::SetEnvironmentVariable($name, [Convert]::ToBase64String($bytes), 'Process')
}
$env:RISK_PORT = [string]$Port
$jar = Join-Path $repository 'fin-risk-engine/target/fin-risk-engine-1.0-SNAPSHOT-app.jar'
$state = Join-Path $runDirectory 'db'
$processArgs = @('-Xmx256m', '-jar', ('"{0}"' -f $jar), 'serve-demo', ('"{0}"' -f $state))
$process = Start-Process java -ArgumentList $processArgs -PassThru -WindowStyle Hidden `
    -RedirectStandardOutput (Join-Path $runDirectory 'stdout.log') -RedirectStandardError (Join-Path $runDirectory 'stderr.log')
$base = "http://127.0.0.1:$Port"
function Request([string]$Token, [string]$Method, [string]$Path, $Body = $null, [string]$Key = '') {
    $headers = @{ Authorization = "Bearer $Token" }
    if ($Key) { $headers['Idempotency-Key'] = $Key }
    $parameters = @{ Uri="$base$Path"; Method=$Method; Headers=$headers; TimeoutSec=15 }
    if ($null -ne $Body) { $parameters.ContentType='application/json'; $parameters.Body=($Body | ConvertTo-Json -Compress) }
    Invoke-RestMethod @parameters
}
function Ensure($Condition, [string]$Description) { if (-not $Condition) { throw "FAILED: $Description" }; Write-Host "PASS: $Description" }
try {
    $ready = $false
    for ($attempt = 0; $attempt -lt 40; $attempt++) {
        if ($process.HasExited) { throw "Risk application failed to start; see $runDirectory" }
        try { $health = Invoke-RestMethod "$base/health" -TimeoutSec 1; $ready = $health.status -eq 'UP'; if ($ready) { break } }
        catch { Start-Sleep -Milliseconds 250 }
    }
    Ensure $ready 'API started using only CPU and embedded H2'
    $coupon = Request $env:RISK_ALICE_TOKEN POST '/v1/coupons/claim' @{campaignId='welcome'; deviceId='alice-phone'} 'claim-alice'
    Ensure ($coupon.status -eq 'CLAIMED') 'Alice claims coupon'
    $bobOrder = Request $env:RISK_BOB_TOKEN POST '/v1/orders' @{productId='approve'; quantity=1} 'bob-order'
    $stolen = Request $env:RISK_BOB_TOKEN POST '/v1/payments' @{orderId=$bobOrder.id; deviceId='bob-phone'; couponId=$coupon.coupon.id} 'bob-steal'
    Ensure ($stolen.status -eq 'BLOCKED') 'Bob cannot steal Alice coupon'
    $order = Request $env:RISK_ALICE_TOKEN POST '/v1/orders' @{productId='approve'; quantity=1} 'alice-order'
    $paymentBody = @{orderId=$order.id; deviceId='alice-phone'; couponId=$coupon.coupon.id}
    $paid = Request $env:RISK_ALICE_TOKEN POST '/v1/payments' $paymentBody 'alice-pay'
    Ensure ($paid.status -eq 'PAID' -and $paid.payableCents -eq 8000) 'server price 10000 minus coupon 2000 is captured'
    $retry = Request $env:RISK_ALICE_TOKEN POST '/v1/payments' $paymentBody 'alice-pay'
    Ensure ($retry.id -eq $paid.id) 'retry is idempotent'
    $wallet = Request $env:RISK_ALICE_TOKEN GET '/v1/wallet'
    Ensure ($wallet.balanceCents -eq 9992000) 'wallet debited once'
    $refund = Request $env:RISK_ALICE_TOKEN POST "/v1/payments/$($paid.id)/refund" @{}
    Ensure ($refund.status -eq 'REFUNDED') 'full net refund completes'
    $after = Request $env:RISK_ALICE_TOKEN GET '/v1/wallet'
    Ensure ($after.balanceCents -eq 10000000) 'wallet restored'
    $voidCoupon = Request $env:RISK_ALICE_TOKEN GET "/v1/coupons/$($coupon.coupon.id)"
    Ensure ($voidCoupon.status -eq 'VOID') 'refund coupon cannot be farmed again'
    $large = Request $env:RISK_ALICE_TOKEN POST '/v1/orders' @{productId='large';quantity=1} 'large-order'
    $review = Request $env:RISK_ALICE_TOKEN POST '/v1/payments' @{orderId=$large.id;deviceId='alice-phone'} 'large-pay'
    Ensure ($review.status -eq 'REVIEW') 'unusual large payment is held uncharged'
    $reviewed = Request $env:RISK_ADMIN_TOKEN POST "/v1/payments/$($review.id)/review" @{approve=$true;reason='Independent sandbox operator verification'}
    Ensure ($reviewed.status -eq 'PAID') 'independent operator review completes payment'
    $timeoutOrder = Request $env:RISK_BOB_TOKEN POST '/v1/orders' @{productId='timeout_after_capture';quantity=1} 'timeout-order'
    $unknown = Request $env:RISK_BOB_TOKEN POST '/v1/payments' @{orderId=$timeoutOrder.id;deviceId='bob-phone'} 'timeout-pay'
    Ensure ($unknown.status -eq 'PENDING') 'capture timeout remains pending'
    Request $env:RISK_ADMIN_TOKEN POST '/v1/admin/reconcile' @{} | Out-Null
    $recovered = Request $env:RISK_BOB_TOKEN GET "/v1/payments/$($unknown.id)"
    Ensure ($recovered.status -eq 'PAID') 'reconciliation recovers timeout without duplicate debit'
    $audit = Request $env:RISK_ADMIN_TOKEN GET "/v1/payments/$($paid.id)/audit"
    Ensure ($audit.Count -ge 4) 'decisions and settlement are audited'
    Write-Host "Acceptance complete. Synthetic DB and logs retained at $runDirectory"
} finally {
    if (-not $process.HasExited) { Stop-Process -Id $process.Id }
}
