param(
    [string]$Port = 'COM11',
    [int]$Baud = 9600,
    [ValidateSet('even','none','odd')][string]$Parity = 'even',
    [int]$WaitMs = 5000,
    [int]$OpenDelay = 0,
    [ValidateSet('on','off')][string]$Rts = 'off',
    [ValidateSet('on','off')][string]$Dtr = 'on',
    [ValidateSet('both','698','645')][string]$Protocol = 'both'
)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss-fff'
$output = Join-Path $root ("serial-sync-" + $stamp + "-" + $PID + '.log')
$bootstrap = $output + '.startup.txt'
try {
    ('Starting local C# compilation: ' + (Get-Date).ToString()) | Out-File -FilePath $bootstrap -Encoding UTF8
    $source = [IO.File]::ReadAllText((Join-Path $root 'diagnostics\SyncSerialProbe.cs'))
    Add-Type -TypeDefinition $source -Language CSharp
    $result = [SyncSerialProbe]::Run($Port, $Baud, $Parity, $WaitMs, $OpenDelay, ($Rts -eq 'on'), ($Dtr -eq 'on'), $Protocol, $output)
    ('Finished, code=' + $result) | Out-File -FilePath $bootstrap -Encoding UTF8 -Append
    [Environment]::Exit($result)
} catch {
    $_.Exception.ToString() | Out-File -FilePath $bootstrap -Encoding UTF8 -Append
    Write-Host $_.Exception.ToString()
    Write-Host ('Startup log: ' + $bootstrap)
    [Environment]::Exit(1)
}
