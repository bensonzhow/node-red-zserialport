# Windows PowerShell 2.0 / Win7. Run from a separate console.
$ErrorActionPreference = 'Stop'
$folder = Split-Path -Parent $MyInvocation.MyCommand.Path
$file = Join-Path $folder ('sync-monitor-' + (Get-Date -Format yyyyMMdd-HHmmss) + '-' + $PID + '.csv')
'time,availableMB,committedMB,commitLimitMB,bridgeCount,bridges_PID_PPID_created_handles_privateMB,nodePIDs,status' | Out-File $file -Encoding utf8
Write-Host "Log: $file (Ctrl+C to finish)"
while ($true) {
    $stamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    try {
        $memory = Get-WmiObject Win32_PerfFormattedData_PerfOS_Memory
        if ($null -eq $memory.CommitLimit -or $memory.CommitLimit -le 0) { throw 'Memory counters unavailable' }
        $processes = @(Get-WmiObject Win32_Process -Filter "Name='zserial-sync.exe' OR Name='node.exe'")
        $bridges = @($processes | Where-Object { $_.Name -eq 'zserial-sync.exe' })
        $details = @($bridges | ForEach-Object {
            '{0}/{1}/{2}/{3}/{4}' -f $_.ProcessId,$_.ParentProcessId,$_.CreationDate,$_.HandleCount,[math]::Round(([double]$_.PrivatePageCount / 1MB))
        })
        $nodes = @($processes | Where-Object { $_.Name -eq 'node.exe' } | ForEach-Object { $_.ProcessId })
        $line = '{0},{1},{2},{3},{4},{5},{6},OK' -f $stamp,$memory.AvailableMBytes,[math]::Round(($memory.CommittedBytes / 1MB)),[math]::Round(($memory.CommitLimit / 1MB)),$bridges.Count,($details -join '|'),($nodes -join '|')
    } catch {
        $line = "$stamp,,,,,,,ERROR " + ($_.Exception.Message -replace '[,\r\n]',' ')
    }
    Add-Content -Path $file -Value $line -Encoding UTF8
    Start-Sleep -Seconds 2
}
