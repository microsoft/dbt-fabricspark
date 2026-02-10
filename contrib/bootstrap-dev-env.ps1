<#

.SYNOPSIS
  Bootstraps a Windows Cloud DevBox with WSL pre-reqs.

.NOTES

  - The script uninstalls Docker Desktop as it interferes with WSL2.

#>

if (wsl -l -q | Select-String -SimpleMatch "Ubuntu-24.04") {
    Write-Host "Unregistering Ubuntu-24.04"
    wsl --unregister Ubuntu-24.04
}

$memGB=[math]::Floor((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory/1GB)
$cpu=[Environment]::ProcessorCount
$swap=[math]::Floor($memGB/4)
@"
[wsl2]
memory=${memGB}GB
processors=$cpu
swap=${swap}GB
networkingMode=NAT
"@ | Set-Content -Path "$env:USERPROFILE\.wslconfig"

Write-Host "Restarting WSL to apply settings"
wsl --shutdown

winget install -e --id Microsoft.GitCredentialManagerCore

Write-Host "Installing Ubuntu-24.04"
wsl --install -d Ubuntu-24.04