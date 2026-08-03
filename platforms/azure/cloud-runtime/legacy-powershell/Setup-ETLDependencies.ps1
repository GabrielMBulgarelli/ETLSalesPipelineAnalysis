# Main script to run all Azure setup and verification scripts
Write-Host "ETL Sales Pipeline Analysis - Azure Setup" -ForegroundColor Cyan
Write-Host "---------------------------------------" -ForegroundColor Cyan

# Step 1: Install required modules
Write-Host "`n[1/2] Installing required Azure modules" -ForegroundColor Yellow
& "$PSScriptRoot.\Install-AzureModules.ps1"

# Step 3: Check Azure Synapse workspace
Write-Host "`n[2/2] Checking Synapse workspace" -ForegroundColor Yellow
& "$PSScriptRoot.\Check-AzureWorkspace.ps1"

Write-Host "`nAll scripts completed!" -ForegroundColor Green