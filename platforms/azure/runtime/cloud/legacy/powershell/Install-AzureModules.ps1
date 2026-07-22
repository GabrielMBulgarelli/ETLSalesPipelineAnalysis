# Force TLS 1.2 
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

# Define required modules - only install what you need
$requiredModules = @(
    "Az.Accounts",  # Authentication and core functionality
    "Az.Synapse",   # For Synapse Analytics
    "Az.Storage"    # For Azure Storage
)

# Step 1: Check prerequisites
Write-Host "Checking prerequisites..." -ForegroundColor Yellow
$needsNuGet = $false
try {
    if (-not (Get-PackageProvider -Name NuGet -ErrorAction SilentlyContinue)) {
        $needsNuGet = $true
    }
} catch {
    $needsNuGet = $true
}

# Install NuGet if needed (much faster check)
if ($needsNuGet) {
    Write-Host "Installing NuGet provider..." -ForegroundColor Yellow
    Install-PackageProvider -Name NuGet -MinimumVersion 2.8.5.201 -Force -Scope CurrentUser
}

# Step 2: Trust PSGallery if not already trusted
$psGallery = Get-PSRepository -Name PSGallery -ErrorAction SilentlyContinue
if ($psGallery -and $psGallery.InstallationPolicy -ne "Trusted") {
    Write-Host "Setting PSGallery as trusted..." -ForegroundColor Yellow
    Set-PSRepository -Name PSGallery -InstallationPolicy Trusted
}

# Step 3: Install required modules (if not already installed)
Write-Host "Checking and installing required modules..." -ForegroundColor Yellow

foreach ($module in $requiredModules) {
    # Check if module is already installed
    if (Get-Module -Name $module -ListAvailable) {
        Write-Host "Module $module is already installed" -ForegroundColor Green
    } else {
        Write-Host "Installing $module..." -ForegroundColor Yellow
        Install-Module -Name $module -Force -Scope CurrentUser -Repository PSGallery
        Write-Host "Done" -ForegroundColor Green
    }
}

Write-Host "`nSimplified installation complete!" -ForegroundColor Green
Write-Host "Please restart PowerShell and run: .\scripts\setup\Test-AzureModules.ps1" -ForegroundColor Cyan