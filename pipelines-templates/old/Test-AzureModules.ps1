# Check Azure modules and connection status
$WarningPreference = 'SilentlyContinue'
$ErrorActionPreference = 'Stop'

Write-Host "Verifying Azure modules..." -ForegroundColor Yellow

try {
    # Check for required modules
    $requiredModules = @("Az.Accounts", "Az.Synapse", "Az.Storage")
    $modulesPresent = $true
    
    foreach ($module in $requiredModules) {
        if (Get-Module -Name $module -ListAvailable) {
            Write-Host "$module is available" -ForegroundColor Green
        } else {
            Write-Host "$module is NOT available" -ForegroundColor Red
            $modulesPresent = $false
        }
    }
    
    # Stop if modules are missing
    if (-not $modulesPresent) {
        throw "Missing required modules. Please install them first."
    }
    
    # Check login status
    Write-Host "`nChecking Azure login status..." -ForegroundColor Yellow
    $context = Get-AzContext -ErrorAction SilentlyContinue
    
    if ($context) {
        Write-Host "Logged in as: $($context.Account)" -ForegroundColor Green
        Write-Host "Subscription: $($context.Subscription.Name)" -ForegroundColor Green
    } else {
        Write-Host "Not logged in. Connecting to Azure..." -ForegroundColor Yellow
        Connect-AzAccount
    }
    
    # List workspaces only if requested
    $checkWorkspaces = $false
    
    if ($checkWorkspaces) {
        Write-Host "`nListing Synapse workspaces..." -ForegroundColor Yellow
        Get-AzSynapseWorkspace | Format-Table Name, ResourceGroupName, Location
    }
    
    Write-Host "`nVerification complete!" -ForegroundColor Green
} 
catch {
    Write-Host "Error occurred: $($_.Exception.Message)" -ForegroundColor Red
}
