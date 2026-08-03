# Import the required module
if (-not (Get-Module -ListAvailable -Name Az.Synapse)) {
  Write-Host "Installing Az.Synapse module..."
  Install-Module -Name Az.Synapse -Force -AllowClobber -Scope CurrentUser
}

# Import the module to the current session
Import-Module -Name Az.Synapse -Force

# Check if logged in
$context = Get-AzContext
if (-not $context) {
  Write-Host "Not logged in. Please login to Azure."
  Connect-AzAccount
}

# Set variables
$subscriptionId = "19db60e7-2ba1-48f6-9df8-665cafa49340"
$resourceGroupName = "ecom-rg-main"
$workspaceName = "ecom-synapse"

# Set the subscription context
Set-AzContext -SubscriptionId $subscriptionId

# Get workspace details
try {
  $workspace = Get-AzSynapseWorkspace -ResourceGroupName $resourceGroupName -Name $workspaceName
  Write-Host "Workspace status: $($workspace.ProvisioningState)" -ForegroundColor Green
  Write-Host "Workspace details:"
  $workspace | Format-List
  
  # Get Spark pools
  Write-Host "Spark Pools:" -ForegroundColor Cyan
  $sparkPools = Get-AzSynapseSparkPool -ResourceGroupName $resourceGroupName -WorkspaceName $workspaceName
  if ($sparkPools) {
      $sparkPools | Format-Table
  } else {
      Write-Host "No Spark Pools found." -ForegroundColor Yellow
  }
  
  # Get SQL pools
  Write-Host "SQL Pools:" -ForegroundColor Cyan
  $sqlPools = Get-AzSynapseSqlPool -ResourceGroupName $resourceGroupName -WorkspaceName $workspaceName
  if ($sqlPools) {
      $sqlPools | Format-Table
  } else {
      Write-Host "No SQL Pools found." -ForegroundColor Yellow
  }
} 
catch {
  Write-Host "Error fetching workspace details: $($_.Exception.Message)" -ForegroundColor Red
  
  # Try to get more detailed error information
  Write-Host "Resource Group exists: $((Get-AzResourceGroup -Name $resourceGroupName -ErrorAction SilentlyContinue) -ne $null)" -ForegroundColor Yellow
  
  # List all Synapse workspaces in the subscription
  Write-Host "All Synapse workspaces in your subscription:" -ForegroundColor Yellow
  Get-AzSynapseWorkspace | Format-Table
}