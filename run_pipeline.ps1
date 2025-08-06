# cap111 Banking Analytics Pipeline Orchestration
# This script runs the complete ELT pipeline in the correct order

param(
    [string]$Target = "dev",
    [switch]$FullRefresh,
    [switch]$TestOnly,
    [switch]$DocsOnly
)

Write-Host "cap111 Banking Analytics Pipeline" -ForegroundColor Green
Write-Host "=====================================" -ForegroundColor Green
Write-Host "Target Environment: $Target" -ForegroundColor Yellow

# Activate virtual environment
if (Test-Path "venv\Scripts\Activate.ps1") {
    & "venv\Scripts\Activate.ps1"
}

# Function to run dbt command with error handling
function Invoke-DbtCommand {
    param($Command, $Description)
    
    Write-Host "`n$Description..." -ForegroundColor Yellow
    Write-Host "Running: $Command" -ForegroundColor Gray
    
    Invoke-Expression $Command
    if ($LASTEXITCODE -ne 0) {
        Write-Host "✗ $Description failed" -ForegroundColor Red
        exit $LASTEXITCODE
    }
    Write-Host "✓ $Description completed successfully" -ForegroundColor Green
}

# Test connection first
Invoke-DbtCommand "dbt debug --target $Target" "Testing Snowflake connection"

if ($DocsOnly) {
    # Only generate and serve documentation
    Invoke-DbtCommand "dbt docs generate --target $Target" "Generating documentation"
    Write-Host "`nStarting documentation server..." -ForegroundColor Yellow
    dbt docs serve
    exit 0
}

if ($TestOnly) {
    # Only run tests
    Invoke-DbtCommand "dbt test --target $Target" "Running data quality tests"
    exit 0
}

# Run snapshots first (for SCD Type 2 tracking)
Write-Host "`n=== SNAPSHOTS ===" -ForegroundColor Cyan
Invoke-DbtCommand "dbt snapshot --target $Target" "Creating/updating snapshots"

# Run staging models
Write-Host "`n=== STAGING LAYER ===" -ForegroundColor Cyan
if ($FullRefresh) {
    Invoke-DbtCommand "dbt run --models staging --target $Target --full-refresh" "Running staging models (full refresh)"
} else {
    Invoke-DbtCommand "dbt run --models staging --target $Target" "Running staging models"
}

# Test staging models
Invoke-DbtCommand "dbt test --models staging --target $Target" "Testing staging models"

# Run core marts
Write-Host "`n=== CORE MARTS ===" -ForegroundColor Cyan
if ($FullRefresh) {
    Invoke-DbtCommand "dbt run --models marts.core --target $Target --full-refresh" "Running core mart models (full refresh)"
} else {
    Invoke-DbtCommand "dbt run --models marts.core --target $Target" "Running core mart models"
}

# Test core marts
Invoke-DbtCommand "dbt test --models marts.core --target $Target" "Testing core mart models"

# Run specialized marts
Write-Host "`n=== SPECIALIZED MARTS ===" -ForegroundColor Cyan
$MartAreas = @("finance", "risk", "customer")

foreach ($Area in $MartAreas) {
    Write-Host "`nProcessing $Area marts..." -ForegroundColor Magenta
    
    if ($FullRefresh) {
        Invoke-DbtCommand "dbt run --models marts.$Area --target $Target --full-refresh" "Running $Area mart models (full refresh)"
    } else {
        Invoke-DbtCommand "dbt run --models marts.$Area --target $Target" "Running $Area mart models"
    }
    
    Invoke-DbtCommand "dbt test --models marts.$Area --target $Target" "Testing $Area mart models"
}

# Final comprehensive test
Write-Host "`n=== FINAL VALIDATION ===" -ForegroundColor Cyan
Invoke-DbtCommand "dbt test --target $Target" "Running all data quality tests"

# Generate documentation
Write-Host "`n=== DOCUMENTATION ===" -ForegroundColor Cyan
Invoke-DbtCommand "dbt docs generate --target $Target" "Generating documentation"

# Pipeline summary
Write-Host "`n" -ForegroundColor Green
Write-Host "🎉 PIPELINE COMPLETED SUCCESSFULLY! 🎉" -ForegroundColor Green
Write-Host "====================================" -ForegroundColor Green
Write-Host "Target Environment: $Target" -ForegroundColor White
Write-Host "Models processed:" -ForegroundColor White
Write-Host "  - Staging models (data cleaning)" -ForegroundColor White
Write-Host "  - Core marts (customer dimension, transaction facts)" -ForegroundColor White
Write-Host "  - Finance marts (KPIs and metrics)" -ForegroundColor White
Write-Host "  - Risk marts (credit risk profiles)" -ForegroundColor White
Write-Host "  - Customer marts (analytics and insights)" -ForegroundColor White
Write-Host "`nNext steps:" -ForegroundColor Yellow
Write-Host "  - Review documentation: dbt docs serve" -ForegroundColor White
Write-Host "  - Check Snowflake for processed data" -ForegroundColor White
Write-Host "  - Connect BI tools to mart schemas" -ForegroundColor White
