# Snowflake Connection Test Script
# This script loads environment variables and tests the dbt connection

# Load environment variables from .env file
if (Test-Path ".env") {
    Write-Host "Loading environment variables from .env file..." -ForegroundColor Yellow
    Get-Content .env | ForEach-Object {
        if ($_ -match "^([^#][^=]+)=(.*)$") {
            $name = $matches[1].Trim()
            $value = $matches[2].Trim()
            Write-Host "  Setting $name" -ForegroundColor Gray
            [Environment]::SetEnvironmentVariable($name, $value, "Process")
        }
    }
} else {
    Write-Host "ERROR: .env file not found! Please create it first." -ForegroundColor Red
    exit 1
}

# Load dbt function
. .\setup_dbt_alias.ps1

# Test connection
Write-Host "`nTesting Snowflake connection..." -ForegroundColor Cyan
Write-Host "==================================" -ForegroundColor Cyan

try {
    dbt debug
    if ($LASTEXITCODE -eq 0) {
        Write-Host "`nConnection successful!" -ForegroundColor Green
        Write-Host "You're ready to run dbt models!" -ForegroundColor Green
    } else {
        Write-Host "`nConnection failed. Please check your credentials." -ForegroundColor Red
    }
} catch {
    Write-Host "`nError running dbt debug: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host "`nNext steps:" -ForegroundColor Yellow
Write-Host "  1. If connection failed, update your .env file with correct credentials" -ForegroundColor White
Write-Host "  2. If successful, run: .\run_pipeline.ps1 -Target dev" -ForegroundColor White
Write-Host "  3. Generate documentation: dbt docs generate && dbt docs serve" -ForegroundColor White
