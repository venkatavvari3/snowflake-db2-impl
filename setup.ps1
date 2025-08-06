# cap111 UK Banking Analytics - Setup Script
# This script helps set up the dbt project for development

Write-Host "cap111 UK Banking Analytics - dbt Setup" -ForegroundColor Green
Write-Host "================================================" -ForegroundColor Green

# Check if Python is installed
Write-Host "`nChecking Python installation..." -ForegroundColor Yellow
try {
    $pythonVersion = python --version 2>&1
    Write-Host "✓ Python found: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "✗ Python not found. Please install Python 3.8+ and add it to PATH." -ForegroundColor Red
    exit 1
}

# Create virtual environment if it doesn't exist
if (-not (Test-Path "venv")) {
    Write-Host "`nCreating virtual environment..." -ForegroundColor Yellow
    python -m venv venv
    Write-Host "✓ Virtual environment created" -ForegroundColor Green
} else {
    Write-Host "`n✓ Virtual environment already exists" -ForegroundColor Green
}

# Activate virtual environment
Write-Host "`nActivating virtual environment..." -ForegroundColor Yellow
& "venv\Scripts\Activate.ps1"

# Install requirements
Write-Host "`nInstalling Python dependencies..." -ForegroundColor Yellow
pip install -r requirements.txt
Write-Host "✓ Dependencies installed" -ForegroundColor Green

# Install dbt packages
Write-Host "`nInstalling dbt packages..." -ForegroundColor Yellow
dbt deps
Write-Host "✓ dbt packages installed" -ForegroundColor Green

# Check if .env file exists
if (-not (Test-Path ".env")) {
    Write-Host "`nCreating .env file from template..." -ForegroundColor Yellow
    Copy-Item ".env.example" ".env"
    Write-Host "✓ .env file created" -ForegroundColor Green
    Write-Host "⚠️  Please edit .env file with your Snowflake credentials" -ForegroundColor Yellow
} else {
    Write-Host "`n✓ .env file already exists" -ForegroundColor Green
}

Write-Host "`n" -ForegroundColor Green
Write-Host "Setup Complete!" -ForegroundColor Green
Write-Host "==============" -ForegroundColor Green
Write-Host "Next steps:" -ForegroundColor White
Write-Host "1. Edit .env file with your Snowflake credentials" -ForegroundColor White
Write-Host "2. Run 'dbt debug' to test connection" -ForegroundColor White
Write-Host "3. Run 'dbt run' to execute the pipeline" -ForegroundColor White
Write-Host "4. Run 'dbt docs generate && dbt docs serve' for documentation" -ForegroundColor White
