# PowerShell function for dbt command
# Add this to your PowerShell profile or run it in your current session

function dbt {
    & "C:\Users\2084732\AppData\Roaming\Python\Python313\Scripts\dbt.exe" @args
}

Write-Host "✅ dbt function created! You can now use 'dbt' command directly." -ForegroundColor Green
Write-Host "💡 Run 'dbt --version' to verify the installation." -ForegroundColor Yellow
