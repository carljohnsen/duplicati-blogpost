# Parse the first argument to the script
param([string]$mode)
if (!$mode) {
    Write-Host "No mode specified. Usage: $PSCommandPath [tool|auto]"
    exit 1
}

# Ensure script runs from the directory containing the 'data_duplicati' folder
$ErrorActionPreference = "Stop"

# Produce a final hash of each of the backup directories and check if they match
function Get-DoubleHashedDirectory {
    param (
        [Parameter(Mandatory = $true)]
        [string]$DirectoryPath
    )
    $tmpFile = "tmp_file_hashes.txt"

    # Find files, sort them for consistency, calculate MD5 of content
    Get-ChildItem -Path $DirectoryPath -File -Recurse | Sort-Object FullName | ForEach-Object { (Get-FileHash $_.FullName -Algorithm MD5).Hash } | Out-File $tmpFile

    # Hash the list of hashes (the "Double Hash")
    $finalHash = (Get-FileHash $tmpFile -Algorithm MD5).Hash

    # Cleanup temp file
    Remove-Item -Force $tmpFile

    return $finalHash
}

Write-Host "Generating test data..."
New-Item -ItemType Directory -Force data_raw

# Check if the data_raw exists and is not empty, to avoid regenerating test data on every run
if (Test-Path "data_raw" -and (Get-ChildItem "data_raw" | Measure-Object).Count -gt 0) {
    Write-Host "data_raw already exists and is not empty. Skipping test data generation."
} else {
    .\data_duplicati\Tools\TestDataGenerator\bin\Debug\net10.0\TestDataGenerator.exe create .\data_raw\ --file-count 1000 --max-file-size 1048576 --max-total-size 104857600 --sparse-factor 30
}

Write-Host "Performing Backup..."
$rsyncArg = if ($mode -eq "tool") { "--remote-sync-json-config=./sync_config.json" } else { "" }
New-Item -ItemType Directory -Force data_backup1
New-Item -ItemType Directory -Force data_backup2
.\data_duplicati\Executables\Duplicati.CommandLine\bin\Debug\net10.0\Duplicati.CommandLine.exe backup file://data_backup1 .\data_raw\ --passphrase=1234 $rsyncArg

if ($mode -eq "tool") {
    Write-Host "Performing Sync..."
    .\data_duplicati\Executables\Duplicati.CommandLine.SyncTool\bin\Debug\net10.0\Duplicati.CommandLine.SyncTool.exe file://data_backup1 file://data_backup2 --confirm --progress
}

Write-Host "Performing Restore 1..."
New-Item -ItemType Directory -Force data_restore1
.\data_duplicati\Executables\Duplicati.CommandLine\bin\Debug\net10.0\Duplicati.CommandLine.exe restore file://data_backup1 --restore-path=.\data_restore1\ --passphrase=1234

Write-Host "Performing Restore 2..."
New-Item -ItemType Directory -Force data_restore2
.\data_duplicati\Executables\Duplicati.CommandLine\bin\Debug\net10.0\Duplicati.CommandLine.exe restore file://data_backup2 --restore-path=.\data_restore2\ --passphrase=1234

Write-Host "Verifying hashes..."

$hash_backup1 = Get-DoubleHashedDirectory -DirectoryPath .\data_backup1\
$hash_backup2 = Get-DoubleHashedDirectory -DirectoryPath .\data_backup2\

# Store the success flag
$verification = $true
if ($hash_backup1 -eq $hash_backup2) {
    Write-Host "Backups match!"
} else {
    Write-Host "Backups do NOT match!"
    $verification = $false
}

$hash_raw = Get-DoubleHashedDirectory -DirectoryPath .\data_raw\
$hash_restore1 = Get-DoubleHashedDirectory -DirectoryPath .\data_restore1\
$hash_restore2 = Get-DoubleHashedDirectory -DirectoryPath .\data_restore2\

if ($hash_raw -eq $hash_restore1) {
    Write-Host "Restore 1 matches raw data!"
} else {
    Write-Host "Restore 1 does NOT match raw data!"
    $verification = $false
}

if ($hash_raw -eq $hash_restore2) {
    Write-Host "Restore 2 matches raw data!"
} else {
    Write-Host "Restore 2 does NOT match raw data!"
    $verification = $false
}

Write-Host "Cleaning up..."

.\data_duplicati\Executables\Duplicati.CommandLine\bin\Debug\net10.0\Duplicati.CommandLine.exe delete file://data_backup1 --passphrase=1234 --version=0 --allow-full-removal=true

Remove-Item -Path .\data_raw\ -Recurse -Force
Remove-Item -Path .\data_backup1\ -Recurse -Force
Remove-Item -Path .\data_backup2\ -Recurse -Force
Remove-Item -Path .\data_restore1\ -Recurse -Force
Remove-Item -Path .\data_restore2\ -Recurse -Force

if ($verification) {
    Write-Host "All verifications passed!"
} else {
    Write-Host "Some verifications failed!"
}

Write-Host "Script finished."