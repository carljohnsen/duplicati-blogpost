# Generate some data
mkdir data_raw
.\data_duplicati\Tools\TestDataGenerator\bin\Debug\net10.0\TestDataGenerator.exe create .\data_raw\ --file-count 10000 --max-file-size 104857600 --max-total-size 1073741824 --sparse-factor 30

# Perform a backup to a local folder
mkdir data_backup1
.\data_duplicati\Executables\Duplicati.CommandLine\bin\Debug\net10.0\Duplicati.CommandLine.exe backup file://data_backup1 .\data_raw\ --passphrase=1234

# Perform a restore
mkdir data_restore1
.\data_duplicati\Executables\Duplicati.CommandLine\bin\Debug\net10.0\Duplicati.CommandLine.exe restore file://data_backup1 --restore-path=.\data_restore1\ --passphrase=1234

# Rsync to a different local folder
mkdir data_backup2
.\data_duplicati\Executables\Duplicati.CommandLine.SyncTool\bin\Debug\net10.0\Duplicati.CommandLine.SyncTool.exe file://data_backup1 file://data_backup2 --confirm --progress

# Perform a restore from the second backup
mkdir data_restore2
.\data_duplicati\Executables\Duplicati.CommandLine\bin\Debug\net10.0\Duplicati.CommandLine.exe restore file://data_backup2 --restore-path=.\data_restore2\ --passphrase=1234

# Produce a final hash of each of the backup directories and check if they match
function Get-DoubleHashedDirectory {
    param (
        [Parameter(Mandatory = $true)]
        [string]$DirectoryPath
    )

    # Find files, sort them for consistency, calculate MD5 of content
    $fileHashes = Get-ChildItem -Path $DirectoryPath -File -Recurse | Sort-Object FullName | Get-FileHash -Algorithm MD5

    # Convert the hash array to a string (with Out-String to format output)
    $hashString = $fileHashes.Hash | Out-String

    # Compute the MD5 hash of the hash string
    $result = Get-FileHash -InputStream ([IO.MemoryStream]::new([char[]]$hashString)) -Algorithm MD5

    # Return the final hash
    return $result.Hash
}
$hash_backup1 = Get-DoubleHashedDirectory -DirectoryPath .\data_backup1\
$hash_backup2 = Get-DoubleHashedDirectory -DirectoryPath .\data_backup2\
if ($hash_backup1 -eq $hash_backup2) { Write-Host "Backups match!" } else { Write-Host "Backups do NOT match!" }

$hash_raw = Get-DoubleHashedDirectory -DirectoryPath .\data_raw\
$hash_restore1 = Get-DoubleHashedDirectory -DirectoryPath .\data_restore1\
$hash_restore2 = Get-DoubleHashedDirectory -DirectoryPath .\data_restore2\
if ($hash_raw -eq $hash_restore1) { Write-Host "Restore 1 matches raw data!" } else { Write-Host "Restore 1 does NOT match raw data!" }
if ($hash_raw -eq $hash_restore2) { Write-Host "Restore 2 matches raw data!" } else { Write-Host "Restore 2 does NOT match raw data!" }

# Cleanup
.\data_duplicati\Executables\Duplicati.CommandLine\bin\Debug\net10.0\Duplicati.CommandLine.exe delete file://data_backup1 --passphrase=1234 --version=0 --allow-full-removal=true
Remove-Item -Path .\data_raw\ -Recurse
Remove-Item -Path .\data_backup1\ -Recurse
Remove-Item -Path .\data_backup2\ -Recurse
Remove-Item -Path .\data_restore1\ -Recurse
Remove-Item -Path .\data_restore2\ -Recurse