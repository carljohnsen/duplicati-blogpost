# Check that we've received two arguments
if ($args.Length -ne 2) {
    Write-Host "Usage: perform_runs.ps1 <path-to-Duplicati.CommandLine.exe> <path-to-TestDataGenerator.exe"
    exit
}

# Check that the arguments are valid paths
if (-not (Test-Path $args[0] -PathType Leaf) -or -not (Test-Path $args[1] -PathType Leaf)) {
    Write-Host "Both arguments must be valid paths to files"
    Write-Host "Usage: perform_runs.ps1 <path-to-Duplicati.CommandLine.exe> <path-to-TestDataGenerator.exe"
    exit
}

# Check that the arguments have the right names
if (-not ($args[0] -like "*Duplicati.CommandLine.exe") -or -not ($args[1] -like "*TestDataGenerator.exe")) {
    Write-Host "Incorrect executeable names"
    Write-Host "Usage: perform_runs.ps1 <path-to-Duplicati.CommandLine.exe> <path-to-TestDataGenerator.exe"
    exit
}

# Check if the data directories exist
if (-not (Test-Path data_small)) {
    & $args[1] create $PWD\data_small --max-file-size 10485760 --max-total-size 1073741824 --file-count 10000
}
if (-not (Test-Path data_large)) {
    & $args[1] create $PWD\data_large --max-file-size 1073741824 --max-total-size 10737418240 --min-file-size 10485760 --file-count 1000
}

# Create a list of names
$fs = @("data_small", "data_large")
$warmup = 1
$runs = 10
$DUPLICATI_BACKUP_DIR = "data_duplicatidir"

# Create the directories
mkdir -Force $DUPLICATI_BACKUP_DIR
mkdir -Force times

foreach ($f in $fs) {
    for ($intensity = 1; $intensity -le 10; $intensity++) {
        for ($i = 1; $i -le $warmup; $i++) {
            & $args[0] backup $DUPLICATI_BACKUP_DIR --passphrase=a $f --cpu-intensity=$intensity
            & $args[0] delete $DUPLICATI_BACKUP_DIR --passphrase=a --allow-full-removal --version=0
        }
        for ($i = 1; $i -le $runs; $i++) {
            $sw = [System.Diagnostics.Stopwatch]::StartNew()
            $p = Start-Process -PassThru -NoNewWindow -FilePath $args[0] -ArgumentList "backup $DUPLICATI_BACKUP_DIR --passphrase=a $f --cpu-intensity=$intensity"
            $process = Get-Process -Id $p.Id
            Wait-Process -Id $p.Id
            $sw.Stop()
            $realTime = $sw.Elapsed
            $userTime = $process.UserProcessorTime
            $sysTime = $process.PrivilegedProcessorTime
            & $args[0] delete $DUPLICATI_BACKUP_DIR --passphrase=a --allow-full-removal --version=0

            # Output the times to a file
            write-output "real $($realTime.TotalSeconds) user $($userTime.TotalSeconds) sys $($sysTime.TotalSeconds)`n" | Out-File -Append -FilePath "times\win-$f$intensity.time"
        }
    }
}