param(
    [string]$DUPLICATI,
    [string]$DATAGEN,
    [string]$SIZES
)

$N_WARMUP = 1
$N_RUNS = 1

switch ($SIZES) {
    "small" {
        $SIZES = @("small")
        $MAX_FILE_SIZE = 10485760  # 10 MB
        $MAX_TOTAL_SIZE = 1073741824  # 1 GB
        $FILE_COUNT = 1000  # 1,000 files
    }
    "medium" {
        $SIZES = @("medium")
        $MAX_FILE_SIZE = 10485760  # 10 MB
        $MAX_TOTAL_SIZE = 10737418240  # 10 GB
        $FILE_COUNT = 10000  # 10,000 files
    }
    "large" {
        $SIZES = @("large")
        $MAX_FILE_SIZE = 104857600  # 100 MB
        $MAX_TOTAL_SIZE = 107374182400  # 100 GB
        $FILE_COUNT = 1000000  # 1,000,000 files
    }
    "all" {
        $SIZES = @("small", "medium", "large")
    }
    default {
        Write-Host "Invalid size specifier: $SIZES"
        Write-Host "Usage: script.ps1 <DUPLICATI> <DATAGEN> <small|medium|large>"
        exit 1
    }
}

$DATA_DIR = Join-Path (Get-Location) "data"
$TIMES_DIR = Join-Path (Get-Location) "times"
New-Item -ItemType Directory -Path $DATA_DIR, $TIMES_DIR -Force | Out-Null

# Helper function to measure command execution time
function Measure-CommandExecution {
    param(
        [string]$Command,
        [string[]]$Arguments
    )

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $p = Start-Process -PassThru -NoNewWindow -FilePath $Command -ArgumentList $Arguments
    $process = Get-Process -Id $p.Id
    Wait-Process -Id $p.Id
    $sw.Stop()

    $realTime = $sw.Elapsed
    $userTime = $process.UserProcessorTime
    $sysTime = $process.PrivilegedProcessorTime

    # Create the output string
    $outputString = "real $($realTime.TotalSeconds) user $($userTime.TotalSeconds) sys $($sysTime.TotalSeconds)`n"

    return $outputString
}

function Blocking-Remove {
    param(
        [string]$Path
    )

    while (Test-Path $Path) {
        try {
            Write-Host "Removing $Path..."
            Remove-Item -Recurse -Force $Path
        }
        catch {
            Write-Host "Failed to remove $Path, retrying..."
        }
    }
}

foreach ($SIZE in $SIZES) {
    $SIZE_PATH = Join-Path $DATA_DIR $SIZE
    if (!(Test-Path $SIZE_PATH)) {
        Write-Host "Generating data..."
        & $DATAGEN create $SIZE_PATH --max-file-size $MAX_FILE_SIZE --max-total-size $MAX_TOTAL_SIZE --file-count $FILE_COUNT
    }

    $BACKUP_DIR = Join-Path $DATA_DIR "backup_$SIZE"
    & $DUPLICATI backup $BACKUP_DIR $SIZE_PATH --passphrase=1234
    $RESTORE_DIR = Join-Path $DATA_DIR "restore_$SIZE"

    foreach ($LEGACY in @("true", "false")) {
        for ($i = 1; $i -le $N_WARMUP; $i++) {
            Blocking-Remove $RESTORE_DIR\*
            Measure-CommandExecution -Command $DUPLICATI -Arguments @( "restore", $BACKUP_DIR, "--restore-path=$RESTORE_DIR", "--passphrase=1234", "--restore-legacy=$LEGACY" )
        }
        exit

        $TIMESFILE = Join-Path $TIMES_DIR "$env:COMPUTERNAME-$SIZE-full-$LEGACY.time"
        for ($i = 1; $i -le $N_RUNS; $i++) {
            Remove-Item -Recurse -Force "$RESTORE_DIR\*" -ErrorAction SilentlyContinue
            $measured_time = Measure-CommandExecution -Command $DUPLICATI -Arguments @( "restore", $BACKUP_DIR, "--restore-path=$RESTORE_DIR", "--passphrase=1234", "--restore-legacy=$LEGACY" )
            $measured_time | Out-File -Append -Encoding utf8 $TIMESFILE
        }

        # Partial Restore
        $FILES = Get-ChildItem -Path $SIZE_PATH -File | Get-Random -Count ($FILE_COUNT / 2)
        $FILES_TO_DELETE = $FILES | Where-Object { ($FILES.IndexOf($_) % 2) -eq 0 }
        $FILES_TO_TOUCH = $FILES | Where-Object { ($FILES.IndexOf($_) % 2) -eq 1 }

        for ($i = 1; $i -le $N_WARMUP; $i++) {
            $FILES_TO_DELETE | ForEach-Object { Remove-Item $_.FullName -Force }
            $FILES_TO_TOUCH | ForEach-Object { Set-Content $_.FullName -Value "a" -NoNewline }
            Measure-CommandExecution -Command $DUPLICATI -Arguments @( "restore", $BACKUP_DIR, "--restore-path=$RESTORE_DIR", "--passphrase=1234", "--restore-legacy=$LEGACY", "--restore-target=/subdir" )
        }

        $TIMESFILE = Join-Path $TIMES_DIR "$env:COMPUTERNAME-$SIZE-partial-$LEGACY.time"
        for ($i = 1; $i -le $N_RUNS; $i++) {
            $FILES_TO_DELETE | ForEach-Object { Remove-Item $_.FullName -Force }
            $FILES_TO_TOUCH | ForEach-Object { Set-Content $_.FullName -Value "a" -NoNewline }
            $measured_time = Measure-CommandExecution -Command $DUPLICATI -Arguments @( "restore", $BACKUP_DIR, "--restore-path=$RESTORE_DIR", "--passphrase=1234", "--restore-legacy=$LEGACY", "--restore-target=/subdir" )
            $measured_time | Out-File -Append -Encoding utf8 $TIMESFILE
        }

        # No Restore
        & $DUPLICATI restore $BACKUP_DIR --restore-path=$RESTORE_DIR --passphrase=1234 --restore-legacy=$LEGACY
        for ($i = 1; $i -le $N_WARMUP; $i++) {
            Measure-CommandExecution -Command $DUPLICATI -Arguments @( "restore", $BACKUP_DIR, "--restore-path=$RESTORE_DIR", "--passphrase=1234", "--restore-legacy=$LEGACY" )
        }
        $TIMESFILE = Join-Path $TIMES_DIR "$env:COMPUTERNAME-$SIZE-norestore-$LEGACY.time"
        for ($i = 1; $i -le $N_RUNS; $i++) {
            $measured_time = Measure-CommandExecution -Command $DUPLICATI -Arguments @( "restore", $BACKUP_DIR, "--restore-path=$RESTORE_DIR", "--passphrase=1234", "--restore-legacy=$LEGACY" )
            $measured_time | Out-File -Append -Encoding utf8 $TIMESFILE
        }

        # Metadata Restore
        for ($i = 1; $i -le $N_WARMUP; $i++) {
            Get-ChildItem -Path $RESTORE_DIR -File | ForEach-Object { Set-ItemProperty -Path $_.FullName -Name LastWriteTime -Value (Get-Date) }
            Measure-CommandExecution -Command $DUPLICATI -Arguments @( "restore", $BACKUP_DIR, "--restore-path=$RESTORE_DIR", "--passphrase=1234", "--restore-legacy=$LEGACY" )
        }

        $TIMESFILE = Join-Path $TIMES_DIR "$env:COMPUTERNAME-$SIZE-metadata-$LEGACY.time"
        for ($i = 1; $i -le $N_RUNS; $i++) {
            Get-ChildItem -Path $RESTORE_DIR -File | ForEach-Object { Set-ItemProperty -Path $_.FullName -Name LastWriteTime -Value (Get-Date) }
            $measured_time = Measure-CommandExecution -Command $DUPLICATI -Arguments @( "restore", $BACKUP_DIR, "--restore-path=$RESTORE_DIR", "--passphrase=1234", "--restore-legacy=$LEGACY" )
            $measured_time | Out-File -Append -Encoding utf8 $TIMESFILE
        }
    }
}
