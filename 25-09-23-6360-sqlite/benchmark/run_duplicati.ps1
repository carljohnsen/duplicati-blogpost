# Ensure lib/duplicati exists
if (!(Test-Path "lib\duplicati")) {
    New-Item -ItemType Directory -Path "lib" -Force | Out-Null
    Set-Location "lib"
    git clone https://github.com/duplicati/duplicati.git
    Set-Location ".."
}

Set-Location "lib\duplicati"
git checkout v2.1.0.120_canary_2025-06-24
dotnet build -c Release

if (!(Test-Path "..\..\data\testdata")) {
    Set-Location "Tools\TestDataGenerator"
    dotnet run -c Release -- create "..\..\..\..\data\testdata" --max-file-size 104857600 --max-total-size 10737418240 --file-count 10000 --sparse-factor 30
    Set-Location "..\.."
}

$dbpath = (Get-Location).Path + "\benchmarking.sqlite"
Remove-Item -Recurse -Force "..\..\data\restore", "..\..\data\backup", "$dbpath*", "$env:LocalAppData\Duplicati", "12*.log" -ErrorAction SilentlyContinue

$duplicati_cli = "Executables\net8\Duplicati.CommandLine\bin\Release\net8.0\Duplicati.CommandLine.exe"

& $duplicati_cli backup "..\..\data\backup" --passphrase=1234 --log-file=120.log --log-file-log-level=Information --dbpath=$dbpath --dblock-size=1mb --blocksize=1kb "..\..\data\testdata" --sqlite-page-cache=64mb
Remove-Item "$dbpath*" -ErrorAction SilentlyContinue
& $duplicati_cli repair "..\..\data\backup" --passphrase=1234 --log-file=120.log --log-file-log-level=Information --sqlite-page-cache=64mb
& $duplicati_cli restore "..\..\data\backup" --passphrase=1234 --log-file=120.log --log-file-log-level=Information --restore-path="..\..\data\restore" --restore-channel-buffer-size=1024 --restore-file-processors=8 --sqlite-page-cache=64mb
& $duplicati_cli delete "..\..\data\backup" --passphrase=1234 --log-file=120.log --log-file-log-level=Information --version=0 --allow-full-removal=true --sqlite-page-cache=64mb
Select-String -Path 120.log -Pattern 'has started', 'has completed' | Set-Content ../../reports/120-summary.log
Remove-Item -Recurse -Force "..\..\data\restore", "..\..\data\backup", "$dbpath*", "$env:LocalAppData\Duplicati" -ErrorAction SilentlyContinue

git checkout v2.1.0.125_canary_2025-07-15
dotnet build -c Release
& $duplicati_cli backup "..\..\data\backup" --passphrase=1234 --log-file=125.log --log-file-log-level=Information --dbpath=$dbpath --dblock-size=1mb --blocksize=1kb "..\..\data\testdata"
Remove-Item "$dbpath*" -ErrorAction SilentlyContinue
& $duplicati_cli repair "..\..\data\backup" --passphrase=1234 --log-file=125.log --log-file-log-level=Information
& $duplicati_cli restore "..\..\data\backup" --passphrase=1234 --log-file=125.log --log-file-log-level=Information --restore-path="..\..\data\restore" --restore-channel-buffer-size=1024 --restore-file-processors=8
& $duplicati_cli delete "..\..\data\backup" --passphrase=1234 --log-file=125.log --log-file-log-level=Information --version=0 --allow-full-removal=true
Select-String -Path 125.log -Pattern 'has started', 'has completed' | Set-Content ../../reports/125-summary.log
Remove-Item -Recurse -Force "..\..\data\restore", "..\..\data\backup", "$dbpath*", "$env:LocalAppData\Duplicati" -ErrorAction SilentlyContinue

git checkout v2.1.0.120_canary_2025-06-24
Set-Location "..\.."
