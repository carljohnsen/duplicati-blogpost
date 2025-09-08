if [ ! -d lib ]; then
  mkdir -p lib
  cd lib
  git clone https://github.com/duplicati/duplicati.git
  cd ../
fi
cd lib/duplicati
git checkout v2.1.0.120_canary_2025-06-24
dotnet build
if [ ! -d ../../data/testdata ]; then
  cd Tools/TestDataGenerator
  dotnet run -- create ../../../../data/testdata --max-file-size 104857600 --max-total-size 10737418240 --file-count 10000 --sparse-factor 30
  cd ../../
fi
dbpath=$(pwd)/benchmarking.sqlite
rm -rf ../../data/restore ../../data/backup $dbpath* Executables/net8/Duplicati.CommandLine/bin/Debug/net8.0/data/dbconfig.json 12*.log
duplicati_cli=Executables/net8/Duplicati.CommandLine/bin/Debug/net8.0/Duplicati.CommandLine
$duplicati_cli backup ../../data/backup --passphrase=1234 --log-file=120.log --log-file-log-level=Information --dbpath=$dbpath --dblock-size=1mb --blocksize=1kb ../../data/testdata --sqlite-page-cache=64mb
rm $dbpath*
$duplicati_cli repair ../../data/backup --passphrase=1234 --log-file=120.log --log-file-log-level=Information --sqlite-page-cache=64mb
$duplicati_cli restore ../../data/backup --passphrase=1234 --log-file=120.log --log-file-log-level=Information --restore-path=../../data/restore --restore-channel-buffer-size=1024 --restore-file-processors=8 --sqlite-page-cache=64mb
$duplicati_cli delete ../../data/backup --passphrase=1234 --log-file=120.log --log-file-log-level=Information --version=0 --allow-full-removal=true --sqlite-page-cache=64mb
sed -n -e '/has started/p' -e '/has completed/p' 120.log > ../../reports/120-summary.log
rm -rf ../../data/restore ../../data/backup $dbpath* Executables/net8/Duplicati.CommandLine/bin/Debug/net8.0/data/dbconfig.json

git checkout v2.1.0.125_canary_2025-07-15
dotnet build
$duplicati_cli backup ../../data/backup --passphrase=1234 --log-file=125.log --log-file-log-level=Information --dbpath=$dbpath --dblock-size=1mb --blocksize=1kb ../../data/testdata
rm $dbpath*
$duplicati_cli repair ../../data/backup --passphrase=1234 --log-file=125.log --log-file-log-level=Information
$duplicati_cli restore ../../data/backup --passphrase=1234 --log-file=125.log --log-file-log-level=Information --restore-path=../../data/restore --restore-channel-buffer-size=1024 --restore-file-processors=8
$duplicati_cli delete ../../data/backup --passphrase=1234 --log-file=125.log --log-file-log-level=Information --version=0 --allow-full-removal=true
sed -n -e '/has started/p' -e '/has completed/p' 125.log > ../../reports/125-summary.log
rm -rf ../../data/restore ../../data/backup $dbpath* Executables/net8/Duplicati.CommandLine/bin/Debug/net8.0/data/dbconfig.json
git checkout v2.1.0.120_canary_2025-06-24
cd ../../
