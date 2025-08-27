if [ ! -d lib ]; then
  mkdir -p lib
  cd lib
  git clone https://github.com/duplicati/duplicati.git
  cd ../
fi
cd lib/duplicati
git checkout v2.1.0.120_canary_2025-06-24
dotnet build -c Release
if [ ! -d ../../data/testdata ]; then
  cd Tools/TestDataGenerator
  dotnet run -c Release -- create ../../../../data/testdata --max-file-size 104857600 --max-total-size 10737418240 --file-count 10000 --sparse-factor 30
  cd ../../
fi
dbpath=benchmarking.sqlite
duplicati_cli=Executables/net8/Duplicati.CommandLine/bin/Release/net8.0/Duplicati.CommandLine
$duplicati_cli backup ../../data/backup --passphrase=1234 --log-file=120-backup.log --dbpath=$dbpath --dblock-size=1mb --blocksize=1kb ../../data/testdata
rm $dbpath
$duplicati_cli repair ../../data/backup --passphrase=1234 --log-file=120-repair.log
$duplicati_cli restore ../../data/backup --passphrase=1234 --log-file=120-restore.log --restore-path=../../data/restore
$duplicati_cli delete ../../data/backup --passphrase=1234 --log-file=120-delete.log --version=0 --allow-full-removal=true
rm -rf ../../data/restore ../../data/backup
git checkout v2.1.0.125_canary_2025-07-15
dotnet build -c Release
$duplicati_cli backup ../../data/backup --passphrase=1234 --log-file=125-backup.log --dbpath=$dbpath --dblock-size=1mb --blocksize=1kb ../../data/testdata
rm $dbpath
$duplicati_cli repair ../../data/backup --passphrase=1234 --log-file=125-repair.log
$duplicati_cli restore ../../data/backup --passphrase=1234 --log-file=125-restore.log --restore-path=../../data/restore
$duplicati_cli delete ../../data/backup --passphrase=1234 --log-file=125-delete.log --version=0 --allow-full-removal=true
rm -rf ../../data/restore ../../data/backup
git checkout v2.1.0.120_canary_2025-06-24
cd ../../