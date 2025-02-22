cd /path/to/where/you/want/the/repos
git clone git@github.com:duplicati/duplicati.git duplicati
cd duplicati
git checkout 5aa06f3
dotnet build -c Release
cd Tools/TestDataGenerator
dotnet build -c Release