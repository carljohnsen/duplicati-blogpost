mkdir data_repos
cd data_repos
git clone git@github.com:duplicati/duplicati.git duplicati
cd duplicati
git checkout 354a804
dotnet build -c Release
cd Tools/TestDataGenerator
dotnet build -c Release