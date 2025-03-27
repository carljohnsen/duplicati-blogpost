mkdir data_repos
cd data_repos
git clone git@github.com:duplicati/duplicati.git duplicati
cd duplicati
git checkout 9dc8c0d
dotnet build -c Release
cd Tools/TestDataGenerator
dotnet build -c Release