mkdir data_repos
cd data_repos
git clone git@github.com:duplicati/duplicati.git duplicati
cp -r duplicati duplicati_testdata
cd duplicati
git checkout d33fa0caf
dotnet build -c Release
cd ../duplicati_testdata
git checkout df76a77
cd Tools/TestDataGenerator
dotnet build -c Release