git clone git@github.com:duplicati/duplicati.git data_duplicati
cd data_duplicati
git checkout master # TODO: switch to release with rsync tool when available
dotnet build
cd Tools/TestDataGenerator
dotnet build
