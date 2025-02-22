cd runner
dotnet run -c Release -- -i 10 -s all
dotnet run -c Release -- -i 10 -s medium --operation sparsity
dotnet run -c Release -- -i 10 -s medium --operation filesizes