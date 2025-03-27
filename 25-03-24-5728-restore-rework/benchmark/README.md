# Restore rework benchmark

# Prerequisites

To run this benchmark, a .NET 8 installation is required and has to be available in the `PATH`:

```sh
dotnet --version
```

Furthermore, to clone the repositories, `git` is required:

```sh
git --version
```

## A Duplicati installation

To run the benchmark, we need to have a clone of the Duplicati repo. To use the exact version used in this benchmark, run the following commands (included in the `setup.sh` script, which creates the `data_repos` directory and clones the repository):

```sh
mkdir data_repos
cd data_repos
git clone git@github.com:duplicati/duplicati.git duplicati
cd duplicati
git checkout 9dc8c0d
dotnet build -c Release
cd Tools/TestDataGenerator
dotnet build -c Release
```

# Running the benchmark

The benchmark itself is written in C#. To run the benchmark, go to the `runner` directory and run the .NET program. E.g. to run the small benchmark with 10 iterations:

```sh
cd runner
dotnet run -c Release -- -i 10 -s small
```

To get all of the time measurements used in this blogpost, run the following commands (they are also available in the `run_all.sh` script):

```sh
cd runner
dotnet run -c Release -- -i 10 -s all
dotnet run -c Release -- -i 10 -s medium --operation sparsity
dotnet run -c Release -- -i 10 -s medium --operation filesizes
```

Note that they will take some time to run and that the large benchmark requires at least 200 GB of free disk space.

# Plotting

The results can be cleaned up and plotted using the `plotting.ipynb` Jupyter notebook also in this directory.
