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

To run the benchmark, the `Duplicati.CommandLine.exe` and `Duplicati.TestDataGenerator.exe` executables must be available. To use the exact version used in this benchmark, run the following commands:

```sh
cd /path/to/where/you/want/the/repos
git clone git@github.com:duplicati/duplicati.git duplicati
cd duplicati
git checkout 298c26b
dotnet build -c Release
cd ..
git clone git@github.com:duplicati/duplicati.git duplicati-testdata
cd duplicati-testdata
git checkout df76a77
dotnet build -c Release
```

The path to the `Duplicati.CommandLine.exe` executable will be `/path/to/where/you/want/the/repos/duplicati/Executables/net8/Duplicati.CommandLine/bin/Release/net8.0/Duplicati.CommandLine`. The path to the `Duplicati.TestDataGenerator.exe` executable will be `/path/to/where/you/want/the/repos/duplicati-testdata/Tools/TestDataGenerator/bin/Release/net8.0/TestDataGenerator`. On Windows, the executables will have have the `.exe` extension.

# Running the benchmark

To run the benchmark on Linux, run the `perform_runs.sh`. To run the benchmark on Windows, run the `perform_runs.ps1` PowerShell script. Both scripts take the same arguments and produce the same output. The three arguments are:

1. The path to the `Duplicati.CommandLine.exe` executable.
2. The path to the `Duplicati.TestDataGenerator.exe` executable.
3. Which dataset to use. The options are `small`, `medium`, `large`, and `all`. The `all` option will run the benchmark for all datasets.

The scripts produce a CSV file with the results of the benchmark and will be placed in the `results` directory in the same directory as the script.

E.g. for Linux:

```sh
./perform_runs.sh /path/to/where/you/want/the/repos/duplicati/Executables/net8/Duplicati.CommandLine/bin/Release/net8.0/Duplicati.CommandLine /path/to/where/you/want/the/repos/duplicati-testdata/Tools/TestDataGenerator/bin/Release/net8.0/TestDataGenerator small
```

E.g. for Windows:

```ps1
.\perform_runs.ps1 /path/to/where/you/want/the/repos/duplicati/Executables/net8/Duplicati.CommandLine/bin/Release/net8.0/Duplicati.CommandLine.exe /path/to/where/you/want/the/repos/duplicati-testdata/Tools/TestDataGenerator/bin/Release/net8.0/TestDataGenerator.exe small
```

# Plotting

The results can be cleaned up and plotted using the `plotting.ipynb` Jupyter notebook also in this directory.