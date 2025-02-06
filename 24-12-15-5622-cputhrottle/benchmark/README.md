# CPU throttle benchmark

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

To run the benchmark, the `Duplicati.CommandLine.exe` executable must be available. The only requirement is that it is a version that supports the `--cpu-intensity` parameter, which means the repo from pull request [\#5622](https://github.com/duplicati/duplicati/pull/5622) onwards, or the [Canary build 2.0.109](https://github.com/duplicati/duplicati/releases/tag/v2.0.9.109_canary_2024-11-06) onwards. To use the exact version used in this benchmark, pull and build the `duplicati` repo from the pull request. Note that we're building the `Release` configuration to benchmark the impact in the most optimized state:

```sh
cd /path/to/your/repos
git clone git@github.com:duplicati/duplicati.git
cd duplicati
git checkout 4fff55f
dotnet build -c Release
```

The path to the `Duplicati.CommandLine.exe` executable will be `/path/to/your/repos/duplicati/Executables/net8/Duplicati.CommandLine/bin/Release/net8.0/Duplicati.CommandLine`. On Windows, the executable will have have the `.exe` extension.

## A test data generation tool

To run the benchmark, we need to have some test data ready. This can either be done manually, by having the two folders `data_small` and `data_large`, or by using Duplicatis built-in test data generation tool. At the time of writing, this tool has not been merged into the main branch, so we need to pull the `add-test-data-generator` branch. Note that we don't need to build a specific configuration, as we're not measuring the performance of the test data generation tool:

```sh
cd /path/to/your/repos
git clone git@github.com:duplicati/duplicati.git duplicati-testdata
cd duplicati-testdata
git checkout add-test-data-generator
dotnet build
```

The path to the `Duplicati.TestDataGenerator.exe` executable will be `/path/to/your/repos/duplicati-testdata/Tools/TestDataGenerator/bin/Release/net8.0/TestDataGenerator`. On Windows, the executable will have have the `.exe` extension.

# Running the benchmark

## Running on UNIX / Linux systems

To run the benchmark on UNIX / Linux systems, execute the following commands:

```sh
export DUPLICATI=/path/to/your/repos/duplicati/Executables/net8/Duplicati.CommandLine/bin/Release/net8.0/Duplicati.CommandLine
export DUPLICATI_TESTDATA=/path/to/your/repos/duplicati-testdata/Tools/TestDataGenerator/bin/Release/net8.0/TestDataGenerator
./perform_runs.sh
```

## Running on Windows systems

To run the benchmark on Windows systems, the paths should be given as arguments to the PowerShell script:

```ps1
.\perform_runs.ps1 "C:\path\to\your\repos\duplicati\Executables\net8\Duplicati.CommandLine\bin\Release\net8.0\Duplicati.CommandLine.exe" "C:\path\to\your\repos\duplicati-testdata\Tools\TestDataGenerator\bin\Release\net8.0\TestDataGenerator.exe"
```

## Results

Running the scripts will create the testdata in the `data_small` and `data_large` folders, if they don't already exist, and run the benchmark on both `data_small` and `data_large` with `--cpu-intensity` values ranging from 1 to 10. The script will output the results to the `times` folder, which will be created if it doesn't already exist. These times can be plotted using the `plotting.ipynb` Jupyter notebook in the parent folder of this README. To run the notebook, the names of the timing files should match this format `{hostname}-{data_folder}{cpu_intensity}.time`, e.g. `mac-data_small1.time`. Then the entries for the machines and their pretty print names must also be changed in the notebook.
