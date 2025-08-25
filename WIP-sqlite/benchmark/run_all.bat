@echo off

set warmup=10000
set repetitions=100000

REM Define sizes array
set sizes=10000 100000 1000000 10000000
REM Define threads array
set threads=1 2 4 8 16 32
REM Define batches array
set batches=0 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768 65536

for %%s in (%sizes%) do (
    .\bin\schema1 --num-entries %%s --num-warmup %warmup% --num-repetitions %repetitions%
    .\bin\schema4 --num-entries %%s --num-warmup %warmup% --num-repetitions %repetitions%
    .\bin\schema7 --num-entries %%s --num-warmup %warmup% --num-repetitions %repetitions%
    .\bin\schema10 --num-entries %%s --num-warmup %warmup% --num-repetitions %repetitions%
    .\bin\pragmas --num-entries %%s --num-warmup %warmup% --num-repetitions %repetitions%
    for %%t in (%threads%) do (
        .\bin\parallel --num-entries %%s --num-warmup %warmup% --num-repetitions %repetitions% --num-threads %%t
    )
    for %%b in (%batches%) do (
        .\bin\batching --num-entries %%s --num-warmup %warmup% --num-repetitions %repetitions% --num-batch %%b
    )
)

REM Build C# project
dotnet build -c Release csharp

REM Run C# benchmark
csharp\bin\Release\net9.0\sqlite_bench.exe --buildTimeout 600

REM Copy results
copy BenchmarkDotNet.Artifacts\results\*.csv reports\
