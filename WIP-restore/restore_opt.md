# Speeding up the restore process
We start by backing up pre-sc-ad to a local folder, as this should remove the upload/download overhead.

This blog post describes the reworked restore process.

It has been merged in [PR #5728](https://github.com/duplicati/duplicati/pull/5728).

It has been part of the releases since [Duplicati 2.1.0.103](https://github.com/duplicati/duplicati/releases/tag/v2.1.0.103_canary_2024-12-21) onwards.

If any issues arise with the new flow, please report them here on the forum and the legacy flow can still be used instead by supplying the option `--restore-legacy=true`.

## TL:DR;
The restore process is slow because it is not parallelized. The restore process is rewritten to be parallelized, and the restore time is reduced by X times.

A noteworthy new behaviour is that the post restore verification has been removed, as it is now performed on the fly during the restore process.

- Graph showing the new process network
- Some graph showing the time reduction

## Machine setup
The following table shows the different machines mentioned:

| Machine | CPU | RAM | OS | .NET |
|---------|-----|-----|----|------|
| MacBook Pro 2021 | (ARM64) M1 Max 10-core (8P+2E) 3.2 GHz | 64 GB LPDDR5-6400 ~400 GB/s | macOS Sequoia 15.2 | 8.0.404 |
| AMD 7975WX | (x86_64) 32-core 4.0 GHz (5.3) | 512 GB DDR5-4800 8-channel ~300 GB/s | Ubuntu 24.04.1 LTS | 8.0.112 |
| AMD 1950X | (x86_64) 16-core 3.4 GHz (4.0) | 128 GB DDR4-3200 4-channel ~200 GB/s | Ubuntu 22.04.4 LTS | 8.0.110 |
| Intel W5-2445 | (x86_64) 10-core 3.1 GHz (4.6) | 128 GB DDR5-4800 4-channel ~150 GB/s | Ubuntu 22.04.5 LTS | 8.0.112 |
| AMD 9800X3D | (x86_64) 8-core 4.7 GHz (5.2) | 96 GB DDR5-6400 2-channel ~100 GB/s | Windows 11 x64 | 8.0.403 |
| Raspberry Pi 3 Model B | (ARM64) 4-core 1.2 GHz | 1 GB LPDDR2-900 ~6 GB/s | Raspbian 11 | 8.0.403 |

## Termonology
We'll be using the following terms in this post:
- _Block_: A blob of data. A file is made up of one or more blocks.
- _Volume_: A zip file containing one or more blocks.
- _Source file_: The original file that was backed up.
- _Target file_: The target file that is being restored. It may be the same path as source, depending on the `--restore-path` parameter.
- _File filter_: The filter that is used to select which files to restore. E.g. for a full restore, the filter would be `"*"`.
- _Local_: The machine that is performing the restore. It may be the same machine as the backup was performed on, but it doesn't have to be.
- _Remote_: The provider storing the backup. E.g. Amazon S3, local file, an SSH server, ...
- _Local database_: The database that keeps track of which files a backup contains, which blocks make up each file, and in which volumes the blocks are stored.
- _Flow_: A sequence of processing steps that are performed in a specific order. A flow can be sequential, parallel, or a combination of both.
- _Legacy flow_: The restore flow that has been in use for many years.
- _New flow_: The restore flow that has been rewritten to be parallelized and is the subject of this blog post.
- _[Communicating Sequential Processes (CSP)](https://www.cs.cmu.edu/~crary/819-f09/Hoare78.pdf)_: A programming paradigm that models concurrent systems as a network of independent processes that communicate through channels. In Duplicati, the CSP library [CoCoL](https://github.com/kenkendk/cocol) is used, but in principle any CSP library could be used. It was chosen since it is already being used in Duplicati, especially in the backup flow.
- _Process_: A CSP process that sequentially performs a specific task, only sharing data through channels. A process can be a thread, a coroutine, or any other form of concurrent execution.
- _Channel_: A CSP channel that is used to communicate between processes. A message can be any object. A channel can be unbuffered, meaning a synchronous/rendezvous channel where the sender and receiver must be ready to communicate, or buffered, meaning an asynchronous channel where the sender can send a message without the receiver being ready to receive it up to a certain buffer size.

# The old restore process
Before describing the new flow, there's value in understanding the old restore flow, its strengths and weaknesses.
The legacy restore process flow is as follows:

1. Combine file filters.
2. Open or restore the local database.
3. Verify the remote files;
    1. Get the list of remote volumes.
    2. Verify that there are no missing or unexpected extra volumes.
4. Prepare the block and file list.
5. Create the directory structure.
6. Scan the existing target files.
7. Scan for existing source files.
8. Patch with local blocks.
9. Get the list of required volumes to download.
10. For each volume:
    1. Download the volume.
    2. Decrypt the volume.
    3. Decompress the volume.
    4. For each block in the decompressed volume:
        1. Extract the block from the zip file.
        2. Check that the size and block hash matches.
        3. Patch all of the target files that need this block.
11. Restore metadata; for each target file:
    1. Download the volume that contains the metadata.
    2. Decrypt the volume.
    3. Decompress the volume.
    4. Extract the metadata from the zip file.
    5. Check that the size and block hash matches.
    6. Restore the metadata from the block(s).
12. Verify the restored files; for each target file:
    1. Read the target file.
    2. Compute the hash of the target file.
    3. Check that the hash and size matches.

The flow is visualized in the following diagram:



This flow has several benefits:
- There is a clear separation of the different steps.
- Volumes are only downloaded once.
- Blocks are only extracted once.
- The flow has a low memory and disk footprint.
- The flow is stable, as it has been in use for many years.

It has the following drawbacks:
- The separation of steps can lead to multiple passes over the same data, moving in and out of memory and disk.
- Each step is sequential, thus not fully utilizing system resources or leveraging overlapping execution. This results in the process potentially being very slow.
- Block writes are scattered across disk, leading to potentially slow writes, as disks favor sequential access patterns.



# The new restore process
Different solutions to the problems presented in the old restore process.
Final solution.

## Tunable parameters
- Number of FileProcessors
- Number of Volume downloaders
- Number of Volume uncompressors
- Size of the Volume cache
- Size of the Block cache

### Example of tuning the restore process for maximum throughput

## Caching strategy
### Volume cache
Resides on disk. Only the volumes that contain blocks still needing to be restored are kept in cache. Volumes can be evicted which triggers a re-download of the volume if needed.

### Block cache
Resides in memory. Only the blocks that are still needed to be restored are kept in cache. Blocks can be evicted which triggers a re-decompression of the block if needed.

### Profiling of disk usage, memory consumption, CPU utilization and time spent under different cache parameters.

# Results
Much fast

# Conclusion
It's great - buy now.