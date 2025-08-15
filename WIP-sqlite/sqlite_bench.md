# Unleashing the power of SQLite to C#

This blog post describes different implementations, optimizations, tunes, and benchmarks of SQLite.
While the work is motivated by the Duplicati project, it is not limited to it, leading to these findings being applicable to other projects as well.

## TL;DR

## Machine setup

# Introduction

Internally, Duplicati uses SQLite to keep track of the files, their blocks, the file hashes, the block hashes, etc.
The reason for using SQLite is that it is a self-contained file-based database, which makes it easy to deploy and use without needing a separate database server.
Furthermore, it's a mature and well-tested database engine, it's lightweight, and it has a small footprint, all of which should make it a good fit for Duplicati, both in terms of performance, stability, deployability, and ease of use.

However, achieving high performance with SQLite requires some tuning and optimizations, especially when dealing with large datasets and high query rates.
This became especially apparent while investigating the performance of Duplicati's recreate database operation.
TODO build argument strength by including a profiling screenshot / flame graph showing the database calls.
Here we found that a considerable amount of time spent was a series of SQL queries, especially the pattern; `SELECT`, return row if found, otherwise, `INSERT` a new row.
As the database grew, each query would take longer and longer, starting at around 200k queries per second, ending below 50k queries per second, with a negative trend.
This led to this investigation, as SQLite is supposed to be fast, and it was not performing as expected.
Even the initial throughput of 200k queries per second was nowhere near the 1M queries per second that SQLite should be able to do.
TODO cite the source of the 1M queries per second.

This blog post isolates SQLite from Duplicati to investigate the performance of SQLite itself to find out what is causing the slowdown.
While the motivation originates from Duplicati, this investigation is self-contained and can be applied to other projects as well.

# Investigations

Before we begin, we need to outline what we are investigating:

- Can we achieve 1M queries per second with SQLite?
- Is C# the bottleneck, or is it the SQLite library?
- Can we still achieve performance on-disk without having to run everything in memory?

## Benchmarks

We're going to run on a simple database schema, with two tables that mimic some found in Duplicati:

```sql
CREATE TABLE "Blockset" (
	"ID" INTEGER PRIMARY KEY,
	"Length" INTEGER NOT NULL
);

CREATE TABLE "BlocksetEntry" (
	"BlocksetID" INTEGER NOT NULL,
	"BlockID" INTEGER NOT NULL
);

CREATE TABLE "Block" (
	"ID" INTEGER PRIMARY KEY,
    "Hash" TEXT NOT NULL,
	"Size" INTEGER NOT NULL
);
```

The `Blockset` table represents a set of blocks.
The `Block` table represents individual blocks, where each block has a hash and a size.
The `BlocksetEntry` table represents the relationship between a blockset and its blocks, allowing for multiple blocks to be associated with a single blockset.

In our benchmarks, the `Blockset` table will showcase the most simple database schema (two integers, with one being the id), the `Block` table will showcase a more complex schema (a string and an integer), and the `BlocksetEntry` table will showcase a many-to-many relationship between the two tables.

We will run the following benchmarks:

### Select

We start by the simplest query, selecting a row from the `Block` table, which is quite commonly used in Duplicati:

```sql
SELECT ID FROM Block WHERE Hash = ? AND Size = ?;
```

### Insert

Similar to the select, we also want to benchmark inserting a row into the `Block` table:

```sql
INSERT INTO Block (Hash, Size) VALUES (?, ?);
```

### Select xor insert `Block`

As mentioned, this is a common pattern in Duplicati, where we first try to select a row, and if it does not exist, we insert it.
We will benchmark two different approaches to this:
Two statements, with the the check performed in C#:

```sql
SELECT ID FROM Block WHERE Hash = ? AND Size = ?;
```

Perform check in userspace, then insert if not found:

```sql
INSERT INTO Block (Hash, Size) VALUES (?, ?);
```

And we also want to measure the impact of moving the data back to userland, by comparing to a combined statement with the check performed in SQL:

```sql
INSERT OR IGNORE INTO Block (Hash, Size) VALUES (?, ?);
SELECT ID FROM Block WHERE Hash = ? AND Size = ?;
```

### Select from Join

As the tables in Duplicati are often very small (schema-wise), but interconnected, we also want to benchmark a join query.
We will select all blocks in a blockset:

```sql
SELECT Block.ID, Block.Hash, Block.Size
FROM Block
JOIN BlocksetEntry ON BlocksetEntry.BlockID = Block.ID
WHERE BlocksetEntry.BlocksetID = ?;
```

### Adding blocksets

Finally, we want to benchmark the process of adding blocksets, where we first create a new blockset:

```sql
INSERT INTO Blockset (Length) VALUES (0);
SELECT last_insert_rowid() AS ID;
```

Then we either pick an existing block or create a new one. Then check if the block already exists:

```sql
SELECT ID FROM Block WHERE Hash = ? AND Size = ?;
```

If it does not exist, we insert it:

```sql
INSERT INTO Block (Hash, Size) VALUES (?, ?);
SELECT last_insert_rowid();
```

Link up the rows through the `BlocksetEntry` table:

```sql
INSERT INTO BlocksetEntry (BlocksetID, BlockID) VALUES (?, ?);
```

And increment the length of the blockset:

```sql
UPDATE Blockset SET Length = Length + 1 WHERE ID = ?;
```

Then with some probability, we'll start a new blockset.

## Tuning

As this blog post focuses on isolating the performance of SQLite, we will start with a C++ implementation, to avoid any overhead from C#. This will allow us to obtain a best-case baseline, which we can then compare against the C# implementations.
We will be using all of the compiler optimizations available; `-O3 -march=native -mtune=native -flto`, and we will be using the `sqlite3` C API directly, to avoid any overhead from higher-level libraries.
To further maximize performance, we will use prepared statements and each query will be run in a transaction.

### Indexes and schema changes

TODO here the database is created from scratch for every benchmark, because we want to investigate the effect on the indices as if they're built "on-the-fly". Deleting and creating the indices only could result in better balance, and it'll put the database in an incorrect state (compared to what we'd expect in the real world).

The obvious first step to improving database performance is to add indices to the tables.
Duplicati already uses indices, so there's not much we can do here.
However, we can modify the schema to ease the work needed by the database engine, hopefully leading to better performance.

Especially, we'll see if we can modify the `Hash` column in the block table, which is currently a `TEXT` column, and we'll investigate different indexing strategies for each of the modified schemas, which should allow for smaller, and thus faster, indices. Whether this is actually the case will be investigated in the benchmarks.
We will investigate the following:

1. The normal `Hash` column as a `TEXT` column, with the index on `(Hash, Size)`.
2. 1 with the index on `Hash` only,. This means that since the query will result in multiple rows, we will have to filter the results in userland to find the correct row.
3. 1 with the index on `Size` only, further filtering in userland.
4. Change the `Hash` column to a `BLOB` column, which is more efficient for storing binary data. This should improve performance, as this should remove any string overhead and the overhead from base64 encoding. Here the index would be on `(Hash, Size)`.
5. 4 with the index on `Hash` only, further filtering in userland.
6. 4 with the index on `Size` only, further filtering in userland.
7. Change the `Hash` column to a `VARCHAR(44)` column, which is more efficient for storing fixed-length strings. Here the index would be on `(Hash, Size)`.
8. 7 with the index on `Hash` only, further filtering in userland.
9. 7 with the index on `Size` only, further filtering in userland.
10. Change the `Hash` column to be four `INTEGER` columns (`h0`, `h1`, `h2`, `h3`) each storing 64-bit integers. This should allow SQLite to index and compare the values more efficiently, as it becomes integer comparisons rather than string comparisons. Like storing as a `BLOB`, this approach also alleviates using base64 encoding. The first index used would be on `(h0, h1, h2, h3, Size)`.
11. 10 with the index on `(h0, Size)` only, further filtering in userland.
12. 10 with the index on `h0` only, further filtering in userland.
13. 10 with the index on `Size` only, further filtering in userland.

Collective graph for select scaled logarithmically by the number of entries in the database on the x-axis, and the throughput (k queries per second) on the y-axis. Because there will be 10 curves, they won't include error.

Zoom into a specific x, and show a boxplot for each of the strategies.

Depending on clutter, we might show the best indexing strategy for each of the schemas to make them easier to compare.

If there's time:
Collective graph for insert scaled logarithmically by the number of entries in the database on the x-axis, and the throughput (k queries per second) on the y-axis. Because there will be 10 curves, they won't include error.

Zoom into a specific x, and show a boxplot for each of the strategies.

Hopefully, they'll agree that the normal strategy works just fine.

### PRAGMAs

A less intrusive way to optimize SQLite is to use PRAGMAs, which are special commands that can be used to change the behavior of the SQLite engine.
We will investigate the following PRAGMAs:

- `synchronous`: This controls how often SQLite flushes data to disk. The default is `FULL`, which is the safest option, but it can be slow as it waits for the OS to confirm that the data is written to disk through a disk sync. We will test with `NORMAL`, which is faster but less safe (it doesn't wait for the disk sync, but instead returns once the data has been written to the OS), and `OFF`, which is the fastest but can lead to data loss in case of a crash.
- `temp_store`: This controls where temporary tables are stored. The default is `DEFAULT`, which uses the disk, but we can also use `MEMORY`, which stores temporary tables in memory, leading to faster access times. While we won't be using temporary tables in our benchmarks, this can still lead to performance improvements as SQLite uses temporary tables for certain operations internally.
- `journal_mode`: This controls how SQLite handles transactions. The default is `DELETE`, which is the safest option, but it can be slow as it creates a journal file on disk. We will test with `WAL`, which is faster and allows for concurrent reads and writes. We won't be testing `OFF`, which is the fastest, as it disables journaling entirely, leading to rollbacks having undefined behavior.
- `cache_size`: This controls the size of the SQLite cache, which is used to store recently accessed data. The default is 2000 pages (around 2 MB), but we can increase this to improve performance.
- `mmap_size`: This controls the size of the memory-mapped file used by SQLite. The default is 0, which means that memory-mapping is disabled. We can enable it to improve performance, especially for large databases.
- `threads`: This controls the number of additional threads that SQLite can use as helper threads for prepared statements. The default is 0, which means that SQLite will not launch any additional helper threads.

Similar to the schema, we'll have all of the pragmas as individual curves with throughput on the y-axis and the number of entries in the database on the x-axis. There'll be an additional curve for no pragmas, and one for all of them. Then, we'll zoom into a specific x and show a boxplot for each of the pragmas.

Or maybe, rather than having the baseline as a curve, all of the curves should be relative to the baseline? Or maybe have both plots. The idea behind the relative one is to quickly rule out the ones that don't improve performance.

Maybe have one set of plots for each benchmark?

### Parallelization

Transactions becomes deferred to allow for parallel reads without locking the entire database.

## Backends

### Duplicati's shipped SQLite

### System.Data.SQLite

### Microsoft.Data.Sqlite

### sqlite3 through PInvoke

### Asynchronous Microsoft.Data.Sqlite

# Combining everything

Go with unchanged queries, adding the indices, using prepared statements, Microsoft.Data.Sqlite for asynchronous operations, and finally the PRAGMAs to optimize the database.

Aggregated, we went from X to Y, leading to a Z% speedup.

Show scaling graphs.

# Future work

Revisit Duplicati to see that these optimizations do in fact speed up the process.

# Conclusion

Immense speed.

Setup the Duplicati repo to run the benchmark against:

```bash
git clone git@github.com:duplicati/duplicati.git data_duplicati_repo
```

It is faster to insert the rows without the index.
However, it's not faster if the index needs to be created afterwards, as that requires a full table scan and sort.
Managing the index during insertion is faster, at least for the multi column index.

TODO try to fill into a temp table without the index, then insert the temp table sorted by the index, which should speed up things.

Changing sqlite (system.data and microsoft) backend did not perform much different from the duplicati backend.

Only looking up one of the two columns (e.g. either hash or length) is faster than looking up in a multi-column index.

Depending on the amount of clashes for each column, filtering afterwards can be faster.

TODO reflect against Duplicati; try to correlate the number of blocks to the the size of the data backed up, to relate the performance numbers to how Duplicati would perform (or at least how the database accesses would perform) - hopefully leading to that the database is now not the bottleneck.
