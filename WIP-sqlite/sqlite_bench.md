# Making SQLite go fast

This blog post describes different implementations, optimizations, tunes, and benchmarks of SQLite.
While the work is motivated by the Duplicati project, it is not limited to it, leading to these findings being applicable to other projects as well.

## TL;DR

## Machine setup

# Introduction

This work started by looking into what takes time during the recreate database operation of Duplicati.
Here we found that most of the time spent was a series of SQL queries, especially the pattern; `SELECT`, return row if found, otherwise, `INSERT` a new row.
As the database grew, each query would take longer and longer, starting at around 200k queries per second, ending below 50k queries per second.
This led to this investigation, as SQLite is supposed to be fast, and it was not performing as expected.
Even the initial throughput of 200k queries per second was nowhere near the 1M queries per second that SQLite should be able to do.

Internally, Duplicati uses SQLite to keep track of the files, their blocks, the file hashes, block hashes, etc.

# Investigations

## Benchmarks

### Select

### Insert

### Insert or Update

### Select then insert

### Select from Join

## Tuning

### Prepare statements

### Indexes

### Multi-column indexes

### PRAGMAs

## Backends

### Duplicati's shipped SQLite

### System.Data.SQLite

### Microsoft.Data.Sqlite

### sqlite3 through PInvoke

### sqlite3 from C++

## Parallelization

Transactions becomes deferred to allow for parallel reads without locking the entire database.

# Combining everything

Go with unchanged queries, adding the indices, using prepared statements, Microsoft.Data.Sqlite for asynchronous operations, and finally the PRAGMAs to optimize the database.

Aggregated, we went from X to Y, leading to a Z% speedup.

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
