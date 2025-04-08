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
