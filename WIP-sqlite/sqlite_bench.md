Setup the Duplicati repo to run the benchmark against:

```bash
git clone git@github.com:duplicati/duplicati.git data_duplicati_repo
```

- Changing sqlite (system.data and microsoft) backend did not perform much different from the duplicati backend.
- Only looking up one of the two columns (e.g. either hash or length) is faster than looking up in a multi-column index.
- Depending on the amount of clashes for each column, filtering afterwards can be faster.
