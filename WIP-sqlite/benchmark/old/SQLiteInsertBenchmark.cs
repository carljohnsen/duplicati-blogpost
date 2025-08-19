using BenchmarkDotNet.Attributes;
using Duplicati.Library.Main.Database;
using System.Data;

namespace sqlite_bench
{
    [Config(typeof(BenchmarkConfig))]
    [MinColumn, MaxColumn, AllStatisticsColumn]
    public class SQLiteInsertBenchmark : SQLiteBenchmarkSequential
    {
        [ParamsAllValues]
        public static Backends Backend { get; set; }

        [ParamsSource(nameof(ValidParams))]
        public BenchmarkParams BenchmarkParams { get; set; } = new BenchmarkParams();

        // The prepared statements
        readonly IDbCommand m_insertBlocksetManagedCommand;
        readonly IDbCommand m_insertBlocksetSelectCommand;
        readonly IDbCommand m_insertBlocksetReturningCommand;
        readonly IDbCommand m_insertBlocksetTempManagedCommand;

        // Runtime state
        List<(long, string)> entries = [];
        readonly List<long> ids = [];
        long next_id = -1;
        readonly Dictionary<(long, string), long> id_map = [];

        // TODO giv den et index og sorter efter det i temp table? burde vaere nemmere at holde batching. Maaske er en array list lignende struktur?
        //[Params(0, 1000, 10000, 100000, 1000000, 10000000)]
        [Params(0)]
        public int PreFilledCount { get; set; } = 0;

        public SQLiteInsertBenchmark() : base(Backend)
        {
            m_insertBlocksetManagedCommand = CreateCommand(SQLQeuriesOriginal.InsertBlocksetManaged);
            m_insertBlocksetSelectCommand = CreateCommand(SQLQeuriesOriginal.InsertBlocksetSelect);
            m_insertBlocksetReturningCommand = CreateCommand(SQLQeuriesOriginal.InsertBlocksetReturning);
            m_insertBlocksetTempManagedCommand = CreateCommand(SQLQeuriesOriginal.InsertTempManaged);
        }

        public void CheckIfCommit(int iteration)
        {
            if (BenchmarkParams.UseTransaction && BenchmarkParams.CommitEveryN > 0 && iteration % BenchmarkParams.CommitEveryN + 1 == BenchmarkParams.CommitEveryN)
            {
                transaction?.Commit();
                transaction?.Dispose();
                transaction = con.BeginTransaction();
            }
        }

        public void CheckIfCreateIndex()
        {
            if (!BenchmarkParams.UseIndex && BenchmarkParams.IndexAfter)
            {
                using var cmd = con.CreateCommand();
                cmd.CommandText = SQLQeuriesOriginal.Index;
                cmd.ExecuteNonQuery();
            }
        }

        public class TupleComparer : IComparer<(long, string)>
        {
            public int Compare((long, string) x, (long, string) y)
            {
                int result = x.Item1.CompareTo(y.Item1);
                if (result != 0)
                    return result;

                return string.Compare(x.Item2, y.Item2, StringComparison.Ordinal);
            }
        }

        protected override void Dispose(bool disposing)
        {
            m_insertBlocksetManagedCommand.Dispose();
            m_insertBlocksetSelectCommand.Dispose();
            m_insertBlocksetReturningCommand.Dispose();
            base.Dispose(disposing);
        }

        [Benchmark]
        public void FillManaged()
        {
            transaction ??= con.BeginTransaction();

            for (int i = 0; i < entries.Count; i++)
            {
                CheckIfCommit(i);

                var (length, fullhash) = entries[i];
                var id = next_id++;
                m_insertBlocksetManagedCommand.SetParameterValue("id", id);
                m_insertBlocksetManagedCommand.SetParameterValue("length", length);
                m_insertBlocksetManagedCommand.SetParameterValue("hash", fullhash);
                _ = m_insertBlocksetManagedCommand.ExecuteNonQuery(transaction);
                ids.Add(id);
            }

            CheckIfCreateIndex();
        }

        //[Benchmark]
        public void FillSelect()
        {
            transaction ??= con.BeginTransaction();

            for (int i = 0; i < entries.Count; i++)
            {
                CheckIfCommit(i);

                var (length, fullhash) = entries[i];
                m_insertBlocksetSelectCommand.SetParameterValue("length", length);
                m_insertBlocksetSelectCommand.SetParameterValue("hash", fullhash);
                var id = m_insertBlocksetSelectCommand.ExecuteScalarInt64(transaction);
                ids.Add(id);
            }

            CheckIfCreateIndex();
        }

        [Benchmark]
        public void FillSortedInMemory()
        {
            transaction ??= con.BeginTransaction();

            var cmp = Comparer<(long, string)>.Create((l, h) =>
            {

                var cmp = l.Item1.CompareTo(h.Item1);
                if (cmp == 0)
                    cmp = l.Item2.CompareTo(h.Item2);
                return cmp;
            });
            entries.Sort(new TupleComparer());

            int i = 0;
            foreach (var entry in entries)
            {
                CheckIfCommit(i++);

                var (length, fullhash) = entry;
                var id = next_id++;
                m_insertBlocksetManagedCommand.SetParameterValue("id", id);
                m_insertBlocksetManagedCommand.SetParameterValue("length", length);
                m_insertBlocksetManagedCommand.SetParameterValue("hash", fullhash);
                _ = m_insertBlocksetManagedCommand.ExecuteNonQuery(transaction);
                ids.Add(id);
            }

            CheckIfCreateIndex();
        }

        [Benchmark]
        public void FillTemp()
        {
            transaction ??= con.BeginTransaction();

            using var cmd = con.CreateCommand();
            cmd.CommandText = SQLQeuriesOriginal.TempTable;
            cmd.ExecuteNonQuery();

            for (int i = 0; i < entries.Count; i++)
            {
                CheckIfCommit(i);

                var (length, fullhash) = entries[i];
                var id = next_id++;
                m_insertBlocksetTempManagedCommand.SetParameterValue("id", id);
                m_insertBlocksetTempManagedCommand.SetParameterValue("length", length);
                m_insertBlocksetTempManagedCommand.SetParameterValue("hash", fullhash);
                _ = m_insertBlocksetTempManagedCommand.ExecuteNonQuery(transaction);
            }

            cmd.CommandText = SQLQeuriesOriginal.FlushTemp;
            cmd.ExecuteNonQuery(transaction);

            CheckIfCreateIndex();
        }

        [Benchmark]
        public void FillTempSorted()
        {
            transaction ??= con.BeginTransaction();

            using var cmd = con.CreateCommand();
            cmd.CommandText = SQLQeuriesOriginal.TempTable;
            cmd.ExecuteNonQuery();

            for (int i = 0; i < entries.Count; i++)
            {
                CheckIfCommit(i);

                var (length, fullhash) = entries[i];
                var id = next_id++;
                m_insertBlocksetTempManagedCommand.SetParameterValue("id", id);
                m_insertBlocksetTempManagedCommand.SetParameterValue("length", length);
                m_insertBlocksetTempManagedCommand.SetParameterValue("hash", fullhash);
                _ = m_insertBlocksetTempManagedCommand.ExecuteNonQuery(transaction);
            }

            cmd.CommandText = SQLQeuriesOriginal.FlushTempSorted;
            cmd.ExecuteNonQuery(transaction);

            CheckIfCreateIndex();
        }

        [Benchmark]
        public void FillTempSortedInMemory()
        {
            transaction ??= con.BeginTransaction();

            using var cmd = con.CreateCommand();
            cmd.CommandText = SQLQeuriesOriginal.TempTable;
            cmd.ExecuteNonQuery();

            entries.Sort(new TupleComparer());

            int i = 0;
            foreach (var entry in entries)
            {
                CheckIfCommit(i++);

                var (length, fullhash) = entry;
                var id = next_id++;
                m_insertBlocksetTempManagedCommand.SetParameterValue("id", id);
                m_insertBlocksetTempManagedCommand.SetParameterValue("length", length);
                m_insertBlocksetTempManagedCommand.SetParameterValue("hash", fullhash);
                _ = m_insertBlocksetTempManagedCommand.ExecuteNonQuery(transaction);
            }

            cmd.CommandText = SQLQeuriesOriginal.FlushTemp;
            cmd.ExecuteNonQuery(transaction);

            CheckIfCreateIndex();
        }

        //[Benchmark]
        public void FillReturning()
        {
            transaction ??= con.BeginTransaction();

            for (int i = 0; i < entries.Count; i++)
            {
                CheckIfCommit(i);

                var (length, fullhash) = entries[i];
                m_insertBlocksetReturningCommand.SetParameterValue("length", length);
                m_insertBlocksetReturningCommand.SetParameterValue("hash", fullhash);
                var id = m_insertBlocksetReturningCommand.ExecuteScalarInt64(transaction);
                ids.Add(id);
            }

            CheckIfCreateIndex();
        }

        //[Benchmark]
        public void FillDictionary()
        {
            for (int i = 0; i < entries.Count; i++)
            {
                var entry = entries[i];
                var id = next_id++;
                id_map[entry] = id;
                ids.Add(id);
            }
        }

        [GlobalSetup]
        public void GlobalSetup()
        {
            var rng = new Random();

            // Generate random data to insert
            for (long i = 0; i < BenchmarkParams.Count; i++)
            {
                entries.Add((rng.NextInt64(), Guid.NewGuid().ToString()));
            }

            if (PreFilledCount > 0)
            {
                last_id = 0;
                DropRows();
                using var cmd = con.CreateCommand();
                using var transaction = con.BeginTransaction();
                cmd.CommandText = m_insertBlocksetManagedCommand.CommandText;
                cmd.Parameters.Add(m_insertBlocksetManagedCommand.Parameters["id"]);
                cmd.Parameters.Add(m_insertBlocksetManagedCommand.Parameters["length"]);
                cmd.Parameters.Add(m_insertBlocksetManagedCommand.Parameters["hash"]);
                cmd.Prepare();

                // Generate data to be filled before the benchmark
                long i;
                for (i = 0; i < PreFilledCount; i++)
                {
                    cmd.SetParameterValue("id", i);
                    cmd.SetParameterValue("length", rng.NextInt64() % 100);
                    cmd.SetParameterValue("hash", Guid.NewGuid().ToString());
                    cmd.ExecuteNonQuery(transaction);
                }
                transaction.Commit();
                last_id = i;
            }
            else
            {
                last_id = 0;
            }

            if (BenchmarkParams.UseTransaction)
                transaction = con.BeginTransaction();
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            last_id = 0;
            DropRows();
            transaction?.Commit();
            transaction?.Dispose();
            last_id = -1;
        }

        [IterationSetup]
        public void IterationSetup()
        {
            DropRows();
            next_id = GetLastRowId() + 1;
            ids.Clear();
            id_map.Clear();
            using var cmd = con.CreateCommand();
            if (BenchmarkParams.UseIndex)
            {
                cmd.CommandText = SQLQeuriesOriginal.CreateIndex;
                cmd.ExecuteNonQuery();
            }
            else
            {
                cmd.CommandText = SQLQeuriesOriginal.DropIndex;
                cmd.ExecuteNonQuery();
            }

            entries = [.. entries.OrderBy(x => Guid.NewGuid())];
        }

        [IterationCleanup]
        public void IterationCleanup()
        {
            if (BenchmarkParams.UseTransaction)
            {
                transaction?.Commit();
                transaction?.Dispose();
                transaction = con.BeginTransaction();
            }
        }

        public static IEnumerable<BenchmarkParams> ValidParams()
        {
            var counts = new[] { 10000, 100000, 1000000 };
            var commitEveryNs = new[] { 0 };

            foreach (var count in counts)
                foreach (var commiteveryn in commitEveryNs)
                {
                    if (commiteveryn > 0 && count <= commiteveryn)
                        continue;
                    yield return new BenchmarkParams { Count = count, CommitEveryN = commiteveryn, UseIndex = false, IndexAfter = false };
                    yield return new BenchmarkParams { Count = count, CommitEveryN = commiteveryn, UseIndex = false, IndexAfter = true };
                    yield return new BenchmarkParams { Count = count, CommitEveryN = commiteveryn, UseIndex = true, IndexAfter = false };
                }
        }
    }
}