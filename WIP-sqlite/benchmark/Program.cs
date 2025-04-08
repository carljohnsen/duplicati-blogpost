using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using Duplicati.Library.Main.Database;
using System.Data;

namespace sqlite_bench
{

    public class Program
    {
        static void Main(string[] args)
        {
#if DEBUG
            /* // Insertion benchmark
            using var b = new SQLiteInsertBenchmark();
            b.GlobalSetup();
            b.IterationSetup();
            b.FillManaged();
            //b.FillReturning();
            //b.FillSelect();
            //b.IterationCleanup();
            //b.GlobalCleanup();
            */

            // Selection benchmark
            using var b = new SQLiteSelectBenchmark();
            Console.WriteLine("GlobalSetup...");
            b.PreFilledCount = 1000;
            b.BenchmarkParams.Count = 100;
            b.GlobalSetup();
            Console.WriteLine("Running SelectBenchmark...");
            b.SelectBenchmark();
            b.SelectHashOnlyBenchmark();
            b.SelectLengthOnlyBenchmark();
            Console.WriteLine("Done!");
#else
            var summary = BenchmarkRunner.Run<SQLiteSelectBenchmark>();
#endif
        }
    }

    public enum Backends
    {
        DuplicatiSQLite,
        //MicrosoftSQLite,
        //SystemSQLite,
        //Dictionary
    };

    public class BenchmarkParams
    {
        public int Count { get; set; } = 1000;
        public int CommitEveryN { get; set; } = 1;
        public bool UseIndex { get; set; } = true;
        public bool IndexAfter { get; set; }
        public bool UseTransaction { get; set; } = true;

        public override string ToString() => $"Count={Count}, CommitEveryN={CommitEveryN}, UseIndex={UseIndex}, IndexAfter={IndexAfter}";
    }

    public class BenchmarkConfig : ManualConfig
    {
        public BenchmarkConfig()
        {
            AddColumn(new ThroughputColumn());
            SummaryStyle = new SummaryStyle(null, true, Perfolizer.Metrology.SizeUnit.B, Perfolizer.Horology.TimeUnit.Nanosecond, true)
                .WithMaxParameterColumnWidth(int.MaxValue) // <-- prevents shortening
                .WithRatioStyle(RatioStyle.Trend);          // optional, for better readability
        }
    }

    public static class SQLQeuries
    {
        public static readonly string CreateIndex = @"
        CREATE UNIQUE INDEX IF NOT EXISTS ""BlocksetLengthHash"" ON ""Blockset"" (""Length"", ""FullHash"");
        CREATE INDEX IF NOT EXISTS ""BlocksetHash"" ON ""Blockset"" (""FullHash"");
        CREATE INDEX IF NOT EXISTS ""BlocksetLength"" ON ""Blockset"" (""Length"");";

        public static readonly string DropAllRows = @"DELETE FROM ""Blockset"" WHERE ""ID"" >= @id";
        public static readonly string DropIndex = @"DROP INDEX IF EXISTS ""BlocksetLengthHash""; DROP INDEX IF EXISTS ""BlocksetHash""; DROP INDEX IF EXISTS ""BlocksetLength"";";

        public static readonly string FindBlockset = @"SELECT ""ID"" FROM ""Blockset"" WHERE ""Length"" = @length AND ""FullHash"" = @hash";
        public static readonly string FindBlocksetHashOnly = @"SELECT ""ID"", ""Length"" FROM ""Blockset"" WHERE ""FullHash"" = @hash";
        public static readonly string FindBlocksetLengthOnly = @"SELECT ""ID"", ""FullHash"" FROM ""Blockset"" WHERE ""Length"" = @length";

        public static readonly string FlushTemp = @"INSERT INTO ""Blockset"" (""ID"", ""Length"", ""FullHash"") SELECT ""ID"", ""Length"", ""FullHash"" FROM ""BlocksetTmp""; DROP TABLE IF EXISTS ""BlocksetTmp""";
        public static readonly string FlushTempSorted = @"INSERT INTO ""Blockset"" (""ID"", ""Length"", ""FullHash"") SELECT ""ID"", ""Length"", ""FullHash"" FROM ""BlocksetTmp"" ORDER BY ""Length"" ASC, ""FullHash"" ASC; DROP TABLE IF EXISTS ""BlocksetTmp""";

        public static readonly string Index = @"CREATE INDEX IF NOT EXISTS ""BlocksetLengthHash"" ON ""Blockset"" (""Length"", ""FullHash"");";

        public static readonly string InsertBlocksetManaged = @"INSERT INTO ""Blockset"" (""ID"", ""Length"", ""FullHash"") VALUES (@id, @length, @hash);";
        public static readonly string InsertBlocksetSelect = @"INSERT INTO ""Blockset"" (""Length"", ""FullHash"") VALUES (@length, @hash); SELECT last_insert_rowid()";
        public static readonly string InsertBlocksetReturning = @"INSERT INTO ""Blockset"" (""Length"", ""FullHash"") VALUES (@length, @hash) RETURNING ""ID""";

        public static readonly string InsertTempManaged = @"INSERT INTO ""BlocksetTmp"" (""ID"", ""Length"", ""FullHash"") VALUES (@id, @length, @hash);";

        public static readonly string LastRowId = @"SELECT ""ID"" FROM ""Blockset"" ORDER BY ""ID"" DESC LIMIT 1";

        public static readonly string TempTable = @"CREATE TEMP TABLE IF NOT EXISTS ""BlocksetTmp"" (""ID"" INTEGER PRIMARY KEY, ""Length"" INTEGER NOT NULL, ""FullHash"" TEXT NOT NULL)";

        public static readonly string[] TableQueries = [
                @"CREATE TABLE IF NOT EXISTS ""Blockset"" (""ID"" INTEGER PRIMARY KEY, ""Length"" INTEGER NOT NULL, ""FullHash"" TEXT NOT NULL)",
                //"PRAGMA synchronous = OFF",
                //"PRAGMA journal_mode = OFF",
                "PRAGMA cache_size = 1000000",
                //"PRAGMA threads = 8",
            ];
    }

    public class SQLiteBenchmark : IDisposable
    {
        // The SQLite connection
        protected IDbConnection con;

        protected IDbTransaction? transaction;

        protected long last_id = -1;

        public SQLiteBenchmark(Backends backend)
        {
            var data_source = "testdb.sqlite";

            switch (backend)
            {
                case Backends.DuplicatiSQLite:
                    con = Duplicati.Library.SQLiteHelper.SQLiteLoader.LoadConnection();
                    con.Close();
                    con.ConnectionString = $"Data Source={data_source}";
                    con.Open();
                    break;
                //case Backends.MicrosoftSQLite:
                //    con = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={data_source}");
                //    con.Open();
                //    break;
                //case Backends.SystemSQLite:
                //    con = new System.Data.SQLite.SQLiteConnection($"Data Source={data_source}");
                //    con.Open();
                //    break;
                //case Backends.Dictionary:
                //    con = new Dictionary<string, string>();
                //    break;
                default:
                    throw new NotImplementedException();
            }
            CreateTables();
        }

        protected IDbCommand CreateCommand(string query)
        {
            var cmd = con.CreateCommand();
            cmd.CommandText = query;
            var parameters = query.Split()
                .Where(s => s.Contains('@'))
                .Select(s => new string([.. s.Where(c => char.IsLetterOrDigit(c))]));

            foreach (var param in parameters)
            {
                var parameter = cmd.CreateParameter();
                parameter.ParameterName = param;
                cmd.Parameters.Add(parameter);
            }

            cmd.Prepare();
            return cmd;
        }

        private void CreateTables()
        {
            using IDbCommand cmd = con.CreateCommand();
            foreach (var query in SQLQeuries.TableQueries)
            {
                cmd.CommandText = query;
                cmd.ExecuteNonQuery();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool _)
        {
            try
            {
                transaction?.Commit();
            }
            catch (InvalidOperationException)
            {
                // Ignore, transaction already committed
            }
            catch (System.Data.SQLite.SQLiteException)
            {
                // Ignore, transaction already committed
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            transaction?.Dispose();
            con.Close();
            con.Dispose();
        }

        protected void DropRows()
        {
            using IDbCommand cmd = con.CreateCommand();
            cmd.CommandText = SQLQeuries.DropAllRows;
            cmd.AddNamedParameter("id", last_id);
            cmd.ExecuteNonQuery();
        }

        protected long GetLastRowId()
        {
            using var cmd = con.CreateCommand();
            cmd.CommandText = SQLQeuries.LastRowId;
            return cmd.ExecuteScalarInt64();
        }
    }

    [Config(typeof(BenchmarkConfig))]
    [MinColumn, MaxColumn, AllStatisticsColumn]
    public class SQLiteInsertBenchmark : SQLiteBenchmark
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
            m_insertBlocksetManagedCommand = CreateCommand(SQLQeuries.InsertBlocksetManaged);
            m_insertBlocksetSelectCommand = CreateCommand(SQLQeuries.InsertBlocksetSelect);
            m_insertBlocksetReturningCommand = CreateCommand(SQLQeuries.InsertBlocksetReturning);
            m_insertBlocksetTempManagedCommand = CreateCommand(SQLQeuries.InsertTempManaged);
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
                cmd.CommandText = SQLQeuries.Index;
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
            cmd.CommandText = SQLQeuries.TempTable;
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

            cmd.CommandText = SQLQeuries.FlushTemp;
            cmd.ExecuteNonQuery(transaction);

            CheckIfCreateIndex();
        }

        [Benchmark]
        public void FillTempSorted()
        {
            transaction ??= con.BeginTransaction();

            using var cmd = con.CreateCommand();
            cmd.CommandText = SQLQeuries.TempTable;
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

            cmd.CommandText = SQLQeuries.FlushTempSorted;
            cmd.ExecuteNonQuery(transaction);

            CheckIfCreateIndex();
        }

        [Benchmark]
        public void FillTempSortedInMemory()
        {
            transaction ??= con.BeginTransaction();

            using var cmd = con.CreateCommand();
            cmd.CommandText = SQLQeuries.TempTable;
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

            cmd.CommandText = SQLQeuries.FlushTemp;
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
                cmd.CommandText = SQLQeuries.CreateIndex;
                cmd.ExecuteNonQuery();
            }
            else
            {
                cmd.CommandText = SQLQeuries.DropIndex;
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

    [Config(typeof(BenchmarkConfig))]
    [MinColumn, MaxColumn, AllStatisticsColumn]
    public class SQLiteSelectBenchmark : SQLiteBenchmark
    {
        [ParamsAllValues]
        public static Backends Backend { get; set; }

        [ParamsSource(nameof(ValidParams))]
        public BenchmarkParams BenchmarkParams { get; set; } = new BenchmarkParams();

        private List<(long, string)> entries = [];

        private readonly IDbCommand m_createIndexCommand;
        private readonly IDbCommand m_dropIndexCommand;
        private readonly IDbCommand m_insertBlocksetManagedCommand;
        private readonly IDbCommand m_selectCommand;
        private readonly IDbCommand m_selectHashOnlyCommand;
        private readonly IDbCommand m_selectLengthOnlyCommand;

        //[Params(0, 1000, 10000, 100000)]
        [Params(100000)]
        public int PreFilledCount { get; set; } = 0;

        public SQLiteSelectBenchmark() : base(Backend)
        {
            m_createIndexCommand = CreateCommand(SQLQeuries.CreateIndex);
            m_dropIndexCommand = CreateCommand(SQLQeuries.DropIndex);
            m_insertBlocksetManagedCommand = CreateCommand(SQLQeuries.InsertBlocksetManaged);
            m_selectCommand = CreateCommand(SQLQeuries.FindBlockset);
            m_selectHashOnlyCommand = CreateCommand(SQLQeuries.FindBlocksetHashOnly);
            m_selectLengthOnlyCommand = CreateCommand(SQLQeuries.FindBlocksetLengthOnly);
        }

        protected override void Dispose(bool disposing)
        {
            m_createIndexCommand.Dispose();
            m_dropIndexCommand.Dispose();
            m_insertBlocksetManagedCommand.Dispose();
            m_selectCommand.Dispose();
            base.Dispose(disposing);
        }

        private void ListIndexAndPlan()
        {
            using var cmd = con.CreateCommand();
            cmd.CommandText = @"PRAGMA index_list(""Blockset"")";
            using (var reader = cmd.ExecuteReader())
            {
                var msg = $"Output when listing indexes on Blockset table:{Environment.NewLine}";
                while (reader.Read())
                {
                    msg += reader.GetString(1);
                    msg += Environment.NewLine;
                }
                msg += "End of output";

                Console.WriteLine(msg);
            }

            foreach (var (query, args) in new[] {
                    (SQLQeuries.FindBlockset, new(object, string)[] { (42L, "length"), ("aoeu", "hash") }),
                    (SQLQeuries.FindBlocksetHashOnly, new(object, string)[] { ("aoeu", "hash") }),
                    (SQLQeuries.FindBlocksetLengthOnly, new(object, string)[] { (42L, "length") }),
                })
            {
                cmd.CommandText = $"EXPLAIN QUERY PLAN {query}";
                foreach (var (argval, argname) in args)
                    cmd.AddNamedParameter(argname, argval);

                using (var reader = cmd.ExecuteReader())
                {
                    if (!reader.Read())
                    {
                        Console.WriteLine($"No rows returned for {query}");
                        continue;
                    }
                    do
                    {
                        Console.WriteLine($"Query: {query}");
                        Console.WriteLine($"{reader.GetString(3)}");
                        //for (int i = 0; i < reader.FieldCount; i++)
                        //{
                        //    Type fieldType = reader.GetFieldType(i);
                        //    object value = reader.GetValue(i);
                        //    Console.WriteLine($"Column {i}: Type={fieldType.Name}, Value={value}");
                        //}
                        break;
                    } while (reader.Read());
                }
            }

        }

        [GlobalSetup]
        public void GlobalSetup()
        {
            var rng = new Random();
            transaction = con.BeginTransaction();
            m_dropIndexCommand.ExecuteNonQuery();
            DropRows();

            // Generate random data to insert
            for (long i = 0; i < BenchmarkParams.Count + PreFilledCount; i++)
            {
                var entry = (rng.NextInt64() % 100, Guid.NewGuid().ToString());
                entries.Add(entry);
                m_insertBlocksetManagedCommand.SetParameterValue("id", i);
                m_insertBlocksetManagedCommand.SetParameterValue("length", entry.Item1);
                m_insertBlocksetManagedCommand.SetParameterValue("hash", entry.Item2);
                m_insertBlocksetManagedCommand.ExecuteNonQuery(transaction);
            }

            if (BenchmarkParams.UseIndex)
                m_createIndexCommand.ExecuteNonQuery(transaction);

            // Shuffle and take a subset of the entries
            entries = [.. entries.OrderBy(x => Guid.NewGuid()).Take(BenchmarkParams.Count)];

            transaction.Commit();
            transaction.Dispose();

            ListIndexAndPlan();

            if (BenchmarkParams.UseTransaction)
                transaction = con.BeginTransaction();

        }

        [Benchmark]
        public void SelectBenchmark()
        {
            transaction ??= con.BeginTransaction();

            for (int i = 0; i < entries.Count; i++)
            {
                var (length, fullhash) = entries[i];
                m_selectCommand.SetParameterValue("length", length);
                m_selectCommand.SetParameterValue("hash", fullhash);
                var id = m_selectCommand.ExecuteScalarInt64(transaction);
                if (id < 0)
                    throw new Exception($"ID not found for {length}, {fullhash}");
            }
        }

        [Benchmark]
        public void SelectHashOnlyBenchmark()
        {
            transaction ??= con.BeginTransaction();

            for (int i = 0; i < entries.Count; i++)
            {
                var (length, fullhash) = entries[i];
                m_selectHashOnlyCommand.SetParameterValue("hash", fullhash);
                using var reader = m_selectHashOnlyCommand.ExecuteReader();
                bool found = false;
                while (reader.Read())
                {
                    var read_id = reader.GetInt64(0);
                    var read_length = reader.GetInt64(1);
                    found = true; break;
                    if (read_length == length)
                    {
                        found = true;
                        break;
                    }
                }
                if (!found)
                    throw new Exception($"Hash {fullhash} not found");
            }
        }

        [Benchmark]
        public void SelectLengthOnlyBenchmark()
        {
            transaction ??= con.BeginTransaction();

            for (int i = 0; i < entries.Count; i++)
            {
                var (length, fullhash) = entries[i];
                m_selectLengthOnlyCommand.SetParameterValue("length", length);
                using var reader = m_selectLengthOnlyCommand.ExecuteReader();
                bool found = false;
                while (reader.Read())
                {
                    var read_id = reader.GetInt64(0);
                    var hash = reader.GetString(1);
                    found = true; break;
                    if (hash == fullhash)
                    {
                        found = true;
                        break;
                    }
                }
                if (!found)
                    throw new Exception($"Length {length} not found");
            }
        }

        public static IEnumerable<BenchmarkParams> ValidParams()
        {
            var counts = new[] { 10000 }; //, 1000, 10000 }; //, 100000, 1000000 };

            foreach (var count in counts)
            {
                //yield return new BenchmarkParams { Count = count, CommitEveryN = 0, UseIndex = false, IndexAfter = false };
                yield return new BenchmarkParams { Count = count, CommitEveryN = 0, UseIndex = true, IndexAfter = false };
            }
        }
    }

    public class ThroughputColumn : IColumn
    {
        public string Id => nameof(ThroughputColumn);
        public string ColumnName => "Throughput (stmts/sec)";

        public bool IsAvailable(Summary summary) => true;
        public bool AlwaysShow => true;
        public ColumnCategory Category => ColumnCategory.Custom;
        public int PriorityInCategory => 1;
        public bool IsNumeric => true;
        public UnitType UnitType => UnitType.Dimensionless;
        public string Legend => "Operations per second (calculated as 1e9 / (Mean ns / Count))";

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase)
        {
            var statistics = summary[benchmarkCase]?.ResultStatistics;
            if (statistics == null) return "N/A";

            // Extract the Count parameter
            var count = (benchmarkCase.Parameters.Items.Where(p => p.Value is BenchmarkParams).First().Value as BenchmarkParams)?.Count ?? 1;
            double meanTime = statistics.Mean;

            if (meanTime <= 0 || count <= 0)
                return "N/A";

            double throughput = 1e9 / (meanTime / count);
            return throughput.ToString("N2");
        }

        public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;
        public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style) => GetValue(summary, benchmarkCase);

    }

}