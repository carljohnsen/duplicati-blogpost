using BenchmarkDotNet.Attributes;
using Duplicati.Library.Main.Database;
using System.Data;
using System.Diagnostics;
using System.Text;

namespace sqlite_bench
{
    [Config(typeof(BenchmarkConfig))]
    [MinColumn, MaxColumn, AllStatisticsColumn]
    public class SQLiteSelectBenchmark : SQLiteBenchmarkSequential
    {
        //[ParamsAllValues]
        public static Backends Backend { get; set; } = Backends.DuplicatiSQLite;

        [ParamsSource(nameof(ValidParams))]
        public BenchmarkParams BenchmarkParams { get; set; } = new BenchmarkParams();

        private List<(long, string)> entries = [];

        private readonly IDbCommand m_createIndexCommand;
        private readonly IDbCommand m_dropIndexCommand;
        private readonly IDbCommand m_dropTableCommand;
        private readonly IDbCommand m_insertBlocksetManagedCommand;
        private readonly IDbCommand m_selectCommand;
        private readonly IDbCommand m_selectHashOnlyCommand;
        private readonly IDbCommand m_selectLengthOnlyCommand;

        private Stopwatch sw = new Stopwatch();

        //[Params(0, 1_000, 10_000, 100_000)]
        [Params(1_000_000)]
        public int PreFilledCount { get; set; } = 0;

        public SQLiteSelectBenchmark() : base(Backend)
        {
            RunNonQueries([SQLQeuriesOriginal.DropIndex, SQLQeuriesOriginal.DropTable, .. SQLQeuriesOriginal.TableQueries]);
            m_createIndexCommand = CreateCommand(SQLQeuriesOriginal.CreateIndex);
            m_dropIndexCommand = CreateCommand(SQLQeuriesOriginal.DropIndex);
            m_dropTableCommand = CreateCommand(SQLQeuriesOriginal.DropTable);
            m_insertBlocksetManagedCommand = CreateCommand(SQLQeuriesOriginal.InsertBlocksetManaged);
            m_selectCommand = CreateCommand(SQLQeuriesOriginal.FindBlockset);
            m_selectHashOnlyCommand = CreateCommand(SQLQeuriesOriginal.FindBlocksetHashOnly);
            m_selectLengthOnlyCommand = CreateCommand(SQLQeuriesOriginal.FindBlocksetLengthOnly);
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
                    (SQLQeuriesOriginal.FindBlockset, new(object, string)[] { (42L, "length"), ("aoeu", "fullhash") }),
                    (SQLQeuriesOriginal.FindBlocksetHashOnly, new(object, string)[] { ("aoeu", "fullhash") }),
                    (SQLQeuriesOriginal.FindBlocksetLengthOnly, new(object, string)[] { (42L, "length") }),
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
            var rng = new Random(20250411);
            // TODO queries.pragma queries has been split up.
            RunNonQueries([SQLQeuriesOriginal.DropIndex, SQLQeuriesOriginal.DropTable, .. SQLQeuriesOriginal.TableQueries]);

            transaction = con.BeginTransaction();

            var buffer = new byte[44];
            var alphanumericChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

            // Generate random data to insert
            for (long i = 0; i < BenchmarkParams.Count + PreFilledCount; i++)
            {
                rng.NextBytes(buffer);
                for (int j = 0; j < buffer.Length; j++)
                    buffer[j] = (byte)(buffer[j] % alphanumericChars.Length);

                var entry = (rng.NextInt64() % 100, new string([.. buffer.Select(x => alphanumericChars[x])]));
                entries.Add(entry);
                m_insertBlocksetManagedCommand.SetParameterValue("id", i);
                m_insertBlocksetManagedCommand.SetParameterValue("length", entry.Item1);
                m_insertBlocksetManagedCommand.SetParameterValue("fullhash", entry.Item2);
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
                m_selectCommand.SetParameterValue("fullhash", fullhash);
                sw.Start();
                var id = m_selectCommand.ExecuteScalarInt64(transaction);
                sw.Stop();
                if (id < 0)
                    throw new Exception($"ID not found for {length}, {fullhash}");
            }

#if DEBUG
            Console.WriteLine($"Stepping took {sw.ElapsedMilliseconds} ms ({(BenchmarkParams.Count / 1000) / sw.Elapsed.TotalSeconds:0.00} kops/sec)");
#endif
        }

        //[Benchmark]
        public void SelectHashOnlyBenchmark()
        {
            transaction ??= con.BeginTransaction();

            for (int i = 0; i < entries.Count; i++)
            {
                var (length, fullhash) = entries[i];
                m_selectHashOnlyCommand.SetParameterValue("fullhash", fullhash);
                using var reader = m_selectHashOnlyCommand.ExecuteReader();
                bool found = false;
                while (reader.Read())
                {
                    var read_id = reader.GetInt64(0);
                    var read_length = reader.GetInt64(1);
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

        //[Benchmark]
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
            var counts = new[] { 1_000_000 }; //, 1_000, 10_000 }; //, 100_000, 1_000_000 };

            foreach (var count in counts)
            {
                //yield return new BenchmarkParams { Count = count, CommitEveryN = 0, UseIndex = false, IndexAfter = false };
                yield return new BenchmarkParams { Count = count, CommitEveryN = 0, UseIndex = true, IndexAfter = false };
            }
        }
    }
}