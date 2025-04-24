using BenchmarkDotNet.Attributes;
using Duplicati.Library.Main.Database;
using System.Data;
using System.Diagnostics;
using System.Text;

namespace sqlite_bench
{
    [Config(typeof(BenchmarkConfig))]
    [MinColumn, MaxColumn, AllStatisticsColumn]
    public class SQLiteSelectParallelBenchmark : SQLiteBenchmarkParallel
    {
        //[ParamsAllValues]
        public static Backends Backend { get; set; } = Backends.DuplicatiSQLite;

        [ParamsSource(nameof(ValidParams))]
        public BenchmarkParams BenchmarkParams { get; set; } = new BenchmarkParams();

        private List<(long, long, string)> entries = [];

        private readonly IDbCommand m_createIndexCommand;
        private readonly IDbCommand m_dropIndexCommand;
        private readonly IDbCommand m_insertBlocksetManagedCommand;
        private readonly List<IDbCommand> m_selectCommands = [];

        //[Params(0, 1_000, 10_000, 100_000)]
        [Params(1_000_000)]
        public int PreFilledCount { get; set; } = 0;

        [Params(1, 2, 4, 8)]
        public static int Parallelism { get; set; } = 1;

        public SQLiteSelectParallelBenchmark() : base(Backend, Parallelism)
        {
            m_createIndexCommand = CreateCommand(cons[0], SQLQeuriesOriginal.CreateIndex);
            m_dropIndexCommand = CreateCommand(cons[0], SQLQeuriesOriginal.DropIndex);
            m_insertBlocksetManagedCommand = CreateCommand(cons[0], SQLQeuriesOriginal.InsertBlocksetManaged);
            for (int i = 0; i < Parallelism; i++)
            {
                m_selectCommands.Add(CreateCommand(cons[i], SQLQeuriesOriginal.FindBlockset));
            }
        }

        protected override void Dispose(bool disposing)
        {
            m_createIndexCommand.Dispose();
            m_dropIndexCommand.Dispose();
            m_insertBlocksetManagedCommand.Dispose();
            for (int i = 0; i < Parallelism; i++)
            {
                m_selectCommands[i].Dispose();
            }
            base.Dispose(disposing);
        }

        private void ListIndexAndPlan()
        {
            using var cmd = cons[0].CreateCommand();
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
            RunNonQueries(cons[0], [SQLQeuriesOriginal.DropIndex, SQLQeuriesOriginal.DropTable, .. SQLQeuriesOriginal.TableQueries], true);

            transactions[0] = cons[0].BeginTransaction();

            var buffer = new byte[44];
            var alphanumericChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

            // Generate random data to insert
            for (long i = 0; i < BenchmarkParams.Count + PreFilledCount; i++)
            {
                rng.NextBytes(buffer);
                for (int j = 0; j < buffer.Length; j++)
                    buffer[j] = (byte)(buffer[j] % alphanumericChars.Length);

                var entry = (i + 1, rng.NextInt64() % 100, new string([.. buffer.Select(x => alphanumericChars[x])]));
                entries.Add(entry);
                m_insertBlocksetManagedCommand.SetParameterValue("id", entry.Item1);
                m_insertBlocksetManagedCommand.SetParameterValue("length", entry.Item2);
                m_insertBlocksetManagedCommand.SetParameterValue("fullhash", entry.Item3);
                m_insertBlocksetManagedCommand.ExecuteNonQuery(transactions[0]);
            }

            if (BenchmarkParams.UseIndex)
                m_createIndexCommand.ExecuteNonQuery(transactions[0]);

            // Shuffle and take a subset of the entries
            entries = [.. entries.OrderBy(x => Guid.NewGuid()).Take(BenchmarkParams.Count)];

            transactions[0]?.Commit();
            transactions[0]?.Dispose();
            transactions[0] = null;

            for (int i = 0; i < Parallelism; i++)
                RunNonQueries(cons[i], SQLQeuriesOriginal.PragmaQueries, false);

            ListIndexAndPlan();
        }

        [Benchmark]
        public void SelectBenchmark()
        {
            int entries_per_thread = entries.Count / Parallelism;
            Parallel.For(0, Parallelism, i =>
            {
                var sw = new Stopwatch();
                int begin = i * entries_per_thread;
                int end = Math.Min((i + 1) * entries_per_thread, entries.Count);
                int n_entries = end - begin;
                //transactions[i] ??= cons[i].BeginTransaction();
                using var cmd = cons[i].CreateCommand();
                cmd.ExecuteNonQuery("BEGIN DEFERRED TRANSACTION;");
                cmd.CommandText = SQLQeuriesOriginal.FindBlockset;
                cmd.AddNamedParameter("length", 0L);
                cmd.AddNamedParameter("fullhash", string.Empty);
                cmd.Prepare();

                for (int j = begin; j < end; j++)
                {
                    var (id, length, fullhash) = entries[j];
                    cmd.SetParameterValue("length", length);
                    cmd.SetParameterValue("fullhash", fullhash);
                    sw.Start();
                    var read_id = cmd.ExecuteScalarInt64();
                    sw.Stop();
                    if (read_id != id)
                        throw new Exception($"ID not found for {length}, {fullhash}");
                }
                cmd.ExecuteNonQuery("COMMIT;");

#if DEBUG
                Console.WriteLine($"Stepping took {sw.ElapsedMilliseconds} ms ({(n_entries / 1000) / sw.Elapsed.TotalSeconds:0.00} kops/sec)");
#endif

                transactions[i]?.Commit();
                transactions[i]?.Dispose();
                transactions[i] = null;
            });
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