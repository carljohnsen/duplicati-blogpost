using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;
using System.Data;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace sqlite_bench_old
{
    [Config(typeof(BenchmarkConfig))]
    [MinColumn, MaxColumn, AllStatisticsColumn]
    public class SQLiteSelectParallelBenchmark : SQLiteBenchmarkParallel
    {
        //[ParamsAllValues]
        public static Backends Backend { get; set; } = Backends.MicrosoftSQLite;

        [ParamsSource(nameof(ValidParams))]
        public BenchmarkParams BenchmarkParams { get; set; } = new BenchmarkParams();

        private List<(long, long, string)> entries = [];


        //[Params(0, 1_000, 10_000, 100_000)]
        [Params(1_000_000)]
        public int PreFilledCount { get; set; } = 0;

        [Params(1, 2, 4, 8)]
        public static int Parallelism { get; set; } = 8;

        public SQLiteSelectParallelBenchmark() : base()
        {

        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
        }

        private async void ListIndexAndPlan(SqliteConnection con)
        {
            using var cmd = con.CreateCommand();
            cmd.CommandText = @"PRAGMA index_list(""Blockset"")";
            using (var reader = await cmd.ExecuteReaderAsync())
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
                    cmd.Parameters.AddWithValue(argname, argval);

                using (var reader = await cmd.ExecuteReaderAsync())
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
                    } while (await reader.ReadAsync());
                }
            }

        }

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            var rng = new Random(20250411);
            using var con = await CreateConnection(Backend);
            await RunNonQueries(con, [SQLQeuriesOriginal.DropIndex, SQLQeuriesOriginal.DropTable, .. SQLQeuriesOriginal.TableQueries, .. SQLQeuriesOriginal.PragmaQueries]);
            using var cmd = con.CreateCommand();

            cmd.CommandText = "BEGIN DEFERRED TRANSACTION;";
            await cmd.ExecuteNonQueryAsync();

            var buffer = new byte[44];
            var alphanumericChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            var m_insertBlocksetManagedCommand = CreateCommand(con, SQLQeuriesOriginal.InsertBlocksetManaged);

            // Generate random data to insert
            for (long i = 0; i < BenchmarkParams.Count + PreFilledCount; i++)
            {
                rng.NextBytes(buffer);
                for (int j = 0; j < buffer.Length; j++)
                    buffer[j] = (byte)(buffer[j] % alphanumericChars.Length);

                var entry = (i + 1, rng.NextInt64() % 100, new string([.. buffer.Select(x => alphanumericChars[x])]));
                entries.Add(entry);
                m_insertBlocksetManagedCommand.Parameters["id"].Value = entry.Item1;
                m_insertBlocksetManagedCommand.Parameters["length"].Value = entry.Item2;
                m_insertBlocksetManagedCommand.Parameters["fullhash"].Value = entry.Item3;
                await m_insertBlocksetManagedCommand.ExecuteNonQueryAsync();
            }

            var m_createIndexCommand = CreateCommand(con, SQLQeuriesOriginal.CreateIndex);
            if (BenchmarkParams.UseIndex)
                await m_createIndexCommand.ExecuteNonQueryAsync();

            cmd.CommandText = "COMMIT;";
            await cmd.ExecuteNonQueryAsync();

            cmd.CommandText = "PRAGMA optimize;";
            await cmd.ExecuteNonQueryAsync();

            // Shuffle and take a subset of the entries
            entries = [.. entries.OrderBy(x => Guid.NewGuid()).Take(BenchmarkParams.Count)];

            ListIndexAndPlan(con);
            con.Close();

            await CreateConnections(Backend, Parallelism);

            //for (int i = 0; i < Parallelism; i++)
            //    RunNonQueries(cons[i], SQLQeuriesOriginal.PragmaQueries, false);

            //for (int i = 0; i < Parallelism; i++)
            //{
            //    m_selectCommands.Add(CreateCommand(cons[i], SQLQeuriesOriginal.FindBlockset));
            //}
        }

        [Benchmark]
        public async Task SelectBenchmark()
        {
            int entries_per_thread = entries.Count / Parallelism;
            var tasks = Enumerable.Range(0, Parallelism).Select(i => Task.Run(async () =>
            {
                var sw = new Stopwatch();
                int begin = i * entries_per_thread;
                int end = Math.Min((i + 1) * entries_per_thread, entries.Count);
                int n_entries = end - begin;
                var con = cons[i];
                using var cmd = con.CreateCommand();
                cmd.Transaction = con.BeginTransaction(deferred: true);
                cmd.CommandText = SQLQeuriesOriginal.FindBlockset;
                cmd.Parameters.AddWithValue("@length", 0L);
                cmd.Parameters.AddWithValue("@fullhash", string.Empty);
                cmd.Prepare();

                for (int j = begin; j < end; j++)
                {
                    var (id, length, fullhash) = entries[j];
                    cmd.Parameters["@length"].Value = length;
                    cmd.Parameters["@fullhash"].Value = fullhash;
                    sw.Start();
                    var read_res = await cmd.ExecuteScalarAsync();
                    var read_id = read_res == null ? -1 : Convert.ToInt64(read_res);
                    sw.Stop();
                    if (read_id != id)
                        throw new Exception($"ID not found for {length}, {fullhash}");
                }
                await cmd.Transaction.CommitAsync();

#if DEBUG
                Console.WriteLine($"Stepping took {sw.ElapsedMilliseconds} ms ({(n_entries / 1000) / sw.Elapsed.TotalSeconds:0.00} kops/sec)");
#endif

                transactions[i]?.Commit();
                transactions[i]?.Dispose();
                transactions[i] = null;
            }));
            await Task.WhenAll(tasks);
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