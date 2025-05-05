using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;
using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace sqlite_bench
{
    [Config(typeof(BenchmarkConfig))]
    [MinColumn, MaxColumn, AllStatisticsColumn]
    public class SQLiteSelectOrInsertParallelBenchmark : SQLiteBenchmarkParallel
    {
        //[ParamsAllValues]
        public static Backends Backend { get; set; } = Backends.MicrosoftSQLite;

        [ParamsSource(nameof(ValidParams))]
        public BenchmarkParams BenchmarkParams { get; set; } = new BenchmarkParams();

        private List<(long, long, string)> entries = [];
        private ConcurrentDictionary<(long, string), long> entry_batch = [];

        //[Params(0, 1_000, 10_000, 100_000)]
        [Params(1_000_000)]
        public int PreFilledCount { get; set; } = 0;

        //[Params(1, 2, 4, 8)]
        [Params(1)]
        public static int Parallelism { get; set; } = 1;

        [Params(1)]//, 1000, 10000)]
        public static int BatchSize { get; set; } = 1;

        public SQLiteSelectOrInsertParallelBenchmark() : base()
        {

        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
        }

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            entries.Clear();
            var rng = new Random(20250411);
            var con = await CreateConnection(Backend);
            await RunNonQueries(con, [SQLQeuriesOriginal.DropIndex, SQLQeuriesOriginal.DropTable, .. SQLQeuriesOriginal.TableQueries, .. SQLQeuriesOriginal.PragmaQueries]);
            var cmd = con.CreateCommand();

            cmd.Transaction = con.BeginTransaction();

            var buffer = new byte[44];
            var alphanumericChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            var m_insertBlocksetManagedCommand = CreateCommand(con, SQLQeuriesOriginal.InsertBlocksetManaged);

            // Gnerate random data to insert
            List<(long, long, string)> existing_entries = [];
            for (long i = 0; i < PreFilledCount; i++)
            {
                rng.NextBytes(buffer);
                for (int j = 0; j < buffer.Length; j++)
                    buffer[j] = (byte)(buffer[j] % alphanumericChars.Length);

                var entry = (i + 1, rng.NextInt64() % 100, new string([.. buffer.Select(x => alphanumericChars[x])]));
                existing_entries.Add(entry);
                m_insertBlocksetManagedCommand.Parameters["id"].Value = entry.Item1;
                m_insertBlocksetManagedCommand.Parameters["length"].Value = entry.Item2;
                m_insertBlocksetManagedCommand.Parameters["fullhash"].Value = entry.Item3;
                await m_insertBlocksetManagedCommand.ExecuteNonQueryAsync();
            }

            var m_createIndexCommand = CreateCommand(con, SQLQeuriesOriginal.CreateIndex);
            if (BenchmarkParams.UseIndex)
                await m_createIndexCommand.ExecuteNonQueryAsync();

            cmd.Transaction.Commit();

            cmd.Transaction = con.BeginTransaction();
            cmd.CommandText = "PRAGMA optimize;";
            await cmd.ExecuteNonQueryAsync();

            int n_existing = (int)Math.Round(BenchmarkParams.Count * 0.5);
            int n_new = (int)Math.Round(BenchmarkParams.Count * 0.25);
            int n_duplicate = (int)Math.Round(BenchmarkParams.Count * 0.25);

            Console.WriteLine($"Existing: {n_existing}, New: {n_new}, Duplicate: {n_duplicate}");

            // Shuffle and take a subset of the entries consisting of 50 % of the existing entries
            entries.AddRange(existing_entries.OrderBy(x => Guid.NewGuid()).Take(n_existing));

            // Generate another 25 % of random data to insert
            for (long i = 0; i < n_new; i++)
            {
                rng.NextBytes(buffer);
                for (int j = 0; j < buffer.Length; j++)
                    buffer[j] = (byte)(buffer[j] % alphanumericChars.Length);

                var entry = (-1, rng.NextInt64() % 100, new string([.. buffer.Select(x => alphanumericChars[x])]));
                entries.Add(entry);
            }

            // Duplicate the remaining 25 %
            entries.AddRange(entries.OrderBy(x => Guid.NewGuid()).Take(n_duplicate));

            // Shuffle the entries
            entries = [.. entries.OrderBy(x => Guid.NewGuid())];

            cmd.Dispose();
            con.Close();
            con.Dispose();

            File.Copy(data_source, $"{data_source}.bak", true);
        }

        [IterationSetup]
        public void IterationSetup()
        {
            entry_batch.Clear();
            File.Copy($"{data_source}.bak", data_source, true);
            CreateConnections(Backend, Parallelism).Wait();
        }

        [IterationCleanup]
        public void IterationCleanup()
        {
            for (int i = 0; i < Parallelism; i++)
            {
                cons[i].Close();
                cons[i].Dispose();
            }
            cons.Clear();
        }

        [Benchmark]
        public async Task SelectBenchmark()
        {
            int entries_per_thread = entries.Count / Parallelism;
            var last_id = await GetLastRowId();
            var tasks = Enumerable.Range(0, Parallelism).Select(i => Task.Run(async () =>
            {
                var sw = new Stopwatch();
                int begin = i * entries_per_thread;
                int end = Math.Min((i + 1) * entries_per_thread, entries.Count);
                int n_entries = end - begin;
#if DEBUG
                Console.WriteLine($"Thread {i}: {begin} - {end} ({n_entries}) ({entries.Count})");
#endif
                var con = cons[i];
                using var cmd = CreateCommand(con, SQLQeuriesOriginal.FindBlockset);
                cmd.Transaction = con.BeginTransaction(deferred: true);
                using var cmd2 = CreateCommand(con, SQLQeuriesOriginal.InsertBlocksetManaged);

                for (int j = begin; j < end; j++)
                {
                    var (id, length, fullhash) = entries[j];

                    sw.Start();
                    cmd.Parameters["length"].Value = length;
                    cmd.Parameters["fullhash"].Value = fullhash;
                    var read_res = await cmd.ExecuteScalarAsync();
                    var read_id = read_res == null ? -1 : Convert.ToInt64(read_res);
                    sw.Stop();

                    if (read_id < 0)
                    {
                        await cmd.Transaction.CommitAsync();
                        cmd2.Transaction = con.BeginTransaction();
                        cmd2.Parameters["id"].Value = ++last_id;
                        cmd2.Parameters["length"].Value = length;
                        cmd2.Parameters["fullhash"].Value = fullhash;
                        await cmd2.ExecuteNonQueryAsync();
                        await cmd2.Transaction.CommitAsync();
                        cmd.Transaction = con.BeginTransaction(deferred: true);
                    }
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
            //var counts = new[] { 1_000, 10_000, 100_000, 1_000_000 };
            var counts = new[] { 100_000, 1_000_000 };

            foreach (var count in counts)
            {
                //yield return new BenchmarkParams { Count = count, CommitEveryN = 0, UseIndex = false, IndexAfter = false };
                yield return new BenchmarkParams { Count = count, CommitEveryN = 0, UseIndex = true, IndexAfter = false };
            }
        }
    }
}