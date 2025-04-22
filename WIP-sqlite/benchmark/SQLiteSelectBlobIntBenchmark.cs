using BenchmarkDotNet.Attributes;
using Duplicati.Library.Main.Database;
using System.Data;
using System.Diagnostics;
using System.Text;

namespace sqlite_bench
{
    [Config(typeof(BenchmarkConfig))]
    [MinColumn, MaxColumn, AllStatisticsColumn]
    public class SQLiteSelectBlobIntBenchmark : SQLiteBenchmark
    {
        [ParamsAllValues]
        public static Backends Backend { get; set; }

        [ParamsSource(nameof(ValidParams))]
        public BenchmarkParams BenchmarkParams { get; set; } = new BenchmarkParams();

        private List<(long, long, byte[])> entries = [];

        private readonly IDbCommand m_createIndexCommand;
        private readonly IDbCommand m_insertBlocksetManagedCommand;
        private readonly IDbCommand m_selectCommand;
        private Stopwatch sw = new Stopwatch();

        //[Params(0, 1_000, 10_000, 100_000)]
        [Params(0)]
        public int PreFilledCount { get; set; } = 0;

        public SQLiteSelectBlobIntBenchmark() : base(Backend)
        {
            RunNonQueries([SQLQeuriesBlobInt.DropIndex, SQLQeuriesBlobInt.DropTable, .. SQLQeuriesBlobInt.TableQueries]);
            m_createIndexCommand = CreateCommand(SQLQeuriesBlobInt.CreateIndex);
            m_insertBlocksetManagedCommand = CreateCommand(SQLQeuriesBlobInt.InsertBlocksetManaged);
            m_selectCommand = CreateCommand(SQLQeuriesBlobInt.FindBlockset);
        }

        protected override void Dispose(bool disposing)
        {
            m_createIndexCommand.Dispose();
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
                    (SQLQeuriesBlobInt.FindBlockset, new(object, string)[] { (new byte[10], "firsthash") }),
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
                        Console.WriteLine();
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
            RunNonQueries([SQLQeuriesBlobInt.DropIndex, SQLQeuriesBlobInt.DropTable, .. SQLQeuriesBlobInt.TableQueries]);

            transaction = con.BeginTransaction();

            var buffer = new byte[32];

            // Generate random data to insert
            for (long i = 0; i < BenchmarkParams.Count + PreFilledCount; i++)
            {
                var hash = new byte[32];
                rng.NextBytes(hash);
                var length = rng.NextInt64() % 100;
                entries.Add((i, length, hash));
                var firsthash = BitConverter.ToInt64(hash, 0);
                Array.Copy(hash, 8, buffer, 0, 24);
                Array.Copy(BitConverter.GetBytes(length), 0, buffer, 24, 8);

                m_insertBlocksetManagedCommand.SetParameterValue("id", i);
                m_insertBlocksetManagedCommand.SetParameterValue("firsthash", firsthash);
                m_insertBlocksetManagedCommand.SetParameterValue("fullhashlength", buffer);
                m_insertBlocksetManagedCommand.ExecuteNonQuery(transaction);
            }

            if (BenchmarkParams.UseIndex)
                m_createIndexCommand.ExecuteNonQuery(transaction);

            // Shuffle and take a subset of the entries
            entries = [.. entries.OrderBy(x => Guid.NewGuid()).Take(BenchmarkParams.Count)];

            using var cmd = con.CreateCommand();
            cmd.CommandText = "PRAGMA optimize;";
            cmd.ExecuteNonQuery(transaction);
            cmd.CommandText = "ANALYZE;";
            cmd.ExecuteNonQuery(transaction);

            transaction.Commit();
            transaction.Dispose();

            cmd.CommandText = "VACUUM;";
            cmd.ExecuteNonQuery();

            ListIndexAndPlan();

            if (BenchmarkParams.UseTransaction)
                transaction = con.BeginTransaction();
        }

        public string PrettyPrintEntry(byte[] entry)
        {
            var sb = new StringBuilder();
            for (int i = 0; i < 32; i++)
            {
                sb.Append(entry[i].ToString("X2"));
            }
            long length = BitConverter.ToInt64(entry[32..40], 0);
            return $"{sb} {length}";
        }

        void PackEntry(long length, byte[] hash, byte[] entry)
        {
            Array.Copy(hash, entry, 32);
            Array.Copy(BitConverter.GetBytes(length), 0, entry, 32, 8);
        }

        [Benchmark]
        public void SelectBenchmark()
        {
            transaction ??= con.BeginTransaction();
            var buffer = new byte[32];
            var sw = new System.Diagnostics.Stopwatch();
            m_selectCommand.Prepare();
            m_selectCommand.Transaction = transaction;

            foreach (var (id, length, hash) in entries)
            {
                var firsthash = BitConverter.ToInt64(hash, 0);
                m_selectCommand.SetParameterValue("firsthash", firsthash);
                sw.Start();
                using var reader = m_selectCommand.ExecuteReader();
                sw.Stop();
                var found = false;
                long read_id = -1;
                while (reader.Read())
                {
                    var matches = true;
                    read_id = reader.GetInt64(0);
                    var aoeu = reader.GetBytes(1, 0, buffer, 0, 32);
                    var read_length = BitConverter.ToInt64(buffer[24..], 0);
                    if (read_length != length)
                        for (int i = 0; i < 24; i++)
                            matches &= buffer[i] == hash[i + 8];
                    if (matches)
                    {
                        found = true;
                        break;
                    }
                }

                if (!found || read_id != id)
                    throw new Exception($"ID not found ({read_id} != {id}) for {BitConverter.ToString(hash)}");

            }
#if DEBUG
            Console.WriteLine($"Stepping took {sw.ElapsedMilliseconds} ms ({(BenchmarkParams.Count / 1000) / sw.Elapsed.TotalSeconds:0.00} kops/sec)");
#endif
        }

        public static IEnumerable<BenchmarkParams> ValidParams()
        {
            var counts = new[] { /*100, 1_000, 10_000, 100_000,*/ 1_000_000 };

            foreach (var count in counts)
            {
                //yield return new BenchmarkParams { Count = count, CommitEveryN = 0, UseIndex = false, IndexAfter = false };
                yield return new BenchmarkParams { Count = count, CommitEveryN = 0, UseIndex = true, IndexAfter = false };
            }
        }
    }
}