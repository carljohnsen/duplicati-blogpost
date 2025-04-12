using BenchmarkDotNet.Attributes;
using Duplicati.Library.Main.Database;
using System.Data;
using System.Text;

namespace sqlite_bench
{
    [Config(typeof(BenchmarkConfig))]
    [MinColumn, MaxColumn, AllStatisticsColumn]
    public class SQLiteSelectBlobBenchmark : SQLiteBenchmark
    {
        [ParamsAllValues]
        public static Backends Backend { get; set; }

        [ParamsSource(nameof(ValidParams))]
        public BenchmarkParams BenchmarkParams { get; set; } = new BenchmarkParams();

        private List<(long, byte[])> entries = [];

        private readonly IDbCommand m_createIndexCommand;
        private readonly IDbCommand m_insertBlocksetManagedCommand;
        private readonly IDbCommand m_selectCommand;

        //[Params(0, 1_000, 10_000, 100_000)]
        [Params(1_000_000)]
        public int PreFilledCount { get; set; } = 0;

        public SQLiteSelectBlobBenchmark() : base(Backend)
        {
            RunNonQueries([SQLQeuriesBlob.DropIndex, SQLQeuriesBlob.DropTable, .. SQLQeuriesBlob.TableQueries]);
            m_createIndexCommand = CreateCommand(SQLQeuriesBlob.CreateIndex);
            m_insertBlocksetManagedCommand = CreateCommand(SQLQeuriesBlob.InsertBlocksetManaged);
            m_selectCommand = CreateCommand(SQLQeuriesBlob.FindBlockset);
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
                    (SQLQeuriesBlob.FindBlockset, new(object, string)[] { (new byte[10], "fullhashlength") }),
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
            RunNonQueries([SQLQeuriesBlob.DropIndex, SQLQeuriesBlob.DropTable, .. SQLQeuriesBlob.TableQueries]);

            transaction = con.BeginTransaction();

            var buffer = new byte[40];

            // Generate random data to insert
            for (long i = 0; i < BenchmarkParams.Count + PreFilledCount; i++)
            {
                var hash_buffer = new byte[40];
                rng.NextBytes(hash_buffer);
                var length = rng.NextInt64() % 100;
                var entry = (length, hash_buffer);
                PackEntry(length, hash_buffer, buffer);

                entries.Add(entry);
                m_insertBlocksetManagedCommand.SetParameterValue("id", i);
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

            byte[] buffer = new byte[40];

            foreach (var (length, hash) in entries)
            {
                PackEntry(length, hash, buffer);
                m_selectCommand.SetParameterValue("fullhashlength", buffer);
                var id = m_selectCommand.ExecuteScalarInt64(transaction);
                if (id < 0)
                    throw new Exception($"ID not found for {buffer}");
            }
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