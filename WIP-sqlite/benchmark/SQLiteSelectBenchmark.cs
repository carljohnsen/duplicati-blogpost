using BenchmarkDotNet.Attributes;
using Duplicati.Library.Main.Database;
using System.Data;

namespace sqlite_bench
{
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
}