using BenchmarkDotNet.Attributes;
using Duplicati.Library.Main.Database;
using Microsoft.Extensions.Caching.Memory;
using System.Data;
using System.Text;

namespace sqlite_bench
{
    [Config(typeof(BenchmarkConfig))]
    [MinColumn, MaxColumn, AllStatisticsColumn]
    public class SQLiteSelectHashIntColumnBenchmark : SQLiteBenchmark
    {
        [ParamsAllValues]
        public static Backends Backend { get; set; } = Backends.SystemSQLite;

        [ParamsSource(nameof(ValidParams))]
        public BenchmarkParams BenchmarkParams { get; set; } = new BenchmarkParams();

        private List<(long, string)> entries = [];

        private readonly IDbCommand m_createIndexCommand;
        private readonly IDbCommand m_dropIndexCommand;
        private readonly IDbCommand m_dropTableCommand;
        private readonly IDbCommand m_insertBlocksetManagedCommand;
        private readonly IDbCommand m_selectCommand;
        private readonly IDbCommand m_selectFullHashOnlyCommand;
        private readonly IDbCommand m_selectLengthOnlyCommand;
        private readonly IDbCommand m_selectHashOnlyIntCommand;
        private readonly IDbCommand m_selectHashIntLengthCommand;

        //[Params(0, 1_000, 10_000, 100_000)]
        [Params(1_000_000)]
        public int PreFilledCount { get; set; } = 0;

        public SQLiteSelectHashIntColumnBenchmark() : base(Backend)
        {
            RunNonQueries([SQLQeuriesHashIntColumn.DropIndex, SQLQeuriesHashIntColumn.DropTable, .. SQLQeuriesHashIntColumn.TableQueries]);
            m_createIndexCommand = CreateCommand(SQLQeuriesHashIntColumn.CreateIndex);
            m_dropIndexCommand = CreateCommand(SQLQeuriesHashIntColumn.DropIndex);
            m_dropTableCommand = CreateCommand(SQLQeuriesHashIntColumn.DropTable);
            m_insertBlocksetManagedCommand = CreateCommand(SQLQeuriesHashIntColumn.InsertBlocksetManaged);
            m_selectCommand = CreateCommand(SQLQeuriesHashIntColumn.FindBlockset);
            m_selectFullHashOnlyCommand = CreateCommand(SQLQeuriesHashIntColumn.FindBlocksetHashOnly);
            m_selectLengthOnlyCommand = CreateCommand(SQLQeuriesHashIntColumn.FindBlocksetLengthOnly);
            m_selectHashOnlyIntCommand = CreateCommand(SQLQeuriesHashIntColumn.FindBlocksetHashOnlyInt);
            m_selectHashIntLengthCommand = CreateCommand(SQLQeuriesHashIntColumn.FindBlocksetHashIntLength);
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
                    (SQLQeuriesHashIntColumn.FindBlockset, new(object, string)[] { (42L, "length"), ("aoeu", "fullhash") }),
                    (SQLQeuriesHashIntColumn.FindBlocksetHashOnly, new(object, string)[] { ("aoeu", "fullhash") }),
                    (SQLQeuriesHashIntColumn.FindBlocksetLengthOnly, new(object, string)[] { (42L, "length") }),
                    (SQLQeuriesHashIntColumn.FindBlocksetHashOnlyInt, new(object, string)[] { (42L, "hash") }),
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
            RunNonQueries([SQLQeuriesHashIntColumn.DropIndex, SQLQeuriesHashIntColumn.DropTable, .. SQLQeuriesHashIntColumn.TableQueries]);
            transaction = con.BeginTransaction();

            var buffer = new byte[32];
            var alphanumericChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

            // Generate random data to insert
            for (long i = 0; i < BenchmarkParams.Count + PreFilledCount; i++)
            {
                rng.NextBytes(buffer);
                for (int j = 0; j < buffer.Length; j++)
                    buffer[j] = (byte)(buffer[j] % alphanumericChars.Length);

                var entry = (rng.NextInt64() % 100, new string([.. buffer.Select(x => alphanumericChars[x])]));
                var hashint = BitConverter.ToInt64(Encoding.UTF8.GetBytes(entry.Item2)[..8], 0);
                var hashrest = entry.Item2[8..];
                entries.Add(entry);
                m_insertBlocksetManagedCommand.SetParameterValue("id", i);
                m_insertBlocksetManagedCommand.SetParameterValue("hash", hashint);
                m_insertBlocksetManagedCommand.SetParameterValue("length", entry.Item1);
                m_insertBlocksetManagedCommand.SetParameterValue("fullhash", hashrest);
                m_insertBlocksetManagedCommand.ExecuteNonQuery(transaction);
            }

            if (BenchmarkParams.UseIndex)
                m_createIndexCommand.ExecuteNonQuery(transaction);

            // Shuffle and take a subset of the entries
            var replicated = entries.Count / 10;
            entries = [.. entries.OrderBy(x => Guid.NewGuid()).Take(BenchmarkParams.Count)];
            //// 10 % duplicates 224
            //entries.AddRange(Enumerable.Repeat(entries.Take(replicated), 10).SelectMany(x => x));
            //// Shuffle again
            //entries = [.. entries.OrderBy(x => Guid.NewGuid())];

            transaction.Commit();
            transaction.Dispose();

            ListIndexAndPlan();

            if (BenchmarkParams.UseTransaction)
                transaction = con.BeginTransaction();
        }

        //[Benchmark]
        public void SelectBenchmark()
        {
            transaction ??= con.BeginTransaction();

            for (int i = 0; i < entries.Count; i++)
            {
                var (length, fullhash) = entries[i];
                m_selectCommand.SetParameterValue("length", length);
                m_selectCommand.SetParameterValue("fullhash", fullhash[8..]);
                var id = m_selectCommand.ExecuteScalarInt64(transaction);
                if (id < 0)
                    throw new Exception($"ID not found for {length}, {fullhash}");
            }
        }

        //[Benchmark]
        public void SelectFullHashOnlyBenchmark()
        {
            transaction ??= con.BeginTransaction();

            for (int i = 0; i < entries.Count; i++)
            {
                var (length, fullhash) = entries[i];
                m_selectFullHashOnlyCommand.SetParameterValue("fullhash", fullhash);
                using var reader = m_selectFullHashOnlyCommand.ExecuteReader();
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

        [Benchmark]
        public void SelectHashOnlyIntBenchmark()
        {
            transaction ??= con.BeginTransaction();

            for (int i = 0; i < entries.Count; i++)
            {
                var (length, fullhash) = entries[i];
                var hashint = BitConverter.ToInt64(Encoding.UTF8.GetBytes(fullhash)[..8], 0);
                m_selectHashOnlyIntCommand.SetParameterValue("hash", hashint);
                using var reader = m_selectHashOnlyIntCommand.ExecuteReader();
                bool found = false;
                while (reader.Read())
                {
                    var read_id = reader.GetInt64(0);
                    var read_length = reader.GetInt64(1);
                    var read_fullhash = reader.GetString(2);
                    if (read_length == length && read_fullhash == fullhash[8..])
                    {
                        found = true;
                        break;
                    }
                }
                if (!found)
                    throw new Exception($"Hash {length} not found {hashint}");
            }
        }

        //[Benchmark]
        public void SelectHashOnlyIntDictBenchmark()
        {
            // 72
            var cache_max = entries.Count / 10;
            var cache_evict = 0.50;
            var cache_options = new MemoryCacheOptions();
            using var cache = new MemoryCache(cache_options);
            transaction ??= con.BeginTransaction();

            for (int i = 0; i < entries.Count; i++)
            {
                var (length, fullhash) = entries[i];
                if (cache.TryGetValue(fullhash, out long cached))
                {
                    var id = cached;
                    continue;
                }
                var hashint = BitConverter.ToInt64(Encoding.UTF8.GetBytes(fullhash)[..8], 0);
                m_selectHashOnlyIntCommand.SetParameterValue("hash", hashint);
                using var reader = m_selectHashOnlyIntCommand.ExecuteReader();
                bool found = false;
                while (reader.Read())
                {
                    var read_id = reader.GetInt64(0);
                    var read_length = reader.GetInt64(1);
                    var read_fullhash = reader.GetString(2);
                    if (read_length == length && read_fullhash == fullhash[8..])
                    {
                        found = true;
                        cache.Set(fullhash, read_id);
                        //if (cache.Count > cache_max)
                        //{
                        //    cache.Compact(cache_evict);
                        //}
                        break;
                    }
                }
                if (!found)
                    throw new Exception($"Hash {length} not found {hashint}");
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