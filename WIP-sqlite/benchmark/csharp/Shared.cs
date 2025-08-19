using BenchmarkDotNet.Attributes;

namespace sqlite_bench
{

    public record Entry
    {
        public long Id { get; init; }
        public required string Hash { get; init; }
        public long Size { get; init; }
        public long BlocksetId { get; init; }
    }

    [MinColumn, MaxColumn, AllStatisticsColumn]
    public abstract class BenchmarkBase()
    {
        [Params(10_000, 100_000)]
        public static long NumEntries = 100_000;
        [Params(1_000, 10_000)]
        public static long NumRepetitions = 10_000;

        private readonly Entry[] m_entries = new Entry[NumEntries];
        protected readonly Entry[] EntriesToTest = new Entry[NumRepetitions];
        private static readonly Random m_random = new(2025_07_08);
        private readonly string[] pragmas = [
            "PRAGMA journal_mode = WAL;",
            "PRAGMA synchronous = NORMAL;",
            "PRAGMA temp_store = MEMORY;",
            "PRAGMA cache_size = -64000;",
            "PRAGMA mmap_size = 64000000;",
            "PRAGMA threads = 8;"
        ];

        private static readonly byte[] m_randomBuffer = new byte[44];
        private static readonly string alphanumericChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        private static string RandomString()
        {
            m_random.NextBytes(m_randomBuffer);
            for (int j = 0; j < m_randomBuffer.Length; j++)
                m_randomBuffer[j] = (byte)(m_randomBuffer[j] % alphanumericChars.Length);
            return new string([.. m_randomBuffer.Select(x => alphanumericChars[x])]);
        }

        public void GlobalSetup()
        {
            this.GlobalCleanup();
            using var con = new System.Data.SQLite.SQLiteConnection($"Data Source=benchmark.sqlite");
            con.Open();
            using var cmd = con.CreateCommand();
            foreach (var pragma in pragmas)
            {
                cmd.CommandText = pragma;
                cmd.ExecuteNonQuery();
            }
            using var transaction = con.BeginTransaction();
            cmd.Transaction = transaction;
            cmd.CommandText = "CREATE TABLE Blockset(ID INTEGER PRIMARY KEY, Length INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "CREATE TABLE BlocksetEntry(BlocksetID INTEGER NOT NULL, BlockID INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "CREATE TABLE Block (ID INTEGER PRIMARY KEY, Hash TEXT NOT NULL, Size INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "CREATE INDEX BlockHashSize ON Block(Hash, Size);";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "CREATE INDEX BlocksetEntryBlocksetID ON BlocksetEntry(BlocksetID);";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "CREATE INDEX BlocksetBlocksetID ON Blockset(ID);";
            cmd.ExecuteNonQuery();

            using var cmd_blockset = con.CreateCommand();
            using var cmd_blockset_entry = con.CreateCommand();
            using var cmd_block = con.CreateCommand();
            cmd_blockset.Transaction = transaction;
            cmd_blockset_entry.Transaction = transaction;
            cmd_block.Transaction = transaction;
            cmd_blockset.CommandText = "INSERT INTO Blockset(ID, Length) VALUES(@id, @length);";
            cmd_blockset_entry.CommandText = "INSERT INTO BlocksetEntry(BlocksetID, BlockID) VALUES(@blocksetid, @blockid);";
            cmd_block.CommandText = "INSERT INTO Block(ID, Hash, Size) VALUES(@id, @hash, @size);";
            cmd_blockset.Parameters.Add(new System.Data.SQLite.SQLiteParameter("@id", System.Data.DbType.Int64));
            cmd_blockset.Parameters.Add(new System.Data.SQLite.SQLiteParameter("@length", System.Data.DbType.Int64));
            cmd_blockset_entry.Parameters.Add(new System.Data.SQLite.SQLiteParameter("@blocksetid", System.Data.DbType.Int64));
            cmd_blockset_entry.Parameters.Add(new System.Data.SQLite.SQLiteParameter("@blockid", System.Data.DbType.Int64));
            cmd_block.Parameters.Add(new System.Data.SQLite.SQLiteParameter("@id", System.Data.DbType.Int64));
            cmd_block.Parameters.Add(new System.Data.SQLite.SQLiteParameter("@hash", System.Data.DbType.String));
            cmd_block.Parameters.Add(new System.Data.SQLite.SQLiteParameter("@size", System.Data.DbType.Int64));
            cmd_blockset.Prepare();
            cmd_blockset_entry.Prepare();
            cmd_block.Prepare();

            long blockset_id = 0, blockset_count = 0;
            for (int i = 0; i < NumEntries; i++)
            {
                var entry = new Entry
                {
                    Id = i,
                    Hash = RandomString(),
                    Size = m_random.Next(1, 1000),
                    BlocksetId = blockset_id
                };
                m_entries[i] = entry;
                blockset_count++;
                cmd_blockset_entry.Parameters["@blocksetid"].Value = blockset_id;
                cmd_blockset_entry.Parameters["@blockid"].Value = entry.Id;
                cmd_blockset_entry.ExecuteNonQuery();

                cmd_block.Parameters["@id"].Value = entry.Id;
                cmd_block.Parameters["@hash"].Value = entry.Hash;
                cmd_block.Parameters["@size"].Value = entry.Size;
                cmd_block.ExecuteNonQuery();

                if (m_random.NextDouble() < 0.05)
                {
                    cmd_blockset.Parameters["@id"].Value = blockset_id;
                    cmd_blockset.Parameters["@length"].Value = blockset_count;
                    cmd_blockset.ExecuteNonQuery();
                    blockset_id++;
                    blockset_count = 0;
                }
            }

            if (blockset_count > 0)
            {
                cmd_blockset.Parameters["@id"].Value = blockset_id;
                cmd_blockset.Parameters["@length"].Value = blockset_count;
                cmd_blockset.ExecuteNonQuery();
            }
            cmd.CommandText = "PRAGMA optimize;";
            cmd.ExecuteNonQuery();
            transaction.Commit();
        }

        public void GlobalCleanup()
        {
            string[] files_to_delete = [
                "benchmark.sqlite",
                "benchmark.sqlite-shm",
                "benchmark.sqlite-wal"
            ];

            foreach (var file in files_to_delete)
                if (File.Exists(file))
                    File.Delete(file);
        }

        [IterationSetup(Target = nameof(Insert))]
        public void IterationSetupInsert()
        {
            for (int i = 0; i < NumRepetitions; i++)
            {
                var entry = new Entry
                {
                    Id = i + NumEntries,
                    Hash = RandomString(),
                    Size = m_random.Next(1, 1000),
                    BlocksetId = 0
                };
                EntriesToTest[i] = entry;
            }
        }
        public abstract void Insert();

        [IterationSetup(Target = nameof(Select))]
        public void IterationSetupSelect()
        {
            for (int i = 0; i < NumRepetitions; i++)
                EntriesToTest[i] = m_entries[m_random.Next(m_entries.Length)];
        }
        public abstract void Select();

        // public abstract void Xor1();
        // public abstract void Xor2();
        // public abstract void Join();
        // public abstract void NewBlockset();
    }
}