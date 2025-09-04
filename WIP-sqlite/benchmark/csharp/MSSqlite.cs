using Microsoft.Data.Sqlite;
using BenchmarkDotNet.Attributes;

namespace sqlite_bench
{

    public class MSSqlite : BenchmarkSync
    {
        private SqliteConnection? m_connection;
        private SqliteCommand? m_command_insert;
        private SqliteCommand? m_command_select;
        private SqliteCommand? m_command_xor2_insert;
        private SqliteCommand? m_command_join;
        private SqliteCommand? m_command_blockset_start;
        private SqliteCommand? m_command_blockset_insert_block;
        private SqliteCommand? m_command_blockset_last_row;
        private SqliteCommand? m_command_blockset_entry_insert;
        private SqliteCommand? m_command_blockset_update;

        public MSSqlite() : base() { }

        [GlobalSetup]
        public new void GlobalSetup()
        {
            base.GlobalSetup();
            m_connection = new SqliteConnection($"Data Source=benchmark.sqlite;Pooling=false");
            m_connection.Open();

            using (var command = m_connection.CreateCommand())
                foreach (var pragma in pragmas)
                {
                    command.CommandText = pragma;
                    command.ExecuteNonQuery();
                }

            m_command_insert = m_connection.CreateCommand();
            m_command_insert.CommandText = "INSERT INTO Block (ID, Hash, Size) VALUES (@id, @hash, @size)";
            m_command_insert.Parameters.Add(new SqliteParameter("@id", System.Data.DbType.Int64));
            m_command_insert.Parameters.Add(new SqliteParameter("@hash", System.Data.DbType.String));
            m_command_insert.Parameters.Add(new SqliteParameter("@size", System.Data.DbType.Int64));
            m_command_insert.Prepare();

            m_command_select = m_connection.CreateCommand();
            m_command_select.CommandText = "SELECT ID FROM Block WHERE Hash = @hash AND Size = @size";
            m_command_select.Parameters.Add(new SqliteParameter("@hash", System.Data.DbType.String));
            m_command_select.Parameters.Add(new SqliteParameter("@size", System.Data.DbType.Int64));
            m_command_select.Prepare();

            m_command_xor2_insert = m_connection.CreateCommand();
            m_command_xor2_insert.CommandText = "INSERT OR IGNORE INTO Block (ID, Hash, Size) VALUES (@id, @hash, @size)";
            m_command_xor2_insert.Parameters.Add(new SqliteParameter("@id", System.Data.DbType.Int64));
            m_command_xor2_insert.Parameters.Add(new SqliteParameter("@hash", System.Data.DbType.String));
            m_command_xor2_insert.Parameters.Add(new SqliteParameter("@size", System.Data.DbType.Int64));
            m_command_xor2_insert.Prepare();

            m_command_join = m_connection.CreateCommand();
            m_command_join.CommandText = "SELECT Block.ID, Block.Hash, Block.Size FROM Block JOIN BlocksetEntry ON BlocksetEntry.BlockID = Block.ID WHERE BlocksetEntry.BlocksetID = @blocksetid;";
            m_command_join.Parameters.Add(new SqliteParameter("@blocksetid", System.Data.DbType.Int64));
            m_command_join.Prepare();

            m_command_blockset_insert_block = m_connection.CreateCommand();
            m_command_blockset_insert_block.CommandText = "INSERT INTO Block (Hash, Size) VALUES (@hash, @size);";
            m_command_blockset_insert_block.Parameters.Add(new SqliteParameter("@hash", System.Data.DbType.String));
            m_command_blockset_insert_block.Parameters.Add(new SqliteParameter("@size", System.Data.DbType.Int64));
            m_command_blockset_insert_block.Prepare();

            m_command_blockset_start = m_connection.CreateCommand();
            m_command_blockset_start.CommandText = "INSERT INTO Blockset (Length) VALUES (0);";
            m_command_blockset_start.Prepare();

            m_command_blockset_last_row = m_connection.CreateCommand();
            m_command_blockset_last_row.CommandText = "SELECT last_insert_rowid();";
            m_command_blockset_last_row.Prepare();

            m_command_blockset_entry_insert = m_connection.CreateCommand();
            m_command_blockset_entry_insert.CommandText = "INSERT INTO BlocksetEntry (BlocksetID, BlockID) VALUES (@blocksetid, @blockid);";
            m_command_blockset_entry_insert.Parameters.Add(new SqliteParameter("@blocksetid", System.Data.DbType.Int64));
            m_command_blockset_entry_insert.Parameters.Add(new SqliteParameter("@blockid", System.Data.DbType.Int64));
            m_command_blockset_entry_insert.Prepare();

            m_command_blockset_update = m_connection.CreateCommand();
            m_command_blockset_update.CommandText = "UPDATE Blockset SET Length = Length + 1 WHERE ID = @id;";
            m_command_blockset_update.Parameters.Add(new SqliteParameter("@id", System.Data.DbType.Int64));
            m_command_blockset_update.Prepare();
        }

        [GlobalCleanup]
        public new void GlobalCleanup()
        {
            m_command_insert?.Dispose();
            m_command_select?.Dispose();
            m_command_xor2_insert?.Dispose();
            m_command_join?.Dispose();
            m_command_blockset_insert_block?.Dispose();
            m_command_blockset_start?.Dispose();
            m_command_blockset_last_row?.Dispose();
            m_command_blockset_entry_insert?.Dispose();
            m_command_blockset_update?.Dispose();
            m_connection?.Close();
            m_connection?.Dispose();
            base.GlobalCleanup();
        }

        [IterationCleanup]
        public override void IterationCleanup()
        {
            using var cmd = m_connection!.CreateCommand();
            using var transaction = m_connection.BeginTransaction();
            cmd.Transaction = transaction;
            cmd.CommandText = "DELETE FROM Block WHERE ID >= @id";
            cmd.Parameters.Add(new SqliteParameter("@id", System.Data.DbType.Int64));
            cmd.Parameters["@id"].Value = NumEntries;
            cmd.ExecuteNonQuery();

            cmd.CommandText = "DELETE FROM BlocksetEntry WHERE BlocksetID >= @id";
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new SqliteParameter("@id", System.Data.DbType.Int64));
            cmd.Parameters["@id"].Value = m_blocksets.Count;
            cmd.ExecuteNonQuery();

            cmd.CommandText = "DELETE FROM Blockset WHERE ID >= @id";
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new SqliteParameter("@id", System.Data.DbType.Int64));
            cmd.Parameters["@id"].Value = m_blocksets.Count;
            cmd.ExecuteNonQuery();

            transaction.Commit();

            cmd.Transaction = null;
            cmd.CommandText = "PRAGMA wal_checkpoint(TRUNCATE);";
            cmd.ExecuteNonQuery();
        }

        [Benchmark]
        public override void Insert()
        {
            using var transaction = m_connection!.BeginTransaction();
            m_command_insert!.Transaction = transaction;
            foreach (var entry in EntriesToTest)
            {
                m_command_insert.Parameters["@id"].Value = entry.Id;
                m_command_insert.Parameters["@hash"].Value = entry.Hash;
                m_command_insert.Parameters["@size"].Value = entry.Size;
                m_command_insert.ExecuteNonQuery();
            }
            transaction.Commit();
        }

        [Benchmark]
        public override void Select()
        {
            using var transaction = m_connection!.BeginTransaction();
            m_command_select!.Transaction = transaction;
            try
            {
                foreach (var entry in EntriesToTest)
                {
                    m_command_select.Parameters["@hash"].Value = entry.Hash;
                    m_command_select.Parameters["@size"].Value = entry.Size;
                    long? id = (long?)m_command_select.ExecuteScalar();
                    if (id != entry.Id)
                        throw new Exception($"Failed to select entry {entry.Id}");
                }
            }
            finally
            {
                transaction.Commit();
            }
        }

        [Benchmark]
        public override void Xor1()
        {
            using var transaction = m_connection!.BeginTransaction();
            m_command_select!.Transaction = transaction;
            m_command_insert!.Transaction = transaction;
            foreach (var entry in EntriesToTest)
            {
                m_command_select.Parameters["@hash"].Value = entry.Hash;
                m_command_select.Parameters["@size"].Value = entry.Size;
                object? id = m_command_select.ExecuteScalar();
                if (id == null)
                {
                    m_command_insert.Parameters["@id"].Value = entry.Id;
                    m_command_insert.Parameters["@hash"].Value = entry.Hash;
                    m_command_insert.Parameters["@size"].Value = entry.Size;
                    m_command_insert.ExecuteNonQuery();
                }
                else if (id is long longId && longId != entry.Id)
                {
                    throw new Exception($"Failed to insert entry {entry.Id}, found {longId}");
                }
            }
            transaction.Commit();
        }

        [Benchmark]
        public override void Xor2()
        {
            using var transaction = m_connection!.BeginTransaction();
            m_command_xor2_insert!.Transaction = transaction;
            m_command_select!.Transaction = transaction;
            foreach (var entry in EntriesToTest)
            {
                m_command_xor2_insert.Parameters["@id"].Value = entry.Id;
                m_command_xor2_insert.Parameters["@hash"].Value = entry.Hash;
                m_command_xor2_insert.Parameters["@size"].Value = entry.Size;
                m_command_xor2_insert.ExecuteNonQuery();

                m_command_select.Parameters["@hash"].Value = entry.Hash;
                m_command_select.Parameters["@size"].Value = entry.Size;
                var id = (long?)m_command_select.ExecuteScalar();
                if (id != entry.Id)
                    throw new Exception($"Failed to select entry {entry.Id}");
            }
            transaction.Commit();
        }

        [Benchmark]
        public override void Join()
        {
            using var transaction = m_connection!.BeginTransaction();
            m_command_join!.Transaction = transaction;
            foreach (var (blocksetId, count, size) in BlocksetToTest)
            {
                m_command_join.Parameters["@blocksetid"].Value = blocksetId;
                using var reader = m_command_join.ExecuteReader();
                long totalSize = 0;
                long totalCount = 0;
                while (reader.Read())
                {
                    totalCount++;
                    totalSize += reader.GetInt64(2); // Size column
                }
                if (totalCount != count)
                    throw new Exception($"Blockset {blocksetId} expected {count} entries, found {totalCount}");
                if (totalSize != size)
                    throw new Exception($"Blockset {blocksetId} expected {size} total size, found {totalSize}");
            }
            transaction.Commit();
        }

        [Benchmark]
        public override void NewBlockset()
        {
            using var transaction = m_connection!.BeginTransaction();
            m_command_select!.Transaction = transaction;
            m_command_blockset_insert_block!.Transaction = transaction;
            m_command_blockset_start!.Transaction = transaction;
            m_command_blockset_last_row!.Transaction = transaction;
            m_command_blockset_entry_insert!.Transaction = transaction;
            m_command_blockset_update!.Transaction = transaction;

            m_command_blockset_start.ExecuteNonQuery();
            long? newBlocksetId = (long?)m_command_blockset_last_row!.ExecuteScalar();
            foreach (var entry in EntriesToTest)
            {
                m_command_select.Parameters["@hash"].Value = entry.Hash;
                m_command_select.Parameters["@size"].Value = entry.Size;
                long? bid = (long?)m_command_select.ExecuteScalar();
                if (bid == null)
                {
                    m_command_blockset_insert_block.Parameters["@hash"].Value = entry.Hash;
                    m_command_blockset_insert_block.Parameters["@size"].Value = entry.Size;
                    m_command_blockset_insert_block.ExecuteNonQuery();
                    bid = (long?)m_command_blockset_last_row!.ExecuteScalar();
                }
                else if (bid != entry.Id)
                {
                    throw new Exception($"Failed to insert/lookup entry {entry.Id}, found {bid}");
                }

                m_command_blockset_entry_insert.Parameters["@blocksetid"].Value = newBlocksetId;
                m_command_blockset_entry_insert.Parameters["@blockid"].Value = bid;
                m_command_blockset_entry_insert.ExecuteNonQuery();

                m_command_blockset_update.Parameters["@id"].Value = newBlocksetId;
                m_command_blockset_update.ExecuteNonQuery();

                if (m_random.NextDouble() < 0.05)
                {
                    m_command_blockset_start.ExecuteNonQuery();
                    newBlocksetId = (long?)m_command_blockset_last_row!.ExecuteScalar();
                }
            }

            transaction.Commit();
        }

    }
}