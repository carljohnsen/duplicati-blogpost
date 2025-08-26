using System.Data;
using BenchmarkDotNet.Attributes;
using Duplicati.Library.Main.Database;
using Duplicati.Library.SQLiteHelper;
using Duplicati.Library.Utility;

namespace sqlite_bench
{

    public class DuplicatiSQLite : BenchmarkSync
    {
        private IDbConnection? m_connection;
        private IDbCommand? m_command_insert;
        private IDbCommand? m_command_select;
        private IDbCommand? m_command_xor2_insert;
        private IDbCommand? m_command_join;
        private IDbCommand? m_command_blockset_start;
        private IDbCommand? m_command_blockset_insert_block;
        private IDbCommand? m_command_blockset_last_row;
        private IDbCommand? m_command_blockset_entry_insert;
        private IDbCommand? m_command_blockset_update;
        protected bool use_pragmas = true;

        public DuplicatiSQLite() : base() { }

        [GlobalSetup]
        public new void GlobalSetup()
        {
            base.GlobalSetup();
            var default_pagecache = MemoryInfo.GetTotalMemoryString(0.01, SQLiteLoader.MINIMUM_SQLITE_PAGE_CACHE_SIZE); // 1% of the total memory
            var pagecache = Sizeparser.ParseSize(default_pagecache, "kb");
            m_connection = SQLiteLoader.LoadConnection("benchmark.sqlite", 0);

            if (use_pragmas)
                using (var command = m_connection.CreateCommand())
                    foreach (var pragma in pragmas)
                        command.ExecuteNonQuery(pragma);

            m_command_insert = m_connection.CreateCommand("INSERT INTO Block (ID, Hash, Size) VALUES (@id, @hash, @size)");
            m_command_insert.Prepare();

            m_command_select = m_connection.CreateCommand("SELECT ID FROM Block WHERE Hash = @hash AND Size = @size");
            m_command_select.Prepare();

            m_command_xor2_insert = m_connection.CreateCommand("INSERT OR IGNORE INTO Block (ID, Hash, Size) VALUES (@id, @hash, @size)");
            m_command_xor2_insert.Prepare();

            m_command_join = m_connection.CreateCommand("SELECT Block.ID, Block.Hash, Block.Size FROM Block JOIN BlocksetEntry ON BlocksetEntry.BlockID = Block.ID WHERE BlocksetEntry.BlocksetID = @blocksetid;");
            m_command_join.Prepare();

            m_command_blockset_insert_block = m_connection.CreateCommand("INSERT INTO Block (Hash, Size) VALUES (@hash, @size);");
            m_command_blockset_insert_block.Prepare();

            m_command_blockset_start = m_connection.CreateCommand("INSERT INTO Blockset (Length) VALUES (0);");
            m_command_blockset_start.Prepare();

            m_command_blockset_last_row = m_connection.CreateCommand("SELECT last_insert_rowid();");
            m_command_blockset_last_row.Prepare();

            m_command_blockset_entry_insert = m_connection.CreateCommand("INSERT INTO BlocksetEntry (BlocksetID, BlockID) VALUES (@blocksetid, @blockid);");
            m_command_blockset_entry_insert.Prepare();

            m_command_blockset_update = m_connection.CreateCommand("UPDATE Blockset SET Length = Length + 1 WHERE ID = @id;");
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

            cmd.SetCommandAndParameters("DELETE FROM Block WHERE ID >= @id")
                .SetParameterValue("@id", NumEntries)
                .ExecuteNonQuery();

            cmd.SetCommandAndParameters("DELETE FROM BlocksetEntry WHERE BlocksetID >= @id")
                .SetParameterValue("@id", m_blocksets.Count)
                .ExecuteNonQuery();

            cmd.SetCommandAndParameters("DELETE FROM Blockset WHERE ID >= @id")
                .SetParameterValue("@id", m_blocksets.Count)
                .ExecuteNonQuery();

            transaction.Commit();
        }

        [Benchmark]
        public override void Insert()
        {
            using var transaction = m_connection!.BeginTransaction();
            m_command_insert!.Transaction = transaction;
            foreach (var entry in EntriesToTest)
            {
                m_command_insert
                    .SetParameterValue("@id", entry.Id)
                    .SetParameterValue("@hash", entry.Hash)
                    .SetParameterValue("@size", entry.Size)
                    .ExecuteNonQuery();
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
                    var id = m_command_select
                        .SetParameterValue("@hash", entry.Hash)
                        .SetParameterValue("@size", entry.Size)
                        .ExecuteScalarInt64(-1);

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
                var id = m_command_select
                    .SetParameterValue("@hash", entry.Hash)
                    .SetParameterValue("@size", entry.Size)
                    .ExecuteScalarInt64(-1);
                if (id == -1)
                {
                    m_command_insert
                        .SetParameterValue("@id", entry.Id)
                        .SetParameterValue("@hash", entry.Hash)
                        .SetParameterValue("@size", entry.Size)
                        .ExecuteNonQuery();
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
                m_command_xor2_insert
                    .SetParameterValue("@id", entry.Id)
                    .SetParameterValue("@hash", entry.Hash)
                    .SetParameterValue("@size", entry.Size)
                    .ExecuteNonQuery();

                var id = m_command_select
                    .SetParameterValue("@hash", entry.Hash)
                    .SetParameterValue("@size", entry.Size)
                    .ExecuteScalarInt64(-1);
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
                using var reader = m_command_join
                    .SetParameterValue("@blocksetid", blocksetId)
                    .ExecuteReader();
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
            long newBlocksetId = m_command_blockset_last_row!.ExecuteScalarInt64(-1);
            foreach (var entry in EntriesToTest)
            {
                long bid = m_command_select
                    .SetParameterValue("@hash", entry.Hash)
                    .SetParameterValue("@size", entry.Size)
                    .ExecuteScalarInt64(-1);
                if (bid == -1)
                {
                    m_command_blockset_insert_block
                        .SetParameterValue("@hash", entry.Hash)
                        .SetParameterValue("@size", entry.Size)
                        .ExecuteNonQuery();
                    bid = m_command_blockset_last_row!.ExecuteScalarInt64(-1);
                }
                else if (bid != entry.Id)
                {
                    throw new Exception($"Failed to insert/lookup entry {entry.Id}, found {bid}");
                }

                m_command_blockset_entry_insert
                    .SetParameterValue("@blocksetid", newBlocksetId)
                    .SetParameterValue("@blockid", bid)
                    .ExecuteNonQuery();

                m_command_blockset_update
                    .SetParameterValue("@id", newBlocksetId)
                    .ExecuteNonQuery();

                if (m_random.NextDouble() < 0.05)
                {
                    m_command_blockset_start.ExecuteNonQuery();
                    newBlocksetId = m_command_blockset_last_row!.ExecuteScalarInt64(-1);
                }
            }

            transaction.Commit();
        }

    }

    public class DuplicatiSQLiteNoPragmas : DuplicatiSQLite
    {
        public DuplicatiSQLiteNoPragmas() : base()
        {
            use_pragmas = false;
        }
    }
}