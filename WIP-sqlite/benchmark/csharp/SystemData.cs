using System.Data.SQLite;
using BenchmarkDotNet.Attributes;

namespace sqlite_bench
{

    public class SystemData : BenchmarkBase
    {
        private SQLiteConnection? m_connection;
        private SQLiteCommand? m_command_insert;
        private SQLiteCommand? m_command_select;
        private SQLiteCommand? m_command_xor2_insert;

        public SystemData() : base() { }

        [GlobalSetup]
        public new void GlobalSetup()
        {
            base.GlobalSetup();
            m_connection = new SQLiteConnection($"Data Source=benchmark.sqlite");
            m_connection.Open();

            m_command_insert = m_connection.CreateCommand();
            m_command_insert.CommandText = "INSERT INTO Block (ID, Hash, Size) VALUES (@id, @hash, @size)";
            m_command_insert.Parameters.Add(new SQLiteParameter("@id", System.Data.DbType.Int64));
            m_command_insert.Parameters.Add(new SQLiteParameter("@hash", System.Data.DbType.String));
            m_command_insert.Parameters.Add(new SQLiteParameter("@size", System.Data.DbType.Int64));
            m_command_insert.Prepare();

            m_command_select = m_connection.CreateCommand();
            m_command_select.CommandText = "SELECT ID FROM Block WHERE Hash = @hash AND Size = @size";
            m_command_select.Parameters.Add(new SQLiteParameter("@hash", System.Data.DbType.String));
            m_command_select.Parameters.Add(new SQLiteParameter("@size", System.Data.DbType.Int64));
            m_command_select.Prepare();

            m_command_xor2_insert = m_connection.CreateCommand();
            m_command_xor2_insert.CommandText = "INSERT OR IGNORE INTO Block (ID, Hash, Size) VALUES (@id, @hash, @size)";
            m_command_xor2_insert.Parameters.Add(new SQLiteParameter("@id", System.Data.DbType.Int64));
            m_command_xor2_insert.Parameters.Add(new SQLiteParameter("@hash", System.Data.DbType.String));
            m_command_xor2_insert.Parameters.Add(new SQLiteParameter("@size", System.Data.DbType.Int64));
        }

        [GlobalCleanup]
        public new void GlobalCleanup()
        {
            m_command_insert?.Dispose();
            m_command_select?.Dispose();
            m_command_xor2_insert?.Dispose();
            m_connection?.Close();
            m_connection?.Dispose();
            base.GlobalCleanup();
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
            transaction.Rollback();
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
                    var id = (long)m_command_select.ExecuteScalar();
                    if (id != entry.Id)
                        throw new Exception($"Failed to select entry {entry.Id}");
                }
            }
            finally
            {
                transaction.Rollback();
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
                object id = m_command_select.ExecuteScalar();
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
            transaction.Rollback();
        }

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
            transaction.Rollback();
        }

    }
}