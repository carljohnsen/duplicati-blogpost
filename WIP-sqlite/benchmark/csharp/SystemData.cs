using System.Data.SQLite;
using BenchmarkDotNet.Attributes;

namespace sqlite_bench
{

    public class SystemData : BenchmarkBase
    {
        private SQLiteConnection? m_connection;
        private SQLiteCommand? m_command_select;

        public SystemData() : base() { }

        [GlobalSetup]
        public new void GlobalSetup()
        {
            base.GlobalSetup();
            m_connection = new SQLiteConnection($"Data Source=benchmark.sqlite");
            m_connection.Open();
            m_command_select = m_connection.CreateCommand();
            m_command_select.CommandText = "SELECT ID FROM Block WHERE Hash = @hash AND Size = @size";
            m_command_select.Parameters.Add(new SQLiteParameter("@hash", System.Data.DbType.String));
            m_command_select.Parameters.Add(new SQLiteParameter("@size", System.Data.DbType.Int64));
            m_command_select.Prepare();
        }

        [GlobalCleanup]
        public new void GlobalCleanup()
        {
            m_command_select?.Dispose();
            m_connection?.Close();
            m_connection?.Dispose();
            base.GlobalCleanup();
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
    }

}