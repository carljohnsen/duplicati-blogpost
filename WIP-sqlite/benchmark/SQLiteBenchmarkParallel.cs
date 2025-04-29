//using Duplicati.Library.Main.Database;
using System.Data;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace sqlite_bench
{
    public class SQLiteBenchmarkParallel : IDisposable
    {
        // The SQLite connections
        protected List<SqliteConnection> cons = [];
        protected List<SqliteTransaction?> transactions = [];

        protected long last_id = -1;
        protected readonly string data_source = "testdb.sqlite";

        public SQLiteBenchmarkParallel()
        {
            // Delete the database file if it exists
            if (File.Exists(data_source))
            {
                File.Delete(data_source);
            }
        }

        protected SqliteCommand CreateCommand(SqliteConnection con, string query)
        {
            var cmd = con.CreateCommand();
            cmd.CommandText = query;
            var parameters = query.Split()
                .Where(s => s.Contains('@'))
                .Select(s => new string([.. s.Where(c => char.IsLetterOrDigit(c))]));

            foreach (var param in parameters)
            {
                var parameter = cmd.CreateParameter();
                parameter.ParameterName = param;
                cmd.Parameters.Add(parameter);
            }

            cmd.Prepare();
            return cmd;
        }

        protected SqliteConnection CreateConnection(Backends backend, string extra_keywords = "")
        {
            SqliteConnection con;

            switch (backend)
            {
                case Backends.MicrosoftSQLite:
                    con = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={data_source}{extra_keywords}");
                    break;
                case Backends.DuplicatiSQLite:
                case Backends.SystemSQLite:
                default:
                    throw new NotImplementedException();
            }

            con.Open();

            return con;
        }

        protected void CreateConnections(Backends backend, int count, string extra_keywords = "")
        {
            for (int i = 0; i < count; i++)
            {
                var con = CreateConnection(backend, extra_keywords);

                RunNonQueries(con, SQLQeuriesOriginal.PragmaQueries).Wait();
                cons.Add(con);

                transactions.Add(null);
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool _)
        {
            for (int i = 0; i < cons.Count; i++)
            {
                var transaction = transactions[i];
                var con = cons[i];

                try
                {
                    transaction?.Commit();
                }
                catch (InvalidOperationException)
                {
                    // Ignore, transaction already committed
                }
                catch (System.Data.SQLite.SQLiteException)
                {
                    // Ignore, transaction already committed
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
                transaction?.Dispose();
                con.Close();
                con.Dispose();
            }
        }

        protected async Task DropRows()
        {
            using SqliteCommand cmd = cons[0].CreateCommand();
            cmd.CommandText = SQLQeuriesOriginal.DropAllRows;
            cmd.Parameters.AddWithValue("@id", last_id);
            await cmd.ExecuteNonQueryAsync();
        }

        protected async Task<long> GetLastRowId()
        {
            using var cmd = cons[0].CreateCommand();
            cmd.CommandText = SQLQeuriesOriginal.LastRowId;
            var result = await cmd.ExecuteScalarAsync();
            return result == null ? -1 : Convert.ToInt64(result);
        }

        protected static async Task RunNonQueries(SqliteConnection con, string[] queries)
        {
            using SqliteCommand cmd = con.CreateCommand();
            foreach (var query in queries)
            {
                cmd.CommandText = query;
                await cmd.ExecuteNonQueryAsync();
            }
        }
    }
}